package broker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// defaultSessionTimeout is used when a client requests a non-positive one.
const defaultSessionTimeout = 30 * time.Second

// initialRebalanceDelay is how long the first rebalance of an empty group waits
// for more members before completing (Kafka's group.initial.rebalance.delay.ms).
// Without it the first consumer of an application forms a generation alone within
// microseconds, and each of the others starting beside it forces another: N
// consumers, N rebalances. Kafka waits three seconds; a local broker need not, but
// it must wait long enough for the rest of one process's consumers to arrive.
var initialRebalanceDelay = 200 * time.Millisecond

// The bounds on a classic member's session.timeout.ms: Apache Kafka's defaults of
// group.min.session.timeout.ms (6 s) and group.max.session.timeout.ms (30 min),
// recorded against apache/kafka (conformance/recordings/joingroup-session-timeout.json).
// A session outside them is INVALID_SESSION_TIMEOUT, checked before anything
// else about the request. A client developed here with a 1 s session would
// otherwise ship a setting every real broker refuses. Variables so a test of
// the expiry machinery can use a short one.
var (
	minSessionTimeout = 6 * time.Second
	maxSessionTimeout = 30 * time.Minute
)

// pendingMemberTTL is how long a member id issued with MEMBER_ID_REQUIRED stays
// valid for the join that should follow it.
const pendingMemberTTL = 5 * time.Minute

// Which protocol a group id belongs to.
const (
	protoClassic  = "classic"
	protoConsumer = "consumer"
)

// groupClaims records which protocol each live group id belongs to. A group is
// classic or KIP-848, never both: with one coordinator asking the other "do you
// have members?" and then joining, two first joins arriving together (one per
// protocol) both saw "no" and both succeeded. Claiming is one step under one lock.
type groupClaims struct {
	mu    sync.Mutex
	owner map[string]string
}

func newGroupClaims() *groupClaims { return &groupClaims{owner: map[string]string{}} }

// claim takes the group id for proto, or confirms proto already holds it.
func (c *groupClaims) claim(id, proto string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.owner[id]; ok {
		return cur == proto
	}
	c.owner[id] = proto
	return true
}

// release gives the id up, if proto holds it (its last member is gone).
func (c *groupClaims) release(id, proto string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.owner[id] == proto {
		delete(c.owner, id)
	}
}

// ownerOf is the protocol holding the id, or "".
func (c *groupClaims) ownerOf(id string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.owner[id]
}

// groupManager holds ephemeral, in-memory consumer-group state. Membership,
// generation, and assignments live only in memory and are rebuilt when clients
// rejoin after a restart; only committed offsets (in the metadata store) are
// durable. In the classic protocol the broker is a relay — the elected client
// leader computes assignments — so we shuttle opaque bytes and never assign
// partitions ourselves. See docs/consumer-groups.md.
//
// All state transitions happen under mu. Handlers block for a deferred JoinGroup
// or SyncGroup response on a per-request channel *after* releasing mu; completions
// and timer callbacks deliver on those channels via non-blocking buffered sends.
type groupManager struct {
	mu     sync.Mutex
	groups map[string]*group

	// hasOffsets reports whether a group has committed offsets. An empty group
	// that has none holds nothing worth keeping and is forgotten; one that has
	// some stays listed (as Empty) because its offsets are durable.
	hasOffsets func(group string) bool

	claims *groupClaims

	// pending holds member ids handed out with MEMBER_ID_REQUIRED and not yet
	// used: group -> member id -> when issued. The join that follows must bring
	// one of these (or the id of an existing member); any other id is unknown.
	pending map[string]map[string]time.Time

	stopped bool // the broker is shutting down: timers do nothing.

	sink GroupEventSink // told what happens in groups; nil = nobody is watching.
}

// stop makes every timer a no-op. After shutdown a session timer would otherwise
// fire into a closed metadata store.
func (gm *groupManager) stop() {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.stopped = true
	for _, g := range gm.groups {
		g.join.stop()
		for _, m := range g.members {
			m.session.stop()
		}
	}
}

func newGroupManager(hasOffsets func(string) bool, claims *groupClaims) *groupManager {
	if claims == nil {
		claims = newGroupClaims()
	}
	return &groupManager{groups: make(map[string]*group), hasOffsets: hasOffsets, claims: claims,
		pending: make(map[string]map[string]time.Time)}
}

// issue remembers a member id handed out for a later join (caller holds mu).
func (gm *groupManager) issue(group, id string) {
	cutoff := time.Now().Add(-pendingMemberTTL)
	for g, ids := range gm.pending {
		for mid, at := range ids {
			if at.Before(cutoff) {
				delete(ids, mid)
			}
		}
		if len(ids) == 0 {
			delete(gm.pending, g)
		}
	}
	if gm.pending[group] == nil {
		gm.pending[group] = map[string]time.Time{}
	}
	gm.pending[group][id] = time.Now()
}

// issued reports whether id was handed out for group and is still valid (caller
// holds mu).
func (gm *groupManager) issued(group, id string) bool {
	at, ok := gm.pending[group][id]
	return ok && time.Since(at) <= pendingMemberTTL
}

// redeem uses up an issued id once the member it was issued to has joined (caller
// holds mu). A join that is refused does not spend the id: the client may retry.
func (gm *groupManager) redeem(group, id string) {
	delete(gm.pending[group], id)
	if len(gm.pending[group]) == 0 {
		delete(gm.pending, group)
	}
}

// groupState is the classic-protocol group lifecycle state.
type groupState int

const (
	stateEmpty groupState = iota
	statePreparingRebalance
	stateCompletingRebalance
	stateStable
)

// String renders the state as the name Kafka reports in Describe/ListGroups.
func (s groupState) String() string {
	switch s {
	case stateEmpty:
		return "Empty"
	case statePreparingRebalance:
		return "PreparingRebalance"
	case stateCompletingRebalance:
		return "CompletingRebalance"
	case stateStable:
		return "Stable"
	default:
		return "Unknown"
	}
}

type group struct {
	id           string
	state        groupState
	generation   int32
	leader       string
	protocolType string // what the members are: "consumer", "connect", ...
	protocol     string // selected assignor name.
	members      map[string]*groupMember
	static       map[string]string // group.instance.id -> member id, for static members.
	holding      bool              // the initial rebalance delay is running: wait for it even if everyone has joined.
	pendingSync  map[string]chan syncResult
	join         expiry // how long the rebalance waits for members to rejoin.
}

// clientRef says who a member is: the client.id it connected with and the host it
// connected from. A group's members are otherwise only opaque generated ids, which
// tells nobody which of their applications is which.
type clientRef struct{ clientID, host string }

type groupMember struct {
	client         clientRef
	id             string
	instanceID     *string
	protocols      []groupProtocolEntry // ordered by client preference.
	assignment     []byte               // from the leader's SyncGroup.
	rejoined       bool
	sessionTimeout time.Duration
	session        expiry // evicts the member if it stops heartbeating.
	joinResult     chan joinResult
	awaitingJoin   bool // a JoinGroup is parked on joinResult.

	// rebalanceTimeout is how long this member says it may take to rejoin a
	// rebalance (the client's max.poll.interval.ms). Zero means "not stated",
	// which JoinGroup v0 cannot state: Kafka then uses the session timeout.
	rebalanceTimeout time.Duration
}

type groupProtocolEntry struct {
	name     string
	metadata []byte
}

type joinResult struct {
	errorCode  int16
	generation int32
	protocol   string
	leaderID   string
	memberID   string
	members    []joinMemberInfo // populated only for the leader.
}

type joinMemberInfo struct {
	memberID   string
	instanceID *string
	metadata   []byte
}

type syncResult struct {
	errorCode  int16
	assignment []byte
}

type leaveResult struct {
	memberID   string
	instanceID *string
	errorCode  int16
}

// hasMembers reports whether the group id is in use here, so the KIP-848
// coordinator can refuse to share it (a group is one protocol or the other).
func (gm *groupManager) hasMembers(group string) bool {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[group]
	return g != nil && len(g.members) > 0
}

// forget drops an empty group object, reporting whether there was one. A group
// with live members is left alone (callers check first).
func (gm *groupManager) forget(group string) bool {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[group]
	if g == nil || len(g.members) > 0 {
		return false
	}
	g.join.stop()
	delete(gm.groups, group)
	gm.claims.release(group, protoClassic)
	return true
}

// join runs the classic JoinGroup relay. It blocks until the rebalance completes
// (all known members rejoined, or the collect timeout), returning this member's
// slice of the result. A first-contact member (empty id) gets MEMBER_ID_REQUIRED
// with a fresh id and must retry (KIP-394) — but only from JoinGroup v4, the
// first version that knows the code. An older client would not know what to do
// with it, so it is given an id and joined in one step, as Kafka does.
func (gm *groupManager) join(ctx context.Context, r *kmsg.JoinGroupRequest, who clientRef) (joinResult, error) {
	refuse := func(code int16) (joinResult, error) {
		return joinResult{errorCode: int16(code), memberID: r.MemberID, generation: -1}, nil
	}
	// Kafka checks the session timeout first of all, before it hands out a member
	// id or looks at protocols.
	if session := time.Duration(r.SessionTimeoutMillis) * time.Millisecond; session < minSessionTimeout || session > maxSessionTimeout {
		return refuse(kerr.InvalidSessionTimeout.Code)
	}
	// A member with no protocols has nothing the group could agree on.
	if len(r.Protocols) == 0 {
		return refuse(kerr.InconsistentGroupProtocol.Code)
	}
	static := r.InstanceID != nil && *r.InstanceID != ""

	gm.mu.Lock()
	// A group running under KIP-848 is not a classic group, whatever else is
	// wrong with this request.
	if owner := gm.claims.ownerOf(r.Group); owner != "" && owner != protoClassic {
		gm.mu.Unlock()
		return refuse(kerr.InconsistentGroupProtocol.Code)
	}
	g := gm.groups[r.Group]

	// Static membership (group.instance.id): the instance id, not the member id,
	// is the member's identity. A restarted instance arrives with no member id and
	// takes over its predecessor's place; a second process claiming an instance
	// that is already held under another member id is fenced.
	if static && g != nil {
		if cur, held := g.static[*r.InstanceID]; held && g.members[cur] != nil {
			switch {
			case r.MemberID == "":
				if res, done := gm.replaceStatic(g, cur, r, who); done {
					gm.mu.Unlock()
					return res, nil
				}
			case r.MemberID != cur:
				gm.mu.Unlock()
				return refuse(kerr.FencedInstanceID.Code)
			}
		}
	}

	switch {
	case r.MemberID == "":
		id := generateMemberID(r.InstanceID)
		// KIP-394: a dynamic member on v4+ is first given an id and told to come
		// back with it. Static members and older clients join in one step.
		if r.GetVersion() >= 4 && !static {
			gm.issue(r.Group, id)
			gm.mu.Unlock()
			return joinResult{errorCode: kerr.MemberIDRequired.Code, memberID: id, generation: -1}, nil
		}
		r.MemberID = id
	case g == nil || g.members[r.MemberID] == nil:
		// An id this coordinator did not issue and no current member holds: the
		// member was removed (or never existed) and must start again.
		if !gm.issued(r.Group, r.MemberID) {
			gm.mu.Unlock()
			return refuse(kerr.UnknownMemberID.Code)
		}
	}

	if !gm.claims.claim(r.Group, protoClassic) {
		gm.mu.Unlock()
		return refuse(kerr.InconsistentGroupProtocol.Code)
	}
	g = gm.getOrCreate(r.Group)

	// Everyone in a group must be the same kind of member and share at least one
	// protocol (assignor). A joiner that cannot agree is refused; it used to be
	// admitted, and the leader was handed empty metadata for it.
	offered := protocolEntries(r.Protocols)
	if others := g.othersThan(r.MemberID); len(others) > 0 {
		if r.ProtocolType != g.protocolType || !commonProtocol(offered, others) {
			gm.mu.Unlock()
			return refuse(kerr.InconsistentGroupProtocol.Code)
		}
	} else {
		g.protocolType = r.ProtocolType
	}

	m := g.members[r.MemberID]
	// A follower that rejoins a stable group with nothing changed is told where
	// the group is; it does not need a rebalance, and forcing one made every
	// such rejoin cost the whole group a generation.
	if m != nil && g.state == stateStable && r.MemberID != g.leader && sameProtocols(m.protocols, offered) {
		m.client = who
		gm.resetSession(g, m)
		res := joinResult{generation: g.generation, protocol: g.protocol, leaderID: g.leader, memberID: m.id}
		gm.mu.Unlock()
		return res, nil
	}
	if m == nil {
		m = &groupMember{id: r.MemberID}
		g.members[r.MemberID] = m
		gm.redeem(r.Group, r.MemberID)
		gm.emit(g.id, "joined", m.id, g.generation, "client "+who.clientID)
	}
	// A retried JoinGroup replaces one still parked: the old request's client is
	// waiting on an answer that this one supersedes, so release it (Kafka answers
	// it UNKNOWN_MEMBER_ID) instead of leaving its goroutine blocked.
	m.failPendingJoin()
	m.client = who
	m.instanceID = r.InstanceID
	if static {
		g.static[*r.InstanceID] = m.id
	}
	m.protocols = offered
	m.sessionTimeout = time.Duration(r.SessionTimeoutMillis) * time.Millisecond
	m.rebalanceTimeout = 0
	if r.RebalanceTimeoutMillis > 0 {
		m.rebalanceTimeout = time.Duration(r.RebalanceTimeoutMillis) * time.Millisecond
	}

	if g.state != statePreparingRebalance {
		wasEmpty := g.state == stateEmpty
		g.beginRebalance()
		if m.rejoined || wasEmpty {
			gm.emit(g.id, "rebalance", m.id, g.generation, "a member joined")
		} else {
			gm.emit(g.id, "rebalance", m.id, g.generation, "a member rejoined")
		}
		g.holding = wasEmpty && initialRebalanceDelay > 0
	}
	m.rejoined = true
	m.awaitingJoin = true
	m.joinResult = make(chan joinResult, 1)
	gm.resetSession(g, m)

	if g.allRejoined() && !g.holding {
		gm.completeJoin(g)
	} else {
		gm.armJoinTimer(g)
	}

	ch := m.joinResult
	gm.mu.Unlock()

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return joinResult{}, ctx.Err()
	}
}

// replaceStatic hands a static member's place to its restarted instance, which
// has a new member id. In a stable group with unchanged protocols that is all
// that happens: no rebalance, the assignment is kept, and done is true with the
// answer. Otherwise the member is swapped in and the caller carries on into a
// normal rebalance (done is false, r.MemberID holds the new id). Caller holds mu.
func (gm *groupManager) replaceStatic(g *group, oldID string, r *kmsg.JoinGroupRequest, who clientRef) (joinResult, bool) {
	old := g.members[oldID]
	newID := generateMemberID(r.InstanceID)
	offered := protocolEntries(r.Protocols)

	// Whatever the old incarnation had parked is answered: it has been replaced.
	if old.awaitingJoin && old.joinResult != nil {
		old.joinResult <- joinResult{errorCode: kerr.FencedInstanceID.Code, memberID: oldID, generation: -1}
		old.awaitingJoin = false
	}
	if ch, ok := g.pendingSync[oldID]; ok {
		ch <- syncResult{errorCode: kerr.FencedInstanceID.Code}
		delete(g.pendingSync, oldID)
	}
	old.session.stop()
	delete(g.members, oldID)

	m := &groupMember{
		id: newID, instanceID: r.InstanceID, client: who, protocols: offered,
		assignment: old.assignment, rejoined: old.rejoined,
		sessionTimeout: time.Duration(r.SessionTimeoutMillis) * time.Millisecond,
	}
	if r.RebalanceTimeoutMillis > 0 {
		m.rebalanceTimeout = time.Duration(r.RebalanceTimeoutMillis) * time.Millisecond
	}
	g.members[newID] = m
	g.static[*r.InstanceID] = newID
	if g.leader == oldID {
		g.leader = newID
	}
	r.MemberID = newID

	if g.state != stateStable || !sameProtocols(old.protocols, offered) {
		return joinResult{}, false
	}
	gm.resetSession(g, m)
	res := joinResult{generation: g.generation, protocol: g.protocol, leaderID: g.leader, memberID: newID}
	if newID == g.leader {
		res.members = g.memberInfos(g.protocol)
	}
	return res, true
}

// fenced reports whether a request naming a static instance comes from a member
// id that no longer holds it (its restarted successor does).
func (g *group) fenced(memberID string, instanceID *string) bool {
	if instanceID == nil || *instanceID == "" {
		return false
	}
	cur, held := g.static[*instanceID]
	return held && cur != memberID
}

// othersThan is the group's members apart from id.
func (g *group) othersThan(id string) []*groupMember {
	out := make([]*groupMember, 0, len(g.members))
	for mid, m := range g.members {
		if mid != id {
			out = append(out, m)
		}
	}
	return out
}

// commonProtocol reports whether at least one offered protocol is supported by
// every one of the given members.
func commonProtocol(offered []groupProtocolEntry, members []*groupMember) bool {
	for _, p := range offered {
		all := true
		for _, m := range members {
			if !m.has(p.name) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

func sameProtocols(a, b []groupProtocolEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].name != b[i].name || !bytes.Equal(a[i].metadata, b[i].metadata) {
			return false
		}
	}
	return true
}

// sync distributes assignments. The leader uploads everyone's assignment; each
// member (leader included) receives its own slice. Followers block until the
// leader's SyncGroup arrives.
func (gm *groupManager) sync(ctx context.Context, r *kmsg.SyncGroupRequest) (syncResult, error) {
	gm.mu.Lock()
	g := gm.groups[r.Group]
	if g == nil {
		gm.mu.Unlock()
		return syncResult{errorCode: kerr.UnknownMemberID.Code}, nil
	}
	if g.fenced(r.MemberID, r.InstanceID) {
		gm.mu.Unlock()
		return syncResult{errorCode: kerr.FencedInstanceID.Code}, nil
	}
	m := g.members[r.MemberID]
	if m == nil {
		gm.mu.Unlock()
		return syncResult{errorCode: kerr.UnknownMemberID.Code}, nil
	}
	if r.Generation != g.generation {
		gm.mu.Unlock()
		return syncResult{errorCode: kerr.IllegalGeneration.Code}, nil
	}
	if g.state == statePreparingRebalance {
		gm.mu.Unlock()
		return syncResult{errorCode: kerr.RebalanceInProgress.Code}, nil
	}
	gm.resetSession(g, m)

	if r.MemberID == g.leader {
		for _, a := range r.GroupAssignment {
			if target := g.members[a.MemberID]; target != nil {
				target.assignment = a.MemberAssignment
			}
		}
		g.state = stateStable
		gm.emit(g.id, "assigned", r.MemberID, g.generation, plural(len(r.GroupAssignment), "member")+" given an assignment by the leader")
		for id, ch := range g.pendingSync {
			ch <- syncResult{assignment: g.members[id].assignment}
			gm.resetSession(g, g.members[id]) // it has been parked; its session restarts now.
		}
		g.pendingSync = make(map[string]chan syncResult)
		assignment := m.assignment
		gm.mu.Unlock()
		return syncResult{assignment: assignment}, nil
	}

	// Follower: return immediately if the leader already synced, else wait.
	if g.state == stateStable {
		assignment := m.assignment
		gm.mu.Unlock()
		return syncResult{assignment: assignment}, nil
	}
	// A second SyncGroup from the same member supersedes one still parked (a
	// retry on a new connection). The first is answered, not dropped: nothing
	// else would ever release it.
	if old, parked := g.pendingSync[r.MemberID]; parked {
		old <- syncResult{errorCode: kerr.UnknownMemberID.Code}
	}
	ch := make(chan syncResult, 1)
	g.pendingSync[r.MemberID] = ch
	gm.mu.Unlock()

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return syncResult{}, ctx.Err()
	}
}

// heartbeat refreshes a member's session and reports whether a rebalance is under
// way (so the member rejoins).
func (gm *groupManager) heartbeat(r *kmsg.HeartbeatRequest) int16 {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[r.Group]
	if g == nil {
		return kerr.UnknownMemberID.Code
	}
	if g.fenced(r.MemberID, r.InstanceID) {
		return kerr.FencedInstanceID.Code
	}
	m := g.members[r.MemberID]
	if m == nil {
		return kerr.UnknownMemberID.Code
	}
	if r.Generation != g.generation {
		return kerr.IllegalGeneration.Code
	}
	gm.resetSession(g, m)
	if g.state == statePreparingRebalance {
		return kerr.RebalanceInProgress.Code
	}
	return errNone
}

// leave removes the named members and, if any were removed, rebalances the rest.
// A member is named by its member id or, for a static member, its instance id.
// Naming a member that is not in the group changes nothing: it used to start a
// rebalance of a perfectly healthy group (a consumer evicted earlier, shutting
// down with its old id, was enough).
func (gm *groupManager) leave(r *kmsg.LeaveGroupRequest) []leaveResult {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[r.Group]

	var results []leaveResult
	removed := false
	for _, who := range membersToLeave(r) {
		res := leaveResult{memberID: who.memberID, instanceID: who.instanceID, errorCode: kerr.UnknownMemberID.Code}
		if g != nil {
			id := who.memberID
			if who.instanceID != nil && *who.instanceID != "" {
				cur, held := g.static[*who.instanceID]
				switch {
				case !held:
					id = ""
				case id != "" && id != cur:
					res.errorCode = kerr.FencedInstanceID.Code
					id = ""
				default:
					id = cur
				}
			}
			if id != "" && g.members[id] != nil {
				gm.emit(g.id, "left", id, g.generation, "client "+g.members[id].client.clientID+" left the group")
				gm.removeMember(g, id)
				res.memberID, res.errorCode = id, errNone
				removed = true
			}
		}
		results = append(results, res)
	}
	if g != nil && removed {
		gm.rebalanceAfterChange(g, "a member left")
	}
	return results
}

// validateCommit decides whether an OffsetCommit may be stored, by Kafka's rules:
//
//   - a commit that names no generation (a simple consumer, or an admin tool
//     resetting offsets) is allowed only while the group has no members;
//   - otherwise the member must be in the group, at the group's generation, and the
//     group must not be waiting for the leader's assignment.
//
// The check used to be skipped whenever the generation was negative, so anything
// could overwrite a running group's offsets, and whenever the group had no
// members, so a consumer that had already left could commit at its old generation.
func (gm *groupManager) validateCommit(groupID, memberID string, instanceID *string, generation int32) int16 {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[groupID]
	if g == nil {
		if generation < 0 {
			return errNone
		}
		return kerr.IllegalGeneration.Code
	}
	if generation < 0 && len(g.members) == 0 {
		return errNone
	}
	if g.fenced(memberID, instanceID) {
		return kerr.FencedInstanceID.Code
	}
	if g.members[memberID] == nil {
		return kerr.UnknownMemberID.Code
	}
	if generation != g.generation {
		return kerr.IllegalGeneration.Code
	}
	if g.state == stateCompletingRebalance {
		return kerr.RebalanceInProgress.Code
	}
	return errNone
}

// --- internal state transitions (all called under gm.mu) ---

func (gm *groupManager) getOrCreate(id string) *group {
	g := gm.groups[id]
	if g == nil {
		g = &group{id: id, state: stateEmpty, members: make(map[string]*groupMember), static: make(map[string]string)}
		gm.groups[id] = g
	}
	return g
}

func (g *group) beginRebalance() {
	g.state = statePreparingRebalance
	for _, m := range g.members {
		m.rejoined = false
	}
	// Followers parked in SyncGroup are waiting for an assignment the leader has
	// not delivered, and this rebalance supersedes the generation it belongs to.
	// Tell them so; otherwise they sit until their own client timeout (30s for
	// the Java client) while the server goroutine stays blocked.
	g.failPendingSyncs(kerr.RebalanceInProgress.Code)
}

// failPendingSyncs answers every parked SyncGroup with code and forgets them.
func (g *group) failPendingSyncs(code int16) {
	for id, ch := range g.pendingSync {
		ch <- syncResult{errorCode: int16(code)} // buffered, never blocks.
		delete(g.pendingSync, id)
	}
}

// failPendingJoin answers a JoinGroup still parked for this member, if any.
func (m *groupMember) failPendingJoin() {
	if m.awaitingJoin && m.joinResult != nil {
		m.joinResult <- joinResult{errorCode: kerr.UnknownMemberID.Code, memberID: m.id, generation: -1}
	}
	m.awaitingJoin = false
}

// rebalanceTimeout is how long a rebalance may wait for members to rejoin: the
// longest any member asked for (Kafka's rule), each falling back to its session
// timeout. This is the client's max.poll.interval.ms, so a consumer busy in a
// handler is waited for rather than evicted.
func (g *group) rebalanceTimeout() time.Duration {
	var longest time.Duration
	for _, m := range g.members {
		t := m.rebalanceTimeout
		if t <= 0 {
			t = m.sessionTimeout
		}
		if t <= 0 {
			t = defaultSessionTimeout
		}
		longest = max(longest, t)
	}
	if longest == 0 {
		longest = defaultSessionTimeout
	}
	return longest
}

func (g *group) allRejoined() bool {
	if len(g.members) == 0 {
		return false
	}
	for _, m := range g.members {
		if !m.rejoined {
			return false
		}
	}
	return true
}

// completeJoin closes the rebalance: drop members that never rejoined, bump the
// generation, elect a leader, pick a common protocol, and deliver every waiting
// JoinGroup response.
func (g *group) completeJoin() (dropped []string) {
	g.join.stop()
	g.holding = false
	for id, m := range g.members {
		if !m.rejoined {
			m.session.stop()
			delete(g.members, id)
			dropped = append(dropped, id)
		}
	}
	if len(g.members) == 0 {
		g.state = stateEmpty
		g.leader = ""
		return dropped
	}
	for inst, id := range g.static {
		if g.members[id] == nil {
			delete(g.static, inst)
		}
	}

	g.generation++
	// A new generation starts with no assignments. If the leader's SyncGroup
	// omits a member, it must get an empty assignment — not the previous
	// generation's, which would hand it partitions another member now owns.
	for _, m := range g.members {
		m.assignment = nil
	}
	g.protocol = g.selectProtocol()
	g.leader = g.electLeader()
	g.state = stateCompletingRebalance
	g.pendingSync = make(map[string]chan syncResult)

	leaderView := g.memberInfos(g.protocol)
	for id, m := range g.members {
		res := joinResult{
			generation: g.generation,
			protocol:   g.protocol,
			leaderID:   g.leader,
			memberID:   id,
		}
		if id == g.leader {
			res.members = leaderView
		}
		m.joinResult <- res
		m.awaitingJoin = false
	}
	return dropped
}

// completeJoin closes the rebalance and forgets the group if nobody was left.
func (gm *groupManager) completeJoin(g *group) {
	for _, id := range g.completeJoin() {
		gm.emit(g.id, "dropped", id, g.generation, "did not rejoin before the rebalance timeout")
	}
	if g.state == stateEmpty {
		gm.dropIfUnused(g)
		return
	}
	gm.emit(g.id, "generation", g.leader, g.generation,
		plural(len(g.members), "member")+", leader "+g.leader+", protocol "+g.protocol)
	// Every member's session starts afresh with the new generation. A member was
	// last heard from when it sent its JoinGroup, possibly a whole rebalance ago;
	// without this its timer could fire in the moment between its join being
	// answered and its SyncGroup arriving.
	for _, m := range g.members {
		gm.resetSession(g, m)
	}
}

// dropIfUnused forgets an Empty group that has no committed offsets. A group
// that still has offsets stays, because those are durable and a consumer
// resuming under the same id expects to find them.
func (gm *groupManager) dropIfUnused(g *group) {
	if gm.groups[g.id] != g || len(g.members) != 0 {
		return
	}
	// No members: the id is free for either protocol again.
	gm.claims.release(g.id, protoClassic)
	if gm.hasOffsets != nil && gm.hasOffsets(g.id) {
		return
	}
	delete(gm.groups, g.id)
}

// armJoinTimer starts the rebalance's wait, unless it is already running: a
// rebalance has one deadline, set when it begins, however many members join it.
func (gm *groupManager) armJoinTimer(g *group) {
	if g.join.armed() {
		return
	}
	wait := g.rebalanceTimeout()
	if g.holding {
		wait = min(wait, initialRebalanceDelay)
	}
	g.join.arm(wait, func(gen uint64) { gm.joinExpired(g, gen) })
}

// joinExpired is the join timer firing: the rebalance stops waiting for members
// that have not rejoined. A firing whose gen is no longer current belongs to a
// rebalance that already ended or restarted, and does nothing (see expiry).
func (gm *groupManager) joinExpired(g *group, gen uint64) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if gm.stopped || !g.join.current(gen) || g.state != statePreparingRebalance {
		return
	}
	gm.completeJoin(g)
}

func (gm *groupManager) resetSession(g *group, m *groupMember) {
	timeout := m.sessionTimeout
	if timeout <= 0 {
		timeout = defaultSessionTimeout
	}
	m.session.arm(timeout, func(gen uint64) { gm.sessionExpired(g, m, gen) })
}

// sessionExpired is a member's session timer firing. A firing whose gen is not
// current was overtaken by a heartbeat (or the member was replaced) while it
// waited for the lock (see expiry).
func (gm *groupManager) sessionExpired(g *group, m *groupMember, gen uint64) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	id := m.id
	if gm.stopped || g.members[id] != m || !m.session.current(gen) {
		return
	}
	// A member parked in JoinGroup or SyncGroup is waiting on the group, not
	// gone: it cannot heartbeat while it waits, and a rebalance may take up to
	// the rebalance timeout, which is normally longer than the session
	// timeout. Evicting it here made a joiner with a short session lose its
	// place every few seconds for as long as another member was busy.
	if _, syncing := g.pendingSync[id]; m.awaitingJoin || syncing {
		gm.resetSession(g, m)
		return
	}
	gm.emit(g.id, "expired", id, g.generation, "client "+m.client.clientID+" sent no heartbeat within its session timeout")
	gm.removeMember(g, id)
	gm.rebalanceAfterChange(g, "a member's session expired")
}

func (gm *groupManager) removeMember(g *group, id string) {
	if m := g.members[id]; m != nil {
		m.session.stop()
		m.failPendingJoin()
		if m.instanceID != nil && g.static[*m.instanceID] == id {
			delete(g.static, *m.instanceID)
		}
	}
	if ch, ok := g.pendingSync[id]; ok {
		ch <- syncResult{errorCode: kerr.UnknownMemberID.Code}
	}
	delete(g.members, id)
	delete(g.pendingSync, id)
}

// rebalanceAfterChange nudges the group after a membership change (leave/expire):
// empty → Empty; otherwise start a rebalance so survivors rejoin, or finish an
// in-flight one if the departure made everyone-left rejoined.
func (gm *groupManager) rebalanceAfterChange(g *group, why string) {
	if len(g.members) == 0 {
		g.join.stop()
		g.state = stateEmpty
		g.leader = ""
		gm.dropIfUnused(g)
		return
	}
	if g.state != statePreparingRebalance {
		g.beginRebalance()
		gm.emit(g.id, "rebalance", "", g.generation, why)
		gm.armJoinTimer(g)
	} else if g.allRejoined() && !g.holding {
		gm.completeJoin(g)
	}
}

func (g *group) selectProtocol() string {
	ids := sortedMemberIDs(g)
	ref := g.members[ids[0]]
	for _, entry := range ref.protocols {
		if g.allSupport(entry.name) {
			return entry.name
		}
	}
	if len(ref.protocols) > 0 {
		return ref.protocols[0].name
	}
	return ""
}

func (g *group) allSupport(name string) bool {
	for _, m := range g.members {
		if !m.has(name) {
			return false
		}
	}
	return true
}

func (g *group) electLeader() string {
	if g.leader != "" {
		if _, ok := g.members[g.leader]; ok {
			return g.leader
		}
	}
	return sortedMemberIDs(g)[0]
}

func (g *group) memberInfos(protocolName string) []joinMemberInfo {
	ids := sortedMemberIDs(g)
	infos := make([]joinMemberInfo, 0, len(ids))
	for _, id := range ids {
		m := g.members[id]
		infos = append(infos, joinMemberInfo{
			memberID:   id,
			instanceID: m.instanceID,
			metadata:   m.metadataFor(protocolName),
		})
	}
	return infos
}

func (m *groupMember) metadataFor(name string) []byte {
	for _, e := range m.protocols {
		if e.name == name {
			return e.metadata
		}
	}
	return nil
}

func (m *groupMember) has(name string) bool {
	for _, e := range m.protocols {
		if e.name == name {
			return true
		}
	}
	return false
}

func sortedMemberIDs(g *group) []string {
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func protocolEntries(ps []kmsg.JoinGroupRequestProtocol) []groupProtocolEntry {
	out := make([]groupProtocolEntry, 0, len(ps))
	for _, p := range ps {
		out = append(out, groupProtocolEntry{name: p.Name, metadata: p.Metadata})
	}
	return out
}

// leaving names one member to remove: by member id, by static instance id, or both.
type leaving struct {
	memberID   string
	instanceID *string
}

func membersToLeave(r *kmsg.LeaveGroupRequest) []leaving {
	if len(r.Members) > 0 {
		out := make([]leaving, 0, len(r.Members))
		for _, m := range r.Members {
			out = append(out, leaving{m.MemberID, m.InstanceID})
		}
		return out
	}
	if r.MemberID != "" {
		return []leaving{{memberID: r.MemberID}}
	}
	return nil
}

func generateMemberID(instanceID *string) string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	id := "doze-" + hex.EncodeToString(buf[:])
	if instanceID != nil && *instanceID != "" {
		return *instanceID + "-" + id
	}
	return id
}

func plural(n int, one string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + one + "s"
}
