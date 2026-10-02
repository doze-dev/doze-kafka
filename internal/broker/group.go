package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// defaultSessionTimeout is used when a client requests a non-positive one.
const defaultSessionTimeout = 30 * time.Second

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
}

func newGroupManager(hasOffsets func(string) bool) *groupManager {
	return &groupManager{groups: make(map[string]*group), hasOffsets: hasOffsets}
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
	id          string
	state       groupState
	generation  int32
	leader      string
	protocol    string // selected assignor name.
	members     map[string]*groupMember
	pendingSync map[string]chan syncResult
	joinTimer   *time.Timer
	joinGen     uint64 // bumped whenever the join timer is stopped or re-armed; see armJoinTimer.
}

type groupMember struct {
	id             string
	instanceID     *string
	protocols      []groupProtocolEntry // ordered by client preference.
	assignment     []byte               // from the leader's SyncGroup.
	rejoined       bool
	sessionTimeout time.Duration
	sessionTimer   *time.Timer
	sessionGen     uint64 // bumped on every reset; a timer callback from an older one is stale.
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
	memberID  string
	errorCode int16
}

// join runs the classic JoinGroup relay. It blocks until the rebalance completes
// (all known members rejoined, or the collect timeout), returning this member's
// slice of the result. A first-contact member (empty id) gets MEMBER_ID_REQUIRED
// with a fresh id and must retry (KIP-394) — but only from JoinGroup v4, the
// first version that knows the code. An older client would not know what to do
// with it, so it is given an id and joined in one step, as Kafka does.
func (gm *groupManager) join(ctx context.Context, r *kmsg.JoinGroupRequest) (joinResult, error) {
	gm.mu.Lock()

	if r.MemberID == "" {
		id := generateMemberID(r.InstanceID)
		if r.GetVersion() >= 4 {
			gm.mu.Unlock()
			return joinResult{errorCode: int16(protocol.ErrMemberIDRequired), memberID: id}, nil
		}
		r.MemberID = id
	}
	g := gm.getOrCreate(r.Group)

	m := g.members[r.MemberID]
	if m == nil {
		m = &groupMember{id: r.MemberID}
		g.members[r.MemberID] = m
	}
	// A retried JoinGroup replaces one still parked: the old request's client is
	// waiting on an answer that this one supersedes, so release it (Kafka answers
	// it UNKNOWN_MEMBER_ID) instead of leaving its goroutine blocked.
	m.failPendingJoin()
	m.instanceID = r.InstanceID
	m.protocols = protocolEntries(r.Protocols)
	m.sessionTimeout = time.Duration(r.SessionTimeoutMillis) * time.Millisecond
	m.rebalanceTimeout = 0
	if r.RebalanceTimeoutMillis > 0 {
		m.rebalanceTimeout = time.Duration(r.RebalanceTimeoutMillis) * time.Millisecond
	}

	if g.state != statePreparingRebalance {
		g.beginRebalance()
	}
	m.rejoined = true
	m.awaitingJoin = true
	m.joinResult = make(chan joinResult, 1)
	gm.resetSession(g, m)

	if g.allRejoined() {
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

// sync distributes assignments. The leader uploads everyone's assignment; each
// member (leader included) receives its own slice. Followers block until the
// leader's SyncGroup arrives.
func (gm *groupManager) sync(ctx context.Context, r *kmsg.SyncGroupRequest) (syncResult, error) {
	gm.mu.Lock()
	g := gm.groups[r.Group]
	if g == nil {
		gm.mu.Unlock()
		return syncResult{errorCode: int16(protocol.ErrUnknownMemberID)}, nil
	}
	m := g.members[r.MemberID]
	if m == nil {
		gm.mu.Unlock()
		return syncResult{errorCode: int16(protocol.ErrUnknownMemberID)}, nil
	}
	if r.Generation != g.generation {
		gm.mu.Unlock()
		return syncResult{errorCode: int16(protocol.ErrIllegalGeneration)}, nil
	}
	if g.state == statePreparingRebalance {
		gm.mu.Unlock()
		return syncResult{errorCode: int16(protocol.ErrRebalanceInProgress)}, nil
	}
	gm.resetSession(g, m)

	if r.MemberID == g.leader {
		for _, a := range r.GroupAssignment {
			if target := g.members[a.MemberID]; target != nil {
				target.assignment = a.MemberAssignment
			}
		}
		g.state = stateStable
		for id, ch := range g.pendingSync {
			ch <- syncResult{assignment: g.members[id].assignment}
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
		return int16(protocol.ErrUnknownMemberID)
	}
	m := g.members[r.MemberID]
	if m == nil {
		return int16(protocol.ErrUnknownMemberID)
	}
	if r.Generation != g.generation {
		return int16(protocol.ErrIllegalGeneration)
	}
	gm.resetSession(g, m)
	if g.state == statePreparingRebalance {
		return int16(protocol.ErrRebalanceInProgress)
	}
	return int16(protocol.ErrNone)
}

// leave removes the named members and triggers a rebalance for the rest.
func (gm *groupManager) leave(r *kmsg.LeaveGroupRequest) []leaveResult {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[r.Group]

	var results []leaveResult
	for _, id := range membersToLeave(r) {
		if g != nil && g.members[id] != nil {
			gm.removeMember(g, id)
			results = append(results, leaveResult{memberID: id, errorCode: int16(protocol.ErrNone)})
		} else {
			results = append(results, leaveResult{memberID: id, errorCode: int16(protocol.ErrUnknownMemberID)})
		}
	}
	if g != nil {
		gm.rebalanceAfterChange(g)
	}
	return results
}

// validateCommit fences a stale OffsetCommit. An unknown/empty group (a simple
// consumer) is allowed; a known group requires a live member at the current
// generation.
func (gm *groupManager) validateCommit(groupID, memberID string, generation int32) int16 {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[groupID]
	if g == nil || len(g.members) == 0 || generation < 0 {
		return int16(protocol.ErrNone)
	}
	if g.members[memberID] == nil {
		return int16(protocol.ErrUnknownMemberID)
	}
	if generation != g.generation {
		return int16(protocol.ErrIllegalGeneration)
	}
	return int16(protocol.ErrNone)
}

// --- internal state transitions (all called under gm.mu) ---

func (gm *groupManager) getOrCreate(id string) *group {
	g := gm.groups[id]
	if g == nil {
		g = &group{id: id, state: stateEmpty, members: make(map[string]*groupMember)}
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
	g.failPendingSyncs(protocol.ErrRebalanceInProgress)
}

// failPendingSyncs answers every parked SyncGroup with code and forgets them.
func (g *group) failPendingSyncs(code protocol.ErrorCode) {
	for id, ch := range g.pendingSync {
		ch <- syncResult{errorCode: int16(code)} // buffered, never blocks.
		delete(g.pendingSync, id)
	}
}

// failPendingJoin answers a JoinGroup still parked for this member, if any.
func (m *groupMember) failPendingJoin() {
	if m.awaitingJoin && m.joinResult != nil {
		m.joinResult <- joinResult{errorCode: int16(protocol.ErrUnknownMemberID), memberID: m.id, generation: -1}
	}
	m.awaitingJoin = false
}

// stopSession cancels the member's session timer and invalidates any callback
// already past Stop() and waiting on the manager lock — time.Timer.Stop cannot
// recall one that has fired, and without the generation it would evict a member
// that heartbeated an instant earlier.
func (m *groupMember) stopSession() {
	if m.sessionTimer != nil {
		m.sessionTimer.Stop()
		m.sessionTimer = nil
	}
	m.sessionGen++
}

// stopJoinTimer cancels the rebalance collect timer, invalidating a callback
// that has already fired. It must run whenever a rebalance ends by any route;
// a timer left behind makes armJoinTimer think one is running, so no later
// rebalance of the group would ever time out.
func (g *group) stopJoinTimer() {
	if g.joinTimer != nil {
		g.joinTimer.Stop()
		g.joinTimer = nil
	}
	g.joinGen++
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
func (g *group) completeJoin() {
	g.stopJoinTimer()
	for id, m := range g.members {
		if !m.rejoined {
			m.stopSession()
			delete(g.members, id)
		}
	}
	if len(g.members) == 0 {
		g.state = stateEmpty
		g.leader = ""
		return
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
}

// completeJoin closes the rebalance and forgets the group if nobody was left.
func (gm *groupManager) completeJoin(g *group) {
	g.completeJoin()
	if g.state == stateEmpty {
		gm.dropIfUnused(g)
	}
}

// dropIfUnused forgets an Empty group that has no committed offsets. A group
// that still has offsets stays, because those are durable and a consumer
// resuming under the same id expects to find them.
func (gm *groupManager) dropIfUnused(g *group) {
	if gm.groups[g.id] != g || len(g.members) != 0 {
		return
	}
	if gm.hasOffsets != nil && gm.hasOffsets(g.id) {
		return
	}
	delete(gm.groups, g.id)
}

func (gm *groupManager) armJoinTimer(g *group) {
	if g.joinTimer != nil {
		return
	}
	g.joinGen++
	gen := g.joinGen
	g.joinTimer = time.AfterFunc(g.rebalanceTimeout(), func() {
		gm.mu.Lock()
		defer gm.mu.Unlock()
		// Stale if the rebalance already ended (or restarted) while this waited
		// for the lock.
		if g.joinGen != gen || g.state != statePreparingRebalance {
			return
		}
		gm.completeJoin(g)
	})
}

func (gm *groupManager) resetSession(g *group, m *groupMember) {
	m.stopSession()
	timeout := m.sessionTimeout
	if timeout <= 0 {
		timeout = defaultSessionTimeout
	}
	id, gen := m.id, m.sessionGen
	m.sessionTimer = time.AfterFunc(timeout, func() {
		gm.mu.Lock()
		defer gm.mu.Unlock()
		// Stale if the member was replaced, or heartbeated, while this waited
		// for the lock.
		if g.members[id] != m || m.sessionGen != gen {
			return
		}
		gm.removeMember(g, id)
		gm.rebalanceAfterChange(g)
	})
}

func (gm *groupManager) removeMember(g *group, id string) {
	if m := g.members[id]; m != nil {
		m.stopSession()
		m.failPendingJoin()
	}
	if ch, ok := g.pendingSync[id]; ok {
		ch <- syncResult{errorCode: int16(protocol.ErrUnknownMemberID)}
	}
	delete(g.members, id)
	delete(g.pendingSync, id)
}

// rebalanceAfterChange nudges the group after a membership change (leave/expire):
// empty → Empty; otherwise start a rebalance so survivors rejoin, or finish an
// in-flight one if the departure made everyone-left rejoined.
func (gm *groupManager) rebalanceAfterChange(g *group) {
	if len(g.members) == 0 {
		g.stopJoinTimer()
		g.state = stateEmpty
		g.leader = ""
		gm.dropIfUnused(g)
		return
	}
	if g.state != statePreparingRebalance {
		g.beginRebalance()
	} else if g.allRejoined() {
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

func membersToLeave(r *kmsg.LeaveGroupRequest) []string {
	if len(r.Members) > 0 {
		ids := make([]string, 0, len(r.Members))
		for _, m := range r.Members {
			ids = append(ids, m.MemberID)
		}
		return ids
	}
	if r.MemberID != "" {
		return []string{r.MemberID}
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
