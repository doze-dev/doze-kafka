package broker

import (
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/storage/meta"

	"github.com/twmb/franz-go/pkg/kerr"
)

// KIP-848 (the broker-driven consumer group protocol) inverts the classic model:
// the broker computes the assignment and drives reconciliation, rather than
// relaying opaque bytes for a client leader. A single ConsumerGroupHeartbeat
// carries join, poll, and leave; the member epoch replaces the generation.
//
// # Who owns a partition
//
// Ownership is the coordinator's own record, not something inferred from what
// clients report. Each member has:
//
//   - assigned: the partitions it may consume now;
//   - revoking: partitions it has been told to give up and has not yet
//     acknowledged giving up.
//
// A partition is free for another member only when it is in neither set of any
// other member. A member acknowledges a revocation by reporting, in a later
// heartbeat, a set of owned partitions that contains none of the revoked ones;
// until then (or until its rebalance timeout fences it) they stay its own.
//
// An earlier version freed a partition as soon as the smaller assignment had been
// SENT, and judged ownership by the last report, so a client that heartbeated
// again before applying the change let a second member receive partitions the
// first was still consuming.
//
// Assignments are keyed by topic id, not name, so a topic deleted and re-created
// between two heartbeats is a different topic: the old id is revoked and the new
// one assigned.

const (
	// cg848HeartbeatMs is the interval the broker asks members to heartbeat at. It
	// also bounds reconciliation latency (a handoff takes a couple of intervals), so
	// it is kept short for snappy local-dev rebalancing.
	cg848HeartbeatMs int32 = 1000

	// cg848DefaultSessionTimeout evicts a member that stops heartbeating when the
	// broker config leaves it unset.
	cg848DefaultSessionTimeout = 45 * time.Second

	assignorUniform = "uniform"
	assignorRange   = "range"
)

// tp is one partition of one topic, by topic id.
type tp struct {
	topic meta.TopicID
	part  int32
}

// tpSet is a set of partitions.
type tpSet map[tp]struct{}

func (s tpSet) has(p tp) bool { _, ok := s[p]; return ok }

func (s tpSet) clone() tpSet {
	out := make(tpSet, len(s))
	for p := range s {
		out[p] = struct{}{}
	}
	return out
}

func (s tpSet) equal(o tpSet) bool {
	if len(s) != len(o) {
		return false
	}
	for p := range s {
		if !o.has(p) {
			return false
		}
	}
	return true
}

func (s tpSet) intersects(o tpSet) bool {
	for p := range s {
		if o.has(p) {
			return true
		}
	}
	return false
}

// byTopic groups the set by topic id, partitions ascending, topics in id order.
func (s tpSet) byTopic() []topicPartitions {
	m := map[meta.TopicID][]int32{}
	for p := range s {
		m[p.topic] = append(m[p.topic], p.part)
	}
	out := make([]topicPartitions, 0, len(m))
	for id, parts := range m {
		slices.Sort(parts)
		out = append(out, topicPartitions{id, parts})
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].topic[:]) < string(out[j].topic[:]) })
	return out
}

type topicPartitions struct {
	topic meta.TopicID
	parts []int32
}

// cg848Manager holds ephemeral KIP-848 group state. Like the classic manager,
// membership/epoch/assignments live only in memory and are rebuilt when members
// rejoin (only committed offsets are durable). It resolves topic names/IDs and
// partition counts through the metadata store.
type cg848Manager struct {
	mu             sync.Mutex
	groups         map[string]*cg848Group
	meta           meta.Store
	log            *slog.Logger
	sessionTimeout time.Duration
	claims         *groupClaims
	stopped        bool           // the broker is shutting down: timers do nothing.
	sink           GroupEventSink // told what happens in groups; nil = nobody is watching.
}

func newCG848Manager(m meta.Store, logger *slog.Logger, sessionTimeout time.Duration, claims *groupClaims) *cg848Manager {
	if sessionTimeout <= 0 {
		sessionTimeout = cg848DefaultSessionTimeout
	}
	if claims == nil {
		claims = newGroupClaims()
	}
	return &cg848Manager{groups: make(map[string]*cg848Group), meta: m, log: logger, sessionTimeout: sessionTimeout, claims: claims}
}

// stop cancels every timer. After the broker has shut down a session timer would
// otherwise fire into a closed metadata store.
func (m *cg848Manager) stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
	for _, g := range m.groups {
		for _, mem := range g.members {
			mem.stopTimers()
		}
	}
}

type cg848Group struct {
	id       string
	epoch    int32  // bumped whenever membership, subscriptions or topics change.
	assignor string // the assignor in use (the members' most common preference).
	members  map[string]*cg848Member
}

type cg848Member struct {
	client     clientRef
	id         string
	instanceID *string
	rack       *string
	assignor   string // the server assignor this member asked for ("" = no preference).

	subscribed []string       // topic names it subscribed to.
	regex      *regexp.Regexp // and/or a pattern (v1).
	regexText  string

	epoch     int32 // the group epoch this member has caught up to.
	prevEpoch int32 // its epoch before that: a heartbeat still carrying it is a retry, not a fenced member.
	target    tpSet // what the assignor wants it to own.
	assigned  tpSet // what it may consume now.
	revoking  tpSet // told to give up, not yet acknowledged.
	sent      tpSet // the assignment last sent to it (nil = nothing sent yet).

	rebalanceTimeout time.Duration
	left             bool // a static member that left (epoch -2): its place is held until its session expires.

	session expiry // evicts the member if it stops heartbeating.
	revoke  expiry // fences it if it does not give up revoked partitions in time.
}

func (mem *cg848Member) stopTimers() {
	mem.session.stop()
	mem.revoke.stop()
}

// subscribes reports whether the member wants topic.
func (mem *cg848Member) subscribes(topic string) bool {
	return slices.Contains(mem.subscribed, topic) || (mem.regex != nil && mem.regex.MatchString(topic))
}

// cg848Result is what heartbeat returns for the handler to encode.
type cg848Result struct {
	errorCode   int16
	errorMsg    string
	memberID    string
	memberEpoch int32
	assignment  *kmsg.ConsumerGroupHeartbeatResponseAssignment // nil => keepalive (no change).
}

func cg848Error(code int16, msg string) cg848Result {
	return cg848Result{errorCode: int16(code), errorMsg: msg}
}

// heartbeat processes one ConsumerGroupHeartbeat: join (epoch 0), poll (epoch > 0),
// or leave (epoch -1, or -2 for a static member that will be back).
func (m *cg848Manager) heartbeat(r *kmsg.ConsumerGroupHeartbeatRequest, who clientRef) cg848Result {
	if r.Group == "" || meta.ValidateGroupID(r.Group) != nil {
		return cg848Error(kerr.InvalidRequest.Code, "the group id is empty or not usable")
	}
	if r.MemberEpoch < -2 {
		return cg848Error(kerr.InvalidRequest.Code, "the member epoch is not a valid value")
	}
	static := r.InstanceID != nil && *r.InstanceID != ""

	m.mu.Lock()
	defer m.mu.Unlock()

	// ---- leave ----
	if r.MemberEpoch < 0 {
		g := m.groups[r.Group]
		if g == nil {
			return cg848Error(kerr.GroupIDNotFound.Code, "no such group")
		}
		mem := g.members[r.MemberID]
		if mem == nil {
			return cg848Error(kerr.UnknownMemberID.Code, "not a member of the group")
		}
		if r.MemberEpoch == -2 && static {
			// A static member going away for a restart: its place and its
			// partitions are held until its session runs out, so coming back does
			// not move anything.
			mem.left = true
			mem.revoke.stop()
			m.resetSession(g, mem)
			return cg848Result{memberID: r.MemberID, memberEpoch: r.MemberEpoch}
		}
		m.emit(g.id, "left", r.MemberID, g.epoch, "a member left the group")
		m.removeMember(g, r.MemberID)
		m.bumpAndReassign(g, "a member left")
		return cg848Result{memberID: r.MemberID, memberEpoch: r.MemberEpoch}
	}

	// ---- join ----
	if r.MemberEpoch == 0 {
		if r.MemberID == "" {
			// v0 clients send no id and expect one; from v1 the client makes its own.
			if r.GetVersion() >= 1 {
				return cg848Error(kerr.InvalidRequest.Code, "a member id is required")
			}
			r.MemberID = generateMemberID(r.InstanceID)
		}
		if r.RebalanceTimeoutMillis <= 0 {
			return cg848Error(kerr.InvalidRequest.Code, "the rebalance timeout must be set when joining")
		}
		if r.SubscribedTopicNames == nil && r.SubscribedTopicRegex == nil {
			return cg848Error(kerr.InvalidRequest.Code, "a subscription must be set when joining")
		}
	}
	assignor := ""
	if r.ServerAssignor != nil && *r.ServerAssignor != "" {
		if assignor = *r.ServerAssignor; assignor != assignorUniform && assignor != assignorRange {
			return cg848Error(kerr.UnsupportedAssignor.Code, "the assignors offered are uniform and range")
		}
	}
	var regex *regexp.Regexp
	if r.SubscribedTopicRegex != nil && *r.SubscribedTopicRegex != "" {
		re, err := regexp.Compile(*r.SubscribedTopicRegex)
		if err != nil {
			return cg848Error(kerr.InvalidRegularExpression.Code, err.Error())
		}
		regex = re
	}

	g := m.groups[r.Group]
	if g == nil {
		if r.MemberEpoch != 0 {
			// Unknown here (after a broker restart, say): the member must rejoin.
			return cg848Error(kerr.UnknownMemberID.Code, "no such member; rejoin")
		}
		// The id is taken for this protocol in one step; a classic group holding
		// it (or one being formed at this instant) wins and this join is refused.
		if !m.claims.claim(r.Group, protoConsumer) {
			return cg848Error(kerr.GroupIDNotFound.Code, "the group id is in use by a classic group")
		}
		g = &cg848Group{id: r.Group, assignor: assignorUniform, members: make(map[string]*cg848Member)}
		m.groups[r.Group] = g
	} else if len(g.members) == 0 && r.MemberEpoch == 0 {
		// An emptied group kept for listing: joining it needs the claim again.
		if !m.claims.claim(r.Group, protoConsumer) {
			return cg848Error(kerr.GroupIDNotFound.Code, "the group id is in use by a classic group")
		}
	}

	mem := g.members[r.MemberID]
	changed := false

	if r.MemberEpoch == 0 {
		// A static instance id identifies the member across restarts.
		if static {
			for id, other := range g.members {
				if id == r.MemberID || other.instanceID == nil || *other.instanceID != *r.InstanceID {
					continue
				}
				if !other.left {
					return cg848Error(kerr.UnreleasedInstanceID.Code, "another member holds this instance id")
				}
				// Its restarted self: take over the place it left, partitions and all.
				other.stopTimers()
				delete(g.members, id)
				mem = &cg848Member{id: r.MemberID, target: other.target, assigned: other.assigned,
					revoking: other.revoking, epoch: other.epoch, prevEpoch: other.prevEpoch,
					subscribed: other.subscribed, regex: other.regex, regexText: other.regexText, assignor: other.assignor}
				g.members[r.MemberID] = mem
			}
		}
		switch {
		case mem == nil:
			mem = &cg848Member{id: r.MemberID, target: tpSet{}, assigned: tpSet{}}
			g.members[r.MemberID] = mem
			changed = true
			m.emit(g.id, "joined", r.MemberID, g.epoch, "client "+who.clientID)
		case mem.sent != nil && !mem.left:
			// A known member rejoining at epoch 0 has dropped everything it held.
			mem.assigned, mem.revoking = tpSet{}, nil
			mem.revoke.stop()
			changed = true
		}
		mem.left = false
		mem.sent = nil // a join is always answered with the full assignment.
		mem.rebalanceTimeout = time.Duration(r.RebalanceTimeoutMillis) * time.Millisecond
	} else {
		if mem == nil || mem.left {
			return cg848Error(kerr.UnknownMemberID.Code, "no such member; rejoin")
		}
		switch {
		case r.MemberEpoch == mem.epoch:
		case r.MemberEpoch == mem.prevEpoch && r.MemberEpoch < mem.epoch:
			// The response that advanced its epoch was lost; tell it again.
			mem.sent = nil
		default:
			return cg848Error(kerr.FencedMemberEpoch.Code, "the member epoch is stale; rejoin")
		}
	}
	mem.client = who
	mem.instanceID, mem.rack = r.InstanceID, r.RackID

	// Subscription and assignor are sent when they change; nil means unchanged.
	if r.SubscribedTopicNames != nil && !slices.Equal(mem.subscribed, r.SubscribedTopicNames) {
		mem.subscribed = append([]string(nil), r.SubscribedTopicNames...)
		changed = true
	}
	if r.SubscribedTopicRegex != nil && *r.SubscribedTopicRegex != mem.regexText {
		mem.regex, mem.regexText = regex, *r.SubscribedTopicRegex
		changed = true
	}
	if r.ServerAssignor != nil && assignor != mem.assignor {
		mem.assignor = assignor
		changed = true
	}
	m.resetSession(g, mem)
	if changed {
		m.bumpAndReassign(g, "a member joined or changed its subscription")
	}

	var reported tpSet // nil = the member did not report what it owns this time.
	if r.Topics != nil {
		reported = tpSet{}
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				reported[tp{meta.TopicID(t.TopicID), p}] = struct{}{}
			}
		}
	}
	m.reconcile(g, mem, reported)

	res := cg848Result{memberID: mem.id, memberEpoch: mem.epoch}
	if mem.sent == nil || !mem.sent.equal(mem.assigned) {
		mem.sent = mem.assigned.clone()
		res.assignment = encodeAssignment(mem.assigned)
	}
	return res
}

// reconcile moves one member a step towards its target, never handing it a
// partition another member still holds.
func (m *cg848Manager) reconcile(g *cg848Group, mem *cg848Member, reported tpSet) {
	// 1. Waiting for it to give partitions up. Only its own report that it no
	//    longer owns any of them counts as having done so.
	if len(mem.revoking) > 0 {
		if reported == nil || reported.intersects(mem.revoking) {
			return
		}
		mem.revoking = nil
		mem.revoke.stop()
	}
	if mem.epoch == g.epoch && mem.assigned.equal(mem.target) {
		return
	}
	// 2. Anything it holds that is no longer its target must be revoked first.
	//    Its epoch does not advance until that is acknowledged.
	keep, revoke := tpSet{}, tpSet{}
	for p := range mem.assigned {
		if mem.target.has(p) {
			keep[p] = struct{}{}
		} else {
			revoke[p] = struct{}{}
		}
	}
	if len(revoke) > 0 {
		mem.assigned, mem.revoking = keep, revoke
		m.armRevokeTimer(g, mem)
		return
	}
	// 3. Nothing to give up: take every target partition nobody else holds, and
	//    move to the group's epoch. Partitions still held elsewhere arrive on a
	//    later heartbeat, once their holder has let go.
	for p := range mem.target {
		if !keep.has(p) && m.free(g, mem.id, p) {
			keep[p] = struct{}{}
		}
	}
	mem.assigned = keep
	if mem.epoch != g.epoch {
		mem.prevEpoch, mem.epoch = mem.epoch, g.epoch
	}
}

// free reports whether no member other than self holds p, as assigned or as a
// revocation not yet acknowledged.
func (m *cg848Manager) free(g *cg848Group, self string, p tp) bool {
	for id, other := range g.members {
		if id != self && (other.assigned.has(p) || other.revoking.has(p)) {
			return false
		}
	}
	return true
}

// armRevokeTimer starts the member's rebalance timeout: how long it has to
// acknowledge a revocation before it is removed and its partitions freed.
func (m *cg848Manager) armRevokeTimer(g *cg848Group, mem *cg848Member) {
	timeout := mem.rebalanceTimeout
	if timeout <= 0 {
		timeout = m.sessionTimeout
	}
	mem.revoke.arm(timeout, func(gen uint64) { m.revokeExpired(g, mem, gen) })
}

// revokeExpired fences a member that did not acknowledge a revocation in time.
// Without it, one member that never reports back keeps its partitions from their
// new owner for ever.
func (m *cg848Manager) revokeExpired(g *cg848Group, mem *cg848Member, gen uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || g.members[mem.id] != mem || !mem.revoke.current(gen) || len(mem.revoking) == 0 {
		return
	}
	m.log.Info("KIP-848 member did not give up revoked partitions within its rebalance timeout; removing it",
		"group", g.id, "member", mem.id)
	m.emit(g.id, "expired", mem.id, g.epoch, "a member did not acknowledge a revocation within its rebalance timeout")
	m.removeMember(g, mem.id)
	m.bumpAndReassign(g, "a member was removed")
}

// bumpAndReassign advances the group epoch and recomputes every member's target.
func (m *cg848Manager) bumpAndReassign(g *cg848Group, why string) {
	if len(g.members) == 0 {
		return
	}
	g.epoch++
	m.assign(g)
	m.emit(g.id, "epoch", "", g.epoch, why+"; "+plural(len(g.members), "member")+" reassigned")
}

func (m *cg848Manager) resetSession(g *cg848Group, mem *cg848Member) {
	mem.session.arm(m.sessionTimeout, func(gen uint64) { m.sessionExpired(g, mem, gen) })
}

// sessionExpired is a member's session timer firing. A firing overtaken by a
// heartbeat carries an old gen and does nothing (see expiry).
func (m *cg848Manager) sessionExpired(g *cg848Group, mem *cg848Member, gen uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || g.members[mem.id] != mem || !mem.session.current(gen) {
		return
	}
	m.emit(g.id, "expired", mem.id, g.epoch, "a member sent no heartbeat within its session timeout")
	m.removeMember(g, mem.id)
	m.bumpAndReassign(g, "a member's session expired")
	m.log.Info("KIP-848 member session expired", "group", g.id, "member", mem.id)
}

func (m *cg848Manager) removeMember(g *cg848Group, id string) {
	if mem := g.members[id]; mem != nil {
		mem.stopTimers()
	}
	delete(g.members, id)
	if len(g.members) == 0 {
		// Nobody left: either protocol may use the id now. The emptied group is
		// kept so it still lists as a consumer group for as long as this broker
		// runs; forget drops it.
		g.epoch++
		m.claims.release(g.id, protoConsumer)
	}
}

// forget drops an emptied group, reporting whether there was one.
func (m *cg848Manager) forget(group string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[group]
	if g == nil || len(g.members) > 0 {
		return false
	}
	delete(m.groups, group)
	return true
}

// topicChanged re-runs the assignor for every group that wants topic, after the
// topic was created, deleted or grown. Assignments are otherwise computed only
// when membership or subscriptions change, so a consumer that subscribed before
// its topic existed (start the app, then create the topic — the common order in
// local development) would otherwise idle for ever. The epoch is bumped when a
// target changed: that is how members learn there is something new to reconcile.
func (m *cg848Manager) topicChanged(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range m.groups {
		wanted := false
		for _, mem := range g.members {
			wanted = wanted || mem.subscribes(topic)
		}
		if !wanted {
			continue
		}
		before := make(map[string]tpSet, len(g.members))
		for id, mem := range g.members {
			before[id] = mem.target
		}
		m.assign(g)
		for id, mem := range g.members {
			if !before[id].equal(mem.target) {
				g.epoch++
				m.emit(g.id, "epoch", "", g.epoch, "topic "+topic+" changed")
				break
			}
		}
	}
}

// hasMembers reports whether the group id has live members here.
func (m *cg848Manager) hasMembers(group string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[group]
	return g != nil && len(g.members) > 0
}

// validateCommit decides whether an OffsetCommit for a KIP-848 group may be
// stored: the member must be in the group and at its own current epoch. A commit
// naming no member is allowed only while the group has none.
func (m *cg848Manager) validateCommit(groupID, memberID string, epoch int32) int16 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int16(m.memberCheck(groupID, memberID, epoch))
}

func (m *cg848Manager) memberCheck(groupID, memberID string, epoch int32) int16 {
	g := m.groups[groupID]
	if g == nil || len(g.members) == 0 {
		if epoch < 0 && memberID == "" {
			return errNone
		}
		return kerr.UnknownMemberID.Code
	}
	mem := g.members[memberID]
	if mem == nil {
		return kerr.UnknownMemberID.Code
	}
	if epoch != mem.epoch {
		return kerr.StaleMemberEpoch.Code
	}
	return errNone
}

// stateName is the group's state as ConsumerGroupDescribe / ListGroups report it.
func (g *cg848Group) stateName() string {
	if len(g.members) == 0 {
		return "Empty"
	}
	for _, mem := range g.members {
		if mem.epoch != g.epoch || len(mem.revoking) > 0 || !mem.assigned.equal(mem.target) {
			return "Reconciling"
		}
	}
	return "Stable"
}

// cg848MemberDesc is one member as ConsumerGroupDescribe shows it.
type cg848MemberDesc struct {
	client     clientRef
	id         string
	instanceID *string
	rack       *string
	epoch      int32
	subscribed []string
	regex      string
	assigned   tpSet
	target     tpSet
}

// describe returns the KIP-848 view of a group, or ok=false if it is unknown.
func (m *cg848Manager) describe(group string) (state string, epoch int32, assignor string, members []cg848MemberDesc, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[group]
	if g == nil {
		return "", 0, "", nil, false
	}
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		mem := g.members[id]
		members = append(members, cg848MemberDesc{
			client: mem.client, id: mem.id, instanceID: mem.instanceID, rack: mem.rack, epoch: mem.epoch,
			subscribed: append([]string(nil), mem.subscribed...), regex: mem.regexText,
			assigned: mem.assigned.clone(), target: mem.target.clone(),
		})
	}
	return g.stateName(), g.epoch, g.assignor, members, true
}
