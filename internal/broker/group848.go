package broker

import (
	"fmt"
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

	// liveness is the offsets retention, told when a group gets members and when it
	// has lost the last one; nil = nobody is counting.
	liveness livenessRecorder

	// bridge is the classic coordinator, for moving a group between the two when a
	// member of the other protocol joins (groupmigrate.go); nil when there is none.
	bridge classicBridge
	// pending holds the member ids handed out to classic clients joining a consumer
	// group with MEMBER_ID_REQUIRED, and not yet used.
	pending map[string]map[string]time.Time

	// assignInterval is the least time between two computations of a group's target
	// assignment (config.ConsumerAssignmentInterval: Kafka's
	// group.consumer.assignment.interval.ms and its default, 1 s; recorded:
	// group-coordinator-defaults).
	assignInterval time.Duration
}

func newCG848Manager(m meta.Store, logger *slog.Logger, sessionTimeout, assignInterval time.Duration, claims *groupClaims) *cg848Manager {
	if sessionTimeout <= 0 {
		sessionTimeout = cg848DefaultSessionTimeout
	}
	if claims == nil {
		claims = newGroupClaims()
	}
	return &cg848Manager{groups: make(map[string]*cg848Group), meta: m, log: logger, sessionTimeout: sessionTimeout, claims: claims,
		assignInterval: assignInterval}
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

	// targetEpoch is the group epoch the members' targets were computed for, and
	// assignedAt when. Below epoch, the group is Assigning: the targets are computed
	// by the first heartbeat or classic JoinGroup at least assignInterval after the
	// last time, never by a leave, an expiry or the passing of time (claim: groups
	// R22a, R22b). A member is at targetEpoch at most.
	targetEpoch int32
	assignedAt  time.Time
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

	// classic is set for a member that arrived through the classic JoinGroup, and is
	// kept alive and told what to do by the classic APIs (groupmigrate.go).
	classic *classicSeat

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

// notAMember and staleEpoch are Kafka's words for the two ways a heartbeat is refused
// most often (recorded: ext probes val-unknown-member-in-live-group, val-epoch-too-high).
func notAMember(member, group string) string {
	return "Member " + member + " is not a member of group " + group + "."
}

func staleEpoch(sent, known int32) string {
	than := "greater"
	if sent < known {
		than = "smaller"
	}
	return fmt.Sprintf("The consumer group member has a %s member epoch (%d) than the one known by the group coordinator (%d). The member must abandon all its partitions and rejoin.", than, sent, known)
}

// staticRefusal is the answer to a heartbeat naming a static instance that is not its
// own: nobody holds it, or another member does (recorded: cs-heartbeat-wrong-instance,
// cs-old-member-heartbeat).
func (g *cg848Group) staticRefusal(member string, instance *string) (cg848Result, bool) {
	switch g.staticCheck(member, instance) {
	case kerr.UnknownMemberID.Code:
		return cg848Error(kerr.UnknownMemberID.Code, "Instance id "+*instance+" is unknown."), true
	case kerr.FencedInstanceID.Code:
		return cg848Error(kerr.FencedInstanceID.Code, "Static member "+member+" with instance id "+*instance+
			" was fenced by member "+g.holderOf(*instance).id+"."), true
	}
	return cg848Result{}, false
}

// heartbeat processes one ConsumerGroupHeartbeat: join (epoch 0), poll (epoch > 0),
// or leave (epoch -1, or -2 for a static member that will be back).
func (m *cg848Manager) heartbeat(r *kmsg.ConsumerGroupHeartbeatRequest, who clientRef) cg848Result {
	// Kafka's checks of the request itself, in its order and with its words (recorded:
	// group-requests-refused, ch-*, ei-c-join-empty-instance).
	switch {
	case r.Group == "":
		return cg848Error(kerr.InvalidRequest.Code, "GroupId can't be empty.")
	case meta.ValidateGroupID(r.Group) != nil:
		return cg848Error(kerr.InvalidRequest.Code, "the group id is not usable")
	case r.InstanceID != nil && *r.InstanceID == "":
		return cg848Error(kerr.InvalidRequest.Code, "InstanceId can't be empty.")
	case r.MemberEpoch == 0 && r.RebalanceTimeoutMillis == -1:
		// Only "not sent": zero is a rebalance timeout (recorded: ch-rebalance-timeout-0).
		return cg848Error(kerr.InvalidRequest.Code, "RebalanceTimeoutMs must be provided in first request.")
	case r.MemberEpoch == 0 && (r.Topics == nil || len(r.Topics) != 0):
		return cg848Error(kerr.InvalidRequest.Code, "TopicPartitions must be empty when (re-)joining.")
	case r.MemberEpoch == 0 && r.SubscribedTopicNames == nil && r.SubscribedTopicRegex == nil:
		return cg848Error(kerr.InvalidRequest.Code, "Either SubscribedTopicNames or SubscribedTopicRegex must be non-null when (re-)joining.")
	case r.MemberEpoch == -2 && r.InstanceID == nil:
		return cg848Error(kerr.InvalidRequest.Code, "InstanceId can't be null.")
	case r.MemberEpoch < -2:
		return cg848Error(kerr.InvalidRequest.Code, "MemberEpoch is invalid.")
	}
	// What a member names is kept for as long as it is a member (bounds.go).
	long := len(r.MemberID) > maxMemberIDBytes || tooLong(deref(r.InstanceID), deref(r.RackID), deref(r.SubscribedTopicRegex))
	for _, name := range r.SubscribedTopicNames {
		long = long || tooLong(name)
	}
	if long {
		return cg848Error(kerr.InvalidRequest.Code, "a member id, instance id, rack id, topic name or pattern is longer than this broker keeps")
	}
	static := r.InstanceID != nil

	m.mu.Lock()
	defer m.mu.Unlock()

	// ---- leave ----
	if r.MemberEpoch < 0 {
		g := m.groups[r.Group]
		if g == nil {
			return cg848Error(kerr.GroupIDNotFound.Code, "Group "+r.Group+" not found.")
		}
		if res, refused := g.staticRefusal(r.MemberID, r.InstanceID); refused {
			return res
		}
		mem := g.members[r.MemberID]
		if mem == nil {
			return cg848Error(kerr.UnknownMemberID.Code, notAMember(r.MemberID, r.Group))
		}
		if r.MemberEpoch == -2 && static {
			// A static member going away for a restart: its place and its
			// partitions are held until its session runs out, so coming back does
			// not move anything (claim: groups R24a, R24b).
			mem.left = true
			mem.revoke.stop()
			m.resetSession(g, mem)
			return cg848Result{memberID: r.MemberID, memberEpoch: r.MemberEpoch}
		}
		m.emit(g, "left", r.MemberID, "a member left the group", "")
		m.removeMember(g, r.MemberID)
		m.bumpForRemoval(g, "a member left")
		m.maybeDowngrade(g)
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
	}
	assignor := ""
	if r.ServerAssignor != nil && *r.ServerAssignor != "" {
		if assignor = *r.ServerAssignor; assignor != assignorUniform && assignor != assignorRange {
			return cg848Error(kerr.UnsupportedAssignor.Code, "ServerAssignor "+assignor+" is not supported. Supported assignors: uniform, range.")
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
	created := false
	if g == nil {
		if r.MemberEpoch != 0 {
			// Unknown here (after a broker restart, say): the member must rejoin
			// (recorded: ch-unknown-member-epoch-5).
			return cg848Error(kerr.GroupIDNotFound.Code, "Consumer group "+r.Group+" not found.")
		}
		// The id is taken for this protocol in one step. A classic group holding it
		// with members is upgraded: they stay, as members of this one.
		adopted, refused := m.acquire(r.Group)
		if refused != nil {
			return *refused
		}
		if adopted == nil {
			// Epoch 1 before anyone has joined: the first member's join takes it to 2, which
			// is the first epoch Kafka gives a member of a new group (recorded).
			adopted = &cg848Group{id: r.Group, epoch: 1, targetEpoch: 1, assignor: assignorUniform, members: make(map[string]*cg848Member)}
			m.groups[r.Group] = adopted
			created = true
		}
		g = adopted
	} else if len(g.members) == 0 && r.MemberEpoch == 0 {
		// An emptied group kept for listing: joining it needs the claim again.
		adopted, refused := m.acquire(r.Group)
		if refused != nil {
			return *refused
		}
		if adopted != nil {
			g = adopted
		}
	}

	mem := g.members[r.MemberID]
	if r.MemberEpoch != 0 {
		if res, refused := g.staticRefusal(r.MemberID, r.InstanceID); refused {
			return res
		}
	}
	if mem != nil && mem.classic != nil {
		// A classic member heartbeating by this API is kept alive and changes nothing
		// else (recorded: cm-classic-member-sends-consumer-heartbeat).
		if r.MemberEpoch != mem.epoch {
			return cg848Error(kerr.FencedMemberEpoch.Code, staleEpoch(r.MemberEpoch, mem.epoch))
		}
		m.resetSession(g, mem)
		return cg848Result{memberID: mem.id, memberEpoch: mem.epoch}
	}
	changed := false

	if r.MemberEpoch == 0 {
		// A static instance id identifies the member across restarts.
		if static {
			for id, other := range g.members {
				if id == r.MemberID || other.instanceID == nil || *other.instanceID != *r.InstanceID {
					continue
				}
				if !other.left {
					return cg848Error(kerr.UnreleasedInstanceID.Code, "Static member "+r.MemberID+" with instance id "+
						*r.InstanceID+" cannot join the group because the instance id is owned by "+id+" member.")
				}
				// Its restarted self: take over the place it left, partitions and all
				// (claim: groups R24b).
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
			// The group is recorded as having members before it has this one
			// (offsetexpiry.go).
			if m.liveness != nil {
				if err := m.liveness.joining(g.id, false, m.sessionTimeout, "consumer"); err != nil {
					if len(g.members) == 0 {
						m.claims.release(g.id, protoConsumer)
						if created {
							delete(m.groups, g.id)
						}
					}
					return cg848Error(kerr.CoordinatorNotAvailable.Code, "the group's members could not be recorded; try again")
				}
			}
			mem = &cg848Member{id: r.MemberID, target: tpSet{}, assigned: tpSet{}}
			g.members[r.MemberID] = mem
			changed = true
			m.emit(g, "joined", r.MemberID, "client "+who.clientID, "")
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
			return cg848Error(kerr.UnknownMemberID.Code, notAMember(r.MemberID, r.Group))
		}
		switch {
		case r.MemberEpoch == mem.epoch:
		case r.MemberEpoch == mem.prevEpoch && r.MemberEpoch < mem.epoch:
			// The response that advanced its epoch was lost; tell it again.
			mem.sent = nil
		default:
			return cg848Error(kerr.FencedMemberEpoch.Code, staleEpoch(r.MemberEpoch, mem.epoch))
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
	} else {
		m.assignIfDue(g)
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
	if mem.epoch == g.targetEpoch && mem.assigned.equal(mem.target) {
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
	if mem.epoch != g.targetEpoch {
		mem.prevEpoch, mem.epoch = mem.epoch, g.targetEpoch
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
	m.emit(g, "expired", mem.id, "a member did not acknowledge a revocation within its rebalance timeout", "")
	m.removeMember(g, mem.id)
	m.bumpForRemoval(g, "a member was removed")
	m.maybeDowngrade(g)
}

// bumpAndReassign advances the group epoch for a change a member's own heartbeat or
// classic JoinGroup made, and computes the targets if it is time to (assignIfDue).
func (m *cg848Manager) bumpAndReassign(g *cg848Group, why string) {
	if len(g.members) == 0 {
		return
	}
	g.epoch++
	detail := why + "; the targets wait for a heartbeat at least the assignment interval after the last ones"
	if m.assignIfDue(g) {
		detail = why + "; " + plural(len(g.members), "member") + " reassigned"
	}
	m.emit(g, "epoch", "", detail, why)
}

// bumpForRemoval advances the group epoch for a member that left or was removed. The
// targets wait for the next heartbeat (recorded: consumer-assignment-interval,
// b-leaves-1500ms; consumer-assignment-expiry, describe-8s-x-expired).
func (m *cg848Manager) bumpForRemoval(g *cg848Group, why string) {
	if len(g.members) == 0 {
		return
	}
	g.epoch++
	m.emit(g, "epoch", "", why+"; the targets wait for the next heartbeat", why)
}

// assignIfDue computes every member's target for the group epoch when they are behind
// it and the last computation is at least assignInterval ago, and reports whether it did.
func (m *cg848Manager) assignIfDue(g *cg848Group) bool {
	if len(g.members) == 0 || g.targetEpoch >= g.epoch || time.Since(g.assignedAt) < m.assignInterval {
		return false
	}
	m.computeTargets(g)
	return true
}

// computeTargets runs the assignor for the group's epoch now.
func (m *cg848Manager) computeTargets(g *cg848Group) {
	m.assign(g)
	g.targetEpoch, g.assignedAt = g.epoch, time.Now()
	m.armClassicRevocations(g)
}

func (m *cg848Manager) resetSession(g *cg848Group, mem *cg848Member) {
	timeout := m.sessionTimeout
	if mem.classic != nil && mem.classic.session > 0 {
		timeout = mem.classic.session // the one its JoinGroup asked for.
	}
	mem.session.arm(timeout, func(gen uint64) { m.sessionExpired(g, mem, gen) })
}

// sessionExpired is a member's session timer firing. A firing overtaken by a
// heartbeat carries an old gen and does nothing (see expiry).
func (m *cg848Manager) sessionExpired(g *cg848Group, mem *cg848Member, gen uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || g.members[mem.id] != mem || !mem.session.current(gen) {
		return
	}
	m.emit(g, "expired", mem.id, "a member sent no heartbeat within its session timeout", "")
	m.removeMember(g, mem.id)
	m.bumpForRemoval(g, "a member's session expired")
	m.maybeDowngrade(g)
	m.log.Info("KIP-848 member session expired", "group", g.id, "member", mem.id)
}

func (m *cg848Manager) removeMember(g *cg848Group, id string) {
	mem := g.members[id]
	if mem != nil {
		mem.stopTimers()
	}
	delete(g.members, id)
	if mem != nil && mem.classic != nil && len(g.members) > 0 && m.liveness != nil && !g.hasClassic() {
		m.liveness.consumerOnly(g.id, m.sessionTimeout)
	}
	if len(g.members) == 0 {
		// Nobody left: either protocol may use the id now. The emptied group is
		// kept so it still lists as a consumer group for as long as this broker
		// runs; forget drops it.
		g.epoch++
		m.claims.release(g.id, protoConsumer)
		if m.liveness != nil {
			m.liveness.emptied(g.id, false, groupKind{protocolType: "consumer", epoch: g.epoch, targetEpoch: g.targetEpoch, assignor: g.assignor})
		}
	}
}

// restoreEmpty puts back an emptied consumer group found in the store at start, at the
// epochs it was at and with its assignor.
func (m *cg848Manager) restoreEmpty(id string, epoch, targetEpoch int32, assignor string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.groups[id] != nil {
		return
	}
	if assignor == "" {
		assignor = assignorUniform
	}
	m.groups[id] = &cg848Group{id: id, epoch: epoch, targetEpoch: targetEpoch, assignor: assignor, members: make(map[string]*cg848Member)}
}

// hasClassic reports whether any member arrived by the classic protocol.
func (g *cg848Group) hasClassic() bool {
	for _, mem := range g.members {
		if mem.classic != nil {
			return true
		}
	}
	return false
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
				m.emit(g, "epoch", "", "topic "+topic+" changed", "topic "+topic+" changed")
				break
			}
		}
		g.targetEpoch, g.assignedAt = g.epoch, time.Now()
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
// naming no member is allowed only while the group has none. A consumer-protocol
// member commits with OffsetCommit v9 or later, the first that carries its epoch as
// such: an older version is UNSUPPORTED_VERSION (recorded: group-requests-refused,
// cm-commit-v8-consumer-member).
func (m *cg848Manager) validateCommit(groupID, memberID string, epoch int32) int16 {
	return m.validateCommitAt(groupID, memberID, epoch, 9)
}

// validateCommitAt is validateCommit for an OffsetCommit of the version given.
func (m *cg848Manager) validateCommitAt(groupID, memberID string, epoch int32, version int16) int16 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g := m.groups[groupID]; g != nil && version < 9 {
		if mem := g.members[memberID]; mem != nil && mem.classic == nil {
			return kerr.UnsupportedVersion.Code
		}
	}
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
		if mem.classic != nil {
			return kerr.IllegalGeneration.Code // a classic member's epoch is its generation.
		}
		return kerr.StaleMemberEpoch.Code
	}
	return errNone
}

// stateName is the group's state as ConsumerGroupDescribe / ListGroups report it.
func (g *cg848Group) stateName() string {
	if len(g.members) == 0 {
		return "Empty"
	}
	if g.targetEpoch < g.epoch {
		return "Assigning"
	}
	for _, mem := range g.members {
		// A static member away for a restart is not at the group's epoch (claim: groups
		// R24a).
		if mem.left || mem.epoch != g.targetEpoch || len(mem.revoking) > 0 || !mem.assigned.equal(mem.target) {
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
func (m *cg848Manager) describe(group string) (state string, epoch, targetEpoch int32, assignor string, members []cg848MemberDesc, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[group]
	if g == nil {
		return "", 0, 0, "", nil, false
	}
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		mem := g.members[id]
		// A member holds what it has been told to give up until it says it has: a
		// classic member by its next JoinGroup (recorded: co-describe-after-1), a
		// consumer-protocol one by a heartbeat that no longer lists them (recorded:
		// consumer-assignment-interval, describe-after-a-heartbeat).
		assigned := mem.assigned.clone()
		for p := range mem.revoking {
			assigned[p] = struct{}{}
		}
		epoch := mem.epoch
		if mem.left {
			epoch = -2 // the epoch it left with, as Kafka shows it (claim: groups R24a).
		}
		members = append(members, cg848MemberDesc{
			client: mem.client, id: mem.id, instanceID: mem.instanceID, rack: mem.rack, epoch: epoch,
			subscribed: append([]string(nil), mem.subscribed...), regex: mem.regexText,
			assigned: assigned, target: mem.target.clone(),
		})
	}
	return g.stateName(), g.epoch, g.targetEpoch, g.assignor, members, true
}

// acquire takes the group id for the consumer protocol. A classic group holding it with
// members is taken over (adopted); one of another protocol type is refused as Kafka
// refuses it. adopted is nil when the id was simply free. Caller holds m.mu.
func (m *cg848Manager) acquire(id string) (adopted *cg848Group, refused *cg848Result) {
	if m.claims.claim(id, protoConsumer) {
		return nil, nil
	}
	if m.bridge != nil && m.claims.ownerOf(id) == protoClassic {
		snap, status := m.bridge.takeClassic(id)
		switch status {
		case takeOK:
			m.log.Info("classic group became a consumer group: a consumer-protocol member joined", "group", id, "members", len(snap.members))
			return m.adopt(id, snap), nil
		case takeNotConsumer:
			r := cg848Error(kerr.GroupIDNotFound.Code, "Cannot upgrade classic group "+id+" to consumer group because the group does not use the consumer embedded protocol.")
			return nil, &r
		case takeNone:
			if m.claims.claim(id, protoConsumer) {
				return nil, nil
			}
		}
	}
	r := cg848Error(kerr.GroupIDNotFound.Code, "the group id is in use by a classic group")
	return nil, &r
}
