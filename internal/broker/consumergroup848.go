package broker

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// KIP-848 (the broker-driven consumer group protocol) inverts the classic model:
// the broker computes the assignment and drives reconciliation, rather than
// relaying opaque bytes for a client leader. A single ConsumerGroupHeartbeat
// carries join, poll, and leave; the member epoch replaces the generation.
//
// Reconciliation is incremental and safe. The broker never hands partition P to
// member B while member A still owns it: each member reports its currently-owned
// partitions in the heartbeat, and the broker only delivers a target partition
// once no other member reports owning it. A member that must give up a partition
// receives an assignment excluding it, releases it, and reports the smaller set on
// its next heartbeat — at which point the partition becomes free for its new owner.
// See docs/consumer-groups.md (KIP-848 section).

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
}

func newCG848Manager(m meta.Store, logger *slog.Logger, sessionTimeout time.Duration) *cg848Manager {
	if sessionTimeout <= 0 {
		sessionTimeout = cg848DefaultSessionTimeout
	}
	return &cg848Manager{groups: make(map[string]*cg848Group), meta: m, log: logger, sessionTimeout: sessionTimeout}
}

type cg848Group struct {
	id       string
	epoch    int32
	assignor string
	members  map[string]*cg848Member
}

type cg848Member struct {
	client       clientRef
	id           string
	epoch        int32
	instanceID   *string
	rack         *string
	subscribed   []string           // subscribed topic names.
	owned        map[string][]int32 // topic -> partitions the member reports owning.
	target       map[string][]int32 // topic -> partitions the assignor wants it to own.
	lastSent     map[string][]int32 // last assignment we returned (to decide keepalive vs send).
	sessionTimer *time.Timer
	sessionGen   uint64 // bumped on every reset; a timer callback from an older one is stale.
}

// cg848Result is what heartbeat returns for the handler to encode.
type cg848Result struct {
	errorCode   int16
	memberID    string
	memberEpoch int32
	assignment  *kmsg.ConsumerGroupHeartbeatResponseAssignment // nil => keepalive (no change).
}

// heartbeat processes one ConsumerGroupHeartbeat: join (epoch 0), poll (epoch > 0),
// or leave (epoch < 0), returning this member's slice of the reconciliation.
func (m *cg848Manager) heartbeat(r *kmsg.ConsumerGroupHeartbeatRequest, who clientRef) cg848Result {
	m.mu.Lock()
	defer m.mu.Unlock()

	// A joining member is expected to name itself; one that does not would share
	// the id "" with every other such member and be handed the same partitions
	// as them. Mint one (the v0 preview clients send none and expect the broker to).
	if r.MemberID == "" && r.MemberEpoch == 0 {
		r.MemberID = generateMemberID(r.InstanceID)
	}

	// Leave: remove the member and rebalance the rest. The negative epoch is echoed;
	// the client ignores negative-epoch responses (it never re-stores them).
	if r.MemberEpoch < 0 {
		if g := m.groups[r.Group]; g != nil && g.members[r.MemberID] != nil {
			m.removeMember(g, r.MemberID)
			m.bumpAndReassign(g)
		}
		return cg848Result{memberID: r.MemberID, memberEpoch: r.MemberEpoch}
	}

	g := m.groups[r.Group]
	if g == nil {
		if r.MemberEpoch != 0 {
			// Unknown group at a non-zero epoch (e.g. after a broker restart): fence
			// so the member abandons its partitions and rejoins at epoch 0.
			return cg848Result{errorCode: int16(protocol.ErrFencedMemberEpoch)}
		}
		g = &cg848Group{id: r.Group, members: make(map[string]*cg848Member)}
		m.groups[r.Group] = g
	}

	if a := assignorName(r.ServerAssignor); a != "" {
		g.assignor = a
	}

	mem := g.members[r.MemberID]
	if mem != nil {
		mem.client = who
	}
	if r.MemberEpoch == 0 {
		// (Re)join fresh: reset owned state and rebalance.
		if mem == nil {
			mem = &cg848Member{id: r.MemberID}
			g.members[r.MemberID] = mem
		}
		mem.client = who
		mem.owned = map[string][]int32{}
		mem.lastSent = nil
		mem.subscribed = append([]string(nil), r.SubscribedTopicNames...)
		mem.instanceID, mem.rack = r.InstanceID, r.RackID
		m.resetSession(g, mem)
		m.bumpAndReassign(g)
	} else {
		if mem == nil {
			return cg848Result{errorCode: int16(protocol.ErrFencedMemberEpoch)}
		}
		if r.MemberEpoch != mem.epoch {
			// A stale epoch (delayed/duplicate heartbeat): tell the member to rejoin.
			return cg848Result{errorCode: int16(protocol.ErrFencedMemberEpoch)}
		}
		// Owned/subscription fields are sent only when they change; nil means
		// "unchanged", so we must not clobber the remembered value.
		if r.Topics != nil {
			mem.owned = m.ownedFromReq(r.Topics)
		}
		if r.SubscribedTopicNames != nil && !slices.Equal(mem.subscribed, r.SubscribedTopicNames) {
			mem.subscribed = append([]string(nil), r.SubscribedTopicNames...)
			m.bumpAndReassign(g)
		}
		mem.instanceID, mem.rack = r.InstanceID, r.RackID
		m.resetSession(g, mem)
	}

	deliver := m.deliverable(g, mem)
	mem.epoch = g.epoch

	// Keepalive: nothing to deliver that changed since last time. The initial join
	// (lastSent == nil) always sends, so the member learns its epoch and assignment.
	if mem.lastSent != nil && assignmentsEqual(deliver, mem.lastSent) {
		return cg848Result{memberID: mem.id, memberEpoch: mem.epoch}
	}
	mem.lastSent = deliver
	return cg848Result{
		memberID:    mem.id,
		memberEpoch: mem.epoch,
		assignment:  m.encodeAssignment(deliver),
	}
}

// deliverable is the assignment to hand this member now: every target partition it
// already owns, plus every target partition no other member still owns (free to
// take). Target partitions held by another member are withheld until that member
// releases them — the safe, incremental handoff.
func (m *cg848Manager) deliverable(g *cg848Group, mem *cg848Member) map[string][]int32 {
	out := make(map[string][]int32)
	for topic, parts := range mem.target {
		for _, p := range parts {
			if owns(mem.owned, topic, p) || m.free(g, mem.id, topic, p) {
				out[topic] = append(out[topic], p)
			}
		}
		slices.Sort(out[topic])
	}
	return out
}

// free reports whether no member other than self currently owns (topic, partition).
//
// "Owns" is what a member reported owning OR what it was last told it owns. The
// report alone is not enough: a member only reports its partitions on a later
// heartbeat, so right after it is handed a partition it believes it owns it
// while the report still says it does not. Judging by the report alone gave the
// same partition to a second member in that window — two consumers reading and
// committing it at once.
func (m *cg848Manager) free(g *cg848Group, self, topic string, p int32) bool {
	for id, other := range g.members {
		if id != self && (owns(other.owned, topic, p) || owns(other.lastSent, topic, p)) {
			return false
		}
	}
	return true
}

// bumpAndReassign advances the group epoch and recomputes every member's target
// assignment. An empty group resets to epoch 0.
func (m *cg848Manager) bumpAndReassign(g *cg848Group) {
	if len(g.members) == 0 {
		g.epoch = 0
		return
	}
	g.epoch++
	m.assign(g)
}

// assign runs the server-side assignor, filling each member's target. Partitions of
// each subscribed topic are distributed only among the members subscribed to it —
// round-robin for "uniform", contiguous ranges for "range".
func (m *cg848Manager) assign(g *cg848Group) {
	for _, mem := range g.members {
		mem.target = make(map[string][]int32)
	}

	for _, topic := range subscribedTopics(g) {
		n := m.partitionCount(topic)
		if n == 0 {
			continue
		}
		subs := subscribersOf(g, topic) // sorted by member id for determinism.
		if len(subs) == 0 {
			continue
		}
		if g.assignor == assignorRange {
			assignRange(g, topic, n, subs)
		} else {
			for p := int32(0); p < n; p++ {
				mem := g.members[subs[int(p)%len(subs)]]
				mem.target[topic] = append(mem.target[topic], p)
			}
		}
	}
}

// assignRange gives each subscriber a contiguous slice of the topic's partitions;
// the first (n mod k) members get one extra.
func assignRange(g *cg848Group, topic string, n int32, subs []string) {
	k := int32(len(subs))
	base, extra := n/k, n%k
	var start int32
	for i, id := range subs {
		count := base
		if int32(i) < extra {
			count++
		}
		for p := start; p < start+count; p++ {
			g.members[id].target[topic] = append(g.members[id].target[topic], p)
		}
		start += count
	}
}

func (m *cg848Manager) resetSession(g *cg848Group, mem *cg848Member) {
	if mem.sessionTimer != nil {
		mem.sessionTimer.Stop()
	}
	mem.sessionGen++
	id, gen := mem.id, mem.sessionGen
	mem.sessionTimer = time.AfterFunc(m.sessionTimeout, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		// Stale if superseded by a rejoin, or if the member heartbeated while
		// this was waiting for the lock (Timer.Stop cannot recall a fired timer).
		if g.members[id] != mem || mem.sessionGen != gen {
			return
		}
		m.removeMember(g, id)
		m.bumpAndReassign(g)
		m.log.Info("KIP-848 member session expired", "group", g.id, "member", id)
	})
}

func (m *cg848Manager) removeMember(g *cg848Group, id string) {
	if mem := g.members[id]; mem != nil {
		if mem.sessionTimer != nil {
			mem.sessionTimer.Stop()
		}
		mem.sessionGen++
	}
	delete(g.members, id)
	if len(g.members) == 0 {
		delete(m.groups, g.id)
	}
}

// topicChanged re-runs the assignor for every group subscribed to topic, after
// the topic was created, deleted or grown. Assignments are otherwise computed
// only when membership or subscriptions change, so a consumer that subscribed
// before its topic existed (start the app, then create the topic — the common
// order in local development) would otherwise idle forever.
func (m *cg848Manager) topicChanged(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range m.groups {
		if !slices.Contains(subscribedTopics(g), topic) {
			continue
		}
		before := make(map[string]map[string][]int32, len(g.members))
		for id, mem := range g.members {
			before[id] = mem.target
		}
		m.assign(g)
		for id, mem := range g.members {
			if !assignmentsEqual(before[id], mem.target) {
				g.epoch++ // members learn of new targets through the epoch.
				break
			}
		}
	}
}

// hasMembers reports whether the group id is in use here, so the classic
// coordinator can refuse to share it (a group is one protocol or the other).
func (m *cg848Manager) hasMembers(group string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[group]
	return g != nil && len(g.members) > 0
}

// --- metadata helpers ---

func (m *cg848Manager) partitionCount(topic string) int32 {
	t, ok, err := m.meta.Topic(topic)
	if err != nil || !ok {
		return 0
	}
	return t.Partitions
}

func (m *cg848Manager) ownedFromReq(topics []kmsg.ConsumerGroupHeartbeatRequestTopic) map[string][]int32 {
	owned := make(map[string][]int32)
	for _, t := range topics {
		if name, ok := m.topicName(t.TopicID); ok {
			owned[name] = append([]int32(nil), t.Partitions...)
		}
	}
	return owned
}

func (m *cg848Manager) topicName(id [16]byte) (string, bool) {
	t, ok, err := m.meta.TopicByID(meta.TopicID(id))
	if err != nil || !ok {
		return "", false
	}
	return t.Name, true
}

func (m *cg848Manager) encodeAssignment(deliver map[string][]int32) *kmsg.ConsumerGroupHeartbeatResponseAssignment {
	a := kmsg.NewConsumerGroupHeartbeatResponseAssignment()
	for _, name := range sortedKeys(deliver) {
		t, ok, err := m.meta.Topic(name)
		if err != nil || !ok {
			continue
		}
		at := kmsg.NewConsumerGroupHeartbeatResponseAssignmentTopic()
		at.TopicID = t.ID
		at.Partitions = deliver[name]
		a.Topics = append(a.Topics, at)
	}
	return &a
}

// describe returns the KIP-848 view of a group for ConsumerGroupDescribe, or
// ok=false if the group is unknown.
func (m *cg848Manager) describe(group string) (state string, epoch int32, assignor string, members []cg848Member, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[group]
	if g == nil {
		return "", 0, "", nil, false
	}
	for _, mem := range g.members {
		members = append(members, *mem)
	}
	return g.stateName(), g.epoch, g.assignor, members, true
}

// --- small helpers ---

func owns(owned map[string][]int32, topic string, p int32) bool {
	return slices.Contains(owned[topic], p)
}

func assignorName(p *string) string {
	if p == nil || *p == "" {
		return ""
	}
	return *p
}

func subscribedTopics(g *cg848Group) []string {
	set := make(map[string]struct{})
	for _, mem := range g.members {
		for _, t := range mem.subscribed {
			set[t] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

func subscribersOf(g *cg848Group, topic string) []string {
	var out []string
	for id, mem := range g.members {
		if slices.Contains(mem.subscribed, topic) {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

func sortedKeys(m map[string][]int32) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func assignmentsEqual(a, b map[string][]int32) bool {
	if len(a) != len(b) {
		return false
	}
	for topic, ap := range a {
		if !slices.Equal(ap, b[topic]) {
			return false
		}
	}
	return true
}

// --- handlers ---

// handleConsumerGroupHeartbeat is the single KIP-848 API that carries join, poll,
// and leave. The broker computes and drives the assignment (see cg848Manager).
func (b *Broker) handleConsumerGroupHeartbeat(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	hr := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)

	// One group id, one protocol. A live classic group is not a consumer group, and
	// joining it here would give every partition to both sets of members.
	if hr.MemberEpoch >= 0 && b.groups.hasMembers(hr.Group) {
		resp := kmsg.NewPtrConsumerGroupHeartbeatResponse()
		resp.SetVersion(req.Header.APIVersion)
		resp.ErrorCode = int16(protocol.ErrGroupIDNotFound)
		return protocol.EncodeResponse(req.Header, resp), nil
	}
	res := b.cg848.heartbeat(hr, clientRef{req.Header.ClientID, req.Conn.RemoteHost()})

	resp := kmsg.NewPtrConsumerGroupHeartbeatResponse()
	resp.SetVersion(req.Header.APIVersion)
	resp.ErrorCode = res.errorCode
	if res.errorCode == int16(protocol.ErrNone) {
		mid := res.memberID
		resp.MemberID = &mid
		resp.MemberEpoch = res.memberEpoch
		resp.HeartbeatIntervalMillis = cg848HeartbeatMs
		resp.Assignment = res.assignment
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// handleConsumerGroupDescribe reports the broker-driven group's state, epoch, and
// members with their current assignments — the data `kafka-consumer-groups
// --describe` shows. Unknown groups get GROUP_ID_NOT_FOUND.
func (b *Broker) handleConsumerGroupDescribe(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	dr := kreq.(*kmsg.ConsumerGroupDescribeRequest)

	resp := kmsg.NewPtrConsumerGroupDescribeResponse()
	resp.SetVersion(req.Header.APIVersion)

	for _, group := range dr.Groups {
		rg := kmsg.NewConsumerGroupDescribeResponseGroup()
		rg.Group = group
		state, epoch, assignor, members, ok := b.cg848.describe(group)
		if !ok {
			rg.ErrorCode = int16(protocol.ErrGroupIDNotFound)
			resp.Groups = append(resp.Groups, rg)
			continue
		}
		rg.State = state
		rg.Epoch = epoch
		rg.AssignmentEpoch = epoch
		rg.AssignorName = assignor
		for i := range members {
			mem := &members[i]
			rm := kmsg.NewConsumerGroupDescribeResponseGroupMember()
			rm.MemberID = mem.id
			rm.ClientID = mem.client.clientID
			rm.ClientHost = mem.client.host
			rm.InstanceID = mem.instanceID
			rm.RackID = mem.rack
			rm.MemberEpoch = mem.epoch
			rm.SubscribedTopics = mem.subscribed
			rm.Assignment = b.cg848.describeAssignment(mem.owned)
			rm.TargetAssignment = b.cg848.describeAssignment(mem.target)
			rg.Members = append(rg.Members, rm)
		}
		resp.Groups = append(resp.Groups, rg)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// describeAssignment renders a topic->partitions map as a ConsumerGroupDescribe
// assignment (topic name + id + partitions).
func (m *cg848Manager) describeAssignment(a map[string][]int32) kmsg.Assignment {
	out := kmsg.NewAssignment()
	for _, name := range sortedKeys(a) {
		t, ok, err := m.meta.Topic(name)
		if err != nil || !ok {
			continue
		}
		at := kmsg.NewAssignmentTopicPartition()
		at.TopicID = t.ID
		at.Topic = name
		at.Partitions = a[name]
		out.TopicPartitions = append(out.TopicPartitions, at)
	}
	return out
}
