package broker

import (
	"encoding/binary"
	"slices"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Mixed groups: classic and consumer-protocol members on one group id (GRP-8, D7).
//
// Kafka 4's default group.consumer.migration.policy is bidirectional, which is what a
// rolling upgrade of an application needs: one instance at a time moves to
// group.protocol=consumer while the others are still classic. What a real broker
// answers is recorded in conformance/recordings/mixed-protocol-groups.json,
// mixed-protocol-rolling.json and mixed-protocol-parked.json, and this is what they say:
//
//   - A consumer-protocol member joining a classic group whose protocol type is
//     "consumer" turns it into a consumer group: the classic members stay, as members
//     of it, with the partitions they held, until they rejoin. A group of any other
//     protocol type (Connect) is refused, GROUP_ID_NOT_FOUND.
//   - A group in the middle of a rebalance is upgraded all the same. What it had parked
//     is answered REBALANCE_IN_PROGRESS, and the members that sent it are members of
//     the consumer group like the others, under the ids they have.
//   - A classic client joining a consumer group becomes a member of it: its JoinGroup is
//     answered with no leader and no member list (nobody relays assignments, the broker
//     computes them) and its SyncGroup carries the partitions it may have now.
//   - A classic member is told to rejoin the way the protocol has always done it, with
//     REBALANCE_IN_PROGRESS on a heartbeat, when its partitions are to change. Its
//     JoinGroup says which partitions it still owns: an eager consumer owns none by
//     then, which is what lets another member take them; a cooperative one owns what it
//     had, is given the part of it that it keeps, and has given up the rest when a later
//     JoinGroup of its own no longer lists it (mixed-protocol-cooperative.json).
//   - A static classic member that restarts (its instance id, no member id) takes the
//     place of the member holding that instance, classic or not; the id it replaces is
//     fenced (mixed-protocol-static.json).
//   - When the last consumer-protocol member leaves, the group is a classic group again:
//     its members are told their generation is gone (ILLEGAL_GENERATION) and rejoin.
//
// An emptied group moves between protocols whole (claims.release), which needs none of
// this.

// classicSeat marks a member of a consumer group that arrived by JoinGroup.
type classicSeat struct {
	protocols []groupProtocolEntry // as offered, in the client's order.
	protocol  string               // the one whose subscription it was read from.
	version   int16                // the version that subscription is written at, and its assignments are.
	session   time.Duration        // the session timeout it asked for.
}

// takeStatus says what takeClassic found.
type takeStatus int

const (
	takeNone        takeStatus = iota // no classic group with members.
	takeOK                            // taken; the claim is now the consumer protocol's.
	takeNotConsumer                   // a classic group, but not of protocol type "consumer".
)

// classicSnapshot is a classic group handed to the consumer-group coordinator.
type classicSnapshot struct {
	generation int32
	members    []classicSnapshotMember
}

type classicSnapshotMember struct {
	id               string
	instanceID       *string
	client           clientRef
	protocols        []groupProtocolEntry
	protocol         string
	assignment       []byte
	session          time.Duration
	rebalanceTimeout time.Duration
}

// classicBridge is the classic coordinator as the consumer-group coordinator sees it,
// for moving a group between them. Both methods take the classic coordinator's lock, so
// the consumer-group coordinator calls them with its own held and never the reverse.
type classicBridge interface {
	// takeClassic removes the classic group id, which has members and is of protocol
	// type "consumer", and moves its claim to the consumer protocol.
	takeClassic(id string) (classicSnapshot, takeStatus)
	// giveClassic makes id a classic group of the members, all of whom must rejoin,
	// and moves its claim to the classic protocol.
	giveClassic(id string, snap classicSnapshot) bool
}

// transfer moves the id from one protocol to another in one step, if from holds it.
func (c *groupClaims) transfer(id, from, to string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.owner[id] != from {
		return false
	}
	c.owner[id] = to
	return true
}

// takeClassic is classicBridge.
func (gm *groupManager) takeClassic(id string) (classicSnapshot, takeStatus) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[id]
	if g == nil || len(g.members) == 0 {
		return classicSnapshot{}, takeNone
	}
	if g.protocolType != "consumer" {
		return classicSnapshot{}, takeNotConsumer
	}
	// A group is upgraded only if every member subscribes the way consumers do: the
	// consumer protocol is what the new coordinator reads the topics from.
	for _, m := range g.members {
		if _, _, ok := seatSubscription(m.protocols); !ok {
			return classicSnapshot{}, takeNotConsumer
		}
	}
	if !gm.claims.transfer(id, protoClassic, protoConsumer) {
		return classicSnapshot{}, takeNone
	}
	snap := classicSnapshot{generation: g.generation}
	g.join.stop()
	// What is parked is answered REBALANCE_IN_PROGRESS, a JoinGroup and a SyncGroup alike
	// (recorded: mixed-protocol-parked). The rebalance it waited on is over; the member
	// keeps its id, asks again with it, and finds itself a member of the consumer group,
	// which is why every member is carried over, the ones that had only just joined too.
	g.failPendingSyncs(kerr.RebalanceInProgress.Code)
	for _, mid := range sortedMemberIDs(g) {
		m := g.members[mid]
		m.session.stop()
		m.answerPendingJoin(kerr.RebalanceInProgress.Code)
		// Between its JoinGroups and the leader's SyncGroup a generation has no
		// assignments yet, and a member still has what the one before gave it
		// (recorded: ps-describe): a cooperative consumer is reading it.
		held := m.assignment
		if g.state == stateCompletingRebalance {
			held = m.previous
		}
		snap.members = append(snap.members, classicSnapshotMember{
			id: m.id, instanceID: m.instanceID, client: m.client, protocols: m.protocols, protocol: g.protocol,
			assignment: held, session: m.sessionTimeout, rebalanceTimeout: m.rebalanceTimeout,
		})
	}
	delete(gm.groups, id)
	return snap, takeOK
}

// giveClassic is classicBridge.
func (gm *groupManager) giveClassic(id string, snap classicSnapshot) bool {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if !gm.claims.transfer(id, protoConsumer, protoClassic) {
		return false
	}
	// A generation no member holds: each one's next heartbeat is told so
	// (ILLEGAL_GENERATION, as Kafka does) and it rejoins.
	g := &group{id: id, state: statePreparingRebalance, generation: snap.generation + 1, protocolType: "consumer",
		members: make(map[string]*groupMember), static: make(map[string]string)}
	gm.groups[id] = g
	for _, sm := range snap.members {
		m := &groupMember{id: sm.id, instanceID: sm.instanceID, client: sm.client, protocols: sm.protocols,
			sessionTimeout: sm.session, rebalanceTimeout: sm.rebalanceTimeout, assignment: sm.assignment}
		g.members[sm.id] = m
		if sm.instanceID != nil && *sm.instanceID != "" {
			g.static[*sm.instanceID] = sm.id
		}
		gm.resetSession(g, m)
	}
	gm.emit(g, "rebalance", "", "the last consumer-protocol member left; the classic members rejoin", "the group became classic again")
	gm.armJoinTimer(g)
	return true
}

// --- the classic ConsumerProtocol bytes ---

// decodeClassicAssignment reads a classic consumer's assignment (version, topics with
// their partitions, user data) as topic names with partitions.
func decodeClassicAssignment(b []byte) map[string][]int32 {
	if len(b) < 6 {
		return nil
	}
	n := int(int32(binary.BigEndian.Uint32(b[2:])))
	b = b[6:]
	if n < 0 || n > len(b)/6 {
		return nil
	}
	out := map[string][]int32{}
	for i := 0; i < n; i++ {
		if len(b) < 2 {
			return nil
		}
		l := int(binary.BigEndian.Uint16(b))
		b = b[2:]
		if len(b) < l+4 {
			return nil
		}
		name := string(b[:l])
		b = b[l:]
		pn := int(int32(binary.BigEndian.Uint32(b)))
		b = b[4:]
		if pn < 0 || pn > len(b)/4 {
			return nil
		}
		for j := 0; j < pn; j++ {
			out[name] = append(out[name], int32(binary.BigEndian.Uint32(b)))
			b = b[4:]
		}
	}
	return out
}

// encodeClassicAssignment is the bytes a classic member gets in its SyncGroup: the topics
// it owns with their partitions and no user data, at the version its subscription was
// written at (recorded: 0, 1 and 3; the layout is the same at every one of them).
func encodeClassicAssignment(version int16, topics map[string][]int32) []byte {
	names := make([]string, 0, len(topics))
	for n := range topics {
		names = append(names, n)
	}
	slices.Sort(names)
	b := binary.BigEndian.AppendUint16(nil, uint16(version))
	b = binary.BigEndian.AppendUint32(b, uint32(len(names)))
	for _, n := range names {
		ps := slices.Clone(topics[n])
		slices.Sort(ps)
		b = binary.BigEndian.AppendUint16(b, uint16(len(n)))
		b = append(b, n...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(ps)))
		for _, p := range ps {
			b = binary.BigEndian.AppendUint32(b, uint32(p))
		}
	}
	return binary.BigEndian.AppendUint32(b, 0xffffffff)
}

// --- the consumer-group coordinator's side ---

// classicSubscription is what a classic consumer's protocol metadata says: the topics it
// subscribes to and, from version 1, the partitions it still owns.
type classicSubscription struct {
	version int16
	topics  []string
	owned   map[string][]int32
}

// decodeClassicSubscription reads a classic consumer's protocol metadata (version, topic
// names, user data; then from version 1 the owned partitions, and fields this broker
// does not use). A version 1 subscription whose owned partitions cannot be read is not
// read at all: taking it to own nothing would free partitions it may be reading.
func decodeClassicSubscription(b []byte) (sub classicSubscription, ok bool) {
	if sub.topics, ok = decodeConsumerSubscription(b); !ok {
		return sub, false
	}
	sub.version = int16(binary.BigEndian.Uint16(b))
	if sub.version < 1 {
		return sub, true
	}
	b = b[6:]
	for range sub.topics {
		b = b[2+int(binary.BigEndian.Uint16(b)):]
	}
	if len(b) < 4 {
		return sub, false
	}
	if n := int(int32(binary.BigEndian.Uint32(b))); n > 0 { // user data; -1 is none.
		if n > len(b)-4 {
			return sub, false
		}
		b = b[n:]
	}
	if sub.owned = decodeClassicAssignment(append([]byte{0, 0}, b[4:]...)); sub.owned == nil {
		return sub, false
	}
	return sub, true
}

// seatSubscription is what a classic member's protocols subscribe to, and the name of
// the protocol it was read from: the first it offered that can be read.
func seatSubscription(protocols []groupProtocolEntry) (sub classicSubscription, protocol string, ok bool) {
	for _, p := range protocols {
		if s, decoded := decodeClassicSubscription(p.metadata); decoded {
			return s, p.name, true
		}
	}
	return classicSubscription{}, "", false
}

// adopt makes a consumer group out of a classic one: each member keeps the partitions
// it held, which no other member may take until it has rejoined and given them up.
// Caller holds m.mu.
func (m *cg848Manager) adopt(id string, snap classicSnapshot) *cg848Group {
	g := &cg848Group{id: id, epoch: snap.generation, assignor: assignorUniform, members: make(map[string]*cg848Member)}
	m.groups[id] = g
	for _, sm := range snap.members {
		sub, protocol, _ := seatSubscription(sm.protocols)
		held := m.partitionsOf(decodeClassicAssignment(sm.assignment))
		mem := &cg848Member{
			id: sm.id, instanceID: sm.instanceID, client: sm.client, subscribed: sub.topics,
			epoch: snap.generation, prevEpoch: snap.generation, target: held.clone(), assigned: held,
			rebalanceTimeout: sm.rebalanceTimeout,
			classic:          &classicSeat{protocols: sm.protocols, protocol: protocol, version: sub.version, session: sm.session},
		}
		g.members[sm.id] = mem
		m.resetSession(g, mem)
	}
	return g
}

// maybeDowngrade turns a consumer group whose members are all classic back into a
// classic group, as Kafka does when the last consumer-protocol member leaves. Caller
// holds m.mu.
func (m *cg848Manager) maybeDowngrade(g *cg848Group) {
	if m.bridge == nil || len(g.members) == 0 {
		return
	}
	snap := classicSnapshot{generation: g.epoch}
	for _, id := range sortedKeys(g.members) {
		mem := g.members[id]
		if mem.classic == nil {
			return
		}
		snap.members = append(snap.members, classicSnapshotMember{
			id: mem.id, instanceID: mem.instanceID, client: mem.client, protocols: mem.classic.protocols,
			protocol: mem.classic.protocol, session: mem.classic.session, rebalanceTimeout: mem.rebalanceTimeout,
			assignment: m.classicAssignmentBytes(mem.classic.version, mem.assigned),
		})
	}
	if !m.bridge.giveClassic(g.id, snap) {
		return
	}
	for _, mem := range g.members {
		mem.stopTimers()
	}
	delete(m.groups, g.id)
	m.emit(g, "downgraded", "", "no consumer-protocol member is left; the group is classic again", "")
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// classicJoin answers a JoinGroup for a group the consumer-group coordinator holds. The
// client becomes one of its members. handled is false when there is no such group with
// members (the id is free, or classic): the classic coordinator takes it from there.
func (m *cg848Manager) classicJoin(r *kmsg.JoinGroupRequest, who clientRef) (res joinResult, handled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[r.Group]
	if g == nil || len(g.members) == 0 {
		return joinResult{}, false
	}
	refuse := func(code int16) (joinResult, bool) {
		return joinResult{errorCode: code, memberID: r.MemberID, generation: -1}, true
	}
	if session := time.Duration(r.SessionTimeoutMillis) * time.Millisecond; session < minSessionTimeout || session > maxSessionTimeout {
		return refuse(kerr.InvalidSessionTimeout.Code)
	}
	protocols := protocolEntries(r.Protocols)
	// A group of consumers has one protocol type; a Connect worker is not one of them.
	// And its classic members share a protocol, as in a classic group (recorded:
	// group-requests-refused, cm-classic-join-custom-assignor; the first classic member
	// may bring any, ca-classic-join-custom-assignor-with-id).
	if r.ProtocolType != "consumer" || len(protocols) == 0 || !g.classicCommonProtocol(r.MemberID, protocols) {
		return refuse(kerr.InconsistentGroupProtocol.Code)
	}
	static := r.InstanceID != nil // "" too (recorded: ei-join-empty-instance).

	id := r.MemberID
	var predecessor *cg848Member // the member a restarted static instance takes the place of.
	if id == "" {
		id = generateMemberID(r.InstanceID)
		switch {
		case static:
			// A static member joins in one step, and if its instance is held, in the
			// holder's place (recorded: sd-s1-join, sd-s2-join, sh-classic-join-same-instance).
			predecessor = g.holderOf(*r.InstanceID)
		case r.GetVersion() >= 4:
			// Kafka from JoinGroup v4 hands out the id first and the client comes back
			// with it (KIP-394); older clients are joined in the one step.
			m.pendingSeat(r.Group, id)
			return joinResult{errorCode: kerr.MemberIDRequired.Code, memberID: id, generation: -1}, true
		}
	}
	// The subscription is read once the member has an id: one that cannot be read is
	// answered MEMBER_ID_REQUIRED first, as any other (recorded:
	// cm-classic-join-unreadable-subscription).
	sub, protocol, ok := seatSubscription(protocols)
	if !ok {
		return refuse(kerr.InconsistentGroupProtocol.Code)
	}
	mem := g.members[id]
	switch {
	case mem != nil && mem.classic == nil:
		return refuse(kerr.UnknownMemberID.Code) // that is a consumer-protocol member's id.
	case mem == nil && r.MemberID != "" && g.staticCheck(id, r.InstanceID) == kerr.FencedInstanceID.Code:
		// Its restarted self has the instance now. Answered with no member id
		// (recorded: sd-s1-join-old-id).
		return joinResult{errorCode: kerr.FencedInstanceID.Code, generation: -1}, true
	}
	// A member id this group did not hand out is taken as it comes: Kafka's consumer
	// group seats it (recorded: cm-classic-join-unknown-member-id).

	// The group is recorded as having a classic member before it has this one
	// (offsetexpiry.go). A join refused here has not used its id up: it comes again.
	if m.liveness != nil {
		if err := m.liveness.joining(g.id, true, time.Duration(r.SessionTimeoutMillis)*time.Millisecond, "consumer"); err != nil {
			return refuse(kerr.CoordinatorNotAvailable.Code)
		}
	}
	m.redeemSeat(r.Group, id)
	changed := false
	wasConsumer := false
	switch {
	case predecessor != nil:
		// The instance has one seat. Its new incarnation has what the old one had and,
		// like any member asking in, keeps of it what its JoinGroup says it owns.
		predecessor.stopTimers()
		delete(g.members, predecessor.id)
		wasConsumer = predecessor.classic == nil
		mem = &cg848Member{id: id, target: predecessor.target, assigned: predecessor.assigned, revoking: predecessor.revoking,
			epoch: predecessor.epoch, prevEpoch: predecessor.prevEpoch, subscribed: predecessor.subscribed}
		g.members[id] = mem
		changed = predecessor.regex != nil
		m.emit(g, "joined", id, "client "+who.clientID+" (classic protocol), the restarted static instance "+*r.InstanceID+" in place of "+predecessor.id, "")
	case mem == nil:
		mem = &cg848Member{id: id, target: tpSet{}, assigned: tpSet{}}
		g.members[id] = mem
		changed = true
		m.emit(g, "joined", id, "client "+who.clientID+" (classic protocol)", "")
	}
	// A member asking in holds what its JoinGroup says it still owns and has given up
	// the rest. An eager consumer owns nothing by then, which is what lets another
	// member take its partitions; a cooperative one goes on reading what it lists
	// (recorded: mixed-protocol-cooperative). It is not a change to the group by
	// itself (recorded: the epoch stays).
	owned := m.partitionsOf(sub.owned)
	for p := range mem.assigned {
		if !owned.has(p) {
			delete(mem.assigned, p)
		}
	}
	mem.client, mem.instanceID = who, r.InstanceID
	if !slices.Equal(mem.subscribed, sub.topics) {
		mem.subscribed = sub.topics
		changed = true
	}
	mem.classic = &classicSeat{protocols: protocols, protocol: protocol, version: sub.version, session: time.Duration(r.SessionTimeoutMillis) * time.Millisecond}
	mem.rebalanceTimeout = time.Duration(r.RebalanceTimeoutMillis) * time.Millisecond
	if mem.rebalanceTimeout <= 0 {
		mem.rebalanceTimeout = mem.classic.session
	}
	mem.left = false
	m.resetSession(g, mem)
	if changed {
		m.bumpAndReassign(g, "a classic member joined or changed its subscription")
	}
	m.reconcile(g, mem, owned)
	m.armClassicRevocations(g)
	res = joinResult{generation: mem.epoch, protocol: protocol, memberID: id}
	if wasConsumer {
		// The member it replaced may have been the last of the consumer protocol.
		m.maybeDowngrade(g)
	}
	return res, true
}

// holderOf is the member that holds a static instance id, or nil.
func (g *cg848Group) holderOf(instance string) *cg848Member {
	for _, mem := range g.members {
		if mem.instanceID != nil && *mem.instanceID == instance {
			return mem
		}
	}
	return nil
}

// staticCheck is what a request naming a static instance is answered when the instance
// is not this member's: UNKNOWN_MEMBER_ID when nobody holds it, FENCED_INSTANCE_ID when
// another member id does (recorded: group-requests-refused, cs-heartbeat-wrong-instance,
// cs-old-member-heartbeat; mixed-protocol-static, sd-s1-heartbeat-old-id).
func (g *cg848Group) staticCheck(memberID string, instance *string) int16 {
	if instance == nil {
		return errNone
	}
	switch holder := g.holderOf(*instance); {
	case holder == nil:
		return kerr.UnknownMemberID.Code
	case holder.id != memberID:
		return kerr.FencedInstanceID.Code
	}
	return errNone
}

// classicCommonProtocol reports whether a classic member joining with these protocols
// shares one with every other classic member of the group.
func (g *cg848Group) classicCommonProtocol(memberID string, offered []groupProtocolEntry) bool {
	for _, p := range offered {
		all := true
		for id, other := range g.members {
			if id == memberID || other.classic == nil {
				continue
			}
			if !slices.ContainsFunc(other.classic.protocols, func(o groupProtocolEntry) bool { return o.name == p.name }) {
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

// partitionsOf is partitions named by topic as a set by topic id; a topic that is not
// there has none.
func (m *cg848Manager) partitionsOf(byName map[string][]int32) tpSet {
	out := tpSet{}
	for name, parts := range byName {
		t, ok, err := m.meta.Topic(name)
		if err != nil || !ok {
			continue
		}
		for _, p := range parts {
			out[tp{t.ID, p}] = struct{}{}
		}
	}
	return out
}

// classicSync answers a SyncGroup from a classic member of a consumer group: nobody
// uploads assignments, the broker has computed them, and the member is given the
// partitions it may consume now.
func (m *cg848Manager) classicSync(r *kmsg.SyncGroupRequest) (assignment []byte, code int16, handled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[r.Group]
	if g == nil || len(g.members) == 0 {
		return nil, 0, false
	}
	mem := g.members[r.MemberID]
	if code := g.staticCheck(r.MemberID, r.InstanceID); code != errNone {
		return nil, code, true
	}
	switch {
	case mem == nil || mem.classic == nil:
		return nil, kerr.UnknownMemberID.Code, true
	case r.Generation != mem.epoch:
		return nil, kerr.IllegalGeneration.Code, true
	case r.ProtocolType != nil && *r.ProtocolType != "consumer",
		r.Protocol != nil && *r.Protocol != mem.classic.protocol:
		// recorded: cm-sync-wrong-protocol-type, cm-sync-wrong-protocol-name.
		return nil, kerr.InconsistentGroupProtocol.Code, true
	case mem.epoch != g.epoch && len(mem.revoking) == 0:
		// The group has moved on since this member joined, and it is not in the middle
		// of giving partitions up: there is nothing for it to collect, it must join
		// again (recorded: ps-a-leader-sync is 27, co-a-resync-1 is not).
		return nil, kerr.RebalanceInProgress.Code, true
	}
	m.resetSession(g, mem)
	mem.sent = mem.assigned.clone()
	return m.classicAssignmentBytes(mem.classic.version, mem.assigned), errNone, true
}

// classicAssignmentBytes is a set of partitions as a classic consumer is told it.
func (m *cg848Manager) classicAssignmentBytes(version int16, a tpSet) []byte {
	topics := map[string][]int32{}
	for _, t := range a.byTopic() {
		if topic, ok, err := m.meta.TopicByID(t.topic); err == nil && ok {
			topics[topic.Name] = append(topics[topic.Name], t.parts...)
		}
	}
	return encodeClassicAssignment(version, topics)
}

// classicHeartbeat keeps a classic member's session and says whether it must rejoin: its
// generation is behind the group's, or what it holds is not what it is to have and
// something can move now.
func (m *cg848Manager) classicHeartbeat(r *kmsg.HeartbeatRequest) (code int16, handled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[r.Group]
	if g == nil || len(g.members) == 0 {
		return 0, false
	}
	mem := g.members[r.MemberID]
	if code := g.staticCheck(r.MemberID, r.InstanceID); code != errNone {
		return code, true
	}
	switch {
	case mem == nil || mem.classic == nil:
		return kerr.UnknownMemberID.Code, true
	case r.Generation != mem.epoch:
		return kerr.IllegalGeneration.Code, true
	}
	m.resetSession(g, mem)
	if mem.epoch != g.epoch || m.classicMustRejoin(g, mem) {
		return kerr.RebalanceInProgress.Code, true
	}
	return errNone, true
}

// classicMustRejoin is whether anything about the member's partitions can change now
// that only its rejoin can bring about: it holds one it is not to have, or is to have
// one nobody holds any more.
func (m *cg848Manager) classicMustRejoin(g *cg848Group, mem *cg848Member) bool {
	for p := range mem.assigned {
		if !mem.target.has(p) {
			return true
		}
	}
	for p := range mem.target {
		if !mem.assigned.has(p) && m.free(g, mem.id, p) {
			return true
		}
	}
	return false
}

// classicLeave removes classic members from a consumer group.
func (m *cg848Manager) classicLeave(r *kmsg.LeaveGroupRequest) (results []leaveResult, handled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[r.Group]
	if g == nil || len(g.members) == 0 {
		return nil, false
	}
	removed := false
	for _, who := range membersToLeave(r) {
		res := leaveResult{memberID: who.memberID, instanceID: who.instanceID, errorCode: kerr.UnknownMemberID.Code}
		mem := g.members[who.memberID]
		if who.memberID == "" && who.instanceID != nil {
			// A static member is named by its instance id, as an admin tool names it
			// (recorded: su-leave-by-instance).
			mem = g.holderOf(*who.instanceID)
		}
		// A consumer-protocol member named here leaves too, as an admin tool removes
		// one (recorded: cm-classic-leave-consumer-member).
		if mem != nil {
			m.emit(g, "left", mem.id, "client "+mem.client.clientID+" left the group", "")
			m.removeMember(g, mem.id)
			res.errorCode = errNone
			removed = true
		}
		results = append(results, res)
	}
	if removed {
		if len(g.members) > 0 {
			// Not downgraded, even when the member named was the last of the consumer
			// protocol: Kafka downgrades when such a member leaves by its own protocol
			// (recorded: cm-describe-after-leave is a consumer group).
			m.bumpAndReassign(g, "a member left")
			m.armClassicRevocations(g)
		}
	}
	return results, true
}

// armClassicRevocations starts the rebalance timeout of every classic member that holds
// a partition it is no longer to have: such a member only gives it up by rejoining, and
// without a deadline one that never does would keep it from its new owner for ever.
// Caller holds m.mu.
func (m *cg848Manager) armClassicRevocations(g *cg848Group) {
	for _, mem := range g.members {
		if mem.classic == nil || mem.revoke.armed() {
			continue
		}
		for p := range mem.assigned {
			if !mem.target.has(p) {
				timeout := mem.rebalanceTimeout
				if timeout <= 0 {
					timeout = m.sessionTimeout
				}
				m.armSeatRevoke(g, mem, timeout)
				break
			}
		}
	}
}

// armSeatRevoke removes a classic member that did not rejoin in time.
func (m *cg848Manager) armSeatRevoke(g *cg848Group, mem *cg848Member, timeout time.Duration) {
	mem.revoke.arm(timeout, func(gen uint64) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.stopped || g.members[mem.id] != mem || !mem.revoke.current(gen) {
			return
		}
		m.emit(g, "expired", mem.id, "a classic member did not rejoin within its rebalance timeout", "")
		m.removeMember(g, mem.id)
		if len(g.members) > 0 {
			m.bumpAndReassign(g, "a member was removed")
			m.maybeDowngrade(g)
		}
	})
}

// pendingSeat remembers a member id handed out for a classic join that follows.
func (m *cg848Manager) pendingSeat(group, id string) {
	if m.pending == nil {
		m.pending = map[string]map[string]time.Time{}
	}
	cutoff := time.Now().Add(-pendingMemberTTL)
	for g, ids := range m.pending {
		for mid, at := range ids {
			if at.Before(cutoff) {
				delete(ids, mid)
			}
		}
		if len(ids) == 0 {
			delete(m.pending, g)
		}
	}
	if m.pending[group] == nil {
		m.pending[group] = map[string]time.Time{}
	}
	m.pending[group][id] = time.Now()
}

// seatIssued reports whether id was handed out for group and is still valid.
func (m *cg848Manager) seatIssued(group, id string) bool {
	at, ok := m.pending[group][id]
	return ok && time.Since(at) <= pendingMemberTTL
}

// redeemSeat uses up an id handed out for group, once the member it was handed to is
// about to be seated.
func (m *cg848Manager) redeemSeat(group, id string) {
	delete(m.pending[group], id)
	if len(m.pending[group]) == 0 {
		delete(m.pending, group)
	}
}
