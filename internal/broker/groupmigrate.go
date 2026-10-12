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
//     REBALANCE_IN_PROGRESS on a heartbeat, when its partitions are to change; its
//     rejoin gives up everything it held, which is what lets another member take them.
//   - When the last consumer-protocol member leaves, the group is a classic group again:
//     its members are told their generation is gone (ILLEGAL_GENERATION) and rejoin.
//
// An emptied group moves between protocols whole (claims.release), which needs none of
// this.

// classicSeat marks a member of a consumer group that arrived by JoinGroup.
type classicSeat struct {
	protocols []groupProtocolEntry // as offered, in the client's order.
	protocol  string               // the one whose subscription it was read from.
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

// encodeClassicAssignment is the bytes a classic member gets in its SyncGroup: version 0,
// the topics it owns with their partitions, no user data.
func encodeClassicAssignment(topics map[string][]int32) []byte {
	names := make([]string, 0, len(topics))
	for n := range topics {
		names = append(names, n)
	}
	slices.Sort(names)
	b := binary.BigEndian.AppendUint16(nil, 0)
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

// seatSubscription is the topics a classic member's protocols subscribe to, and the
// name of the protocol they were read from: the first it offered that can be read.
func seatSubscription(protocols []groupProtocolEntry) (topics []string, protocol string, ok bool) {
	for _, p := range protocols {
		if t, decoded := decodeConsumerSubscription(p.metadata); decoded {
			return t, p.name, true
		}
	}
	return nil, "", false
}

// adopt makes a consumer group out of a classic one: each member keeps the partitions
// it held, which no other member may take until it has rejoined and given them up.
// Caller holds m.mu.
func (m *cg848Manager) adopt(id string, snap classicSnapshot) *cg848Group {
	g := &cg848Group{id: id, epoch: snap.generation, assignor: assignorUniform, members: make(map[string]*cg848Member)}
	m.groups[id] = g
	for _, sm := range snap.members {
		topics, protocol, _ := seatSubscription(sm.protocols)
		held := tpSet{}
		for name, parts := range decodeClassicAssignment(sm.assignment) {
			t, ok, err := m.meta.Topic(name)
			if err != nil || !ok {
				continue
			}
			for _, p := range parts {
				held[tp{t.ID, p}] = struct{}{}
			}
		}
		mem := &cg848Member{
			id: sm.id, instanceID: sm.instanceID, client: sm.client, subscribed: topics,
			epoch: snap.generation, prevEpoch: snap.generation, target: held.clone(), assigned: held,
			rebalanceTimeout: sm.rebalanceTimeout,
			classic:          &classicSeat{protocols: sm.protocols, protocol: protocol, session: sm.session},
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
			assignment: m.classicAssignmentBytes(mem.assigned),
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
	topics, protocol, ok := seatSubscription(protocols)
	if r.ProtocolType != "consumer" || len(protocols) == 0 || !ok {
		return refuse(kerr.InconsistentGroupProtocol.Code)
	}

	id := r.MemberID
	if id == "" {
		// Kafka from JoinGroup v4 hands out the id first and the client comes back with
		// it (KIP-394); older clients are joined in the one step.
		id = generateMemberID(r.InstanceID)
		if r.GetVersion() >= 4 {
			m.pendingSeat(r.Group, id)
			return joinResult{errorCode: kerr.MemberIDRequired.Code, memberID: id, generation: -1}, true
		}
	}
	mem := g.members[id]
	switch {
	case mem != nil && mem.classic == nil:
		return refuse(kerr.UnknownMemberID.Code) // that is a consumer-protocol member's id.
	case mem == nil && r.MemberID != "" && !m.seatIssued(r.Group, id):
		return refuse(kerr.UnknownMemberID.Code)
	}

	// The group is recorded as having a classic member before it has this one
	// (offsetexpiry.go). A join refused here has not used its id up: it comes again.
	if m.liveness != nil {
		if err := m.liveness.joining(g.id, true, time.Duration(r.SessionTimeoutMillis)*time.Millisecond); err != nil {
			return refuse(kerr.CoordinatorNotAvailable.Code)
		}
	}
	m.redeemSeat(r.Group, id)
	changed := false
	if mem == nil {
		mem = &cg848Member{id: id, target: tpSet{}, assigned: tpSet{}}
		g.members[id] = mem
		changed = true
		m.emit(g, "joined", id, "client "+who.clientID+" (classic protocol)", "")
	} else {
		// A member asking in again has dropped everything it held: that is what a
		// classic rejoin means, and what lets another member take the partitions. It
		// is not a change to the group by itself (recorded: the epoch stays).
		mem.assigned, mem.revoking = tpSet{}, nil
		mem.revoke.stop()
	}
	mem.client, mem.instanceID = who, r.InstanceID
	if !slices.Equal(mem.subscribed, topics) {
		mem.subscribed = topics
		changed = true
	}
	mem.classic = &classicSeat{protocols: protocols, protocol: protocol, session: time.Duration(r.SessionTimeoutMillis) * time.Millisecond}
	mem.rebalanceTimeout = time.Duration(r.RebalanceTimeoutMillis) * time.Millisecond
	if mem.rebalanceTimeout <= 0 {
		mem.rebalanceTimeout = mem.classic.session
	}
	mem.left = false
	m.resetSession(g, mem)
	if changed {
		m.bumpAndReassign(g, "a classic member joined or changed its subscription")
	}
	m.reconcile(g, mem, tpSet{})
	m.armClassicRevocations(g)
	return joinResult{generation: mem.epoch, protocol: protocol, memberID: id}, true
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
	switch {
	case mem == nil || mem.classic == nil:
		return nil, kerr.UnknownMemberID.Code, true
	case r.Generation != mem.epoch:
		return nil, kerr.IllegalGeneration.Code, true
	case mem.epoch != g.epoch && len(mem.revoking) == 0:
		// The group has moved on since this member joined, and it is not in the middle
		// of giving partitions up: there is nothing for it to collect, it must join
		// again (recorded: ps-a-leader-sync is 27, co-a-resync-1 is not).
		return nil, kerr.RebalanceInProgress.Code, true
	}
	m.resetSession(g, mem)
	mem.sent = mem.assigned.clone()
	return m.classicAssignmentBytes(mem.assigned), errNone, true
}

// classicAssignmentBytes is a set of partitions as a classic consumer is told it.
func (m *cg848Manager) classicAssignmentBytes(a tpSet) []byte {
	topics := map[string][]int32{}
	for _, t := range a.byTopic() {
		if topic, ok, err := m.meta.TopicByID(t.topic); err == nil && ok {
			topics[topic.Name] = append(topics[topic.Name], t.parts...)
		}
	}
	return encodeClassicAssignment(topics)
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
		if mem := g.members[who.memberID]; mem != nil && mem.classic != nil {
			m.emit(g, "left", mem.id, "client "+mem.client.clientID+" left the group", "")
			m.removeMember(g, mem.id)
			res.errorCode = errNone
			removed = true
		}
		results = append(results, res)
	}
	if removed {
		if len(g.members) > 0 {
			m.bumpAndReassign(g, "a member left")
			m.armClassicRevocations(g)
			m.maybeDowngrade(g)
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
