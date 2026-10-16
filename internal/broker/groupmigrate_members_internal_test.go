package broker

import (
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Classic members of a consumer group that are not plain eager, dynamic consumers: one
// that keeps partitions across a rejoin (a cooperative assignor), and a static one. What
// a real broker does with each is recorded in conformance/recordings/
// mixed-protocol-cooperative.json and mixed-protocol-static.json.

// owningSubscription is a classic subscription at this version (1 or above) that says
// which partitions of the topic the member still owns.
func owningSubscription(version uint16, topic string, owned ...int32) []byte {
	b := binary.BigEndian.AppendUint16(nil, version)
	b = binary.BigEndian.AppendUint32(b, 1)
	b = binary.BigEndian.AppendUint16(b, uint16(len(topic)))
	b = append(b, topic...)
	b = binary.BigEndian.AppendUint32(b, 0xffffffff) // no user data.
	if len(owned) == 0 {
		b = binary.BigEndian.AppendUint32(b, 0)
	} else {
		b = binary.BigEndian.AppendUint32(b, 1)
		b = binary.BigEndian.AppendUint16(b, uint16(len(topic)))
		b = append(b, topic...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(owned)))
		for _, p := range owned {
			b = binary.BigEndian.AppendUint32(b, uint32(p))
		}
	}
	if version >= 2 {
		b = binary.BigEndian.AppendUint32(b, 0xffffffff) // generation: none.
	}
	if version >= 3 {
		b = binary.BigEndian.AppendUint16(b, 0xffff) // rack: null.
	}
	return b
}

// seatJoin is a classic JoinGroup to a consumer group with this subscription.
func (r *expiryRig) seatJoin(t *testing.T, group, member string, instance *string, meta []byte) joinResult {
	t.Helper()
	req := joinReqSeat(group, member, rigClassicSession, rigClassicSession, "consumer", meta)
	req.InstanceID = instance
	res, handled := r.b.cg848.classicJoin(req, clientRef{"c", "h"})
	if !handled {
		t.Fatal("the consumer group did not take the classic join")
	}
	return res
}

func (r *expiryRig) seatSync(t *testing.T, group, member string, gen int32) (partitions []int32, raw []byte, code int16) {
	t.Helper()
	s := kmsg.NewPtrSyncGroupRequest()
	s.Group, s.MemberID, s.Generation = group, member, gen
	raw, code, handled := r.b.cg848.classicSync(s)
	if !handled {
		t.Fatal("the consumer group did not take the classic sync")
	}
	for _, ps := range decodeClassicAssignment(raw) {
		partitions = append(partitions, ps...)
	}
	slices.Sort(partitions)
	return partitions, raw, code
}

// twoOwners is the partitions more than one member of the group holds, as assigned or as
// a revocation it has not acknowledged: there must never be one.
func (r *expiryRig) twoOwners(group string) []int32 {
	r.b.cg848.mu.Lock()
	defer r.b.cg848.mu.Unlock()
	g := r.b.cg848.groups[group]
	if g == nil {
		return nil
	}
	holders := map[tp]int{}
	for _, mem := range g.members {
		for p := range mem.assigned {
			holders[p]++
		}
		for p := range mem.revoking {
			holders[p]++
		}
	}
	var twice []int32
	for p, n := range holders {
		if n > 1 {
			twice = append(twice, p.part)
		}
	}
	return twice
}

func assignedTo(res cg848Result) []int32 {
	var out []int32
	if res.assignment != nil {
		for _, t := range res.assignment.Topics {
			out = append(out, t.Partitions...)
		}
	}
	slices.Sort(out)
	return out
}

// A classic member that says in its JoinGroup which partitions it still owns keeps them:
// that is how a cooperative consumer goes on reading through a rebalance. The ones it is
// to lose are another member's only once a later JoinGroup of its own no longer lists
// them (recorded: co-a-rejoin-1 to co-n-heartbeat-after-2a). Every rejoin used to drop
// everything the member held, so the other member was handed partitions the first was
// still reading.
func TestACooperativeClassicMemberKeepsWhatItOwnsUntilItGivesItUp(t *testing.T) {
	shortSessions(t)
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	topic := r.withPartitions(t, "a", 4)
	a, gen := r.holding(t, "g", "a", rigClassicSession, rigClassicSession, 0, 1, 2, 3)
	n := r.join848(t, "g", "n", "a")
	noneOwned := map[[16]byte][]int32{topic: {}}
	if got := assignedTo(r.hb848("g", "n", n.memberEpoch, noneOwned)); len(got) != 0 {
		t.Fatalf("the new member was given %v while the classic member holds everything", got)
	}
	if code, _ := r.classicHeartbeat("g", a, gen); code != kerr.RebalanceInProgress.Code {
		t.Fatalf("the classic member's heartbeat = %d, want REBALANCE_IN_PROGRESS", code)
	}

	// It joins again owning all four: it has given nothing up yet.
	first := r.seatJoin(t, "g", a, nil, owningSubscription(1, "a", 0, 1, 2, 3))
	if first.errorCode != 0 || first.generation != gen {
		t.Fatalf("rejoin owning everything: %+v, want generation %d (it has not caught up while it holds what it must give up)", first, gen)
	}
	kept, _, code := r.seatSync(t, "g", a, first.generation)
	if code != 0 || !slices.Equal(kept, []int32{0, 1}) {
		t.Fatalf("its SyncGroup: code %d partitions %v, want the two it keeps", code, kept)
	}
	if got := assignedTo(r.hb848("g", "n", n.memberEpoch, noneOwned)); len(got) != 0 {
		t.Fatalf("the new member was given %v, which the classic member has not given up", got)
	}
	if twice := r.twoOwners("g"); len(twice) != 0 {
		t.Fatalf("partitions %v have two owners", twice)
	}
	if code, _ := r.classicHeartbeat("g", a, first.generation); code != kerr.RebalanceInProgress.Code {
		t.Fatalf("its heartbeat while it still has partitions to give up = %d, want REBALANCE_IN_PROGRESS", code)
	}

	// It joins again owning the two it kept: the other two are free now.
	second := r.seatJoin(t, "g", a, nil, owningSubscription(1, "a", 0, 1))
	if second.errorCode != 0 || second.generation != n.memberEpoch {
		t.Fatalf("rejoin owning what it kept: %+v, want generation %d", second, n.memberEpoch)
	}
	if kept, _, code := r.seatSync(t, "g", a, second.generation); code != 0 || !slices.Equal(kept, []int32{0, 1}) {
		t.Fatalf("its second SyncGroup: code %d partitions %v", code, kept)
	}
	if got := assignedTo(r.hb848("g", "n", n.memberEpoch, noneOwned)); !slices.Equal(got, []int32{2, 3}) {
		t.Fatalf("the new member was given %v, want the two the classic member gave up", got)
	}
	if twice := r.twoOwners("g"); len(twice) != 0 {
		t.Fatalf("partitions %v have two owners", twice)
	}
	if code, _ := r.classicHeartbeat("g", a, second.generation); code != 0 {
		t.Fatalf("its heartbeat once settled = %d", code)
	}
}

// An eager member's JoinGroup owns nothing, whatever version its subscription is written
// at: it gave everything up before it asked.
func TestAnEagerClassicMemberGivesUpEverythingWhenItRejoins(t *testing.T) {
	for _, meta := range [][]byte{subscriptionBytes("a"), owningSubscription(1, "a"), owningSubscription(3, "a")} {
		r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
		topic := r.withPartitions(t, "a", 4)
		shortSessions(t)
		a, _ := r.holding(t, "g", "a", rigClassicSession, rigClassicSession, 0, 1, 2, 3)
		n := r.join848(t, "g", "n", "a")
		again := r.seatJoin(t, "g", a, nil, meta)
		if again.errorCode != 0 || again.generation != n.memberEpoch {
			t.Fatalf("rejoin: %+v", again)
		}
		if kept, _, _ := r.seatSync(t, "g", a, again.generation); !slices.Equal(kept, []int32{0, 1}) {
			t.Fatalf("it holds %v after rejoining, want its two", kept)
		}
		if got := assignedTo(r.hb848("g", "n", n.memberEpoch, map[[16]byte][]int32{topic: {}})); !slices.Equal(got, []int32{2, 3}) {
			t.Fatalf("the new member was given %v", got)
		}
		if twice := r.twoOwners("g"); len(twice) != 0 {
			t.Fatalf("partitions %v have two owners", twice)
		}
	}
}

// The assignment a classic member is given is written at the version its subscription
// was (recorded: 0 in mixed-protocol-rolling, 1 in co-a-resync-1, 3 in v3-c-sync).
func TestAClassicMembersAssignmentIsAtItsSubscriptionsVersion(t *testing.T) {
	for version, meta := range map[uint16][]byte{0: subscriptionBytes("a"), 1: owningSubscription(1, "a"), 3: owningSubscription(3, "a")} {
		r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
		r.join848(t, "g", "n", "a")
		first := r.seatJoin(t, "g", "", nil, meta)
		res := r.seatJoin(t, "g", first.memberID, nil, meta)
		_, raw, code := r.seatSync(t, "g", first.memberID, res.generation)
		if code != 0 || len(raw) < 2 || binary.BigEndian.Uint16(raw) != version {
			t.Errorf("subscription at version %d: sync code %d, assignment % x", version, code, raw)
		}
	}
}

// A static classic member that restarts takes its own place in a consumer group: the
// instance has one seat, under the new member id, and the old id is fenced (recorded:
// sd-s2-join to sd-describe-after-restart). It used to be seated a second time, the old
// seat holding its partitions until its session ran out.
func TestAStaticClassicMemberRestartingInAConsumerGroupIsNotSeatedTwice(t *testing.T) {
	shortSessions(t)
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	r.withPartitions(t, "a", 2)
	r.join848(t, "g", "n", "a")
	instance := "i1"

	// A static member joins in one step: no MEMBER_ID_REQUIRED round trip.
	s1 := r.seatJoin(t, "g", "", &instance, subscriptionBytes("a"))
	if s1.errorCode != 0 || s1.memberID == "" {
		t.Fatalf("static join: %+v, want it joined at once", s1)
	}
	if got := r.members("g"); len(got) != 2 || !got[s1.memberID] {
		t.Fatalf("members after the static join: %v", got)
	}

	// It restarts: the same instance, no member id.
	s2 := r.seatJoin(t, "g", "", &instance, subscriptionBytes("a"))
	if s2.errorCode != 0 || s2.memberID == "" || s2.memberID == s1.memberID {
		t.Fatalf("the restarted instance's join: %+v, want a new member id", s2)
	}
	if got := r.members("g"); len(got) != 2 || !got[s2.memberID] || got[s1.memberID] {
		t.Fatalf("members after the restart (true: classic) = %v, want the new id in the old one's place", got)
	}
	if twice := r.twoOwners("g"); len(twice) != 0 {
		t.Fatalf("partitions %v have two owners", twice)
	}

	hb := func(member string, inst *string) int16 {
		h := kmsg.NewPtrHeartbeatRequest()
		h.Group, h.MemberID, h.Generation, h.InstanceID = "g", member, s2.generation, inst
		code, _ := r.b.cg848.classicHeartbeat(h)
		return code
	}
	if code := hb(s1.memberID, &instance); code != kerr.FencedInstanceID.Code {
		t.Errorf("the old id's heartbeat with the instance = %d, want FENCED_INSTANCE_ID (82)", code)
	}
	if code := hb(s1.memberID, nil); code != kerr.UnknownMemberID.Code {
		t.Errorf("the old id's heartbeat with no instance = %d, want UNKNOWN_MEMBER_ID (25)", code)
	}
	if code := hb(s2.memberID, &instance); code != 0 && code != kerr.RebalanceInProgress.Code {
		t.Errorf("the new id's heartbeat = %d", code)
	}
	// Its SyncGroup and its JoinGroup are fenced too, the join with no member id in the
	// answer (recorded: sd-s1-sync-old-id, sd-s1-join-old-id); a commit is from a member
	// nobody knows (sd-s1-commit-old-id).
	oldSync := kmsg.NewPtrSyncGroupRequest()
	oldSync.Group, oldSync.MemberID, oldSync.Generation, oldSync.InstanceID = "g", s1.memberID, s2.generation, &instance
	if _, code, _ := r.b.cg848.classicSync(oldSync); code != kerr.FencedInstanceID.Code {
		t.Errorf("the old id's SyncGroup = %d, want FENCED_INSTANCE_ID (82)", code)
	}
	if res := r.seatJoin(t, "g", s1.memberID, &instance, subscriptionBytes("a")); res.errorCode != kerr.FencedInstanceID.Code || res.memberID != "" {
		t.Errorf("the old id's JoinGroup = %+v, want FENCED_INSTANCE_ID (82) and no member id", res)
	}
	if code := r.b.cg848.validateCommit("g", s1.memberID, s2.generation); code != kerr.UnknownMemberID.Code {
		t.Errorf("the old id's commit = %d, want UNKNOWN_MEMBER_ID (25)", code)
	}
	if got := r.members("g"); len(got) != 2 || !got[s2.memberID] {
		t.Fatalf("members after the old incarnation's requests: %v", got)
	}

	// It leaves by its instance id, as an admin tool removes a static member.
	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group = "g"
	lm := kmsg.NewLeaveGroupRequestMember()
	lm.InstanceID = &instance
	leave.Members = append(leave.Members, lm)
	if res, handled := r.b.cg848.classicLeave(leave); !handled || len(res) != 1 || res[0].errorCode != 0 {
		t.Fatalf("leave by instance id: %+v", res)
	}
	if got := r.members("g"); len(got) != 1 {
		t.Fatalf("members after the instance left: %v", got)
	}
}

// A classic static member takes the place of a consumer-protocol member holding its
// instance id, and a group left with only classic members is a classic group (recorded:
// sh-classic-join-same-instance, sh-describe).
func TestAStaticClassicMemberReplacesAConsumerProtocolHolderOfItsInstance(t *testing.T) {
	shortSessions(t)
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	instance := "j1"
	hb := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hb.Group, hb.MemberID, hb.InstanceID, hb.RebalanceTimeoutMillis = "g", "n1", &instance, 60_000
	hb.SubscribedTopicNames = []string{"a"}
	hb.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	held := r.b.cg848.heartbeat(hb, clientRef{})
	if held.errorCode != 0 {
		t.Fatalf("static consumer-protocol join: %+v", held)
	}

	s := r.seatJoin(t, "g", "", &instance, subscriptionBytes("a"))
	if s.errorCode != 0 || s.memberID == "" || s.generation != held.memberEpoch {
		t.Fatalf("the classic join with the held instance: %+v, want it joined at generation %d", s, held.memberEpoch)
	}
	if r.b.cg848.hasMembers("g") || r.b.claims.ownerOf("g") != protoClassic {
		t.Fatalf("the group is still a consumer group (owner %q)", r.b.claims.ownerOf("g"))
	}
	d, ok := r.b.groups.describe("g")
	if !ok || len(d.members) != 1 || d.members[0].id != s.memberID {
		t.Fatalf("the classic group's members: %+v", d.members)
	}
}
