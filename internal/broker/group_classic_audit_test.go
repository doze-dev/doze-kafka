package broker_test

import (
	"context"
	"github.com/doze-dev/doze-kafka/internal/broker"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Tests added by the October audit for the classic coordinator. Raw requests, so
// the exact code and timing are asserted.

const (
	codeInconsistentProtocol = int16(23)
	codeFencedInstance       = int16(82)
)

func twoClients(t *testing.T) (context.Context, *kgo.Client, *kgo.Client) {
	t.Helper()
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	t.Cleanup(cancel)
	return ctx, newClient(t, addr), newClient(t, addr)
}

// A member waiting in JoinGroup or SyncGroup cannot heartbeat. Its session timer
// must not evict it while it waits: the rebalance may legitimately take up to the
// rebalance timeout, which is normally the longer of the two.
func TestGroupClassic_ParkedMemberIsNotEvictedByItsOwnSession(t *testing.T) {
	t.Cleanup(broker.SetMinSessionTimeoutForTest(0)) // B's 1.5 s session is below Kafka's 6 s minimum; the test is about the timer.
	ctx, A, B := twoClients(t)
	const g = "parked-not-evicted"
	ida, _ := stableGroupOfOne(t, ctx, A, g, testSessionMs, testRebalanceMsDefault)

	// B joins with a 1.5s session. A does not rejoin for 3s (it is "busy").
	idb := acquireID(t, ctx, B, g)
	jB := joinAsync(ctx, B, g, idb, 1500, testRebalanceMsDefault)
	select {
	case o := <-jB:
		t.Fatalf("B's parked JoinGroup was answered after %v with code %d; it should wait for the rebalance", o.took.Round(time.Millisecond), o.resp.ErrorCode)
	case <-time.After(3 * time.Second):
	}
	jA := joinAsync(ctx, A, g, ida, testSessionMs, testRebalanceMsDefault)
	rA := mustJoin(t, await(t, jA, 5*time.Second, "A join"), "A join")
	rB := mustJoin(t, await(t, jB, 5*time.Second, "B join"), "B join")
	if rB.MemberID != idb || rB.Generation != rA.Generation {
		t.Fatalf("B joined as %q at generation %d, A at %d", rB.MemberID, rB.Generation, rA.Generation)
	}

	// The same while B waits in SyncGroup for a slow leader.
	sB := syncAsync(ctx, B, g, idb, rB.Generation, nil)
	select {
	case o := <-sB:
		t.Fatalf("B's parked SyncGroup was answered after %v with code %d", o.took.Round(time.Millisecond), o.resp.ErrorCode)
	case <-time.After(3 * time.Second):
	}
	await(t, syncAsync(ctx, A, g, ida, rA.Generation, map[string]string{ida: "a", idb: "b"}), 5*time.Second, "leader sync")
	if o := await(t, sB, 5*time.Second, "B sync"); o.err != nil || o.resp.ErrorCode != codeNone || string(o.resp.MemberAssignment) != "b" {
		t.Fatalf("B's sync: %+v %v, want its assignment", o.resp, o.err)
	}
}

// Members of a new group that arrive together form one generation. Without the
// initial delay the first formed generation 1 alone within microseconds and every
// other member forced another rebalance.
func TestGroupClassic_MembersStartingTogetherFormOneGeneration(t *testing.T) {
	ctx, A, B := twoClients(t)
	const g = "start-together"
	ida, idb := acquireID(t, ctx, A, g), acquireID(t, ctx, B, g)
	jA := joinAsync(ctx, A, g, ida, testSessionMs, testRebalanceMsDefault)
	time.Sleep(50 * time.Millisecond)
	jB := joinAsync(ctx, B, g, idb, testSessionMs, testRebalanceMsDefault)
	rA := mustJoin(t, await(t, jA, 5*time.Second, "A join"), "A join")
	rB := mustJoin(t, await(t, jB, 5*time.Second, "B join"), "B join")
	if rA.Generation != 1 || rB.Generation != 1 {
		t.Fatalf("generations %d and %d, want both members in generation 1", rA.Generation, rB.Generation)
	}
	leader := rA
	if rB.LeaderID == idb {
		leader = rB
	}
	if len(leader.Members) != 2 {
		t.Errorf("the leader of generation 1 sees %d member(s), want 2", len(leader.Members))
	}
}

func TestGroupClassic_InconsistentProtocolsAreRefused(t *testing.T) {
	ctx, A, B := twoClients(t)
	const g = "protocols"
	stableGroupOfOne(t, ctx, A, g, testSessionMs, testRebalanceMsDefault) // consumer / range.

	idb := acquireID(t, ctx, B, g)
	try := func(mut func(*kmsg.JoinGroupRequest)) int16 {
		r := joinRequest(g, idb, testSessionMs, testRebalanceMsDefault)
		mut(r)
		resp, err := r.RequestWith(ctx, B)
		if err != nil {
			t.Fatal(err)
		}
		return resp.ErrorCode
	}
	if c := try(func(r *kmsg.JoinGroupRequest) { r.ProtocolType = "connect" }); c != codeInconsistentProtocol {
		t.Errorf("a connect member joining a consumer group: code %d, want INCONSISTENT_GROUP_PROTOCOL (23)", c)
	}
	if c := try(func(r *kmsg.JoinGroupRequest) { r.Protocols[0].Name = "sticky" }); c != codeInconsistentProtocol {
		t.Errorf("no protocol in common: code %d, want 23", c)
	}
	if c := try(func(r *kmsg.JoinGroupRequest) { r.Protocols = nil }); c != codeInconsistentProtocol {
		t.Errorf("no protocols at all: code %d, want 23", c)
	}

	// The protocol type is the group's, not always "consumer".
	r := joinRequest("workers", "", testSessionMs, testRebalanceMsDefault)
	r.ProtocolType = "connect"
	r.InstanceID = kmsg.StringPtr("w1") // static: joins in one step.
	if resp, err := r.RequestWith(ctx, B); err != nil || resp.ErrorCode != codeNone {
		t.Fatalf("connect group: %v %+v", err, resp)
	}
	lg := kmsg.NewPtrListGroupsRequest()
	resp, err := lg.RequestWith(ctx, A)
	if err != nil {
		t.Fatal(err)
	}
	for _, grp := range resp.Groups {
		if want := map[string]string{"workers": "connect", g: "consumer"}[grp.Group]; want != "" && grp.ProtocolType != want {
			t.Errorf("ListGroups: %s has protocol type %q, want %q", grp.Group, grp.ProtocolType, want)
		}
	}
}

// Naming a member that is not in the group changes nothing. It used to start a
// rebalance of a healthy group.
func TestGroupClassic_LeaveOfAnUnknownMemberDoesNotRebalance(t *testing.T) {
	ctx, A, B := twoClients(t)
	const g = "leave-unknown"
	ida, gen := stableGroupOfOne(t, ctx, A, g, testSessionMs, testRebalanceMsDefault)
	leaveGroup(t, ctx, B, g, "not-a-member")
	if c := heartbeatCode(t, ctx, A, g, ida, gen); c != codeNone {
		t.Fatalf("after a LeaveGroup for an unknown member the live member's heartbeat is %d, want 0", c)
	}
	if st := listedGroups(t, ctx, A)[g]; st != "Stable" {
		t.Errorf("the group is %q, want Stable", st)
	}
}

func commitAs(t *testing.T, ctx context.Context, cl *kgo.Client, group, member string, gen int32, offset int64, metadata string) int16 {
	t.Helper()
	oc := kmsg.NewPtrOffsetCommitRequest()
	oc.Group, oc.MemberID, oc.Generation = group, member, gen
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = "t"
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Offset = offset
	if metadata != "" {
		rp.Metadata = kmsg.StringPtr(metadata)
	}
	rt.Partitions = append(rt.Partitions, rp)
	oc.Topics = append(oc.Topics, rt)
	resp, err := oc.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	return resp.Topics[0].Partitions[0].ErrorCode
}

// Who may commit a group's offsets. The check was skipped for a negative
// generation and for a group with no members, so anything could overwrite a
// running group's offsets and a member that had left could still commit.
func TestGroupClassic_OffsetCommitIsFenced(t *testing.T) {
	ctx, A, B := twoClients(t)
	mustCreateTopic(t, ctx, A, "t", 1)
	const g = "fenced-commits"

	if c := commitAs(t, ctx, B, "no-such-group", "", -1, 1, ""); c != codeNone {
		t.Errorf("a simple consumer's commit to a new group id: code %d, want 0", c)
	}
	if c := commitAs(t, ctx, B, "another-new-group", "m", 7, 1, ""); c != codeIllegalGeneration {
		t.Errorf("generation 7 for a group that does not exist: code %d, want ILLEGAL_GENERATION (22)", c)
	}

	ida, gen := stableGroupOfOne(t, ctx, A, g, testSessionMs, testRebalanceMsDefault)
	for _, tc := range []struct {
		name   string
		member string
		gen    int32
		want   int16
	}{
		{"the member at its generation", ida, gen, codeNone},
		{"nobody, generation -1, into a live group", "", -1, codeUnknownMember},
		{"an unknown member, generation -1", "ghost", -1, codeUnknownMember},
		{"the member, generation -1", ida, -1, codeIllegalGeneration},
		{"the member, an old generation", ida, gen - 1, codeIllegalGeneration},
		{"an unknown member at the current generation", "ghost", gen, codeUnknownMember},
	} {
		if c := commitAs(t, ctx, B, g, tc.member, tc.gen, 5, ""); c != tc.want {
			t.Errorf("%s: code %d, want %d", tc.name, c, tc.want)
		}
	}

	// Waiting for the leader's assignment: not yet.
	idb := acquireID(t, ctx, B, g)
	jB := joinAsync(ctx, B, g, idb, testSessionMs, testRebalanceMsDefault)
	time.Sleep(100 * time.Millisecond)
	rA := mustJoin(t, await(t, joinAsync(ctx, A, g, ida, testSessionMs, testRebalanceMsDefault), 5*time.Second, "A"), "A")
	mustJoin(t, await(t, jB, 5*time.Second, "B"), "B")
	if c := commitAs(t, ctx, A, g, ida, rA.Generation, 6, ""); c != codeRebalanceInProg {
		t.Errorf("a commit while the group awaits its assignment: code %d, want REBALANCE_IN_PROGRESS (27)", c)
	}

	// A member that has left can no longer commit, even at its old generation.
	leaveGroup(t, ctx, A, g, ida)
	leaveGroup(t, ctx, B, g, idb)
	if c := commitAs(t, ctx, A, g, ida, rA.Generation, 999, ""); c != codeUnknownMember {
		t.Errorf("a departed member committing at its old generation: code %d, want UNKNOWN_MEMBER_ID (25)", c)
	}
	// With nobody in it, an admin reset (no member, generation -1) is allowed again.
	if c := commitAs(t, ctx, B, g, "", -1, 0, ""); c != codeNone {
		t.Errorf("an offset reset of the now-empty group: code %d, want 0", c)
	}
}

func staticJoin(ctx context.Context, cl *kgo.Client, group, member, instance string) (*kmsg.JoinGroupResponse, error) {
	r := joinRequest(group, member, testSessionMs, testRebalanceMsDefault)
	r.InstanceID = kmsg.StringPtr(instance)
	return r.RequestWith(ctx, cl)
}

// Static membership (group.instance.id): a restarted instance takes its
// predecessor's place without a rebalance, and the predecessor is fenced.
func TestGroupClassic_StaticMembership(t *testing.T) {
	ctx, A, B := twoClients(t)
	const g = "static"

	first, err := staticJoin(ctx, A, g, "", "pod-0")
	if err != nil || first.ErrorCode != codeNone || first.MemberID == "" {
		t.Fatalf("a static member joins in one step: %v %+v", err, first)
	}
	await(t, syncAsync(ctx, A, g, first.MemberID, first.Generation, map[string]string{first.MemberID: "assignment-1"}), 5*time.Second, "sync")

	// The process restarts: same instance id, no member id.
	start := time.Now()
	second, err := staticJoin(ctx, B, g, "", "pod-0")
	if err != nil || second.ErrorCode != codeNone {
		t.Fatalf("the restarted instance: %v %+v", err, second)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("the restart took %v; it should not wait out the old incarnation's session", took.Round(time.Millisecond))
	}
	if second.MemberID == first.MemberID || second.Generation != first.Generation {
		t.Errorf("restart: member %q generation %d, want a new member id at the same generation %d (no rebalance)",
			second.MemberID, second.Generation, first.Generation)
	}
	if o := await(t, syncAsync(ctx, B, g, second.MemberID, second.Generation, nil), 5*time.Second, "sync after restart"); o.err != nil || string(o.resp.MemberAssignment) != "assignment-1" {
		t.Errorf("the restarted instance's assignment is %q, want the one its predecessor held", o.resp.MemberAssignment)
	}

	// The old incarnation is fenced, by name.
	hb := kmsg.NewPtrHeartbeatRequest()
	hb.Group, hb.MemberID, hb.Generation, hb.InstanceID = g, first.MemberID, first.Generation, kmsg.StringPtr("pod-0")
	if resp, err := hb.RequestWith(ctx, A); err != nil || resp.ErrorCode != codeFencedInstance {
		t.Errorf("the old incarnation's heartbeat: %v code %d, want FENCED_INSTANCE_ID (82)", err, resp.ErrorCode)
	}
	if resp, err := staticJoin(ctx, A, g, first.MemberID, "pod-0"); err != nil || resp.ErrorCode != codeFencedInstance {
		t.Errorf("the old incarnation rejoining under its old member id: %v code %d, want 82", err, resp.ErrorCode)
	}

	// Leaving by instance id (the member id need not be known).
	lr := kmsg.NewPtrLeaveGroupRequest()
	lr.Group = g
	lm := kmsg.NewLeaveGroupRequestMember()
	lm.InstanceID = kmsg.StringPtr("pod-0")
	lr.Members = append(lr.Members, lm)
	resp, err := lr.RequestWith(ctx, A)
	if err != nil || len(resp.Members) != 1 || resp.Members[0].ErrorCode != codeNone || resp.Members[0].InstanceID == nil || *resp.Members[0].InstanceID != "pod-0" {
		t.Fatalf("leave by instance id: %v %+v", err, resp)
	}
	if st, listed := listedGroups(t, ctx, A)[g]; listed && st != "Empty" {
		t.Errorf("after the static member left the group is %q", st)
	}
}

// A second SyncGroup from the same follower supersedes the first, which is
// answered rather than left parked for ever.
func TestGroupClassic_DuplicateFollowerSyncAnswersTheFirst(t *testing.T) {
	ctx, A, B := twoClients(t)
	B2 := newClient(t, A.OptValue(kgo.SeedBrokers).([]string)[0])
	const g = "dup-sync"
	ida, _ := stableGroupOfOne(t, ctx, A, g, testSessionMs, testRebalanceMsDefault)
	idb := acquireID(t, ctx, B, g)
	jB := joinAsync(ctx, B, g, idb, testSessionMs, testRebalanceMsDefault)
	time.Sleep(100 * time.Millisecond)
	rA := mustJoin(t, await(t, joinAsync(ctx, A, g, ida, testSessionMs, testRebalanceMsDefault), 5*time.Second, "A"), "A")
	mustJoin(t, await(t, jB, 5*time.Second, "B"), "B")

	first := syncAsync(ctx, B, g, idb, rA.Generation, nil)
	time.Sleep(150 * time.Millisecond)
	second := syncAsync(ctx, B2, g, idb, rA.Generation, nil)
	if o := await(t, first, 3*time.Second, "the superseded sync"); o.err != nil || o.resp.ErrorCode != codeUnknownMember {
		t.Fatalf("the first of two parked syncs: %+v %v, want it answered (UNKNOWN_MEMBER_ID)", o.resp, o.err)
	}
	await(t, syncAsync(ctx, A, g, ida, rA.Generation, map[string]string{ida: "a", idb: "b"}), 5*time.Second, "leader sync")
	if o := await(t, second, 5*time.Second, "the second sync"); o.err != nil || string(o.resp.MemberAssignment) != "b" {
		t.Fatalf("the second sync: %+v %v", o.resp, o.err)
	}
}

// OffsetFetch that names its partitions returns the metadata committed with them.
func TestGroupClassic_OffsetFetchReturnsCommittedMetadata(t *testing.T) {
	ctx, A, _ := twoClients(t)
	mustCreateTopic(t, ctx, A, "t", 2)
	if c := commitAs(t, ctx, A, "md", "", -1, 42, "my-metadata"); c != codeNone {
		t.Fatal(c)
	}
	of := kmsg.NewPtrOffsetFetchRequest()
	of.Group = "md"
	ot := kmsg.NewOffsetFetchRequestTopic()
	ot.Topic, ot.Partitions = "t", []int32{0, 1}
	of.Topics = append(of.Topics, ot)
	rg := kmsg.NewOffsetFetchRequestGroup()
	rg.Group = "md"
	gt := kmsg.NewOffsetFetchRequestGroupTopic()
	gt.Topic, gt.Partitions = "t", []int32{0, 1}
	rg.Topics = append(rg.Topics, gt)
	of.Groups = append(of.Groups, rg)
	resp, err := of.RequestWith(ctx, A)
	if err != nil {
		t.Fatal(err)
	}
	type po struct {
		offset int64
		md     *string
	}
	got := map[int32]po{}
	for _, grp := range resp.Groups {
		for _, tp := range grp.Topics {
			for _, p := range tp.Partitions {
				got[p.Partition] = po{p.Offset, p.Metadata}
			}
		}
	}
	for _, tp := range resp.Topics {
		for _, p := range tp.Partitions {
			got[p.Partition] = po{p.Offset, p.Metadata}
		}
	}
	if p := got[0]; p.offset != 42 || p.md == nil || *p.md != "my-metadata" {
		t.Errorf("partition 0: offset %d metadata %v, want 42 and my-metadata", p.offset, p.md)
	}
	if p := got[1]; p.offset != -1 {
		t.Errorf("partition 1 (never committed): offset %d, want -1", p.offset)
	}
}

// A follower that rejoins a stable group with nothing changed is told where the
// group is, with no rebalance. A member id the coordinator never issued is unknown.
func TestGroupClassic_UnchangedFollowerRejoinAndUnknownIDs(t *testing.T) {
	ctx, A, B := twoClients(t)
	const g = "rejoin"
	ida, idb := acquireID(t, ctx, A, g), acquireID(t, ctx, B, g)
	jA := joinAsync(ctx, A, g, ida, testSessionMs, testRebalanceMsDefault)
	jB := joinAsync(ctx, B, g, idb, testSessionMs, testRebalanceMsDefault)
	rA := mustJoin(t, await(t, jA, 5*time.Second, "A"), "A")
	mustJoin(t, await(t, jB, 5*time.Second, "B"), "B")
	leader, follower, fc := ida, idb, B
	if rA.LeaderID == idb {
		leader, follower, fc = idb, ida, A
	}
	lc := A
	if leader == idb {
		lc = B
	}
	await(t, syncAsync(ctx, lc, g, leader, rA.Generation, map[string]string{ida: "a", idb: "b"}), 5*time.Second, "leader sync")

	o := await(t, joinAsync(ctx, fc, g, follower, testSessionMs, testRebalanceMsDefault), 3*time.Second, "follower rejoin")
	if o.err != nil || o.resp.ErrorCode != codeNone || o.resp.Generation != rA.Generation {
		t.Fatalf("an unchanged follower rejoining a stable group: %+v %v, want generation %d at once", o.resp, o.err, rA.Generation)
	}
	if c := heartbeatCode(t, ctx, lc, g, leader, rA.Generation); c != codeNone {
		t.Errorf("the leader's heartbeat after that rejoin is %d: the group was rebalanced for nothing", c)
	}

	if resp, err := joinRequest(g, "made-up-id", testSessionMs, testRebalanceMsDefault).RequestWith(ctx, fc); err != nil || resp.ErrorCode != codeUnknownMember {
		t.Errorf("a member id the coordinator never issued: %v code %d, want UNKNOWN_MEMBER_ID (25)", err, resp.ErrorCode)
	}
	// An issued id is good for exactly one join.
	fresh := acquireID(t, ctx, fc, "other-group")
	if resp, err := joinRequest(g, fresh, testSessionMs, testRebalanceMsDefault).RequestWith(ctx, fc); err != nil || resp.ErrorCode != codeUnknownMember {
		t.Errorf("an id issued for another group: %v code %d, want 25", err, resp.ErrorCode)
	}
}

// A member removed while its SyncGroup is parked is told so.
func TestGroupClassic_RemovedMembersParkedSyncIsAnswered(t *testing.T) {
	ctx, A, B := twoClients(t)
	const g = "removed-sync"
	ida, _ := stableGroupOfOne(t, ctx, A, g, testSessionMs, testRebalanceMsDefault)
	idb := acquireID(t, ctx, B, g)
	jB := joinAsync(ctx, B, g, idb, testSessionMs, testRebalanceMsDefault)
	time.Sleep(100 * time.Millisecond)
	rA := mustJoin(t, await(t, joinAsync(ctx, A, g, ida, testSessionMs, testRebalanceMsDefault), 5*time.Second, "A"), "A")
	mustJoin(t, await(t, jB, 5*time.Second, "B"), "B")

	sB := syncAsync(ctx, B, g, idb, rA.Generation, nil)
	time.Sleep(150 * time.Millisecond)
	leaveGroup(t, ctx, A, g, idb) // B is removed (by an admin, say) while parked.
	if o := await(t, sB, 3*time.Second, "the removed member's sync"); o.err != nil || o.resp.ErrorCode != codeUnknownMember {
		t.Fatalf("the removed member's parked sync: %+v %v, want UNKNOWN_MEMBER_ID", o.resp, o.err)
	}
}
