package broker

import (
	"context"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// A classic group upgraded in the middle of a rebalance. What a real broker answers the
// requests it had parked, and what becomes of the members that sent them, is recorded in
// conformance/recordings/mixed-protocol-parked.json.

// parkedJoin sends a member's JoinGroup to the classic coordinator and returns once it is
// parked there.
func (r *expiryRig) parkedJoin(t *testing.T, group, member string) <-chan joinResult {
	t.Helper()
	out := make(chan joinResult, 1)
	go func() {
		res, _ := r.b.groups.join(context.Background(), joinReqSeat(group, member, rigClassicSession, rigClassicSession, "consumer", subscriptionBytes("a")), clientRef{"c", "h"})
		out <- res
	}()
	r.waitFor(t, "the JoinGroup to be parked", func() bool {
		r.b.groups.mu.Lock()
		defer r.b.groups.mu.Unlock()
		g := r.b.groups.groups[group]
		return g != nil && g.members[member] != nil && g.members[member].awaitingJoin
	})
	return out
}

func (r *expiryRig) waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !done(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func answer[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("%s was not answered", what)
		panic("unreachable")
	}
}

// newClassicID is the first half of a v7 join: the id the coordinator hands out.
func (r *expiryRig) newClassicID(t *testing.T, group string) string {
	t.Helper()
	first, err := r.b.groups.join(context.Background(), joinReqSeat(group, "", rigClassicSession, rigClassicSession, "consumer", subscriptionBytes("a")), clientRef{"c", "h"})
	if err != nil || first.errorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("first join: %v %+v", err, first)
	}
	return first.memberID
}

// A member parked in JoinGroup when a consumer-protocol member upgrades the group is told
// REBALANCE_IN_PROGRESS and keeps its id (recorded: pj-b-join-parked), and that id is a
// member of the consumer group, with nothing assigned, which its next JoinGroup finds
// (pj-describe, pj-b-rejoin). It used to be told UNKNOWN_MEMBER_ID, which makes a client
// drop its id, while the id kept a seat and a share of the partitions until its session
// ran out.
func TestUpgradeAnswersAParkedJoinRebalanceInProgressAndKeepsItsSeat(t *testing.T) {
	shortSessions(t)
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	r.withPartitions(t, "a", 4)
	a, _ := r.holding(t, "g", "a", rigClassicSession, rigClassicSession, 0, 1, 2, 3)
	b := r.newClassicID(t, "g")
	parked := r.parkedJoin(t, "g", b)

	n := r.join848(t, "g", "n", "a")
	got := answer(t, "the parked JoinGroup", parked)
	if got.errorCode != kerr.RebalanceInProgress.Code || got.memberID != b {
		t.Fatalf("the parked JoinGroup was answered code %d member %q, want REBALANCE_IN_PROGRESS (27) and its own id %q", got.errorCode, got.memberID, b)
	}
	members := r.members("g")
	if !members[a] || !members[b] || len(members) != 3 {
		t.Fatalf("members of the consumer group (true: classic) = %v, want A, B and the new member", members)
	}
	r.b.cg848.mu.Lock()
	seat := r.b.cg848.groups["g"].members[b]
	held, epoch := len(seat.assigned), seat.epoch
	r.b.cg848.mu.Unlock()
	if held != 0 || epoch != 1 {
		t.Fatalf("B's seat holds %d partitions at epoch %d, want none at the classic group's generation 1", held, epoch)
	}
	// What the client does with 27 is join again, with the id it still has.
	again := r.classicJoinSeat(t, "g", b, rigClassicSession, rigClassicSession, "a")
	if again.errorCode != 0 || again.memberID != b || again.generation != n.memberEpoch {
		t.Fatalf("B's JoinGroup after the upgrade: %+v, want its id at generation %d", again, n.memberEpoch)
	}
	if got := r.members("g"); len(got) != 3 {
		t.Fatalf("B was seated twice: %v", got)
	}
}

// A group upgraded between its JoinGroups and its SyncGroups: the parked SyncGroup and the
// leader's, when it comes, are REBALANCE_IN_PROGRESS (recorded: ps-b-sync-parked,
// ps-a-leader-sync), and each member has in the consumer group what it held in the
// generation before, which the new generation had not replaced yet (ps-describe).
func TestUpgradeBetweenJoinAndSyncKeepsWhatTheMembersHeld(t *testing.T) {
	shortSessions(t)
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	topic := r.withPartitions(t, "a", 4)
	a, _ := r.holding(t, "g", "a", rigClassicSession, rigClassicSession, 0, 1, 2, 3)
	b := r.newClassicID(t, "g")
	parkedB := r.parkedJoin(t, "g", b)
	ja, err := r.b.groups.join(context.Background(), joinReqSeat("g", a, rigClassicSession, rigClassicSession, "consumer", subscriptionBytes("a")), clientRef{"c", "h"})
	if err != nil || ja.errorCode != 0 || ja.generation != 2 {
		t.Fatalf("A's rejoin: %v %+v", err, ja)
	}
	if jb := answer(t, "B's JoinGroup", parkedB); jb.errorCode != 0 || jb.generation != 2 {
		t.Fatalf("B's join: %+v", jb)
	}

	syncOf := func(member string) *kmsg.SyncGroupRequest {
		s := kmsg.NewPtrSyncGroupRequest()
		s.Group, s.MemberID, s.Generation = "g", member, 2
		return s
	}
	parkedSync := make(chan syncResult, 1)
	go func() {
		res, _ := r.b.groups.sync(context.Background(), syncOf(b))
		parkedSync <- res
	}()
	r.waitFor(t, "the SyncGroup to be parked", func() bool {
		r.b.groups.mu.Lock()
		defer r.b.groups.mu.Unlock()
		_, ok := r.b.groups.groups["g"].pendingSync[b]
		return ok
	})

	n := r.join848(t, "g", "n", "a")
	if got := answer(t, "the parked SyncGroup", parkedSync); got.errorCode != kerr.RebalanceInProgress.Code {
		t.Fatalf("the parked SyncGroup was answered %d, want REBALANCE_IN_PROGRESS (27)", got.errorCode)
	}
	if _, code, handled := r.b.cg848.classicSync(syncOf(a)); !handled || code != kerr.RebalanceInProgress.Code {
		t.Fatalf("the leader's SyncGroup after the upgrade: handled %v code %d, want REBALANCE_IN_PROGRESS (27)", handled, code)
	}

	r.b.cg848.mu.Lock()
	heldA, heldB := len(r.b.cg848.groups["g"].members[a].assigned), len(r.b.cg848.groups["g"].members[b].assigned)
	r.b.cg848.mu.Unlock()
	if heldA != 4 || heldB != 0 {
		t.Fatalf("after the upgrade A holds %d partitions and B %d, want the 4 A had in the generation before, and none", heldA, heldB)
	}
	// So the new member is given nothing A may still be reading.
	if res := r.hb848("g", "n", n.memberEpoch, map[[16]byte][]int32{topic: {}}); res.assignment != nil && len(res.assignment.Topics) != 0 {
		t.Fatalf("the consumer-protocol member was handed partitions the classic member has not given up: %+v", res.assignment)
	}
}
