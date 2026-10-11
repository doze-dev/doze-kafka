package broker_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Tests added by the October audit for the KIP-848 coordinator. Each plays the
// members by hand, one heartbeat at a time.

func setup848(t *testing.T, session time.Duration, topics map[string]int32) (context.Context, string, *kgo.Client, map[string][16]byte) {
	t.Helper()
	addr := broker848(t, session)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	t.Cleanup(cancel)
	admin := newClient(t, addr)
	ids := map[string][16]byte{}
	for name, n := range topics {
		mustCreateTopic(t, ctx, admin, name, n)
		ids[name] = topicIDOf(t, ctx, admin, name)
	}
	return ctx, addr, admin, ids
}

// The handover. B must not be given a partition until A has reported that it no
// longer owns it. Sending A the smaller assignment is not enough: A may still be
// consuming those partitions when its next heartbeat goes out.
func TestGroupKIP848_RevokedPartitionsWaitForTheOwnersAcknowledgement(t *testing.T) {
	ctx, addr, _, ids := setup848(t, 0, map[string]int32{"t": 4})
	tid := ids["t"]
	a, b := new848(t, addr, "handover", "member-a"), new848(t, addr, "handover", "member-b")

	a.join(t, ctx, []string{"t"})
	a.reconcile(true)
	a.hb(t, ctx, false) // A reports [0 1 2 3].
	if got := a.ownedOf(tid); !slices.Equal(got, []int32{0, 1, 2, 3}) {
		t.Fatalf("a lone member should own every partition, got %v", got)
	}
	b.join(t, ctx, []string{"t"})

	// A heartbeats without a report (it has nothing new to say) and is told to
	// shrink. It has NOT applied that yet: it still holds all four.
	a.hb(t, ctx, false)
	for i := 0; i < 3; i++ {
		b.hb(t, ctx, true)
		b.reconcile(true)
		if got := b.ownedOf(tid); len(got) > 0 {
			t.Fatalf("B was given %v while A (which holds %v) has not acknowledged giving anything up", got, a.ownedOf(tid))
		}
		a.hb(t, ctx, false) // still no report from A.
	}

	// A applies the smaller assignment and reports it. Now B may have the rest.
	a.reconcile(true)
	a.hb(t, ctx, false)
	b.hb(t, ctx, true)
	b.reconcile(true)
	if len(a.ownedOf(tid)) != 2 || len(b.ownedOf(tid)) != 2 || doubleOwned(a, b) != "" {
		t.Fatalf("after A acknowledged: A=%v B=%v, want two each and nothing shared", a.ownedOf(tid), b.ownedOf(tid))
	}
}

// A member that never acknowledges a revocation is removed when its rebalance
// timeout runs out, and its partitions go to their new owner. Without that, one
// stuck member holds the group's partitions hostage for ever.
func TestGroupKIP848_MemberThatNeverAcknowledgesIsFenced(t *testing.T) {
	ctx, addr, _, ids := setup848(t, 0, map[string]int32{"t": 4})
	tid := ids["t"]
	a, b := new848(t, addr, "stuck", "member-a"), new848(t, addr, "stuck", "member-b")
	short := func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.RebalanceTimeoutMillis = 1000 }

	a.absorb(a.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) {
		short(r)
		r.SubscribedTopicNames = []string{"t"}
		r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	}))
	a.reconcile(true)
	a.hb(t, ctx, false)
	b.join(t, ctx, []string{"t"})

	var fenced int16
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) && fenced == 0 {
		r := a.raw(t, ctx, short) // heartbeating, but stuck: never reports.
		fenced = r.ErrorCode
		time.Sleep(200 * time.Millisecond)
	}
	if fenced != 25 && fenced != 110 {
		t.Fatalf("a member that ignored a revocation for 6s with a 1s rebalance timeout: last code %d, want it removed", fenced)
	}
	for i := 0; i < 3; i++ {
		b.hb(t, ctx, true)
		b.reconcile(true)
	}
	if got := b.ownedOf(tid); !slices.Equal(got, []int32{0, 1, 2, 3}) {
		t.Fatalf("after the stuck member was removed B owns %v, want all four", got)
	}
}

// Claiming partitions does not make them yours: ownership is the coordinator's
// record. A member reporting partitions it was never assigned used to be believed,
// which left the group unable to finish reconciling.
func TestGroupKIP848_OwnershipIsNotTakenFromReports(t *testing.T) {
	ctx, addr, admin, ids := setup848(t, 0, map[string]int32{"t": 4})
	tid := ids["t"]
	a, b := new848(t, addr, "claims", "member-a"), new848(t, addr, "claims", "member-b")
	settle := func() {
		for i := 0; i < 4; i++ {
			for _, m := range []*member848{a, b} {
				m.hb(t, ctx, true)
				m.reconcile(true)
			}
		}
	}
	a.join(t, ctx, []string{"t"})
	a.reconcile(true)
	b.join(t, ctx, []string{"t"})
	settle()

	// B lies: it reports owning everything.
	b.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) {
		rt := kmsg.NewConsumerGroupHeartbeatRequestTopic()
		rt.TopicID, rt.Partitions = tid, []int32{0, 1, 2, 3}
		r.Topics = append(r.Topics, rt)
	})
	settle()
	if g := describe848(t, ctx, admin, "claims"); g.State != "Stable" {
		t.Errorf("after a member claimed partitions it was not assigned the group is %q, want Stable", g.State)
	}
	if len(a.ownedOf(tid)) != 2 || len(b.ownedOf(tid)) != 2 || doubleOwned(a, b) != "" {
		t.Errorf("A=%v B=%v, want the two-each split undisturbed", a.ownedOf(tid), b.ownedOf(tid))
	}
}

// A topic deleted and re-created is a different topic (a new id). The member is
// told to give up the old one and is given the new one. Keyed by name, nothing
// appeared to change and it was told nothing.
func TestGroupKIP848_RecreatedTopicIsReassignedByItsNewID(t *testing.T) {
	ctx, addr, admin, ids := setup848(t, 0, map[string]int32{"t": 2})
	oldID := ids["t"]
	a := new848(t, addr, "recreate", "member-a")
	a.join(t, ctx, []string{"t"})
	a.reconcile(true)
	a.hb(t, ctx, false)

	deleteTopic848(t, ctx, admin, "t")
	mustCreateTopic(t, ctx, admin, "t", 2)
	newID := topicIDOf(t, ctx, admin, "t")
	if newID == oldID {
		t.Fatal("test setup: the re-created topic kept its id")
	}
	for i := 0; i < 5; i++ {
		a.hb(t, ctx, i%2 == 0)
		a.reconcile(true)
	}
	if old, cur := a.ownedOf(oldID), a.ownedOf(newID); len(old) != 0 || !slices.Equal(cur, []int32{0, 1}) {
		t.Fatalf("after delete and re-create the member holds old id %v, new id %v; want none and [0 1]", old, cur)
	}
	if g := describe848(t, ctx, admin, "recreate"); g.State != "Stable" {
		t.Errorf("the group is %q, want Stable", g.State)
	}
}

func TestGroupKIP848_RequestsAreValidated(t *testing.T) {
	ctx, addr, _, _ := setup848(t, 0, map[string]int32{"t": 2})
	code := func(group, id string, mut func(*kmsg.ConsumerGroupHeartbeatRequest)) int16 {
		m := new848(t, addr, group, id)
		m.assign = ""
		return m.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) {
			r.SubscribedTopicNames = []string{"t"}
			mut(r)
		}).ErrorCode
	}
	for _, tc := range []struct {
		name string
		mut  func(*kmsg.ConsumerGroupHeartbeatRequest)
		want int16
	}{
		{"a valid join", func(*kmsg.ConsumerGroupHeartbeatRequest) {}, 0},
		{"an assignor that is not offered", func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.ServerAssignor = kmsg.StringPtr("bogus") }, 112},
		{"no subscription", func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.SubscribedTopicNames = nil }, 42},
		{"no rebalance timeout", func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.RebalanceTimeoutMillis = -1 }, 42},
		{"no group id", func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.Group = "" }, 42},
		{"an epoch below -2", func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.MemberEpoch = -3 }, 42},
		{"a pattern that does not compile", func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.SubscribedTopicRegex = kmsg.StringPtr("(") }, 128}, // INVALID_REGULAR_EXPRESSION. (It was answered as 117, UNKNOWN_SUBSCRIPTION_ID, from a mistyped constant.)
		{"an unknown member at a later epoch", func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.MemberEpoch = 3 }, 25},
		{"leaving a group that does not exist", func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.MemberEpoch = -1 }, 69},
	} {
		if got := code("validate-"+tc.name, "m", tc.mut); got != tc.want {
			t.Errorf("%s: code %d, want %d", tc.name, got, tc.want)
		}
	}
	// A stale epoch on a live member is fenced; an unknown member of a live group is unknown.
	a := new848(t, addr, "validate-live", "member-a")
	a.join(t, ctx, []string{"t"})
	if c := a.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.MemberEpoch = a.epoch + 5 }).ErrorCode; c != 110 {
		t.Errorf("a member at an epoch it never reached: code %d, want FENCED_MEMBER_EPOCH (110)", c)
	}
	ghost := new848(t, addr, "validate-live", "ghost")
	if c := ghost.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.MemberEpoch = 1 }).ErrorCode; c != 25 {
		t.Errorf("an unknown member of a live group: code %d, want UNKNOWN_MEMBER_ID (25)", c)
	}
	if c := ghost.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.MemberEpoch = -1 }).ErrorCode; c != 25 {
		t.Errorf("an unknown member leaving: code %d, want 25", c)
	}
}

// A regex subscription (v1) is resolved against the topics that exist, including
// ones created later. It used to be accepted and ignored: the member got nothing.
func TestGroupKIP848_RegexSubscription(t *testing.T) {
	ctx, addr, admin, ids := setup848(t, 0, map[string]int32{"orders.eu": 2, "orders.us": 1, "payments": 3})
	a := new848(t, addr, "regex", "member-a")
	a.assign = ""
	a.absorb(a.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) {
		r.SubscribedTopicRegex = kmsg.StringPtr(`^orders\..*`)
		r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	}))
	a.reconcile(true)
	if len(a.ownedOf(ids["orders.eu"])) != 2 || len(a.ownedOf(ids["orders.us"])) != 1 || len(a.ownedOf(ids["payments"])) != 0 {
		t.Fatalf("a member subscribed to ^orders\\..* owns eu=%v us=%v payments=%v", a.ownedOf(ids["orders.eu"]), a.ownedOf(ids["orders.us"]), a.ownedOf(ids["payments"]))
	}
	mustCreateTopic(t, ctx, admin, "orders.apac", 2)
	apac := topicIDOf(t, ctx, admin, "orders.apac")
	for i := 0; i < 3; i++ {
		a.hb(t, ctx, true)
		a.reconcile(true)
	}
	if got := a.ownedOf(apac); len(got) != 2 {
		t.Errorf("a topic created later that matches the pattern: owned %v, want both partitions", got)
	}
	g := describe848(t, ctx, admin, "regex")
	if len(g.Members) != 1 || g.Members[0].SubscribedTopicRegex == nil || *g.Members[0].SubscribedTopicRegex != `^orders\..*` {
		t.Errorf("ConsumerGroupDescribe does not show the pattern: %+v", g.Members)
	}
}

// Static membership: a member that leaves with epoch -2 keeps its place and its
// partitions until its session expires, so its restarted self resumes without a
// rebalance; while it is still live, a second claimant is refused.
func TestGroupKIP848_StaticMembership(t *testing.T) {
	ctx, addr, _, ids := setup848(t, 0, map[string]int32{"t": 4})
	tid := ids["t"]
	a, b := new848(t, addr, "static848", "member-a"), new848(t, addr, "static848", "member-b")
	a.inst, b.inst = kmsg.StringPtr("pod-0"), kmsg.StringPtr("pod-1")
	a.join(t, ctx, []string{"t"})
	a.reconcile(true)
	b.join(t, ctx, []string{"t"})
	for i := 0; i < 4; i++ {
		for _, m := range []*member848{a, b} {
			m.hb(t, ctx, true)
			m.reconcile(true)
		}
	}
	held := a.ownedOf(tid)
	if len(held) != 2 {
		t.Fatalf("setup: A owns %v", held)
	}

	// A second process claiming pod-0 while it is live.
	dup := new848(t, addr, "static848", "member-dup")
	dup.inst = kmsg.StringPtr("pod-0")
	if resp := dup.join(t, ctx, []string{"t"}); resp.ErrorCode != 111 {
		t.Errorf("a second member with a live instance id: code %d, want UNRELEASED_INSTANCE_ID (111)", resp.ErrorCode)
	}

	// pod-0 goes away for a restart, and comes back under a new member id.
	if resp := a.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.MemberEpoch = -2 }); resp.ErrorCode != 0 {
		t.Fatalf("static leave: code %d", resp.ErrorCode)
	}
	bEpoch := b.epoch
	b.hb(t, ctx, true)
	b.reconcile(true)
	if b.epoch != bEpoch || len(b.ownedOf(tid)) != 2 {
		t.Errorf("the other member was disturbed by a static member's restart: epoch %d -> %d, owns %v", bEpoch, b.epoch, b.ownedOf(tid))
	}
	a2 := new848(t, addr, "static848", "member-a-restarted")
	a2.inst = kmsg.StringPtr("pod-0")
	if resp := a2.join(t, ctx, []string{"t"}); resp.ErrorCode != 0 {
		t.Fatalf("the restarted instance: code %d", resp.ErrorCode)
	}
	a2.reconcile(true)
	if got := a2.ownedOf(tid); !slices.Equal(got, held) {
		t.Errorf("the restarted instance owns %v, want the %v its predecessor held", got, held)
	}
}

// The uniform assignor balances across all topics and moves as little as it can.
func TestGroupKIP848_UniformAssignorIsBalancedAndSticky(t *testing.T) {
	topics := map[string]int32{"wide": 3}
	for i := 0; i < 6; i++ {
		topics[fmt.Sprintf("single-%d", i)] = 1
	}
	ctx, addr, admin, _ := setup848(t, 0, topics)
	names := make([]string, 0, len(topics))
	for n := range topics {
		names = append(names, n)
	}
	ms := []*member848{new848(t, addr, "balance", "m0"), new848(t, addr, "balance", "m1"), new848(t, addr, "balance", "m2")}
	settle := func(members []*member848) {
		for i := 0; i < 6; i++ {
			for _, m := range members {
				m.hb(t, ctx, true)
				m.reconcile(true)
			}
		}
	}
	for _, m := range ms {
		m.join(t, ctx, names)
		m.reconcile(true)
	}
	settle(ms)
	for _, m := range ms {
		if n := len(m.owned); n != 3 {
			t.Errorf("%s owns %d of 9 partitions across 7 topics, want 3 (it was 7/1/1 when each topic was dealt on its own)", m.id, n)
		}
	}
	if g := describe848(t, ctx, admin, "balance"); g.State != "Stable" {
		t.Fatalf("group is %q", g.State)
	}

	// A fourth member joins: 9 partitions over 4 members is 3/2/2/2. Exactly two
	// partitions need to move, and none between the three that were already there.
	before := map[ownedPart]string{}
	for _, m := range ms {
		for p := range m.owned {
			before[p] = m.id
		}
	}
	m3 := new848(t, addr, "balance", "m3")
	m3.join(t, ctx, names)
	all := append(ms, m3)
	settle(all)
	movedBetweenOld := 0
	for _, m := range ms {
		for p := range m.owned {
			if before[p] != m.id {
				movedBetweenOld++
			}
		}
	}
	if len(m3.owned) != 2 || movedBetweenOld != 0 || doubleOwned(all...) != "" {
		t.Errorf("after a fourth member joined: it owns %d (want 2), %d partition(s) moved between the existing members (want 0)", len(m3.owned), movedBetweenOld)
	}
}

// A group's state does not depend on the order partitions are reported in, and an
// emptied group is still a consumer group.
func TestGroupKIP848_StateAndListing(t *testing.T) {
	ctx, addr, admin, ids := setup848(t, 0, map[string]int32{"t": 4})
	a := new848(t, addr, "listing", "member-a")
	a.join(t, ctx, []string{"t"})
	a.reconcile(true)
	a.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) {
		rt := kmsg.NewConsumerGroupHeartbeatRequestTopic()
		rt.TopicID, rt.Partitions = ids["t"], []int32{3, 1, 0, 2} // not sorted.
		r.Topics = append(r.Topics, rt)
	})
	if g := describe848(t, ctx, admin, "listing"); g.State != "Stable" {
		t.Errorf("a fully reconciled group reported out of order is %q, want Stable", g.State)
	}

	list := func(mut func(*kmsg.ListGroupsRequest)) map[string]string {
		lr := kmsg.NewPtrListGroupsRequest()
		mut(lr)
		resp, err := lr.RequestWith(ctx, admin)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for _, g := range resp.Groups {
			out[g.Group] = g.GroupType + "/" + g.GroupState
		}
		return out
	}
	for _, f := range [][]string{{"Consumer"}, {"consumer"}, {"CONSUMER"}} {
		if got := list(func(r *kmsg.ListGroupsRequest) { r.TypesFilter = f }); got["listing"] == "" {
			t.Errorf("ListGroups with type filter %v did not return the consumer group", f)
		}
	}
	if got := list(func(r *kmsg.ListGroupsRequest) { r.StatesFilter = []string{"stable"} }); got["listing"] == "" {
		t.Error("ListGroups with state filter [stable] did not return a Stable group")
	}
	if got := list(func(r *kmsg.ListGroupsRequest) { r.TypesFilter = []string{"Classic"} }); got["listing"] != "" {
		t.Error("a consumer group was returned for the type filter Classic")
	}

	commitAs(t, ctx, admin, "listing", a.id, a.epoch, 1, "")
	a.leave(t, ctx)
	if got := list(func(*kmsg.ListGroupsRequest) {}); got["listing"] != "consumer/Empty" {
		t.Errorf("after its last member left the group is listed as %q, want consumer/Empty", got["listing"])
	}
	if g := describe848(t, ctx, admin, "listing"); g.ErrorCode != 0 || g.State != "Empty" {
		t.Errorf("ConsumerGroupDescribe of the emptied group: code %d state %q, want Empty", g.ErrorCode, g.State)
	}
}

// Only a member of the group, at its current epoch, may commit or fetch offsets
// as that member.
func TestGroupKIP848_OffsetsAreFencedByMemberAndEpoch(t *testing.T) {
	ctx, addr, admin, _ := setup848(t, 0, map[string]int32{"t": 1})
	a := new848(t, addr, "fence848", "member-a")
	a.join(t, ctx, []string{"t"})
	a.reconcile(true)
	for _, tc := range []struct {
		name   string
		member string
		epoch  int32
		want   int16
	}{
		{"the member at its epoch", a.id, a.epoch, 0},
		{"the member at another epoch", a.id, a.epoch + 7, 113},
		{"an unknown member", "not-a-member", a.epoch, 25},
		{"nobody, into a live group", "", -1, 25},
	} {
		if c := commitAs(t, ctx, admin, "fence848", tc.member, tc.epoch, 5, ""); c != tc.want {
			t.Errorf("OffsetCommit by %s: code %d, want %d", tc.name, c, tc.want)
		}
	}
	of := kmsg.NewPtrOffsetFetchRequest()
	rg := kmsg.NewOffsetFetchRequestGroup()
	rg.Group, rg.MemberID, rg.MemberEpoch = "fence848", kmsg.StringPtr("not-a-member"), 55
	of.Groups = append(of.Groups, rg)
	resp, err := of.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Groups) != 1 || resp.Groups[0].ErrorCode != 25 {
		t.Errorf("OffsetFetch as an unknown member: %+v, want UNKNOWN_MEMBER_ID (25)", resp.Groups)
	}
}

// One group id, one protocol, decided in one step. A classic join and a KIP-848
// join for the same new id, sent together, used to both succeed now and then.
// (The older check for this compared ListGroups entries, which the client merges
// by name, so it could not fail.)
//
// The classic member's subscription here is not the consumer protocol's, which is what
// keeps its group from being taken in by the new coordinator (a group of real consumers
// is: TestGroupCoordinators_MembersOfBothProtocolsShareAGroup); what is held is that a
// group id is never live under both coordinators at once.
func TestGroupCoordinators_ConcurrentFirstJoinsCannotBothWin(t *testing.T) {
	ctx, addr, _, _ := setup848(t, 0, map[string]int32{"t": 1})
	both := 0
	for round := 0; round < 150; round++ {
		group := fmt.Sprintf("race-%d", round)
		classic, consumer := newClient(t, addr), new848(t, addr, group, "m")
		var wg sync.WaitGroup
		var classicCode, consumerCode int16
		wg.Add(2)
		go func() {
			defer wg.Done()
			r := joinRequest(group, "", testSessionMs, testRebalanceMsDefault)
			r.InstanceID = kmsg.StringPtr("i") // static: joins in one step.
			resp, err := r.RequestWith(ctx, classic)
			if err != nil {
				classicCode = -1
				return
			}
			classicCode = resp.ErrorCode
		}()
		go func() {
			defer wg.Done()
			consumerCode = consumer.join(t, ctx, []string{"t"}).ErrorCode
		}()
		wg.Wait()
		if classicCode == 0 && consumerCode == 0 {
			both++
		}
		if classicCode != 0 && consumerCode != 0 {
			t.Fatalf("round %d: neither join succeeded (classic %d, consumer %d)", round, classicCode, consumerCode)
		}
		classic.Close()
	}
	if both > 0 {
		t.Fatalf("in %d of 150 rounds a group id ended up live under both protocols", both)
	}
}
