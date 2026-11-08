package console

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Gr-04: the reasons for the rebalances run most-common first, and a tie runs by name.
func TestTheTimelineListsReasonsMostCommonFirstThenByName(t *testing.T) {
	events := []TrafficGroupEvent{
		ev(1, 1*time.Minute, "rebalance", "", "z", "zeta"),
		ev(2, 2*time.Minute, "rebalance", "", "a", "alpha"),
		ev(3, 3*time.Minute, "rebalance", "", "m", "mid"),
		ev(4, 4*time.Minute, "rebalance", "", "m", "mid"),
	}
	v := buildTimeline(events, "all", gt0.Add(10*time.Minute), "/g")
	var got []string
	for _, r := range v.Reasons {
		got = append(got, r.Reason+"="+itoa(r.N))
	}
	if want := "mid=2 alpha=1 zeta=1"; strings.Join(got, " ") != want {
		t.Errorf("reasons run %q, want %q", strings.Join(got, " "), want)
	}
}

// Gr-04: the problems count is the expiries held, and the chips link to the filters.
func TestTheTimelineCountsProblemsAndLinksItsChips(t *testing.T) {
	events := []TrafficGroupEvent{
		ev(1, 0, "expired", "m1", "client a sent no heartbeat", ""),
		ev(2, time.Minute, "joined", "m2", "client b", ""),
		ev(3, 2*time.Minute, "expired", "m2", "client b sent no heartbeat", ""),
	}
	v := buildTimeline(events, "members", gt0.Add(5*time.Minute), "/g")
	if v.Problems != 2 {
		t.Errorf("problems = %d, want 2", v.Problems)
	}
	want := map[string]string{"all": "/g?tab=timeline", "rebalances": "/g?tab=timeline&show=rebalances", "members": "/g?tab=timeline&show=members", "problems": "/g?tab=timeline&show=problems"}
	if len(v.Chips) != 4 {
		t.Fatalf("chips = %+v", v.Chips)
	}
	for _, ch := range v.Chips {
		if ch.URL != want[ch.Key] {
			t.Errorf("chip %s links to %q, want %q", ch.Key, ch.URL, want[ch.Key])
		}
		if ch.On != (ch.Key == "members") {
			t.Errorf("chip %s On = %v with the members filter shown", ch.Key, ch.On)
		}
	}
}

// flapEvents is four rebalances and the expiries at the offsets given (seconds from the start).
func flapEvents(expiries ...float64) []TrafficGroupEvent {
	var events []TrafficGroupEvent
	seq := uint64(0)
	for _, s := range expiries {
		seq++
		events = append(events, ev(seq, time.Duration(s*float64(time.Second)), "expired", "m", "client c sent no heartbeat", ""))
	}
	for i := 0; i < 4; i++ {
		seq++
		events = append(events, ev(seq, time.Duration(i)*time.Second, "rebalance", "", "x", "x"))
	}
	return events
}

// Gr-17: three expiries are enough to give gaps, and the gaps are steady only while their
// spread is within a tenth of their mean (or a second).
func TestFlappingIsSteadyOnlyWhileTheGapsHoldWithinATenthOfTheirMean(t *testing.T) {
	now := gt0.Add(time.Minute)
	for _, tc := range []struct {
		name     string
		expiries []float64
		gaps     int
		steady   bool
	}{
		{"three expiries give two gaps", []float64{0, 30, 60}, 2, true},
		{"two expiries give none", []float64{0, 30}, 0, false},
		{"gaps of 30 s and 40 s are not steady", []float64{0, 30, 70}, 2, false},
		{"a spread of exactly a second at a mean of ten seconds is steady", []float64{0, 9.5, 20}, 2, true},
	} {
		v := buildTimeline(flapEvents(tc.expiries...), "all", now, "/g")
		if v.Flap == nil {
			t.Fatalf("%s: not flapping", tc.name)
		}
		if len(v.Flap.Gaps) != tc.gaps || v.Flap.Steady != tc.steady {
			t.Errorf("%s: gaps %v steady %v, want %d gaps steady %v", tc.name, v.Flap.Gaps, v.Flap.Steady, tc.gaps, tc.steady)
		}
	}
}

// The short spans of Gr-17 round to the nearest second and turn to minutes at two.
func TestShortSpansRoundAndTurnToMinutesAtTwo(t *testing.T) {
	for d, want := range map[time.Duration]string{1400 * time.Millisecond: "1 s", 1500 * time.Millisecond: "2 s", 119 * time.Second: "119 s", 2 * time.Minute: "2 min"} {
		if got := spanShort(d); got != want {
			t.Errorf("spanShort(%v) = %q, want %q", d, got, want)
		}
	}
}

// Gr-18: the unit follows the whole span, and each unit starts at exactly its threshold.
func TestTheRetentionUnitStartsAtItsThreshold(t *testing.T) {
	for _, tc := range []struct {
		d, span time.Duration
		want    string
	}{
		{48 * time.Hour, 48 * time.Hour, "2.0 d"},
		{2 * time.Hour, 2 * time.Hour, "2.0 h"},
		{2 * time.Minute, 2 * time.Minute, "2.0 min"},
		{30 * time.Second, 30 * time.Second, "30.0 s"},
		{90 * time.Second, 119 * time.Second, "90.0 s"},
	} {
		if got := unitSpan(tc.d, tc.span); got != tc.want {
			t.Errorf("unitSpan(%v, %v) = %q, want %q", tc.d, tc.span, got, tc.want)
		}
	}
}

// Gr-18: "in" a moment, each unit starting at exactly its threshold; the moment itself is "now".
func TestTheCountdownChangesUnitAtItsThreshold(t *testing.T) {
	for d, want := range map[time.Duration]string{
		0:                            "now (at the next check)",
		119 * time.Second:            "in 119 s",
		2 * time.Minute:              "in 2 min",
		119 * time.Minute:            "in 119 min",
		2 * time.Hour:                "in 2 h",
		47 * time.Hour:               "in 47 h",
		48 * time.Hour:               "in 2 days",
		7*24*time.Hour + 3*time.Hour: "in 7 days",
	} {
		if got := untilText(d); got != want {
			t.Errorf("untilText(%v) = %q, want %q", d, got, want)
		}
	}
}

// Gr-18: the bar's percentage is the time used over the retention.
func TestTheRetentionBarIsTheShareOfTheRetentionUsed(t *testing.T) {
	now := gt0
	empty := now.Add(-3 * 24 * time.Hour)
	c := &Console{inspector: expiringInspector{fakeInspector{}, OffsetExpiry{Retention: 7 * 24 * time.Hour, EmptySince: empty, Interval: time.Minute,
		Offsets: []OffsetExpiryAt{{Topic: "orders", At: empty.Add(7 * 24 * time.Hour)}}}, true}}
	v := c.expiryOf("idle", true, now)
	if v == nil || v.Pct != 42 {
		t.Fatalf("view = %+v, want 42 percent (3 of 7 days)", v)
	}
	if v.Used != "3.0 d" || v.Of != "7.0 d" {
		t.Errorf("used %q of %q", v.Used, v.Of)
	}
}

// Gr-09: the partitions a member owns, with the log end where the broker said, in topic then
// partition order whoever owns them.
func TestPartitionsWithNoCommitRunByTopicThenPartition(t *testing.T) {
	c := &Console{}
	desc := []GroupMember{
		{ID: "m1", Assigned: map[string][]int32{"b": {1, 0}, "a": {2}}},
		{ID: "m2", Assigned: map[string][]int32{"a": {0}, "b": {2}}},
	}
	views := []memberView{{Label: "one"}, {Label: "two"}}
	bounds := map[string][]PartitionBound{
		"a": {{Partition: 0, Latest: 10}, {Partition: 1, Latest: 11}},
		"b": {{Partition: 0, Latest: 20}, {Partition: 1, Latest: 21}},
	}
	rows := c.noCommitRows(desc, views, bounds)
	var got []string
	for _, r := range rows {
		got = append(got, r.Topic+itoa(int(r.Partition))+"/"+r.Owner)
	}
	if want := "a0/two a2/one b0/one b1/one b2/two"; strings.Join(got, " ") != want {
		t.Errorf("rows run %q, want %q", strings.Join(got, " "), want)
	}
	for _, r := range rows {
		wantEnd := !(r.Partition == 2) // the bounds hold two partitions of each topic.
		if r.HasEnd != wantEnd {
			t.Errorf("%s/%d HasEnd = %v, want %v", r.Topic, r.Partition, r.HasEnd, wantEnd)
		}
	}
	if rows[0].LogEnd != 10 || rows[3].LogEnd != 21 {
		t.Errorf("log ends = %d, %d", rows[0].LogEnd, rows[3].LogEnd)
	}
}

// A partition two members both claim (a leader's assignment the broker does not police) keeps
// the order the members came in.
func TestAPartitionTwoMembersClaimKeepsTheMembersInTheirOrder(t *testing.T) {
	c := &Console{}
	desc := []GroupMember{{ID: "m1", Assigned: map[string][]int32{"a": {0}}}, {ID: "m2", Assigned: map[string][]int32{"a": {0}}}}
	rows := c.noCommitRows(desc, []memberView{{Label: "one"}, {Label: "two"}}, nil)
	if len(rows) != 2 || rows[0].Owner != "one" || rows[1].Owner != "two" {
		t.Errorf("rows = %+v", rows)
	}
}

// Gr-11: an assignment to a topic that no longer exists is named as such.
func TestAnAssignmentToADeletedTopicIsNamedAsDeleted(t *testing.T) {
	got := partsText([]TopicParts{{Topic: "orders", Partitions: []int32{0, 1}}, {TopicID: "abcdefghijklmnopqrstuvwxyz", Partitions: []int32{3}}})
	if want := "orders:0,1 (deleted topic abcdef…uvwxyz):3"; got != want {
		t.Errorf("partsText = %q, want %q", got, want)
	}
}

// Gr-12: the epoch arrow shows where the member's own epoch changed, and only then.
func TestAHandoffShowsAnArrowOnlyWhereTheMemberMovedEpoch(t *testing.T) {
	insp := GroupInspection{State: InspectOK, Group: GroupSnapshot{Kind: "consumer", State: "Stable", Epoch: 12, TargetEpoch: 12, Members: []MemberSnapshot{
		{ID: "moved", Epoch: 12, PrevEpoch: 11, State: "STABLE"},
		{ID: "same", Epoch: 12, PrevEpoch: 12, State: "STABLE"},
		{ID: "new", Epoch: 12, State: "STABLE"},
	}}}
	rows := handoffsOf(insp)
	if len(rows) != 3 || rows[0].Epoch != "11 → 12" || rows[1].Epoch != "12" || rows[2].Epoch != "12" {
		t.Errorf("rows = %+v", rows)
	}
}

// Gr-13: a member that states no session timeout does not pull the range down to nothing.
func TestAMemberWithNoSessionTimeoutIsLeftOutOfTheRange(t *testing.T) {
	insp := GroupInspection{State: InspectOK, Group: GroupSnapshot{Kind: "classic", Members: []MemberSnapshot{
		{ID: "a", SessionTimeout: 45 * time.Second}, {ID: "b"}}}}
	if f := classicOf(insp); f == nil || f.Session != "45 s" {
		t.Errorf("facts = %+v, want a session of 45 s", f)
	}
}

// Gr-05: at the very moment of the deadline the group is late, with nothing left.
func TestAGroupAtItsRebalanceDeadlineIsLate(t *testing.T) {
	insp := GroupInspection{State: InspectOK, Group: GroupSnapshot{Kind: "classic", State: "PreparingRebalance", RebalanceTimeout: 45 * time.Second,
		RebalanceDeadline: gt0, Members: []MemberSnapshot{{ID: "a"}}}}
	if v := rebalanceOf(insp, gt0); v == nil || !v.Late || v.Left != "0 s" {
		t.Errorf("view = %+v", v)
	}
}

// Gr-14: how far behind the last stable offset a group is, and a committed partition the topic
// does not have is passed over.
func TestHeldBackCountsTheLagToTheLastStableOffset(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "tx", 1, 2)
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.TransactionalID("checkout-b1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()
	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "tx", Partition: 0, Value: []byte("open")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	rows := []groupLagRow{
		{Topic: "tx", Partition: 0, Committed: 1, Latest: 5},
		{Topic: "tx", Partition: 1, Committed: 0, Latest: 9}, // a partition the topic does not have.
	}
	v := heldBackOf(c, ctx, rows, time.Now())
	if v == nil || len(v.Rows) != 1 {
		t.Fatalf("view = %+v", v)
	}
	if r := v.Rows[0]; r.Stable != 2 || r.Held != 3 || r.Behind != 1 {
		t.Errorf("row = %+v, want stable 2, held 3, behind 1", r)
	}
	_ = cl.EndTransaction(ctx, kgo.TryAbort)
}

// aclNames: a group ACL names the group literally, by "*", or by prefix.
func TestAGroupACLNamesTheGroupLiterallyByStarOrByPrefix(t *testing.T) {
	for _, tc := range []struct {
		a    ACLRow
		want bool
	}{
		{ACLRow{Name: "g1", Pattern: "literal"}, true},
		{ACLRow{Name: "*", Pattern: "literal"}, true},
		{ACLRow{Name: "other", Pattern: "literal"}, false},
		{ACLRow{Name: "g", Pattern: "prefixed"}, true},
		{ACLRow{Name: "x", Pattern: "prefixed"}, false},
	} {
		if got := aclNames(tc.a, "g1"); got != tc.want {
			t.Errorf("aclNames(%+v, g1) = %v, want %v", tc.a, got, tc.want)
		}
	}
}

// Gr-16: the refusal counts what was refused of this group (named directly or as the group a
// request came through), keeps the broker's own error code, and lists only the ACLs about this
// group that name this principal or every principal.
func TestTheDeniedNoteIsAboutThisGroupAndThePrincipalsACLs(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "x1", Seq: 1, First: now.Add(-50 * time.Second), Time: now.Add(-40 * time.Second), Count: 5, ClientID: "someone-else", Principal: "User:other", API: "JoinGroup",
			Group: "unrelated", Error: "GROUP_AUTHORIZATION_FAILED", Class: "denied"},
		{ID: "x2", Seq: 2, First: now.Add(-30 * time.Second), Time: now.Add(-20 * time.Second), Count: 2, ClientID: "etl", Principal: "User:loyalty", API: "FindCoordinator",
			ViaGroup: "g-b1", Error: "CLUSTER_AUTHORIZATION_FAILED", Class: "denied"},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	ctx := context.Background()
	for _, a := range []ACLRow{
		{Resource: "group", Name: "g-b1", Pattern: "literal", Principal: "User:loyalty", Host: "*", Operation: "describe", Permission: "deny"},
		{Resource: "group", Name: "*", Pattern: "literal", Principal: "User:*", Host: "*", Operation: "read", Permission: "allow"},
		{Resource: "group", Name: "g-b1", Pattern: "literal", Principal: "User:other", Host: "*", Operation: "read", Permission: "deny"},
		{Resource: "group", Name: "unrelated", Pattern: "literal", Principal: "User:loyalty", Host: "*", Operation: "read", Permission: "deny"},
		{Resource: "topic", Name: "g-b1", Pattern: "literal", Principal: "User:loyalty", Host: "*", Operation: "read", Permission: "deny"},
	} {
		if err := c.be.CreateACL(ctx, a); err != nil {
			t.Fatalf("storing %+v: %v", a, err)
		}
	}
	v := c.deniedOf(ctx, "g-b1", now)
	if v == nil {
		t.Fatal("no refusal found for a group a request came through")
	}
	if v.Count != 2 || v.Clients != 1 || v.Error != "CLUSTER_AUTHORIZATION_FAILED" || v.API != "FindCoordinator" || v.Needs != "Describe" {
		t.Errorf("view = %+v", v)
	}
	var got []string
	for _, a := range v.ACLs {
		got = append(got, a.Principal+" "+a.Operation)
	}
	if len(got) != 2 || !(strings.Contains(strings.Join(got, ","), "User:loyalty describe") && strings.Contains(strings.Join(got, ","), "User:* read")) {
		t.Errorf("ACLs listed = %v, want the loyalty deny and the everyone allow", got)
	}
}

// Gr-16: a refusal the broker gave no error for is named by the group error code.
func TestARefusalWithNoErrorIsNamedGroupAuthorizationFailed(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "e1", Seq: 1, First: now.Add(-30 * time.Second), Time: now.Add(-20 * time.Second), Count: 1, ClientID: "etl", Principal: "User:loyalty", API: "JoinGroup",
			Group: "g-b1", Class: "denied"},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	v := c.deniedOf(context.Background(), "g-b1", now)
	if v == nil || v.Error != "GROUP_AUTHORIZATION_FAILED" {
		t.Errorf("view = %+v", v)
	}
}
