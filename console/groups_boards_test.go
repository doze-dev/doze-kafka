package console

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var gt0 = time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)

func ev(seq uint64, at time.Duration, kind, member, detail, cause string) TrafficGroupEvent {
	return TrafficGroupEvent{Seq: seq, Time: gt0.Add(at), Kind: kind, Member: member, Detail: detail, Cause: cause}
}

// Gr-04, GRP-24: the window and the counts come from the events, at the moment given.
func TestTimelineCountsTheLastHourFromTheEvents(t *testing.T) {
	events := []TrafficGroupEvent{
		ev(1, 0, "joined", "m1", "client app", ""),
		ev(2, 0, "rebalance", "m1", "a member joined", "a member joined"),
		ev(3, 5*time.Minute, "rebalance", "", "a member left", "a member left"),
		ev(4, 65*time.Minute, "rebalance", "", "a member joined", "a member joined"), // 75 min before "now".
		ev(5, 100*time.Minute, "rebalance", "", "a member joined", "a member joined"),
	}
	v := buildTimeline(events, "all", gt0.Add(2*time.Hour), "/g")
	if v.Rebalances != 2 { // the two within the hour before 16:00 are at 15:05 and 15:40.
		t.Errorf("rebalances in the last hour = %d, want 2", v.Rebalances)
	}
	if v.Held != 5 || v.Full || !v.Since.Equal(gt0) {
		t.Errorf("held %d full %v since %v", v.Held, v.Full, v.Since)
	}
	if len(v.Reasons) != 1 || v.Reasons[0].Reason != "a member joined" || v.Reasons[0].N != 2 {
		t.Errorf("reasons = %+v", v.Reasons)
	}
	if v.Rows[0].Seq != 5 {
		t.Errorf("newest first: %d", v.Rows[0].Seq)
	}
	for _, tc := range []struct {
		show string
		want int
	}{{"all", 5}, {"rebalances", 4}, {"members", 1}, {"problems", 0}} {
		if got := len(buildTimeline(events, tc.show, gt0.Add(2*time.Hour), "/g").Rows); got != tc.want {
			t.Errorf("chip %s keeps %d rows, want %d", tc.show, got, tc.want)
		}
	}
	full := make([]TrafficGroupEvent, groupEventsHeld)
	if v := buildTimeline(full, "all", gt0, "/g"); !v.Full {
		t.Error("a full ring is not said to be full")
	}
}

// Gr-17: expired, joined, expired, with the gaps, is flapping; a restart is not.
func TestTimelineNamesAFlappingGroup(t *testing.T) {
	var events []TrafficGroupEvent
	seq := uint64(0)
	add := func(at time.Duration, kind, member, detail, cause string) {
		seq++
		events = append(events, ev(seq, at, kind, member, detail, cause))
	}
	for i := 0; i < 4; i++ {
		base := time.Duration(i) * 30 * time.Second
		add(base, "expired", "m"+itoa(i), "client depot-router sent no heartbeat for 10.0 s (its session timeout)", "")
		add(base+time.Millisecond, "rebalance", "", "a member's session expired", "a member's session expired")
		add(base+4*time.Second, "joined", "n"+itoa(i), "client depot-router", "")
	}
	now := gt0.Add(2 * time.Minute)
	v := buildTimeline(events, "all", now, "/g")
	if v.Flap == nil || v.Flap.Every != "30 s" || v.Flap.Count != 4 || !v.Flap.Steady || len(v.Flap.Gaps) != 3 || v.Flap.Gaps[0] != "30.0 s" {
		t.Fatalf("flap = %+v", v.Flap)
	}
	// The second join of a client that lost a member to an expiry is a rejoin.
	var rejoins int
	for _, r := range v.Rows {
		if r.Kind == "joined" && strings.Contains(r.Detail, "rejoined with a new member id") {
			rejoins++
		}
	}
	if rejoins != 4 {
		t.Errorf("%d joins say they rejoined, want 4 (each follows an expiry of the same client)", rejoins)
	}
	// Three rebalances in ten minutes, or none in the last ten: not flapping.
	if v := buildTimeline(events[:9], "all", now, "/g"); v.Flap != nil {
		t.Errorf("three rebalances flagged: %+v", v.Flap)
	}
	if v := buildTimeline(events, "all", now.Add(time.Hour), "/g"); v.Flap != nil {
		t.Errorf("an old loop flagged: %+v", v.Flap)
	}
}

func TestRebalanceViewSaysWhoHasRejoinedAndTimeLeft(t *testing.T) {
	now := gt0
	insp := GroupInspection{State: InspectOK, Group: GroupSnapshot{Kind: "classic", State: "PreparingRebalance", Generation: 7,
		RebalanceTimeout: 45 * time.Second, RebalanceDeadline: now.Add(31 * time.Second), Members: []MemberSnapshot{
			{ID: "a", ClientID: "app", Rejoined: true, RejoinedAt: now.Add(-14 * time.Second)},
			{ID: "b", ClientID: "app", LastHeartbeat: now.Add(-4 * time.Second)}}}}
	v := rebalanceOf(insp, now)
	if v == nil || v.Next != 8 || v.Waiting != 1 || v.Total != 2 || v.Left != "31 s" || v.Of != "45 s" || v.Late {
		t.Fatalf("view = %+v", v)
	}
	if !v.Rows[0].Rejoined || v.Rows[1].Rejoined {
		t.Errorf("rows = %+v", v.Rows)
	}
	if got := rebalanceOf(insp, now.Add(time.Minute)); !got.Late || got.Left != "0 s" {
		t.Errorf("past the deadline: %+v", got)
	}
	insp.Group.State = "Stable"
	if rebalanceOf(insp, now) != nil || rebalanceOf(GroupInspection{State: InspectNone}, now) != nil {
		t.Error("a view for a group that is not rebalancing")
	}
	// The initial rebalance delay is not the rebalance timeout.
	insp.Group.State, insp.Group.Holding = "PreparingRebalance", true
	if h := rebalanceOf(insp, now); !h.Holding || h.Of != "" {
		t.Errorf("holding: %+v", h)
	}
}

func TestHandoffRowsAreTheCoordinatorsRecord(t *testing.T) {
	insp := GroupInspection{State: InspectOK, Group: GroupSnapshot{Kind: "consumer", State: "Reconciling", Epoch: 13, TargetEpoch: 13, Members: []MemberSnapshot{
		{ID: "a", Epoch: 12, PrevEpoch: 11, State: "RECONCILING", Target: []TopicParts{{Topic: "orders", Partitions: []int32{0, 1}}},
			Current: []TopicParts{{Topic: "orders", Partitions: []int32{0, 1, 2}}}, Revoking: []TopicParts{{Topic: "orders", Partitions: []int32{2}}}},
		{ID: "b", Epoch: 13, State: "RECONCILING", Target: []TopicParts{{Topic: "orders", Partitions: []int32{2}}}},
	}}}
	rows := handoffsOf(insp)
	if len(rows) != 2 {
		t.Fatalf("rows = %+v", rows)
	}
	a, b := rows[0], rows[1]
	if a.Epoch != "12 → 13" || a.Target != "orders:0,1" || a.Current != "orders:0,1,2" || a.Releasing != "2" || a.Waiting != "" {
		t.Errorf("a = %+v", a)
	}
	if b.Epoch != "13" || b.Current != "" || b.Waiting != "2" || !b.Reconciling {
		t.Errorf("b = %+v", b)
	}
	if handoffsOf(GroupInspection{State: InspectOK, Group: GroupSnapshot{Kind: "classic"}}) != nil {
		t.Error("a classic group has no handoffs")
	}
}

func TestClassicFactsAndAheadAndLeftovers(t *testing.T) {
	insp := GroupInspection{State: InspectOK, Group: GroupSnapshot{Kind: "classic", Generation: 7, ProtocolType: "consumer", ProtocolName: "range", Leader: "a",
		Members: []MemberSnapshot{
			{ID: "a", SessionTimeout: 45 * time.Second, RebalanceTimeout: 45 * time.Second, Protocols: []string{"range", "cooperative-sticky"}},
			{ID: "b", SessionTimeout: 45 * time.Second, Protocols: []string{"range"}}}}}
	f := classicOf(insp)
	if f == nil || f.Generation != 7 || f.Protocol != "range" || f.Offered != "range, cooperative-sticky" || f.Session != "45 s" || f.Rebalance != "45 s" {
		t.Errorf("classic facts = %+v", f)
	}
	rows := []groupLagRow{
		{Topic: "t", Partition: 0, Committed: 1204, Earliest: 0, Latest: 41, Ahead: true},
		{Topic: "t", Partition: 1, Committed: 1128, Earliest: 3, Latest: 38, Ahead: true},
		{Topic: "t", Partition: 2, Committed: 5, Latest: 40},
		{Topic: "gone", Partition: 0, Committed: 312, Deleted: true},
	}
	a := aheadOf(rows)
	if a == nil || a.N != 2 || a.Of != 3 || a.Exist != 41+35 {
		t.Errorf("ahead = %+v", a)
	}
	if aheadOf(rows[2:3]) != nil {
		t.Error("ahead with nothing ahead")
	}
	topics, n := leftoverTopics(rows)
	if len(topics) != 1 || topics[0] != "gone" || n != 1 {
		t.Errorf("leftovers = %v %d", topics, n)
	}
}

// The page: a rebalance under way, from the coordinator's memory; a consumer group's
// target and current; the board's words for each.
func TestGroupPageShowsARebalanceAndAHandoff(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "orders", 2, 3)
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("snap-client"), kgo.ConsumerGroup("rbg"), kgo.ConsumeTopics("orders"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		cl.PollFetches(ctx)
	}()
	deadline := time.Now().Add(8 * time.Second)
	var id string
	for id == "" {
		if d, err := c.be.DescribeGroup(context.Background(), "rbg"); err == nil && len(d.Members) == 1 && len(d.Members[0].Assigned["orders"]) == 2 {
			id = d.Members[0].ID
		} else if time.Now().After(deadline) {
			t.Fatal("the member never showed")
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.inspector = fakeInspector{"rbg": {ID: "rbg", Kind: "classic", State: "PreparingRebalance", Generation: 7, Leader: id,
		RebalanceTimeout: 45 * time.Second, RebalanceDeadline: time.Now().Add(31 * time.Second), Members: []MemberSnapshot{
			{ID: id, ClientID: "snap-client", Addr: "127.0.0.1:55173", Rejoined: true, RejoinedAt: time.Now().Add(-3 * time.Second), SessionTimeout: 45 * time.Second, Protocols: []string{"range"}},
			{ID: "other", ClientID: "slow", Addr: "127.0.0.1:55174", LastHeartbeat: time.Now().Add(-4 * time.Second)}}}}
	body := readable(req(t, c, "GET", "/_console/groups/rbg?tab=members", nil).Body.String())
	for _, want := range []string{"A rebalance is under way", "waiting for 1 of its 2 members to rejoin", "Generation 8 is formed",
		"Waiting for members to rejoin", "s left of 45 s", "not yet · last heartbeat 4 s ago", "Joined generation 8?", "rejoined", "REBALANCE_IN_PROGRESS",
		"The group", "Protocol type", "chosen"} {
		if want == "chosen" { // one protocol offered: nothing to choose between.
			if strings.Contains(body, "chosen from") {
				t.Errorf("a single offered protocol is shown as a choice")
			}
			continue
		}
		if !strings.Contains(body, want) {
			t.Errorf("the rebalancing page lacks %q:\n%s", want, body)
		}
	}
	c.inspector = fakeInspector{"rbg": {ID: "rbg", Kind: "consumer", State: "Reconciling", Epoch: 13, TargetEpoch: 13, Members: []MemberSnapshot{
		{ID: id, ClientID: "snap-client", Epoch: 12, PrevEpoch: 11, State: "RECONCILING", Target: []TopicParts{{Topic: "orders", Partitions: []int32{0}}},
			Current: []TopicParts{{Topic: "orders", Partitions: []int32{0, 1}}}, Revoking: []TopicParts{{Topic: "orders", Partitions: []int32{1}}}}}}}
	body = readable(req(t, c, "GET", "/_console/groups/rbg?tab=members", nil).Body.String())
	for _, want := range []string{"Target assignment", "Current assignment", "RECONCILING", "12 → 13", "orders:0,1", "releasing 1", "example, not this group", "not read from the member"} {
		if !strings.Contains(body, want) {
			t.Errorf("the handoff page lacks %q:\n%s", want, body)
		}
	}
}

type expiringInspector struct {
	fakeInspector
	exp OffsetExpiry
	ok  bool
}

func (e expiringInspector) OffsetExpiry(string) (OffsetExpiry, bool) { return e.exp, e.ok }

// Gr-18 (GRP-19): the countdown is the broker's; with no retention the page says offsets are
// kept for ever, and with a broker that cannot say it says nothing about retention.
func TestOffsetsRetentionCountdownIsOnlyWhatTheBrokerReports(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "orders", 1, 5)
	commitFor(t, addr, "idle", "orders", map[int32]int64{0: 2})
	page := func() string { return readable(req(t, c, "GET", "/_console/groups/idle", nil).Body.String()) }

	if body := page(); strings.Contains(body, "expire") || strings.Contains(body, "kept for ever") {
		t.Errorf("a console that cannot see the retention says something about it:\n%s", body)
	}
	c.inspector = expiringInspector{fakeInspector{}, OffsetExpiry{}, true}
	if body := page(); !strings.Contains(body, "keeps committed offsets for ever") || !strings.Contains(body, "offsets-retention") {
		t.Errorf("no retention:\n%s", body)
	}
	empty := time.Now().Add(-3 * 24 * time.Hour)
	c.inspector = expiringInspector{fakeInspector{}, OffsetExpiry{Retention: 7 * 24 * time.Hour, EmptySince: empty, Interval: 10 * time.Minute,
		Offsets: []OffsetExpiryAt{{Topic: "orders", At: empty.Add(7 * 24 * time.Hour)}}}, true}
	body := page()
	for _, want := range []string{"Offsets are kept for now", "in 3 days", "3.0 d of 7.0 d used", "7.0 d", "offsets.retention.minutes", "auto.offset.reset", "1 offset of this group will expire"} {
		if !strings.Contains(body, want) {
			t.Errorf("the countdown lacks %q:\n%s", want, body)
		}
	}
	c.inspector = expiringInspector{fakeInspector{}, OffsetExpiry{Retention: time.Hour}, true}
	if body := page(); !strings.Contains(body, "none of this group's does as things stand") {
		t.Errorf("nothing expiring is not said:\n%s", body)
	}
}

func TestExpiryTexts(t *testing.T) {
	for d, want := range map[time.Duration]string{-time.Second: "now (at the next check)", 40 * time.Second: "in 40 s", 12 * time.Minute: "in 12 min", 5 * time.Hour: "in 5 h", 100 * time.Hour: "in 4 days"} {
		if got := untilText(d); got != want {
			t.Errorf("untilText(%v) = %q, want %q", d, got, want)
		}
	}
	if got := unitSpan(36*time.Hour, 7*24*time.Hour); got != "1.5 d" {
		t.Errorf("unitSpan = %q", got)
	}
	if got := unitSpan(90*time.Second, 10*time.Minute); got != "1.5 min" {
		t.Errorf("unitSpan = %q", got)
	}
}

// Gr-10: ahead of the log end, with arithmetic and no story about a recreated topic (GRP-14).
func TestAheadOfLogEndAdvice(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "pts", 1, 4)
	commitFor(t, addr, "ahead", "pts", map[int32]int64{0: 1200})
	body := readable(req(t, c, "GET", "/_console/groups/ahead", nil).Body.String())
	for _, want := range []string{"Ahead of log end on 1 of 1 partition", "OFFSET_OUT_OF_RANGE", "re-read the 4 that exist", "ahead of log end by 1,196"} {
		if !strings.Contains(body, want) {
			t.Errorf("the ahead page lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "recreated") {
		t.Error("the page tells a story about a recreated topic that cannot happen through the API (GRP-14)")
	}
}

// Gr-11 (GRP-14): the claim about offsets after a topic is deleted is the recorded one (G2).
func TestLeftoverOffsetsSayWhatKafkaDoes(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "kept", 1, 3)
	commitFor(t, addr, "audit", "kept", map[int32]int64{0: 1})
	data := groupPage(t, c, "audit", "")
	data["LeftTopics"], data["LeftN"] = []string{"harbour.invoices"}, 1
	rec := httptest.NewRecorder()
	c.partial(rec, "group_live", data)
	body := readable(rec.Body.String())
	for _, want := range []string{"Why this row is here", "harbour.invoices", "Kafka drops the offsets of a deleted topic, a moment after the topic goes and not at once", "this broker drops them in the same step", "earlier version", "does not count toward the lag"} {
		if !strings.Contains(body, want) {
			t.Errorf("the leftover note lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "keeps a group's committed offsets after its topic is deleted") {
		t.Error("the board's old claim is back")
	}
}

// Gr-14: records past the last stable offset, from an open transaction, with the transaction named.
func TestHeldBackByAnOpenTransaction(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "tx", 1, 2)
	commitFor(t, addr, "rc", "tx", map[int32]int64{0: 1})
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.TransactionalID("checkout-1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()
	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "tx", Partition: 0, Value: []byte("in a transaction")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	body := readable(req(t, c, "GET", "/_console/groups/rc", nil).Body.String())
	for _, want := range []string{"3 records held back by an open transaction", "Open transaction checkout-1", "cannot tell which this group does", "Lag to last stable"} {
		if !strings.Contains(body, want) {
			t.Errorf("the held-back note lacks %q:\n%s", want, body)
		}
	}
	if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	if body := readable(req(t, c, "GET", "/_console/groups/rc", nil).Body.String()); strings.Contains(body, "held back by an open transaction") {
		t.Error("the note stays after the transaction ended")
	}
}

// Gr-16 (GRP-21): the refusal names the operation the API needs, not a guess.
func TestDeniedGroupNamesTheOperationTheAPINeeds(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "d1", Seq: 1, First: now.Add(-30 * time.Second), Time: now.Add(-20 * time.Second), Count: 3, ClientID: "loyalty-etl", Principal: "User:loyalty", API: "OffsetFetch", Version: 8,
			Group: "g-loyalty-etl", Error: "GROUP_AUTHORIZATION_FAILED", ErrorCode: 30, Class: "denied"},
	}}
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "pts", 1, 1)
	commitFor(t, addr, "g-loyalty-etl", "pts", map[int32]int64{0: 0})
	body := readable(req(t, c, "GET", "/_console/groups/g-loyalty-etl", nil).Body.String())
	for _, want := range []string{"Refused GROUP_AUTHORIZATION_FAILED", "OffsetFetch v8", "from loyalty-etl as User:loyalty", "3 refused requests", "needs Describe on this group",
		"a Read deny does not refuse a Describe", "Check User:loyalty on this group", "No ACL on this group names this principal"} {
		if !strings.Contains(body, want) {
			t.Errorf("the denied note lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "denies Read to") {
		t.Error("the board's Read claim for OffsetFetch is back (GRP-21)")
	}
	for api, want := range map[string]string{"JoinGroup": "Read", "OffsetCommit": "Read", "OffsetFetch": "Describe", "FindCoordinator": "Describe", "DeleteGroups": "Delete", "OffsetDelete": "Delete"} {
		if groupAPINeeds[api] != want {
			t.Errorf("%s needs %q, want %q", api, groupAPINeeds[api], want)
		}
	}
}
