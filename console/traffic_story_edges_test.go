package console

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestSmallTextHelpers(t *testing.T) {
	for in, want := range map[string]string{"": "", "A": "a", "Abc": "abc", "abc": "abc", "ÉCOLE": "éCOLE", "1Abc": "1Abc"} {
		if got := lowerFirst(in); got != want {
			t.Errorf("lowerFirst(%q) = %q, want %q", in, got, want)
		}
	}
	if pluralOf(1, "member") != "1 member" || pluralOf(0, "member") != "0 members" || pluralOf(3, "error") != "3 errors" {
		t.Error("pluralOf")
	}
	for in, want := range map[string]string{"Stable": "ok", "stable": "ok", "PreparingRebalance": "warn", "CompletingRebalance": "warn", "Reconciling": "warn", "assigning": "warn", "Dead": "bad", "Empty": "", "": ""} {
		if got := stateTone(in); got != want {
			t.Errorf("stateTone(%q) = %q, want %q", in, got, want)
		}
	}
	if refusalWhy("UNKNOWN_TOPIC_OR_PARTITION") == "" || refusalWhy("NOT_AN_ERROR_NAME") != "" || refusalWhy("") != "" {
		t.Error("refusalWhy")
	}
}

func TestNearestTopic(t *testing.T) {
	topics := []TopicSummary{{Name: "orders"}, {Name: "payments"}, {Name: "orderz-long"}, {Name: "ab"}}
	for _, tc := range []struct {
		name string
		want string
		ok   bool
	}{
		{"ordres", "orders", true}, // two edits of six letters
		{"orders", "", false},
		{"paymnts", "payments", true},
		{"zzzzzz", "", false},
		{"a", "ab", true},
		{"abc", "ab", true},
	} {
		got, ok := nearestTopic(tc.name, topics)
		if got != tc.want || ok != tc.ok {
			t.Errorf("nearestTopic(%q) = %q, %v; want %q, %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
	if _, ok := nearestTopic("x", nil); ok {
		t.Error("no topics")
	}
	// At most a third of the name, or two.
	if got, ok := nearestTopic("abcdefghi", []TopicSummary{{Name: "abcdefzzz"}}); !ok || got != "abcdefzzz" {
		t.Errorf("three edits of nine: %q %v", got, ok)
	}
	if _, ok := nearestTopic("abcdefghi", []TopicSummary{{Name: "abcdezzzz"}}); ok {
		t.Error("four edits of nine")
	}
}

func TestRefusalHeadlines(t *testing.T) {
	for _, tc := range []struct {
		api   string
		code  int16
		class string
		want  string
	}{
		{"Produce", 3, "refused", "Produce to a topic that does not exist"},
		{"Fetch", 3, "refused", "Fetch for a topic that does not exist"},
		{"Produce", 29, "denied", "Produce on a topic the principal may not use"},
		{"JoinGroup", 30, "denied", "JoinGroup for a group the principal may not use"},
		{"InitProducerId", 90, "refused", "InitProducerId from a producer a newer one replaced"},
		{"Fetch", 1, "denied", "Fetch was denied"},
		{"Fetch", 1, "failed", "Fetch failed"},
		{"Fetch", 1, "not-implemented", "Fetch is not served here"},
		{"Fetch", 1, "refused", "Fetch was refused"},
		{"Fetch", 1, "", "Fetch was refused"},
	} {
		if got := refusalHeadline(tc.api, tc.code, tc.class); got != tc.want {
			t.Errorf("refusalHeadline(%s, %d, %s) = %q", tc.api, tc.code, tc.class, got)
		}
	}
}

func TestErrorSummaryOfTheMostFrequentRefusal(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Facts.KafkaMajor = 4; o.Facts.AutoCreateTopics = false })
	topicWith(t, c, "invoices", 2, 0)
	topicWith(t, c, "other", 1, 0)
	ctx := context.Background()
	t0 := time.Now()
	if v := c.errorSummary(ctx, nil); v != nil {
		t.Error("a summary of nothing")
	}
	if v := c.errorSummary(ctx, []TrafficRefusal{{Name: "REBALANCE", Class: "expected", Count: 9}}); v != nil {
		t.Error("expected flow is a refusal")
	}
	top := TrafficRefusal{Code: 3, Name: "UNKNOWN_TOPIC_OR_PARTITION", Class: "refused", Count: 5, API: "Produce", Topics: []string{"invoicez"}, Last: t0, Latest: "r42",
		Clients: []TrafficClientCount{{"checkout-api", 4}, {"other", 1}}}
	rs := []TrafficRefusal{
		{Name: "REBALANCE_IN_PROGRESS", Class: "expected", Count: 99},
		top,
		{Name: "A", Class: "denied", Clients: []TrafficClientCount{{"ca", 1}}},
		{Name: "B", Class: "failed"},
		{Name: "C", Class: "refused", Clients: []TrafficClientCount{{"cc", 1}}},
		{Name: "D", Class: "refused"},
	}
	v := c.errorSummary(ctx, rs)
	if v == nil {
		t.Fatal("no summary")
	}
	if v.Name != "UNKNOWN_TOPIC_OR_PARTITION" || v.Class != "refused" || v.ClassWord != "Refused" || v.Tone != "deny" || v.Headline != "Produce to a topic that does not exist" ||
		v.Client != "checkout-api" || v.Seen != "4 times" || !v.SeenAt.Equal(t0) || v.Said != "checkout-api produced to invoicez, which is not on this broker." ||
		v.Verdict != "Real Kafka would refuse the same call." || v.RowURL != c.inspectURL(KindRequest, "r42") {
		t.Errorf("%+v", v)
	}
	if v.Create != "invoicez" || v.CreateURL != "/_console/new/topic?name=invoicez" || v.TopicsURL != "/_console/topics" || v.TopicCount != 2 || v.Nearest != "invoices" || v.NearestURL != "/_console/topics/invoices" {
		t.Errorf("topic hints: %+v", v)
	}
	if !v.HasFix || !strings.Contains(v.Fix, "auto-create is off on this broker") || v.Cause == "" || v.Cause[0] >= 'A' && v.Cause[0] <= 'Z' || v.Meaning == "" {
		t.Errorf("fix %q cause %q", v.Fix, v.Cause)
	}
	if len(v.Others) != 3 || v.Others[0] != (errorOther{"A", "ca"}) || v.Others[1] != (errorOther{"B", ""}) || v.Others[2] != (errorOther{"C", "cc"}) {
		t.Errorf("others %+v", v.Others)
	}
	// One time, a request that is not a produce, a class of its own.
	one := TrafficRefusal{Code: 3, Name: "UNKNOWN_TOPIC_OR_PARTITION", Class: "denied", API: "Fetch", Topics: []string{"a", "b"}, Clients: []TrafficClientCount{{"cl", 1}}}
	v = c.errorSummary(ctx, []TrafficRefusal{one})
	if v.Seen != "1 time" || v.Said != "cl sent Fetch for a, b." || v.Headline != "Fetch for a topic that does not exist" || v.Verdict != "An ACL or a login decided this: nothing is wrong with the broker." || len(v.Others) != 0 {
		t.Errorf("%+v", v)
	}
	if v.Create != "a" || v.Nearest != "" {
		t.Errorf("create %q nearest %q", v.Create, v.Nearest)
	}
	v = c.errorSummary(ctx, []TrafficRefusal{{Code: 35, Name: "UNSUPPORTED_VERSION", Class: "not-implemented", API: "Foo"}})
	if v.Said != "a client sent Foo." || v.Client != "" || v.Seen != "" || v.Verdict != "This broker does not do this; real Kafka would." || v.RowURL != "" || v.Create != "" {
		t.Errorf("%+v", v)
	}
	v = c.errorSummary(ctx, []TrafficRefusal{{Code: 1, Name: "X", Class: "failed", API: "Fetch"}})
	if v.Verdict != "The broker could not do this. Its log says why." {
		t.Errorf("%q", v.Verdict)
	}
	v = c.errorSummary(ctx, []TrafficRefusal{{Name: "CONNECTION_CLOSED", Class: "failed", API: "Produce"}})
	if v.Headline != "Produce ended the connection" || v.Meaning != "The broker closed the connection because of this request." {
		t.Errorf("%q / %q", v.Headline, v.Meaning)
	}
}

func TestTouchedTopic(t *testing.T) {
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "1", Kind: "produce", Topics: []string{"orders"}, ClientID: "w2", Count: 3},
		{ID: "2", Kind: "produce", Topics: []string{"orders"}, ClientID: "w1"},
		{ID: "3", Kind: "produce", Topics: []string{"orders"}, ClientID: "w1", Count: 2, Error: "X", Class: "refused"},
		{ID: "4", Kind: "fetch", Topics: []string{"orders"}, Remote: "r:5", Count: 1},
		{ID: "5", Kind: "fetch", Topics: []string{"orders"}, ClientID: "r1", Count: 6, Error: "Y", Class: "expected"},
		{ID: "6", Kind: "fetch", Topics: []string{"orders"}, ClientID: "r1", Count: 0, Error: "Z", Class: "failed"},
		{ID: "7", Kind: "metadata", Topics: []string{"orders"}, ClientID: "m"},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "orders", 1, 0)
	got := c.touched(context.Background(), "orders")
	if got.Produces != 6 || got.Fetches != 8 || got.FetchesFolded != 6 || got.Errors != 3 || !got.Exists || got.TopicURL != "/_console/topics/orders" {
		t.Errorf("%+v", got)
	}
	if strings.Join(got.Producers, ",") != "w1,w2" || strings.Join(got.Consumers, ",") != "r1,r:5" {
		t.Errorf("producers %v consumers %v", got.Producers, got.Consumers)
	}
	if c.touched(context.Background(), "nowhere").Exists {
		t.Error("a topic that is not there exists")
	}
}

func TestStoryTextAndView(t *testing.T) {
	c := newTestConsole(t)
	t0 := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	view := trafficView{TrafficFilter: TrafficFilter{Group: "g"}}
	text := func(e TrafficEvent, nested bool, before string) string {
		return storyText(e, c.trafficRow(e, view), nested, before)
	}
	if got := text(TrafficEvent{API: "Heartbeat", Group: "g", Error: "REBALANCE_IN_PROGRESS"}, false, ""); got != "existing member told to rejoin" {
		t.Errorf("%q", got)
	}
	hb := TrafficEvent{API: "Heartbeat", Group: "g", Detail: "generation 4"}
	if got := text(hb, true, ""); got != "generation 4 accepted" {
		t.Errorf("%q", got)
	}
	folded := TrafficEvent{API: "Heartbeat", Group: "g", Detail: "generation 4", Count: 3, First: t0, Time: t0.Add(6 * time.Second)}
	if got := text(folded, true, ""); got != "generation 4 accepted, every 3 s" {
		t.Errorf("%q", got)
	}
	plainFolded := TrafficEvent{API: "Heartbeat", Group: "g", Detail: "member m1", Count: 3, First: t0, Time: t0.Add(6 * time.Second)}
	if got := text(plainFolded, false, "before the join"); got != "member m1, every 3 s, before the join" {
		t.Errorf("%q", got)
	}
	if got := text(plainFolded, false, ""); got != "member m1, every 3 s" {
		t.Errorf("%q", got)
	}
	commit := TrafficEvent{API: "OffsetCommit", Group: "g", Topics: []string{"t"}, Parts: "p0"}
	if got := text(commit, false, ""); got != "t p0" {
		t.Errorf("%q", got)
	}
	commit.HasGen, commit.Gen = true, 7
	if got := text(commit, false, ""); got != "t p0, generation 7" {
		t.Errorf("%q", got)
	}
	if got := text(TrafficEvent{API: "Fetch", Group: "g", ViaGroup: "g"}, false, ""); got != "fetch by a member" {
		t.Errorf("%q", got)
	}
	if got := text(TrafficEvent{API: "JoinGroup", Group: "g", Detail: "member joined"}, false, ""); got != "member joined" {
		t.Errorf("%q", got)
	}

	rb := TrafficRebalance{Start: t0, End: t0.Add(1500 * time.Millisecond), Done: true, From: 3, To: 4, Cause: "m2 joined", Members: 2, MembersBefore: 1,
		Rows: []TrafficEvent{{API: "JoinGroup", Group: "g", Conn: 5, ID: "j1"}}}
	running := TrafficRebalance{Start: t0, From: 4, To: 5, Cause: "", Members: 3}
	st := TrafficStory{Group: "g", Protocol: "classic", Requests: 9, Folded: 2, Rebalances: []TrafficRebalance{running, rb},
		Before: []TrafficEvent{{API: "Heartbeat", Group: "g", ID: "h1", Conn: 6}}}
	sv := c.storyView(st, map[uint64]bool{5: true})
	if sv.Group != "g" || sv.Requests != 9 || sv.Folded != 2 || sv.Rebalances != 2 || sv.Epochs || sv.BeforeGen != "generation 3" {
		t.Errorf("%+v", sv)
	}
	if len(sv.Blocks) != 2 || sv.Blocks[0].Title != "the group's membership changed · rebalancing now" || sv.Blocks[0].Dur != "" || sv.Blocks[0].Epoch {
		t.Errorf("running block: %+v", sv.Blocks[0])
	}
	if b := sv.Blocks[1]; b.Title != "m2 joined · 2 members now · 2 ms" && b.Title != "m2 joined · 2 members now · 1.5 s" || len(b.Rows) != 1 || !b.Rows[0].Nested || b.Rows[0].ConnKey != "5" {
		t.Errorf("done block: %+v", b)
	}
	if len(sv.Before) != 1 || sv.Before[0].Nested || sv.Before[0].ConnKey != "" {
		t.Errorf("before: %+v", sv.Before)
	}
	// The last rebalance in the list decides the wording of "before".
	st.Rebalances = []TrafficRebalance{running, rb}
	st.Before = []TrafficEvent{{API: "Heartbeat", Group: "g", Detail: "d", Count: 2, First: t0, Time: t0.Add(2 * time.Second)}}
	if got := c.storyView(st, nil).Before[0].StoryText; got != "d, every 2 s, before the join" {
		t.Errorf("%q", got)
	}
	st.Rebalances = []TrafficRebalance{rb, running}
	if got := c.storyView(st, nil).Before[0].StoryText; got != "d, every 2 s, before the rebalance" {
		t.Errorf("%q", got)
	}
	st.Rebalances = nil
	if got := c.storyView(st, nil).Before[0].StoryText; got != "d, every 2 s" {
		t.Errorf("%q", got)
	}

	// KIP-848 groups have epochs, not generations.
	e := TrafficStory{Group: "g", Protocol: "848", Rebalances: []TrafficRebalance{{Done: true, From: 8, To: 9, Cause: "x joined", Members: 1, Start: t0, End: t0.Add(time.Second)}}}
	sv = c.storyView(e, nil)
	if !sv.Epochs || sv.BeforeGen != "epoch 8" || !sv.Blocks[0].Epoch || sv.Blocks[0].Title != "x joined · 1 member now" {
		t.Errorf("848: %+v", sv)
	}
	if sv = c.storyView(TrafficStory{Group: "g", Rebalances: []TrafficRebalance{{Done: true, From: 0, Cause: "c", Members: 1}}}, nil); sv.BeforeGen != "" {
		t.Errorf("no generation before: %q", sv.BeforeGen)
	}
}

func TestGroupSummaryWhyTheLastRebalanceHappened(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "gs", 1, 1)
	commitFor(t, addr, "grp", "gs", map[int32]int64{0: 0})
	ctx := context.Background()
	done := TrafficRebalance{Done: true, From: 6, To: 7, Cause: "a member joined", Members: 3, MembersBefore: 2}
	st := TrafficStory{Group: "grp", Protocol: "classic", Topics: []string{"gs"}, Rebalances: []TrafficRebalance{{From: 7, To: 8, Cause: "still going"}, done}, Generations: []TrafficGeneration{{Gen: 7}}}
	g := c.groupSummary(ctx, "grp", st)
	if g.Group != "grp" || g.GroupURL != "/_console/groups/grp" || g.TimelineURL != "/_console/groups/grp?tab=timeline" || g.State != "Empty" || g.StateTone != "" ||
		g.Protocol != "classic protocol" || !g.Has || g.Why != "A member joined." || !g.JoinNote || g.Epochs || len(g.Generations) != 1 || len(g.Topics) != 1 {
		t.Errorf("%+v", g)
	}
	if g.WhyDetail != "The group had 2 members at generation 6 and 3 at generation 7." {
		t.Errorf("%q", g.WhyDetail)
	}
	// No members before, or no earlier generation: no detail.
	done.MembersBefore = 0
	st.Rebalances = []TrafficRebalance{done}
	if g = c.groupSummary(ctx, "grp", st); g.WhyDetail != "" {
		t.Errorf("%q", g.WhyDetail)
	}
	done.MembersBefore, done.From = 2, 0
	st.Rebalances = []TrafficRebalance{done}
	if g = c.groupSummary(ctx, "grp", st); g.WhyDetail != "" {
		t.Errorf("%q", g.WhyDetail)
	}
	// A rebalance still going explains nothing; an empty cause is the default sentence; epochs have no join note.
	st.Rebalances = []TrafficRebalance{{Cause: "x"}}
	if g = c.groupSummary(ctx, "grp", st); g.Has || g.Why != "" {
		t.Errorf("%+v", g)
	}
	st.Protocol = "848"
	st.Rebalances = []TrafficRebalance{{Done: true, From: 3, To: 4, Members: 2, MembersBefore: 1}}
	g = c.groupSummary(ctx, "grp", st)
	if g.Why != "The group's membership changed." || g.JoinNote || !g.Epochs || g.Protocol != "KIP-848 protocol" || g.WhyDetail != "The group had 1 member at epoch 3 and 2 at epoch 4." {
		t.Errorf("%+v", g)
	}
	st.Protocol, st.Rebalances = "848", []TrafficRebalance{{Done: true, Cause: "m joined", Members: 1}}
	if g = c.groupSummary(ctx, "grp", st); g.JoinNote {
		t.Error("a join note for a KIP-848 group")
	}
	// A group the broker does not know has no state.
	if g = c.groupSummary(ctx, "ghost", TrafficStory{Group: "ghost"}); g.State != "" || g.Has {
		t.Errorf("%+v", g)
	}
}
