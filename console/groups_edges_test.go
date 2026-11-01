package console

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func groupPage(t *testing.T, c *Console, id, query string) map[string]any {
	t.Helper()
	r := httptest.NewRequest("GET", "/_console/groups/"+id+query, nil)
	data, err := c.groupData(r, id)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func lagRowsOf(data map[string]any) map[string]groupLagRow {
	out := map[string]groupLagRow{}
	for _, r := range data["Offsets"].([]groupLagRow) {
		out[r.Topic+"/"+itoa(int(r.Partition))] = r
	}
	return out
}

// Each committed offset's row on the group page: its lag and how much of the log that
// is, whether it is ahead of the log or behind where the log now starts.
func TestGroupPageLagRows(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "lg", 4, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.be.CreateTopic(ctx, "lt", 2, nil); err != nil {
		t.Fatal(err)
	}
	for p := int32(0); p < 4; p++ {
		bulkProduce(t, cl, "lg", p, 100, vals("m"), time.Time{})
	}
	for p := int32(0); p < 2; p++ {
		bulkProduce(t, cl, "lt", p, 50, vals("m"), time.Time{})
		deleteBefore(t, cl, "lt", p, 20)
	}
	commitFor(t, addr, "g", "lg", map[int32]int64{0: 90, 1: 99, 2: 100, 3: 100})
	commitFor(t, addr, "g", "lt", map[int32]int64{0: 20, 1: 10})
	commitFor(t, addr, "ahead", "lg", map[int32]int64{0: 150})
	commitFor(t, addr, "caught", "lg", map[int32]int64{0: 100})
	commitFor(t, addr, "tie", "lg", map[int32]int64{0: 90, 1: 90})

	rows := lagRowsOf(groupPage(t, c, "g", ""))
	for key, want := range map[string]struct {
		lag     int64
		percent int
		trimmed bool
	}{
		"lg/0": {10, 10, false}, "lg/1": {1, 6, false}, "lg/2": {0, 0, false}, "lt/0": {30, 60, false}, "lt/1": {40, 80, true},
	} {
		r := rows[key]
		if r.Lag != want.lag || r.LagPercent != want.percent || r.Trimmed != want.trimmed || r.Ahead || r.Deleted {
			t.Errorf("%s: %+v, want lag %d, %d%%, trimmed=%v", key, r, want.lag, want.percent, want.trimmed)
		}
	}
	if r := rows["lt/0"]; r.Earliest != 20 || r.Latest != 50 {
		t.Errorf("lt/0 bounds %d..%d", r.Earliest, r.Latest)
	}
	data := groupPage(t, c, "g", "")
	if data["TotalLag"].(int64) != 10+1+30+40 {
		t.Errorf("total lag %v", data["TotalLag"])
	}
	if lg := data["Largest"].(*groupLagRow); lg.Topic != "lt" || lg.Partition != 1 {
		t.Errorf("largest %+v", lg)
	}

	data = groupPage(t, c, "ahead", "")
	if r := lagRowsOf(data)["lg/0"]; !r.Ahead || r.AheadBy != 50 || r.Lag != 0 || data["AnyAhead"] != true {
		t.Errorf("ahead: %+v", r)
	}
	if _, ok := data["Largest"]; ok {
		t.Error("a group with no lag names a largest lag")
	}
	data = groupPage(t, c, "caught", "")
	if _, ok := data["Largest"]; ok || data["TotalLag"].(int64) != 0 {
		t.Errorf("caught up: %v", data["TotalLag"])
	}
	// Equal lags: the first stays the largest.
	if lg := groupPage(t, c, "tie", "")["Largest"].(*groupLagRow); lg.Partition != 0 {
		t.Errorf("a tie went to partition %d", lg.Partition)
	}
}

func TestGroupPageTabsAndActions(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "tb", 1, nil); err != nil {
		t.Fatal(err)
	}
	commitFor(t, addr, "tabs", "tb", map[int32]int64{0: 0})
	for query, want := range map[string]string{"": "lag", "?tab=members": "members", "?tab=timeline": "timeline", "?tab=offsets": "offsets", "?tab=bogus": "lag"} {
		data := groupPage(t, c, "tabs", query)
		if data["Tab"] != want {
			t.Errorf("%q: tab %v, want %s", query, data["Tab"], want)
		}
		for _, tl := range data["Tabs"].([]tabLink) {
			wantURL := c.groupURL("tabs")
			if tl.Key != "lag" {
				wantURL += "?tab=" + tl.Key
			}
			if tl.URL != wantURL || tl.On != (tl.Key == want) {
				t.Errorf("%q: tab %s is %+v", query, tl.Key, tl)
			}
			if tl.Key == "members" && tl.Count != "0" {
				t.Errorf("members count %q", tl.Count)
			}
		}
	}
	data := groupPage(t, c, "tabs", "")
	if data["CanAct"] != true || data["NoCommits"] != false {
		t.Errorf("an empty group with offsets: CanAct %v, NoCommits %v", data["CanAct"], data["NoCommits"])
	}
}

// The group list's summary: who has members, the lag in all, and which groups
// with nobody in them are behind.
func TestGroupsHomeCountsAndNeedsALook(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "gh", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "gh", 0, 10, vals("m"), time.Time{})
	commitFor(t, addr, "behind", "gh", map[int32]int64{0: 4})
	commitFor(t, addr, "caughtup", "gh", map[int32]int64{0: 10})
	commitFor(t, addr, "overshoot", "gh", map[int32]int64{0: 99})
	live, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup("live"), kgo.ConsumeTopics("gh"), kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	go func() {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		live.PollFetches(pctx)
	}()
	deadline := time.Now().Add(8 * time.Second)
	for {
		gs, err := c.be.ListGroups(ctx)
		if err != nil {
			t.Fatal(err)
		}
		joined := false
		for _, g := range gs {
			joined = joined || (g.ID == "live" && g.Members == 1)
		}
		if joined {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the consumer never joined")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// A group with a member and nothing committed: nothing to act on, no commits to show.
	if d := groupPage(t, c, "live", ""); d["NoCommits"] != true || d["CanAct"] != false {
		t.Errorf("a live group without commits: NoCommits %v, CanAct %v", d["NoCommits"], d["CanAct"])
	}
	data, err := c.groupsHomeData(httptest.NewRequest("GET", "/_console/groups", nil))
	if err != nil {
		t.Fatal(err)
	}
	if data["WithMembers"] != 1 || data["MemberCount"] != 1 {
		t.Errorf("with members %v, members %v; want 1 and 1", data["WithMembers"], data["MemberCount"])
	}
	if data["TotalLag"].(int64) != 6 {
		t.Errorf("total lag %v, want 6 (an offset past the end is no lag)", data["TotalLag"])
	}
	need := data["NeedALook"].([]groupRow)
	if len(need) != 1 || need[0].ID != "behind" || need[0].Lag != 6 || !need[0].Behind {
		t.Errorf("needs a look: %+v", need)
	}
}

// A group's coordinator generation is the latest the recorder knows.
func TestGroupGenerationComesFromTheLatestEvent(t *testing.T) {
	src := &fakeTraffic{groups: map[string][]TrafficGroupEvent{"gen": {
		{Seq: 1, Kind: "join", Generation: 3}, {Seq: 2, Kind: "join", Generation: 4}, {Seq: 3, Kind: "note", Generation: 0},
	}}}
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	if err := c.be.CreateTopic(context.Background(), "gn", 1, nil); err != nil {
		t.Fatal(err)
	}
	commitFor(t, addr, "gen", "gn", map[int32]int64{0: 0})
	if g := groupPage(t, c, "gen", "")["Generation"]; g != int32(4) {
		t.Errorf("generation %v, want 4", g)
	}
	rows := c.groupRows(context.Background(), []GroupSummary{{ID: "gen"}, {ID: "none"}})
	if rows[0].Generation != 4 || rows[1].Generation != 0 {
		t.Errorf("generations %d, %d; want 4, 0", rows[0].Generation, rows[1].Generation)
	}
	// A generation of zero is not a generation.
	src.groups["gen"] = []TrafficGroupEvent{{Seq: 1, Generation: 0}}
	if g := groupPage(t, c, "gen", "")["Generation"]; g != int32(0) {
		t.Errorf("generation %v, want 0", g)
	}
}

// groupRows: lag is against the log end and never negative; a deleted topic's leftover
// offset is not lag and does not count as an offset.
func TestGroupRowsLagAndLeftovers(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	for _, n := range []string{"ra", "rb", "gone"} {
		if err := c.be.CreateTopic(ctx, n, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	bulkProduce(t, cl, "ra", 0, 10, vals("m"), time.Time{})
	bulkProduce(t, cl, "rb", 0, 10, vals("m"), time.Time{})
	commitFor(t, addr, "multi", "ra", map[int32]int64{0: 7})
	commitFor(t, addr, "multi", "rb", map[int32]int64{0: 8})
	commitFor(t, addr, "past", "ra", map[int32]int64{0: 30})
	commitFor(t, addr, "orphan", "gone", map[int32]int64{0: 1})
	if err := c.be.DeleteTopic(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	rows := c.groupRows(ctx, []GroupSummary{{ID: "multi"}, {ID: "past"}, {ID: "orphan"}})
	if r := rows[0]; r.Lag != 3+2 || !r.Behind || !r.HasOffsets || strings.Join(r.Topics, ",") != "ra,rb" {
		t.Errorf("multi: %+v", r)
	}
	if r := rows[1]; r.Lag != 0 || r.Behind || !r.HasOffsets {
		t.Errorf("past: %+v", r)
	}
	if r := rows[2]; r.Lag != 0 || r.Behind || r.HasOffsets || len(r.Topics) != 0 {
		t.Errorf("orphan: %+v", r)
	}
}
