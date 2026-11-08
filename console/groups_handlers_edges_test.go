package console

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A timeout the member does not have is not written as "0 s".
func TestAZeroTimeoutHasNoText(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		if got := secondsText(d); got != "" {
			t.Errorf("secondsText(%v) = %q, want nothing", d, got)
		}
	}
	if got := secondsText(1500 * time.Millisecond); got != "1.5 s" {
		t.Errorf("secondsText(1.5 s) = %q", got)
	}
}

// Members that disagree are shown as the shortest and the longest of their timeouts.
func TestTimeoutsThatDisagreeAreShownAsASpan(t *testing.T) {
	if got := spanOf([]time.Duration{45 * time.Second, 10 * time.Second, 30 * time.Second}); got != "10 s to 45 s" {
		t.Errorf("a span of 10, 30 and 45 s = %q", got)
	}
	if got := spanOf([]time.Duration{45 * time.Second, 45 * time.Second}); got != "45 s" {
		t.Errorf("members that agree = %q", got)
	}
	if got := spanOf(nil); got != "" {
		t.Errorf("no timeouts = %q", got)
	}
}

// A member that reports no timeout takes no part in the span the Members tab shows.
func TestMembersWithNoTimeoutAreLeftOutOfTheFacts(t *testing.T) {
	insp := GroupInspection{State: InspectOK, Group: GroupSnapshot{Members: []MemberSnapshot{
		{ID: "a", SessionTimeout: 0, RebalanceTimeout: 0},
		{ID: "b", SessionTimeout: 45 * time.Second, RebalanceTimeout: 30 * time.Second, Leader: true},
	}}}
	f := groupFacts(insp, nil)
	if f.Session != "45 s" || f.Rebalance != "30 s" || f.Leader != "b" || f.NoLeader {
		t.Errorf("the facts = %+v, want the one timeout each that a member holds", f)
	}
}

// A heartbeat is fine while it is inside the session timeout, and not otherwise.
func TestAHeartbeatIsOKOnlyInsideTheSessionTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		session time.Duration
		beat    time.Time
		want    bool
	}{
		{"recent", 45 * time.Second, time.Now().Add(-2 * time.Second), true},
		{"older than the timeout", 5 * time.Second, time.Now().Add(-time.Minute), false},
		{"never", 45 * time.Second, time.Time{}, false},
		{"no timeout", 0, time.Now().Add(-time.Second), false},
		{"no timeout and a beat from the future", 0, time.Now().Add(time.Hour), false},
	} {
		var mv memberView
		mv.snapshotMember(MemberSnapshot{SessionTimeout: tc.session, LastHeartbeat: tc.beat})
		if mv.HeartbeatOK != tc.want {
			t.Errorf("%s: HeartbeatOK = %v, want %v", tc.name, mv.HeartbeatOK, tc.want)
		}
	}
}

// A group's generation is the coordinator's; a consumer-protocol group has none, so its
// epoch stands in, and says so.
func TestAGroupsGenerationFallsBackToItsEpochOnlyWithoutOne(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "t", 1, 3)
	commitFor(t, addr, "gen", "t", map[int32]int64{0: 1})
	for _, tc := range []struct {
		name       string
		gen, epoch int32
		want       int32
		wantLabel  string
	}{
		{"an epoch and no generation", 0, 5, 5, "epoch"},
		{"a generation and an epoch", 7, 3, 7, "generation"},
		{"neither", 0, 0, 0, "generation"},
	} {
		c.inspector = fakeInspector{"gen": {ID: "gen", State: "Empty", Generation: tc.gen, Epoch: tc.epoch}}
		r := httptest.NewRequest("GET", "/_console/groups/gen", nil)
		data, err := c.groupData(r, "gen")
		if err != nil {
			t.Fatal(err)
		}
		if data["Generation"] != tc.want || data["GenLabel"] != tc.wantLabel {
			t.Errorf("%s: generation %v (%v), want %d (%s)", tc.name, data["Generation"], data["GenLabel"], tc.want, tc.wantLabel)
		}
	}
}

// emptyGroupWithoutOffsets makes a group that exists, has no member and has committed
// nothing: a member joins and leaves.
func emptyGroupWithoutOffsets(t *testing.T, c *Console, addr, group, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	cl.PollFetches(ctx)
	cancel()
	cl.CloseAllowingRebalance()
	deadline := time.Now().Add(5 * time.Second)
	for {
		d, err := c.be.DescribeGroup(context.Background(), group)
		offs, _ := c.be.GroupOffsets(context.Background(), group)
		if err == nil && len(d.Members) == 0 && len(offs) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the group never became empty and uncommitted: %+v %v %v", d, offs, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A group nothing has been committed for has no offsets to expire, and none to reset.
func TestAnEmptyGroupWithNoOffsetsHasNothingToExpireOrReset(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "t", 1, 3)
	emptyGroupWithoutOffsets(t, c, addr, "bare", "t")
	c.inspector = expiringInspector{fakeInspector{}, OffsetExpiry{}, true}

	data, err := c.groupData(httptest.NewRequest("GET", "/_console/groups/bare", nil), "bare")
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := data["Expiry"].(*expiryView); e != nil {
		t.Errorf("a group with no offsets has an expiry: %+v", e)
	}
	data, err = c.groupData(httptest.NewRequest("GET", "/_console/groups/bare?tab=offsets", nil), "bare")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := data["Reset"]; ok {
		t.Error("a group with no offsets is offered a reset form")
	}
	if rows, _ := data["NoCommitRows"].([]noCommitRow); len(rows) != 0 {
		t.Errorf("a group with no members lists partitions without commits: %+v", rows)
	}
}

// A member with partitions and no commit is listed with where the log ends; a group that
// has commits lists none.
func TestPartitionsWithoutACommitAreListedOnlyForAGroupWithNoOffsets(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "t", 1, 4)
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup("fresh"), kgo.ConsumeTopics("t"), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		cl.PollFetches(ctx)
	}()
	deadline := time.Now().Add(8 * time.Second)
	for {
		data, err := c.groupData(httptest.NewRequest("GET", "/_console/groups/fresh", nil), "fresh")
		if err == nil {
			if rows, _ := data["NoCommitRows"].([]noCommitRow); len(rows) == 1 {
				if !rows[0].HasEnd || rows[0].LogEnd != 4 || rows[0].Topic != "t" {
					t.Fatalf("the row = %+v, want topic t ending at 4", rows[0])
				}
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no partition without a commit was listed: %v", data["NoCommitRows"])
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Commit for a second group first, then let a member join it: it has offsets.
	commitFor(t, addr, "busy", "t", map[int32]int64{0: 1})
	busy, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup("busy"), kgo.ConsumeTopics("t"), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		busy.PollFetches(ctx)
	}()
	deadline = time.Now().Add(8 * time.Second)
	for {
		d, err := c.be.DescribeGroup(context.Background(), "busy")
		if err == nil && len(d.Members) == 1 && len(d.Members[0].Assigned["t"]) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the member never joined")
		}
		time.Sleep(50 * time.Millisecond)
	}
	data, err := c.groupData(httptest.NewRequest("GET", "/_console/groups/busy", nil), "busy")
	if err != nil {
		t.Fatal(err)
	}
	if rows, _ := data["NoCommitRows"].([]noCommitRow); len(rows) != 0 {
		t.Errorf("a group with a commit lists partitions without one: %+v", rows)
	}
}

// keysStrip is the text of the keys strip at the foot of a page.
func keysStrip(body string) string {
	i := strings.Index(body, `<footer class="keys"`)
	if i < 0 {
		return ""
	}
	end := strings.Index(body[i:], "</footer>")
	return readable(body[i : i+end])
}

// With no group, the page is the one that says how a group appears, with its own keys,
// and names a topic that exists; with one, it is the list.
func TestTheGroupsHomeNamesATopicAndKeepsItsKeysForTheEmptyCase(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "orders", 1, 1)
	empty := req(t, c, "GET", "/_console/groups", nil).Body.String()
	if !strings.Contains(empty, "--topic orders") {
		t.Error("the example command names no topic that exists")
	}
	if keys := keysStrip(empty); !strings.Contains(keys, "gggroups") || strings.Contains(keys, "/filter") || strings.Contains(keys, "jkmove") {
		t.Errorf("the empty page keeps the keys of a list: %q", keys)
	}
	commitFor(t, addr, "some", "orders", map[int32]int64{0: 1})
	full := req(t, c, "GET", "/_console/groups", nil).Body.String()
	if keys := keysStrip(full); !strings.Contains(keys, "/filter") || !strings.Contains(keys, "jkmove") {
		t.Errorf("the list page lacks the keys of a list: %q", keys)
	}
}
