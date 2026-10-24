package broker

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// The KIP-848 server assignors, run on their own. The range assignor is documented in
// group848_assign.go: each subscriber gets a contiguous slice of a topic's partitions
// and the first (n mod k) get one extra, topic by topic. The uniform one spreads all
// partitions evenly, keeps a member's partitions where it can, and gives an unowned
// one to the eligible member with the fewest, the first of them on a tie.

func assignGroup(ids ...string) *cg848Group {
	g := &cg848Group{id: "g", members: map[string]*cg848Member{}}
	for _, id := range ids {
		g.members[id] = &cg848Member{id: id, target: tpSet{}, assigned: tpSet{}}
	}
	return g
}

func topicOf(b byte, partitions int32, subscribers ...string) topicInfo {
	return topicInfo{name: fmt.Sprintf("t%d", b), id: meta.TopicID{b}, partitions: partitions, subscribers: subscribers}
}

func partsOf(g *cg848Group, member string, topic byte) []int32 {
	var out []int32
	for p := range g.members[member].target {
		if p.topic == (meta.TopicID{topic}) {
			out = append(out, p.part)
		}
	}
	slices.Sort(out)
	return out
}

// within runs f and fails the test, rather than hanging it, if f does not return.
func within(t *testing.T, d time.Duration, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not finish within %v", what, d)
	}
}

func TestAssignRangeGivesEachSubscriberAContiguousSlice(t *testing.T) {
	for _, tc := range []struct {
		partitions int32
		want       [][]int32 // for a, b, c.
	}{
		{7, [][]int32{{0, 1, 2}, {3, 4}, {5, 6}}},
		{6, [][]int32{{0, 1}, {2, 3}, {4, 5}}},
		{4, [][]int32{{0, 1}, {2}, {3}}},
		{2, [][]int32{{0}, {1}, nil}},
		{1, [][]int32{{0}, nil, nil}},
	} {
		g := assignGroup("a", "b", "c")
		within(t, 5*time.Second, "assignRange", func() { assignRange(g, topicOf(1, tc.partitions, "a", "b", "c")) })
		for i, id := range []string{"a", "b", "c"} {
			if got := partsOf(g, id, 1); !slices.Equal(got, tc.want[i]) {
				t.Errorf("%d partitions: %s got %v, want %v", tc.partitions, id, got, tc.want[i])
			}
		}
	}
}

// Topics with the same partition count line up: the same member gets the same slice of each.
func TestAssignRangeLinesUpTopicsOfTheSameSize(t *testing.T) {
	g := assignGroup("a", "b")
	within(t, 5*time.Second, "assignRange", func() {
		assignRange(g, topicOf(1, 5, "a", "b"))
		assignRange(g, topicOf(2, 5, "a", "b"))
	})
	for _, topic := range []byte{1, 2} {
		if got := partsOf(g, "a", topic); !slices.Equal(got, []int32{0, 1, 2}) {
			t.Errorf("a on topic %d: %v, want [0 1 2]", topic, got)
		}
		if got := partsOf(g, "b", topic); !slices.Equal(got, []int32{3, 4}) {
			t.Errorf("b on topic %d: %v, want [3 4]", topic, got)
		}
	}
}

func TestAssignUniformGivesTiesToTheFirstSubscriber(t *testing.T) {
	g := assignGroup("a", "b", "c")
	within(t, 5*time.Second, "assignUniform", func() {
		assignUniform(g, []topicInfo{topicOf(1, 3, "a", "b", "c")}, map[string]tpSet{})
	})
	for i, id := range []string{"a", "b", "c"} {
		if got := partsOf(g, id, 1); !slices.Equal(got, []int32{int32(i)}) {
			t.Errorf("%s got %v, want [%d]", id, got, i)
		}
	}
}

func TestAssignUniformIsEvenAndKeepsWhatAMemberHad(t *testing.T) {
	// a had everything; b and c have just arrived. Nobody is moved more than evenness needs.
	g := assignGroup("a", "b", "c")
	prev := map[string]tpSet{"a": {}}
	for p := int32(0); p < 6; p++ {
		prev["a"][tp{meta.TopicID{1}, p}] = struct{}{}
	}
	within(t, 5*time.Second, "assignUniform", func() {
		assignUniform(g, []topicInfo{topicOf(1, 6, "a", "b", "c")}, prev)
	})
	for _, id := range []string{"a", "b", "c"} {
		if n := len(g.members[id].target); n != 2 {
			t.Errorf("%s has %d partitions, want 2", id, n)
		}
	}
	// What a keeps is from what it had.
	for p := range g.members["a"].target {
		if !prev["a"].has(p) {
			t.Errorf("a was given %v, which it did not have", p)
		}
	}
	// And together they are every partition, once.
	seen := tpSet{}
	for _, m := range g.members {
		for p := range m.target {
			if seen.has(p) {
				t.Errorf("%v is assigned twice", p)
			}
			seen[p] = struct{}{}
		}
	}
	if len(seen) != 6 {
		t.Errorf("%d partitions assigned, want 6", len(seen))
	}
}

// The first partition (in topic order) can be the one that has to move to even things out.
func TestAssignUniformMovesTheVeryFirstPartitionWhenThatIsWhatEvensItOut(t *testing.T) {
	g := assignGroup("a", "b")
	prev := map[string]tpSet{"b": {}}
	prev["b"][tp{meta.TopicID{1}, 0}] = struct{}{}
	for p := int32(0); p < 3; p++ {
		prev["b"][tp{meta.TopicID{2}, p}] = struct{}{}
	}
	within(t, 5*time.Second, "assignUniform", func() {
		// Topic 1 has a single partition both want; topic 2 is only b's.
		assignUniform(g, []topicInfo{topicOf(1, 1, "a", "b"), topicOf(2, 3, "b")}, prev)
	})
	if got := partsOf(g, "a", 1); !slices.Equal(got, []int32{0}) {
		t.Errorf("a got %v of the shared topic, want [0]: b held 4 and a none", got)
	}
	if got := partsOf(g, "b", 2); !slices.Equal(got, []int32{0, 1, 2}) {
		t.Errorf("b got %v of its own topic", got)
	}
}

func TestAssignUniformOfABalancedGroupMovesNothingAndEnds(t *testing.T) {
	g := assignGroup("a", "b", "c")
	prev := map[string]tpSet{}
	for i, id := range []string{"a", "b", "c"} {
		prev[id] = tpSet{tp{meta.TopicID{1}, int32(i)}: {}}
	}
	within(t, 5*time.Second, "assignUniform", func() {
		assignUniform(g, []topicInfo{topicOf(1, 3, "a", "b", "c")}, prev)
	})
	for i, id := range []string{"a", "b", "c"} {
		if got := partsOf(g, id, 1); !slices.Equal(got, []int32{int32(i)}) {
			t.Errorf("%s got %v, want [%d]", id, got, i)
		}
	}
}

// The group's assignor is the one most members asked for; a tie, or nobody asking, is uniform.
func TestAssignPicksTheAssignorMostMembersAskedFor(t *testing.T) {
	m := new848Manager(t)
	for _, tc := range []struct {
		votes []string
		want  string
	}{
		{nil, assignorUniform},
		{[]string{"", ""}, assignorUniform},
		{[]string{assignorRange}, assignorRange},
		{[]string{assignorRange, assignorUniform}, assignorUniform},
		{[]string{assignorRange, assignorRange, assignorUniform}, assignorRange},
		{[]string{assignorRange, "", ""}, assignorRange},
	} {
		g := &cg848Group{id: "g", members: map[string]*cg848Member{}}
		for i, v := range tc.votes {
			id := fmt.Sprint("m", i)
			g.members[id] = &cg848Member{id: id, assignor: v, target: tpSet{}, assigned: tpSet{}}
		}
		m.assign(g)
		if g.assignor != tc.want {
			t.Errorf("votes %q: assignor %q, want %q", tc.votes, g.assignor, tc.want)
		}
	}
}

// The topics a group wants are resolved from the store, in name order, those that exist.
func TestSubscribedTopicsAreTheExistingOnesInNameOrder(t *testing.T) {
	m := new848Manager(t)
	for _, name := range []string{"beta", "alpha"} {
		if _, err := m.meta.CreateTopic(name, 2, nil); err != nil {
			t.Fatal(err)
		}
	}
	g := assignGroup("x", "y")
	g.members["x"].subscribed = []string{"beta", "alpha", "missing"}
	g.members["y"].subscribed = []string{"alpha"}
	got := m.subscribedTopics(g)
	if len(got) != 2 || got[0].name != "alpha" || got[1].name != "beta" {
		t.Fatalf("topics %+v, want alpha then beta", got)
	}
	if !slices.Equal(got[0].subscribers, []string{"x", "y"}) || !slices.Equal(got[1].subscribers, []string{"x"}) || got[0].partitions != 2 {
		t.Errorf("subscribers: %+v", got)
	}
}
