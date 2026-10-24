package traffic

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// storyRig is a recorder whose clock the test moves, with a group "g".
type storyRig struct {
	r   *Recorder
	now time.Time
}

func newStoryRig() *storyRig {
	s := &storyRig{now: t0}
	s.r = New(100, nil)
	s.r.now = func() time.Time { return s.now }
	s.r.ConnOpened(1, "a")
	return s
}

func (s *storyRig) at(d time.Duration) { s.now = t0.Add(d) }

func (s *storyRig) rebalance(d time.Duration) {
	s.at(d)
	s.r.GroupEvent("g", "rebalance", "m", 1, "a member joined", 1, "a member joined")
}

func (s *storyRig) generation(d time.Duration, gen int32) {
	s.at(d)
	s.r.GroupEvent("g", "generation", "m", gen, "", 1, "")
}

func (s *storyRig) request(d time.Duration, api protocol.APIKey, detail string) {
	s.r.Request(obs(1, "c", api, d, func(o *protocol.Observation) {
		o.Group, o.Kind, o.Detail = "g", "groups", detail
	}))
}

// where reports which block the request with this API and detail sits in: the
// rebalance's "To" generation, -1 for the requests before the oldest rebalance, or -2
// for not found.
func where(st Story, api, detail string) int {
	for _, e := range st.Before {
		if e.API == api && e.Detail == detail {
			return -1
		}
	}
	for _, rb := range st.Rebalances {
		for _, e := range rb.Rows {
			if e.API == api && e.Detail == detail {
				return int(rb.To)
			}
		}
	}
	return -2
}

// A request that names a generation goes under the rebalance that opened it, whatever
// the time it was sent; one that names a generation the recorder never saw is placed by
// its time; and only a JoinGroup or SyncGroup names one.
func TestARequestThatNamesAGenerationGoesUnderItsRebalance(t *testing.T) {
	s := newStoryRig()
	s.generation(0, 1)
	s.rebalance(10 * time.Second)
	s.generation(12*time.Second, 2)
	s.rebalance(20 * time.Second)
	s.generation(22*time.Second, 3)
	s.request(21*time.Second, kmsg.JoinGroup, "generation 2")
	s.request(30*time.Second, kmsg.SyncGroup, "generation 2 · leader")
	s.request(31*time.Second, kmsg.Heartbeat, "generation 2")
	s.request(32*time.Second, kmsg.SyncGroup, "generation 9")
	s.request(33*time.Second, kmsg.JoinGroup, "generation 7 · member")
	st := s.r.Story("g")
	for _, c := range []struct {
		api, detail string
		want        int
	}{
		{"JoinGroup", "generation 2", 2},
		{"SyncGroup", "generation 2 · leader", 2},
		{"Heartbeat", "generation 2", 3},
		{"SyncGroup", "generation 9", 3},
		{"JoinGroup", "generation 7 · member", 3},
	} {
		if got := where(st, c.api, c.detail); got != c.want {
			t.Errorf("%s %q is under generation %d, want %d", c.api, c.detail, got, c.want)
		}
	}
}

// A request naming a generation the recorder knows of but saw no rebalance for goes in
// the requests before the oldest rebalance.
func TestARequestNamingAnUnseenGenerationGoesBefore(t *testing.T) {
	s := newStoryRig()
	s.generation(0, 1)
	s.rebalance(10 * time.Second)
	s.generation(12*time.Second, 2)
	s.request(11*time.Second, kmsg.SyncGroup, "generation 1")
	st := s.r.Story("g")
	if got := where(st, "SyncGroup", "generation 1"); got != -1 {
		t.Errorf("under %d, want before the oldest rebalance", got)
	}
}

// A request that names no generation is not taken for one that names generation 0.
func TestARequestNamingNoGenerationIsNotPlacedUnderGenerationZero(t *testing.T) {
	s := newStoryRig()
	s.rebalance(10 * time.Second)
	s.generation(12*time.Second, 0)
	s.rebalance(20 * time.Second)
	s.generation(22*time.Second, 5)
	s.request(30*time.Second, kmsg.Heartbeat, "")
	if got := where(s.r.Story("g"), "Heartbeat", ""); got != 5 {
		t.Errorf("heartbeat under generation %d, want 5", got)
	}
}

// A JoinGroup that names the first (oldest) rebalance's generation stays there although
// it was sent during a later one, and so does a request that leads into a rebalance.
func TestAJoinNamingTheOldestGenerationStaysUnderIt(t *testing.T) {
	s := newStoryRig()
	s.rebalance(10 * time.Second)
	s.generation(12*time.Second, 2)
	s.rebalance(20 * time.Second)
	s.generation(22*time.Second, 3)
	s.request(21*time.Second, kmsg.JoinGroup, "generation 2")
	s.request(21500*time.Millisecond, kmsg.FindCoordinator, "")
	st := s.r.Story("g")
	if got := where(st, "JoinGroup", "generation 2"); got != 2 {
		t.Errorf("join under generation %d, want 2", got)
	}
	if got := where(st, "FindCoordinator", ""); got != 3 {
		t.Errorf("find-coordinator under generation %d, want 3 (it came during that rebalance)", got)
	}
}

// A request sent up to joinLead before a rebalance began, and no more, leads into it.
func TestARequestExactlyJoinLeadBeforeARebalanceLeadsIntoIt(t *testing.T) {
	s := newStoryRig()
	s.rebalance(joinLead)
	s.request(0, kmsg.FindCoordinator, "")
	st := s.r.Story("g")
	if len(st.Rebalances) != 1 || len(st.Rebalances[0].Rows) != 1 || len(st.Before) != 0 {
		t.Errorf("rows under the rebalance %d, before %d", len(st.Rebalances[0].Rows), len(st.Before))
	}
}

func TestNamedGeneration(t *testing.T) {
	for _, c := range []struct {
		api, detail string
		want        int32
	}{
		{"JoinGroup", "generation 9 · x", 9},
		{"JoinGroup", "generation 10", 10},
		{"SyncGroup", "generation 105 · y", 105},
		{"JoinGroup", "generation 0", 0},
		{"JoinGroup", "generation x", 0},
		{"JoinGroup", "member 3", 0},
		{"Heartbeat", "generation 4", 0},
	} {
		if got := namedGeneration(Event{API: c.api, Detail: c.detail}); got != c.want {
			t.Errorf("%s %q: %d, want %d", c.api, c.detail, got, c.want)
		}
	}
}
