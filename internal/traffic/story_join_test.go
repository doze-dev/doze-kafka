package traffic

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

func rowsOf(rb Rebalance) []string {
	var out []string
	for _, e := range rb.Rows {
		out = append(out, e.API+" "+e.Detail)
	}
	return out
}

// Three members join 0.4 s apart: the second and third arrive while a rebalance is in
// progress or just after one. A JoinGroup goes under the rebalance that produced the
// generation its response names, a FindCoordinator that arrives during a rebalance stays
// in it, and neither is filed under the next block (Traf-12).
func TestJoinsGoUnderTheRebalanceThatProducedTheirGeneration(t *testing.T) {
	r := New(100, nil)
	now := t0
	r.now = func() time.Time { return now }
	at := func(d time.Duration) { now = t0.Add(d) }
	r.ConnOpened(1, "a")
	grp := func(o *protocol.Observation) { o.Group, o.Kind = "g", "groups" }
	det := func(s string) func(*protocol.Observation) { return func(o *protocol.Observation) { o.Detail = s } }
	ms := time.Millisecond

	at(0)
	r.GroupEvent("g", "generation", "m1", 2, "1 member", 1, "")
	// Member two: rebalance 2 -> 3 noted at 20.4 s, generation 3 at 26 s.
	r.Request(obs(1, "b", kmsg.FindCoordinator, 20*time.Second, grp))
	r.Request(obs(1, "b", kmsg.JoinGroup, 20*time.Second+300*ms, grp))
	at(20*time.Second + 400*ms)
	r.GroupEvent("g", "rebalance", "m2", 2, "a member joined", 1, "a member joined")
	// Member three, 0.4 s later: during the rebalance; its generation is 3.
	r.Request(obs(1, "c", kmsg.FindCoordinator, 20*time.Second+800*ms, grp))
	r.Request(obs(1, "c", kmsg.JoinGroup, 21*time.Second+200*ms, grp, det("generation 3")))
	// Member four, 0.4 s after that, still during it.
	r.Request(obs(1, "d", kmsg.FindCoordinator, 21*time.Second+600*ms, grp))
	// Member two's join was sent before the rebalance was noted, answered with 3.
	r.Request(obs(1, "b", kmsg.JoinGroup, 20*time.Second+350*ms, grp, det("generation 3")))
	at(22 * time.Second)
	r.GroupEvent("g", "generation", "m1", 3, "3 members", 3, "")
	// Member four's second join: the next rebalance 3 -> 4 is noted at 22.9 s.
	r.Request(obs(1, "d", kmsg.JoinGroup, 22*time.Second+500*ms, grp, det("generation 4")))
	at(22*time.Second + 900*ms)
	r.GroupEvent("g", "rebalance", "m4", 3, "a member joined", 3, "a member joined")
	at(26 * time.Second)
	r.GroupEvent("g", "generation", "m1", 4, "4 members", 4, "")

	s := r.Story("g")
	if len(s.Rebalances) != 2 {
		t.Fatalf("%d rebalances", len(s.Rebalances))
	}
	newer, older := s.Rebalances[0], s.Rebalances[1]
	if older.To != 3 || newer.To != 4 {
		t.Fatalf("blocks %d->%d and %d->%d", older.From, older.To, newer.From, newer.To)
	}
	count := func(rb Rebalance, api, detail string) int {
		n := 0
		for _, e := range rb.Rows {
			if e.API == api && e.Detail == detail {
				n++
			}
		}
		return n
	}
	if got := count(older, "JoinGroup", "generation 3"); got != 2 {
		t.Errorf("under 2 -> 3, JoinGroup generation 3 x%d, want 2: %v", got, rowsOf(older))
	}
	if got := count(older, "FindCoordinator", ""); got != 3 {
		t.Errorf("under 2 -> 3, FindCoordinator x%d, want 3 (one arrived mid-rebalance): %v", got, rowsOf(older))
	}
	if count(newer, "JoinGroup", "generation 3") != 0 || count(newer, "FindCoordinator", "") != 0 {
		t.Errorf("generation 3's requests filed under 3 -> 4: %v", rowsOf(newer))
	}
	if got := count(newer, "JoinGroup", "generation 4"); got != 1 {
		t.Errorf("under 3 -> 4, JoinGroup generation 4 x%d, want 1: %v", got, rowsOf(newer))
	}
}

// A LeaveGroup is the cause of the rebalance that follows it: it goes under that one,
// not under the generation the member leaves, after its last heartbeat.
func TestALeaveGroupIsUnderTheRebalanceItTriggers(t *testing.T) {
	r := New(100, nil)
	now := t0
	r.now = func() time.Time { return now }
	at := func(d time.Duration) { now = t0.Add(d) }
	r.ConnOpened(1, "a")
	grp := func(o *protocol.Observation) { o.Group, o.Kind = "g", "groups" }

	at(0)
	r.GroupEvent("g", "generation", "m1", 1, "1 member", 1, "")
	at(10 * time.Second)
	r.GroupEvent("g", "rebalance", "m2", 1, "a member joined", 1, "a member joined")
	at(12 * time.Second)
	r.GroupEvent("g", "generation", "m1", 2, "2 members", 2, "")
	r.Request(obs(1, "b", kmsg.Heartbeat, 20*time.Second, idle("g"), grp))
	r.Request(obs(1, "b", kmsg.LeaveGroup, 25*time.Second, grp))
	at(25*time.Second + 50*time.Millisecond)
	r.GroupEvent("g", "rebalance", "m1", 2, "a member left", 2, "a member left")
	at(27 * time.Second)
	r.GroupEvent("g", "generation", "m1", 3, "1 member", 1, "")

	s := r.Story("g")
	if len(s.Rebalances) != 2 {
		t.Fatalf("%d rebalances", len(s.Rebalances))
	}
	newer, older := s.Rebalances[0], s.Rebalances[1]
	for _, e := range older.Rows {
		if e.API == "LeaveGroup" {
			t.Errorf("LeaveGroup under 1 -> 2, the generation it left: %v", rowsOf(older))
		}
	}
	found := false
	for _, e := range newer.Rows {
		found = found || e.API == "LeaveGroup"
	}
	if !found {
		t.Errorf("LeaveGroup is not under 2 -> 3, the rebalance it triggered: %v", rowsOf(newer))
	}
}
