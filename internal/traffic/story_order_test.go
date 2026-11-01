package traffic

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// The requests that lead to a rebalance begin before the coordinator notes it: the
// client finds the coordinator, is told MEMBER_ID_REQUIRED, and joins. They belong to the
// rebalance they began, not to the generation before it (Traf-12).
func TestTheRequestsThatTriggerARebalanceAreUnderIt(t *testing.T) {
	r := New(100, nil)
	now := t0
	r.now = func() time.Time { return now }
	at := func(d time.Duration) { now = t0.Add(d) }
	r.ConnOpened(1, "a")
	grp := func(o *protocol.Observation) { o.Group, o.Kind = "g", "groups" }

	at(0)
	r.GroupEvent("g", "generation", "m1", 6, "1 member", 1, "")
	r.Request(obs(1, "app", kmsg.OffsetCommit, 10*time.Second, grp))
	// The new member: FindCoordinator, JoinGroup (asked for a member id), JoinGroup. The
	// coordinator notes the rebalance at the last of them.
	r.Request(obs(1, "new", kmsg.FindCoordinator, 20*time.Second, func(o *protocol.Observation) { o.Group, o.Kind = "g", "groups" }))
	r.Request(obs(1, "new", kmsg.JoinGroup, 20*time.Second+300*time.Millisecond, grp, func(o *protocol.Observation) {
		o.ErrorCode, o.Class = 79, protocol.ClassExpected
	}))
	r.Request(obs(1, "new", kmsg.JoinGroup, 20*time.Second+600*time.Millisecond, grp))
	at(20*time.Second + 700*time.Millisecond)
	r.GroupEvent("g", "rebalance", "m2", 6, "a member joined", 1, "a member joined")
	at(23 * time.Second)
	r.GroupEvent("g", "generation", "m1", 7, "2 members", 2, "")
	r.Request(obs(1, "new", kmsg.SyncGroup, 23*time.Second+100*time.Millisecond, grp))

	s := r.Story("g")
	if len(s.Rebalances) != 1 {
		t.Fatalf("%d rebalances: %+v", len(s.Rebalances), s.Rebalances)
	}
	var apis []string
	for _, e := range s.Rebalances[0].Rows {
		apis = append(apis, e.API)
	}
	want := []string{"FindCoordinator", "JoinGroup", "JoinGroup", "SyncGroup"}
	if len(apis) != len(want) {
		t.Fatalf("under the rebalance: %v, want %v", apis, want)
	}
	for i := range want {
		if apis[i] != want[i] {
			t.Errorf("under the rebalance: %v, want %v (in the order they happened)", apis, want)
			break
		}
	}
	if len(s.Before) != 1 || s.Before[0].API != "OffsetCommit" {
		t.Errorf("before = %+v, want only the earlier OffsetCommit", s.Before)
	}
}

// A run of heartbeats folded into one row that began in the generation before and was
// still beating in the next sits under the rebalance it was last seen in: a block that
// ended before its last beat would say it is over while the member is still beating.
func TestAFoldedHeartbeatAcrossGenerationsSitsWhereItWasLastSeen(t *testing.T) {
	r := New(100, nil)
	now := t0
	r.now = func() time.Time { return now }
	at := func(d time.Duration) { now = t0.Add(d) }
	r.ConnOpened(1, "a")
	grp := func(o *protocol.Observation) { o.Group, o.Kind = "g", "groups" }

	at(0)
	r.GroupEvent("g", "generation", "m1", 1, "1 member", 1, "")
	at(20 * time.Second)
	r.GroupEvent("g", "rebalance", "m2", 1, "a member joined", 1, "a member joined")
	at(22 * time.Second)
	r.GroupEvent("g", "generation", "m1", 2, "2 members", 2, "")
	// Beats from before the rebalance to after it, all identical: one row.
	for i := 1; i <= 10; i++ {
		r.Request(obs(1, "app", kmsg.Heartbeat, time.Duration(i)*3*time.Second, idle("g"), grp))
	}
	s := r.Story("g")
	if len(s.Rebalances) != 1 {
		t.Fatalf("%d rebalances", len(s.Rebalances))
	}
	for _, e := range s.Before {
		if e.Time.After(s.Rebalances[0].Start) {
			t.Errorf("a row last seen %v sits before the rebalance that began %v: %+v", e.Time.Sub(t0), s.Rebalances[0].Start.Sub(t0), e)
		}
	}
	found := false
	for _, e := range s.Rebalances[0].Rows {
		if e.API == "Heartbeat" {
			found = true
		}
	}
	if !found {
		t.Errorf("the folded heartbeat, last seen after the rebalance began, is not under it: %+v", s)
	}
}
