package broker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// Gr-04 / Gr-17 (GRP-23): an expiry says how long the member was silent, a leave says it
// said goodbye, and the generation that ends a wait says how long the wait was.
func TestClassicEventsSayHowLongAndWhy(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(func(string) bool { return true }, nil)
	log := &eventLog{}
	gm.sink = log
	a, _ := gm.join(context.Background(), joinReq("g", ""), clientRef{clientID: "app-1"})
	g := gm.groups["g"]
	m := g.members[a.memberID]
	m.lastHeartbeat = time.Now().Add(-10 * time.Second)
	gm.sessionExpired(g, m, m.session.gen)
	e, ok := log.find("expired")
	if !ok || !strings.Contains(e.detail, "no heartbeat for 10.0 s (its session timeout)") {
		t.Errorf("expired event = %+v", e)
	}
	b, _ := gm.join(context.Background(), joinReq("h", ""), clientRef{clientID: "app-2"})
	lr := kmsg.NewPtrLeaveGroupRequest()
	lr.SetVersion(0)
	lr.Group, lr.MemberID = "h", b.memberID
	gm.leave(lr)
	if e, _ := log.find("left"); !strings.Contains(e.detail, "said goodbye (LeaveGroup)") {
		t.Errorf("left event = %+v", e)
	}
}

// GRP-25: a rebalance in progress shows when it began, when it gives up waiting and who
// has rejoined; the timeout is the longest any member asked for, with no cap.
func TestSnapshotOfARebalanceCarriesTheDeadlineAndWhoRejoined(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	first, _ := gm.join(context.Background(), joinReq("g", ""), clientRef{clientID: "slow"})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = gm.join(context.Background(), joinReq("g", ""), clientRef{clientID: "fast"})
	}()
	var snap GroupSnapshot
	for i := 0; i < 2000; i++ {
		snap = gm.inspect(time.Now())[0]
		if snap.State == "PreparingRebalance" && len(snap.Members) == 2 && !snap.RebalanceDeadline.IsZero() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if snap.State != "PreparingRebalance" || snap.RebalanceStarted.IsZero() || !snap.RebalanceDeadline.After(snap.RebalanceStarted) {
		t.Fatalf("snapshot of a rebalance: %+v", snap)
	}
	if got := snap.RebalanceDeadline.Sub(snap.RebalanceStarted); got < snap.RebalanceTimeout-time.Second || got > snap.RebalanceTimeout+time.Second {
		t.Errorf("deadline is %v after the start, want the rebalance timeout %v", got, snap.RebalanceTimeout)
	}
	var rejoined, notYet int
	for _, m := range snap.Members {
		if m.Rejoined {
			rejoined++
			if m.RejoinedAt.IsZero() {
				t.Errorf("%s rejoined with no time", m.ID)
			}
		} else {
			notYet++
			if m.ID != first.memberID {
				t.Errorf("%s has not rejoined, only the first member should not have", m.ID)
			}
		}
	}
	if rejoined != 1 || notYet != 1 {
		t.Errorf("rejoined %d, not yet %d, want 1 and 1", rejoined, notYet)
	}
	g := gm.groups["g"]
	gm.mu.Lock()
	gen := g.join.gen
	gm.mu.Unlock()
	gm.joinExpired(g, gen)
	<-done
}

func TestRebalanceTimeoutIsTheLongestAnyMemberAskedFor(t *testing.T) {
	g := &group{members: map[string]*groupMember{
		"a": {},
		"b": {rebalanceTimeout: 10 * time.Second},
		"c": {rebalanceTimeout: 45 * time.Second},
	}}
	if got := g.rebalanceTimeout(); got != 45*time.Second {
		t.Errorf("rebalanceTimeout = %v, want 45s", got)
	}
	g.members["c"].rebalanceTimeout = 10 * time.Minute
	if got := g.rebalanceTimeout(); got != 10*time.Minute {
		t.Errorf("rebalanceTimeout = %v: there is no cap, want 10m", got)
	}
}

// GRP-20: a consumer-protocol member in the snapshot is STABLE or RECONCILING, with the
// epoch it had before, the target the assignor wants, and what the coordinator holds as
// its current (what it may consume plus what it was told to give up).
func TestSnapshotOfAHandoffShowsTargetCurrentAndState(t *testing.T) {
	m := new848Manager(t)
	topic, err := m.meta.CreateTopic("t", 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	join848(t, m, "g", "a")
	join848(t, m, "g", "b")
	g := m.groups["g"]
	at := func(ps ...int32) tpSet {
		s := tpSet{}
		for _, p := range ps {
			s[tp{topic.ID, p}] = struct{}{}
		}
		return s
	}
	m.mu.Lock()
	g.targetEpoch = g.epoch
	for _, mem := range g.members {
		mem.epoch = g.epoch
	}
	g.members["a"].target, g.members["a"].assigned, g.members["a"].revoking = at(0, 1), at(0, 1), at(2)
	g.members["a"].prevEpoch = g.epoch - 1
	g.members["b"].target, g.members["b"].assigned, g.members["b"].revoking = at(2), at(), at()
	m.mu.Unlock()

	snap := m.inspect(time.Now())[0]
	byID := map[string]MemberSnapshot{}
	for _, ms := range snap.Members {
		byID[ms.ID] = ms
	}
	a, b := byID["a"], byID["b"]
	if a.State != "RECONCILING" || b.State != "RECONCILING" {
		t.Errorf("states %q %q, want both RECONCILING", a.State, b.State)
	}
	if a.PrevEpoch != g.epoch-1 {
		t.Errorf("a's previous epoch = %d, want %d", a.PrevEpoch, g.epoch-1)
	}
	want := func(got []TopicParts, name string, parts ...int32) {
		t.Helper()
		if len(got) != 1 || got[0].Topic != name || len(got[0].Partitions) != len(parts) {
			t.Errorf("got %+v, want %s %v", got, name, parts)
			return
		}
		for i, p := range parts {
			if got[0].Partitions[i] != p {
				t.Errorf("got %+v, want %s %v", got, name, parts)
			}
		}
	}
	want(a.Target, "t", 0, 1)
	want(a.Current, "t", 0, 1, 2) // it still holds 2 until it says it let go.
	want(a.Revoking, "t", 2)
	want(b.Target, "t", 2)
	if len(b.Current) != 0 {
		t.Errorf("b holds %+v, want nothing yet", b.Current)
	}
	// Once the handoff is done both are STABLE.
	m.mu.Lock()
	g.members["a"].assigned, g.members["a"].revoking = at(0, 1), at()
	g.members["b"].assigned = at(2)
	m.mu.Unlock()
	for _, ms := range m.inspect(time.Now())[0].Members {
		if ms.State != "STABLE" {
			t.Errorf("%s is %s after the handoff", ms.ID, ms.State)
		}
	}
}
