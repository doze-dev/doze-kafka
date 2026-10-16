package broker

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// time.Timer.Stop cannot recall a timer that has already fired and is waiting for
// the coordinator's lock. Each timer therefore carries an identity, and a firing
// whose identity is no longer current must do nothing. These call the timer
// bodies directly with a stale identity, which is exactly what that race delivers.

func joinReq(group, member string) *kmsg.JoinGroupRequest {
	r := kmsg.NewPtrJoinGroupRequest()
	r.SetVersion(3) // joins in one step, no MEMBER_ID_REQUIRED round trip.
	r.Group, r.MemberID = group, member
	r.SessionTimeoutMillis, r.RebalanceTimeoutMillis = 30_000, 30_000
	r.ProtocolType = "consumer"
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name = "range"
	r.Protocols = append(r.Protocols, p)
	return r
}

func noDelay(t *testing.T) {
	t.Helper()
	old := defaultInitialRebalanceDelay
	defaultInitialRebalanceDelay = 0
	t.Cleanup(func() { defaultInitialRebalanceDelay = old })
}

func TestStaleSessionTimerDoesNotEvictALiveMember(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	res, err := gm.join(context.Background(), joinReq("g", ""), clientRef{})
	if err != nil || res.errorCode != 0 {
		t.Fatalf("join: %v %+v", err, res)
	}
	g := gm.groups["g"]
	m := g.members[res.memberID]
	stale := m.session.gen

	// The member heartbeats (a new timer, a new identity) just as the old timer
	// fires and reaches the lock second.
	hb := kmsg.NewPtrHeartbeatRequest()
	hb.Group, hb.MemberID, hb.Generation = "g", res.memberID, res.generation
	if code := gm.heartbeat(hb); code != 0 && code != 27 {
		t.Fatalf("heartbeat: %d", code)
	}
	gm.sessionExpired(g, m, stale)
	if gm.groups["g"] == nil || gm.groups["g"].members[res.memberID] == nil {
		t.Fatal("a session timer that was overtaken by a heartbeat evicted the member anyway")
	}
	// The current one does evict.
	gm.sessionExpired(g, m, m.session.gen)
	if grp := gm.groups["g"]; grp != nil && grp.members[res.memberID] != nil {
		t.Fatal("the current session timer did not evict a silent member")
	}
}

func TestStaleJoinTimerDoesNotEndALaterRebalance(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	a, _ := gm.join(context.Background(), joinReq("g", ""), clientRef{})
	g := gm.groups["g"]

	// B's join starts a rebalance and arms the join timer.
	done := make(chan joinResult, 1)
	go func() {
		r, _ := gm.join(context.Background(), joinReq("g", ""), clientRef{})
		done <- r
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		gm.mu.Lock()
		armed := g.state == statePreparingRebalance && g.join.armed()
		gen := g.join.gen
		gm.mu.Unlock()
		if armed {
			// A firing from an earlier arming must not complete this rebalance
			// (which would drop A, who has not rejoined yet).
			gm.joinExpired(g, gen-1)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the rebalance never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	gm.mu.Lock()
	state, hasA := g.state, g.members[a.memberID] != nil
	gm.mu.Unlock()
	if state != statePreparingRebalance || !hasA {
		t.Fatalf("a stale join timer ended the rebalance (state %v, A still a member: %v)", state, hasA)
	}
	// A rejoins; the rebalance completes normally.
	if r, err := gm.join(context.Background(), joinReq("g", a.memberID), clientRef{}); err != nil || r.errorCode != 0 {
		t.Fatalf("A rejoin: %v %+v", err, r)
	}
	<-done
}

// When the last member leaves mid-rebalance the join timer is stopped. Left armed
// (or left recorded as armed), no later rebalance of the group could time out.
func TestJoinTimerIsClearedWhenTheGroupEmpties(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(func(string) bool { return true }, nil) // "has offsets": the group object is kept.
	a, _ := gm.join(context.Background(), joinReq("g", ""), clientRef{})
	g := gm.groups["g"]
	go gm.join(context.Background(), joinReq("g", ""), clientRef{}) // starts a rebalance; arms the timer.
	deadline := time.Now().Add(2 * time.Second)
	for {
		gm.mu.Lock()
		armed := g.join.armed()
		var ids []string
		for id := range g.members {
			ids = append(ids, id)
		}
		gm.mu.Unlock()
		if armed && len(ids) == 2 {
			lr := kmsg.NewPtrLeaveGroupRequest()
			lr.Group = "g"
			for _, id := range ids {
				m := kmsg.NewLeaveGroupRequestMember()
				m.MemberID = id
				lr.Members = append(lr.Members, m)
			}
			gm.leave(lr)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("setup: the second member never joined")
		}
		time.Sleep(5 * time.Millisecond)
	}
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if g.join.armed() || g.state != stateEmpty {
		t.Fatalf("after everyone left: join timer armed=%v, state %v; want it cleared and Empty", g.join.armed(), g.state)
	}
	_ = a
}

// A member's session restarts when the rebalance it was waiting in completes. It
// was last heard from when it sent its JoinGroup; left running from then, the
// timer could fire between the join being answered and the SyncGroup arriving.
func TestSessionRestartsWhenARebalanceCompletes(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	a, _ := gm.join(context.Background(), joinReq("g", ""), clientRef{})
	g := gm.groups["g"]

	done := make(chan joinResult, 1)
	go func() {
		r, _ := gm.join(context.Background(), joinReq("g", ""), clientRef{})
		done <- r
	}()
	var b *groupMember
	for deadline := time.Now().Add(2 * time.Second); b == nil; {
		gm.mu.Lock()
		for id, m := range g.members {
			if id != a.memberID && m.awaitingJoin {
				b = m
			}
		}
		gm.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("B never parked")
		}
		time.Sleep(5 * time.Millisecond)
	}
	gm.mu.Lock()
	atJoin := b.session.gen
	gm.mu.Unlock()

	if r, err := gm.join(context.Background(), joinReq("g", a.memberID), clientRef{}); err != nil || r.errorCode != 0 {
		t.Fatalf("A rejoin: %v %+v", err, r)
	}
	<-done
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if b.session.gen == atJoin {
		t.Fatal("B's session timer is still the one armed when it sent its JoinGroup; it was not restarted when the rebalance completed")
	}
}

// ---- KIP-848 coordinator timers ----

func join848(t *testing.T, m *cg848Manager, group, member string) cg848Result {
	t.Helper()
	r := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	r.Group, r.MemberID, r.RebalanceTimeoutMillis = group, member, 60_000
	r.SubscribedTopicNames = []string{"t"}
	r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{} // a join owns nothing.
	res := m.heartbeat(r, clientRef{})
	if res.errorCode != 0 {
		t.Fatalf("join: %+v", res)
	}
	return res
}

func new848Manager(t *testing.T) *cg848Manager {
	t.Helper()
	store, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return newCG848Manager(store, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Hour, 0, nil)
}

func TestKIP848StaleSessionTimerDoesNotEvictALiveMember(t *testing.T) {
	m := new848Manager(t)
	res := join848(t, m, "g", "a")
	g := m.groups["g"]
	mem := g.members["a"]
	stale := mem.session.gen

	hb := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hb.Group, hb.MemberID, hb.MemberEpoch = "g", "a", res.memberEpoch
	if r := m.heartbeat(hb, clientRef{}); r.errorCode != 0 {
		t.Fatalf("heartbeat: %+v", r)
	}
	m.sessionExpired(g, mem, stale)
	if g.members["a"] == nil {
		t.Fatal("a session timer overtaken by a heartbeat evicted the member anyway")
	}
	m.sessionExpired(g, mem, mem.session.gen)
	if g.members["a"] != nil {
		t.Fatal("the current session timer did not evict a silent member")
	}
}

// After the broker stops, a timer that fires does nothing: the metadata store it
// would consult to reassign is closed.
func TestKIP848TimersDoNothingAfterStop(t *testing.T) {
	m := new848Manager(t)
	join848(t, m, "g", "a")
	g := m.groups["g"]
	mem := g.members["a"]
	gen := mem.session.gen
	m.stop()
	m.sessionExpired(g, mem, gen)
	m.sessionExpired(g, mem, mem.session.gen)
	if g.members["a"] == nil {
		t.Fatal("a session timer ran after the coordinator was stopped")
	}
	if mem.session.armed() {
		t.Error("stop left a session timer armed")
	}
}

// expiry is the one timer both coordinators use. A callback carries the identity
// it was armed with, and only the current arming's may act.
func TestExpiryOnlyTheCurrentArmingActs(t *testing.T) {
	var e expiry
	if e.armed() || e.current(e.gen) {
		t.Fatal("a zero expiry is neither armed nor current")
	}
	fired := make(chan uint64, 4)
	e.arm(time.Hour, func(gen uint64) { fired <- gen })
	first := e.gen
	if !e.armed() || !e.current(first) {
		t.Fatal("after arm, the timer is armed and its own gen is current")
	}
	// Re-arming (a heartbeat) makes the first arming stale, even though its
	// callback may already be on its way.
	e.arm(time.Hour, func(gen uint64) { fired <- gen })
	if e.current(first) || !e.current(e.gen) || e.gen == first {
		t.Errorf("after re-arming, gen %d should be stale and %d current", first, e.gen)
	}
	second := e.gen
	e.stop()
	if e.armed() || e.current(second) {
		t.Error("after stop, nothing is current: a late callback must do nothing")
	}
	// A real firing is handed the gen it was armed with.
	e.arm(time.Millisecond, func(gen uint64) { fired <- gen })
	want := e.gen
	select {
	case got := <-fired:
		if got != want || !e.current(got) {
			t.Errorf("fired with gen %d, armed with %d", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the timer did not fire")
	}
	e.stop()
}

// A group losing its last member is a generation of its own, as on Kafka: the member
// that joins the emptied group is in the generation two on from the last one's
// (recorded: offsets-retention-recreate, again-join-g-rejoin answers 3).
func TestAnEmptiedClassicGroupHasMovedOnAGeneration(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(func(string) bool { return true }, nil) // kept when empty, as in a broker.
	first, err := gm.join(context.Background(), joinReq("g", ""), clientRef{})
	if err != nil || first.errorCode != 0 || first.generation != 1 {
		t.Fatalf("first join: %v %+v", err, first)
	}
	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group, leave.MemberID = "g", first.memberID
	if res := gm.leave(leave); len(res) != 1 || res[0].errorCode != 0 {
		t.Fatalf("leave: %+v", res)
	}
	again, err := gm.join(context.Background(), joinReq("g", ""), clientRef{})
	if err != nil || again.errorCode != 0 {
		t.Fatalf("second join: %v %+v", err, again)
	}
	if again.generation != 3 {
		t.Fatalf("the member joining the emptied group is in generation %d, want 3", again.generation)
	}
}
