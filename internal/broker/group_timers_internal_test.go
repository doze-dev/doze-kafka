package broker

import (
	"context"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
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
	old := initialRebalanceDelay
	initialRebalanceDelay = 0
	t.Cleanup(func() { initialRebalanceDelay = old })
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
	stale := m.sessionGen

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
	gm.sessionExpired(g, m, m.sessionGen)
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
		armed := g.state == statePreparingRebalance && g.joinTimer != nil
		gen := g.joinGen
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
		armed := g.joinTimer != nil
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
	if g.joinTimer != nil || g.state != stateEmpty {
		t.Fatalf("after everyone left: join timer armed=%v, state %v; want it cleared and Empty", g.joinTimer != nil, g.state)
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
	atJoin := b.sessionGen
	gm.mu.Unlock()

	if r, err := gm.join(context.Background(), joinReq("g", a.memberID), clientRef{}); err != nil || r.errorCode != 0 {
		t.Fatalf("A rejoin: %v %+v", err, r)
	}
	<-done
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if b.sessionGen == atJoin {
		t.Fatal("B's session timer is still the one armed when it sent its JoinGroup; it was not restarted when the rebalance completed")
	}
}
