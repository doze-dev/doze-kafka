package broker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

type loggedEvent struct {
	group, kind, member string
	generation          int32
	detail              string
}

type eventLog struct {
	mu  sync.Mutex
	got []loggedEvent
}

func (l *eventLog) GroupEvent(group, kind, member string, generation int32, detail string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.got = append(l.got, loggedEvent{group, kind, member, generation, detail})
}

func (l *eventLog) kinds() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, e := range l.got {
		out = append(out, e.kind)
	}
	return strings.Join(out, " ")
}

func (l *eventLog) find(kind string) (loggedEvent, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.got {
		if e.kind == kind {
			return e, true
		}
	}
	return loggedEvent{}, false
}

// "Why does my consumer keep rebalancing?" is answered by the order things
// happened in, so the classic coordinator says each step as it takes it.
func TestClassicCoordinatorTellsTheTimeline(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	log := &eventLog{}
	gm.sink = log

	a, err := gm.join(context.Background(), joinReq("g", ""), clientRef{clientID: "app-1"})
	if err != nil || a.errorCode != 0 {
		t.Fatalf("join: %v %+v", err, a)
	}
	if got := log.kinds(); got != "joined rebalance generation" {
		t.Fatalf("after the first join: %q, want %q", got, "joined rebalance generation")
	}
	if e, _ := log.find("generation"); e.generation != 1 || e.member != a.memberID || !strings.Contains(e.detail, "1 member") {
		t.Errorf("generation event = %+v", e)
	}
	if e, _ := log.find("joined"); !strings.Contains(e.detail, "app-1") {
		t.Errorf("the joined event does not name the client: %+v", e)
	}

	sr := kmsg.NewPtrSyncGroupRequest()
	sr.Group, sr.MemberID, sr.Generation = "g", a.memberID, a.generation
	sa := kmsg.NewSyncGroupRequestGroupAssignment()
	sa.MemberID, sa.MemberAssignment = a.memberID, []byte{1}
	sr.GroupAssignment = append(sr.GroupAssignment, sa)
	if res, err := gm.sync(context.Background(), sr); err != nil || res.errorCode != 0 {
		t.Fatalf("sync: %v %+v", err, res)
	}
	if got := log.kinds(); !strings.HasSuffix(got, "assigned") {
		t.Errorf("after the leader synced: %q", got)
	}

	// Silence ends it: the session expires, and the group says so before anything else.
	g := gm.groups["g"]
	m := g.members[a.memberID]
	gm.sessionExpired(g, m, m.session.gen)
	e, ok := log.find("expired")
	if !ok || e.member != a.memberID || !strings.Contains(e.detail, "session timeout") {
		t.Fatalf("no expiry event: %q", log.kinds())
	}
}

func TestClassicCoordinatorTellsWhoLeftAndWhoWasDropped(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(func(string) bool { return true }, nil)
	log := &eventLog{}
	gm.sink = log
	a, _ := gm.join(context.Background(), joinReq("g", ""), clientRef{clientID: "app-1"})

	lr := kmsg.NewPtrLeaveGroupRequest()
	lr.SetVersion(0)
	lr.Group, lr.MemberID = "g", a.memberID
	gm.leave(lr)
	e, ok := log.find("left")
	if !ok || e.member != a.memberID || !strings.Contains(e.detail, "app-1") {
		t.Fatalf("no left event naming the client: %q", log.kinds())
	}

	// A member that does not rejoin before the rebalance times out is dropped, and
	// the log says which one.
	log2 := &eventLog{}
	gm2 := newGroupManager(nil, nil)
	gm2.sink = log2
	first, _ := gm2.join(context.Background(), joinReq("h", ""), clientRef{clientID: "slow"})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = gm2.join(context.Background(), joinReq("h", ""), clientRef{clientID: "fast"})
	}()
	g := gm2.groups["h"]
	for {
		gm2.mu.Lock()
		armed := g.state == statePreparingRebalance && g.join.armed()
		gen := g.join.gen
		gm2.mu.Unlock()
		if armed {
			gm2.joinExpired(g, gen) // the rebalance timeout: "slow" never rejoined.
			break
		}
	}
	<-done
	e, ok = log2.find("dropped")
	if !ok || e.member != first.memberID {
		t.Fatalf("no dropped event for the member that never rejoined: %q", log2.kinds())
	}
}

func TestKIP848CoordinatorTellsTheTimeline(t *testing.T) {
	m := new848Manager(t)
	log := &eventLog{}
	m.sink = log

	res := join848(t, m, "g", "a")
	if got := log.kinds(); got != "joined epoch" {
		t.Fatalf("after a join: %q, want %q", got, "joined epoch")
	}
	if e, _ := log.find("epoch"); e.generation < 1 || e.generation != int32(res.memberEpoch) {
		t.Errorf("epoch event = %+v, member epoch %d", e, res.memberEpoch)
	}

	// Silence ends it.
	g := m.groups["g"]
	mem := g.members["a"]
	m.sessionExpired(g, mem, mem.session.gen)
	if e, ok := log.find("expired"); !ok || e.member != "a" {
		t.Fatalf("no expiry event: %q", log.kinds())
	}

	// A second member leaving.
	join848(t, m, "h", "x")
	join848(t, m, "h", "y")
	leave := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	leave.Group, leave.MemberID, leave.MemberEpoch = "h", "y", -1
	if r := m.heartbeat(leave, clientRef{}); r.errorCode != 0 {
		t.Fatalf("leave: %+v", r)
	}
	if e, ok := log.find("left"); !ok || e.member != "y" || e.group != "h" {
		t.Fatalf("no left event: %q", log.kinds())
	}
	_ = fmt.Sprint
}
