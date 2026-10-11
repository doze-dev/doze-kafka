package broker

import (
	"context"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// A member the group did not have is "a member joined": the story (Traf-12) names that
// as the cause when a new client comes into a stable group.
func TestRebalanceCauseOfANewMemberIsAMemberJoined(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	log := &eventLog{}
	gm.sink = log
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, err := gm.join(ctx, joinReq("g", ""), clientRef{clientID: "app-1"})
	if err != nil || a.errorCode != 0 {
		t.Fatalf("join: %v %+v", err, a)
	}
	sr := kmsg.NewPtrSyncGroupRequest()
	sr.Group, sr.MemberID, sr.Generation = "g", a.memberID, a.generation
	sa := kmsg.NewSyncGroupRequestGroupAssignment()
	sa.MemberID, sa.MemberAssignment = a.memberID, []byte{1}
	sr.GroupAssignment = append(sr.GroupAssignment, sa)
	if res, err := gm.sync(ctx, sr); err != nil || res.errorCode != 0 {
		t.Fatalf("sync: %v %+v", err, res)
	}
	causes := func() []string {
		log.mu.Lock()
		defer log.mu.Unlock()
		var out []string
		for _, e := range log.got {
			if e.kind == "rebalance" {
				out = append(out, e.cause)
			}
		}
		return out
	}
	// A second client joins the stable group: a new member.
	go gm.join(ctx, joinReq("g", ""), clientRef{clientID: "app-2"})
	deadline := time.Now().Add(5 * time.Second)
	for len(causes()) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	got := causes()
	if len(got) < 2 || got[1] != "a member joined" {
		t.Errorf("rebalance causes = %q, want the second to be \"a member joined\" (a new member)", got)
	}
}
