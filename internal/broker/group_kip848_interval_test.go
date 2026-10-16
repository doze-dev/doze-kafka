package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// A consumer group's targets are computed at most once a second (Kafka's
// group.consumer.assignment.interval.ms default), by a heartbeat: a join a moment
// after the last targets leaves the group Assigning, with the joiner at the targets'
// epoch, however long nobody heartbeats; a leave never computes them; the first
// heartbeat after the interval does (recorded: consumer-assignment-interval,
// consumer-assignment-expiry).
func TestGroupKIP848_TargetsWaitForAHeartbeatAfterTheAssignmentInterval(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	defer admin.Close()
	mustCreateTopic(t, ctx, admin, "iv", 2)

	a, b := newCGMember(t, addr, "iv-g", "member-a"), newCGMember(t, addr, "iv-g", "member-b")
	a.heartbeat(t, ctx, []string{"iv"}, false)
	if rb := b.heartbeat(t, ctx, []string{"iv"}, false); rb.ErrorCode != 0 || rb.MemberEpoch != a.epoch {
		t.Fatalf("b's join a moment after a's: code %d epoch %d, want 0 and a's epoch %d (the targets')", rb.ErrorCode, rb.MemberEpoch, a.epoch)
	}
	check := func(when, state string, epoch, targets int32) {
		t.Helper()
		g := describe848(t, ctx, admin, "iv-g")
		if g.State != state || g.Epoch != epoch || g.AssignmentEpoch != targets {
			t.Errorf("%s: %s epoch %d targets %d, want %s %d %d", when, g.State, g.Epoch, g.AssignmentEpoch, state, epoch, targets)
		}
	}
	first := a.epoch
	check("at once", "Assigning", first+1, first)
	a.heartbeat(t, ctx, nil, false)
	check("after a heartbeat within the interval", "Assigning", first+1, first)
	time.Sleep(1500 * time.Millisecond)
	check("1.5 s later with no heartbeat", "Assigning", first+1, first)
	if r := a.heartbeat(t, ctx, nil, false); r.Assignment == nil {
		t.Errorf("a's heartbeat after the interval was not told new partitions")
	}
	check("after a's heartbeat past the interval", "Reconciling", first+1, first+1)

	// A leave long after the last targets still waits for a heartbeat.
	time.Sleep(1500 * time.Millisecond)
	leave := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	leave.Group, leave.MemberID, leave.MemberEpoch = "iv-g", b.id, -1
	if r, err := leave.RequestWith(ctx, b.cl); err != nil || r.ErrorCode != 0 {
		t.Fatalf("b's leave: %v %+v", err, r)
	}
	check("after b left", "Assigning", first+2, first+1)
	a.heartbeat(t, ctx, nil, true)
	check("after a's next heartbeat", "Stable", first+2, first+2)
}
