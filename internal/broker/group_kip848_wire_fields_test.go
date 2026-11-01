package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// What the ConsumerGroupHeartbeat handler fills in: the interval a live member is asked
// to keep (none once it has left), the message only an error carries, and the names
// ConsumerGroupDescribe gives the topics of an assignment.
func TestGroupKIP848_HeartbeatAnswersCarryTheirIntervalAndMessage(t *testing.T) {
	addr := startBrokerAssigningAtOnce(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, newClient(t, addr), "dt", 2)

	m := new848(t, addr, "dg", "dm1")
	join := m.join(t, ctx, []string{"dt"})
	if join.ErrorCode != 0 || join.ErrorMessage != nil || join.HeartbeatIntervalMillis != 1000 || join.Assignment == nil {
		t.Fatalf("join: code %d message %v interval %d assignment %v, want 0, none, 1000 and an assignment", join.ErrorCode, join.ErrorMessage, join.HeartbeatIntervalMillis, join.Assignment)
	}

	// The assignment is described with the name of its topic.
	dr := kmsg.NewPtrConsumerGroupDescribeRequest()
	dr.Groups = []string{"dg"}
	desc, err := dr.RequestWith(ctx, m.cl)
	if err != nil || len(desc.Groups) != 1 || len(desc.Groups[0].Members) != 1 {
		t.Fatalf("ConsumerGroupDescribe: %v %+v", err, desc)
	}
	tps := desc.Groups[0].Members[0].Assignment.TopicPartitions
	if len(tps) != 1 || tps[0].Topic != "dt" || len(tps[0].Partitions) != 2 {
		t.Errorf("the described assignment: %+v, want topic dt with its 2 partitions", tps)
	}

	// An epoch from nowhere is refused with a message.
	bad := m.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.MemberEpoch = join.MemberEpoch + 50 })
	if bad.ErrorCode != kerr.FencedMemberEpoch.Code || bad.ErrorMessage == nil || *bad.ErrorMessage == "" {
		t.Errorf("an epoch ahead: code %d message %v, want FENCED_MEMBER_EPOCH with a message", bad.ErrorCode, bad.ErrorMessage)
	}

	// Leaving is answered at epoch -1 with no interval to keep.
	left := m.leave(t, ctx)
	if left.ErrorCode != 0 || left.MemberEpoch != -1 || left.HeartbeatIntervalMillis != 0 {
		t.Errorf("leave: code %d epoch %d interval %d, want 0, -1 and no interval", left.ErrorCode, left.MemberEpoch, left.HeartbeatIntervalMillis)
	}
}
