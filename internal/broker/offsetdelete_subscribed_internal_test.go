package broker

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// GRP-4: OffsetDelete of a group with members refuses the topics a member is subscribed
// to with GROUP_SUBSCRIBED_TO_TOPIC and deletes the offsets of any other topic. Recorded
// against Kafka 4.3.1 for both protocols: conformance/recordings/offset-delete-subscribed.json
// (claim G7).
func offsetDeleteCodes(t *testing.T, b *Broker, group string, topics ...string) map[string]int16 {
	t.Helper()
	req := kmsg.NewPtrOffsetDeleteRequest()
	req.Group = group
	for _, name := range topics {
		rt := kmsg.NewOffsetDeleteRequestTopic()
		rt.Topic = name
		rp := kmsg.NewOffsetDeleteRequestTopicPartition()
		rp.Partition = 0
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
	}
	resp := kmsg.NewPtrOffsetDeleteResponse()
	if err := b.handleOffsetDelete(context.Background(), &protocol.Request{}, req, resp); err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("top-level code %d", resp.ErrorCode)
	}
	out := map[string]int16{}
	for _, rt := range resp.Topics {
		out[rt.Topic] = rt.Partitions[0].ErrorCode
	}
	return out
}

func TestOffsetDeleteRefusesOnlyTheTopicsAMemberReads_Classic(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a", "b")
	r.join(t, "busy", "a")
	r.commit(t, "busy", "a", 3)
	r.commit(t, "busy", "b", 4)
	got := offsetDeleteCodes(t, r.b, "busy", "b", "a")
	if got["b"] != 0 || got["a"] != kerr.GroupSubscribedToTopic.Code {
		t.Fatalf("codes = %v, want b 0 and a 86", got)
	}
	if r.has("busy", "b") || !r.has("busy", "a") {
		t.Errorf("b survived: %v, a gone: %v", r.has("busy", "b"), !r.has("busy", "a"))
	}
}

func TestOffsetDeleteRefusesOnlyTheTopicsAMemberReads_Consumer(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "t", "u")
	join848(t, r.b.cg848, "kbusy", "m1") // subscribes to t.
	r.commit(t, "kbusy", "t", 3)
	r.commit(t, "kbusy", "u", 4)
	got := offsetDeleteCodes(t, r.b, "kbusy", "u", "t")
	if got["u"] != 0 || got["t"] != kerr.GroupSubscribedToTopic.Code {
		t.Fatalf("codes = %v, want u 0 and t 86", got)
	}
	if r.has("kbusy", "u") || !r.has("kbusy", "t") {
		t.Error("the unsubscribed topic's offset was not the only one removed")
	}
}

// A subscription this broker cannot read is treated as reading everything: nothing is removed.
func TestOffsetDeleteRefusesEveryTopicWhenTheSubscriptionIsUnreadable(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a", "b")
	noDelay(t)
	req := joinReq("opaque", "")
	req.Protocols[0].Metadata = []byte{1, 2, 3}
	res, err := r.b.groups.join(context.Background(), req, clientRef{})
	if err != nil || res.errorCode != 0 {
		t.Fatalf("join: %v %+v", err, res)
	}
	r.commit(t, "opaque", "b", 4)
	if got := offsetDeleteCodes(t, r.b, "opaque", "b"); got["b"] != kerr.GroupSubscribedToTopic.Code {
		t.Errorf("codes = %v, want 86", got)
	}
}
