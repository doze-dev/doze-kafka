package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/twmb/franz-go/pkg/kerr"
)

func produceN(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, n int) {
	t.Helper()
	for i := range n {
		rec := &kgo.Record{Topic: topic, Value: []byte{byte(i)}}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
}

func commitOffset(t *testing.T, ctx context.Context, cl *kgo.Client, group, topic string, offset int64) {
	t.Helper()
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group, req.Generation = group, -1 // a "simple" commit: no group membership.
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Offset = offset
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || resp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("OffsetCommit: %v %+v", err, resp)
	}
}

func fetchCommitted(t *testing.T, ctx context.Context, cl *kgo.Client, group, topic string) int64 {
	t.Helper()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic = topic
	rt.Partitions = []int32{0}
	req.Topics = append(req.Topics, rt)
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = group
	gt := kmsg.NewOffsetFetchRequestGroupTopic()
	gt.Topic = topic
	gt.Partitions = []int32{0}
	g.Topics = append(g.Topics, gt)
	req.Groups = append(req.Groups, g)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("OffsetFetch: %v", err)
	}
	if len(resp.Groups) > 0 { // v8+ shape.
		for _, tp := range resp.Groups[0].Topics {
			for _, p := range tp.Partitions {
				return p.Offset
			}
		}
	}
	for _, tp := range resp.Topics {
		for _, p := range tp.Partitions {
			return p.Offset
		}
	}
	t.Fatalf("OffsetFetch returned nothing for %s/%s", group, topic)
	return 0
}

// nameFetchClient is a client pinned to Kafka 3.0 request versions, the last
// whose Fetch addresses a topic by name; at newer versions a topic is named by
// its id and these tests would be asking about an unknown id (code 100).
func nameFetchClient(t *testing.T, addr string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.MaxVersions(kversion.V3_0_0()), kgo.DialTimeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func fetchCode(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, offset int64) (int16, int) {
	t.Helper()
	req := kmsg.NewPtrFetchRequest()
	req.MaxBytes = 1 << 20
	req.MaxWaitMillis = 50
	rt := kmsg.NewFetchRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.FetchOffset = offset
	rp.PartitionMaxBytes = 1 << 20
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	p := resp.Topics[0].Partitions[0]
	return p.ErrorCode, len(p.RecordBatches)
}

// TestFetchBeyondTheLogEnd: an offset the log never reached must be reported,
// not answered with "no data" — otherwise a consumer holding it waits, then skips
// every record up to it as the log grows. Equal to the log end is fine (caught up).
func TestFetchBeyondTheLogEnd(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, cl, "t", 1)
	produceN(t, ctx, cl, "t", 3)
	fcl := nameFetchClient(t, addr)

	oor := kerr.OffsetOutOfRange.Code
	if code, n := fetchCode(t, ctx, fcl, "t", 3); code != 0 || n != 0 {
		t.Errorf("fetch at the log end: code %d with %d bytes, want 0 and none (caught up)", code, n)
	}
	if code, _ := fetchCode(t, ctx, fcl, "t", 4); code != oor {
		t.Errorf("fetch above the log end: code %d, want OFFSET_OUT_OF_RANGE (%d)", code, oor)
	}
	if code, n := fetchCode(t, ctx, fcl, "t", 0); code != 0 || n == 0 {
		t.Errorf("fetch at 0: code %d bytes %d, want data", code, n)
	}
}

// TestDeleteAndRecreateDropsCommittedOffsets is the test-teardown pattern: same
// topic and group names across runs. Before, the new topic inherited the old
// offset (10) while its log held 3 records, and a consumer using it hung, then
// skipped records 3-9 once the log grew past it.
func TestDeleteAndRecreateDropsCommittedOffsets(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	mustCreateTopic(t, ctx, cl, "orders", 1)
	produceN(t, ctx, cl, "orders", 10)
	commitOffset(t, ctx, cl, "billing", "orders", 10)
	if got := fetchCommitted(t, ctx, cl, "billing", "orders"); got != 10 {
		t.Fatalf("setup: committed offset = %d, want 10", got)
	}

	dreq := kmsg.NewPtrDeleteTopicsRequest()
	dreq.TopicNames = []string{"orders"}
	dt := kmsg.NewDeleteTopicsRequestTopic()
	n := "orders"
	dt.Topic = &n
	dreq.Topics = append(dreq.Topics, dt)
	if _, err := dreq.RequestWith(ctx, cl); err != nil {
		t.Fatal(err)
	}

	mustCreateTopic(t, ctx, cl, "orders", 1)
	produceN(t, ctx, cl, "orders", 3)

	if got := fetchCommitted(t, ctx, cl, "billing", "orders"); got != -1 {
		t.Errorf("the recreated topic inherited committed offset %d, want -1 (none)", got)
	}
	if code, _ := fetchCode(t, ctx, nameFetchClient(t, addr), "orders", 10); code != kerr.OffsetOutOfRange.Code {
		t.Errorf("fetch at the stale offset 10 on a 3-record log: code %d, want OFFSET_OUT_OF_RANGE", code)
	}
}
