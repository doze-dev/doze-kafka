package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// grow sends one CreatePartitions entry and returns its answer.
func grow(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, count int32, validateOnly bool, assignment ...[]int32) kmsg.CreatePartitionsResponseTopic {
	t.Helper()
	req := kmsg.NewPtrCreatePartitionsRequest()
	req.ValidateOnly = validateOnly
	rt := kmsg.NewCreatePartitionsRequestTopic()
	rt.Topic, rt.Count = topic, count
	for _, r := range assignment {
		a := kmsg.NewCreatePartitionsRequestTopicAssignment()
		a.Replicas = r
		rt.Assignment = append(rt.Assignment, a)
	}
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) != 1 {
		t.Fatalf("CreatePartitions %s: %v %+v", topic, err, resp)
	}
	return resp.Topics[0]
}

func partitionCount(t *testing.T, ctx context.Context, cl *kgo.Client, topic string) int {
	t.Helper()
	req := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = &topic
	req.Topics = append(req.Topics, mt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	mtp, ok := findTopic(resp, topic)
	if !ok {
		t.Fatalf("no metadata for %s", topic)
	}
	return len(mtp.Partitions)
}

// Partitions only ever increase, and the answer for each way a request can be wrong
// is the one Kafka gives, because Terraform and Spring branch on the code.
func TestCreatePartitionsSemantics(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, cl, "grow", 2)

	for _, tc := range []struct {
		name  string
		topic string
		count int32
		want  *kerr.Error
	}{
		{"the same count", "grow", 2, kerr.InvalidPartitions},
		{"fewer", "grow", 1, kerr.InvalidPartitions},
		{"above the broker's limit", "grow", 1001, kerr.InvalidPartitions},
		{"unknown topic", "nope", 4, kerr.UnknownTopicOrPartition},
	} {
		got := grow(t, ctx, cl, tc.topic, tc.count, false)
		if got.ErrorCode != tc.want.Code {
			t.Errorf("%s: code %d, want %s", tc.name, got.ErrorCode, tc.want.Message)
		}
	}
	if n := partitionCount(t, ctx, cl, "grow"); n != 2 {
		t.Fatalf("rejected requests changed the topic: %d partitions", n)
	}

	// An assignment must describe what one node can do.
	if got := grow(t, ctx, cl, "grow", 4, false, []int32{1}); got.ErrorCode != kerr.InvalidReplicaAssignment.Code {
		t.Errorf("an assignment for 1 new partition when 2 are added: code %d", got.ErrorCode)
	}
	if got := grow(t, ctx, cl, "grow", 3, false, []int32{7}); got.ErrorCode != kerr.InvalidReplicaAssignment.Code {
		t.Errorf("an assignment naming another broker: code %d", got.ErrorCode)
	}

	// validateOnly says yes and changes nothing.
	if got := grow(t, ctx, cl, "grow", 6, true); got.ErrorCode != 0 {
		t.Errorf("validateOnly refused a valid request: %d", got.ErrorCode)
	}
	if n := partitionCount(t, ctx, cl, "grow"); n != 2 {
		t.Fatalf("validateOnly changed the topic: %d partitions", n)
	}

	if got := grow(t, ctx, cl, "grow", 4, false, []int32{1}, []int32{1}); got.ErrorCode != 0 {
		t.Fatalf("growing 2 to 4 with an assignment for this node: %d %v", got.ErrorCode, got.ErrorMessage)
	}
	if n := partitionCount(t, ctx, cl, "grow"); n != 4 {
		t.Errorf("after growing to 4: %d partitions", n)
	}

	// The same topic twice in one request is malformed; neither is acted on.
	req := kmsg.NewPtrCreatePartitionsRequest()
	for range 2 {
		rt := kmsg.NewCreatePartitionsRequestTopic()
		rt.Topic, rt.Count = "grow", 9
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range resp.Topics {
		if r.ErrorCode != kerr.InvalidRequest.Code {
			t.Errorf("a topic named twice: code %d", r.ErrorCode)
		}
	}
	if n := partitionCount(t, ctx, cl, "grow"); n != 4 {
		t.Errorf("a malformed request changed the topic: %d partitions", n)
	}
}

// Growth is recorded: a restart finds the new count, the new partitions take
// records, and nothing already written moves.
func TestCreatePartitionsSurvivesARestart(t *testing.T) {
	skipShort(t)
	dir := t.TempDir()
	addr, stop := bootAt(t, "127.0.0.1:0", dir, 4)
	cl := newClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, cl, "keep", 1)
	pr := newClient(t, addr)
	if err := pr.ProduceSync(ctx, &kgo.Record{Topic: "keep", Value: []byte("before")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	pr.Close()
	if got := grow(t, ctx, cl, "keep", 3, false); got.ErrorCode != 0 {
		t.Fatalf("grow: %d", got.ErrorCode)
	}
	cl.Close()
	stop()

	stop = runBrokerAt(t, addr, dir, 4)
	defer stop()
	cl = newClient(t, addr)
	defer cl.Close()
	if n := partitionCount(t, ctx, cl, "keep"); n != 3 {
		t.Fatalf("after a restart the topic has %d partitions, want 3", n)
	}
	p, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.ProduceSync(ctx, &kgo.Record{Topic: "keep", Partition: 2, Value: []byte("new")}).FirstErr(); err != nil {
		t.Fatalf("producing to a partition added before the restart: %v", err)
	}
	if end := listOffset(t, ctx, cl, "keep", 0, -1); end != 1 {
		t.Errorf("partition 0's end moved to %d after growth", end)
	}
	if end := listOffset(t, ctx, cl, "keep", 2, -1); end != 1 {
		t.Errorf("partition 2's end is %d, want 1", end)
	}
}

// A consumer already running in a group ends up covering the added partitions, under
// each protocol: classic by rejoining on the metadata change, KIP-848 by the broker
// reassigning when the topic grew.
func TestCreatePartitionsReachesALiveConsumer(t *testing.T) {
	skipShort(t)
	for _, tc := range []struct {
		name string
		opts []kgo.Opt
	}{
		{"classic", nil},
		{"kip848", kip848Opts()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := startBroker(t, 4)
			cl := newClient(t, addr)
			defer cl.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			mustCreateTopic(t, ctx, cl, "live", 2)

			c, err := kgo.NewClient(append([]kgo.Opt{
				kgo.SeedBrokers(addr), kgo.ConsumerGroup("g-" + tc.name), kgo.ConsumeTopics("live"),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.MetadataMaxAge(500 * time.Millisecond), kgo.MetadataMinAge(100 * time.Millisecond),
			}, tc.opts...)...)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			// The consumer is a member (and has been assigned both partitions) before
			// the topic grows.
			joined, stop := context.WithTimeout(ctx, 3*time.Second)
			c.PollFetches(joined)
			stop()

			if got := grow(t, ctx, cl, "live", 4, false); got.ErrorCode != 0 {
				t.Fatalf("grow: %d", got.ErrorCode)
			}
			p, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			for part := int32(0); part < 4; part++ {
				if err := p.ProduceSync(ctx, &kgo.Record{Topic: "live", Partition: part, Value: []byte{byte('a' + part)}}).FirstErr(); err != nil {
					t.Fatal(err)
				}
			}
			seen := map[int32]bool{}
			for len(seen) < 4 {
				fs := c.PollFetches(ctx)
				if ctx.Err() != nil {
					t.Fatalf("the consumer only ever saw partitions %v of 4", seen)
				}
				fs.EachRecord(func(r *kgo.Record) { seen[r.Partition] = true })
			}
		})
	}
}
