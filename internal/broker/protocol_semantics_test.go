package broker_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/twmb/franz-go/pkg/kerr"
)

func metadataNames(resp *kmsg.MetadataResponse) map[string]int16 {
	out := map[string]int16{}
	for _, t := range resp.Topics {
		name := ""
		if t.Topic != nil {
			name = *t.Topic
		}
		out[name] = t.ErrorCode
	}
	return out
}

// In Metadata v1+ a null topic list means "all topics" and an empty list means
// "none"; v0 cannot say null, so there empty means all. An empty list was treated
// as "all" everywhere, so a client asking only for the broker list was handed the
// whole registry.
func TestMetadata_NullVersusEmptyTopicList(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "a", 1)
	mustCreateTopic(t, ctx, cl, "b", 1)

	all := kmsg.NewPtrMetadataRequest() // Topics nil: null.
	resp, err := all.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if got := metadataNames(resp); len(got) != 2 {
		t.Errorf("a null topic list returned %v, want both topics", got)
	}

	none := kmsg.NewPtrMetadataRequest()
	none.Topics = []kmsg.MetadataRequestTopic{} // present but empty.
	resp, err = none.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if got := metadataNames(resp); len(got) != 0 {
		t.Errorf("an empty topic list returned %v, want none", got)
	}
	if len(resp.Brokers) != 1 {
		t.Errorf("the broker list should still be returned, got %d", len(resp.Brokers))
	}

	// v0 has no null: empty means everything.
	old, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.MaxVersions(kversion.V0_9_0()), kgo.DialTimeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	v0 := kmsg.NewPtrMetadataRequest()
	v0.Topics = []kmsg.MetadataRequestTopic{}
	resp, err = v0.RequestWith(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	if v0.GetVersion() != 0 {
		t.Fatalf("test setup: pinned client sent Metadata v%d", v0.GetVersion())
	}
	if got := metadataNames(resp); len(got) != 2 {
		t.Errorf("a v0 empty list returned %v, want both topics", got)
	}
}

// Metadata v10+ can name a topic by id alone; those entries were silently dropped.
func TestMetadata_TopicByIDOnly(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "named", 2)
	id := topicIDOf(t, ctx, cl, "named")

	req := kmsg.NewPtrMetadataRequest()
	known := kmsg.NewMetadataRequestTopic()
	known.TopicID = id
	unknown := kmsg.NewMetadataRequestTopic()
	unknown.TopicID = [16]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	req.Topics = append(req.Topics, known, unknown)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Topics) != 2 {
		t.Fatalf("got %d topic entries for 2 requested by id", len(resp.Topics))
	}
	byID := map[[16]byte]kmsg.MetadataResponseTopic{}
	for _, mt := range resp.Topics {
		byID[mt.TopicID] = mt
	}
	if mt := byID[id]; mt.ErrorCode != 0 || mt.Topic == nil || *mt.Topic != "named" || len(mt.Partitions) != 2 {
		t.Errorf("topic by id: %+v, want the topic named, with 2 partitions", mt)
	}
	if mt := byID[unknown.TopicID]; mt.ErrorCode != kerr.UnknownTopicID.Code {
		t.Errorf("unknown id: code %d, want UNKNOWN_TOPIC_ID (%d)", mt.ErrorCode, kerr.UnknownTopicID.Code)
	}
}

// MaxBytes is a request-wide limit. Only the first partition with data may exceed
// it (so one large batch can always be fetched); the rest must fit what is left.
func TestFetch_RequestMaxBytesIsSharedAcrossPartitions(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	producer, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.DisableIdempotentWrite(), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	mustCreateTopic(t, ctx, producer, "wide", 3)
	payload := make([]byte, 20_000)
	rand.Read(payload) // incompressible, so every batch is ~20 KB.
	for p := int32(0); p < 3; p++ {
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: "wide", Partition: p, Value: payload}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	fcl := nameFetchClient(t, addr)
	fetch := func(maxBytes int32) [3]int {
		req := kmsg.NewPtrFetchRequest()
		req.MaxBytes = maxBytes
		req.MaxWaitMillis = 50
		rt := kmsg.NewFetchRequestTopic()
		rt.Topic = "wide"
		for p := int32(0); p < 3; p++ {
			rp := kmsg.NewFetchRequestTopicPartition()
			rp.Partition = p
			rp.PartitionMaxBytes = 1 << 20
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, fcl)
		if err != nil {
			t.Fatal(err)
		}
		var got [3]int
		for _, p := range resp.Topics[0].Partitions {
			got[p.Partition] = len(p.RecordBatches)
		}
		return got
	}

	if got := fetch(1 << 20); got[0] == 0 || got[1] == 0 || got[2] == 0 {
		t.Errorf("a generous MaxBytes should return all three partitions, got %v", got)
	}
	if got := fetch(30_000); got[0] == 0 || got[1] != 0 || got[2] != 0 {
		t.Errorf("MaxBytes=30000 holds one 20 KB batch and has 10 KB left: want data from partition 0 only, got %v", got)
	}
	if got := fetch(10); got[0] == 0 || got[1] != 0 || got[2] != 0 {
		t.Errorf("MaxBytes=10: the first partition still returns its batch (soft limit) and the rest nothing, got %v", got)
	}
}
