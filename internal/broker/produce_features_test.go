package broker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestProduce_CompressionRoundTrip proves compressed record batches round-trip. The
// broker stores batches verbatim (it never decodes the payload), so a producer's
// codec must survive untouched to the consumer. Each supported codec is exercised.
func TestProduce_CompressionRoundTrip(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	codecs := []struct {
		name  string
		codec kgo.CompressionCodec
	}{
		{"gzip", kgo.GzipCompression()},
		{"snappy", kgo.SnappyCompression()},
		{"lz4", kgo.Lz4Compression()},
		{"zstd", kgo.ZstdCompression()},
	}
	for _, c := range codecs {
		t.Run(c.name, func(t *testing.T) {
			topic := "compress-" + c.name
			producer, err := kgo.NewClient(
				kgo.SeedBrokers(addr),
				kgo.ProducerBatchCompression(c.codec),
				kgo.DialTimeout(2*time.Second),
			)
			if err != nil {
				t.Fatalf("producer: %v", err)
			}
			mustCreateTopic(t, ctx, producer, topic, 1)

			const n = 40
			for i := range n {
				// A repetitive, compressible payload.
				val := fmt.Sprintf("%s-record-%d-%s", c.name, i, "AAAAAAAAAAAAAAAAAAAA")
				if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(val)}).FirstErr(); err != nil {
					t.Fatalf("produce %d: %v", i, err)
				}
			}
			producer.Close()

			got := consumeFromStart(t, ctx, addr, topic, n)
			for i := range n {
				want := fmt.Sprintf("%s-record-%d-%s", c.name, i, "AAAAAAAAAAAAAAAAAAAA")
				if got[i] != want {
					t.Fatalf("%s record %d = %q, want %q", c.name, i, got[i], want)
				}
			}
		})
	}
}

// TestFetch_LongPollWakesOnProduce proves the Fetch long-poll: a fetch on an empty
// partition with a MinBytes requirement blocks, then is woken by a produce (rather
// than returning empty immediately or waiting the full MaxWait).
func TestFetch_LongPollWakesOnProduce(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cl := newClient(t, addr)
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "longpoll", 1)

	// Topic UUID (Fetch v13 addresses topics by id).
	mreq := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	name := "longpoll"
	mt.Topic = &name
	mreq.Topics = append(mreq.Topics, mt)
	mresp, err := mreq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	topicID := mresp.Topics[0].TopicID

	// Fire a long-poll fetch (empty partition, MinBytes=1) in the background.
	type result struct {
		elapsed time.Duration
		batches int
		err     error
	}
	done := make(chan result, 1)
	go func() {
		freq := kmsg.NewPtrFetchRequest()
		freq.ReplicaID = -1
		freq.MaxWaitMillis = 3000
		freq.MinBytes = 1
		freq.MaxBytes = 1 << 20
		ft := kmsg.NewFetchRequestTopic()
		ft.Topic = "longpoll"
		ft.TopicID = topicID
		fp := kmsg.NewFetchRequestTopicPartition()
		fp.Partition = 0
		fp.FetchOffset = 0
		fp.PartitionMaxBytes = 1 << 20
		ft.Partitions = append(ft.Partitions, fp)
		freq.Topics = append(freq.Topics, ft)

		start := time.Now()
		resp, err := freq.RequestWith(ctx, cl)
		r := result{elapsed: time.Since(start), err: err}
		if err == nil && len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 {
			r.batches = len(resp.Topics[0].Partitions[0].RecordBatches)
		}
		done <- r
	}()

	// Produce after a clear delay; the parked fetch must wake on it.
	time.Sleep(400 * time.Millisecond)
	producer := newClient(t, addr)
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: "longpoll", Value: []byte("wakeup")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	producer.Close()

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("fetch: %v", r.err)
		}
		if r.batches == 0 {
			t.Fatal("long-poll fetch returned no data (did not block for the produce)")
		}
		if r.elapsed < 300*time.Millisecond {
			t.Fatalf("fetch returned in %v — it did not long-poll (returned empty immediately)", r.elapsed)
		}
		if r.elapsed > 2800*time.Millisecond {
			t.Fatalf("fetch took %v — it waited the full MaxWait instead of waking on produce", r.elapsed)
		}
	case <-ctx.Done():
		t.Fatal("long-poll fetch never returned")
	}
}

// TestProduce_AcksZero covers the acks=0 path: the broker sends no response, yet the
// data is durably stored (a subsequent consume finds it).
func TestProduce_AcksZero(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	admin := newClient(t, addr)
	mustCreateTopic(t, ctx, admin, "acks0", 1)
	admin.Close()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequiredAcks(kgo.NoAck()),
		kgo.DisableIdempotentWrite(), // idempotency requires acks=all.
		kgo.DialTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	const n = 10
	for i := range n {
		// With acks=0 there is no broker response; ProduceSync completes once written.
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: "acks0", Value: fmt.Appendf(nil, "a%d", i)}).FirstErr(); err != nil {
			t.Fatalf("produce a%d: %v", i, err)
		}
	}
	producer.Close()

	got := consumeFromStart(t, ctx, addr, "acks0", n)
	for i := range n {
		if want := fmt.Sprintf("a%d", i); got[i] != want {
			t.Fatalf("acks=0 record %d = %q, want %q", i, got[i], want)
		}
	}
}
