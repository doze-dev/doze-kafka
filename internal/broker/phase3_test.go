package broker_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func mustCreateTopic(t *testing.T, ctx context.Context, cl *kgo.Client, name string, partitions int32) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = name
	ct.NumPartitions = partitions
	ct.ReplicationFactor = 1
	req.Topics = append(req.Topics, ct)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("create topic %s: %v", name, err)
	}
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic %s: %+v", name, resp.Topics)
	}
}

// consumeFromStart reads exactly n records from a topic's start (no consumer
// group — direct partition consumption) and returns their values in order.
func consumeFromStart(t *testing.T, ctx context.Context, addr, topic string, n int) []string {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer cl.Close()

	var got []string
	for len(got) < n {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.DeadlineExceeded) || errors.Is(e.Err, context.Canceled) {
					t.Fatalf("timed out consuming (%d/%d): %v", len(got), n, e.Err)
				}
				t.Fatalf("fetch error: %v", e.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	return got
}

// TestPhase3_ProduceConsumeRoundTripAndRestart is the Phase 3 acceptance gate: a
// franz-go producer and consumer round-trip records through a topic, order is
// preserved within the partition, and the data survives a broker restart.
func TestPhase3_ProduceConsumeRoundTripAndRestart(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))

	stop := runBrokerAt(t, addr, dir, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const n = 50
	producer := newClient(t, addr)
	mustCreateTopic(t, ctx, producer, "events", 1)

	for i := range n {
		rec := &kgo.Record{Topic: "events", Value: fmt.Appendf(nil, "m%d", i)}
		if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce m%d: %v", i, err)
		}
	}
	producer.Close()

	// Consume and verify order.
	got := consumeFromStart(t, ctx, addr, "events", n)
	for i := range n {
		if want := fmt.Sprintf("m%d", i); got[i] != want {
			t.Fatalf("record %d = %q, want %q (ordering broken)", i, got[i], want)
		}
	}

	// Restart over the same data dir; the records must survive.
	stop()
	stop2 := runBrokerAt(t, addr, dir, 4)
	defer stop2()

	got2 := consumeFromStart(t, ctx, addr, "events", n)
	for i := range n {
		if want := fmt.Sprintf("m%d", i); got2[i] != want {
			t.Fatalf("after restart, record %d = %q, want %q", i, got2[i], want)
		}
	}
}
