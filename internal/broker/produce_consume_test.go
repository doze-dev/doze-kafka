package broker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestProduceConsume_ProduceConsumeRoundTripAndRestart is the Phase 3 acceptance gate: a
// franz-go producer and consumer round-trip records through a topic, order is
// preserved within the partition, and the data survives a broker restart.
func TestProduceConsume_ProduceConsumeRoundTripAndRestart(t *testing.T) {
	node := startNode(t)
	addr := node.Addr

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
	node.Restart()

	got2 := consumeFromStart(t, ctx, addr, "events", n)
	for i := range n {
		if want := fmt.Sprintf("m%d", i); got2[i] != want {
			t.Fatalf("after restart, record %d = %q, want %q", i, got2[i], want)
		}
	}
}
