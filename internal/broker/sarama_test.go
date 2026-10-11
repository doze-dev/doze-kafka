package broker_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// Sarama is a second, independent Kafka client (distinct from franz-go) that speaks
// older, non-flexible API versions and its own quirks. These tests are the
// cross-client check: compatibility bugs are frequently client-specific, so a green
// franz suite alone can hide encoding gaps. Sarama uses only the classic consumer
// group protocol.

func saramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0 // exercises non-flexible-era request/response versions.
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Metadata.Retry.Max = 3
	cfg.Metadata.Retry.Backoff = 200 * time.Millisecond
	return cfg
}

// TestSarama_ProduceConsumeRoundTrip proves a non-franz client produces and directly
// consumes (no group) through the broker, in order.
func TestSarama_ProduceConsumeRoundTrip(t *testing.T) {
	addr := startBroker(t, 4)

	// Create the topic with franz so partition count is explicit.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fr := newClient(t, addr)
	mustCreateTopic(t, ctx, fr, "sarama-rt", 1)
	fr.Close()

	cfg := saramaConfig()
	producer, err := sarama.NewSyncProducer([]string{addr}, cfg)
	if err != nil {
		t.Fatalf("sarama producer: %v", err)
	}
	defer producer.Close()

	const n = 30
	for i := range n {
		msg := &sarama.ProducerMessage{Topic: "sarama-rt", Value: sarama.StringEncoder(fmt.Sprintf("s%d", i))}
		if _, _, err := producer.SendMessage(msg); err != nil {
			t.Fatalf("sarama send s%d: %v", i, err)
		}
	}

	consumer, err := sarama.NewConsumer([]string{addr}, cfg)
	if err != nil {
		t.Fatalf("sarama consumer: %v", err)
	}
	defer consumer.Close()
	pc, err := consumer.ConsumePartition("sarama-rt", 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("sarama consume partition: %v", err)
	}
	defer pc.Close()

	deadline := time.After(15 * time.Second)
	for i := range n {
		select {
		case msg := <-pc.Messages():
			if want := fmt.Sprintf("s%d", i); string(msg.Value) != want {
				t.Fatalf("sarama record %d = %q, want %q (ordering)", i, msg.Value, want)
			}
		case <-deadline:
			t.Fatalf("sarama timed out after %d/%d records", i, n)
		}
	}
}

// saramaGroupHandler collects consumed values and signals when a target count is hit.
type saramaGroupHandler struct {
	mu    sync.Mutex
	got   map[string]bool
	total int
	done  chan struct{}
	once  sync.Once
}

func (h *saramaGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *saramaGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *saramaGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		sess.MarkMessage(msg, "")
		h.mu.Lock()
		h.got[string(msg.Value)] = true
		full := len(h.got) >= h.total
		h.mu.Unlock()
		if full {
			h.once.Do(func() { close(h.done) })
		}
	}
	return nil
}

// TestSarama_ConsumerGroupConsumeCommitResume drives the classic consumer-group
// protocol from Sarama (a different client than the franz Phase 4 tests): a group
// consumes and commits, and after a restart a fresh session resumes from the
// committed offset.
func TestSarama_ConsumerGroupConsumeCommitResume(t *testing.T) {
	dir := t.TempDir()
	addr, stop := bootAt(t, "127.0.0.1:0", dir, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	fr := newClient(t, addr)
	mustCreateTopic(t, ctx, fr, "sarama-grp", 1)
	fr.Close()

	cfg := saramaConfig()
	produce := func(from, count int) {
		producer, err := sarama.NewSyncProducer([]string{addr}, cfg)
		if err != nil {
			t.Fatalf("sarama producer: %v", err)
		}
		defer producer.Close()
		for i := from; i < from+count; i++ {
			msg := &sarama.ProducerMessage{Topic: "sarama-grp", Value: sarama.StringEncoder(fmt.Sprintf("g%d", i))}
			if _, _, err := producer.SendMessage(msg); err != nil {
				t.Fatalf("sarama send g%d: %v", i, err)
			}
		}
	}

	// consumeGroup runs a group session until it has seen wantCount distinct values,
	// then returns them.
	consumeGroup := func(wantCount int) map[string]bool {
		group, err := sarama.NewConsumerGroup([]string{addr}, "sarama-cg", cfg)
		if err != nil {
			t.Fatalf("sarama group: %v", err)
		}
		defer group.Close()
		h := &saramaGroupHandler{got: map[string]bool{}, total: wantCount, done: make(chan struct{})}
		gctx, gcancel := context.WithCancel(ctx)
		defer gcancel()
		go func() {
			for gctx.Err() == nil {
				if err := group.Consume(gctx, []string{"sarama-grp"}, h); err != nil {
					return
				}
			}
		}()
		select {
		case <-h.done:
			return h.got
		case <-time.After(20 * time.Second):
			t.Fatalf("sarama group only consumed %d/%d", len(h.got), wantCount)
			return nil
		}
	}

	produce(0, 20)
	first := consumeGroup(20)
	for i := range 20 {
		if !first[fmt.Sprintf("g%d", i)] {
			t.Fatalf("sarama group missing g%d in first batch", i)
		}
	}

	// Restart; produce more; the group must resume from the committed offset and see
	// only the new records.
	stop()
	stop2 := runBrokerAt(t, addr, dir, 4)
	defer stop2()

	produce(20, 10)
	resumed := consumeGroup(10)
	for i := 20; i < 30; i++ {
		if !resumed[fmt.Sprintf("g%d", i)] {
			t.Fatalf("sarama group missing g%d after resume", i)
		}
	}
	if resumed["g0"] {
		t.Fatal("sarama group replayed g0 after restart (offset not resumed)")
	}
}

// TestSarama_TransactionalProducer: a second client library drives the transaction
// APIs end to end. One transaction commits and one aborts, and a read-committed reader
// sees only the committed records.
func TestSarama_TransactionalProducer(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	fr := newClient(t, addr)
	mustCreateTopic(t, ctx, fr, "sarama-tx", 1)

	cfg := saramaConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Producer.Idempotent = true
	cfg.Producer.Transaction.ID = "sarama-tx-1"
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	p, err := sarama.NewAsyncProducer([]string{addr}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	txn := func(commit bool, vals ...string) {
		t.Helper()
		if err := p.BeginTxn(); err != nil {
			t.Fatalf("BeginTxn: %v", err)
		}
		for _, v := range vals {
			p.Input() <- &sarama.ProducerMessage{Topic: "sarama-tx", Partition: 0, Value: sarama.StringEncoder(v)}
		}
		for range vals {
			select {
			case <-p.Successes():
			case err := <-p.Errors():
				t.Fatalf("produce in a transaction: %v", err)
			case <-ctx.Done():
				t.Fatal("timed out waiting for the transaction's records to be acknowledged")
			}
		}
		if commit {
			if err := p.CommitTxn(); err != nil {
				t.Fatalf("CommitTxn: %v", err)
			}
		} else if err := p.AbortTxn(); err != nil {
			t.Fatalf("AbortTxn: %v", err)
		}
	}
	txn(false, "sarama-aborted-1", "sarama-aborted-2")
	txn(true, "sarama-kept-1", "sarama-kept-2", "sarama-kept-3")

	got := values(reader(t, addr, "sarama-tx", true), 2*time.Second)
	if !eq(got, []string{"sarama-kept-1", "sarama-kept-2", "sarama-kept-3"}) {
		t.Errorf("read-committed saw %v, want only the committed transaction", got)
	}
}
