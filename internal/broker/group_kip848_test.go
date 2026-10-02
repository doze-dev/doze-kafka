package broker_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kip848Opts returns the client options that make franz-go actually use the KIP-848
// broker-driven protocol. In franz v1.21.5 the new protocol is behind a beta
// opt-in: it requires a context value plus a sticky/range balancer, otherwise the
// client silently falls back to classic even with GroupProtocol("consumer").
func kip848Opts() []kgo.Opt {
	var betaKey any = "opt_in_kafka_next_gen_balancer_beta" // any-typed key: matches franz's lookup, dodges the vet string-key check.
	ctx848 := context.WithValue(context.Background(), betaKey, true)
	return []kgo.Opt{
		kgo.WithContext(ctx848),
		kgo.GroupProtocol("consumer"),
		kgo.Balancers(kgo.StickyBalancer()),
	}
}

// newProtocolConsume reads exactly n records from a topic as a member of a KIP-848
// (group.protocol=consumer) group, from the committed offset or the topic start,
// committing through the last record if commit is true. The only difference from the
// classic groupConsume is kgo.GroupProtocol("consumer"), which drives the
// broker-side ConsumerGroupHeartbeat path.
func newProtocolConsume(t *testing.T, ctx context.Context, addr, group, topic string, n int, commit bool) []string {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.DialTimeout(2 * time.Second),
	}, kip848Opts()...)...)
	if err != nil {
		t.Fatalf("new-protocol consumer client: %v", err)
	}
	defer cl.Close()

	var got []string
	for len(got) < n {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.DeadlineExceeded) || errors.Is(e.Err, context.Canceled) {
					t.Fatalf("timed out consuming group %s (%d/%d): %v", group, len(got), n, e.Err)
				}
				t.Fatalf("new-protocol fetch error: %v", e.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	if commit {
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			t.Fatalf("commit offsets for group %s: %v", group, err)
		}
	}
	return got
}

// TestGroupKIP848_NewProtocolConsumeCommitResume is the Phase 6 acceptance gate: a
// franz-go client using the KIP-848 broker-driven protocol (group.protocol=consumer,
// no classic override) joins, gets a broker-computed assignment, consumes, and
// commits; after a broker restart a fresh member of the same group resumes from the
// committed offset — no replay.
func TestGroupKIP848_NewProtocolConsumeCommitResume(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))

	stop := runBrokerAt(t, addr, dir, 4) // version 4 advertises ConsumerGroupHeartbeat.

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	producer := newClient(t, addr)
	mustCreateTopic(t, ctx, producer, "orders848", 1)

	const first = 20
	for i := range first {
		rec := &kgo.Record{Topic: "orders848", Value: fmt.Appendf(nil, "o%d", i)}
		if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce o%d: %v", i, err)
		}
	}

	got := newProtocolConsume(t, ctx, addr, "shipping848", "orders848", first, true)
	for i := range first {
		if want := fmt.Sprintf("o%d", i); got[i] != want {
			t.Fatalf("record %d = %q, want %q", i, got[i], want)
		}
	}

	// Restart; committed offsets must survive.
	stop()
	stop2 := runBrokerAt(t, addr, dir, 4)
	defer stop2()

	const more = 10
	producer2 := newClient(t, addr)
	for i := first; i < first+more; i++ {
		rec := &kgo.Record{Topic: "orders848", Value: fmt.Appendf(nil, "o%d", i)}
		if err := producer2.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce o%d: %v", i, err)
		}
	}
	producer2.Close()

	resumed := newProtocolConsume(t, ctx, addr, "shipping848", "orders848", more, true)
	for i := range more {
		if want := fmt.Sprintf("o%d", first+i); resumed[i] != want {
			t.Fatalf("after resume, record %d = %q, want %q (replayed or lost)", i, resumed[i], want)
		}
	}
}

// TestGroupKIP848_NewProtocolTwoConsumersSplit checks the broker-side assignor and
// incremental reconciliation: two members of one group each get a partition of a
// two-partition topic, and together they consume every record with both doing real
// work.
func TestGroupKIP848_NewProtocolTwoConsumersSplit(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	defer runBrokerAt(t, addr, dir, 4)()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const parts = 2
	const perPart = 25
	total := parts * perPart

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.DialTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	mustCreateTopic(t, ctx, producer, "work848", parts)

	var (
		mu          sync.Mutex
		seen        = map[string]int{}
		perConsumer = make([]int, 2)
		assignedN   = make([]int, 2)
	)
	stable := make(chan struct{})
	var stableOnce sync.Once

	consume := func(idx int) {
		cl, err := kgo.NewClient(append([]kgo.Opt{
			kgo.SeedBrokers(addr),
			kgo.ConsumerGroup("workers848"),
			kgo.ConsumeTopics("work848"),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, a map[string][]int32) {
				n := 0
				for _, ps := range a {
					n += len(ps)
				}
				mu.Lock()
				assignedN[idx] = n
				both := assignedN[0] > 0 && assignedN[1] > 0
				mu.Unlock()
				if both {
					stableOnce.Do(func() { close(stable) })
				}
			}),
			kgo.DialTimeout(2 * time.Second),
		}, kip848Opts()...)...)
		if err != nil {
			t.Errorf("consumer %d: %v", idx, err)
			return
		}
		defer cl.Close()
		for {
			fetches := cl.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
						return
					}
					t.Errorf("consumer %d fetch error: %v", idx, e.Err)
					return
				}
			}
			var done bool
			fetches.EachRecord(func(r *kgo.Record) {
				mu.Lock()
				seen[string(r.Value)]++
				perConsumer[idx]++
				done = len(seen) == total
				mu.Unlock()
			})
			if done {
				cancel()
			}
		}
	}

	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func(i int) { defer wg.Done(); consume(i) }(i)
	}

	// Wait for a stable two-member split (each holds a partition) before producing.
	select {
	case <-stable:
	case <-ctx.Done():
		t.Fatalf("KIP-848 group did not reach a two-member split assignment in time")
	}

	for p := range parts {
		for i := range perPart {
			rec := &kgo.Record{Topic: "work848", Partition: int32(p), Value: fmt.Appendf(nil, "p%d-%d", p, i)}
			if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
				t.Fatalf("produce p%d-%d: %v", p, i, err)
			}
		}
	}
	producer.Close()

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total {
		var missing []string
		for p := range parts {
			for i := range perPart {
				v := fmt.Sprintf("p%d-%d", p, i)
				if seen[v] == 0 {
					missing = append(missing, v)
				}
			}
		}
		sort.Strings(missing)
		t.Fatalf("delivered %d/%d distinct records; missing %v", len(seen), total, missing)
	}
	if perConsumer[0] == 0 || perConsumer[1] == 0 {
		t.Fatalf("partitions not split: consumer0 read %d, consumer1 read %d", perConsumer[0], perConsumer[1])
	}
}
