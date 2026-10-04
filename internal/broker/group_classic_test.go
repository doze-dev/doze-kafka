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

// groupConsume reads exactly n records from a topic as a member of the named
// consumer group, starting from the group's committed offset (or the topic start
// when nothing is committed). If commit is true it commits through the last
// consumed record before returning. Autocommit is disabled so the commit point is
// deterministic.
func groupConsume(t *testing.T, ctx context.Context, addr, group, topic string, n int, commit bool) []string {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumerGroup(group),
		kgo.GroupProtocol("classic"), // pin to classic; version 4 also advertises KIP-848.
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.DialTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("group consumer client: %v", err)
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
				t.Fatalf("group fetch error: %v", e.Err)
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

// TestGroupClassic_GroupConsumeCommitAndResume is the core Phase 4 acceptance gate: a
// franz-go group consumer joins, gets an assignment, consumes, and commits; then
// after a broker restart a fresh consumer in the same group resumes from the
// committed offset — it sees only records produced after the commit, not a replay.
func TestGroupClassic_GroupConsumeCommitAndResume(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))

	stop := runBrokerAt(t, addr, dir, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	producer := newClient(t, addr)
	mustCreateTopic(t, ctx, producer, "orders", 1)

	const first = 20
	for i := range first {
		rec := &kgo.Record{Topic: "orders", Value: fmt.Appendf(nil, "o%d", i)}
		if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce o%d: %v", i, err)
		}
	}

	// First member consumes everything and commits its position.
	got := groupConsume(t, ctx, addr, "shipping", "orders", first, true)
	for i := range first {
		if want := fmt.Sprintf("o%d", i); got[i] != want {
			t.Fatalf("record %d = %q, want %q", i, got[i], want)
		}
	}

	// Restart the broker over the same data dir: committed offsets must survive.
	stop()
	stop2 := runBrokerAt(t, addr, dir, 4)
	defer stop2()

	// Produce more, then rejoin the same group. Resuming from the committed offset,
	// we must see exactly the new records — no replay of the first batch.
	const more = 10
	producer2 := newClient(t, addr)
	for i := first; i < first+more; i++ {
		rec := &kgo.Record{Topic: "orders", Value: fmt.Appendf(nil, "o%d", i)}
		if err := producer2.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce o%d: %v", i, err)
		}
	}
	producer2.Close()

	resumed := groupConsume(t, ctx, addr, "shipping", "orders", more, true)
	for i := range more {
		if want := fmt.Sprintf("o%d", first+i); resumed[i] != want {
			t.Fatalf("after resume, record %d = %q, want %q (replayed or lost)", i, resumed[i], want)
		}
	}
}

// TestGroupClassic_TwoConsumersSplitPartitions checks that two members of one group split
// a two-partition topic: each is assigned a partition, and together they consume
// every record. It waits for a stable two-member assignment (both partitions
// covered) before producing, so neither member drains the topic before the other
// joins.
func TestGroupClassic_TwoConsumersSplitPartitions(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	defer runBrokerAt(t, addr, dir, 4)()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const parts = 2
	const perPart = 25
	total := parts * perPart

	// Manual partitioner so we control which partition each record lands on.
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.DialTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	mustCreateTopic(t, ctx, producer, "work", parts)

	var (
		mu          sync.Mutex
		seen        = map[string]int{} // value -> delivery count.
		perConsumer = make([]int, 2)
		assignedN   = make([]int, 2) // partitions currently assigned to each member.
	)
	stable := make(chan struct{})
	var stableOnce sync.Once

	held := func(idx int, a map[string][]int32, sign int) {
		n := 0
		for _, ps := range a {
			n += len(ps)
		}
		mu.Lock()
		assignedN[idx] += sign * n
		bothHavePartition := assignedN[0] > 0 && assignedN[1] > 0
		mu.Unlock()
		if bothHavePartition {
			stableOnce.Do(func() { close(stable) })
		}
	}

	consume := func(idx int) {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(addr),
			kgo.ConsumerGroup("workers"),
			kgo.GroupProtocol("classic"), // pin to classic; version 4 also advertises KIP-848.
			kgo.ConsumeTopics("work"),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			// The balancer is cooperative: a callback reports what was ADDED (or
			// taken away), not the whole assignment. Counting only the latest
			// callback's size called a member that kept its partition "empty", and
			// the test then waited a minute for a split that had already happened.
			kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, a map[string][]int32) {
				held(idx, a, +1)
			}),
			kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, a map[string][]int32) {
				held(idx, a, -1)
			}),
			kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, a map[string][]int32) {
				held(idx, a, -1)
			}),
			kgo.DialTimeout(2*time.Second),
		)
		if err != nil {
			t.Errorf("consumer %d client: %v", idx, err)
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
				cancel() // all distinct records delivered; unblock both pollers.
			}
		}
	}

	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func(i int) { defer wg.Done(); consume(i) }(i)
	}

	// Wait for a stable two-member group (each holds a partition) before producing.
	select {
	case <-stable:
	case <-ctx.Done():
		t.Fatalf("group did not reach a two-member split assignment in time")
	}

	for p := range parts {
		for i := range perPart {
			rec := &kgo.Record{Topic: "work", Partition: int32(p), Value: fmt.Appendf(nil, "p%d-%d", p, i)}
			if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
				t.Fatalf("produce p%d-%d: %v", p, i, err)
			}
		}
	}
	producer.Close()

	wg.Wait()

	// Every produced record delivered at least once.
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
	// Both members did real work — the partitions were genuinely split.
	if perConsumer[0] == 0 || perConsumer[1] == 0 {
		t.Fatalf("partitions not split: consumer0 read %d, consumer1 read %d", perConsumer[0], perConsumer[1])
	}
}
