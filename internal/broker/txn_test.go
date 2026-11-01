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

// txProducer is a transactional producer for id on partition 0.
func txProducer(t *testing.T, addr, id string, extra ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(addr), kgo.TransactionalID(id), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.DialTimeout(3 * time.Second), kgo.RequestRetries(2),
	}, extra...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// reader consumes topic partition 0 from the start at an isolation level.
func reader(t *testing.T, addr, topic string, committed bool) *kgo.Client {
	t.Helper()
	iso := kgo.ReadUncommitted()
	if committed {
		iso = kgo.ReadCommitted()
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
		kgo.FetchIsolationLevel(iso), kgo.FetchMaxWait(200*time.Millisecond), kgo.DialTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// values polls for up to d and returns what arrived.
func values(cl *kgo.Client, d time.Duration) []string {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	var out []string
	for ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) { out = append(out, string(r.Value)) })
	}
	return out
}

func send(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, vals ...string) {
	t.Helper()
	for _, v := range vals {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte(v)}).FirstErr(); err != nil {
			t.Fatalf("produce %q: %v", v, err)
		}
	}
}

func commit(t *testing.T, ctx context.Context, cl *kgo.Client, ok bool) {
	t.Helper()
	if err := cl.EndTransaction(ctx, kgo.TransactionEndTry(ok)); err != nil {
		t.Fatalf("end transaction (commit=%v): %v", ok, err)
	}
}

func eq(got, want []string) bool { return fmt.Sprint(got) == fmt.Sprint(want) }

// A read-committed consumer sees a transaction's records only once it commits, never
// if it aborts, and a read-uncommitted one sees them all. An open transaction holds
// the last stable offset where it began.
func TestTransactionsCommitAndAbort(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	mustCreateTopic(t, ctx, admin, "tx", 1)

	p := txProducer(t, addr, "p1")
	committed, uncommitted := reader(t, addr, "tx", true), reader(t, addr, "tx", false)

	send0 := func(ok bool, vals ...string) {
		if err := p.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		send(t, ctx, p, "tx", vals...)
		if !ok {
			// While it is open, a read-committed consumer waits and an uncommitted one reads on.
			if got := values(uncommitted, time.Second); !eq(got, vals) {
				t.Errorf("read-uncommitted during the transaction saw %v, want %v", got, vals)
			}
			if got := values(committed, 800*time.Millisecond); len(got) != 0 {
				t.Errorf("read-committed saw %v of an open transaction", got)
			}
			if lso := listOffsetIso(t, ctx, admin, "tx", -1, 1); lso != 0 {
				t.Errorf("read-committed latest offset = %d during the transaction, want its start 0", lso)
			}
		}
		commit(t, ctx, p, ok)
	}

	send0(false, "aborted-1", "aborted-2")
	send0(true, "kept-1", "kept-2")

	if got := values(committed, 2*time.Second); !eq(got, []string{"kept-1", "kept-2"}) {
		t.Errorf("read-committed saw %v, want only the committed transaction's records", got)
	}
	// Four records and two markers: offsets 0-1 aborted, 2 marker, 3-4 kept, 5 marker.
	if end := listOffset(t, ctx, admin, "tx", 0, -1); end != 6 {
		t.Errorf("log end = %d, want 6 (four records and two markers)", end)
	}
	if lso := listOffsetIso(t, ctx, admin, "tx", -1, 1); lso != 6 {
		t.Errorf("last stable offset after both ended = %d, want 6", lso)
	}
}

// listOffsetIso is ListOffsets at an isolation level.
func listOffsetIso(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, ts int64, iso int8) int64 {
	t.Helper()
	return listOffsetIsoPart(t, ctx, cl, topic, 0, ts, iso)
}

// listOffsetIsoPart is listOffsetIso for any partition.
func listOffsetIsoPart(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, part int32, ts int64, iso int8) int64 {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID, req.IsolationLevel = -1, iso
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition, rp.Timestamp = part, ts
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || resp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("ListOffsets: %v %+v", err, resp)
	}
	return resp.Topics[0].Partitions[0].Offset
}

// A second producer with the same transactional id takes the id over: the first one's
// open transaction is aborted and it is fenced, so it cannot write into a decided
// transaction.
func TestAnotherProducerWithTheSameIDFencesTheFirst(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, newClient(t, addr), "fence", 1)

	old := txProducer(t, addr, "same")
	if err := old.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	send(t, ctx, old, "fence", "from-the-old-producer")

	fresh := txProducer(t, addr, "same")
	if err := fresh.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	send(t, ctx, fresh, "fence", "from-the-new-producer")
	commit(t, ctx, fresh, true)

	// The old producer is fenced: it cannot produce or commit.
	if err := old.ProduceSync(ctx, &kgo.Record{Topic: "fence", Partition: 0, Value: []byte("late")}).FirstErr(); err == nil {
		t.Error("the fenced producer could still write")
	}
	if err := old.EndTransaction(ctx, kgo.TryCommit); err == nil {
		t.Error("the fenced producer could still commit")
	}
	if got := values(reader(t, addr, "fence", true), 2*time.Second); !eq(got, []string{"from-the-new-producer"}) {
		t.Errorf("read-committed saw %v, want only the new producer's record", got)
	}
}

// A transaction left open past its timeout is aborted and its producer fenced.
func TestATransactionPastItsTimeoutIsAborted(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	mustCreateTopic(t, ctx, admin, "slow", 1)

	p := txProducer(t, addr, "slow-p", kgo.TransactionTimeout(2*time.Second))
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	send(t, ctx, p, "slow", "never-committed")
	if lso := listOffsetIso(t, ctx, admin, "slow", -1, 1); lso != 0 {
		t.Fatalf("LSO while open = %d, want 0", lso)
	}
	deadline := time.Now().Add(15 * time.Second)
	for listOffsetIso(t, ctx, admin, "slow", -1, 1) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the transaction was not aborted after its timeout")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := p.EndTransaction(ctx, kgo.TryCommit); err == nil {
		t.Error("the producer of a timed-out transaction could commit it")
	}
	if got := values(reader(t, addr, "slow", true), time.Second); len(got) != 0 {
		t.Errorf("read-committed saw %v of an aborted transaction", got)
	}
}

// The consume-process-produce loop with exactly-once: some transactions are aborted on
// purpose and the broker is restarted mid-run, and the output has every input once,
// no more, with the group's offset exactly at the end.
func TestExactlyOnceSurvivesAbortsAndARestart(t *testing.T) {
	skipShort(t)
	dir := t.TempDir()
	addr, stop := bootAt(t, "127.0.0.1:0", dir, 4)
	defer func() { stop() }()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	mustCreateTopic(t, ctx, admin, "in", 1)
	mustCreateTopic(t, ctx, admin, "out", 1)
	const n = 60
	var inputs []string
	for i := range n {
		inputs = append(inputs, fmt.Sprintf("m%03d", i))
	}
	plain := newClient(t, addr)
	for _, v := range inputs {
		if err := plain.ProduceSync(ctx, &kgo.Record{Topic: "in", Partition: 0, Value: []byte(v)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	newSession := func() *kgo.GroupTransactSession {
		s, err := kgo.NewGroupTransactSession(kgo.SeedBrokers(addr), kgo.TransactionalID("eos"), kgo.ConsumerGroup("eos-g"), kgo.ConsumeTopics("in"),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.RecordPartitioner(kgo.ManualPartitioner()),
			kgo.DialTimeout(3*time.Second), kgo.FetchMaxWait(200*time.Millisecond), kgo.FetchMaxBytes(256), kgo.RequestRetries(3))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	processed, round, restarted := 0, 0, false
	sess := newSession()
	for processed < n && ctx.Err() == nil {
		fs := sess.PollFetches(ctx)
		recs := fs.Records()
		if len(recs) == 0 {
			continue
		}
		round++
		if err := sess.Begin(); err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			sess.Produce(ctx, &kgo.Record{Topic: "out", Partition: 0, Value: append([]byte("out-"), r.Value...)}, func(_ *kgo.Record, err error) {
				if err != nil && !errors.Is(err, kgo.ErrAborting) {
					t.Errorf("produce: %v", err)
				}
			})
		}
		// Every third round the work is thrown away; midway through, the broker restarts.
		wantCommit := kgo.TransactionEndTry(round%3 != 0)
		if round == 5 && !restarted {
			restarted = true
			stop()
			stop = runBrokerAt(t, addr, dir, 4)
		}
		ok, err := sess.End(ctx, wantCommit)
		if err != nil {
			// A restart in the middle can fail an end; the session aborts and carries on.
			t.Logf("round %d: end: %v", round, err)
			continue
		}
		if ok && round%3 != 0 {
			processed += len(recs)
		}
	}
	sess.Close()

	out := values(reader(t, addr, "out", true), 3*time.Second)
	want := make([]string, 0, n)
	for _, v := range inputs {
		want = append(want, "out-"+v)
	}
	if !eq(out, want) {
		t.Errorf("the output has %d records (want %d, each input once, in order):\n got  %v\n want %v", len(out), n, out, want)
	}
	// The aborted rounds really did write: they are in the log, and hidden from readers
	// who ask for committed data.
	if all := values(reader(t, addr, "out", false), 2*time.Second); len(all) <= n {
		t.Errorf("read-uncommitted saw %d records, want more than %d: the aborted rounds left no records, so the test proved little", len(all), n)
	}
	if off := committedOffset(t, ctx, admin, "eos-g", "in"); off != n {
		t.Errorf("the group's offset on the input is %d, want %d", off, n)
	}
}

// committedOffset is a group's committed offset on partition 0 of topic.
func committedOffset(t *testing.T, ctx context.Context, cl *kgo.Client, group, topic string) int64 {
	t.Helper()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic, rt.Partitions = topic, []int32{0}
	req.Topics = append(req.Topics, rt)
	req.SetVersion(5)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
		t.Fatalf("OffsetFetch: %v %+v", err, resp)
	}
	return resp.Topics[0].Partitions[0].Offset
}
