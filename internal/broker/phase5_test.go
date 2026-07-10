package broker_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// listOffset queries a partition's earliest (-2) or latest (-1) offset via
// ListOffsets, driving the same path a real consumer uses to resolve AtStart/AtEnd.
func listOffset(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, partition int32, timestamp int64) int64 {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition = partition
	rp.Timestamp = timestamp
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("ListOffsets: %v", err)
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("ListOffsets: malformed response %+v", resp.Topics)
	}
	pr := resp.Topics[0].Partitions[0]
	if pr.ErrorCode != 0 {
		t.Fatalf("ListOffsets: error code %d", pr.ErrorCode)
	}
	return pr.Offset
}

// waitStableEarliest polls the earliest offset until it stops advancing (two equal
// readings ~150ms apart) or the deadline passes, then returns it. Once production
// stops, background retention reaches a fixed point quickly, so this pins down a
// stable log-start offset without racing the reaper.
func waitStableEarliest(t *testing.T, ctx context.Context, cl *kgo.Client, topic string) int64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	prev := int64(-1)
	for time.Now().Before(deadline) {
		cur := listOffset(t, ctx, cl, topic, 0, -2)
		if cur == prev && cur > 0 {
			return cur
		}
		prev = cur
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("earliest offset never stabilised above 0 (retention did not trim)")
	return 0
}

// TestPhase5_RetentionTrimsUnderLoadAndSurvivesRestart is the Phase 5 acceptance
// gate: under sustained produce with a size bound configured, old segments are
// deleted (the log-start offset advances past 0); the retained tail is contiguous
// and correctly offset; and it survives a graceful restart — nothing acked and
// still within retention is lost.
func TestPhase5_RetentionTrimsUnderLoadAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))

	cfg := config.Default()
	cfg.Version = 4
	cfg.ListenAddr = addr
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.MetaDB = filepath.Join(dir, "meta.db")
	cfg.SegmentBytes = 4 << 10    // 4 KiB segments → frequent rolls.
	cfg.RetentionBytes = 16 << 10 // keep ~16 KiB per partition.
	cfg.FsyncInterval = 25 * time.Millisecond

	stop := runBroker(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	producer := newClient(t, addr)
	mustCreateTopic(t, ctx, producer, "logs", 1)

	// One record per offset (synchronous produce → one batch each), value "m{offset}".
	// The total on disk (thousands of small batches) dwarfs the 16 KiB bound, so
	// retention must delete old segments.
	const n = 4000
	for i := range n {
		rec := &kgo.Record{Topic: "logs", Value: fmt.Appendf(nil, "m%d", i)}
		if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce m%d: %v", i, err)
		}
	}
	producer.Close()

	admin := newClient(t, addr)
	defer admin.Close()

	if latest := listOffset(t, ctx, admin, "logs", 0, -1); latest != n {
		t.Fatalf("latest offset = %d, want %d", latest, n)
	}
	earliest := waitStableEarliest(t, ctx, admin, "logs")
	t.Logf("retention trimmed to earliest offset %d of %d produced", earliest, n)

	// The retained tail consumes contiguously, correctly offset: record at offset o
	// has value "m{o}".
	verifyTail := func(when string) {
		got := consumeFromStart(t, ctx, addr, "logs", int(int64(n)-earliest))
		if want := fmt.Sprintf("m%d", earliest); got[0] != want {
			t.Fatalf("%s: first retained record = %q, want %q", when, got[0], want)
		}
		for i, v := range got {
			if want := fmt.Sprintf("m%d", earliest+int64(i)); v != want {
				t.Fatalf("%s: record %d = %q, want %q (tail not contiguous)", when, i, v, want)
			}
		}
	}
	verifyTail("before restart")

	// Graceful restart over the same data dir: the trimmed state and the retained
	// tail must persist exactly.
	stop()
	stop2 := runBroker(t, cfg)
	defer stop2()

	admin2 := newClient(t, addr)
	defer admin2.Close()
	if e := listOffset(t, ctx, admin2, "logs", 0, -2); e != earliest {
		t.Fatalf("after restart earliest = %d, want %d (retention state not durable)", e, earliest)
	}
	if l := listOffset(t, ctx, admin2, "logs", 0, -1); l != n {
		t.Fatalf("after restart latest = %d, want %d", l, n)
	}
	verifyTail("after restart")
}
