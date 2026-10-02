package broker_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// TestRetention_TrimsUnderLoadAndSurvivesRestart: under sustained produce with a size bound configured, old segments are
// deleted (the log-start offset advances past 0); the retained tail is contiguous
// and correctly offset; and it survives a graceful restart — nothing acked and
// still within retention is lost.
func TestRetention_TrimsUnderLoadAndSurvivesRestart(t *testing.T) {
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
	// The total on disk (~45 KiB of small batches) is nearly three times the 16 KiB
	// bound, so retention must delete old segments. Every produce here is acks=all
	// and so fsyncs; a few hundred keeps the run to seconds, where thousands made
	// it fsync-bound and flaky against its deadline on a slow disk.
	const n = 600
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
	verifyTail := func(when string, from int64) {
		got := consumeFromStart(t, ctx, addr, "logs", int(int64(n)-from))
		if want := fmt.Sprintf("m%d", from); got[0] != want {
			t.Fatalf("%s: first retained record = %q, want %q", when, got[0], want)
		}
		for i, v := range got {
			if want := fmt.Sprintf("m%d", from+int64(i)); v != want {
				t.Fatalf("%s: record %d = %q, want %q (tail not contiguous)", when, i, v, want)
			}
		}
	}
	verifyTail("before restart", earliest)

	// Graceful restart over the same data dir: the trimmed state and the retained
	// tail must persist exactly.
	stop()
	stop2 := runBroker(t, cfg)
	defer stop2()

	admin2 := newClient(t, addr)
	defer admin2.Close()
	// Retention keeps trimming after a restart (a sweep now covers partitions that
	// have not been opened yet), so the start offset may move FORWARD; what must
	// never happen is trimmed data coming back.
	restartedEarliest := listOffset(t, ctx, admin2, "logs", 0, -2)
	if restartedEarliest < earliest {
		t.Fatalf("after restart earliest = %d, below %d: trimmed data came back (retention state not durable)", restartedEarliest, earliest)
	}
	if l := listOffset(t, ctx, admin2, "logs", 0, -1); l != n {
		t.Fatalf("after restart latest = %d, want %d", l, n)
	}
	verifyTail("after restart", restartedEarliest)
}
