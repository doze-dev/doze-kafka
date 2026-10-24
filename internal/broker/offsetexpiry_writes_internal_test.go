package broker

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// countingExpiry counts the writes a sweep makes, and can show every offset as one
// from before commit times were kept.
type countingExpiry struct {
	meta.Store
	expires   int
	unstamped bool
}

func (c *countingExpiry) ExpireOffsets(group string, expire func(topic string, partition int32, committedAt int64) bool) (int, error) {
	c.expires++
	return c.Store.ExpireOffsets(group, expire)
}

func (c *countingExpiry) ListOffsets(group string) ([]meta.OffsetEntry, error) {
	es, err := c.Store.ListOffsets(group)
	if c.unstamped {
		for i := range es {
			es[i].CommittedAt = 0
		}
	}
	return es, err
}

func newCountingRig(t *testing.T, now time.Time) (*expiryRig, *countingExpiry) {
	t.Helper()
	dir := t.TempDir()
	r := &expiryRig{now: now}
	m, err := meta.OpenWithClock(filepath.Join(dir, "meta.db"), func() time.Time { return r.now })
	if err != nil {
		t.Fatal(err)
	}
	c := &countingExpiry{Store: m}
	logs, err := log.OpenManager(filepath.Join(dir, "data"), log.ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.OffsetsRetention = testRetention
	b, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), logs, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Stop(); logs.Close(); m.Close() })
	b.expiry.stopAndFlush()
	r.b, r.m, r.exp = b, c, b.expiry
	b.expiry.now, b.expiry.started = func() time.Time { return r.now }, now
	if _, err := m.CreateTopic("a", 1, nil); err != nil {
		t.Fatal(err)
	}
	return r, c
}

// A sweep that finds nothing to expire writes nothing (an idle broker stays idle: the
// comment in expireOffsets), and one that finds an offset with no commit time writes,
// to give it one.
func TestOffsetRetentionSweepWritesOnlyWhenThereIsSomethingToDo(t *testing.T) {
	r, c := newCountingRig(t, time.Now())
	r.commit(t, "solo", "a", 1)

	r.exp.sweep()
	if c.expires != 0 {
		t.Fatalf("a sweep of a fresh offset wrote %d times", c.expires)
	}
	c.unstamped = true
	r.exp.sweep()
	if c.expires != 1 {
		t.Fatalf("a sweep of an offset with no commit time wrote %d times, want 1", c.expires)
	}
	c.unstamped = false
	r.advance(testRetention + time.Second)
	r.exp.sweep()
	if c.expires != 2 || r.has("solo", "a") {
		t.Fatalf("a sweep of an expired offset: %d writes, offset kept: %v", c.expires, r.has("solo", "a"))
	}
}
