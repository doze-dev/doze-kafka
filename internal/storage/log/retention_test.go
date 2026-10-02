package log

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// fillSegments appends count fixed-size batches (forcing rolls at segmentBytes) and
// returns the batch size in bytes.
func fillSegments(t *testing.T, p *Partition, count int) int {
	t.Helper()
	batch := makeBatch(1, make([]byte, 200)) // 227 bytes on disk.
	for range count {
		if _, err := p.Append(batch); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	return len(batch)
}

func TestRetentionByBytesDropsOldestSegments(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 512, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()

	const n = 40
	fillSegments(t, p, n) // ~227 B batches into 512 B segments → ~20 segments.
	if len(p.segments) < 3 {
		t.Fatalf("expected several segments, got %d", len(p.segments))
	}
	leoBefore := p.LogEndOffset()

	// Keep the partition under ~1 KiB — a couple of segments.
	if err := (RetentionPolicy{MaxBytes: 1024}).Enforce(p); err != nil {
		t.Fatalf("enforce: %v", err)
	}

	start := p.LogStartOffset()
	if start == 0 {
		t.Fatalf("log-start offset did not advance; retention deleted nothing")
	}
	if p.LogEndOffset() != leoBefore {
		t.Fatalf("LEO changed under retention: %d != %d", p.LogEndOffset(), leoBefore)
	}
	// Deleted data is gone; the retained tail still reads.
	if _, err := p.Read(0, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("Read(0) = %v, want ErrOffsetOutOfRange", err)
	}
	if got, err := p.Read(start, 1<<20); err != nil || len(got) == 0 {
		t.Fatalf("Read(start=%d): %d bytes, err=%v", start, len(got), err)
	}

	// Total retained size is within the bound plus the active segment.
	var total int64
	for _, s := range p.segments {
		total += s.sizeBytes
	}
	if total > 1024+p.segmentBytes {
		t.Fatalf("retained %d bytes, above bound+active", total)
	}
}

func TestRetentionByAgeDropsOldSegments(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 512, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()

	fillSegments(t, p, 40)
	if len(p.segments) < 3 {
		t.Fatalf("expected several segments, got %d", len(p.segments))
	}

	// Age every sealed segment well past the cutoff; the active one stays fresh.
	old := time.Now().Add(-time.Hour)
	for i := 0; i < len(p.segments)-1; i++ {
		p.segments[i].lastModified = old
	}

	if err := (RetentionPolicy{MaxAge: time.Minute}).Enforce(p); err != nil {
		t.Fatalf("enforce: %v", err)
	}
	if len(p.segments) != 1 {
		t.Fatalf("age retention left %d segments, want 1 (only the active)", len(p.segments))
	}
	if p.LogStartOffset() != p.active().baseOffset {
		t.Fatalf("log-start %d != active base %d", p.LogStartOffset(), p.active().baseOffset)
	}
}

func TestRetentionNeverDeletesActiveSegment(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()

	fillSegments(t, p, 5) // one segment (no roll).
	// A bound smaller than the data must still leave the single active segment.
	if err := (RetentionPolicy{MaxBytes: 1}).Enforce(p); err != nil {
		t.Fatalf("enforce: %v", err)
	}
	if len(p.segments) != 1 || p.LogStartOffset() != 0 {
		t.Fatalf("active segment was deleted: segments=%d start=%d", len(p.segments), p.LogStartOffset())
	}
}

// TestRecoverAfterCrashWithoutClose simulates a kill -9: data is synced but the
// partition is never closed, then reopened from the same directory. Everything
// durably written must recover.
func TestRecoverAfterCrashWithoutClose(t *testing.T) {
	dir := t.TempDir()
	crashed, err := openPartition(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}

	const n = 100
	for range n {
		if _, err := crashed.Append(makeBatch(1, []byte("payload"))); err != nil {
			t.Fatal(err)
		}
	}
	if err := crashed.Sync(); err != nil { // fsync, but deliberately no close().
		t.Fatalf("sync: %v", err)
	}

	reopened, err := openPartition(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if leo := reopened.LogEndOffset(); leo != n {
		t.Fatalf("recovered LEO=%d, want %d", leo, n)
	}
	if got, err := reopened.Read(0, 1<<20); err != nil || len(got) == 0 {
		t.Fatalf("read after crash-recovery: %d bytes, err=%v", len(got), err)
	}
	_ = crashed.close() // release the crashed handle now that recovery is verified.
}

// TestManagerSyncAndRetention exercises the manager's exported maintenance entry
// points (used by the background loop) against a real partition.
func TestManagerSyncAndRetention(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir, ManagerOptions{
		SegmentBytes:       512,
		IndexIntervalBytes: 4096,
		FsyncInterval:      10 * time.Millisecond,
		Retention:          RetentionPolicy{MaxBytes: 1024},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, p, 40)

	if err := m.Sync(); err != nil {
		t.Fatalf("manager sync: %v", err)
	}
	if err := m.EnforceRetention(); err != nil {
		t.Fatalf("manager retention: %v", err)
	}
	if p.LogStartOffset() == 0 {
		t.Fatalf("retention via manager did not advance the log-start offset")
	}
}

// After a restart nothing is open, and a sweep of only the open set never trims a
// topic nobody has touched since: its old segments would stay on disk forever.
func TestRetentionTrimsPartitionsThatAreNotOpen(t *testing.T) {
	dir := t.TempDir()
	opts := ManagerOptions{SegmentBytes: 512, IndexIntervalBytes: 64}

	m, err := OpenManager(dir, opts) // no retention yet: just write data.
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.Partition("cold", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		if _, err := p.Append(makeBatch(1, bytes.Repeat([]byte{'x'}, 100))); err != nil {
			t.Fatal(err)
		}
	}
	segs := func() int { b, _ := listSegmentBases(filepath.Join(dir, "cold-0")); return len(b) }
	before := segs()
	if before < 4 {
		t.Fatalf("setup: only %d segments", before)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	opts.Retention = RetentionPolicy{MaxBytes: 1024}
	m2, err := OpenManager(dir, opts) // restarted: "cold" is not open.
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	if err := m2.EnforceRetention(); err != nil { // the open-set sweep alone does nothing here.
		t.Fatal(err)
	}
	if got := segs(); got != before {
		t.Fatalf("the open-set sweep changed an unopened partition (%d -> %d segments)", before, got)
	}
	if err := m2.EnforceClosedRetention(); err != nil {
		t.Fatal(err)
	}
	after := segs()
	if after >= before || after < 1 {
		t.Fatalf("segments %d -> %d; the cold partition was not trimmed (or the newest was lost)", before, after)
	}

	// Opening it afterwards finds a consistent log: the start offset follows the
	// first surviving segment and the end offset is untouched.
	p2, err := m2.Partition("cold", 0)
	if err != nil {
		t.Fatal(err)
	}
	if p2.LogEndOffset() != 40 {
		t.Errorf("LEO = %d, want 40", p2.LogEndOffset())
	}
	if p2.LogStartOffset() <= 0 {
		t.Errorf("log start offset = %d, want it advanced past the removed segments", p2.LogStartOffset())
	}
	if _, err := p2.Read(p2.LogStartOffset(), 1<<20); err != nil {
		t.Errorf("the first retained offset is not readable: %v", err)
	}
}
