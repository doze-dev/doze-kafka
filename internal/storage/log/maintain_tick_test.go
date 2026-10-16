package log

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// The maintenance loop's own lines used to be reached only when its one-second
// tick happened to land inside some other test, so the package's coverage read
// 84.9% on most runs and 85.1% on the others, and the floor had to be lowered to
// the smaller number by hand. These tests reach each of those lines on purpose:
// the tick's two warnings (by waiting for them), and the "nothing to do" returns
// (by calling them).

// lockedBuffer is a log sink the maintenance goroutine and the test both touch.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A tick whose compaction and whose retention both fail says so, once each per
// tick, and the loop goes on (the next tick says it again).
func TestMaintenanceTickReportsAFailedCompactionAndAFailedRetention(t *testing.T) {
	var out lockedBuffer
	m, err := OpenManager(t.TempDir(), ManagerOptions{
		SegmentBytes: 512, IndexIntervalBytes: 4096,
		Logger: slog.New(slog.NewTextHandler(&out, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close() // its error is the broken segment's; this test is about the loop.

	// Two topics with several sealed segments each and, so far, no limits: the
	// ticks that pass meanwhile have nothing to do on either.
	p, err := m.Partition("broken", 0)
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, p, 12)
	quiet, err := m.Partition("quiet", 0)
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, quiet, 12)

	// The oldest segment's file is closed under the partition: the cleaner cannot
	// read it and retention cannot close it again.
	p.mu.Lock()
	if len(p.segments) < 3 {
		p.mu.Unlock()
		t.Fatalf("want several segments, got %d", len(p.segments))
	}
	err = p.segments[0].log.Close()
	p.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	// Only now does the topic compact and exceed its size bound, so no tick can
	// have trimmed the broken segment away before it was broken.
	m.SetTopicLimits("broken", TopicLimits{
		Retention:  RetentionPolicy{MaxBytes: 1},
		Compaction: CompactionPolicy{Enabled: true, MaxLag: time.Nanosecond},
	})

	deadline := time.Now().Add(15 * time.Second)
	for _, want := range []string{"compaction failed", "retention enforcement failed"} {
		for !strings.Contains(out.String(), want) {
			if time.Now().After(deadline) {
				t.Fatalf("no %q from the maintenance loop within 15 s; it logged:\n%s", want, out.String())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	// The loop is still running: a later tick repeats the warning.
	first := strings.Count(out.String(), "retention enforcement failed")
	for strings.Count(out.String(), "retention enforcement failed") == first {
		if time.Now().After(deadline) {
			t.Fatalf("the loop stopped after its first failure; it logged:\n%s", out.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if strings.Contains(out.String(), "quiet") {
		t.Errorf("a topic with no limits was reported:\n%s", out.String())
	}
}

// The "nothing to do" answers, called rather than waited for: a policy with no
// bound trims nothing, open or not, and a topic that is not compacted is passed over.
func TestADisabledPolicyDoesNothing(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir, ManagerOptions{SegmentBytes: 512, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	p, err := m.Partition("plain", 0)
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, p, 12)
	p.mu.Lock()
	segments, end := len(p.segments), p.logEndOffset
	p.mu.Unlock()
	if segments < 3 {
		t.Fatalf("want several segments, got %d", segments)
	}

	var none RetentionPolicy
	if none.enabled() {
		t.Fatal("a policy with no bound is not enabled")
	}
	if err := none.Enforce(p); err != nil {
		t.Fatal(err)
	}
	if err := none.enforceDir(p.dir); err != nil {
		t.Fatal(err)
	}
	if err := m.EnforceCompaction(); err != nil {
		t.Fatal(err)
	}
	if err := m.EnforceRetention(); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.segments) != segments || p.logEndOffset != end || p.logStartOffset != 0 {
		t.Errorf("nothing was to change: %d segments (was %d), end %d (was %d), start %d", len(p.segments), segments, p.logEndOffset, end, p.logStartOffset)
	}
}

// A partition that is not open and has a single segment is left as it is by a
// policy that would otherwise trim it (the newest segment is never removed),
// and a directory that cannot be listed is an error, not "nothing to do". Both
// were reached only when the manager's first sweep happened to meet such a
// directory.
func TestClosedRetentionKeepsTheOnlySegmentAndReportsAnUnreadableDirectory(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, p, 3) // far below the segment size: one segment.
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	before, err := listSegmentBases(dir)
	if err != nil || len(before) != 1 {
		t.Fatalf("want one segment, got %v (%v)", before, err)
	}
	tight := RetentionPolicy{MaxBytes: 1, MaxAge: time.Nanosecond}
	if err := tight.enforceDir(dir); err != nil {
		t.Fatal(err)
	}
	if after, _ := listSegmentBases(dir); len(after) != 1 || after[0] != before[0] {
		t.Errorf("the only segment was touched: %v, was %v", after, before)
	}
	if err := tight.enforceDir(dir + "-gone"); err == nil {
		t.Error("a directory that cannot be listed must be an error")
	}
}
