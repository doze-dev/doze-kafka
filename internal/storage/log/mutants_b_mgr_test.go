package log

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Tests of the retention sweep's bounds and of the manager's bookkeeping: what it
// removes, what it reports, and what it leaves on disk.

// retentionFixture is a partition of segments holding two batches each.
func retentionFixture(t *testing.T) (*Partition, int64) {
	t.Helper()
	p, err := openPartition(t.TempDir(), ManagerOptions{SegmentBytes: 600, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.close() })
	fillSegments(t, p, 8) // 227 byte batches: two to a segment.
	if len(p.segments) != 4 {
		t.Fatalf("%d segments, want 4", len(p.segments))
	}
	var total int64
	for _, s := range p.segments {
		total += s.sizeBytes
	}
	return p, total
}

func logFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".log") {
			out = append(out, e.Name())
		}
	}
	return out
}

// The size bound is a bound on the total: a partition of exactly that size is within
// it; one byte over loses its oldest segment, whole, and the log start moves up to
// the next segment's base.
func TestB_RetentionBySizeKeepsExactlyTheBound(t *testing.T) {
	p, total := retentionFixture(t)
	if err := (RetentionPolicy{MaxBytes: total}).Enforce(p); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 4 || p.LogStartOffset() != 0 {
		t.Fatalf("at exactly the bound: %d segments, log start %d", len(p.segments), p.LogStartOffset())
	}
	if err := (RetentionPolicy{MaxBytes: total - 1}).Enforce(p); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 3 || p.LogStartOffset() != 2 {
		t.Fatalf("one byte over: %d segments, log start %d, want 3 and 2", len(p.segments), p.LogStartOffset())
	}
	if got := logFiles(t, p.dir); len(got) != 3 || got[0] != segmentName(2)+".log" {
		t.Errorf("segment files = %v", got)
	}
}

// A policy with only a size bound has no age bound, and one with only an age bound has no
// size bound: young segments, and a partition under its size, are kept.
func TestB_RetentionBoundsOfZeroAreOff(t *testing.T) {
	p, total := retentionFixture(t)
	if err := (RetentionPolicy{MaxBytes: total + 1}).Enforce(p); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 4 {
		t.Errorf("a size bound with no age bound removed segments: %d left", len(p.segments))
	}
	if err := (RetentionPolicy{MaxAge: time.Hour}).Enforce(p); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 4 {
		t.Errorf("an age bound of an hour removed young segments (or a size bound of zero removed the partition): %d left", len(p.segments))
	}
}

// Segments older than the age bound go, oldest first, and the newest never goes.
func TestB_RetentionByAgeRemovesTheOldAndKeepsTheLast(t *testing.T) {
	p, _ := retentionFixture(t)
	old := time.Now().Add(-2 * time.Hour)
	p.mu.Lock()
	p.segments[0].lastModified, p.segments[1].lastModified = old, old
	p.mu.Unlock()
	if err := (RetentionPolicy{MaxAge: time.Hour}).Enforce(p); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 2 || p.LogStartOffset() != 4 {
		t.Fatalf("%d segments, log start %d, want the two old ones gone: 2 and 4", len(p.segments), p.LogStartOffset())
	}
	// Everything old: the last segment stays.
	p.mu.Lock()
	for _, s := range p.segments {
		s.lastModified = old
	}
	p.mu.Unlock()
	if err := (RetentionPolicy{MaxAge: time.Hour}).Enforce(p); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 1 || len(logFiles(t, p.dir)) != 1 {
		t.Errorf("%d segments, files %v: the active segment must stay", len(p.segments), logFiles(t, p.dir))
	}
	// And a lone old segment is not removed by an age bound.
	if err := (RetentionPolicy{MaxAge: time.Nanosecond}).Enforce(p); err != nil {
		t.Fatal(err)
	}
	if len(p.segments) != 1 {
		t.Errorf("the only segment was removed")
	}
}

// The same bounds on a partition that is not open, from file sizes and times alone.
func TestB_RetentionOfAClosedPartitionFollowsTheSameBounds(t *testing.T) {
	build := func(t *testing.T) (dir string, total int64) {
		p, total := retentionFixture(t)
		dir = p.dir
		if err := p.close(); err != nil {
			t.Fatal(err)
		}
		return dir, total
	}
	bases := func(t *testing.T, dir string) []int64 {
		b, err := listSegmentBases(dir)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	t.Run("size", func(t *testing.T) {
		dir, total := build(t)
		if err := (RetentionPolicy{MaxBytes: total}).enforceDir(dir); err != nil {
			t.Fatal(err)
		}
		if got := bases(t, dir); !reflect.DeepEqual(got, []int64{0, 2, 4, 6}) {
			t.Fatalf("at exactly the bound: %v", got)
		}
		if err := (RetentionPolicy{MaxBytes: total - 1}).enforceDir(dir); err != nil {
			t.Fatal(err)
		}
		if got := bases(t, dir); !reflect.DeepEqual(got, []int64{2, 4, 6}) {
			t.Fatalf("one byte over: %v", got)
		}
	})
	t.Run("zero bounds are off", func(t *testing.T) {
		dir, total := build(t)
		if err := (RetentionPolicy{MaxBytes: total + 1}).enforceDir(dir); err != nil {
			t.Fatal(err)
		}
		if got := bases(t, dir); len(got) != 4 {
			t.Fatalf("a size bound alone removed segments: %v", got)
		}
		if err := (RetentionPolicy{MaxAge: time.Hour}).enforceDir(dir); err != nil {
			t.Fatal(err)
		}
		if got := bases(t, dir); len(got) != 4 {
			t.Fatalf("an age bound alone removed young segments: %v", got)
		}
	})
	t.Run("age", func(t *testing.T) {
		dir, _ := build(t)
		old := time.Now().Add(-2 * time.Hour)
		for _, b := range []int64{0, 2} {
			if err := os.Chtimes(filepath.Join(dir, segmentName(b)+".log"), old, old); err != nil {
				t.Fatal(err)
			}
		}
		if err := (RetentionPolicy{MaxAge: time.Hour}).enforceDir(dir); err != nil {
			t.Fatal(err)
		}
		if got := bases(t, dir); !reflect.DeepEqual(got, []int64{4, 6}) {
			t.Fatalf("segments left %v, want the two young ones", got)
		}
	})
}

// A partition that is not open and cannot be trimmed is an error of the sweep, and the
// sweep goes on to the others.
func TestB_ClosedRetentionReportsADirectoryItCouldNotTrim(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir, ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096, Retention: RetentionPolicy{MaxBytes: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	bad := filepath.Join(dir, "broken-0")
	if err := os.MkdirAll(bad, 0o755); err != nil {
		t.Fatal(err)
	}
	// The older segment's log is a link to nothing: the sweep cannot size it.
	if err := os.Symlink(filepath.Join(bad, "nowhere"), filepath.Join(bad, segmentName(0)+".log")); err != nil {
		t.Skipf("no symlinks here: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bad, segmentName(5)+".log"), make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := m.EnforceClosedRetention(); err == nil {
		t.Error("EnforceClosedRetention reported nothing for a partition it could not size")
	}
}

// ---- the manager ----

func openLoggedManager(t *testing.T, opts ManagerOptions) (*Manager, *lockedBuffer) {
	t.Helper()
	var out lockedBuffer
	opts.Logger = slog.New(slog.NewTextHandler(&out, nil))
	if opts.SegmentBytes == 0 {
		opts.SegmentBytes = 1 << 20
	}
	if opts.IndexIntervalBytes == 0 {
		opts.IndexIntervalBytes = 4096
	}
	m, err := OpenManager(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return m, &out
}

// breakPartition closes a segment's file under the partition: from then on a flush of
// it, and the partition's close, fail.
func breakPartition(t *testing.T, p *Partition) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.segments[0].log.Close(); err != nil {
		t.Fatal(err)
	}
}

// The manager answers for partitions that are not open from their files, keeps the answer
// until the files change, and does not keep an answer that failed.
func TestB_BoundsOfAClosedPartitionAreCachedUntilTheFilesChange(t *testing.T) {
	m, _ := openLoggedManager(t, ManagerOptions{})
	defer m.Close()
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, p, 3)
	dir := p.dir
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m2, err := OpenManager(filepath.Dir(dir), ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	start, end, err := m2.Bounds("t", 0)
	if err != nil || start != 0 || end != 3 {
		t.Fatalf("Bounds = %d, %d, %v, want 0, 3", start, end, err)
	}
	tp := topicPartition{"t", 0}
	c, ok := m2.bounds[tp]
	if !ok || c.start != 0 || c.end != 3 {
		t.Fatalf("the answer was not kept: %+v, %v", c, ok)
	}
	// While the files are as they were, the kept answer is the answer.
	m2.bounds[tp] = cachedBounds{sig: c.sig, start: 7, end: 9}
	if s, e, _ := m2.Bounds("t", 0); s != 7 || e != 9 {
		t.Errorf("the kept answer was not used for unchanged files: %d, %d", s, e)
	}
	// A change to the files makes it stale.
	f, err := os.OpenFile(filepath.Join(dir, segmentName(0)+".log"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(makeBatch(1, make([]byte, 20))); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, e, err := m2.Bounds("t", 0); err != nil || e == 9 {
		t.Errorf("after the log changed, Bounds end = %d, %v: a stale answer", e, err)
	}
	// A partition with nothing on disk has nothing to keep.
	if s, e, err := m2.Bounds("empty", 0); err != nil || s != 0 || e != 0 {
		t.Errorf("Bounds of a partition with no files = %d, %d, %v", s, e, err)
	}
	if _, kept := m2.bounds[topicPartition{"empty", 0}]; kept {
		t.Error("an answer with nothing to identify it was kept")
	}
}

// Creating a topic's partition directories makes those of its partitions, and no more.
func TestB_EnsurePartitionDirsMakesExactlyTheTopicsPartitions(t *testing.T) {
	m, _ := openLoggedManager(t, ManagerOptions{})
	defer m.Close()
	if err := m.EnsurePartitionDirs("orders", 3); err != nil {
		t.Fatal(err)
	}
	var dirs []string
	ents, _ := os.ReadDir(m.dir)
	for _, e := range ents {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	if want := []string{"orders-0", "orders-1", "orders-2"}; !reflect.DeepEqual(dirs, want) {
		t.Errorf("directories = %v, want %v", dirs, want)
	}
}

// Removing a topic removes its partitions, closing the open ones, and not the directory of
// a partition number past the count it was given; a partition that will not close is the
// error, and its directory still goes.
func TestB_RemoveTopicRemovesExactlyItsPartitionsAndReportsACloseThatFailed(t *testing.T) {
	m, _ := openLoggedManager(t, ManagerOptions{})
	defer m.Close()
	for _, d := range []string{"t-0", "t-1", "t-2", "u-0"} {
		if err := os.MkdirAll(filepath.Join(m.dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.RemoveTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	for d, want := range map[string]bool{"t-0": false, "t-1": false, "t-2": true, "u-0": true} {
		_, err := os.Stat(filepath.Join(m.dir, d))
		if (err == nil) != want {
			t.Errorf("%s exists = %v, want %v", d, err == nil, want)
		}
	}

	p, err := m.Partition("b", 0)
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, p, 2)
	breakPartition(t, p)
	if err := m.RemoveTopic("b", 1); err == nil {
		t.Error("RemoveTopic reported nothing for a partition that would not close")
	}
	if _, err := os.Stat(filepath.Join(m.dir, "b-0")); !os.IsNotExist(err) {
		t.Errorf("the directory of the partition that would not close is still there: %v", err)
	}
	if m.OpenPartitions() != 0 {
		t.Errorf("%d partitions still open", m.OpenPartitions())
	}
}

// A directory with a partition's name but a partition number at or past the registry's
// count is a stray; one below it, and one of a topic the registry does not know, are as
// the registry says.
func TestB_StrayDirsStartAtThePartitionCount(t *testing.T) {
	m, _ := openLoggedManager(t, ManagerOptions{})
	defer m.Close()
	for _, d := range []string{"t-0", "t-1", "t-2", "t-3", "gone-0", "x"} {
		if err := os.MkdirAll(filepath.Join(m.dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	got := m.StrayDirs(func(topic string) (int32, bool) { return 3, topic == "t" })
	if want := []string{"gone-0", "t-3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("StrayDirs = %v, want %v", got, want)
	}
}

func TestB_SplitPartitionDirNeedsATopicBeforeTheDash(t *testing.T) {
	for name, want := range map[string]bool{"-3": false, "a-3": true, "a-": false, "a-b-12": true, "a-03": false} {
		if _, _, ok := splitPartitionDir(name); ok != want {
			t.Errorf("splitPartitionDir(%q) ok = %v, want %v", name, ok, want)
		}
	}
}

// A topic's limits that say nothing of the segment size leave the manager's in force.
func TestB_TopicLimitsWithoutASegmentSizeKeepTheManagersSize(t *testing.T) {
	m, _ := openLoggedManager(t, ManagerOptions{SegmentBytes: 1 << 20})
	defer m.Close()
	m.SetTopicLimits("t", TopicLimits{Retention: RetentionPolicy{MaxBytes: 1 << 30}})
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, p, 4)
	if p.segmentBytes != 1<<20 || len(p.segments) != 1 {
		t.Errorf("segment size %d with %d segments, want the manager's 1 MiB and one segment", p.segmentBytes, len(p.segments))
	}
	m.SetTopicLimits("t", TopicLimits{SegmentBytes: 512})
	if p.segmentBytes != 512 {
		t.Errorf("segment size %d after the topic set 512", p.segmentBytes)
	}
}

// A flush that fails is the manager's error, from the first partition that failed.
func TestB_ManagerSyncReportsAFailedFlush(t *testing.T) {
	boom := errors.New("the disk failed")
	var fail bool
	m, _ := openLoggedManager(t, ManagerOptions{BeforeSync: func() error {
		if fail {
			return boom
		}
		return nil
	}})
	defer func() { fail = false; m.Close() }()
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, p, 1)
	fail = true
	if err := m.Sync(); !errors.Is(err, boom) {
		t.Errorf("Sync = %v, want the flush error", err)
	}
	fail = false
	if err := m.Sync(); err != nil {
		t.Errorf("Sync after the disk recovered = %v", err)
	}
}

// A stop that cannot close a partition says so and does not say the stop was clean; one
// that closes them all says so and names them.
func TestB_ManagerCloseReportsAPartitionThatWillNotCloseAndLeavesNoCleanMarker(t *testing.T) {
	m, _ := openLoggedManager(t, ManagerOptions{})
	for _, name := range []string{"good", "bad"} {
		p, err := m.Partition(name, 0)
		if err != nil {
			t.Fatal(err)
		}
		fillSegments(t, p, 2)
		if name == "bad" {
			breakPartition(t, p)
		}
	}
	dir := m.dir
	if err := m.Close(); err == nil {
		t.Fatal("Close reported nothing for a partition that would not close")
	}
	if _, err := os.Stat(filepath.Join(dir, cleanStopFile)); !os.IsNotExist(err) {
		t.Errorf("a stop that failed left the clean-shutdown marker: %v", err)
	}

	m, _ = openLoggedManager(t, ManagerOptions{})
	for _, name := range []string{"one", "two"} {
		p, err := m.Partition(name, 0)
		if err != nil {
			t.Fatal(err)
		}
		fillSegments(t, p, 2)
	}
	dir = m.dir
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	names, ok := readNameList(filepath.Join(dir, cleanStopFile), cleanStopHeader)
	if !ok || len(names) != 0 {
		t.Fatalf("clean-shutdown = %v, %v, want a marker that leaves no partition unvouched: both were closed", names, ok)
	}
}

// The compaction sweep says what it did and says nothing when it did nothing: a pass that
// looked and removed nothing is not reported as compacting, and unreadable batches are
// reported only when there are some.
func TestB_CompactionSweepLogsOnlyWhatItDid(t *testing.T) {
	m, out := openLoggedManager(t, ManagerOptions{SegmentBytes: 1})
	defer m.Close()
	m.SetTopicLimits("t", TopicLimits{SegmentBytes: 1, Compaction: CompactionPolicy{Enabled: true}})
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}))
	mustAppend(t, p, kvBatch(codecNone, kv{"b", "1"}))
	mustAppend(t, p, kvBatch(codecNone, kv{"c", "1"}))
	// A pass over sealed segments with nothing to remove.
	if err := m.EnforceCompaction(); err != nil {
		t.Fatal(err)
	}
	if !p.LastClean().Ran {
		t.Fatal("setup: the pass did not run")
	}
	if got := out.String(); strings.Contains(got, "compacted a partition") || strings.Contains(got, "cannot read") {
		t.Errorf("a pass that removed nothing was logged:\n%s", got)
	}
	// One that removes a record says how many.
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "2"}))
	mustAppend(t, p, kvBatch(codecNone, kv{"d", "1"}))
	if err := m.EnforceCompaction(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "compacted a partition") || !strings.Contains(got, "records_removed=1") {
		t.Errorf("a pass that removed a record was not logged:\n%s", got)
	}
	// Batches it cannot read are reported once they are there.
	junk := makeBatch(1, []byte("not a record"))
	mustAppend(t, p, junk)
	mustAppend(t, p, kvBatch(codecNone, kv{"e", "1"}))
	if err := m.EnforceCompaction(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "cannot read") || !strings.Contains(got, "batches=1") {
		t.Errorf("unreadable batches were not reported:\n%s", got)
	}
}

// The maintenance loop's start-up sweep of unopened partitions says nothing when it
// finds nothing wrong.
func TestB_MaintenanceLoopIsQuietWhenNothingIsWrong(t *testing.T) {
	m, out := openLoggedManager(t, ManagerOptions{Retention: RetentionPolicy{MaxBytes: 1 << 30}})
	if err := m.Close(); err != nil { // Close waits for the loop, whose first sweep has run by then.
		t.Fatal(err)
	}
	if got := out.String(); got != "" {
		t.Errorf("the loop logged on a healthy start:\n%s", got)
	}
}
