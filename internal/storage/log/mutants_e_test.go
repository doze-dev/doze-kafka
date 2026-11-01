package log

import (
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// eventLog records what passes the file seam.
type eventLog struct {
	mu sync.Mutex
	ev []FSEvent
}

func (l *eventLog) add(e FSEvent) {
	l.mu.Lock()
	l.ev = append(l.ev, e)
	l.mu.Unlock()
}

func (l *eventLog) snapshot() []FSEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]FSEvent(nil), l.ev...)
}

func (l *eventLog) reset() {
	l.mu.Lock()
	l.ev = nil
	l.mu.Unlock()
}

// count is how many events are of op, at a path ending in suffix, and Done (or not).
func (l *eventLog) count(op FSOp, suffix string, done bool) int {
	n := 0
	for _, e := range l.snapshot() {
		if e.Op == op && e.Done == done && strings.HasSuffix(e.Path, suffix) {
			n++
		}
	}
	return n
}

func recordFS(t *testing.T) *eventLog {
	t.Helper()
	l := &eventLog{}
	t.Cleanup(SetFSHookForTest(func(e FSEvent) error { l.add(e); return nil }))
	return l
}

// Every operation at the file seam that succeeds is reported as done, so that a
// model of the disk (internal/powercut) sees it; one that did nothing is not.
func TestE_TheSeamReportsWhatItDid(t *testing.T) {
	dir := t.TempDir()
	l := recordFS(t)

	f, _, err := fsOpenFile(filepath.Join(dir, "f"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := (fsWriter{f}).Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got := l.count(FSWrite, "/f", true); got != 1 {
		t.Errorf("a completed write was reported done %d times, want 1", got)
	}
	if err := fsTruncate(f, 2); err != nil {
		t.Fatal(err)
	}
	if got := l.count(FSTruncate, "/f", true); got != 1 {
		t.Errorf("a completed truncate was reported done %d times, want 1", got)
	}
	if st, _ := f.Stat(); st.Size() != 2 {
		t.Errorf("size after truncate %d", st.Size())
	}

	if err := fsRemove(filepath.Join(dir, "f")); err != nil {
		t.Fatal(err)
	}
	if got := l.count(FSRemove, "/f", true); got != 1 {
		t.Errorf("a completed remove was reported done %d times, want 1", got)
	}
	l.reset()
	if err := fsRemove(filepath.Join(dir, "absent")); err != nil {
		t.Fatalf("removing what is not there: %v", err)
	}
	if got := l.count(FSRemove, "/absent", true); got != 0 {
		t.Errorf("a remove of a missing file was reported done")
	}
}

// A directory that appears between the check and the make is not made by this
// call and is not an error; the seam is told of no mkdir that did not happen.
func TestE_MkdirBeatenToItIsNotAnError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	l := &eventLog{}
	t.Cleanup(SetFSHookForTest(func(e FSEvent) error {
		l.add(e)
		if e.Op == FSMkdir && !e.Done {
			return os.Mkdir(dir, 0o755) // someone else got there first.
		}
		return nil
	}))
	created, err := fsMkdir(dir)
	if err != nil || created {
		t.Fatalf("fsMkdir = %v, %v, want not created and no error", created, err)
	}
	if l.count(FSMkdir, "/d", true) != 0 {
		t.Error("a mkdir this call did not make was reported done")
	}
	// A file where the directory should be is an error.
	file := filepath.Join(t.TempDir(), "x")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := fsMkdir(file); err == nil {
		t.Error("a file in the place of a directory was taken for the directory")
	}
}

func TestE_SyncDirectoryOfAMissingDirectoryFails(t *testing.T) {
	if err := syncDirectory(t.TempDir()); err != nil {
		t.Errorf("existing: %v", err)
	}
	if err := syncDirectory(filepath.Join(t.TempDir(), "absent")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("absent: %v, want not-exist", err)
	}
}

func eOpen(t *testing.T, opts ManagerOptions) *Partition {
	t.Helper()
	p, err := openPartition(filepath.Join(t.TempDir(), "t-0"), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.close() })
	return p
}

// A Sync with nothing new to flush returns at once, without waiting on a
// flush that is in progress.
func TestE_SyncWithNothingNewDoesNotWaitForTheLock(t *testing.T) {
	p := eOpen(t, testOpts())
	mustAppend(t, p, makeBatch(1, []byte("x")))
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	p.syncMu.Lock()
	done := make(chan error, 1)
	go func() { done <- p.Sync() }()
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(3 * time.Second):
		t.Error("a Sync with nothing written since the last one waited for the flush lock")
		p.syncMu.Unlock()
		<-done
		return
	}
	p.syncMu.Unlock()
}

// A Sync that waited for the lock while another's flush covered its write does
// not flush again.
func TestE_SyncCoveredWhileWaitingFlushesNothing(t *testing.T) {
	p := eOpen(t, testOpts())
	l := recordFS(t)
	mustAppend(t, p, makeBatch(1, []byte("x")))
	p.syncMu.Lock()
	done := make(chan error, 1)
	go func() { done <- p.Sync() }()
	time.Sleep(200 * time.Millisecond) // it is past the first check and waits.
	p.syncedSeq.Store(p.appendSeq.Load())
	l.reset()
	p.syncMu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := l.count(FSFsync, "", false); n != 0 {
		t.Errorf("%d fsyncs by a Sync whose write another flush had covered", n)
	}
}

// The directory is flushed when its entries changed and not when they did not.
func TestE_TheDirectoryIsFlushedOnlyWhenItsEntriesChanged(t *testing.T) {
	p := eOpen(t, testOpts())
	l := recordFS(t)
	mustAppend(t, p, makeBatch(1, []byte("x")))
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	if l.count(FSDirSync, "t-0", false) != 1 {
		t.Fatalf("the first Sync flushed the directory %d times, want 1 (the segment's name)", l.count(FSDirSync, "t-0", false))
	}
	l.reset()
	mustAppend(t, p, makeBatch(1, []byte("y")))
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	if n := l.count(FSDirSync, "t-0", false); n != 0 {
		t.Errorf("a Sync after an append that changed no entry flushed the directory %d times", n)
	}
	if l.count(FSFsync, ".log", false) == 0 {
		t.Error("the append was not flushed")
	}
}

func TestE_SyncDirLockedFlushesOnlyAChangedDirectory(t *testing.T) {
	p := eOpen(t, testOpts())
	l := recordFS(t)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.syncDirLocked(); err != nil {
		t.Fatal(err)
	}
	n1 := l.count(FSDirSync, "t-0", false)
	if err := p.syncDirLocked(); err != nil {
		t.Fatal(err)
	}
	if n1 != 1 || l.count(FSDirSync, "t-0", false) != 1 {
		t.Errorf("directory flushes: %d after the first call, %d after the second; want 1 and 1", n1, l.count(FSDirSync, "t-0", false))
	}
}

// Removing the oldest segment changes the directory: the next Sync flushes it.
func TestE_DroppingTheOldestSegmentIsFlushedBySync(t *testing.T) {
	opts := testOpts()
	opts.SegmentBytes = 100
	p := eOpen(t, opts)
	for range 4 {
		mustAppend(t, p, makeBatch(1, []byte(strings.Repeat("x", 50))))
	}
	if len(p.segments) < 3 {
		t.Fatalf("setup: %d segments", len(p.segments))
	}
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	err := p.dropOldestLocked()
	p.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	l := recordFS(t)
	mustAppend(t, p, makeBatch(1, []byte("z")))
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	if l.count(FSDirSync, "t-0", false) != 1 {
		t.Errorf("the removal of a segment was not flushed to the directory (%d dirsyncs)", l.count(FSDirSync, "t-0", false))
	}
}

// failNthLogWrite makes the nth write to a .log file fail (from 1).
func failNthLogWrite(t *testing.T, n int) *eventLog {
	t.Helper()
	l := &eventLog{}
	seen := 0
	t.Cleanup(SetFSHookForTest(func(e FSEvent) error {
		l.add(e)
		if e.Op == FSWrite && !e.Done && strings.HasSuffix(e.Path, ".log") {
			seen++
			if seen == n {
				return errors.New("injected write failure")
			}
		}
		return nil
	}))
	return l
}

// A first batch that fails to write leaves no trace: the flush counter does not
// move, so no flush is made for a write that never happened.
func TestE_AFailedFirstBatchLeavesNoWriteToFlush(t *testing.T) {
	p := eOpen(t, testOpts())
	mustAppend(t, p, makeBatch(1, []byte("a")))
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	before := p.appendSeq.Load()
	l := failNthLogWrite(t, 1)
	if _, err := p.Append(makeBatch(1, []byte("b"))); err == nil {
		t.Fatal("the failing write was not reported")
	}
	if got := p.appendSeq.Load(); got != before {
		t.Errorf("appendSeq moved from %d to %d for a write that failed before anything was placed", before, got)
	}
	if n := l.count(FSTruncate, "", true); n > 1 {
		t.Errorf("%d truncates for one failed write", n)
	}
	if p.LogEndOffset() != 1 {
		t.Errorf("log end %d", p.LogEndOffset())
	}
}

func indexSize(t *testing.T, p *Partition) int64 {
	t.Helper()
	st, err := os.Stat(filepath.Join(p.dir, segmentName(0)+".index"))
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

// A multi-batch append that fails part-way is taken back whole: the log, and the
// index entries it added, on disk and in memory.
func TestE_AFailedAppendTakesBackItsIndexEntries(t *testing.T) {
	opts := testOpts()
	opts.IndexIntervalBytes = 1 // every batch gets an index entry.
	p := eOpen(t, opts)
	mustAppend(t, p, makeBatch(1, []byte("first")))
	if got := indexSize(t, p); got != indexEntryBytes {
		t.Fatalf("setup: index is %d bytes", got)
	}
	logSize := p.active().sizeBytes
	failNthLogWrite(t, 2) // the first batch of the pair goes in, the second fails.
	pair := append(makeBatch(1, []byte("second")), makeBatch(1, []byte("third"))...)
	if _, err := p.Append(pair); err == nil {
		t.Fatal("the failing write was not reported")
	}
	if n := len(p.active().index.entries); n != 1 {
		t.Errorf("%d index entries in memory after the failed append, want 1", n)
	}
	if got := indexSize(t, p); got != indexEntryBytes {
		t.Errorf("index file is %d bytes after the failed append, want %d", got, indexEntryBytes)
	}
	if p.active().sizeBytes != logSize || p.LogEndOffset() != 1 {
		t.Errorf("size %d (want %d), log end %d (want 1)", p.active().sizeBytes, logSize, p.LogEndOffset())
	}
}

// Rewinding a segment that gained no index entry does not touch its index file.
func TestE_AFailedAppendThatAddedNoIndexEntryLeavesTheIndexFile(t *testing.T) {
	p := eOpen(t, testOpts()) // a 4096-byte interval: one entry, for the first batch.
	mustAppend(t, p, makeBatch(1, []byte("first")))
	l := failNthLogWrite(t, 2)
	pair := append(makeBatch(1, []byte("second")), makeBatch(1, []byte("third"))...)
	if _, err := p.Append(pair); err == nil {
		t.Fatal("the failing write was not reported")
	}
	if n := l.count(FSTruncate, ".index", false); n != 0 {
		t.Errorf("the index file was cut %d times though no entry was added", n)
	}
}

func captureLog() (*slog.Logger, *bytes.Buffer) {
	var b bytes.Buffer
	return slog.New(slog.NewTextHandler(&b, nil)), &b
}

// An unvalidated partition's sealed segments are checked; one that is whole says
// nothing, and one that was cut says so.
func TestE_ADamagedSealedSegmentIsReportedAndAWholeOneIsNot(t *testing.T) {
	opts := testOpts()
	opts.SegmentBytes = 100
	dir := filepath.Join(t.TempDir(), "t-0")
	p, err := openPartition(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		mustAppend(t, p, makeBatch(1, []byte(strings.Repeat("x", 50))))
	}
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	opts.unvalidated = true
	lg, out := captureLog()
	opts.Logger = lg
	q, err := openPartition(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	q.close()
	if strings.Contains(out.String(), "damaged") {
		t.Errorf("a whole log was reported damaged: %s", out)
	}
	// Tear the tail of the first (sealed) segment.
	first := filepath.Join(dir, segmentName(0)+".log")
	st, _ := os.Stat(first)
	if err := os.Truncate(first, st.Size()-5); err != nil {
		t.Fatal(err)
	}
	lg, out = captureLog()
	opts.Logger = lg
	q, err = openPartition(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	q.close()
	if !strings.Contains(out.String(), "damaged") {
		t.Errorf("a cut segment was not reported: %q", out)
	}
}

// A log start recorded at the base of the second segment (a crash before the
// first was removed) is finished on open: the first segment goes, files and all,
// and the directory change is flushed by the next Sync.
func TestE_AnInterruptedStartMoveIsFinishedOnOpen(t *testing.T) {
	opts := testOpts()
	opts.SegmentBytes = 100
	dir := filepath.Join(t.TempDir(), "t-0")
	p, err := openPartition(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		mustAppend(t, p, makeBatch(1, []byte(strings.Repeat("x", 50))))
	}
	base1 := p.segments[1].baseOffset
	nseg := len(p.segments)
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	if err := writeLogStart(dir, base1); err != nil {
		t.Fatal(err)
	}
	opts.vouched = true // a trusted open: nothing else touches the directory counter.
	q, err := openPartition(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	if len(q.segments) != nseg-1 || q.segments[0].baseOffset != base1 || q.LogStartOffset() != base1 {
		t.Fatalf("segments %d (want %d), first base %d (want %d), start %d", len(q.segments), nseg-1, q.segments[0].baseOffset, base1, q.LogStartOffset())
	}
	for _, ext := range []string{".log", ".index"} {
		if _, err := os.Stat(filepath.Join(dir, segmentName(0)+ext)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("segment 0%s is still on disk: %v", ext, err)
		}
	}
	// The removal is a directory change the next flush covers; and a later roll
	// is one too.
	l := recordFS(t)
	mustAppend(t, q, makeBatch(1, []byte("z")))
	if err := q.Sync(); err != nil {
		t.Fatal(err)
	}
	if l.count(FSDirSync, "t-0", false) != 1 {
		t.Errorf("the removal was not flushed to the directory (%d dirsyncs)", l.count(FSDirSync, "t-0", false))
	}
	for range 4 {
		mustAppend(t, q, makeBatch(1, []byte(strings.Repeat("x", 50))))
	}
	l.reset()
	if err := q.Sync(); err != nil {
		t.Fatal(err)
	}
	if l.count(FSDirSync, "t-0", false) != 1 {
		t.Errorf("the segments rolled since were not flushed to the directory (%d dirsyncs)", l.count(FSDirSync, "t-0", false))
	}
}

// Creating a topic's partition directories makes exactly the partitions asked for.
func TestE_EnsurePartitionDirsMakesExactlyTheRequested(t *testing.T) {
	root := t.TempDir()
	n := 0
	t.Cleanup(SetFSHookForTest(func(e FSEvent) error {
		if e.Op == FSMkdir && !e.Done {
			n++
			if n > 20 {
				return errors.New("runaway mkdir")
			}
		}
		return nil
	}))
	m := openTestManager(t, root, testOpts())
	if err := m.EnsurePartitionDirs("t", 3); err != nil {
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(root)
	var got []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "t-") {
			got = append(got, e.Name())
		}
	}
	if strings.Join(got, ",") != "t-0,t-1,t-2" {
		t.Errorf("directories %v, want t-0 t-1 t-2", got)
	}
}

// RemoveTopic removes the partitions it is told of and returns.
func TestE_RemoveTopicRemovesExactlyItsPartitions(t *testing.T) {
	root := t.TempDir()
	m, err := OpenManager(root, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.EnsurePartitionDirs("t", 3); err != nil {
		t.Fatal(err)
	}
	if err := m.EnsurePartitionDirs("keep", 1); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- m.RemoveTopic("t", 3) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RemoveTopic did not return") // m is left locked: it is not closed.
	}
	for _, name := range []string{"t-0", "t-1", "t-2"} {
		if _, err := os.Stat(filepath.Join(root, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s is still there: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "keep-0")); err != nil {
		t.Errorf("another topic's directory: %v", err)
	}
	m.Close()
}

// Removing stray directories flushes the data directory only if it removed any.
func TestE_RemoveStrayTopicDirsFlushesOnlyAfterARemoval(t *testing.T) {
	root := t.TempDir()
	m := openTestManager(t, root, testOpts())
	if err := m.EnsurePartitionDirs("a", 2); err != nil {
		t.Fatal(err)
	}
	l := recordFS(t)
	if n, err := m.RemoveStrayTopicDirs("nothing"); err != nil || n != 0 {
		t.Fatalf("%d, %v", n, err)
	}
	if got := l.count(FSDirSync, filepath.Base(root), false); got != 0 {
		t.Errorf("the data directory was flushed %d times though nothing was removed", got)
	}
	if n, err := m.RemoveStrayTopicDirs("a"); err != nil || n != 2 {
		t.Fatalf("%d, %v", n, err)
	}
	if got := l.count(FSDirSync, filepath.Base(root), false); got != 1 {
		t.Errorf("the data directory was flushed %d times after a removal, want 1", got)
	}
}

// The clean-stop marker is gone, and its removal flushed, before the manager is
// open for writing.
func TestE_TheMarkersRemovalIsFlushedOnOpen(t *testing.T) {
	root := t.TempDir()
	m, err := OpenManager(root, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, p, makeBatch(1, []byte("x")))
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, cleanStopFile)); err != nil {
		t.Fatal(err)
	}
	l := recordFS(t)
	m2, err := OpenManager(root, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	ev := l.snapshot()
	for i, e := range ev {
		if e.Op == FSRemove && e.Done && strings.HasSuffix(e.Path, cleanStopFile) {
			if i+1 >= len(ev) || ev[i+1].Op != FSDirSync || ev[i+1].Path != root {
				t.Fatalf("after the marker's removal came %+v, want a flush of the data directory", ev[min(i+1, len(ev)-1)])
			}
			return
		}
	}
	t.Fatal("the marker was not removed through the seam")
}

// A clean stop lists the partitions it cannot vouch for, and leaves out a
// directory that holds no segment.
func TestE_ACleanStopDoesNotListAnEmptyDirectory(t *testing.T) {
	root := t.TempDir()
	m, err := OpenManager(root, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.Partition("u", 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, p, makeBatch(1, []byte("x")))
	killManager(m) // no clean stop: nothing vouches for u-0 afterwards.
	m2, err := OpenManager(root, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.EnsurePartitionDirs("t", 1); err != nil { // an empty directory.
		t.Fatal(err)
	}
	if err := m2.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, cleanStopFile))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	got := map[string]bool{}
	for _, l := range lines[1:] {
		got[l] = true
	}
	if !got["u-0"] || got["t-0"] || len(got) != 1 {
		t.Errorf("the clean stop lists %v, want only u-0", lines[1:])
	}
}
