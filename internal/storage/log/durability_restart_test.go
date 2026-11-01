package log

// What a restart may believe about the bytes it finds (durability.go,
// cleanstop.go). Every test here counts real fsyncs at the file seam and reads
// back what a power cut would leave: a counter of calls that were meant to lead
// to an fsync is how the hole these tests close stayed open.

import (
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/powercut"
)

// watchDisk puts a power-cut model over root for the length of the test.
func watchDisk(t *testing.T, root string) *powercut.Model {
	t.Helper()
	pc := powercut.New(root)
	t.Cleanup(SetFSHookForTest(func(ev FSEvent) error {
		return pc.Observe(string(ev.Op), ev.Path, ev.To, ev.Done)
	}))
	return pc
}

// kill drops a partition as SIGKILL drops a process: its files are closed with
// nothing flushed. What it wrote stays in the page cache, so it reads back.
func kill(p *Partition) {
	for _, s := range p.segments {
		s.close()
	}
}

// afterCut opens the partition directory name as a power cut would leave it.
func afterCut(t *testing.T, pc *powercut.Model, name string, mode powercut.Mode, opts ManagerOptions) *Partition {
	t.Helper()
	cut, err := pc.Cut(t.TempDir(), mode, rand.New(rand.NewSource(7)))
	if err != nil {
		t.Fatal(err)
	}
	p, err := openPartition(filepath.Join(cut, name), opts)
	if err != nil {
		t.Fatalf("open after a power cut (%s): %v", mode, err)
	}
	t.Cleanup(func() { p.close() })
	return p
}

// F1 at the log. A process is killed with a tail it never flushed. The next
// process reads that tail back, and a flush it is asked for (the acks=all of a
// duplicate retry) must reach the disk: "nothing written since I started" is not
// "nothing to flush".
func TestAFlushAfterARestartCoversWhatThePreviousProcessLeftUnflushed(t *testing.T) {
	for _, mode := range powercut.Modes {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			pc := watchDisk(t, root)
			dir := filepath.Join(root, "t-0")
			opts := ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 64}
			p, err := openPartition(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			mustAppend(t, p, makeBatch(1, []byte("flushed")))
			if err := p.Sync(); err != nil {
				t.Fatal(err)
			}
			mustAppend(t, p, makeBatch(1, []byte("never flushed by the process that wrote it")))
			kill(p)

			p2, err := openPartition(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer p2.close()
			if p2.LogEndOffset() != 2 {
				t.Fatalf("setup: the restart reads back %d records, want 2", p2.LogEndOffset())
			}
			logs, dirs := pc.FsyncsOf(".log"), pc.Dirsyncs()
			if err := p2.Sync(); err != nil {
				t.Fatal(err)
			}
			if got := pc.FsyncsOf(".log") - logs; got != 1 {
				t.Errorf("the first flush after the restart fsynced the log %d time(s), want 1", got)
			}
			if got := pc.Dirsyncs() - dirs; got != 1 {
				t.Errorf("the first flush after the restart fsynced the directory %d time(s), want 1: its entries were made by a process that may not have flushed them", got)
			}
			// And once it has, there is nothing more to pay.
			logs = pc.FsyncsOf(".log")
			if err := p2.Sync(); err != nil {
				t.Fatal(err)
			}
			if got := pc.FsyncsOf(".log") - logs; got != 0 {
				t.Errorf("a second flush with nothing new fsynced %d time(s)", got)
			}

			after := afterCut(t, pc, "t-0", mode, opts)
			if got := after.LogEndOffset(); got != 2 {
				t.Fatalf("after a power cut the log ends at %d: the flush was reported done with a record still in the page cache", got)
			}
			checkEveryOffset(t, after, 2)
		})
	}
}

// F4. The first append after a restart rolls the segment. The roll seals it, and
// the seal is the last time anything looks at that segment: it must be flushed
// whole, including the tail the previous process left.
func TestARollAfterARestartFlushesTheSegmentItSeals(t *testing.T) {
	for _, mode := range powercut.Modes {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			pc := watchDisk(t, root)
			dir := filepath.Join(root, "t-0")
			opts := ManagerOptions{SegmentBytes: 300, IndexIntervalBytes: 64}
			p, err := openPartition(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 { // two batches fit; a third rolls.
				mustAppend(t, p, makeBatch(1, []byte(strings.Repeat("x", 40))))
			}
			if len(p.segments) != 1 {
				t.Fatalf("setup: %d segments", len(p.segments))
			}
			kill(p)

			p2, err := openPartition(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer p2.close()
			logs := pc.FsyncsOf(".log")
			mustAppend(t, p2, makeBatch(1, []byte(strings.Repeat("y", 40)))) // rolls.
			if len(p2.segments) != 2 {
				t.Fatalf("setup: the append did not roll (%d segments)", len(p2.segments))
			}
			if got := pc.FsyncsOf(".log") - logs; got != 1 {
				t.Fatalf("the roll fsynced %d log file(s), want 1: the segment it sealed", got)
			}
			if err := p2.Sync(); err != nil { // what acks=all does for the new record.
				t.Fatal(err)
			}

			after := afterCut(t, pc, "t-0", mode, opts)
			if got := after.LogEndOffset(); got != 3 {
				t.Fatalf("after a power cut the log ends at %d, want 3", got)
			}
			checkEveryOffset(t, after, 3)
		})
	}
}

// A record acknowledged in a segment that a roll has just created is only as
// durable as that segment's name: fsyncing a file whose directory entry was never
// flushed keeps the bytes and loses the file.
func TestARecordInANewSegmentSurvivesAPowerCut(t *testing.T) {
	root := t.TempDir()
	pc := watchDisk(t, root)
	dir := filepath.Join(root, "t-0")
	opts := ManagerOptions{SegmentBytes: 300, IndexIntervalBytes: 64}
	p, err := openPartition(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	for i := range 5 { // offsets 0-1, 2-3, 4: a roll before the third and the fifth.
		mustAppend(t, p, makeBatch(1, []byte(strings.Repeat("x", 40))))
		if err := p.Sync(); err != nil {
			t.Fatal(err)
		}
		after := afterCut(t, pc, "t-0", powercut.Truncate, opts)
		if got := after.LogEndOffset(); got != int64(i+1) {
			t.Fatalf("after record %d was flushed and the power went, the log ends at %d (%d segments on disk)", i, got, len(after.segments))
		}
	}
	if len(p.segments) != 3 {
		t.Fatalf("setup: %d segments, want 3", len(p.segments))
	}
}

// killManager stops a manager as SIGKILL would: nothing flushed, no marker.
func killManager(m *Manager) { m.AbandonForTest() }

func openTestManager(t *testing.T, dir string, opts ManagerOptions) *Manager {
	t.Helper()
	m, err := OpenManager(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

// A stop that flushed everything says so, and the next start need not flush it
// again; a stop that did not, says nothing, and the next start trusts nothing.
func TestOnlyACleanStopIsVouchedFor(t *testing.T) {
	root := t.TempDir()
	pc := watchDisk(t, root)
	opts := testOpts()

	m := openTestManager(t, root, opts)
	for _, part := range []int32{0, 1} {
		p, err := m.Partition("t", part)
		if err != nil {
			t.Fatal(err)
		}
		mustAppend(t, p, makeBatch(1, []byte("x")))
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, cleanStopFile)); err != nil {
		t.Fatalf("a clean stop left no marker: %v", err)
	}

	// Run 2: clean start. The marker is gone before anything is written, and a
	// flush of what the last run flushed costs nothing.
	m = openTestManager(t, root, opts)
	if _, err := os.Stat(filepath.Join(root, cleanStopFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the marker of the last stop is still there once the manager is open (%v): a kill from here would be taken for a clean stop", err)
	}
	p0, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	before := pc.Fsyncs() + pc.Dirsyncs()
	if err := p0.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := pc.Fsyncs() + pc.Dirsyncs() - before; got != 0 {
		t.Errorf("after a clean stop, flushing an untouched partition cost %d fsync(s), want 0", got)
	}
	mustAppend(t, p0, makeBatch(1, []byte("y")))
	killManager(m)

	// Run 3: the last stop was a kill. Partition 0 was written by run 2; partition
	// 1 was not even opened by it, and is trusted no more for that: nobody can say
	// run 2 did not touch it.
	m = openTestManager(t, root, opts)
	for _, part := range []int32{0, 1} {
		p, err := m.Partition("t", part)
		if err != nil {
			t.Fatal(err)
		}
		before := pc.FsyncsOf(".log")
		if err := p.Sync(); err != nil {
			t.Fatal(err)
		}
		if got := pc.FsyncsOf(".log") - before; got != 1 {
			t.Errorf("after a kill, the first flush of partition %d fsynced its log %d time(s), want 1", part, got)
		}
	}
}

// Partitions open lazily. A run that follows a kill and never opens a partition
// has not flushed it, so its own clean stop cannot vouch for it: the next run
// must still treat it as the kill left it.
func TestACleanStopDoesNotVouchForWhatItNeverOpened(t *testing.T) {
	root := t.TempDir()
	pc := watchDisk(t, root)
	opts := testOpts()

	m := openTestManager(t, root, opts)
	for _, part := range []int32{0, 1} {
		p, err := m.Partition("t", part)
		if err != nil {
			t.Fatal(err)
		}
		mustAppend(t, p, makeBatch(1, []byte("in the page cache")))
	}
	killManager(m)

	// Run 2 opens partition 0 only, and stops cleanly.
	m = openTestManager(t, root, opts)
	if _, err := m.Partition("t", 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// Run 3 starts from a clean stop. Partition 0 is vouched for; partition 1 is not.
	m = openTestManager(t, root, opts)
	want := map[int32]int{0: 0, 1: 1}
	for _, part := range []int32{0, 1} {
		p, err := m.Partition("t", part)
		if err != nil {
			t.Fatal(err)
		}
		before := pc.FsyncsOf(".log")
		if err := p.Sync(); err != nil {
			t.Fatal(err)
		}
		if got := pc.FsyncsOf(".log") - before; got != want[part] {
			t.Errorf("partition %d: the first flush fsynced its log %d time(s), want %d", part, got, want[part])
		}
	}
	// And the record run 1 left in partition 1 is on disk now.
	cut, err := pc.Cut(t.TempDir(), powercut.Truncate, nil)
	if err != nil {
		t.Fatal(err)
	}
	m2 := openTestManager(t, cut, opts)
	if _, end, err := m2.Bounds("t", 1); err != nil || end != 1 {
		t.Fatalf("after a power cut partition 1 ends at %d (%v), want 1", end, err)
	}
}

// A stop whose flush failed is not a clean stop.
func TestAStopThatCouldNotFlushLeavesNoMarker(t *testing.T) {
	root := t.TempDir()
	fail := false
	opts := testOpts()
	opts.BeforeSync = func() error {
		if fail {
			return errDisk
		}
		return nil
	}
	m := openTestManager(t, root, opts)
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, p, makeBatch(1, []byte("x")))
	fail = true
	if err := m.Close(); !errors.Is(err, errDisk) {
		t.Fatalf("Close = %v, want the failed flush", err)
	}
	if _, err := os.Stat(filepath.Join(root, cleanStopFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a stop whose flush failed left the clean-stop marker (%v)", err)
	}
}

// oldDataDir makes root look like a data directory written before sealed
// segments were guaranteed flushed: no list of unvalidated partitions, no marker.
func oldDataDir(t *testing.T, root string) {
	t.Helper()
	for _, f := range []string{unvalidatedFile, cleanStopFile} {
		if err := os.Remove(filepath.Join(root, f)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
}

// F4. A sealed segment damaged by an earlier version's roll (its header page
// reached the disk, its body did not) used to be served to consumers as it was,
// for good: recovery only ever looked at the active segment. In a partition not
// known to have been sealed safely every segment is checked; the damaged one is
// cut at its first bad batch, and what follows it is kept.
func TestADamagedSealedSegmentIsFoundAndCutAndTheRestIsKept(t *testing.T) {
	root := t.TempDir()
	opts := ManagerOptions{SegmentBytes: 300, IndexIntervalBytes: 64}
	m := openTestManager(t, root, opts)
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 { // two per segment: offsets 0-1, 2-3, 4.
		mustAppend(t, p, makeBatch(1, []byte(strings.Repeat("x", 40))))
	}
	if len(p.segments) != 3 {
		t.Fatalf("setup: %d segments", len(p.segments))
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	// The damage: in the first segment, the first batch whole and the second with
	// its header and no body.
	first := filepath.Join(root, "t-0", segmentName(0)+".log")
	mutateFile(t, first, func(b []byte) []byte {
		one := len(b) / 2
		for i := one + batchFullHeaderBytes; i < len(b); i++ {
			b[i] = 0
		}
		return b
	})
	oldDataDir(t, root)

	m = openTestManager(t, root, opts)
	// Its bounds, read without opening it, are what opening it will give.
	bs, be, err := m.Bounds("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	p, err = m.Partition("t", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if p.LogStartOffset() != bs || p.LogEndOffset() != be || be != 5 {
		t.Fatalf("bounds [%d,%d) before opening, [%d,%d) after; want an end of 5", bs, be, p.LogStartOffset(), p.LogEndOffset())
	}
	// Every read returns whole, valid batches; offset 1 is gone and a reader
	// asking for it is carried on to the next record there is.
	var got []int64
	for next := int64(0); next < p.LogEndOffset(); {
		data, err := p.Read(next, 1<<20)
		if err != nil || len(data) == 0 {
			t.Fatalf("Read(%d) = %d bytes, %v", next, len(data), err)
		}
		if err := ValidateBatches(data); err != nil {
			t.Fatalf("Read(%d) served a batch that fails validation: %v", next, err)
		}
		for pos := 0; pos < len(data); {
			info, _ := parseHeader(data[pos:])
			got = append(got, info.baseOffset)
			next = info.baseOffset + int64(info.recordCount)
			pos += int(info.totalBytes)
		}
	}
	if want := []int64{0, 2, 3, 4}; !equalOffsets(got, want) {
		t.Fatalf("offsets readable after recovery: %v, want %v (the damaged batch cut, everything after it kept)", got, want)
	}

	// Checked once: after it has been flushed the partition leaves the list, and
	// the next start does not read it all again.
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if names, ok := readNameList(filepath.Join(root, unvalidatedFile), unvalidatedHeader); !ok || len(names) != 0 {
		t.Fatalf("after a checked partition was flushed the unvalidated list is %v (ok=%v), want it empty", names, ok)
	}
}

// A partition from before sealed segments were flushed by their rolls is not
// believed anywhere: its first flush covers every segment, sealed ones too, and
// only then does it leave the list.
func TestAnUnvalidatedPartitionIsFlushedWholeBeforeItIsBelieved(t *testing.T) {
	root := t.TempDir()
	opts := ManagerOptions{SegmentBytes: 300, IndexIntervalBytes: 64}
	m := openTestManager(t, root, opts)
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		mustAppend(t, p, makeBatch(1, []byte(strings.Repeat("x", 40))))
	}
	killManager(m)
	oldDataDir(t, root)
	// From here the model watches: it has seen none of these files flushed, which
	// is what this process knows about them too.
	pc := watchDisk(t, root)

	m = openTestManager(t, root, opts)
	p, err = m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.saveUnvalidated(); err != nil {
		t.Fatal(err)
	}
	if names, _ := readNameList(filepath.Join(root, unvalidatedFile), unvalidatedHeader); len(names) != 1 {
		t.Fatalf("a partition that has been checked and not yet flushed is off the list: %v", names)
	}
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := pc.FsyncsOf(".log"); got != 3 {
		t.Fatalf("the first flush fsynced %d log file(s), want all 3 segments", got)
	}
	after := afterCut(t, pc, "t-0", powercut.Truncate, opts)
	if got := after.LogEndOffset(); got != 5 {
		t.Fatalf("after a power cut the log ends at %d, want 5", got)
	}
	checkEveryOffset(t, after, 5)
	if err := m.saveUnvalidated(); err != nil {
		t.Fatal(err)
	}
	if names, ok := readNameList(filepath.Join(root, unvalidatedFile), unvalidatedHeader); !ok || len(names) != 0 {
		t.Fatalf("flushed whole, and still on the list: %v", names)
	}
}

func equalOffsets(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A partition made by this version is never on the list: its sealed segments
// were flushed by the rolls that sealed them, and a restart after a kill looks
// at the active segment only.
func TestANewDataDirectoryHasNothingToValidate(t *testing.T) {
	root := t.TempDir()
	opts := ManagerOptions{SegmentBytes: 300, IndexIntervalBytes: 64}
	m := openTestManager(t, root, opts)
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		mustAppend(t, p, makeBatch(1, []byte(strings.Repeat("x", 40))))
	}
	killManager(m)
	if names, ok := readNameList(filepath.Join(root, unvalidatedFile), unvalidatedHeader); !ok || len(names) != 0 {
		t.Fatalf("unvalidated list of a new data directory: %v (ok=%v), want it there and empty", names, ok)
	}
	pc := watchDisk(t, root)
	m = openTestManager(t, root, opts)
	p, err = m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := pc.FsyncsOf(".log"); got != 1 {
		t.Errorf("after a kill, a partition of 3 segments fsynced %d log file(s) on its first flush, want 1 (the active one; the sealed ones were flushed when they were sealed)", got)
	}
}

// After a kill the whole active segment is verified, not a window at its end:
// what was never flushed is not bounded by a window, and a page of it can be
// missing anywhere.
func TestRecoveryAfterAKillVerifiesTheWholeActiveSegment(t *testing.T) {
	root := t.TempDir()
	opts := ManagerOptions{SegmentBytes: 64 << 20, IndexIntervalBytes: 4096}
	m := openTestManager(t, root, opts)
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	big := []byte(strings.Repeat("r", 1<<20))
	for range 10 { // 10 MiB: the first batches are outside the 8 MiB window.
		mustAppend(t, p, makeBatch(1, big))
	}
	killManager(m)
	// The second batch's body never reached the disk; everything after it did.
	path := filepath.Join(root, "t-0", segmentName(0)+".log")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	one := int64(batchFullHeaderBytes + len(big))
	if _, err := f.WriteAt(make([]byte, 4096), one+batchFullHeaderBytes+100); err != nil {
		t.Fatal(err)
	}
	f.Close()

	m = openTestManager(t, root, opts)
	_, end, err := m.Bounds("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	p, err = m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	if p.LogEndOffset() != 1 || end != 1 {
		t.Fatalf("the log ends at %d (bounds said %d) with a corrupt batch at offset 1, 9 MiB before the end; want 1", p.LogEndOffset(), end)
	}
}
