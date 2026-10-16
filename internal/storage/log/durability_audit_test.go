package log

// Tests added by the October audit. Each pins a property that an earlier test
// claimed to cover but did not: every one was run against the code with the
// property removed and failed.

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// hookFsync replaces fsyncFile for the test. fn sees every fsync.
func hookFsync(t *testing.T, fn func(f *os.File) error) {
	t.Helper()
	old := fsyncFile
	fsyncFile = fn
	t.Cleanup(func() { fsyncFile = old })
}

// fsyncCounter counts fsyncs by file suffix (".log", ".index").
type fsyncCounter struct{ log, index atomic.Int64 }

func (c *fsyncCounter) hook(t *testing.T) {
	hookFsync(t, func(f *os.File) error {
		switch {
		case strings.HasSuffix(f.Name(), ".log"):
			c.log.Add(1)
		case strings.HasSuffix(f.Name(), ".index"):
			c.index.Add(1)
		}
		return f.Sync()
	})
}

var errDisk = errors.New("input/output error")

// ---- Bounds must report what opening the partition would report ----

func TestBoundsAgreesWithRecoveryAfterEveryKindOfDamage(t *testing.T) {
	const batches, perBatch = 20, 3
	build := func(t *testing.T) (dir, logPath string) {
		dir = t.TempDir()
		m, err := OpenManager(dir, ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 64})
		if err != nil {
			t.Fatal(err)
		}
		p, err := m.Partition("t", 0)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < batches; i++ {
			if _, err := p.Append(makeBatch(perBatch, []byte("payload-payload"))); err != nil {
				t.Fatal(err)
			}
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		return dir, filepath.Join(dir, "t-0", segmentName(0)+".log")
	}
	one := int64(len(makeBatch(perBatch, []byte("payload-payload"))))

	for _, tc := range []struct {
		name   string
		damage func(f *os.File, size int64)
		want   int64
	}{
		{"untouched", func(*os.File, int64) {}, 60},
		{"last byte flipped (CRC)", func(f *os.File, size int64) {
			var b [1]byte
			f.ReadAt(b[:], size-1)
			b[0] ^= 0xff
			f.WriteAt(b[:], size-1)
		}, 57},
		{"tail body zeroed (never reached the disk)", func(f *os.File, size int64) {
			f.WriteAt(make([]byte, 10), size-10)
		}, 57},
		{"tail batch has magic 1", func(f *os.File, size int64) {
			f.WriteAt([]byte{1}, size-one+batchMagicOff)
		}, 57},
		{"a batch in the middle is zeroed", func(f *os.File, size int64) {
			f.WriteAt(make([]byte, one), 10*one)
		}, 30},
		{"truncated mid-batch", func(f *os.File, size int64) { f.Truncate(size - 9) }, 57},
		{"trailing bytes with a plausible header", func(f *os.File, size int64) {
			junk := make([]byte, 70) // a whole header that parses, with no valid checksum.
			binary.BigEndian.PutUint64(junk[0:], 60)
			binary.BigEndian.PutUint32(junk[batchLengthOff:], uint32(len(junk)-batchOverhead))
			junk[batchMagicOff] = batchMagicV2
			binary.BigEndian.PutUint32(junk[batchLastOffsetDeltaOff:], 4)
			binary.BigEndian.PutUint32(junk[batchRecordCountOff:], 5)
			f.WriteAt(junk, size)
		}, 60},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, logPath := build(t)
			f, err := os.OpenFile(logPath, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			info, _ := f.Stat()
			tc.damage(f, info.Size())
			f.Close()

			m, err := OpenManager(dir, ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 64})
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			_, closedEnd, err := m.Bounds("t", 0)
			if err != nil {
				t.Fatal(err)
			}
			p, err := m.Partition("t", 0)
			if err != nil {
				t.Fatal(err)
			}
			if opened := p.LogEndOffset(); closedEnd != opened || opened != tc.want {
				t.Errorf("Bounds (unopened) = %d, opened = %d, want both %d", closedEnd, opened, tc.want)
			}
		})
	}
}

// Recovery makes its truncation durable: without the fsync a second crash brings
// the torn bytes back.
func TestRecoveryFsyncsItsTruncation(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	p.Append(makeBatch(1, []byte("a")))
	p.Append(makeBatch(1, []byte("b")))
	p.close()
	logPath := filepath.Join(dir, segmentName(0)+".log")
	info, _ := os.Stat(logPath)
	os.Truncate(logPath, info.Size()-3)

	var c fsyncCounter
	c.hook(t)
	p, err = openPartition(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if p.LogEndOffset() != 1 {
		t.Fatalf("log end after a torn tail = %d, want 1", p.LogEndOffset())
	}
	if c.log.Load() == 0 {
		t.Error("recovery truncated the log without fsyncing it")
	}
}

// ---- group commit ----

// Sync may only record as durable what was written before its fsync began. A
// write that lands during the fsync is not covered by it.
func TestSyncDoesNotClaimWritesThatArrivedDuringTheFsync(t *testing.T) {
	p, err := openPartition(t.TempDir(), testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.Append(makeBatch(1, []byte("before"))); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	hookFsync(t, func(f *os.File) error {
		if strings.HasSuffix(f.Name(), ".log") {
			once.Do(func() { p.Append(makeBatch(1, []byte("during"))) })
		}
		return f.Sync()
	})
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	if synced, appended := p.syncedSeq.Load(), p.appendSeq.Load(); synced != 1 || appended != 2 {
		t.Fatalf("after one Sync: syncedSeq=%d appendSeq=%d; the write that arrived mid-fsync must not count as durable (want 1 and 2)", synced, appended)
	}
	// And it is picked up by the next one.
	if err := p.Sync(); err != nil || p.syncedSeq.Load() != 2 {
		t.Fatalf("second Sync: err=%v syncedSeq=%d", err, p.syncedSeq.Load())
	}
}

// BAD-3. On Linux a failed fsync can drop the dirty pages and clear the error,
// so the next fsync "succeeds" for bytes that never reached the disk. The fault
// here is exactly that: the failing call discards what was not yet synced and the
// next call succeeds. Retrying and acknowledging is the bug; Kafka takes the log
// directory offline, and so does this: every later flush and write is refused
// until a restart lets recovery decide from what is really on disk.
func TestFsyncFailureDoesNotFalselySucceed(t *testing.T) {
	for _, suffix := range []string{".log", ".index"} {
		t.Run(suffix, func(t *testing.T) {
			dir := t.TempDir()
			p, err := openPartition(dir, ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer p.close()
			if _, err := p.Append(makeBatch(1, []byte("x"))); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int64
			hookFsync(t, func(f *os.File) error {
				if !strings.HasSuffix(f.Name(), suffix) {
					return f.Sync()
				}
				if calls.Add(1) == 1 {
					// The kernel's behaviour: the pages are dropped, the error is
					// reported once.
					_ = f.Truncate(0)
					return errDisk
				}
				return f.Sync() // the retry "succeeds".
			})
			if err := p.Sync(); !errors.Is(err, errDisk) {
				t.Fatalf("first Sync = %v, want the fsync error", err)
			}
			if p.syncedSeq.Load() != 0 {
				t.Fatal("a failed fsync advanced the durable mark")
			}
			err = p.Sync()
			if err == nil {
				t.Fatal("the retry after a failed fsync reported success for data the kernel had dropped")
			}
			if !errors.Is(err, ErrStorageOffline) {
				t.Fatalf("second Sync = %v, want ErrStorageOffline", err)
			}
			if p.syncedSeq.Load() != 0 {
				t.Fatal("the durable mark advanced across a failed fsync")
			}
			if _, err := p.Append(makeBatch(1, []byte("y"))); !errors.Is(err, ErrStorageOffline) {
				t.Fatalf("an append after a failed fsync = %v, want ErrStorageOffline", err)
			}
			if n := calls.Load(); n != 1 {
				t.Errorf("the %s was fsynced %d time(s); a failed fsync must not be retried", suffix, n)
			}
		})
	}
}

// A roll seals the segment it leaves. If a flush of that segment is already in
// progress the seal must wait for it and learn how it ended: seeing "clean" and
// moving on left the segment unsynced when that flush then failed.
func TestRollWaitsForAnInFlightFsyncAndLearnsItFailed(t *testing.T) {
	p, err := openPartition(t.TempDir(), ManagerOptions{SegmentBytes: 300, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.Append(makeBatch(1, []byte("first"))); err != nil {
		t.Fatal(err)
	}
	first := p.segments[0]

	inFsync, release := make(chan struct{}), make(chan struct{})
	// Whatever this test concludes, the flush it holds must be let go before the
	// partition is closed: a seal that did not wait used to fail the test and then
	// hang it for the whole timeout, behind its own blocked fsync.
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	defer letGo()
	var calls atomic.Int64
	hookFsync(t, func(f *os.File) error {
		if f != first.log {
			return f.Sync()
		}
		if calls.Add(1) == 1 {
			close(inFsync)
			<-release
			return errDisk // the flush that was in progress fails.
		}
		return f.Sync()
	})

	timerErr := make(chan error, 1)
	go func() { timerErr <- p.Sync() }() // the periodic timer.
	<-inFsync

	rolled := make(chan error, 1)
	go func() { _, err := p.Append(makeBatch(1, make([]byte, 280))); rolled <- err }() // forces a roll.
	select {
	case err := <-rolled:
		t.Fatalf("the roll finished (err=%v) while the segment's flush was still in progress", err)
	case <-time.After(150 * time.Millisecond):
	}
	letGo()
	if err := <-timerErr; !errors.Is(err, errDisk) {
		t.Fatalf("the failed flush reported %v", err)
	}
	// The seal must not see "clean" and move on, and must not retry (BAD-3): it
	// learns how the flush ended, and the roll does not happen.
	if err := <-rolled; !errors.Is(err, ErrStorageOffline) || !errors.Is(err, errDisk) {
		t.Fatalf("roll: %v, want the failed flush reported as ErrStorageOffline", err)
	}
	if len(p.segments) != 1 {
		t.Fatalf("a segment whose flush failed was rolled off: %d segments", len(p.segments))
	}
	if !first.logDirty.Load() || calls.Load() != 1 {
		t.Errorf("segment dirty=%v after %d fsync attempt(s); want dirty and exactly the one failed attempt", first.logDirty.Load(), calls.Load())
	}
}

// Rolling seals the old segment with an fsync, with no timer involved.
func TestRollFsyncsTheSegmentItSeals(t *testing.T) {
	p, err := openPartition(t.TempDir(), ManagerOptions{SegmentBytes: 200, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	var c fsyncCounter
	c.hook(t)
	p.Append(makeBatch(1, make([]byte, 100)))
	if c.log.Load() != 0 {
		t.Fatal("an append fsynced")
	}
	p.Append(makeBatch(1, make([]byte, 100))) // exceeds 200 bytes: rolls.
	if len(p.segments) != 2 || c.log.Load() != 1 {
		t.Errorf("after a roll: %d segments, %d log fsync(s); want 2 and 1", len(p.segments), c.log.Load())
	}
}

// A flush that never happened is an error even if the file has since been closed.
func TestSyncAfterAFailedCloseIsNotReportedAsSuccess(t *testing.T) {
	p, err := openPartition(t.TempDir(), testOpts())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(makeBatch(1, []byte("x"))); err != nil {
		t.Fatal(err)
	}
	fail := true
	hookFsync(t, func(f *os.File) error {
		if fail {
			return errDisk
		}
		return f.Sync()
	})
	if err := p.close(); err == nil {
		t.Fatal("close with a failing fsync should report it")
	}
	fail = false
	if err := p.Sync(); err == nil {
		t.Fatalf("Sync returned nil though the only flush failed and the file is closed (syncedSeq=%d appendSeq=%d)", p.syncedSeq.Load(), p.appendSeq.Load())
	}
}

// ---- close and the periodic timer ----

func TestCloseAndPeriodicSyncFlushWhatWasWritten(t *testing.T) {
	var c fsyncCounter
	c.hook(t)
	m, err := OpenManager(t.TempDir(), testOpts())
	if err != nil {
		t.Fatal(err)
	}
	p, _ := m.Partition("t", 0)
	q, _ := m.Partition("u", 0)
	p.Append(makeBatch(1, []byte("x")))

	if err := m.Sync(); err != nil { // what the timer calls.
		t.Fatal(err)
	}
	if n := c.log.Load(); n != 1 {
		t.Fatalf("Manager.Sync fsynced %d logs, want exactly the 1 that was written to", n)
	}
	if err := m.Sync(); err != nil || c.log.Load() != 1 {
		t.Fatalf("a second Sync with nothing new fsynced again (%d)", c.log.Load())
	}

	p.Append(makeBatch(1, []byte("y")))
	q.Append(makeBatch(1, []byte("z")))
	before := c.log.Load()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if n := c.log.Load() - before; n != 2 {
		t.Errorf("Close fsynced %d logs, want the 2 with unflushed writes", n)
	}
}

// The index is flushed when it gained an entry, not on every sync.
func TestIndexIsFsyncedOnlyWhenItChanged(t *testing.T) {
	p, err := openPartition(t.TempDir(), ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	var c fsyncCounter
	c.hook(t)
	for i := 0; i < 4; i++ {
		p.Append(makeBatch(1, []byte("x")))
		if err := p.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if c.log.Load() != 4 || c.index.Load() != 1 {
		t.Errorf("4 appends and syncs: %d log fsyncs, %d index fsyncs; want 4 and 1 (only the first batch adds an index entry)", c.log.Load(), c.index.Load())
	}
}

// ---- index write failure ----

// An append that fails leaves no trace. It used to leave its bytes counted in the
// segment, so the retry wrote the same offset a second time after them.
func TestFailedAppendDoesNotLeaveADuplicateOffset(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.Append(makeBatch(1, []byte("a"))); err != nil {
		t.Fatal(err)
	}
	seg := p.active()
	good := seg.index.file
	ro, err := os.Open(good.Name()) // read-only: the next index write fails.
	if err != nil {
		t.Fatal(err)
	}
	seg.index.file = ro
	if _, err := p.Append(makeBatch(1, []byte("first-try"))); err == nil {
		t.Fatal("the append should have failed with the index unwritable")
	}
	seg.index.file = good
	ro.Close()
	off, err := p.Append(makeBatch(1, []byte("retry")))
	if err != nil || off != 1 {
		t.Fatalf("retry: offset %d, err %v; want offset 1", off, err)
	}
	raw, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var offsets []int64
	for pos := 0; pos < len(raw); {
		info, ok := parseHeader(raw[pos:])
		if !ok {
			t.Fatalf("unparseable batch at %d", pos)
		}
		offsets = append(offsets, info.baseOffset)
		pos += int(info.totalBytes)
	}
	if len(offsets) != 2 || offsets[0] != 0 || offsets[1] != 1 {
		t.Errorf("batches on disk have base offsets %v, want [0 1]", offsets)
	}
}

// ---- retention of partitions that are not open ----

func TestClosedRetentionSkipsOpenPartitionsAndKeepsTheNewestSegment(t *testing.T) {
	dir := t.TempDir()
	write := func(m *Manager, topic string) {
		p, err := m.Partition(topic, 0)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 5; i++ {
			if _, err := p.Append(makeBatch(1, make([]byte, 150))); err != nil {
				t.Fatal(err)
			}
		}
	}
	segs := func(topic string) int {
		logs, _ := filepath.Glob(filepath.Join(dir, topic+"-0", "*.log"))
		return len(logs)
	}
	m, err := OpenManager(dir, ManagerOptions{SegmentBytes: 200, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	write(m, "closed")
	write(m, "open")
	m.Close()
	if segs("closed") != 5 || segs("open") != 5 {
		t.Fatalf("setup: %d and %d segments, want 5 each", segs("closed"), segs("open"))
	}

	m, err = OpenManager(dir, ManagerOptions{SegmentBytes: 200, IndexIntervalBytes: 4096, Retention: RetentionPolicy{MaxBytes: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err := m.Partition("open", 0); err != nil {
		t.Fatal(err)
	}
	if err := m.EnforceClosedRetention(); err != nil {
		t.Fatal(err)
	}
	if n := segs("open"); n != 5 {
		t.Errorf("the open partition lost segments to the closed-partition sweep (%d left of 5): its files are in use", n)
	}
	if n := segs("closed"); n != 1 {
		t.Errorf("the closed partition has %d segments after retention, want exactly the newest one", n)
	}
	if _, end, _ := m.Bounds("closed", 0); end != 5 {
		t.Errorf("the closed partition's end offset is %d after retention, want 5 (the newest segment must survive)", end)
	}
}
