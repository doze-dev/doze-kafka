package log

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func openCounting(t *testing.T, hook func() error) *Partition {
	t.Helper()
	opts := testOpts()
	opts.BeforeSync = hook
	p, err := openPartition(filepath.Join(t.TempDir(), "t-0"), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.close() })
	return p
}

// The periodic timer used to fsync every open partition whether or not anything
// had been written. A sync with nothing new to flush must not touch the disk.
func TestSyncIsSkippedWhenNothingChanged(t *testing.T) {
	var fsyncs atomic.Int64
	p := openCounting(t, func() error { fsyncs.Add(1); return nil })

	for range 10 {
		if err := p.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if n := fsyncs.Load(); n != 0 {
		t.Fatalf("%d fsyncs on a partition that was never written", n)
	}

	if _, err := p.Append(makeBatch(1, []byte("x"))); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if err := p.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if n := fsyncs.Load(); n != 1 {
		t.Fatalf("%d fsyncs for one write followed by ten syncs, want exactly 1", n)
	}
}

// Most appends add no index entry (one per interval); the index must be flushed
// only when it changed.
func TestIndexIsFlushedOnlyWhenItChanged(t *testing.T) {
	opts := testOpts()
	opts.IndexIntervalBytes = 1 << 20 // one entry for the first batch, then none for a long while.
	p, err := openPartition(filepath.Join(t.TempDir(), "t-0"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	seg := p.active()

	if _, err := p.Append(makeBatch(1, []byte("first"))); err != nil {
		t.Fatal(err)
	}
	if !seg.index.dirty.Load() {
		t.Fatal("the first batch adds an index entry, which must mark the index dirty")
	}
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	if seg.index.dirty.Load() {
		t.Fatal("the index is still dirty after a successful sync")
	}
	if _, err := p.Append(makeBatch(1, []byte("second"))); err != nil {
		t.Fatal(err)
	}
	if seg.index.dirty.Load() {
		t.Fatal("an append that adds no index entry marked the index dirty")
	}
	if !seg.logDirty.Load() {
		t.Fatal("any append must mark the log dirty")
	}
}

// Concurrent acks=all writers on one partition must share fsyncs, and none may
// return before a flush that covers its own write.
func TestGroupCommitSharesFsyncs(t *testing.T) {
	var fsyncs atomic.Int64
	p := openCounting(t, func() error {
		fsyncs.Add(1)
		time.Sleep(5 * time.Millisecond) // a slow disk, so callers pile up behind one flush.
		return nil
	})

	const writers = 32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := p.Append(makeBatch(1, []byte("x"))); err != nil {
				t.Error(err)
				return
			}
			mine := p.appendSeq.Load() // at least this writer's own sequence.
			if err := p.Sync(); err != nil {
				t.Error(err)
				return
			}
			if got := p.syncedSeq.Load(); got < mine {
				t.Errorf("Sync returned with durable seq %d, below the write it was asked to cover (%d)", got, mine)
			}
		}()
	}
	close(start)
	wg.Wait()

	n := fsyncs.Load()
	if n < 1 || n >= writers {
		t.Fatalf("%d fsyncs for %d concurrent writers; group commit should need fewer than one each", n, writers)
	}
	t.Logf("%d writers shared %d fsyncs", writers, n)
}

// A failed flush must fail every writer it was meant to cover — group commit must
// never turn one caller's failure into another's success.
func TestGroupCommitDoesNotHideAFailure(t *testing.T) {
	errDisk := errors.New("disk on fire")
	p := openCounting(t, func() error { time.Sleep(2 * time.Millisecond); return errDisk })

	var wg sync.WaitGroup
	var failed atomic.Int64
	const writers = 16
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Append(makeBatch(1, []byte("x"))); err != nil {
				t.Error(err)
				return
			}
			if err := p.Sync(); err != nil {
				if !errors.Is(err, errDisk) {
					t.Errorf("unexpected error: %v", err)
				}
				failed.Add(1)
			}
		}()
	}
	wg.Wait()
	if failed.Load() != writers {
		t.Fatalf("%d of %d writers saw the failure; the rest were told their data was durable", failed.Load(), writers)
	}
	if got := p.syncedSeq.Load(); got != 0 {
		t.Fatalf("durable seq advanced to %d though every fsync failed", got)
	}
}

// Shutdown closes partitions in parallel; every one must still be flushed.
func TestManagerCloseFlushesEveryPartition(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	const parts = 100
	for i := int32(0); i < parts; i++ {
		p, err := m.Partition("many", i)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Append(makeBatch(2, []byte("payload"))); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	m2, err := OpenManager(dir, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	for i := int32(0); i < parts; i++ {
		p, err := m2.Partition("many", i)
		if err != nil || p.LogEndOffset() != 2 {
			t.Fatalf("partition %d after reopen: LEO %v, %v", i, p, err)
		}
	}
}
