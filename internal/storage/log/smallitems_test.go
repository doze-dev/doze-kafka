package log

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

var errInjected = &os.PathError{Op: "write", Path: "x", Err: syscall.EIO}

// A payload of several batches whose second cannot be written leaves nothing of
// itself in the log: the first used to stay, answered as failed and read by
// consumers, and written twice by a plain producer's retry.
func TestAPayloadWhoseSecondBatchFailsLeavesNothing(t *testing.T) {
	p, err := openPartition(t.TempDir(), ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	mustAppend(t, p, makeBatch(1, []byte("before")))
	payload := append(makeBatch(1, []byte("first")), makeBatch(1, []byte("second"))...)
	writes := 0
	restore := SetFSHookForTest(func(e FSEvent) error {
		if !e.Done && e.Op == FSWrite && strings.HasSuffix(e.Path, ".log") {
			writes++
			if writes == 2 {
				return errInjected
			}
		}
		return nil
	})
	_, err = p.Append(payload)
	restore()
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("append: %v, want the injected error", err)
	}
	if got := p.LogEndOffset(); got != 1 {
		t.Fatalf("the log ends at %d after a refused payload, want 1", got)
	}
	data, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("first")) {
		t.Fatal("the first batch of a refused payload is in the log")
	}
	if off, err := p.Append(payload); err != nil || off != 1 {
		t.Fatalf("the retry: offset %d, %v; want 1", off, err)
	}
	if got := p.LogEndOffset(); got != 3 {
		t.Fatalf("after the retry the log ends at %d, want 3", got)
	}
}

// A directory flush that fails is a failure: the flush that needed it reports it
// (syncDir discarded every error, so a new segment's name could be lost while
// acks=all was answered).
func TestADirectoryFlushThatFailsIsReported(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	mustAppend(t, p, makeBatch(1, []byte("x")))
	if runtime.GOOS == "windows" {
		t.Skip("a directory is not flushed on Windows (dirsync_windows.go)")
	}
	// The directory cannot be opened to be flushed: the real call fails, not the seam.
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o755)
	if err := p.Sync(); err == nil {
		t.Fatal("Sync of a fresh partition whose directory cannot be flushed succeeded")
	}
}

// The transaction mark whose directory flush failed is flushed again by the next
// transactional append, not taken as done because the file is there.
func TestATransactionMarkWhoseFlushFailedIsFlushedAgain(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	mustAppend(t, p, makeBatch(1, []byte("plain")))
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	var fail atomic.Bool
	fail.Store(true)
	dirsyncs := 0
	restore := SetFSHookForTest(func(e FSEvent) error {
		if e.Op == FSDirSync && !e.Done {
			if fail.Load() {
				return errInjected
			}
			dirsyncs++
		}
		return nil
	})
	defer restore()
	if _, err := p.Append(txKVBatch(7, 0, kv{"k", "v"})); err == nil {
		t.Fatal("a transactional append whose mark could not be flushed succeeded")
	}
	if _, err := os.Stat(filepath.Join(dir, txnMarkerFile)); err != nil {
		t.Skip("the mark was not left behind; nothing to retry")
	}
	fail.Store(false)
	if _, err := p.Append(txKVBatch(7, 0, kv{"k", "v"})); err != nil {
		t.Fatal(err)
	}
	if dirsyncs == 0 {
		t.Fatal("the retry found the mark's file and did not flush its directory")
	}
}

// DeleteRecords to the end of a partition that is offline changes nothing: the
// new start used to be written and then the request answered as failed.
func TestDeleteRecordsOnAnOfflinePartitionChangesNothing(t *testing.T) {
	dir := t.TempDir()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	mustAppend(t, p, makeBatch(1, []byte("a")))
	mustAppend(t, p, makeBatch(1, []byte("b")))
	restore := SetFsyncForTest(func(*os.File) error { return errInjected })
	if err := p.Sync(); err == nil {
		t.Fatal("setup: the failed fsync did not fail the sync")
	}
	restore()
	if !p.Offline() {
		t.Fatal("setup: the partition is not offline")
	}
	for _, to := range []int64{1, HighWatermark} {
		if _, err := p.DeleteBefore(to); err == nil {
			t.Fatalf("DeleteRecords to %d on an offline partition succeeded", to)
		}
	}
	if got := p.LogStartOffset(); got != 0 {
		t.Fatalf("the log start moved to %d", got)
	}
	if got := readLogStart(dir); got > 0 {
		t.Fatalf("the log-start file says %d", got)
	}
}

// The timer's flush of an offline partition is reported once, with its topic and
// partition, not on every tick.
func TestAnOfflinePartitionIsReportedOnceByTheTimer(t *testing.T) {
	var logged bytes.Buffer
	m, err := OpenManager(t.TempDir(), ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 64,
		Logger: slog.New(slog.NewTextHandler(&logged, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.EnsurePartitionDirs("orders", 1); err != nil {
		t.Fatal(err)
	}
	p, err := m.ExistingPartition("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, p, makeBatch(1, []byte("a")))
	restore := SetFsyncForTest(func(*os.File) error { return errInjected })
	defer restore()
	for range 5 {
		m.periodicSync()
	}
	if n := strings.Count(logged.String(), "periodic fsync failed"); n != 1 {
		t.Fatalf("five ticks over an offline partition logged %d lines, want 1:\n%s", n, logged.String())
	}
	if !strings.Contains(logged.String(), "topic=orders") || !strings.Contains(logged.String(), "partition=0") {
		t.Fatalf("the line does not name the partition: %s", logged.String())
	}
}
