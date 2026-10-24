package log

import (
	"os"
	"path/filepath"
	"testing"
)

// A limited read with no byte limit (maxBytes 0) still returns every batch below the limit.
func TestMutA_LimitedReadWithNoByteLimitReturnsEverythingBelowTheLimit(t *testing.T) {
	p, _ := fiveBatches(t)
	p.mu.Lock()
	data, err := p.readLocked(0, 0, 100)
	p.mu.Unlock()
	if err != nil || !eq64(batchBases(t, data), []int64{0, 3, 6, 9, 12}) {
		t.Errorf("readLocked(0, 0, 100) = %d bytes, %v", len(data), err)
	}
}

// An index whose file cannot be read is an error, not an empty index.
func TestMutA_IndexLoadReportsAReadFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.index")
	ix, _, err := openIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	ix.append(0, 0)
	ix.append(5, 50)
	ix.close()
	f, err := os.OpenFile(path, os.O_WRONLY, 0) // size 32, but not readable.
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := (&Index{file: f}).load(); err == nil {
		t.Error("load of an unreadable index file reported nothing")
	}
}

// Dropping a segment removes its index with its log, whether by DeleteRecords or
// by finishing the job at the next open.
func TestMutA_RemovedSegmentsLeaveNoIndexBehind(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	fillSegments(t, p, 30)
	if _, err := p.DeleteBefore(11); err != nil {
		t.Fatal(err)
	}
	logs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	idx, _ := filepath.Glob(filepath.Join(dir, "*.index"))
	if len(logs) != len(idx) {
		t.Errorf("after DeleteBefore: %d logs and %d indexes: %v %v", len(logs), len(idx), logs, idx)
	}
	p.close()

	// A crash after the new start was recorded and before the segments below it
	// were removed: the next open removes them, indexes too.
	dir2 := t.TempDir()
	p = openSmall(t, dir2)
	fillSegments(t, p, 30)
	p.close()
	if err := os.WriteFile(filepath.Join(dir2, logStartFile), []byte("11\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	p = openSmall(t, dir2)
	defer p.close()
	logs, _ = filepath.Glob(filepath.Join(dir2, "*.log"))
	idx, _ = filepath.Glob(filepath.Join(dir2, "*.index"))
	if len(logs) != len(idx) || len(logs) == 0 || p.LogStartOffset() != 11 || p.segments[0].baseOffset != 9 {
		t.Errorf("after reopen: %d logs and %d indexes, start %d, first segment %d", len(logs), len(idx), p.LogStartOffset(), p.segments[0].baseOffset)
	}
}
