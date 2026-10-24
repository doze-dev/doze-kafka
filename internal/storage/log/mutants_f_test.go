package log

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// opsOf runs fn and returns the file-seam operations (before they run) it made.
func opsOf(fn func()) []FSEvent {
	var ops []FSEvent
	restore := SetFSHookForTest(func(e FSEvent) error {
		if !e.Done {
			ops = append(ops, e)
		}
		return nil
	})
	defer restore()
	fn()
	return ops
}

func indexOfOp(ops []FSEvent, from int, match func(FSEvent) bool) int {
	for i := from; i < len(ops); i++ {
		if match(ops[i]) {
			return i
		}
	}
	return -1
}

// A swap flushes the directory after the marker is created (before the renames) and
// again after the renames (before the marker goes), so the names it changes are on disk
// in that order.
func TestF_SwapFlushesTheDirectoryAroundItsRenames(t *testing.T) {
	p := swapFixture(t, t.TempDir())
	defer p.close()
	ops := opsOf(func() { compact(t, p, eager) })
	isMark := func(e FSEvent) bool { return e.Op == FSCreate && strings.HasSuffix(e.Path, swapMarkExt) }
	isLogRename := func(e FSEvent) bool { return e.Op == FSRename && strings.HasSuffix(e.To, ".log") }
	isIdxRename := func(e FSEvent) bool { return e.Op == FSRename && strings.HasSuffix(e.To, ".index") }
	isMarkGone := func(e FSEvent) bool { return e.Op == FSRemove && strings.HasSuffix(e.Path, swapMarkExt) }
	isSyncDir := func(e FSEvent) bool { return e.Op == FSDirSync }
	swaps := 0
	for from := 0; ; {
		m := indexOfOp(ops, from, isMark)
		if m < 0 {
			break
		}
		swaps++
		lr := indexOfOp(ops, m, isLogRename)
		ir := indexOfOp(ops, m, isIdxRename)
		gone := indexOfOp(ops, m, isMarkGone)
		if lr < 0 || ir < 0 || gone < 0 {
			t.Fatalf("swap %d: incomplete operations %v", swaps, ops[m:])
		}
		if indexOfOp(ops[:lr], m, isSyncDir) < 0 {
			t.Errorf("swap %d: no directory flush between the marker and the renames", swaps)
		}
		if indexOfOp(ops[:gone], ir, isSyncDir) < 0 {
			t.Errorf("swap %d: no directory flush between the renames and the marker's removal", swaps)
		}
		from = gone
	}
	if swaps == 0 {
		t.Fatal("the pass swapped no segment")
	}
}

// removeFixture is a partition whose middle sealed segments each hold only records a
// later one supersedes, so a pass removes them outright.
func removeFixture(t *testing.T) *Partition {
	t.Helper()
	p := smallPartition(t, t.TempDir())
	pad := strings.Repeat("p", 380)
	for i := 0; i < 5; i++ {
		mustAppend(t, p, kvBatch(codecNone, kv{"a", string(rune('0'+i)) + pad}))
	}
	return p
}

// A segment the pass removes outright leaves the directory unflushed; the next Sync
// flushes it.
func TestF_ARemovedSegmentLeavesTheDirectoryToBeFlushedBySync(t *testing.T) {
	p := removeFixture(t)
	defer p.close()
	before := len(p.segments)
	compact(t, p, eager)
	if len(p.segments) >= before {
		t.Fatalf("segments %d -> %d: the fixture removed none", before, len(p.segments))
	}
	p.mu.Lock()
	dirty := p.dirSynced < p.dirSeq
	p.mu.Unlock()
	if !dirty {
		t.Fatal("the removal of a segment left nothing for the directory flush to do")
	}
	mustAppend(t, p, kvBatch(codecNone, kv{"z", "1"}))
	ops := opsOf(func() {
		if err := p.Sync(); err != nil {
			t.Fatal(err)
		}
	})
	if indexOfOp(ops, 0, func(e FSEvent) bool { return e.Op == FSDirSync && e.Path == p.dir }) < 0 {
		t.Errorf("Sync did not flush the directory: %v", ops)
	}
}

// When the log of a segment being removed cannot be removed, the pass fails, the segment
// stays in the list and its file stays on disk; when only its index cannot, the log is
// gone, the segment goes, and the pass still succeeds.
func TestF_RemovingASegmentWhoseFilesWillNotGo(t *testing.T) {
	t.Run("log", func(t *testing.T) {
		p := removeFixture(t)
		defer p.close()
		before := len(p.segments)
		failed := ""
		restore := SetFSHookForTest(func(e FSEvent) error {
			if !e.Done && e.Op == FSRemove && strings.HasSuffix(e.Path, ".log") && !strings.Contains(e.Path, "cleaned") && failed == "" {
				failed = e.Path
				return &fsError{op: "remove", err: syscall.EIO}
			}
			return nil
		})
		_, _ = p.Compact(eager, time.Now().Add(time.Hour))
		restore()
		if failed == "" {
			t.Fatal("no segment log was removed")
		}
		if _, serr := os.Stat(failed); serr != nil {
			t.Errorf("the log that would not go is not there: %v", serr)
		}
		if len(p.segments) < before-1 {
			t.Errorf("segments %d -> %d", before, len(p.segments))
		}
		for _, s := range p.segments {
			if s.log.Name() == failed {
				return
			}
		}
		t.Errorf("the segment whose log could not be removed left the list")
	})
	t.Run("index", func(t *testing.T) {
		p := removeFixture(t)
		defer p.close()
		before := len(p.segments)
		hit := false
		restore := SetFSHookForTest(func(e FSEvent) error {
			if !e.Done && e.Op == FSRemove && strings.HasSuffix(e.Path, ".index") && !strings.Contains(e.Path, "cleaned") {
				hit = true
				return &fsError{op: "remove", err: syscall.EIO}
			}
			return nil
		})
		_, _ = p.Compact(eager, time.Now().Add(time.Hour))
		restore()
		if !hit {
			t.Fatal("no index removal was attempted")
		}
		if len(p.segments) >= before {
			t.Errorf("the segment whose log was removed stayed in the list (%d -> %d)", before, len(p.segments))
		}
	})
}

// A segment that was still to be flushed is still to be flushed after a failed swap
// puts a reopened one in its place.
func TestF_AFailedSwapKeepsTheSegmentOnTheListToFlush(t *testing.T) {
	p := swapFixture(t, t.TempDir())
	defer p.close()
	p.mu.Lock()
	first := p.segments[0]
	p.unsynced = append(p.unsynced[:0], first)
	p.mu.Unlock()
	failed := false
	restore := SetFSHookForTest(func(e FSEvent) error {
		if !e.Done && e.Op == FSRename && strings.HasSuffix(e.To, ".log") {
			failed = true
			return &fsError{op: "rename", err: syscall.ENOSPC}
		}
		return nil
	})
	_, _ = p.Compact(eager, time.Now().Add(time.Hour))
	restore()
	if !failed {
		t.Fatal("no swap was attempted")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.unsynced) != 1 || p.unsynced[0] != p.segments[0] {
		t.Errorf("unsynced %v is not the segment now serving (%p)", p.unsynced, p.segments[0])
	}
}

// An index file is written whole, flushed, and its failures are the caller's.
func TestF_WriteIndexFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.index")
	ops := opsOf(func() {
		if err := writeIndexFile(path, nil); err != nil {
			t.Fatal(err)
		}
	})
	if indexOfOp(ops, 0, func(e FSEvent) bool { return e.Op == FSWrite }) >= 0 {
		t.Errorf("an empty index was written to: %v", ops)
	}
	if indexOfOp(ops, 0, func(e FSEvent) bool { return e.Op == FSFsync }) < 0 {
		t.Errorf("an empty index was not flushed: %v", ops)
	}
	entries := []indexEntry{{offset: 1, position: 2}, {offset: 3, position: 4}}
	ops = opsOf(func() {
		if err := writeIndexFile(path, entries); err != nil {
			t.Fatal(err)
		}
	})
	w := indexOfOp(ops, 0, func(e FSEvent) bool { return e.Op == FSWrite })
	f := indexOfOp(ops, 0, func(e FSEvent) bool { return e.Op == FSFsync })
	if w < 0 || f < w {
		t.Errorf("want a write then a flush: %v", ops)
	}
	if st, _ := os.Stat(path); st == nil || st.Size() != 32 {
		t.Errorf("index file: %v", st)
	}
	for _, op := range []FSOp{FSWrite, FSFsync} {
		boom := errors.New("injected")
		restore := SetFSHookForTest(func(e FSEvent) error {
			if !e.Done && e.Op == op {
				return boom
			}
			if !e.Done && e.Op == FSFsync && op == FSWrite {
				t.Error("flushed after a failed write")
			}
			return nil
		})
		err := writeIndexFile(path, entries)
		restore()
		if !errors.Is(err, boom) {
			t.Errorf("a failed %s: writeIndexFile = %v", op, err)
		}
	}
}
