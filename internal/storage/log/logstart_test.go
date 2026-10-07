package log

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openSmall(t *testing.T, dir string) *Partition {
	t.Helper()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 1000, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func segmentFiles(t *testing.T, dir string) int {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	return len(m)
}

// DeleteRecords moves the start into the middle of a segment: nothing below it is
// served, the records above are unmoved, and segments wholly below it leave the disk.
func TestDeleteBeforeHidesWhatIsBelowAndFreesWholeSegments(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	fillSegments(t, p, 30) // 227-byte single-record batches, 3 to a segment: offset 11 is inside one.
	if p.segments[1].baseOffset != 3 {
		t.Fatalf("expected 3 records per segment, got a second segment at %d", p.segments[1].baseOffset)
	}
	end := p.LogEndOffset()
	before := segmentFiles(t, dir)

	start, err := p.DeleteBefore(11)
	if err != nil || start != 11 {
		t.Fatalf("DeleteBefore(11) = %d, %v", start, err)
	}
	if base := p.segments[0].baseOffset; base != 9 {
		t.Errorf("first segment begins at %d, want 9: the one holding the start is kept whole", base)
	}
	if p.LogStartOffset() != 11 || p.LogEndOffset() != end {
		t.Errorf("start %d end %d, want 11 and %d", p.LogStartOffset(), p.LogEndOffset(), end)
	}
	if after := segmentFiles(t, dir); after >= before {
		t.Errorf("segments on disk %d -> %d: the ones wholly below the start were kept", before, after)
	}
	for _, off := range []int64{0, 10} {
		if _, err := p.Read(off, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Errorf("Read(%d) = %v, want ErrOffsetOutOfRange", off, err)
		}
	}
	if got, err := p.Read(11, 1<<20); err != nil || len(got) == 0 {
		t.Errorf("Read(11) = %d bytes, %v", len(got), err)
	}

	// A start at or below the current one changes nothing; above the end is refused.
	if s, err := p.DeleteBefore(3); err != nil || s != 11 {
		t.Errorf("DeleteBefore(3) = %d, %v, want a no-op at 11", s, err)
	}
	if _, err := p.DeleteBefore(end + 1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("DeleteBefore above the end = %v", err)
	}
	if _, err := p.DeleteBefore(-2); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("DeleteBefore(-2) = %v", err)
	}
	if p.LogStartOffset() != 11 {
		t.Errorf("a refused request moved the start to %d", p.LogStartOffset())
	}
}

// The start is recorded, so a restart (and a read of the bounds without opening the
// partition) finds it where it was put rather than at the first segment's base.
func TestDeleteBeforeSurvivesReopeningAndIsInTheClosedBounds(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	fillSegments(t, p, 12)
	if _, err := p.DeleteBefore(5); err != nil {
		t.Fatal(err)
	}
	end := p.LogEndOffset()
	p.close()

	if s, e, err := boundsFromDisk(dir); err != nil || s != 5 || e != end {
		t.Errorf("closed bounds = %d..%d, %v, want 5..%d", s, e, err, end)
	}
	p = openSmall(t, dir)
	defer p.close()
	if p.LogStartOffset() != 5 || p.LogEndOffset() != end {
		t.Errorf("reopened: start %d end %d, want 5 and %d", p.LogStartOffset(), p.LogEndOffset(), end)
	}
	if _, err := p.Read(4, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("Read(4) after reopening = %v", err)
	}
	// New records continue from the end and are readable.
	next, err := p.Append(makeBatch(1, []byte("after")))
	if err != nil || next != end {
		t.Fatalf("append after = %d, %v, want %d", next, err, end)
	}
	if got, err := p.Read(end, 1<<20); err != nil || len(got) == 0 {
		t.Errorf("Read(%d) = %d bytes, %v", end, len(got), err)
	}
}

// Emptying a partition (-1) removes what was in the active segment too, instead of
// holding it until the segment rolls, and the log carries on from where it ended.
func TestDeleteBeforeHighWatermarkEmptiesThePartitionOnDisk(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	defer p.close()
	fillSegments(t, p, 5)
	end := p.LogEndOffset()

	if s, err := p.DeleteBefore(HighWatermark); err != nil || s != end {
		t.Fatalf("DeleteBefore(-1) = %d, %v, want %d", s, err, end)
	}
	if n := segmentFiles(t, dir); n != 1 {
		t.Errorf("%d segment files remain, want only the empty active one", n)
	}
	var size int64
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			info, _ := e.Info()
			size += info.Size()
		}
	}
	if size != 0 {
		t.Errorf("%d bytes of deleted records are still on disk", size)
	}
	if got, err := p.Read(end, 1<<20); err != nil || len(got) != 0 {
		t.Errorf("Read at the new start: %d bytes, %v, want none and no error", len(got), err)
	}
	if next, err := p.Append(makeBatch(1, []byte("x"))); err != nil || next != end {
		t.Errorf("append = %d, %v, want offset %d", next, err, end)
	}
}

// Retention still works on a partition whose start was moved, and never moves it back.
func TestRetentionDoesNotMoveADeletedStartBack(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	defer p.close()
	fillSegments(t, p, 30)
	if _, err := p.DeleteBefore(21); err != nil {
		t.Fatal(err)
	}
	if err := (RetentionPolicy{MaxBytes: 600}).Enforce(p); err != nil {
		t.Fatal(err)
	}
	if p.LogStartOffset() < 21 {
		t.Errorf("start is %d after retention, below the 21 that was deleted to", p.LogStartOffset())
	}
}
