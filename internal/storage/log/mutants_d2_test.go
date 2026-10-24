package log

import "testing"

// A segment wholly below the start that could not be removed yet is still in the
// partition and invisible: lookups begin at the first segment that holds a visible
// offset, and answer from it.
func TestD_LookupsSkipWholeSegmentsBelowTheStartThatAreStillThere(t *testing.T) {
	p, err := openPartition(t.TempDir(), ManagerOptions{SegmentBytes: 150, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	for i := range 16 {
		mustAppend(t, p, timedBatch([]int64{int64(1000 - i)}, codecNone, false))
	}
	if len(p.segments) < 5 {
		t.Fatalf("%d segments", len(p.segments))
	}
	for _, start := range []int64{p.segments[3].baseOffset, p.segments[3].baseOffset + 1} {
		p.mu.Lock()
		p.logStartOffset = start // as DeleteBefore leaves it when the files below could not be removed
		got := p.visibleFrom()
		p.mu.Unlock()
		if want := 3; got != want {
			t.Errorf("start %d: visibleFrom = %d, want %d", start, got, want)
		}
		if off, ts, found := lookup(t, p, 0); !found || off != start || ts != 1000-start {
			t.Errorf("start %d: lookup(0) = %d %d %v", start, off, ts, found)
		}
		if off, ts, found, err := p.OffsetOfMaxTimestamp(); err != nil || !found || off != start || ts != 1000-start {
			t.Errorf("start %d: max = %d %d %v %v", start, off, ts, found, err)
		}
	}
}
