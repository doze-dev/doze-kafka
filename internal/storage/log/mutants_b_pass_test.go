package log

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Tests of the cleaner's whole passes: what a pass counts, what it leaves on disk, and
// how it decides what to clean. Each reads back from the files and from a reopen.

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// onePerSegment opens a partition that rolls before every batch, so each batch appended
// sits in a segment of its own and the last one appended is the active one.
func onePerSegment(t *testing.T, interval int64) *Partition {
	t.Helper()
	p, err := openPartition(t.TempDir(), ManagerOptions{SegmentBytes: 1, IndexIntervalBytes: interval})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.close() })
	return p
}

// ageAll sets every segment's age to t0 (in memory, which is what the cleaner reads).
func ageAll(p *Partition) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range p.segments {
		s.lastModified = t0
	}
}

func compactAt(t *testing.T, p *Partition, pol CompactionPolicy, now time.Time) CleanResult {
	t.Helper()
	res, err := p.Compact(pol, now)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out
}

func cleanedToOnDisk(t *testing.T, dir string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, cleanedFile))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// One pass over three sealed segments rewrites the one that keeps something, deletes the
// one it empties, leaves the one it does not change, and says so: counts, bytes, the
// files that are left, the offset it cleaned to, and the same after a reopen.
func TestB_ACompactionPassIsCountedAndWrittenToDisk(t *testing.T) {
	p := onePerSegment(t, 64)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}, kv{"c", "1"})) // 0-1: a goes, c stays.
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "2"}))               // 2: goes, and its segment with it.
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "3"}))               // 3: the latest of a.
	mustAppend(t, p, kvBatch(codecNone, kv{"z", "1"}))               // 4: active.
	if len(p.segments) != 4 {
		t.Fatalf("%d segments", len(p.segments))
	}
	sizes := []int64{p.segments[0].sizeBytes, p.segments[1].sizeBytes, p.segments[2].sizeBytes}
	ageAll(p)

	res := compactAt(t, p, eager, t0.Add(time.Hour))
	if !res.Ran || res.RecordsRemoved != 2 || res.SegmentsRewritten != 1 || res.SegmentsRemoved != 1 || res.MarkersRemoved != 0 || res.Unreadable != 0 {
		t.Fatalf("result = %+v", res)
	}
	if !res.Complete {
		t.Errorf("the pass reached the end of the sealed log but says it did not: %+v", res)
	}
	if res.BytesBefore != sizes[0]+sizes[1]+sizes[2] {
		t.Errorf("BytesBefore = %d, want %d", res.BytesBefore, sizes[0]+sizes[1]+sizes[2])
	}
	// What is left of the first segment is a batch of c alone, and the third is untouched.
	oneC := int64(len(kvBatch(codecNone, kv{"c", "1"})))
	if res.BytesAfter != oneC+sizes[2] {
		t.Errorf("BytesAfter = %d, want %d", res.BytesAfter, oneC+sizes[2])
	}
	if got := p.LastClean(); !reflect.DeepEqual(got, res) {
		t.Errorf("LastClean = %+v, want %+v", got, res)
	}
	if got := cleanedToOnDisk(t, p.dir); got != "4\n" {
		t.Errorf("cleaner-offset = %q, want the active segment's base 4", got)
	}
	var logs []string
	for _, n := range dirNames(t, p.dir) {
		if strings.HasSuffix(n, ".log") {
			logs = append(logs, n)
		}
	}
	if want := []string{segmentName(0) + ".log", segmentName(3) + ".log", segmentName(4) + ".log"}; !reflect.DeepEqual(logs, want) {
		t.Errorf("segment files = %v, want %v", logs, want)
	}
	check := func(p *Partition, what string) {
		t.Helper()
		if got := show(dump(t, p)); got != "1:c=1 3:a=3 4:z=1" {
			t.Errorf("%s: records = %q", what, got)
		}
		if p.LogStartOffset() != 0 || p.LogEndOffset() != 5 {
			t.Errorf("%s: log %d..%d", what, p.LogStartOffset(), p.LogEndOffset())
		}
	}
	check(p, "after the pass")
	dir := p.dir
	p.close()
	p2, err := openPartition(dir, ManagerOptions{SegmentBytes: 1, IndexIntervalBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.close()
	check(p2, "after a reopen")
	// A second pass has nothing to do and says nothing.
	if res := compactAt(t, p2, eager, t0.Add(time.Hour)); res.Ran {
		t.Errorf("a second pass ran: %+v", res)
	}
}

// A log of one sealed segment and the active one is cleaned.
func TestB_ASingleSealedSegmentIsCleaned(t *testing.T) {
	p := onePerSegment(t, 64)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}, kv{"a", "2"}))
	mustAppend(t, p, kvBatch(codecNone, kv{"z", "1"}))
	if len(p.segments) != 2 {
		t.Fatalf("%d segments", len(p.segments))
	}
	res := compactAt(t, p, eager, time.Now().Add(time.Hour))
	if !res.Ran || res.RecordsRemoved != 1 {
		t.Fatalf("result = %+v", res)
	}
	if got := show(dump(t, p)); got != "1:a=2 2:z=1" {
		t.Errorf("records = %q", got)
	}
}

// A key map that holds exactly as many keys as the limit stops there: the pass is not
// complete, it cleans only what the map covered, and says where it got to.
func TestB_AFullKeyMapStopsThePassAtTheLimit(t *testing.T) {
	p := onePerSegment(t, 64)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}, kv{"b", "1"})) // 0-1
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "2"}, kv{"b", "2"})) // 2-3
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "3"}, kv{"b", "3"})) // 4-5
	mustAppend(t, p, kvBatch(codecNone, kv{"z", "1"}))               // 6: active.
	ageAll(p)
	pol := CompactionPolicy{Enabled: true, MaxMapEntries: 2}
	res := compactAt(t, p, pol, t0.Add(time.Hour))
	if !res.Ran || res.Complete || res.RecordsRemoved != 0 || res.SegmentsRewritten != 0 {
		t.Fatalf("first pass = %+v: the map of two keys covers the first segment alone, which has nothing to remove", res)
	}
	if got := cleanedToOnDisk(t, p.dir); got != "2\n" {
		t.Errorf("cleaner-offset = %q, want the second segment's base 2", got)
	}
	if got := show(dump(t, p)); got != "0:a=1 1:b=1 2:a=2 3:b=2 4:a=3 5:b=3 6:z=1" {
		t.Errorf("records = %q", got)
	}
}

// A segment that begins exactly where the last pass stopped has not been cleaned: it is
// dirty, it is in the key map, and a tombstone in it is not yet old enough to go however
// old the segment is.
func TestB_ASegmentAtTheCleanedOffsetIsDirtyAndKeepsItsTombstones(t *testing.T) {
	p := onePerSegment(t, 64)
	pol := CompactionPolicy{Enabled: true, DeleteRetention: 10 * time.Minute}
	mustAppend(t, p, kvBatch(codecNone, kv{"k", "1"}, kv{"a", "1"}))                   // 0-1
	mustAppend(t, p, kvBatch(codecNone, kv{"k", "<nil>"}))                             // 2: the tombstone of k.
	mustAppend(t, p, kvBatch(codecNone, kv{"m", "1"}, kv{"m", "2"}, kv{"t", "<nil>"})) // 3-5: active.
	ageAll(p)
	res := compactAt(t, p, pol, t0.Add(time.Hour))
	if !res.Ran || res.RecordsRemoved != 1 {
		t.Fatalf("first pass = %+v, want k's first value removed", res)
	}
	if got := cleanedToOnDisk(t, p.dir); got != "3\n" {
		t.Fatalf("cleaner-offset = %q", got)
	}
	mustAppend(t, p, kvBatch(codecNone, kv{"n", "1"})) // 6: the third segment is sealed, at offset 3 = cleaned-to.
	ageAll(p)
	res = compactAt(t, p, pol, t0.Add(time.Hour))
	if !res.Ran || res.RecordsRemoved != 2 { // m=1 and the tombstone of k, whose segment was cleaned before.
		t.Fatalf("second pass = %+v, want m=1 and k's tombstone removed", res)
	}
	if !res.Complete {
		t.Errorf("second pass incomplete: %+v", res)
	}
	got := show(dump(t, p))
	if want := "1:a=1 4:m=2 5:t=<nil> 6:n=1"; got != want {
		t.Errorf("records = %q, want %q: the tombstone of t is in a segment that had not been cleaned before this pass and stays", got, want)
	}
}

// A tombstone goes once its segment has been cleaned and has aged delete.retention.ms:
// at that age exactly, not a nanosecond before.
func TestB_ATombstoneGoesAtExactlyDeleteRetention(t *testing.T) {
	const retention = 10 * time.Minute
	for _, c := range []struct {
		name string
		now  time.Time
		gone bool
	}{{"one nanosecond short", t0.Add(retention - time.Nanosecond), false}, {"exactly", t0.Add(retention), true}} {
		t.Run(c.name, func(t *testing.T) {
			p := onePerSegment(t, 64)
			pol := CompactionPolicy{Enabled: true, DeleteRetention: retention}
			mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}))
			mustAppend(t, p, kvBatch(codecNone, kv{"k", "<nil>"})) // 1
			mustAppend(t, p, kvBatch(codecNone, kv{"m", "1"}))     // 2: active.
			ageAll(p)
			compactAt(t, p, pol, t0.Add(time.Hour)) // nothing to remove; the first two segments are now cleaned.
			if got := cleanedToOnDisk(t, p.dir); got != "2\n" {
				t.Fatalf("cleaner-offset = %q", got)
			}
			mustAppend(t, p, kvBatch(codecNone, kv{"n", "1"})) // 3: segment 2 is sealed and dirty.
			ageAll(p)
			res := compactAt(t, p, pol, c.now)
			has := hasRow(dump(t, p), "k", "<nil>")
			if !res.Ran || has == c.gone {
				t.Errorf("at %s: result %+v, tombstone present = %v, want present = %v", c.now.Sub(t0), res, has, !c.gone)
			}
			if c.gone && res.RecordsRemoved != 1 {
				t.Errorf("RecordsRemoved = %d, want the tombstone", res.RecordsRemoved)
			}
		})
	}
}

// A segment is old enough for min.compaction.lag.ms at exactly that age, and old enough
// to force a pass for max.compaction.lag.ms at exactly that age.
func TestB_CompactionLagsAreInclusive(t *testing.T) {
	build := func(t *testing.T) *Partition {
		p := onePerSegment(t, 64)
		mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}, kv{"a", "2"}))
		mustAppend(t, p, kvBatch(codecNone, kv{"z", "1"}))
		ageAll(p)
		return p
	}
	const lag = 10 * time.Minute
	for _, c := range []struct {
		name string
		pol  CompactionPolicy
		age  time.Duration
		runs bool
	}{
		{"min lag, a nanosecond short", CompactionPolicy{Enabled: true, MinLag: lag}, lag - time.Nanosecond, false},
		{"min lag, exactly", CompactionPolicy{Enabled: true, MinLag: lag}, lag, true},
		// A dirty ratio that cannot be met (it is at most 1) leaves only the max lag to force the pass.
		{"max lag, a nanosecond short", CompactionPolicy{Enabled: true, MinLag: time.Hour, MaxLag: lag, MinDirtyRatio: 2}, lag - time.Nanosecond, false},
		{"max lag, exactly", CompactionPolicy{Enabled: true, MinLag: time.Hour, MaxLag: lag, MinDirtyRatio: 2}, lag, true},
		{"max lag, past it, min lag not reached", CompactionPolicy{Enabled: true, MinLag: time.Hour, MaxLag: lag, MinDirtyRatio: 2}, lag + time.Second, true},
		{"dirty ratio met exactly", CompactionPolicy{Enabled: true, MinDirtyRatio: 1}, time.Hour, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := build(t)
			res := compactAt(t, p, c.pol, t0.Add(c.age))
			if res.Ran != c.runs {
				t.Fatalf("Ran = %v, want %v (%+v)", res.Ran, c.runs, res)
			}
			want := "0:a=1 1:a=2 2:z=1"
			if c.runs {
				want = "1:a=2 2:z=1"
			}
			if got := show(dump(t, p)); got != want {
				t.Errorf("records = %q, want %q", got, want)
			}
		})
	}
}

// A segment that ends exactly at the last stable offset is whole below it and is cleaned.
func TestB_ASegmentEndingAtTheLastStableOffsetIsCleaned(t *testing.T) {
	p := onePerSegment(t, 64)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}, kv{"a", "2"})) // 0-1
	mustAppend(t, p, txKVBatch(5, 0, kv{"t", "1"}))                  // 2: the open transaction begins where the sealed log ends.
	if got := lso(t, p); got != 2 {
		t.Fatalf("setup: last stable offset %d, want 2", got)
	}
	res := compactAt(t, p, eager, time.Now().Add(time.Hour))
	if !res.Ran || res.RecordsRemoved != 1 {
		t.Fatalf("result = %+v", res)
	}
	if got := show(dump(t, p)); got != "1:a=2 2:t=1" {
		t.Errorf("records = %q", got)
	}
}

// The rewritten segment's index is built as an append builds one: an entry for its first
// batch and one each time the interval's worth of log has passed, here exactly one batch.
func TestB_ARewrittenSegmentsIndexFollowsTheInterval(t *testing.T) {
	one := int64(len(kvBatch(codecNone, kv{"k1", "x"})))
	p, err := openPartition(t.TempDir(), ManagerOptions{SegmentBytes: 4 * one, IndexIntervalBytes: one})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	mustAppend(t, p, kvBatch(codecNone, kv{"k1", "x"})) // 0: superseded.
	mustAppend(t, p, kvBatch(codecNone, kv{"k1", "y"})) // 1
	mustAppend(t, p, kvBatch(codecNone, kv{"k2", "y"})) // 2
	mustAppend(t, p, kvBatch(codecNone, kv{"k3", "y"})) // 3
	mustAppend(t, p, kvBatch(codecNone, kv{"z1", "y"})) // 4: active.
	if len(p.segments) != 2 {
		t.Fatalf("%d segments", len(p.segments))
	}
	res := compactAt(t, p, eager, time.Now().Add(time.Hour))
	if !res.Ran || res.RecordsRemoved != 1 || res.SegmentsRewritten != 1 {
		t.Fatalf("result = %+v", res)
	}
	want := []indexEntry{{1, 0}, {2, one}, {3, 2 * one}}
	if got := readIndexFileB(t, filepath.Join(p.dir, segmentName(0)+".index")); !reflect.DeepEqual(got, want) {
		t.Errorf("index on disk = %v, want %v", got, want)
	}
	if got := p.segments[0].index.entries; !reflect.DeepEqual(got, want) {
		t.Errorf("index in memory = %v, want %v", got, want)
	}
	// And a read at each offset finds its batch through that index.
	for off := int64(1); off <= 3; off++ {
		data, err := p.Read(off, 1<<20)
		if err != nil || len(data) == 0 {
			t.Fatalf("read at %d: %d bytes, %v", off, len(data), err)
		}
		if info, _ := parseHeader(data); info.baseOffset != off {
			t.Errorf("read at %d starts at the batch of %d", off, info.baseOffset)
		}
	}
}

// Batches whose records cannot be read stay as they are and are counted; a transaction's
// marker stays while a batch of it does, readable or not, and a batch of another kind does
// not hold a marker.
func TestB_UnreadableBatchesAreKeptAndCountedAndTheirMarkersFollowThem(t *testing.T) {
	junk := func(pid int64, tx bool) []byte {
		b := makeBatch(1, []byte("not a record"))
		if tx {
			binary.BigEndian.PutUint16(b[batchAttributesOff:], batchTransactionalBit)
		}
		binary.BigEndian.PutUint64(b[batchProducerIDOff:], uint64(pid))
		sealCRCOf(b)
		return b
	}
	t.Run("a transactional one holds its marker", func(t *testing.T) {
		p := onePerSegment(t, 64)
		mustAppend(t, p, junk(9, true)) // 0
		mustMarker(t, p, 9, true)       // 1
		mustAppend(t, p, kvBatch(codecNone, kv{"z", "1"}))
		res := compactAt(t, p, eager, time.Now().Add(time.Hour))
		if !res.Ran || res.Unreadable != 1 || res.MarkersRemoved != 0 || res.RecordsRemoved != 0 {
			t.Fatalf("result = %+v: the unreadable batch is kept as it is and its marker with it", res)
		}
		data, err := p.Read(1, 1<<20)
		if err != nil || len(data) == 0 {
			t.Fatalf("read the marker: %d bytes, %v", len(data), err)
		}
		if info, _ := parseHeader(data); info.baseOffset != 1 || attrsOf(data)&batchControlBit == 0 {
			t.Errorf("the batch at 1 is not the marker: %+v", info)
		}
	})
	t.Run("a plain one holds nothing", func(t *testing.T) {
		p := onePerSegment(t, 64)
		mustAppend(t, p, junk(9, false)) // 0
		mustAppend(t, p, controlBatch(9, 0, true, 0, 1000))
		mustAppend(t, p, kvBatch(codecNone, kv{"z", "1"}))
		res := compactAt(t, p, eager, time.Now().Add(time.Hour))
		if !res.Ran || res.Unreadable != 1 || res.MarkersRemoved != 1 || res.RecordsRemoved != 0 {
			t.Fatalf("result = %+v: the marker of a producer with no transactional batch goes", res)
		}
		// The marker's segment is not the first, so it is gone, and the log reads on.
		if _, err := p.Read(0, 1<<20); err != nil {
			t.Errorf("read at 0: %v", err)
		}
	})
}

// A marker removed by the pass is counted once for each.
func TestB_RemovedMarkersAreCounted(t *testing.T) {
	p := onePerSegment(t, 64)
	mustAppend(t, p, txKVBatch(1, 0, kv{"a", "1"})) // 0
	mustMarker(t, p, 1, true)                       // 1
	mustAppend(t, p, txKVBatch(2, 0, kv{"a", "2"})) // 2
	mustMarker(t, p, 2, true)                       // 3
	mustAppend(t, p, kvBatch(codecNone, kv{"z", "1"}))
	res := compactAt(t, p, eager, time.Now().Add(time.Hour))
	if !res.Ran || res.RecordsRemoved != 1 || res.MarkersRemoved != 1 {
		t.Fatalf("result = %+v, want a=1 and its marker removed, the marker of a=2 kept", res)
	}
}

// The swap refuses a segment that is the active one or is no longer in the partition, and
// removes the cleaned files it was given.
func TestB_SwapSegmentRefusesTheActiveAndTheUnknownSegment(t *testing.T) {
	p := onePerSegment(t, 64)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}))
	mustAppend(t, p, kvBatch(codecNone, kv{"b", "1"}))
	active := p.segments[len(p.segments)-1]
	stray, _, err := openSegment(t.TempDir(), 77, 64, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stray.close()
	for name, seg := range map[string]*Segment{"active": active, "stray": stray} {
		logTmp, idxTmp := filepath.Join(p.dir, "x.log"+cleanedExt), filepath.Join(p.dir, "x.index"+cleanedExt)
		for _, f := range []string{logTmp, idxTmp} {
			if err := os.WriteFile(f, []byte("cleaned"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		before := show(dump(t, p))
		done, err := p.swapSegment(seg, logTmp, idxTmp, false)
		if done || err != nil {
			t.Errorf("%s: swapSegment = %v, %v, want false, nil", name, done, err)
		}
		for _, f := range []string{logTmp, idxTmp} {
			if _, err := os.Stat(f); !os.IsNotExist(err) {
				t.Errorf("%s: %s was not removed (%v)", name, filepath.Base(f), err)
			}
		}
		if got := show(dump(t, p)); got != before {
			t.Errorf("%s: the log changed: %q -> %q", name, before, got)
		}
		if len(p.segments) != 2 || p.segments[1] != active {
			t.Errorf("%s: the segment list changed", name)
		}
	}
}

// A read that failed on a segment retention has meanwhile removed is not worth an error;
// one that failed on a segment still in the log is.
func TestB_CleanerReadFailureIsReportedOnlyForASegmentStillInTheLog(t *testing.T) {
	p := onePerSegment(t, 64)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}))
	mustAppend(t, p, kvBatch(codecNone, kv{"b", "1"}))
	boom := errors.New("boom")
	if err := p.cleanerReadFailed(p.segments[0], boom); err != boom {
		t.Errorf("a segment in the log: %v, want the read error", err)
	}
	gone, _, err := openSegment(t.TempDir(), 5, 64, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer gone.close()
	if err := p.cleanerReadFailed(gone, boom); err != nil {
		t.Errorf("a segment no longer in the log: %v, want nil", err)
	}
}
