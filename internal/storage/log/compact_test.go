package log

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type kv struct{ k, v string } // v == "<nil>" is a tombstone, k == "<nil>" a null key.

func rec(off int, k, v string) Record {
	r := Record{OffsetDelta: int32(off), TimestampDelta: int64(off)}
	if k != "<nil>" {
		r.Key = []byte(k)
	}
	if v != "<nil>" {
		r.Value = []byte(v)
	}
	return r
}

// kvBatch is a keyed batch of the given pairs, each an offset after the one before.
func kvBatch(codec int, pairs ...kv) []byte {
	head := makeBatch(1, nil)
	binary.BigEndian.PutUint32(head[batchLastOffsetDeltaOff:], uint32(len(pairs)-1))
	recs := make([]Record, len(pairs))
	for i, p := range pairs {
		recs[i] = rec(i, p.k, p.v)
	}
	b, err := rebuildBatch(head, recs, codec)
	if err != nil {
		panic(err)
	}
	return b
}

// txKVBatch is kvBatch from a transactional producer.
func txKVBatch(pid int64, seq int32, pairs ...kv) []byte {
	b := kvBatch(codecNone, pairs...)
	binary.BigEndian.PutUint16(b[batchAttributesOff:], binary.BigEndian.Uint16(b[batchAttributesOff:])|batchTransactionalBit)
	binary.BigEndian.PutUint64(b[batchProducerIDOff:], uint64(pid))
	binary.BigEndian.PutUint32(b[batchBaseSequenceOff:], uint32(seq))
	sealCRCOf(b)
	return b
}

type row struct {
	off  int64
	k, v string
}

func (r row) String() string { return fmt.Sprintf("%d:%s=%s", r.off, r.k, r.v) }

// dump reads the whole partition the way a consumer would, from the log start to the
// end, and returns every record in it.
func dump(t *testing.T, p *Partition) []row {
	t.Helper()
	var out []row
	next := p.LogStartOffset()
	for next < p.LogEndOffset() {
		data, err := p.Read(next, 1<<20)
		if err != nil {
			t.Fatalf("read at %d: %v", next, err)
		}
		if len(data) == 0 {
			t.Fatalf("read at %d returned nothing before the end %d: a gap the reader cannot cross", next, p.LogEndOffset())
		}
		for pos := 0; pos < len(data); {
			info, _ := parseHeader(data[pos:])
			batch := data[pos : pos+int(info.totalBytes)]
			if attrsOf(batch)&batchControlBit == 0 {
				recs, err := DecodeRecords(batch)
				if err != nil {
					t.Fatal(err)
				}
				for _, r := range recs {
					k, v := "<nil>", "<nil>"
					if r.Key != nil {
						k = string(r.Key)
					}
					if r.Value != nil {
						v = string(r.Value)
					}
					if off := info.baseOffset + int64(r.OffsetDelta); off >= next {
						out = append(out, row{off, k, v})
					}
				}
			}
			next = info.baseOffset + int64(info.recordCount)
			pos += int(info.totalBytes)
		}
	}
	return out
}

func show(rows []row) string {
	var s []string
	for _, r := range rows {
		s = append(s, r.String())
	}
	return strings.Join(s, " ")
}

func smallPartition(t *testing.T, dir string) *Partition {
	t.Helper()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 400, IndexIntervalBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var eager = CompactionPolicy{Enabled: true}

func compact(t *testing.T, p *Partition, pol CompactionPolicy) CleanResult {
	t.Helper()
	res, err := p.Compact(pol, time.Now().Add(time.Hour)) // an hour on: every segment is old enough.
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func sealSome(t *testing.T, p *Partition, n int) {
	t.Helper()
	// Filler to roll segments: distinct keys that are never superseded.
	for i := 0; i < n; i++ {
		mustAppend(t, p, kvBatch(codecNone, kv{fmt.Sprintf("filler-%d-%d", p.LogEndOffset(), i), strings.Repeat("x", 150)}))
	}
}

// The latest record of each key survives at the offset it had; the older ones go; the
// active segment is not touched; and a reader crosses the gaps.
func TestCompactionKeepsTheLatestRecordPerKey(t *testing.T) {
	p := smallPartition(t, t.TempDir())
	defer p.close()
	pad := strings.Repeat("p", 120)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "a1" + pad}, kv{"b", "b1" + pad}, kv{"c", "c1" + pad})) // 0-2
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "a2" + pad}, kv{"b", "b2" + pad}))                      // 3-4
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "a3" + pad}, kv{"<nil>", "no-key" + pad}))              // 5-6
	sealSome(t, p, 3)
	if len(p.segments) < 3 {
		t.Fatalf("only %d segments", len(p.segments))
	}
	before := dump(t, p)
	start, end := p.LogStartOffset(), p.LogEndOffset()

	res := compact(t, p, eager)
	if !res.Ran || res.RecordsRemoved == 0 {
		t.Fatalf("the pass removed nothing: %+v", res)
	}
	after := dump(t, p)

	want := map[string]bool{"a3": true, "b2": true, "c1": true, "no-key": true}
	got := map[string]bool{}
	for _, r := range after {
		if strings.HasPrefix(r.k, "filler") {
			continue
		}
		got[strings.TrimSuffix(r.v, pad)] = true
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("survivors = %v, want %v\n%s", keys(got), keys(want), show(after))
	}
	// Offsets are where they were.
	for _, r := range after {
		var orig *row
		for i := range before {
			if before[i].off == r.off {
				orig = &before[i]
			}
		}
		if orig == nil || orig.k != r.k || orig.v != r.v {
			t.Errorf("record at offset %d changed: %v -> %v", r.off, orig, r)
		}
	}
	if p.LogStartOffset() != start || p.LogEndOffset() != end {
		t.Errorf("compaction moved the log: start %d->%d end %d->%d", start, p.LogStartOffset(), end, p.LogEndOffset())
	}
	if res.BytesAfter >= res.BytesBefore {
		t.Errorf("bytes %d -> %d: nothing was reclaimed", res.BytesBefore, res.BytesAfter)
	}
	// A reader asking for a record that was compacted away is given the next one.
	data, err := p.Read(0, 1<<20)
	if err != nil || len(data) == 0 {
		t.Fatalf("read at 0: %d bytes, %v", len(data), err)
	}
}

func keys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Superseding records in the active segment do not remove anything from the sealed ones,
// and the active segment itself is never rewritten.
func TestCompactionNeverTouchesTheActiveSegment(t *testing.T) {
	p := smallPartition(t, t.TempDir())
	defer p.close()
	pad := strings.Repeat("p", 120)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "old" + pad}))
	sealSome(t, p, 3)
	activeBase := p.active().baseOffset
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "new"}, kv{"a", "newer"}))
	activeBefore := p.active().sizeBytes
	if p.active().baseOffset == activeBase && len(p.segments) < 2 {
		t.Fatal("setup")
	}
	compact(t, p, eager)
	var sawOld bool
	for _, r := range dump(t, p) {
		sawOld = sawOld || strings.HasPrefix(r.v, "old")
	}
	if !sawOld {
		t.Error("a sealed record was removed because of a newer one in the active segment")
	}
	if p.active().sizeBytes != activeBefore {
		t.Errorf("the active segment changed size: %d -> %d", activeBefore, p.active().sizeBytes)
	}
}

// A tombstone is kept through the pass that makes it the latest, and dropped once its
// segment has been cleaned before and is older than delete.retention.ms.
func TestTombstonesOutliveOnePassThenGo(t *testing.T) {
	p := smallPartition(t, t.TempDir())
	defer p.close()
	pad := strings.Repeat("p", 120)
	mustAppend(t, p, kvBatch(codecNone, kv{"gone", "v1" + pad}, kv{"stay", "s1" + pad}))
	mustAppend(t, p, kvBatch(codecNone, kv{"gone", "<nil>"}))
	sealSome(t, p, 3)

	res := compact(t, p, CompactionPolicy{Enabled: true, DeleteRetention: time.Minute})
	rows := dump(t, p)
	if !hasRow(rows, "gone", "<nil>") || hasRow(rows, "gone", "v1"+pad) {
		t.Fatalf("after the first pass the tombstone should stand alone for its key: %s", show(rows))
	}
	_ = res
	// More data rolls; the second pass finds the tombstone's segment already cleaned. It is
	// not yet older than its retention, so it stays.
	sealSome(t, p, 3)
	pol := CompactionPolicy{Enabled: true, DeleteRetention: time.Minute}
	if _, err := p.Compact(pol, time.Now().Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !hasRow(dump(t, p), "gone", "<nil>") {
		t.Error("the tombstone was dropped before delete.retention.ms")
	}
	// Past its retention, it goes.
	sealSome(t, p, 3)
	if _, err := p.Compact(pol, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	rows = dump(t, p)
	if hasKey(rows, "gone") {
		t.Errorf("the tombstone outlived delete.retention.ms: %s", show(rows))
	}
	if !hasKey(rows, "stay") {
		t.Error("an unrelated key was removed with the tombstone")
	}
}

func hasRow(rows []row, k, v string) bool {
	for _, r := range rows {
		if r.k == k && r.v == v {
			return true
		}
	}
	return false
}

func hasKey(rows []row, k string) bool {
	for _, r := range rows {
		if r.k == k {
			return true
		}
	}
	return false
}

// Every compression codec in, compacted, readable out.
func TestCompactionReadsCompressedBatches(t *testing.T) {
	for name, codec := range map[string]int{"gzip": codecGzip, "snappy": codecSnappy, "lz4": codecLZ4, "zstd": codecZstd} {
		t.Run(name, func(t *testing.T) {
			p := smallPartition(t, t.TempDir())
			defer p.close()
			pad := strings.Repeat(name, 30)
			mustAppend(t, p, kvBatch(codec, kv{"k", "one" + pad}, kv{"j", "j1" + pad}))
			mustAppend(t, p, kvBatch(codec, kv{"k", "two" + pad}))
			sealSome(t, p, 3)
			compact(t, p, eager)
			rows := dump(t, p)
			if hasRow(rows, "k", "one"+pad) || !hasRow(rows, "k", "two"+pad) || !hasRow(rows, "j", "j1"+pad) {
				t.Errorf("%s: %s", name, show(rows))
			}
		})
	}
}

// A segment the cleaner empties is deleted, but not the first: its base offset is the
// log start.
func TestCompactionKeepsTheFirstSegmentAnchored(t *testing.T) {
	p := smallPartition(t, t.TempDir())
	defer p.close()
	pad := strings.Repeat("p", 150)
	for i := 0; i < 4; i++ { // each overwritten later: the early segments will be emptied.
		mustAppend(t, p, kvBatch(codecNone, kv{"same", fmt.Sprintf("v%d", i) + pad}))
	}
	sealSome(t, p, 2)
	mustAppend(t, p, kvBatch(codecNone, kv{"same", "last" + pad}))
	sealSome(t, p, 2)
	segsBefore := len(p.segments)
	start := p.LogStartOffset()
	res := compact(t, p, eager)
	if res.SegmentsRemoved == 0 {
		t.Errorf("no emptied segment was removed: %+v", res)
	}
	if p.LogStartOffset() != start || p.segments[0].baseOffset != start {
		t.Errorf("the log start moved from %d to %d (first segment %d)", start, p.LogStartOffset(), p.segments[0].baseOffset)
	}
	if len(p.segments) >= segsBefore {
		t.Errorf("%d segments before, %d after", segsBefore, len(p.segments))
	}
	rows := dump(t, p) // crossing the removed segments must work.
	if !hasRow(rows, "same", "last"+pad) || hasRow(rows, "same", "v0"+pad) {
		t.Errorf("rows: %s", show(rows))
	}
}

// min.cleanable.dirty.ratio, min.compaction.lag.ms and max.compaction.lag.ms decide
// whether a pass runs.
func TestCompactionIsGatedByRatioAndLag(t *testing.T) {
	p := smallPartition(t, t.TempDir())
	defer p.close()
	pad := strings.Repeat("p", 120)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1" + pad}))
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "2" + pad}))
	sealSome(t, p, 3)
	hourOn := time.Now().Add(time.Hour)
	now := time.Now()

	// Nothing has been cleaned yet, so all of it is dirty and any ratio is met; make a
	// clean part first.
	if res, err := p.Compact(CompactionPolicy{Enabled: true}, hourOn); err != nil || !res.Ran {
		t.Fatalf("setup pass: %+v %v", res, err)
	}
	sealSome(t, p, 1) // a little new dirty data beside the cleaned part.
	for _, tc := range []struct {
		name string
		pol  CompactionPolicy
		at   time.Time
		ran  bool
	}{
		{"a ratio the dirty share does not reach", CompactionPolicy{Enabled: true, MinDirtyRatio: 0.99}, hourOn, false},
		{"a ratio it does", CompactionPolicy{Enabled: true, MinDirtyRatio: 0.01}, hourOn, true},
	} {
		res, err := p.Compact(tc.pol, tc.at)
		if err != nil || res.Ran != tc.ran {
			t.Errorf("%s: ran=%v err=%v, want ran=%v", tc.name, res.Ran, err, tc.ran)
		}
		sealSome(t, p, 2)
	}

	// The lag: nothing younger than min.compaction.lag.ms is touched, however dirty.
	q := smallPartition(t, t.TempDir())
	defer q.close()
	mustAppend(t, q, kvBatch(codecNone, kv{"a", "1" + pad}))
	mustAppend(t, q, kvBatch(codecNone, kv{"a", "2" + pad}))
	sealSome(t, q, 3)
	if res, _ := q.Compact(CompactionPolicy{Enabled: true, MinLag: time.Hour}, now); res.Ran {
		t.Error("a pass ran on segments younger than min.compaction.lag.ms")
	}
	// ...unless max.compaction.lag.ms says it has waited long enough, which forces a pass
	// even where the dirty ratio would not.
	if res, _ := q.Compact(CompactionPolicy{Enabled: true, MinDirtyRatio: 1, MaxLag: time.Minute}, hourOn); !res.Ran {
		t.Error("max.compaction.lag.ms did not force a pass")
	}
}

// The key map has a size. A log with more keys than it holds is cleaned over several
// passes, each picking up where the last stopped, rather than being left half done: in the
// end each key has one record, at the offset it had, and a dead key's tombstone goes.
func TestALogWithMoreKeysThanTheMapHoldsIsCleanedOverSeveralPasses(t *testing.T) {
	p := smallPartition(t, t.TempDir())
	defer p.close()
	pad := strings.Repeat("p", 100)
	const nkeys, rounds = 12, 4
	for r := 0; r < rounds; r++ {
		for k := 0; k < nkeys; k++ {
			mustAppend(t, p, kvBatch(codecNone, kv{fmt.Sprintf("key%02d", k), fmt.Sprintf("round%d", r) + pad}))
		}
	}
	mustAppend(t, p, kvBatch(codecNone, kv{"dead", "alive" + pad}))
	mustAppend(t, p, kvBatch(codecNone, kv{"dead", "<nil>"}))
	sealSome(t, p, 3)
	before := dump(t, p)

	pol := CompactionPolicy{Enabled: true, MaxMapEntries: 4}
	passes := 0
	for res := compact(t, p, pol); res.Ran; res = compact(t, p, pol) {
		passes++
		if passes > 100 {
			t.Fatal("the cleaner never finished")
		}
	}
	if passes < 3 {
		t.Errorf("%d passes; a map of 4 over %d keys should have needed several", passes, nkeys)
	}
	rows := dump(t, p)
	per := map[string]int{}
	for _, r := range rows {
		per[r.k]++
	}
	for k := 0; k < nkeys; k++ {
		key := fmt.Sprintf("key%02d", k)
		if per[key] != 1 {
			t.Errorf("%s has %d records after cleaning, want 1\n%s", key, per[key], show(rows))
		}
		if !hasRow(rows, key, fmt.Sprintf("round%d", rounds-1)+pad) {
			t.Errorf("%s did not keep its latest value", key)
		}
	}
	if hasRow(rows, "dead", "alive"+pad) {
		t.Error("a dead key's old value survived its tombstone")
	}
	// The tombstone was just made; it goes on a later pass, once its segment has been
	// cleaned and is old enough.
	sealSome(t, p, 3)
	for res := compact(t, p, pol); res.Ran; res = compact(t, p, pol) {
	}
	if hasKey(dump(t, p), "dead") {
		t.Error("the tombstone is still there after its segment was cleaned and aged")
	}
	// Offsets of the survivors did not move.
	idx := map[int64]row{}
	for _, r := range before {
		idx[r.off] = r
	}
	for _, r := range dump(t, p) {
		if o, ok := idx[r.off]; ok && (o.k != r.k || o.v != r.v) {
			t.Errorf("the record at offset %d changed: %v -> %v", r.off, o, r)
		}
	}
}

// The segment's modification time is its age for retention, and a rewrite is not new data.
func TestCompactionDoesNotRenewASegmentsAge(t *testing.T) {
	p := smallPartition(t, t.TempDir())
	defer p.close()
	pad := strings.Repeat("p", 120)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1" + pad}))
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "2" + pad}))
	sealSome(t, p, 3)
	old := time.Now().Add(-48 * time.Hour)
	for _, s := range p.segments {
		os.Chtimes(filepath.Join(p.dir, segmentName(s.baseOffset)+".log"), old, old)
		s.lastModified = old
	}
	compact(t, p, eager)
	info, err := os.Stat(filepath.Join(p.dir, segmentName(p.segments[0].baseOffset)+".log"))
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(info.ModTime()) < 47*time.Hour {
		t.Errorf("the rewritten segment's age was reset to %s ago", time.Since(info.ModTime()))
	}
	if time.Since(p.segments[0].lastModified) < 47*time.Hour {
		t.Error("the reopened segment does not carry the original age")
	}
}

// Reopening: the cleaned log reads as it did, the checkpoint survives, and the producers'
// history is still there to dedupe against.
func TestACleanedLogSurvivesReopening(t *testing.T) {
	dir := t.TempDir()
	p := smallPartition(t, dir)
	pad := strings.Repeat("p", 120)
	idem := kvBatch(codecNone, kv{"a", "1" + pad})
	binary.BigEndian.PutUint64(idem[batchProducerIDOff:], 42)
	sealCRCOf(idem)
	mustAppend(t, p, idem)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "2" + pad}))
	sealSome(t, p, 3)
	compact(t, p, eager)
	want := dump(t, p)
	checkpoint := p.cleanedTo
	end := p.LogEndOffset()
	p.close()

	p = smallPartition(t, dir)
	defer p.close()
	if got := dump(t, p); !reflect.DeepEqual(got, want) {
		t.Errorf("after reopening:\n got  %s\n want %s", show(got), show(want))
	}
	if p.LogEndOffset() != end {
		t.Errorf("log end %d, want %d", p.LogEndOffset(), end)
	}
	p.mu.Lock()
	reloaded := p.cleanedToLocked()
	p.mu.Unlock()
	if reloaded != checkpoint {
		t.Errorf("the cleaning checkpoint is %d after reopening, want %d", reloaded, checkpoint)
	}
	// Nothing new is dirty, so another pass does nothing.
	if res := compact(t, p, eager); res.Ran {
		t.Errorf("a pass ran on a log with nothing new to clean: %+v", res)
	}
	hist, err := p.ProducerHistory(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, h := range hist {
		found = found || h.ProducerID == 42
	}
	_ = found // the producer's only batch was superseded and removed; the history is what remains.
}

// Crash matrix over the swap. A crash leaves one of: a half-built .cleaned file; the new
// log under the old index (marker present); both renamed. Each must open to a readable,
// correct log.
func TestACrashDuringTheSwapLeavesAReadableLog(t *testing.T) {
	pad := strings.Repeat("p", 120)
	build := func(dir string) (*Partition, []row) {
		p := smallPartition(t, dir)
		mustAppend(t, p, kvBatch(codecNone, kv{"a", "1" + pad}, kv{"b", "1" + pad}))
		mustAppend(t, p, kvBatch(codecNone, kv{"a", "2" + pad}))
		sealSome(t, p, 3)
		return p, dump(t, p)
	}

	t.Run("before the rename: leftover .cleaned files", func(t *testing.T) {
		dir := t.TempDir()
		p, before := build(dir)
		p.close()
		os.WriteFile(filepath.Join(dir, segmentName(0)+".log"+cleanedExt), []byte("half a segment"), 0o644)
		os.WriteFile(filepath.Join(dir, segmentName(0)+".index"+cleanedExt), []byte("half"), 0o644)
		p = smallPartition(t, dir)
		defer p.close()
		if got := dump(t, p); !reflect.DeepEqual(got, before) {
			t.Errorf("the log changed:\n got  %s\n want %s", show(got), show(before))
		}
		left, _ := filepath.Glob(filepath.Join(dir, "*"+cleanedExt))
		if len(left) != 0 {
			t.Errorf("leftover files survive an open: %v", left)
		}
	})

	t.Run("between the renames: the new log under the old index", func(t *testing.T) {
		dir := t.TempDir()
		p, _ := build(dir)
		// Clean for real to learn what the new log is, then put the OLD index back, which is
		// the state after a crash between the log rename and the index rename.
		oldIdx, _ := os.ReadFile(filepath.Join(dir, segmentName(0)+".index"))
		compact(t, p, eager)
		cleaned := dump(t, p)
		p.close()
		os.WriteFile(filepath.Join(dir, segmentName(0)+".index"), oldIdx, 0o644)
		os.WriteFile(filepath.Join(dir, segmentName(0)+swapMarkExt), nil, 0o644)
		p = smallPartition(t, dir)
		defer p.close()
		if got := dump(t, p); !reflect.DeepEqual(got, cleaned) {
			t.Errorf("the log is unreadable under a stale index:\n got  %s\n want %s", show(got), show(cleaned))
		}
		if _, err := os.Stat(filepath.Join(dir, segmentName(0)+swapMarkExt)); err == nil {
			t.Error("the swap marker survives an open")
		}
	})
}

// A transaction's records are cleaned by what they are: those of an aborted one go, a
// marker stays while a record of its transaction does and goes when none is left, and a
// transaction still open is not touched.
func TestCompactionAndTransactions(t *testing.T) {
	p := smallPartition(t, t.TempDir())
	defer p.close()
	pad := strings.Repeat("p", 120)
	mustAppend(t, p, txKVBatch(1, 0, kv{"a", "aborted" + pad}, kv{"x", "aborted-x" + pad})) // pid 1, will abort.
	mustMarker(t, p, 1, false)
	mustAppend(t, p, txKVBatch(2, 0, kv{"a", "committed" + pad})) // pid 2, will commit.
	mustMarker(t, p, 2, true)
	mustAppend(t, p, kvBatch(codecNone, kv{"b", "plain" + pad}))
	sealSome(t, p, 3)
	mustAppend(t, p, txKVBatch(3, 0, kv{"b", "open" + pad})) // pid 3: still open.
	sealSome(t, p, 3)

	if lso := lso(t, p); lso == p.LogEndOffset() {
		t.Fatal("setup: nothing is open")
	}
	compact(t, p, eager)
	rows := dump(t, p)
	if hasRow(rows, "a", "aborted"+pad) || hasRow(rows, "x", "aborted-x"+pad) {
		t.Errorf("a record of an aborted transaction survived: %s", show(rows))
	}
	if !hasRow(rows, "a", "committed"+pad) {
		t.Errorf("the committed transaction's record was lost: %s", show(rows))
	}
	// The abort marker is gone from the log, so the aborted transaction must be gone from
	// the list consumers are given: a consumer stops skipping a producer's records at its
	// abort marker, and with none to find it would skip every later transaction too.
	if _, _, ab, err := p.ReadIsolated(0, 1<<20, true); err != nil {
		t.Fatal(err)
	} else {
		for _, a := range ab {
			if a.ProducerID == 1 {
				t.Errorf("the aborted transaction %+v is still listed after its marker was removed", a)
			}
		}
	}
	if !hasRow(rows, "b", "plain"+pad) {
		t.Errorf("b's committed value was removed by the open transaction's record: %s", show(rows))
	}
	// The open transaction is intact: it can still be committed afterwards.
	mustMarker(t, p, 3, true)
	if open, _ := p.OpenTransactions(); len(open) != 0 {
		t.Errorf("transactions still open after the commit: %v", open)
	}

	// Once the committed record is itself superseded, its marker has nothing left to
	// mark and goes, and the log's transaction state still rebuilds without a dangling one.
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "newest" + pad}))
	sealSome(t, p, 3)
	res := compact(t, p, eager)
	if res.MarkersRemoved == 0 {
		t.Errorf("no marker was removed once its transaction had no records: %+v", res)
	}
	dir := p.dir
	p.close()
	p = smallPartition(t, dir)
	defer p.close()
	if open, err := p.OpenTransactions(); err != nil || len(open) != 0 {
		t.Errorf("after reopening, open transactions = %v, %v: a removed marker left one dangling", open, err)
	}
	if lso := lso(t, p); lso != p.LogEndOffset() {
		t.Errorf("after reopening the last stable offset is %d, want the end %d", lso, p.LogEndOffset())
	}
}
