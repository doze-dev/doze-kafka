package log

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Tests that pin the cleaner's, the swap's and the retention sweep's behaviour at the
// edges (see plan/evidence/mutation-survivors.md). Each reads back what is on disk.

// stampBase sets a batch's base offset (the CRC does not cover it).
func stampBase(b []byte, base int64) []byte {
	binary.BigEndian.PutUint64(b[batchBaseOffsetOff:], uint64(base))
	return b
}

// rawSegment writes the batches back to back into a segment's log file as they are
// (no recovery, no truncation) and opens it.
func rawSegment(t *testing.T, dir string, base int64, tail []byte, batches ...[]byte) *Segment {
	t.Helper()
	var raw []byte
	for _, b := range batches {
		raw = append(raw, b...)
	}
	raw = append(raw, tail...)
	if err := os.WriteFile(filepath.Join(dir, segmentName(base)+".log"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	s, _, err := openSegment(dir, base, 64, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.close() })
	return s
}

func readIndexFileB(t *testing.T, path string) []indexEntry {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw)%indexEntryBytes != 0 {
		t.Fatalf("index file is %d bytes, not a whole number of entries", len(raw))
	}
	var out []indexEntry
	for i := 0; i < len(raw); i += indexEntryBytes {
		out = append(out, indexEntry{int64(binary.BigEndian.Uint64(raw[i:])), int64(binary.BigEndian.Uint64(raw[i+8:]))})
	}
	return out
}

func TestB_AbortedSetCoversBothEnds(t *testing.T) {
	s := newAbortedSet([]AbortedTxn{{ProducerID: 1, FirstOffset: 5, LastOffset: 9}})
	for _, c := range []struct {
		pid, off int64
		want     bool
	}{{1, 4, false}, {1, 5, true}, {1, 7, true}, {1, 9, true}, {1, 10, false}, {2, 7, false}} {
		if got := s.covers(c.pid, c.off); got != c.want {
			t.Errorf("covers(%d, %d) = %v, want %v", c.pid, c.off, got, c.want)
		}
	}
}

// A payload whose second batch is cut short is judged by the whole batches before it.
func TestB_CheckRecordsStopsAtATornSecondBatch(t *testing.T) {
	keyed := kvBatch(codecNone, kv{"a", "1"})
	torn := kvBatch(codecNone, kv{"b", "2"})
	torn = torn[:len(torn)-3]
	exact := func(parts ...[]byte) []byte { // capacity is length: reading past the end is a panic.
		var b []byte
		for _, p := range parts {
			b = append(b, p...)
		}
		return b[:len(b):len(b)]
	}
	if err := CheckRecords(exact(keyed, torn), RecordCheck{RequireKeys: true}); err != nil {
		t.Fatalf("a whole keyed batch and a torn one: %v", err)
	}
	// The null key in the first batch is found even with the torn one behind it.
	nokey := kvBatch(codecNone, kv{"<nil>", "1"})
	if err := CheckRecords(exact(nokey, torn), RecordCheck{RequireKeys: true}); !errors.Is(err, ErrNoKey) {
		t.Fatalf("a null key before a torn batch: %v, want ErrNoKey", err)
	}
	// A keyless batch is found in the second position, past the first.
	if err := CheckRecords(exact(keyed, nokey), RecordCheck{RequireKeys: true}); !errors.Is(err, ErrNoKey) {
		t.Fatalf("a null key in the second batch: %v, want ErrNoKey", err)
	}
}

// A batch that is only a header is a whole batch: its count says one record and there is
// none, which is refused.
func TestB_CheckRecordsLooksAtABatchOfExactlyAHeader(t *testing.T) {
	b := makeBatch(1, nil)
	if len(b) != batchFullHeaderBytes {
		t.Fatalf("%d bytes", len(b))
	}
	if err := CheckRecords(b, RecordCheck{RequireKeys: true}); !errors.Is(err, ErrBadRecords) {
		t.Errorf("a header whose one record is missing: %v, want ErrBadRecords", err)
	}
}

// The topic's stored-size limit is the max.message.bytes less the 61 byte header, and a
// batch that inflates to exactly that is stored.
func TestB_CheckRecordsStoredSizeLimitIsMaxStoredLessTheHeader(t *testing.T) {
	const value = 5000
	body := len(EncodeRecords([]Record{{Key: []byte("k"), Value: make([]byte, value)}}))
	b := bombBatch(t, codecGzip, 0, value)
	if err := CheckRecords(b, RecordCheck{MaxStored: int64(batchFullHeaderBytes + body)}); err != nil {
		t.Errorf("a batch that stores at exactly the limit: %v", err)
	}
	if err := CheckRecords(b, RecordCheck{MaxStored: int64(batchFullHeaderBytes + body - 1)}); !errors.Is(err, ErrStoredTooLarge) {
		t.Errorf("a batch one byte over the limit: %v, want ErrStoredTooLarge", err)
	}
	if err := CheckRecords(b, RecordCheck{MaxStored: int64(batchFullHeaderBytes + body + 200)}); err != nil {
		t.Errorf("a batch under the limit: %v", err)
	}
	// A limit smaller than the header leaves no room at all.
	if err := CheckRecords(b, RecordCheck{MaxStored: 10}); !errors.Is(err, ErrStoredTooLarge) {
		t.Errorf("a limit below the header size: %v, want ErrStoredTooLarge", err)
	}
}

// Past the ceiling of what the broker will inflate, the error is the ceiling's even when
// the topic's limit is at (or just past) it: the topic's limit is the smaller of the two
// only when it is smaller.
func TestB_CheckRecordsNamesTheCeilingWhenTheTopicLimitIsNotBelowIt(t *testing.T) {
	big := bombBatch(t, codecGzip, 0, MaxInflatedBytes+1<<20)
	for _, max := range []int64{MaxInflatedBytes + batchFullHeaderBytes, MaxInflatedBytes + batchFullHeaderBytes + 1<<20, MaxInflatedBytes + 100} {
		if err := CheckRecords(big, RecordCheck{MaxStored: max}); !errors.Is(err, ErrInflatedTooLarge) || errors.Is(err, ErrStoredTooLarge) {
			t.Errorf("MaxStored %d: %v, want ErrInflatedTooLarge and not ErrStoredTooLarge", max, err)
		}
	}
	// One byte under, the topic's limit is the smaller and is what is named.
	if err := CheckRecords(big, RecordCheck{MaxStored: MaxInflatedBytes + batchFullHeaderBytes - 1}); !errors.Is(err, ErrStoredTooLarge) {
		t.Errorf("MaxStored just below the ceiling: %v, want ErrStoredTooLarge", err)
	}
}

// batchesOf visits every whole batch of a segment, including one that ends exactly at the
// end of the file, and stops quietly at a tail that is not a whole batch.
func TestB_BatchesOfVisitsWholeBatchesAndStopsAtTornTails(t *testing.T) {
	whole := func() [][]byte {
		return [][]byte{stampBase(makeBatch(1, make([]byte, 40)), 0), stampBase(makeBatch(1, make([]byte, 90)), 1), stampBase(makeBatch(1, nil), 2)}
	}
	visit := func(s *Segment) (bases []int64, err error) {
		err = batchesOf(s, func(pos int64, batch []byte, info batchInfo) error {
			if int64(len(batch)) != info.totalBytes {
				t.Errorf("batch at %d is %d bytes, header says %d", pos, len(batch), info.totalBytes)
			}
			bases = append(bases, info.baseOffset)
			return nil
		})
		return
	}
	// The last batch is only the 61 byte header and ends exactly at the file's end.
	got, err := visit(rawSegment(t, t.TempDir(), 0, nil, whole()...))
	if err != nil || !reflect.DeepEqual(got, []int64{0, 1, 2}) {
		t.Fatalf("whole batches: %v, %v", got, err)
	}
	// Fewer than a header's bytes after the last batch.
	got, err = visit(rawSegment(t, t.TempDir(), 0, make([]byte, 20), whole()...))
	if err != nil || !reflect.DeepEqual(got, []int64{0, 1, 2}) {
		t.Fatalf("a 20 byte tail: %v, %v", got, err)
	}
	// A header that says more than is left of the file.
	cut := stampBase(makeBatch(1, make([]byte, 300)), 3)[:150]
	got, err = visit(rawSegment(t, t.TempDir(), 0, cut, whole()...))
	if err != nil || !reflect.DeepEqual(got, []int64{0, 1, 2}) {
		t.Fatalf("a batch cut short: %v, %v", got, err)
	}
	// A whole header whose length cannot even cover the header: not a batch.
	bad := make([]byte, 80)
	got, err = visit(rawSegment(t, t.TempDir(), 0, bad, whole()[:1]...))
	if err != nil || !reflect.DeepEqual(got, []int64{0}) {
		t.Fatalf("a zeroed tail: %v, %v", got, err)
	}
}

// keysOf maps each key to the offset of its last record (base offset plus the record's
// delta), and leaves out controls, aborted batches and null keys.
func TestB_KeysOfGivesTheLatestOffsetOfEachKey(t *testing.T) {
	dir := t.TempDir()
	plain := stampBase(kvBatch(codecNone, kv{"a", "1"}, kv{"b", "2"}, kv{"<nil>", "3"}, kv{"a", "4"}), 10)
	tx := stampBase(txKVBatch(7, 0, kv{"t", "x"}, kv{"u", "y"}), 20)
	tx2 := stampBase(txKVBatch(8, 0, kv{"live", "x"}), 30)
	s := rawSegment(t, dir, 10, nil, plain, tx, tx2)

	got, err := keysOf(s, newAbortedSet([]AbortedTxn{{ProducerID: 7, FirstOffset: 20, LastOffset: 25}}))
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64]int64{hashKey([]byte("a")): 13, hashKey([]byte("b")): 11, hashKey([]byte("live")): 30}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keysOf = %v, want %v", got, want)
	}
	// Nothing aborted: the transactional keys are there too.
	got, _ = keysOf(s, newAbortedSet(nil))
	if got[hashKey([]byte("t"))] != 20 || got[hashKey([]byte("u"))] != 21 {
		t.Fatalf("without an abort the transactional keys are missing: %v", got)
	}
}

// rebuildIndex writes an index from the log alone: the first batch, then one entry each
// time a whole index interval of log has passed, and nothing for a batch that is not
// whole.
func TestB_RebuildIndexFromTheLog(t *testing.T) {
	const interval = defaultIndexRebuildInterval
	dir := t.TempDir()
	base := filepath.Join(dir, segmentName(0))
	write := func(batches ...[]byte) {
		t.Helper()
		var raw []byte
		for _, b := range batches {
			raw = append(raw, b...)
		}
		if err := os.WriteFile(base+".log", raw, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(base+".index", []byte("stale"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := rebuildIndex(base); err != nil {
			t.Fatal(err)
		}
	}
	check := func(what string, want []indexEntry) {
		t.Helper()
		if got := readIndexFileB(t, base+".index"); !(len(got) == 0 && len(want) == 0) && !reflect.DeepEqual(got, want) {
			t.Errorf("%s: index = %v, want %v", what, got, want)
		}
	}

	// A single batch of just a header is whole: it has an entry.
	write(stampBase(makeBatch(1, nil), 5))
	check("one header-only batch", []indexEntry{{5, 0}})

	// A first batch of exactly one interval: the second is the first at or past it.
	first := stampBase(makeBatch(1, make([]byte, interval-batchFullHeaderBytes)), 0)
	if len(first) != interval {
		t.Fatalf("first batch is %d bytes", len(first))
	}
	write(first, stampBase(makeBatch(1, make([]byte, 100)), 1), stampBase(makeBatch(1, make([]byte, 100)), 2))
	check("an interval then two small ones", []indexEntry{{0, 0}, {1, interval}})

	// One byte short of the interval: the second batch has no entry, the third has.
	short := stampBase(makeBatch(1, make([]byte, interval-batchFullHeaderBytes-1)), 0)
	write(short, stampBase(makeBatch(1, make([]byte, 100)), 1), stampBase(makeBatch(1, make([]byte, 100)), 2))
	check("an interval less a byte", []indexEntry{{0, 0}, {2, int64(len(short)) + 161}})

	// A last batch cut short is not indexed, and the whole ones before it are.
	torn := stampBase(makeBatch(1, make([]byte, 300)), 1)[:200]
	write(first, torn)
	check("a torn last batch", []indexEntry{{0, 0}})
	write(torn)
	check("only a torn batch", nil)
	// A tail shorter than a header is ignored.
	write(first, make([]byte, 30))
	check("a short tail", []indexEntry{{0, 0}})

	// No log, no index and no error.
	if err := os.Remove(base + ".log"); err != nil {
		t.Fatal(err)
	}
	os.Remove(base + ".index")
	if err := rebuildIndex(base); err != nil {
		t.Fatalf("rebuildIndex with no log: %v", err)
	}
	if _, err := os.Stat(base + ".index"); !os.IsNotExist(err) {
		t.Errorf("an index was written for a log that is not there: %v", err)
	}
}

// The next open finishes every swap a crash left and removes every leftover cleaned
// file, whatever order the directory lists them in.
func TestB_RecoverSwapsHandlesEveryMarkerAndLeftover(t *testing.T) {
	dir := t.TempDir()
	for _, base := range []int64{0, 10, 20} {
		raw := append(stampBase(makeBatch(1, make([]byte, 30)), base), stampBase(makeBatch(1, make([]byte, 30)), base+1)...)
		name := filepath.Join(dir, segmentName(base))
		if err := os.WriteFile(name+".log", raw, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name+".index", []byte("not the index of this log"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, base := range []int64{0, 10} {
		if err := os.WriteFile(filepath.Join(dir, segmentName(base)+swapMarkExt), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	leftovers := []string{segmentName(5) + ".log" + cleanedExt, segmentName(30) + ".log" + cleanedExt, segmentName(30) + cleanerIndex + cleanedExt, "x.tmp"}
	for _, n := range leftovers {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := recoverSwaps(dir); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	for _, n := range names {
		if strings.HasSuffix(n, swapMarkExt) || strings.HasSuffix(n, cleanedExt) || strings.HasSuffix(n, ".tmp") {
			t.Errorf("%s was left behind by recoverSwaps (dir: %v)", n, names)
		}
	}
	for _, base := range []int64{0, 10} {
		want := []indexEntry{{base, 0}}
		if got := readIndexFileB(t, filepath.Join(dir, segmentName(base)+".index")); !reflect.DeepEqual(got, want) {
			t.Errorf("index of %d = %v, want %v", base, got, want)
		}
	}
	if got, _ := os.ReadFile(filepath.Join(dir, segmentName(20)+".index")); string(got) != "not the index of this log" {
		t.Errorf("an index with no marker was rewritten: %q", got)
	}
}
