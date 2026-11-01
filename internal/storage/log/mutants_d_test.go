package log

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// dBatch is a record-batch-v2 with the header fields a test names and a payload
// that is not decoded.
func dBatch(pid int64, epoch int16, seq, count int32, attrs uint16, maxTs int64, payload []byte) []byte {
	buf := make([]byte, batchFullHeaderBytes+len(payload))
	binary.BigEndian.PutUint32(buf[batchLengthOff:], uint32(len(buf)-batchOverhead))
	buf[batchMagicOff] = batchMagicV2
	binary.BigEndian.PutUint16(buf[batchAttributesOff:], attrs)
	binary.BigEndian.PutUint32(buf[batchLastOffsetDeltaOff:], uint32(count-1))
	binary.BigEndian.PutUint64(buf[batchFirstTimestampOff:], uint64(maxTs))
	binary.BigEndian.PutUint64(buf[batchMaxTimestampOff:], uint64(maxTs))
	binary.BigEndian.PutUint64(buf[batchProducerIDOff:], uint64(pid))
	binary.BigEndian.PutUint16(buf[batchProducerEpochOff:], uint16(epoch))
	binary.BigEndian.PutUint32(buf[batchBaseSequenceOff:], uint32(seq))
	binary.BigEndian.PutUint32(buf[batchRecordCountOff:], uint32(count))
	copy(buf[batchFullHeaderBytes:], payload)
	sealCRC(buf)
	return buf
}

func dOpenBig(t *testing.T, dir string) *Partition {
	t.Helper()
	p, err := openPartition(dir, ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestD_MaxTimestampOfNeedsAWholeHeader(t *testing.T) {
	b := dBatch(-1, 0, -1, 1, 0, 777, nil)
	if len(b) != batchFullHeaderBytes {
		t.Fatal(len(b))
	}
	if got := MaxTimestampOf(b); got != 777 {
		t.Errorf("MaxTimestampOf(61-byte header) = %d, want 777", got)
	}
	if got := MaxTimestampOf(b[:batchFullHeaderBytes-1]); got != -1 {
		t.Errorf("MaxTimestampOf(60 bytes) = %d, want -1", got)
	}
}

// The producer history reports each idempotent batch with its sequence range, epoch,
// offset and stored maximum timestamp, skips what carries no sequence, and survives a
// reopen; the last batch is read even when it is a bare header ending the segment.
func TestD_ProducerHistoryListsIdempotentBatches(t *testing.T) {
	dir := t.TempDir()
	p := dOpenBig(t, dir)
	pay := []byte("payload")
	mustAppend(t, p, dBatch(7, 3, 10, 4, 0, 1234, pay))           // 0-3
	mustAppend(t, p, dBatch(0, 0, 0, 1, 0, 50, pay))              // 4: pid 0 and seq 0 count
	mustAppend(t, p, dBatch(-1, 0, -1, 1, 0, 60, pay))            // 5: not idempotent
	mustAppend(t, p, dBatch(9, 1, -1, 1, 0, 70, pay))             // 6: no sequence
	mustAppend(t, p, dBatch(11, 2, math.MaxInt32, 3, 0, 80, pay)) // 7-9: the sequence wraps
	mustAppend(t, p, txBatch(8, 5, 0))                            // 10: a transaction's batch counts
	if _, wrote := mustMarker(t, p, 8, true); !wrote {            // 11: its marker does not
		t.Fatal("no marker written")
	}
	mustAppend(t, p, dBatch(12, 4, 5, 1, uint16(codecGzip), 90, nil)) // 12: a bare header at the end

	want := []ProducerBatch{
		{ProducerID: 7, Epoch: 3, FirstSeq: 10, LastSeq: 13, BaseOffset: 0, MaxTimestamp: 1234},
		{ProducerID: 0, Epoch: 0, FirstSeq: 0, LastSeq: 0, BaseOffset: 4, MaxTimestamp: 50},
		{ProducerID: 11, Epoch: 2, FirstSeq: math.MaxInt32, LastSeq: 1, BaseOffset: 7, MaxTimestamp: 80},
		{ProducerID: 8, Epoch: 5, FirstSeq: 0, LastSeq: 0, BaseOffset: 10, MaxTimestamp: 0},
		{ProducerID: 12, Epoch: 4, FirstSeq: 5, LastSeq: 5, BaseOffset: 12, MaxTimestamp: 90},
	}
	got, err := p.ProducerHistory(1 << 20)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("history = %+v, %v\nwant %+v", got, err, want)
	}
	p.close()
	p = dOpenBig(t, dir)
	defer p.close()
	got, err = p.ProducerHistory(1 << 20)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("history after reopen = %+v, %v\nwant %+v", got, err, want)
	}
}

// The history reaches back over whole segments: the newest is always read, and an older
// one only while the bytes read so far are below the limit.
func TestD_ProducerHistoryWalksBackOverSegmentsWithinTheLimit(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	for i := range 9 {
		mustAppend(t, p, dBatch(1, 0, int32(i), 1, 0, int64(i), make([]byte, 166))) // 227 bytes: three to a segment.
	}
	if len(p.segments) < 3 {
		t.Fatalf("%d segments, want at least 3", len(p.segments))
	}
	// Sequence i is at offset i, so a segment's first offset says which it holds.
	from := func(k int) []int32 {
		var out []int32
		for i := p.segments[k].baseOffset; i < 9; i++ {
			out = append(out, int32(i))
		}
		return out
	}
	last := len(p.segments) - 1
	seqs := func(limit int64) []int32 {
		t.Helper()
		h, err := p.ProducerHistory(limit)
		if err != nil {
			t.Fatal(err)
		}
		var out []int32
		for _, b := range h {
			out = append(out, b.FirstSeq)
		}
		return out
	}
	// The newest segment is always read; each older one only while the bytes of those
	// read so far are below the limit.
	acc := int64(0)
	for k := last; k >= 0; k-- {
		acc += p.segments[k].sizeBytes
		if got, want := seqs(acc), from(k); !reflect.DeepEqual(got, want) {
			t.Errorf("ProducerHistory(%d) = %v, want %v", acc, got, want)
		}
		if k > 0 {
			if got, want := seqs(acc+1), from(k-1); !reflect.DeepEqual(got, want) {
				t.Errorf("ProducerHistory(%d) = %v, want %v", acc+1, got, want)
			}
		}
	}
	if got, want := seqs(1), from(last); !reflect.DeepEqual(got, want) {
		t.Errorf("ProducerHistory(1) = %v, want %v", got, want)
	}
	if got, want := seqs(1<<30), from(0); !reflect.DeepEqual(got, want) {
		t.Errorf("ProducerHistory(huge) = %v, want %v", got, want)
	}
}

// controlTypeOf reads a marker's type only from a record that is whole.
func TestD_ControlTypeOfRefusesATruncatedRecord(t *testing.T) {
	full := controlBatch(42, 3, true, 9, 5000)
	const keyAt = batchFullHeaderBytes + 5 // length, attributes, two deltas, key length.
	for _, c := range []struct {
		name string
		b    []byte
		typ  int16
		ok   bool
	}{
		{"whole", full, controlCommit, true},
		{"key is the last thing", full[:keyAt+4], controlCommit, true},
		{"no record at all", full[:batchFullHeaderBytes], 0, false},
		{"only the record length", full[:batchFullHeaderBytes+1], 0, false},
		{"length and attributes", full[:batchFullHeaderBytes+2], 0, false},
		{"up to the key length", full[:keyAt], 0, false},
		{"half the key", full[:keyAt+2], 0, false},
		{"all but one byte of the key", full[:keyAt+3], 0, false},
	} {
		typ, ok := controlTypeOf(c.b)
		if ok != c.ok || typ != c.typ {
			t.Errorf("%s: controlTypeOf = %d, %v; want %d, %v", c.name, typ, ok, c.typ, c.ok)
		}
	}
	short := append([]byte(nil), full...)
	short[keyAt-1] = 4 // a key length of 2 (zigzag): too short to hold version and type.
	if _, ok := controlTypeOf(short); ok {
		t.Error("a key of two bytes was read as a marker type")
	}
	abort := controlBatch(42, 3, false, 9, 5000)
	if typ, ok := controlTypeOf(abort); !ok || typ != controlAbort {
		t.Errorf("abort marker type = %d, %v", typ, ok)
	}
}

// Producer id 0 is a producer: its transactional batch opens a transaction. A
// transactional batch with no producer id (negative) opens none.
func TestD_TransactionOfProducerZero(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	mustAppend(t, p, makeBatch(1, []byte("x"))) // 0
	mustAppend(t, p, txBatch(-1, 0, 0))         // 1: nobody to end it
	mustAppend(t, p, txBatch(0, 0, 0))          // 2
	check := func(p *Partition) {
		t.Helper()
		open, err := p.OpenTransactions()
		if err != nil || !reflect.DeepEqual(open, map[int64]int64{0: 2}) {
			t.Errorf("open transactions = %v, %v; want only producer 0 from offset 2", open, err)
		}
		if got := lso(t, p); got != 2 {
			t.Errorf("LSO = %d, want 2", got)
		}
	}
	check(p)
	p.close()
	p = openSmall(t, dir)
	defer p.close()
	check(p)
}

// Ending a transaction nobody has open writes nothing and says so with offset -1.
func TestD_AppendMarkerWithoutAnOpenTransactionWritesNothing(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	mustAppend(t, p, txBatch(4, 0, 0))
	end := p.LogEndOffset()
	off, wrote, err := p.AppendMarker(99, 0, true, 0, 1000)
	if err != nil || wrote || off != -1 {
		t.Fatalf("AppendMarker for a producer with nothing open = %d, %v, %v; want -1, false, nil", off, wrote, err)
	}
	if p.LogEndOffset() != end {
		t.Errorf("the log grew from %d to %d", end, p.LogEndOffset())
	}
}

// The aborted transactions touching a range are those whose span meets it, nearest
// the start first, whatever order they were ended in.
func TestD_AbortedInRangeAndOrder(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	mustAppend(t, p, txBatch(1, 0, 0)) // 0
	mustAppend(t, p, txBatch(2, 0, 0)) // 1
	mustMarker(t, p, 2, false)         // 2: pid 2 is ended first
	mustMarker(t, p, 1, false)         // 3
	if _, err := p.OpenTransactions(); err != nil {
		t.Fatal(err)
	}
	a1 := AbortedTxn{ProducerID: 1, FirstOffset: 0, LastOffset: 3}
	a2 := AbortedTxn{ProducerID: 2, FirstOffset: 1, LastOffset: 2}
	if !reflect.DeepEqual(p.txn.aborted, []AbortedTxn{a2, a1}) {
		t.Fatalf("recorded in marker order: %+v", p.txn.aborted)
	}
	for _, c := range []struct {
		from, to int64
		want     []AbortedTxn
	}{
		{0, 100, []AbortedTxn{a1, a2}}, // by first offset, not by marker.
		{3, 100, []AbortedTxn{a1}},     // a2's marker (2) is below 3; a1's is at 3.
		{4, 100, nil},
		{0, 1, []AbortedTxn{a1}}, // a2 starts at 1, which is not below 1.
		{0, 2, []AbortedTxn{a1, a2}},
		{0, 0, nil},
	} {
		if got := p.abortedIn(c.from, c.to); !reflect.DeepEqual(got, c.want) {
			t.Errorf("abortedIn(%d, %d) = %+v, want %+v", c.from, c.to, got, c.want)
		}
	}
}

// An aborted transaction whose marker is the very first offset kept is still reported
// after a reopen; one whose marker is below it is not.
func TestD_AbortedTransactionWhoseMarkerIsAtTheStartSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	mustAppend(t, p, txBatch(1, 0, 0)) // 0
	mustMarker(t, p, 1, false)         // 1
	mustAppend(t, p, makeBatch(1, []byte("x")))
	if _, err := p.DeleteBefore(1); err != nil {
		t.Fatal(err)
	}
	p.close()
	p = openSmall(t, dir)
	defer p.close()
	_, _, aborted, err := p.ReadIsolated(1, 1<<20, true)
	if err != nil || !reflect.DeepEqual(aborted, []AbortedTxn{{ProducerID: 1, FirstOffset: 0, LastOffset: 1}}) {
		t.Fatalf("aborted after reopening with the start at the marker = %+v, %v", aborted, err)
	}
	if _, err := p.DeleteBefore(2); err != nil {
		t.Fatal(err)
	}
	p.close()
	p = openSmall(t, dir)
	defer p.close()
	if _, _, aborted, _ := p.ReadIsolated(2, 1<<20, true); len(aborted) != 0 {
		t.Errorf("an aborted transaction whose marker is below the start is listed: %+v", aborted)
	}
}

// bigControl is a control batch whose record is longer than the part of a batch that
// recovery reads first.
func bigControl(pid int64, commit bool) []byte {
	b := controlBatch(pid, 0, commit, 0, 1000)
	// The record's value is 6 bytes at the end followed by a zero header count; replace
	// the record with one whose value is 100 bytes.
	var put [binary.MaxVarintLen64]byte
	var rec []byte
	rec = append(rec, 0)
	rec = append(rec, put[:binary.PutVarint(put[:], 0)]...)
	rec = append(rec, put[:binary.PutVarint(put[:], 0)]...)
	rec = append(rec, put[:binary.PutVarint(put[:], 4)]...)
	typ := controlAbort
	if commit {
		typ = controlCommit
	}
	rec = binary.BigEndian.AppendUint16(rec, 0)
	rec = binary.BigEndian.AppendUint16(rec, uint16(typ))
	rec = append(rec, put[:binary.PutVarint(put[:], 100)]...)
	rec = append(rec, make([]byte, 100)...)
	rec = append(rec, put[:binary.PutVarint(put[:], 0)]...)
	body := append(append([]byte(nil), put[:binary.PutVarint(put[:], int64(len(rec)))]...), rec...)
	out := append(append([]byte(nil), b[:batchFullHeaderBytes]...), body...)
	binary.BigEndian.PutUint32(out[batchLengthOff:], uint32(len(out)-batchOverhead))
	sealCRCOf(out)
	return out
}

// A marker with a long record is read whole when the transactions are rebuilt, so an
// abort is recognised as one.
func TestD_LongControlRecordIsReadWholeOnRecovery(t *testing.T) {
	for _, commit := range []bool{false, true} {
		dir := t.TempDir()
		p := openSmall(t, dir)
		mustAppend(t, p, txBatch(5, 0, 0))
		mustAppend(t, p, bigControl(5, commit))
		p.close()
		p = openSmall(t, dir)
		open, err := p.OpenTransactions()
		if err != nil || len(open) != 0 {
			t.Errorf("commit=%v: open after reopening = %v, %v", commit, open, err)
		}
		_, _, aborted, err := p.ReadIsolated(0, 1<<20, true)
		wantAborted := 0
		if !commit {
			wantAborted = 1
		}
		if err != nil || len(aborted) != wantAborted {
			t.Errorf("commit=%v: aborted after reopening = %+v, %v", commit, aborted, err)
		}
		p.close()
	}
}

// Transactions whose batches are bare headers ending the segment are rebuilt too.
func TestD_BareHeaderTransactionalBatchesAreRebuilt(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	mustAppend(t, p, dBatch(3, 0, 0, 1, batchTransactionalBit|uint16(codecGzip), 0, nil))
	p.close()
	p = openSmall(t, dir)
	defer p.close()
	open, err := p.OpenTransactions()
	if err != nil || !reflect.DeepEqual(open, map[int64]int64{3: 0}) {
		t.Fatalf("open = %v, %v; want producer 3 from 0", open, err)
	}
}

// The first transactional append leaves the marker file and remembers it.
func TestD_FirstTransactionalAppendMarksThePartition(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	mustAppend(t, p, makeBatch(1, []byte("x")))
	if p.txnMarked {
		t.Fatal("marked before any transactional batch")
	}
	mustAppend(t, p, txBatch(1, 0, 0))
	if !p.txnMarked {
		t.Error("not marked after a transactional batch")
	}
	if _, err := os.Stat(filepath.Join(p.dir, txnMarkerFile)); err != nil {
		t.Error(err)
	}
}

// --- timestamps ---

func TestD_TimestampLookupsThatFindNothingAnswerMinusOne(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	off, ts, found, err := p.OffsetOfMaxTimestamp()
	if err != nil || found || off != -1 || ts != -1 {
		t.Errorf("max of an empty log = %d %d %v %v", off, ts, found, err)
	}
	off, ts, found, err = p.OffsetForTimestamp(5)
	if err != nil || found || off != -1 || ts != -1 {
		t.Errorf("lookup in an empty log = %d %d %v %v", off, ts, found, err)
	}
	mustAppend(t, p, timedBatch([]int64{10, 20}, codecNone, false))
	off, ts, found, err = p.OffsetForTimestamp(21)
	if err != nil || found || off != -1 || ts != -1 {
		t.Errorf("lookup past every record = %d %d %v %v", off, ts, found, err)
	}
}

// The scan keeps a mark at the first batch and one each time markEvery bytes have
// passed, remembers the largest timestamp before each, and keeps it when it is
// extended after an append.
func TestD_TimestampMarksFollowMarkEvery(t *testing.T) {
	b := func(ts int64) []byte { return timedBatch([]int64{ts}, codecNone, false) }
	sz := int64(len(b(1)))
	old := markEvery
	defer func() { markEvery = old }()

	markEvery = sz // every batch crosses it
	p := dOpenBig(t, t.TempDir())
	defer p.close()
	for _, ts := range []int64{-30, 40, 20} {
		mustAppend(t, p, b(ts))
	}
	s := p.segments[0]
	if err := s.extendScan(); err != nil {
		t.Fatal(err)
	}
	want := []tsMark{{0, noTimestamp}, {sz, -30}, {2 * sz, 40}}
	if !reflect.DeepEqual(s.ts.marks, want) || s.ts.max != 40 || s.ts.scanned != 3*sz {
		t.Fatalf("after three batches: marks %v max %d scanned %d; want %v, 40, %d", s.ts.marks, s.ts.max, s.ts.scanned, want, 3*sz)
	}
	mustAppend(t, p, b(15))
	if err := s.extendScan(); err != nil {
		t.Fatal(err)
	}
	want = append(want, tsMark{3 * sz, 40})
	if !reflect.DeepEqual(s.ts.marks, want) || s.ts.max != 40 || s.ts.scanned != 4*sz {
		t.Fatalf("after a fourth: marks %v max %d scanned %d; want %v, 40, %d", s.ts.marks, s.ts.max, s.ts.scanned, want, 4*sz)
	}

	markEvery = sz + 1 // a mark only after two batches
	q := dOpenBig(t, t.TempDir())
	defer q.close()
	for _, ts := range []int64{1, 2, 3, 4, 5} {
		mustAppend(t, q, b(ts))
	}
	s = q.segments[0]
	if err := s.extendScan(); err != nil {
		t.Fatal(err)
	}
	want = []tsMark{{0, noTimestamp}, {2 * sz, 2}, {4 * sz, 4}}
	if !reflect.DeepEqual(s.ts.marks, want) {
		t.Errorf("marks with markEvery just over a batch = %v, want %v", s.ts.marks, want)
	}
}

// Timestamps below zero are timestamps: the largest of them is found.
func TestD_NegativeTimestampsAreFound(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	mustAppend(t, p, timedBatch([]int64{-80}, codecNone, false))
	mustAppend(t, p, timedBatch([]int64{-50}, codecNone, false))
	if off, ts, found, err := p.OffsetOfMaxTimestamp(); err != nil || !found || off != 1 || ts != -50 {
		t.Errorf("max = %d %d %v %v, want offset 1 ts -50", off, ts, found, err)
	}
}

// A batch that is only a header is a whole batch to the scan: a lookup reaches it, and it
// answers at the grain its header gives (its records cannot be read).
func TestD_ABareHeaderBatchEndingTheSegmentIsScanned(t *testing.T) {
	dir := t.TempDir()
	p := openSmall(t, dir)
	mustAppend(t, p, dBatch(-1, 0, -1, 1, uint16(codecGzip), 500, nil))
	check := func(p *Partition) {
		t.Helper()
		if off, ts, found := lookup(t, p, 400); !found || off != 0 || ts != 500 {
			t.Errorf("lookup(400) = %d %d %v, want offset 0 ts 500", off, ts, found)
		}
		if off, ts, found, err := p.OffsetOfMaxTimestamp(); err != nil || !found || off != 0 || ts != 500 {
			t.Errorf("max = %d %d %v %v", off, ts, found, err)
		}
	}
	check(p)
	p.close()
	p = openSmall(t, dir)
	defer p.close()
	check(p)
}

// With the start moved into a later segment, lookups see only what is at or past it.
func TestD_LookupsWithTheStartInALaterSegment(t *testing.T) {
	p, err := openPartition(t.TempDir(), ManagerOptions{SegmentBytes: 150, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	for i := range 16 {
		mustAppend(t, p, timedBatch([]int64{int64(1000 - i)}, codecNone, false)) // offset i has timestamp 1000-i
	}
	if len(p.segments) < 4 {
		t.Fatalf("%d segments", len(p.segments))
	}
	// Every start, from each segment's first offset to one past it.
	for _, start := range []int64{p.segments[2].baseOffset, p.segments[2].baseOffset + 1, p.segments[3].baseOffset} {
		if _, err := p.DeleteBefore(start); err != nil {
			t.Fatal(err)
		}
		if off, ts, found := lookup(t, p, 0); !found || off != start || ts != 1000-start {
			t.Errorf("start %d: lookup(0) = %d %d %v, want offset %d ts %d", start, off, ts, found, start, 1000-start)
		}
		if off, ts, found := lookup(t, p, 1000-start); !found || off != start || ts != 1000-start {
			t.Errorf("start %d: lookup(%d) = %d %d %v, want the first visible record", start, 1000-start, off, ts, found)
		}
		if off, ts, found, err := p.OffsetOfMaxTimestamp(); err != nil || !found || off != start || ts != 1000-start {
			t.Errorf("start %d: max = %d %d %v %v, want offset %d ts %d", start, off, ts, found, err, start, 1000-start)
		}
	}
}

// A batch wholly below the start whose records cannot be read is not what a lookup
// answers with, even when it ends exactly at the start.
func TestD_UnreadableBatchEndingAtTheStartIsSkipped(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	mustAppend(t, p, dBatch(-1, 0, -1, 1, uint16(codecGzip), 900, []byte("not gzip"))) // 0
	mustAppend(t, p, timedBatch([]int64{500}, codecNone, false))                       // 1
	if _, err := p.DeleteBefore(1); err != nil {
		t.Fatal(err)
	}
	if off, ts, found := lookup(t, p, 0); !found || off != 1 || ts != 500 {
		t.Errorf("lookup(0) = %d %d %v, want offset 1 ts 500", off, ts, found)
	}
	if off, ts, found, err := p.OffsetOfMaxTimestamp(); err != nil || !found || off != 1 || ts != 500 {
		t.Errorf("max = %d %d %v %v, want offset 1 ts 500", off, ts, found, err)
	}
}

// The largest timestamp of a segment whose start is cut is taken over the records still
// visible, not from the batch header, which counts the hidden ones.
func TestD_MaxTimestampIsOverVisibleRecordsNotTheHeader(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	mustAppend(t, p, timedBatch([]int64{1100, 900, 1000}, codecNone, false)) // 0-2
	if _, err := p.DeleteBefore(1); err != nil {
		t.Fatal(err)
	}
	if off, ts, found, err := p.OffsetOfMaxTimestamp(); err != nil || !found || off != 2 || ts != 1000 {
		t.Errorf("max = %d %d %v %v, want offset 2 ts 1000", off, ts, found, err)
	}
}
