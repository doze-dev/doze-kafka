package log

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// timedBatch builds a record-batch-v2 whose records carry the given timestamps, one
// record per timestamp, compressed with codec, in the form a real client writes.
func timedBatch(timestamps []int64, codec int, appendTime bool) []byte {
	var recs []byte
	var put [binary.MaxVarintLen64]byte
	first, max := timestamps[0], timestamps[0]
	for _, ts := range timestamps {
		if ts > max {
			max = ts
		}
	}
	for i, ts := range timestamps {
		var r []byte
		r = append(r, 0) // attributes.
		r = append(r, put[:binary.PutVarint(put[:], ts-first)]...)
		r = append(r, put[:binary.PutVarint(put[:], int64(i))]...)
		r = append(r, put[:binary.PutVarint(put[:], -1)]...) // null key.
		r = append(r, put[:binary.PutVarint(put[:], 1)]...)  // value of one byte.
		r = append(r, 'x')
		r = append(r, put[:binary.PutVarint(put[:], 0)]...) // no headers.
		recs = append(recs, put[:binary.PutVarint(put[:], int64(len(r)))]...)
		recs = append(recs, r...)
	}
	body := recs
	switch codec {
	case codecGzip:
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		w.Write(recs)
		w.Close()
		body = b.Bytes()
	case codecSnappy:
		body = snappy.Encode(nil, recs)
	case codecLZ4:
		var b bytes.Buffer
		w := lz4.NewWriter(&b)
		w.Write(recs)
		w.Close()
		body = b.Bytes()
	case codecZstd:
		e, _ := zstd.NewWriter(nil)
		body = e.EncodeAll(recs, nil)
	}
	buf := make([]byte, batchFullHeaderBytes+len(body))
	binary.BigEndian.PutUint32(buf[batchLengthOff:], uint32(len(buf)-batchOverhead))
	buf[batchMagicOff] = batchMagicV2
	attrs := uint16(codec)
	if appendTime {
		attrs |= batchLogAppendTimeBit
	}
	binary.BigEndian.PutUint16(buf[batchAttributesOff:], attrs)
	binary.BigEndian.PutUint32(buf[batchLastOffsetDeltaOff:], uint32(len(timestamps)-1))
	binary.BigEndian.PutUint64(buf[batchFirstTimestampOff:], uint64(first))
	binary.BigEndian.PutUint64(buf[batchMaxTimestampOff:], uint64(max))
	binary.BigEndian.PutUint32(buf[batchRecordCountOff:], uint32(len(timestamps)))
	copy(buf[batchFullHeaderBytes:], body)
	sealCRC(buf)
	return buf
}

func mustAppend(t *testing.T, p *Partition, b []byte) {
	t.Helper()
	if _, err := p.Append(b); err != nil {
		t.Fatal(err)
	}
}

func lookup(t *testing.T, p *Partition, target int64) (int64, int64, bool) {
	t.Helper()
	off, ts, found, err := p.OffsetForTimestamp(target)
	if err != nil {
		t.Fatalf("OffsetForTimestamp(%d): %v", target, err)
	}
	return off, ts, found
}

// Every codec yields the same offsets and timestamps, including the Java client's
// xerial-framed snappy.
func TestBatchRecordsReadsEveryCodec(t *testing.T) {
	stamps := []int64{1000, 1005, 990, 1100}
	for _, tc := range []struct {
		name  string
		codec int
	}{{"none", codecNone}, {"gzip", codecGzip}, {"snappy", codecSnappy}, {"lz4", codecLZ4}, {"zstd", codecZstd}} {
		t.Run(tc.name, func(t *testing.T) {
			b := timedBatch(stamps, tc.codec, false)
			binary.BigEndian.PutUint64(b[batchBaseOffsetOff:], 40)
			var gotOff, gotTS []int64
			if err := batchRecords(b, func(o, ts int64) bool { gotOff, gotTS = append(gotOff, o), append(gotTS, ts); return true }); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(gotOff) != "[40 41 42 43]" || fmt.Sprint(gotTS) != fmt.Sprint(stamps) {
				t.Errorf("offsets %v timestamps %v", gotOff, gotTS)
			}
		})
	}
	// snappy-java's framing: 16-byte header, then length-prefixed blocks.
	recs := timedBatch(stamps, codecNone, false)[batchFullHeaderBytes:]
	block := snappy.Encode(nil, recs)
	xerial := append(append([]byte{}, xerialMagic...), 0, 0, 0, 1, 0, 0, 0, 1)
	xerial = binary.BigEndian.AppendUint32(xerial, uint32(len(block)))
	xerial = append(xerial, block...)
	b := timedBatch(stamps, codecNone, false)[:batchFullHeaderBytes]
	b = append(b, xerial...)
	b[batchAttributesOff+1] = codecSnappy
	binary.BigEndian.PutUint32(b[batchLengthOff:], uint32(len(b)-batchOverhead))
	n := 0
	if err := batchRecords(b, func(int64, int64) bool { n++; return true }); err != nil || n != 4 {
		t.Errorf("xerial snappy: %d records, %v", n, err)
	}
	// A batch that is cut short is an error, not a short answer.
	if err := batchRecords(timedBatch(stamps, codecGzip, false)[:batchFullHeaderBytes+3], func(int64, int64) bool { return true }); err == nil {
		t.Error("a truncated compressed batch decoded")
	}
}

// The first record at or past the timestamp, exactly: inside a batch, between
// batches, with timestamps out of order, and at both ends.
func TestOffsetForTimestampIsExact(t *testing.T) {
	for _, codec := range []int{codecNone, codecSnappy, codecZstd} {
		t.Run(fmt.Sprint("codec", codec), func(t *testing.T) {
			p := openSmall(t, t.TempDir())
			defer p.close()
			mustAppend(t, p, timedBatch([]int64{100, 100, 105}, codec, false)) // offsets 0-2
			mustAppend(t, p, timedBatch([]int64{200, 150, 300}, codec, false)) // offsets 3-5

			for _, tc := range []struct {
				target int64
				off    int64
				ts     int64
				found  bool
			}{
				{0, 0, 100, true}, {100, 0, 100, true}, {101, 2, 105, true}, {105, 2, 105, true},
				{106, 3, 200, true}, {150, 3, 200, true}, {201, 5, 300, true}, {300, 5, 300, true},
				{301, -1, -1, false},
			} {
				off, ts, found := lookup(t, p, tc.target)
				if off != tc.off || ts != tc.ts || found != tc.found {
					t.Errorf("target %d: offset %d ts %d found %v, want %d %d %v", tc.target, off, ts, found, tc.off, tc.ts, tc.found)
				}
			}
		})
	}
	// Nothing to find in an empty partition.
	e := openSmall(t, t.TempDir())
	defer e.close()
	if _, _, found := lookup(t, e, 0); found {
		t.Error("an empty partition has a record")
	}
}

// Across many segments and many marks, every record is found by its own timestamp,
// and one just past it finds the next; a reopened partition answers the same.
func TestOffsetForTimestampAcrossSegmentsAndRestart(t *testing.T) {
	old := markEvery
	markEvery = 300 // a mark every couple of batches.
	defer func() { markEvery = old }()

	dir := t.TempDir()
	p := openSmall(t, dir)
	const n = 60
	for i := range n {
		mustAppend(t, p, timedBatch([]int64{int64(1000 + i*10)}, codecNone, false))
	}
	if len(p.segments) < 5 {
		t.Fatalf("only %d segments", len(p.segments))
	}
	check := func(p *Partition) {
		t.Helper()
		for i := range n {
			ts := int64(1000 + i*10)
			if off, got, found := lookup(t, p, ts); !found || off != int64(i) || got != ts {
				t.Fatalf("target %d: offset %d ts %d found %v, want %d", ts, off, got, found, i)
			}
			if i+1 < n {
				if off, _, found := lookup(t, p, ts+1); !found || off != int64(i+1) {
					t.Fatalf("target %d: offset %d found %v, want %d", ts+1, off, found, i+1)
				}
			}
		}
		if _, _, found := lookup(t, p, int64(1000+n*10)); found {
			t.Error("a timestamp after the last record found one")
		}
	}
	check(p)
	p.close()
	p = openSmall(t, dir)
	defer p.close()
	check(p)

	// The active segment grows after a lookup has summarised it.
	mustAppend(t, p, timedBatch([]int64{5000}, codecNone, false))
	if off, _, found := lookup(t, p, 4000); !found || off != n {
		t.Errorf("a record appended after a lookup is not found: offset %d found %v", off, found)
	}
}

// Records below the log start are not answers, and do not decide the largest timestamp.
func TestTimestampLookupsRespectDeletedRecords(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	mustAppend(t, p, timedBatch([]int64{900, 1000, 1100}, codecNone, false)) // 0-2: the largest is at offset 2
	mustAppend(t, p, timedBatch([]int64{200, 300}, codecNone, false))        // 3-4
	if _, err := p.DeleteBefore(3); err != nil {
		t.Fatal(err)
	}
	if off, ts, found := lookup(t, p, 0); !found || off != 3 || ts != 200 {
		t.Errorf("target 0 after deleting to 3: offset %d ts %d found %v, want the first visible record", off, ts, found)
	}
	if _, _, found := lookup(t, p, 1000); found {
		t.Error("found a record that was deleted")
	}
	if off, ts, found, err := p.OffsetOfMaxTimestamp(); err != nil || !found || off != 4 || ts != 300 {
		t.Errorf("max timestamp = offset %d ts %d found %v %v, want 4 and 300 (the 1100 was deleted)", off, ts, found, err)
	}
}

// The largest timestamp is the first of its ties; LogAppendTime stamps every record
// with the batch's.
func TestMaxTimestampAndLogAppendTime(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	if _, _, found, _ := p.OffsetOfMaxTimestamp(); found {
		t.Error("an empty partition has a max timestamp")
	}
	mustAppend(t, p, timedBatch([]int64{10, 500, 20}, codecNone, false)) // 0-2
	mustAppend(t, p, timedBatch([]int64{500, 30}, codecNone, false))     // 3-4: ties with offset 1
	if off, ts, found, err := p.OffsetOfMaxTimestamp(); err != nil || !found || off != 1 || ts != 500 {
		t.Errorf("max = %d %d %v %v, want the first 500 at offset 1", off, ts, found, err)
	}
	// A LogAppendTime batch: the records' own deltas are ignored.
	mustAppend(t, p, timedBatch([]int64{1, 2, 3}, codecNone, true)) // header max 3
	got := []int64{}
	b := timedBatch([]int64{1, 2, 3}, codecNone, true)
	batchRecords(b, func(_, ts int64) bool { got = append(got, ts); return true })
	if fmt.Sprint(got) != "[3 3 3]" {
		t.Errorf("LogAppendTime record timestamps = %v, want the batch's 3 for each", got)
	}
}

// The start can fall inside a batch: the records of it below the start are hidden
// from a timestamp lookup just as they are from a fetch.
func TestTimestampLookupSkipsHiddenRecordsInsideABatch(t *testing.T) {
	p := openSmall(t, t.TempDir())
	defer p.close()
	mustAppend(t, p, timedBatch([]int64{900, 1000, 1100}, codecNone, false)) // 0-2
	if _, err := p.DeleteBefore(2); err != nil {
		t.Fatal(err)
	}
	if off, ts, found := lookup(t, p, 950); !found || off != 2 || ts != 1100 {
		t.Errorf("target 950 with offsets 0 and 1 deleted: offset %d ts %d found %v, want offset 2 (1100)", off, ts, found)
	}
	if off, ts, found, _ := p.OffsetOfMaxTimestamp(); !found || off != 2 || ts != 1100 {
		t.Errorf("max timestamp = %d %d %v", off, ts, found)
	}
}
