package log

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

// Tests that pin the exact edges of the batch header checks, the record codec and
// the decompression readers: the cases a boundary mistake of one byte or one record
// would turn into a wrong answer or a different error. They were written against the
// mutants of scripts/mutate that the older tests let live.

// batchWith is a batch of the given codec with count records declared and body as
// its records section, sealed.
func batchWith(codec int, count int32, body []byte) []byte {
	b := makeBatch(count, body)
	binary.BigEndian.PutUint16(b[batchAttributesOff:], uint16(codec))
	sealCRC(b)
	return b
}

func TestMutA_CheckBatchShapeAtItsEdges(t *testing.T) {
	// A compressed batch of exactly the header (a body that inflates to the records
	// it counts is not the shape check's to look at) is a whole batch.
	hdrOnly := batchWith(codecGzip, 1, nil)
	if len(hdrOnly) != batchFullHeaderBytes {
		t.Fatalf("test batch is %d bytes", len(hdrOnly))
	}
	if _, err := checkBatchShape(hdrOnly); err != nil {
		t.Errorf("a header-sized compressed batch: %v", err)
	}
	if _, err := checkBatchShape(hdrOnly[:batchFullHeaderBytes-1]); !errors.Is(err, ErrCorruptBatch) {
		t.Errorf("one byte short of a header: %v", err)
	}

	// A record count of zero is refused even when the two header fields agree on it.
	zero := makeBatch(1, []byte("x"))
	binary.BigEndian.PutUint32(zero[batchLastOffsetDeltaOff:], 0xFFFFFFFF) // delta -1: 0 records.
	binary.BigEndian.PutUint32(zero[batchRecordCountOff:], 0)
	sealCRC(zero)
	if _, err := checkBatchShape(zero); !errors.Is(err, ErrCorruptBatch) {
		t.Errorf("a batch of zero records: %v", err)
	}

	// An uncompressed batch cannot hold more records than it has bytes of body.
	if _, err := checkBatchShape(batchWith(codecNone, 3, []byte("ab"))); !errors.Is(err, ErrCorruptBatch) {
		t.Errorf("3 records in 2 bytes: %v", err)
	}
	if _, err := checkBatchShape(batchWith(codecNone, 2, []byte("ab"))); err != nil {
		t.Errorf("2 records in 2 bytes: %v", err)
	}
	if _, err := checkBatchShape(batchWith(codecNone, 3, []byte("abcdef"))); err != nil {
		t.Errorf("3 records in 6 bytes: %v", err)
	}
	// A compressed one may be denser than that, up to maxCompressedRecords.
	if _, err := checkBatchShape(batchWith(codecGzip, 100, []byte("ab"))); err != nil {
		t.Errorf("100 compressed records in 2 bytes: %v", err)
	}
	if _, err := checkBatchShape(batchWith(codecGzip, maxCompressedRecords, []byte("ab"))); err != nil {
		t.Errorf("the most compressed records: %v", err)
	}
	if _, err := checkBatchShape(batchWith(codecGzip, maxCompressedRecords+1, []byte("ab"))); !errors.Is(err, ErrCorruptBatch) {
		t.Errorf("one past the most compressed records: %v", err)
	}
}

func TestMutA_ParseHeaderLengthEdge(t *testing.T) {
	mk := func(length int32, total int) []byte {
		b := make([]byte, total)
		binary.BigEndian.PutUint32(b[batchLengthOff:], uint32(length))
		return b
	}
	// The shortest length that covers the header read so far.
	info, ok := parseHeader(mk(batchHeaderMinBytes-batchOverhead, batchHeaderMinBytes))
	if !ok || info.totalBytes != batchHeaderMinBytes {
		t.Errorf("shortest legal length: %+v, %v", info, ok)
	}
	if _, ok := parseHeader(mk(batchHeaderMinBytes-batchOverhead-1, batchHeaderMinBytes)); ok {
		t.Error("a length one short of covering the header was accepted")
	}
	// Lengths between that and the sum of the two constants are fine too.
	if info, ok := parseHeader(mk(20, 40)); !ok || info.totalBytes != 32 {
		t.Errorf("length 20: %+v, %v", info, ok)
	}
	if _, ok := parseHeader(mk(15, batchHeaderMinBytes-1)); ok {
		t.Error("fewer bytes than the minimum header was accepted")
	}
}

func TestMutA_StampLogAppendTimeEdges(t *testing.T) {
	// A header-only batch (61 bytes) is stamped like any other.
	a := batchWith(codecGzip, 1, nil)
	StampLogAppendTime(a, 1234)
	if attrs := binary.BigEndian.Uint16(a[batchAttributesOff:]); attrs&batchLogAppendTimeBit == 0 || attrs&batchCompressionMask != codecGzip {
		t.Errorf("attributes after the stamp: %#x", attrs)
	}
	if ts := binary.BigEndian.Uint64(a[batchMaxTimestampOff:]); ts != 1234 {
		t.Errorf("max timestamp = %d", ts)
	}
	if !crcOK(a) {
		t.Error("the stamp left a CRC that does not match")
	}

	// A good batch followed by a truncated one: the first is stamped and the walk
	// stops at the second without reading past the payload.
	first := makeBatch(2, []byte("hello"))
	second := makeBatch(2, []byte("worldworld"))
	payload := append(append([]byte{}, first...), second[:len(second)-3]...)
	StampLogAppendTime(payload, 99)
	if binary.BigEndian.Uint64(payload[batchMaxTimestampOff:]) != 99 || !crcOK(payload[:len(first)]) {
		t.Error("the first batch was not stamped")
	}
	cut := payload[len(first):]
	if binary.BigEndian.Uint64(cut[batchMaxTimestampOff:]) == 99 {
		t.Error("the truncated batch was stamped")
	}

	// Two whole batches are both stamped.
	both := append(append([]byte{}, makeBatch(1, []byte("x"))...), makeBatch(1, []byte("y"))...)
	StampLogAppendTime(both, 7)
	n := len(both) / 2
	for i, b := range [][]byte{both[:n], both[n:]} {
		if binary.BigEndian.Uint64(b[batchMaxTimestampOff:]) != 7 || !crcOK(b) {
			t.Errorf("batch %d was not stamped", i)
		}
	}
}

func TestMutA_ParseBatchProducerFields(t *testing.T) {
	// Shorter than a full header: the producer is -1 whatever the bytes say.
	short := makeBatch(1, nil)[:batchHeaderMinBytes+3]
	binary.BigEndian.PutUint32(short[batchLengthOff:], 100)
	h, ok := parseHeaderOK(short)
	if !ok || h.ProducerID != -1 || h.RecordCount != 1 || h.TotalBytes != 112 {
		t.Errorf("ParseBatch of a partial header = %+v, %v", h, ok)
	}
	// Exactly a full header carries the producer fields.
	full := makeBatch(1, nil)
	if len(full) != batchFullHeaderBytes {
		t.Fatalf("%d", len(full))
	}
	binary.BigEndian.PutUint64(full[batchProducerIDOff:], 4242)
	binary.BigEndian.PutUint16(full[batchProducerEpochOff:], 3)
	binary.BigEndian.PutUint32(full[batchBaseSequenceOff:], 17)
	binary.BigEndian.PutUint16(full[batchAttributesOff:], batchTransactionalBit)
	h, ok = parseHeaderOK(full)
	if !ok || h.ProducerID != 4242 || h.ProducerEpoch != 3 || h.BaseSequence != 17 || !h.Transactional || h.Control {
		t.Errorf("ParseBatch of a full header = %+v, %v", h, ok)
	}
}

func parseHeaderOK(b []byte) (BatchHeader, bool) { return ParseBatch(b) }

// --- the record codec ---

func TestMutA_DecodeRecordsEdges(t *testing.T) {
	// A header-sized batch of no records is an empty list, not an error.
	recs, err := DecodeRecords(makeBatchZero(codecNone))
	if err != nil || len(recs) != 0 {
		t.Errorf("zero records: %v, %v", recs, err)
	}
	if _, err := DecodeRecords(makeBatchZero(codecNone)[:batchFullHeaderBytes-1]); !errors.Is(err, ErrBadRecords) {
		t.Errorf("shorter than a header: %v", err)
	}

	// A header whose value is the last thing in the record, and one with an empty key.
	rec := []Record{{TimestampDelta: 5, OffsetDelta: 0, Key: []byte("k"), Value: []byte("v"),
		Headers: []RecordHeader{{Key: "", Value: []byte("last")}, {Key: "h", Value: nil}}}}
	b := batchWith(codecNone, 1, EncodeRecords(rec))
	got, err := DecodeRecords(b)
	if err != nil || len(got) != 1 || len(got[0].Headers) != 2 {
		t.Fatalf("decode: %+v, %v", got, err)
	}
	if got[0].Headers[0].Key != "" || string(got[0].Headers[0].Value) != "last" || got[0].Headers[1].Key != "h" || got[0].Headers[1].Value != nil {
		t.Errorf("headers = %+v", got[0].Headers)
	}
	// Same, the final header value ending exactly at the end of the batch.
	one := []Record{{Key: nil, Value: nil, Headers: []RecordHeader{{Key: "a", Value: []byte("xyz")}}}}
	if got, err := DecodeRecords(batchWith(codecNone, 1, EncodeRecords(one))); err != nil || string(got[0].Headers[0].Value) != "xyz" {
		t.Errorf("a header value that ends the batch: %+v, %v", got, err)
	}
}

func makeBatchZero(codec int) []byte {
	b := makeBatch(1, nil)
	binary.BigEndian.PutUint32(b[batchLastOffsetDeltaOff:], 0xFFFFFFFF)
	binary.BigEndian.PutUint32(b[batchRecordCountOff:], 0)
	binary.BigEndian.PutUint16(b[batchAttributesOff:], uint16(codec))
	sealCRC(b)
	return b
}

// record bytes: a length varint then the record's own bytes.
func rawRecords(recs ...[]byte) []byte {
	var out []byte
	for _, r := range recs {
		out = appendVarint(out, int64(len(r)))
		out = append(out, r...)
	}
	return out
}

func TestMutA_DecodeRecordsRefusesDamagedRecordsForTheRightReason(t *testing.T) {
	cases := []struct {
		name  string
		count int32
		body  []byte
		want  string
	}{
		// A record of length 0 has no attributes byte.
		{"zero-length record", 1, rawRecords(nil, nil, nil, nil), "cut short"},
		// Length 1: the attributes byte and nothing for the timestamp.
		{"one-byte record", 1, rawRecords([]byte{0}, nil, nil, nil), "a varint is cut short"},
		// Attributes, timestamp delta, offset delta, key len 0, value len 0, then a
		// header count that runs past the record (50 headers, none there).
		{"header count past the record", 1, rawRecords([]byte{0, 0, 0, 0, 0, 100}, nil, nil, nil), "header count runs past"},
		// A negative header count.
		{"negative header count", 1, rawRecords([]byte{0, 0, 0, 0, 0, 1}, nil, nil, nil), "header count runs past"},
		// A header with a negative key length.
		{"header with no key", 1, rawRecords([]byte{0, 0, 0, 0, 0, 2, 1, 0, 0}, nil, nil, nil), "a header has no key"},
		// A length that runs past the record.
		{"key past the record", 1, rawRecords([]byte{0, 0, 0, 20, 0, 0}, nil, nil, nil), "a length runs past the record"},
	}
	for _, c := range cases {
		_, err := DecodeRecords(batchWith(codecNone, c.count, c.body))
		if !errors.Is(err, ErrBadRecords) || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want ErrBadRecords mentioning %q", c.name, err, c.want)
		}
	}
	// A count the body cannot hold.
	if _, err := DecodeRecords(batchWith(codecNone, 9, []byte{1, 2, 3})); !errors.Is(err, ErrBadRecords) {
		t.Errorf("count past the body: %v", err)
	}
	// A record length that runs past the batch.
	if _, err := DecodeRecords(batchWith(codecNone, 1, []byte{100, 1})); !errors.Is(err, ErrBadRecords) {
		t.Errorf("length past the batch: %v", err)
	}
	// A negative record length.
	if _, err := DecodeRecords(batchWith(codecNone, 1, []byte{1, 1})); !errors.Is(err, ErrBadRecords) {
		t.Errorf("negative record length: %v", err)
	}
}

// --- walking records ---

func TestMutA_WalkRecordsEdges(t *testing.T) {
	// A header-sized batch of no records walks to nothing without an error.
	for _, f := range []func(b []byte) error{
		func(b []byte) error {
			return walkRecords(b, 1<<20, false, func(int, recordHead) bool { t.Error("visited"); return true })
		},
		func(b []byte) error {
			return batchRecords(b, func(int64, int64) bool { t.Error("visited"); return true })
		},
	} {
		if err := f(makeBatchZero(codecNone)); err != nil {
			t.Errorf("zero records: %v", err)
		}
	}
	// A record that is only its three leading fields is whole.
	fields := []byte{0, 4, 2} // attributes, timestamp delta 2, offset delta 1.
	var seen []recordHead
	err := walkRecords(batchWith(codecNone, 1, rawRecords(fields)), 1<<20, false, func(_ int, h recordHead) bool {
		seen = append(seen, h)
		return true
	})
	if err != nil || len(seen) != 1 || seen[0].TimestampDelta != 2 || seen[0].OffsetDelta != 1 {
		t.Errorf("a record of just its fields: %+v, %v", seen, err)
	}
	// A record that is empty, and one shorter than its own fields.
	err = walkRecords(batchWith(codecNone, 1, rawRecords(nil, nil, nil)), 1<<20, false, func(int, recordHead) bool { return true })
	if err == nil || !strings.Contains(err.Error(), "is empty") {
		t.Errorf("an empty record: %v", err)
	}
	err = walkRecords(batchWith(codecNone, 1, rawRecords([]byte{0}, []byte{0, 0, 0}, nil)), 1<<20, false, func(int, recordHead) bool { return true })
	if err == nil || !strings.Contains(err.Error(), "shorter than its own fields") {
		t.Errorf("a record shorter than its fields: %v", err)
	}
	// A key length of 0 is an empty key; any negative one is null.
	for _, c := range []struct {
		keyLen byte // zigzag varint.
		null   bool
	}{{0, false}, {1, true}, {3, true}, {2, false}} {
		rec := append([]byte{0, 0, 0, c.keyLen}, make([]byte, 1)...)
		var got []recordHead
		err := walkRecords(batchWith(codecNone, 1, rawRecords(rec)), 1<<20, true, func(_ int, h recordHead) bool { got = append(got, h); return true })
		if err != nil || len(got) != 1 || got[0].KeyNull != c.null {
			t.Errorf("key length byte %d: %+v, %v", c.keyLen, got, err)
		}
	}
}

// --- the bounded reader ---

type chunkReader struct{ data []byte }

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.data)
	c.data = c.data[n:]
	return n, nil
}

func TestMutA_BoundedReaderPassesExactlyTheLimit(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 10)
	r := &boundedReader{r: &chunkReader{data: data}, left: 10}
	p := make([]byte, 64)
	n, err := r.Read(p)
	if n != 10 || err != nil || !bytes.Equal(p[:n], data[:10]) {
		t.Fatalf("first read = %d, %v; want the 10 bytes within the limit", n, err)
	}
	if _, err := r.Read(p); !errors.Is(err, ErrInflatedTooLarge) {
		t.Errorf("the read after the limit: %v, want ErrInflatedTooLarge", err)
	}

	// Exactly the limit and no more is not too large.
	out, err := readAllInflated(&boundedReader{r: &chunkReader{data: data[:10]}, left: 10}, 10)
	if err != nil || len(out) != 10 {
		t.Errorf("exactly the limit: %d bytes, %v", len(out), err)
	}
	// One more is.
	_, err = readAllInflated(&boundedReader{r: &chunkReader{data: data[:11]}, left: 10}, 10)
	if !errors.Is(err, ErrInflatedTooLarge) {
		t.Errorf("one past the limit: %v", err)
	}
	// A limit of zero admits nothing.
	if _, err := readAllInflated(&boundedReader{r: &chunkReader{data: data[:1]}, left: 0}, 0); !errors.Is(err, ErrInflatedTooLarge) {
		t.Errorf("a limit of 0: %v", err)
	}
	if out, err := readAllInflated(&boundedReader{r: &chunkReader{}, left: 0}, 0); err != nil || len(out) != 0 {
		t.Errorf("an empty stream under a limit of 0: %d, %v", len(out), err)
	}
}

// --- snappy ---

func xerial(blocks ...[]byte) []byte {
	out := append([]byte{}, xerialMagic...)
	out = append(out, 0, 0, 0, 1, 0, 0, 0, 1) // version, compatible version.
	for _, b := range blocks {
		out = binary.BigEndian.AppendUint32(out, uint32(len(b)))
		out = append(out, b...)
	}
	return out
}

func TestMutA_XerialStreamsAndTheirDamage(t *testing.T) {
	c1, _ := compress(codecSnappy, []byte("hello, "))
	c2, _ := compress(codecSnappy, []byte("snappy world"))
	got, err := Decompress(codecSnappy, xerial(c1, c2), 1<<20)
	if err != nil || string(got) != "hello, snappy world" {
		t.Fatalf("two blocks: %q, %v", got, err)
	}
	if got, err := Decompress(codecSnappy, xerial(), 1<<20); err != nil || len(got) != 0 {
		t.Errorf("no blocks: %q, %v", got, err)
	}
	// A header with fewer than the 16 bytes.
	if _, err := Decompress(codecSnappy, xerialMagic, 1<<20); !errors.Is(err, errRecords) {
		t.Errorf("a cut header: %v", err)
	}
	// Three bytes where a block's length belongs.
	if _, err := Decompress(codecSnappy, append(xerial(), 0, 0, 0), 1<<20); err == nil || !strings.Contains(err.Error(), "stream is cut short") {
		t.Errorf("a cut length: %v", err)
	}
	// Exactly four, and it says a block of 5 follows: the block is short, not the stream.
	if _, err := Decompress(codecSnappy, append(xerial(), 0, 0, 0, 5), 1<<20); err == nil || !strings.Contains(err.Error(), "block is cut short") {
		t.Errorf("a block that is not there: %v", err)
	}
	// A block of length zero is not a snappy block.
	if _, err := Decompress(codecSnappy, append(xerial(), 0, 0, 0, 0), 1<<20); err == nil || strings.Contains(err.Error(), "cut short") {
		t.Errorf("an empty block: %v, want the snappy decoder's refusal", err)
	}
	// A block of the declared size that is not snappy.
	if _, err := Decompress(codecSnappy, xerial([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 1}), 1<<20); !errors.Is(err, errRecords) {
		t.Errorf("garbage block: %v", err)
	}
	// The limit is shared by the blocks: each is within it, together they are past it.
	if _, err := Decompress(codecSnappy, xerial(c1, c2), 10); !errors.Is(err, ErrInflatedTooLarge) {
		t.Errorf("blocks past the limit together: %v", err)
	}
}

func TestMutA_SnappyBlockClaimsAreCheckedAgainstItsSize(t *testing.T) {
	// A block header (a varint) that claims n bytes, then one byte that is not a
	// well-formed element: the claim is judged before the body is.
	claim := func(n uint64) []byte { return binary.AppendUvarint(nil, n) }
	// 22 bytes per block byte and 64 over is the most a block's size can mean: past
	// it the claim is refused on its face.
	if _, err := snappyBlock(append(claim(22*2+64+1), 0), 1<<20); err == nil || !strings.Contains(err.Error(), "claims to inflate") {
		t.Errorf("a claim past what the block's size can mean: %v", err)
	}
	// At it, the claim is believable and the body is what fails.
	if _, err := snappyBlock(append(claim(22*2+64), 0), 1<<20); err == nil || strings.Contains(err.Error(), "claims to inflate") {
		t.Errorf("a claim at the edge of believable: %v", err)
	}
	if _, err := snappyBlock(append(claim(50), 0), 1<<20); err == nil || strings.Contains(err.Error(), "claims to inflate") {
		t.Errorf("a modest claim: %v", err)
	}
}

// --- gzip ---

func gz(s string) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte(s))
	w.Close()
	return b.Bytes()
}

func TestMutA_GzipMembersAndWhatFollowsThem(t *testing.T) {
	two := append(gz("first,"), gz("second")...)
	if got, err := Decompress(codecGzip, two, 1<<20); err != nil || string(got) != "first,second" {
		t.Errorf("two members: %q, %v", got, err)
	}
	three := append(append(gz("a"), gz("b")...), gz("c")...)
	if got, err := Decompress(codecGzip, three, 1<<20); err != nil || string(got) != "abc" {
		t.Errorf("three members: %q, %v", got, err)
	}
	// Bytes that do not start a member end the data.
	trail := append(gz("kept"), "not gzip at all"...)
	if got, err := Decompress(codecGzip, trail, 1<<20); err != nil || string(got) != "kept" {
		t.Errorf("a member then junk: %q, %v", got, err)
	}
	// A member that is cut inside is an error, not an end.
	whole := gz("some text that is long enough to cut inside of")
	if _, err := Decompress(codecGzip, whole[:len(whole)-6], 1<<20); err == nil {
		t.Error("a cut member was read as a whole stream")
	}
	if got, err := Decompress(codecGzip, gz("one"), 1<<20); err != nil || string(got) != "one" {
		t.Errorf("one member: %q, %v", got, err)
	}
}
