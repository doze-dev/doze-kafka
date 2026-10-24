package log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"
)

// Tests for the produce-path checks (produce.go): the edges of each rule, with
// real encoded batches, asserting the refusal's kind and its exact words.

const (
	tNow = int64(1_000_000)
)

func edgeRules() ProduceRules {
	r := testRules()
	r.NowMs = tNow
	return r
}

// vbytes is a zig-zag varint.
func vb(v int64) []byte { return appendVarint(nil, v) }

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// fields is a record's bytes after its length: attributes, deltas, key, value, headers.
func fields(tsDelta, offDelta int64, keyLen int64, key string, valLen int64, val string, headers []byte) []byte {
	return cat([]byte{0}, vb(tsDelta), vb(offDelta), vb(keyLen), []byte(key), vb(valLen), []byte(val), headers)
}

// withLen puts the record's length in front of its fields.
func withLen(n int64, payload []byte) []byte { return cat(vb(n), payload) }

// auto puts the true length in front.
func auto(payload []byte) []byte { return withLen(int64(len(payload)), payload) }

// rawBatch is an uncompressed batch of count records whose section is body, with
// the timestamps and sequence the caller wants.
func rawBatch(count int32, body []byte, first, max int64) []byte {
	b := withBody(codecNone, count, body)
	binary.BigEndian.PutUint64(b[batchFirstTimestampOff:], uint64(first))
	binary.BigEndian.PutUint64(b[batchMaxTimestampOff:], uint64(max))
	sealCRCOf(b)
	return b
}

func setAttrs(b []byte, f func(uint16) uint16) []byte {
	binary.BigEndian.PutUint16(b[batchAttributesOff:], f(attrsOf(b)))
	sealCRCOf(b)
	return b
}

func refusedWith(t *testing.T, name string, records []byte, rules ProduceRules, kind RefusalKind, contains string) *Refusal {
	t.Helper()
	_, r := CheckProduce(records, rules)
	if r == nil {
		t.Errorf("%s: accepted, want kind %d containing %q", name, kind, contains)
		return nil
	}
	if r.Kind != kind || !strings.Contains(r.Error(), contains) {
		t.Errorf("%s: refused kind %d %q, want kind %d containing %q", name, r.Kind, r.Error(), kind, contains)
	}
	return r
}

func accepted(t *testing.T, name string, records []byte, rules ProduceRules) AcceptedBatch {
	t.Helper()
	a, r := CheckProduce(records, rules)
	if r != nil {
		t.Fatalf("%s: refused: kind %d %q", name, r.Kind, r.Error())
	}
	return a
}

// --- firstBatch / RequestRefusal ---

func header(length int32, magic byte, total int) []byte {
	b := make([]byte, total)
	if total >= batchLengthOff+4 {
		binary.BigEndian.PutUint32(b[batchLengthOff:], uint32(length))
	}
	if total > batchMagicOff {
		b[batchMagicOff] = magic
	}
	return b
}

func TestRequestRefusalEdges(t *testing.T) {
	// 16 bytes are too few to hold the magic: no batch, nothing to refuse, however
	// the length field reads. 17 bytes hold it, and a length of 0 is refused.
	if tx, r := RequestRefusal([][]byte{header(0, 2, batchMagicOff)}); r != nil || tx {
		t.Errorf("16 bytes: %v, %v; want no batch and no refusal", tx, r)
	}
	if _, r := RequestRefusal([][]byte{header(0, 2, batchMagicOff+1)}); r == nil || r.Kind != RefuseCorrupt {
		t.Errorf("17 bytes with a length of 0: %v, want CORRUPT_MESSAGE", r)
	}
	// A length below 14 is refused, with Kafka's words; 14 is not.
	_, r := RequestRefusal([][]byte{header(13, 2, 13+batchOverhead)})
	if r == nil || r.Kind != RefuseCorrupt || !r.BeforeLog || r.KafkaMessage != "Record size 13 is less than the minimum record overhead (14)" {
		t.Errorf("length 13: %+v", r)
	}
	if _, r := RequestRefusal([][]byte{header(14, 2, 14+batchOverhead)}); r != nil {
		t.Errorf("length 14: %+v, want no refusal", r)
	}
	// A format version past 2 or below 0 is refused; 0, 1 and 2 are not.
	for _, magic := range []byte{0, 1, 2} {
		if _, r := RequestRefusal([][]byte{header(20, magic, 20+batchOverhead)}); r != nil {
			t.Errorf("magic %d: %+v, want no refusal", magic, r)
		}
	}
	_, r = RequestRefusal([][]byte{header(20, 3, 20+batchOverhead)})
	if r == nil || r.Kind != RefuseCorrupt || r.KafkaMessage != "Invalid magic found in record: 3" {
		t.Errorf("magic 3: %+v", r)
	}
	_, r = RequestRefusal([][]byte{header(20, 0xff, 20+batchOverhead)})
	if r == nil || r.KafkaMessage != "Invalid magic found in record: -1" {
		t.Errorf("magic -1: %+v", r)
	}
	// A batch that says it is longer than what was sent is no batch, not corrupt.
	if tx, r := RequestRefusal([][]byte{header(100, 2, 40)}); r != nil || tx {
		t.Errorf("a cut-off batch: %v, %v", tx, r)
	}
}

func TestRequestRefusalSeesTheTransactionalBitOfAnyPartitionsFirstBatch(t *testing.T) {
	plain := rawBatch(1, auto(fields(0, 0, 1, "k", 1, "v", vb(0))), 0, 0)
	txn := setAttrs(rawBatch(1, auto(fields(0, 0, 1, "k", 1, "v", vb(0))), 0, 0), func(a uint16) uint16 { return a | batchTransactionalBit })
	if tx, r := RequestRefusal([][]byte{plain, plain}); r != nil || tx {
		t.Errorf("two plain partitions: %v, %v", tx, r)
	}
	if tx, r := RequestRefusal([][]byte{plain, txn}); r != nil || !tx {
		t.Errorf("the second partition transactional: %v, %v", tx, r)
	}
	if tx, r := RequestRefusal([][]byte{txn}); r != nil || !tx {
		t.Errorf("transactional: %v, %v", tx, r)
	}
	// The bit's place in a version 1 message is not a transactional bit.
	old := rawBatch(1, auto(fields(0, 0, 1, "k", 1, "v", vb(0))), 0, 0)
	old[batchMagicOff] = 1
	binary.BigEndian.PutUint16(old[batchAttributesOff:], batchTransactionalBit)
	if tx, _ := RequestRefusal([][]byte{old}); tx {
		t.Error("a magic 1 message with that bit set is reported transactional")
	}
	// A corrupt first batch fails the request, and no transactional answer comes with it.
	bad := header(3, 2, 20)
	tx, r := RequestRefusal([][]byte{txn, bad})
	if r == nil || tx {
		t.Errorf("a corrupt partition after a transactional one: %v, %v", tx, r)
	}
}

// --- shape of the request ---

func TestCheckProduceShapeOfTheRequest(t *testing.T) {
	good := rawBatch(1, auto(fields(0, 0, 1, "k", 1, "v", vb(0))), 0, 0)
	accepted(t, "good", good, edgeRules())

	refusedWith(t, "empty", nil, edgeRules(), RefuseInvalidRecord, "hold no whole record batch (0 bytes)")
	refusedWith(t, "two batches", append(append([]byte{}, good...), good...), edgeRules(), RefuseInvalidRecord, "more than one record batch")
	// Trailing bytes too short to be a batch are ignored.
	a := accepted(t, "trailing bytes", append(append([]byte{}, good...), 1, 2, 3), edgeRules())
	if !bytes.Equal(a.Batch, good) || a.Header.TotalBytes != len(good) {
		t.Errorf("accepted batch is %d bytes (header says %d), want %d", len(a.Batch), a.Header.TotalBytes, len(good))
	}
	// Garbage after the batch that is itself corrupt: Kafka's answer has no text there.
	r := refusedWith(t, "corrupt second", append(append([]byte{}, good...), header(3, 2, 20)...), edgeRules(), RefuseCorrupt, "")
	if r != nil && r.KafkaMessage != "" {
		t.Errorf("a corrupt second batch carries Kafka's text %q", r.KafkaMessage)
	}
	// A corrupt first batch has it.
	r = refusedWith(t, "corrupt first", header(20, 3, 20+batchOverhead), edgeRules(), RefuseCorrupt, "Invalid magic found in record: 3")
	if r != nil && r.KafkaMessage != "Invalid magic found in record: 3" {
		t.Errorf("KafkaMessage = %q", r.KafkaMessage)
	}
	old := append([]byte{}, good...)
	old[batchMagicOff] = 1
	refusedWith(t, "magic 1", old, edgeRules(), RefuseInvalidRecord, "format version 1; only version 2 is taken")
	// A base offset that is not 0.
	based := append([]byte{}, good...)
	binary.BigEndian.PutUint64(based[batchBaseOffsetOff:], 7)
	refusedWith(t, "base offset", based, edgeRules(), RefuseInvalidRecord, "base offset is 7")
}

func TestRefusalErrorIsKafkasTextWhenItHasOne(t *testing.T) {
	if got := (&Refusal{Reason: "ours", KafkaMessage: "theirs"}).Error(); got != "theirs" {
		t.Errorf("Error() = %q", got)
	}
	if got := (&Refusal{Reason: "ours"}).Error(); got != "ours" {
		t.Errorf("Error() = %q", got)
	}
	if got := (&Refusal{Reason: "ours", KafkaMessage: "theirs"}).Message(); got != "theirs" {
		t.Errorf("Message() = %q", got)
	}
}

func TestRefusalKindsAreNonZeroAndDistinct(t *testing.T) {
	seen := map[RefusalKind]bool{}
	for _, k := range []RefusalKind{RefuseInvalidRecord, RefuseCorrupt, RefuseTooLarge, RefuseUnreadable, RefuseTimestamp, RefuseCompression} {
		if k <= 0 || seen[k] {
			t.Errorf("kind %d is zero, negative or repeated", k)
		}
		seen[k] = true
	}
}

// --- header checks ---

func TestCheckProduceHeaderEdges(t *testing.T) {
	body := auto(fields(0, 0, 1, "k", 1, "v", vb(0)))
	good := rawBatch(1, body, 0, 0)

	// Exactly max.message.bytes is stored, one over is refused.
	rules := edgeRules()
	rules.MaxBatchBytes = int64(len(good))
	accepted(t, "at the limit", good, rules)
	rules.MaxBatchBytes--
	r := refusedWith(t, "over the limit", good, rules, RefuseTooLarge, fmt.Sprintf("%d bytes is larger than max.message.bytes (%d)", len(good), rules.MaxBatchBytes))
	if r != nil && !r.BeforeLog {
		t.Error("too large is refused before the log")
	}

	// A batch of exactly the header's length has no records; shorter is cut short.
	empty := makeBatch(1, nil)
	refusedWith(t, "header only", empty, edgeRules(), RefuseInvalidRecord, "record 0 of 1 has no length")
	short := header(int32(60-batchOverhead), 2, 60)
	refusedWith(t, "60 bytes", short, edgeRules(), RefuseCorrupt, "60 bytes is shorter than its header (61)")

	// Checksum.
	badCRC := append([]byte{}, good...)
	badCRC[len(badCRC)-1] ^= 0xff
	refusedWith(t, "checksum", badCRC, edgeRules(), RefuseCorrupt, "checksum does not match")

	// Unknown codec.
	unk := withBody(codecNone, 1, body)
	binary.BigEndian.PutUint16(unk[batchAttributesOff:], 5)
	sealCRCOf(unk)
	refusedWith(t, "codec 5", unk, edgeRules(), RefuseUnreadable, "unknown compression codec 5")

	// Last offset delta and count.
	mod := func(f func(b []byte)) []byte {
		b := append([]byte{}, good...)
		f(b)
		sealCRCOf(b)
		return b
	}
	setDelta := func(d int32) []byte {
		return mod(func(b []byte) { binary.BigEndian.PutUint32(b[batchLastOffsetDeltaOff:], uint32(d)) })
	}
	setCount := func(n int32) []byte {
		return mod(func(b []byte) { binary.BigEndian.PutUint32(b[batchRecordCountOff:], uint32(n)) })
	}
	refusedWith(t, "delta -1", setDelta(-1), edgeRules(), RefuseInvalidRecord, "the batch's last offset delta is -1")
	refusedWith(t, "delta -5", setDelta(-5), edgeRules(), RefuseInvalidRecord, "the batch's last offset delta is -5")
	if r := refusedWith(t, "count 0", setCount(0), edgeRules(), RefuseInvalidRecord, "the batch says it holds 0 records"); r != nil && r.Error() != "the batch says it holds 0 records" {
		t.Errorf("count 0 is refused as %q, not for the count alone", r.Error())
	}
	refusedWith(t, "count -2", setCount(-2), edgeRules(), RefuseInvalidRecord, "the batch says it holds -2 records")
	// 3 records said, 5 by the offsets (last is record 4).
	three := mod(func(b []byte) {
		binary.BigEndian.PutUint32(b[batchRecordCountOff:], 3)
		binary.BigEndian.PutUint32(b[batchLastOffsetDeltaOff:], 4)
	})
	refusedWith(t, "3 vs 5", three, edgeRules(), RefuseInvalidRecord, "holds 3 records and that its last is record 4")
	// A control batch.
	refusedWith(t, "control", setAttrs(append([]byte{}, good...), func(a uint16) uint16 { return a | batchControlBit }), edgeRules(), RefuseInvalidRecord, "clients may not write control batches")

	// Idempotence fields: a producer id needs a sequence; none (-1, -1) needs nothing.
	pid := func(id int64, seq int32) []byte {
		return mod(func(b []byte) {
			binary.BigEndian.PutUint64(b[batchProducerIDOff:], uint64(id))
			binary.BigEndian.PutUint32(b[batchBaseSequenceOff:], uint32(seq))
		})
	}
	accepted(t, "no producer", pid(-1, -1), edgeRules())
	accepted(t, "pid 0 seq 0", pid(0, 0), edgeRules())
	accepted(t, "pid 9 seq 4", pid(9, 4), edgeRules())
	accepted(t, "no pid, a sequence", pid(-1, 3), edgeRules())
	refusedWith(t, "pid 0 seq -1", pid(0, -1), edgeRules(), RefuseInvalidRecord, "a batch with a producer id (0) and sequence -1")
	refusedWith(t, "pid 12 seq -1", pid(12, -1), edgeRules(), RefuseInvalidRecord, "a producer id (12) and sequence -1")
}

func TestCheckProduceZstdNeedsVersion7(t *testing.T) {
	packed, err := compress(codecZstd, EncodeRecords([]Record{rec(0, "k", "v")}))
	if err != nil {
		t.Fatal(err)
	}
	zs := withBody(codecZstd, 1, packed)
	for v, want := range map[int16]bool{5: false, 6: false, 7: true, 8: true} {
		rules := edgeRules()
		rules.Version = v
		_, r := CheckProduce(zs, rules)
		if want && r != nil {
			t.Errorf("version %d: zstd refused: %v", v, r)
		}
		if !want && (r == nil || r.Kind != RefuseCompression || !strings.Contains(r.Error(), fmt.Sprintf("this is version %d", v))) {
			t.Errorf("version %d: zstd: %+v, want UNSUPPORTED_COMPRESSION_TYPE", v, r)
		}
	}
	// The other codecs are fine on an old version.
	for _, codec := range []int{codecNone, codecGzip, codecSnappy, codecLZ4} {
		p, _ := compress(codec, EncodeRecords([]Record{rec(0, "k", "v")}))
		rules := edgeRules()
		rules.Version = 3
		if _, r := CheckProduce(withBody(codec, 1, p), rules); r != nil {
			t.Errorf("codec %d at version 3: %v", codec, r)
		}
	}
}

// --- the target codec ---

func records3() []Record {
	return []Record{rec(0, "k0", strings.Repeat("x", 3000)), rec(1, "k1", strings.Repeat("y", 3000)), rec(2, "k2", strings.Repeat("z", 3000))}
}

func codecOf(b []byte) int { return int(attrsOf(b) & batchCompressionMask) }

func TestCheckProduceRewritesIntoTheTopicsCodec(t *testing.T) {
	recs := records3()
	body := EncodeRecords(recs)
	for _, tc := range []struct{ from, to int }{
		{codecNone, codecGzip}, {codecGzip, codecNone}, {codecGzip, codecSnappy}, {codecSnappy, codecGzip}, {codecLZ4, codecNone}, {codecNone, codecZstd},
	} {
		packed, _ := compress(tc.from, body)
		in := withBody(tc.from, 3, packed)
		rules := edgeRules()
		rules.Target = tc.to
		a := accepted(t, fmt.Sprintf("%d to %d", tc.from, tc.to), in, rules)
		if codecOf(a.Batch) != tc.to {
			t.Errorf("%d to %d: stored in codec %d", tc.from, tc.to, codecOf(a.Batch))
		}
		if !crcOK(a.Batch) {
			t.Errorf("%d to %d: the rewritten batch's checksum is wrong", tc.from, tc.to)
		}
		if a.Header.TotalBytes != len(a.Batch) {
			t.Errorf("%d to %d: header says %d bytes, the batch is %d", tc.from, tc.to, a.Header.TotalBytes, len(a.Batch))
		}
		got, err := DecodeRecords(a.Batch)
		if err != nil || len(got) != 3 || string(got[1].Key) != "k1" || string(got[2].Value) != strings.Repeat("z", 3000) {
			t.Errorf("%d to %d: stored records %v, %v", tc.from, tc.to, got, err)
		}
		// Kept as it came when the topic takes the producer's codec, or already this one.
		if tc.from != tc.to {
			same := edgeRules()
			same.Target = tc.from
			if a2 := accepted(t, "same codec", in, same); !bytes.Equal(a2.Batch, in) {
				t.Errorf("codec %d kept as it came but was changed", tc.from)
			}
		}
	}
	// The codec value that means "the producer's" is the one the test constant says,
	// and a codec number is not mistaken for it: a snappy batch on a gzip topic is
	// rewritten as gzip.
	packed, _ := compress(codecSnappy, body)
	rules := edgeRules()
	rules.Target = codecGzip
	if a := accepted(t, "snappy to gzip", withBody(codecSnappy, 3, packed), rules); codecOf(a.Batch) != codecGzip {
		t.Errorf("a snappy batch on a gzip topic was stored in codec %d", codecOf(a.Batch))
	}
}

func TestCheckProduceRewrittenRecordsAreNumberedFromZero(t *testing.T) {
	recs := []Record{rec(0, "a", "1"), rec(5, "b", "2"), rec(9, "c", "3")}
	in := withBody(codecNone, 3, EncodeRecords(recs))
	rules := edgeRules()
	rules.Target = codecGzip
	a := accepted(t, "renumber", in, rules)
	got, err := DecodeRecords(a.Batch)
	if err != nil || len(got) != 3 {
		t.Fatal(got, err)
	}
	for i, r := range got {
		if r.OffsetDelta != int32(i) {
			t.Errorf("record %d has offset delta %d", i, r.OffsetDelta)
		}
	}
	// A topic that keeps the producer's batch keeps those offsets when uncompressed,
	// and refuses them when compressed.
	rules.Target = TargetProducer
	if a := accepted(t, "kept", in, rules); !bytes.Equal(a.Batch, in) {
		t.Error("an uncompressed batch with offsets 0, 5, 9 was not kept as it came")
	}
	packed, _ := compress(codecGzip, EncodeRecords(recs))
	refusedWith(t, "compressed, not 0 1 2", withBody(codecGzip, 3, packed), rules, RefuseUnreadable, "not numbered 0, 1, 2")
	// Sequential compressed is fine.
	seq, _ := compress(codecGzip, EncodeRecords([]Record{rec(0, "a", "1"), rec(1, "b", "2"), rec(2, "c", "3")}))
	accepted(t, "compressed 0 1 2", withBody(codecGzip, 3, seq), rules)
}

func TestCheckProduceSizeLimitsOnTheStoredBatch(t *testing.T) {
	recs := records3()
	body := EncodeRecords(recs)
	gz, _ := compress(codecGzip, body)
	in := withBody(codecGzip, 3, gz)

	// To uncompressed: the stored size is the header and the records inflated.
	stored := int64(batchFullHeaderBytes + len(body))
	if int64(len(in)) >= stored {
		t.Fatal("the test batch does not compress")
	}
	rules := edgeRules()
	rules.Target = codecNone
	rules.MaxBatchBytes = stored
	a := accepted(t, "at the limit", in, rules)
	if int64(len(a.Batch)) != stored {
		t.Errorf("stored %d bytes, want %d", len(a.Batch), stored)
	}
	rules.MaxBatchBytes = stored - 1
	r := refusedWith(t, "one over", in, rules, RefuseTooLarge, fmt.Sprintf("is %d bytes, more than max.message.bytes (%d)", stored, stored-1))
	if r != nil && !r.BeforeLog {
		t.Error("not refused before the log")
	}

	// To a compressed codec: the size held is the rewritten one, exactly.
	rules = edgeRules()
	rules.Target = codecSnappy
	big := accepted(t, "snappy target", in, rules)
	n := int64(len(big.Batch))
	if int64(len(in)) >= n-1 {
		t.Fatalf("the gzip batch (%d bytes) is not smaller than the snappy one (%d)", len(in), n)
	}
	rules.MaxBatchBytes = n
	accepted(t, "rewritten at the limit", in, rules)
	rules.MaxBatchBytes = n - 1
	refusedWith(t, "rewritten one over", in, rules, RefuseTooLarge, fmt.Sprintf("is %d bytes, more than max.message.bytes (%d)", n, n-1))
}

// A batch to be stored uncompressed is held to the limit on the size its records
// inflate to before they are written out again; offsets that shrink as they are
// renumbered do not let it through.
func TestCheckProduceUncompressedTargetIsHeldToTheInflatedSize(t *testing.T) {
	recs := []Record{rec(0, "k", strings.Repeat("x", 500)), rec(400, "k", strings.Repeat("y", 500)), rec(900, "k", strings.Repeat("z", 500))}
	body := EncodeRecords(recs)
	gz, _ := compress(codecGzip, body)
	in := withBody(codecGzip, 3, gz)
	arrived := int64(batchFullHeaderBytes + len(body))

	rules := edgeRules()
	rules.Target = codecNone
	rules.MaxBatchBytes = arrived - 1 // the rewritten batch (offsets 0, 1, 2) would be smaller than this.
	refusedWith(t, "inflated size", in, rules, RefuseTooLarge, fmt.Sprintf("is %d bytes, more than max.message.bytes (%d)", arrived, arrived-1))
}

// --- the header the log stores ---

func TestCheckProduceSetsTheMaxTimestampAndClearsTheTimestampTypeBit(t *testing.T) {
	// Deltas 100, 300, 200 from 1,000,000: the maximum is 1,000,300.
	recs := []Record{
		{TimestampDelta: 100, OffsetDelta: 0, Key: []byte("a"), Value: []byte("1")},
		{TimestampDelta: 300, OffsetDelta: 1, Key: []byte("b"), Value: []byte("2")},
		{TimestampDelta: 200, OffsetDelta: 2, Key: []byte("c"), Value: []byte("3")},
	}
	build := func(max int64, lat bool) []byte {
		b := rawBatch(3, EncodeRecords(recs), tNow, max)
		if lat {
			setAttrs(b, func(a uint16) uint16 { return a | batchLogAppendTimeBit })
		}
		return b
	}
	check := func(name string, in []byte) {
		t.Helper()
		a := accepted(t, name, in, edgeRules())
		if got := int64(binary.BigEndian.Uint64(a.Batch[batchMaxTimestampOff:])); got != tNow+300 {
			t.Errorf("%s: max timestamp %d, want %d", name, got, tNow+300)
		}
		if attrsOf(a.Batch)&batchLogAppendTimeBit != 0 {
			t.Errorf("%s: the timestamp-type bit is still set", name)
		}
		if !crcOK(a.Batch) {
			t.Errorf("%s: checksum not resealed", name)
		}
	}
	check("wrong max, bit clear", build(5, false))
	check("wrong max, bit set", build(5, true))
	check("right max, bit set", build(tNow+300, true))

	right := build(tNow+300, false)
	a := accepted(t, "right", right, edgeRules())
	if !bytes.Equal(a.Batch, right) {
		t.Error("a batch whose header is right was changed")
	}

	// On a LogAppendTime topic the header is the broker's to set later and is left alone.
	rules := edgeRules()
	rules.LogAppendTime = true
	wrong := build(5, false)
	a = accepted(t, "log append time", wrong, rules)
	if got := int64(binary.BigEndian.Uint64(a.Batch[batchMaxTimestampOff:])); got != 5 {
		t.Errorf("a LogAppendTime topic's max timestamp was changed to %d", got)
	}
}

func TestCheckProduceMaxTimestampOfRecordsWithNoTimestamp(t *testing.T) {
	// A batch whose records have no timestamp (-1) is not held to the clock, and its
	// maximum is -1.
	body := auto(fields(0, 0, 1, "k", 1, "v", vb(0)))
	b := rawBatch(1, body, -1, 12345)
	rules := edgeRules()
	rules.BeforeMaxMs, rules.AfterMaxMs = 10, 10
	a := accepted(t, "no timestamp", b, rules)
	if got := int64(binary.BigEndian.Uint64(a.Batch[batchMaxTimestampOff:])); got != -1 {
		t.Errorf("max timestamp %d, want -1", got)
	}
	// And with the type bit set by a client, it is refused (nothing to check it against).
	stamped := setAttrs(rawBatch(1, body, -1, -1), func(a uint16) uint16 { return a | batchLogAppendTimeBit })
	refusedWith(t, "stamped, no timestamp", stamped, rules, RefuseTimestamp, "")
}

// --- timestamps ---

func TestCheckProduceTimestampBoundsAreInclusive(t *testing.T) {
	rules := edgeRules()
	rules.AfterMaxMs, rules.BeforeMaxMs = 100, 200
	at := func(delta int64) []byte {
		return rawBatch(1, auto(fields(delta, 0, 1, "k", 1, "v", vb(0))), tNow, tNow+delta)
	}
	accepted(t, "now", at(0), rules)
	accepted(t, "100 ahead", at(100), rules)
	accepted(t, "200 behind", at(-200), rules)
	want := fmt.Sprintf("Timestamp %d of message with offset 0 is out of range. The timestamp should be within [%d, %d]", tNow+101, tNow-200, tNow+100)
	r := refusedWith(t, "101 ahead", at(101), rules, RefuseTimestamp, "")
	if r != nil {
		if len(r.Records) != 1 || r.Records[0].Index != 0 || r.Records[0].Message != want {
			t.Errorf("101 ahead names %+v, want %q", r.Records, want)
		}
		if r.KafkaMessage != "One or more records have been rejected due to invalid timestamp" {
			t.Errorf("KafkaMessage = %q", r.KafkaMessage)
		}
	}
	r = refusedWith(t, "201 behind", at(-201), rules, RefuseTimestamp, "")
	if r != nil {
		want = fmt.Sprintf("Timestamp %d of message with offset 0 is out of range. The timestamp should be within [%d, %d]", tNow-201, tNow-200, tNow+100)
		if len(r.Records) != 1 || r.Records[0].Message != want {
			t.Errorf("201 behind names %+v, want %q", r.Records, want)
		}
	}
}

func TestCheckProduceTimestampWindowWithNoBeforeBound(t *testing.T) {
	// "No bound" is MaxInt64; the window the message prints wraps as Kafka's does.
	rules := edgeRules()
	rules.BeforeMaxMs = math.MaxInt64
	rules.AfterMaxMs = math.MaxInt64
	b := rawBatch(1, auto(fields(0, 0, 1, "k", 1, "v", vb(0))), tNow, tNow)
	accepted(t, "unbounded", b, rules)
	if got := add64(math.MaxInt64, 1); got != math.MinInt64 {
		t.Errorf("add64 wraps to %d", got)
	}
	if got := sub64(math.MinInt64, 1); got != math.MaxInt64 {
		t.Errorf("sub64 wraps to %d", got)
	}
	if add64(5, 3) != 8 || sub64(5, 3) != 2 || add64(-5, 3) != -2 || sub64(3, 5) != -2 {
		t.Errorf("add64/sub64: %d %d %d %d", add64(5, 3), sub64(5, 3), add64(-5, 3), sub64(3, 5))
	}
	// The producer's clock a long way behind is refused under a finite bound.
	rules.BeforeMaxMs = 1000
	old := rawBatch(1, auto(fields(0, 0, 1, "k", 1, "v", vb(0))), tNow-5000, tNow-5000)
	refusedWith(t, "behind", old, rules, RefuseTimestamp, "")
}

func TestCheckProduceTimestampIsFirstPlusDelta(t *testing.T) {
	// The record's time is the batch's first timestamp plus its delta: 400 behind a
	// clock whose bound is 300.
	rules := edgeRules()
	rules.BeforeMaxMs = 300
	b := rawBatch(1, auto(fields(50, 0, 1, "k", 1, "v", vb(0))), tNow-450, tNow-400)
	refusedWith(t, "first plus delta", b, rules, RefuseTimestamp, fmt.Sprintf("")) // 400 behind.
	ok := rawBatch(1, auto(fields(200, 0, 1, "k", 1, "v", vb(0))), tNow-450, tNow-250)
	accepted(t, "first plus delta, in range", ok, rules) // 250 behind, though first alone is 450 behind.
}

func TestCheckProduceClientMayNotStampLogAppendTime(t *testing.T) {
	rules := edgeRules()
	rules.LogAppendTime = true
	body := cat(auto(fields(7, 0, 2, "ab", 3, "xyz", vb(0))), auto(fields(8, 1, -1, "", -1, "", vb(0))))
	clean := rawBatch(2, body, 5000, 5008)
	a := accepted(t, "log append time topic", clean, rules)
	if !bytes.Equal(a.Batch, clean) {
		t.Error("a LogAppendTime topic changed an unstamped batch")
	}
	stamped := setAttrs(rawBatch(2, body, 5000, 5008), func(a uint16) uint16 { return a | batchLogAppendTimeBit })
	r := refusedWith(t, "stamped", stamped, rules, RefuseTimestamp, "")
	if r == nil {
		return
	}
	w0 := "Invalid timestamp type in message DefaultRecord(offset=0, timestamp=5007, key=2 bytes, value=3 bytes). Producer should not set timestamp type to LogAppendTime."
	w1 := "Invalid timestamp type in message DefaultRecord(offset=1, timestamp=5008, key=0 bytes, value=0 bytes). Producer should not set timestamp type to LogAppendTime."
	if len(r.Records) != 2 || r.Records[0] != (RecordRefusal{0, w0}) || r.Records[1] != (RecordRefusal{1, w1}) {
		t.Errorf("records named: %+v", r.Records)
	}
}

// --- keys on a compacting topic ---

func TestCheckProduceRequireKeys(t *testing.T) {
	rules := edgeRules()
	rules.RequireKeys = true
	rules.TopicPartition = "orders-3"
	keyed := cat(auto(fields(0, 0, 0, "", 1, "v", vb(0))), auto(fields(0, 1, 1, "k", 1, "v", vb(0))))
	accepted(t, "empty and set keys", rawBatch(2, keyed, 0, 0), rules)

	// Five keyless records of seven: all named, the first three shown.
	var body []byte
	for i := 0; i < 7; i++ {
		if i == 2 || i == 5 {
			body = append(body, auto(fields(0, int64(i), 1, "k", 1, "v", vb(0)))...)
		} else {
			body = append(body, auto(fields(0, int64(i), -1, "", 1, "v", vb(0)))...)
		}
	}
	r := refusedWith(t, "keyless", rawBatch(7, body, 0, 0), rules, RefuseInvalidRecord, "")
	if r == nil {
		return
	}
	var idx []int32
	for _, x := range r.Records {
		idx = append(idx, x.Index)
		if x.Message != "Compacted topic cannot accept message without key in topic partition orders-3" {
			t.Errorf("message %q", x.Message)
		}
	}
	if fmt.Sprint(idx) != "[0 1 3 4 6]" {
		t.Errorf("records named %v", idx)
	}
	m := "Compacted topic cannot accept message without key in topic partition orders-3"
	want := fmt.Sprintf("One or more records have been rejected due to 5 record errors in total, and only showing the first three errors at most: [RecordError(batchIndex=0, message='%s'), RecordError(batchIndex=1, message='%s'), RecordError(batchIndex=3, message='%s')]", m, m, m)
	if r.KafkaMessage != want {
		t.Errorf("KafkaMessage = %q\nwant %q", r.KafkaMessage, want)
	}
	// Two keyless: shows both.
	two := cat(auto(fields(0, 0, -1, "", 1, "v", vb(0))), auto(fields(0, 1, -1, "", 1, "v", vb(0))))
	r = refusedWith(t, "two keyless", rawBatch(2, two, 0, 0), rules, RefuseInvalidRecord, "2 record errors in total")
	if r != nil && !strings.HasSuffix(r.KafkaMessage, fmt.Sprintf("[RecordError(batchIndex=0, message='%s'), RecordError(batchIndex=1, message='%s')]", m, m)) {
		t.Errorf("KafkaMessage = %q", r.KafkaMessage)
	}
	// A topic that does not require keys takes them.
	rules.RequireKeys = false
	accepted(t, "keyless allowed", rawBatch(2, two, 0, 0), rules)
}

// A timestamp refusal names keyless and out-of-range records together, in record order.
func TestCheckProduceNamesKeylessAndLateRecordsInOrder(t *testing.T) {
	rules := edgeRules()
	rules.RequireKeys = true
	rules.AfterMaxMs = 10
	far := int64(1000) // ahead of the clock by more than 10.
	body := cat(
		auto(fields(0, 0, -1, "", 1, "v", vb(0))),   // keyless
		auto(fields(far, 1, 1, "k", 1, "v", vb(0))), // late
		auto(fields(0, 2, -1, "", 1, "v", vb(0))),   // keyless
		auto(fields(far, 3, 1, "k", 1, "v", vb(0))), // late
		auto(fields(0, 4, 1, "k", 1, "v", vb(0))),   // fine
	)
	r := refusedWith(t, "mixed", rawBatch(5, body, tNow, tNow+far), rules, RefuseTimestamp, "")
	if r == nil {
		return
	}
	var idx []int32
	for _, x := range r.Records {
		idx = append(idx, x.Index)
	}
	if fmt.Sprint(idx) != "[0 1 2 3]" {
		t.Errorf("records named %v, want [0 1 2 3]", idx)
	}
	if !strings.HasPrefix(r.Records[0].Message, "Compacted topic") || !strings.HasPrefix(r.Records[1].Message, "Timestamp ") {
		t.Errorf("messages: %+v", r.Records)
	}
}

func TestSortRefusals(t *testing.T) {
	mk := func(idx ...int32) []RecordRefusal {
		var out []RecordRefusal
		for i, x := range idx {
			out = append(out, RecordRefusal{Index: x, Message: fmt.Sprint(i)})
		}
		return out
	}
	rs := mk(5, 3, 9, 1, 3, 0, 5)
	sortRefusals(rs)
	var idx, order []string
	for _, r := range rs {
		idx = append(idx, fmt.Sprint(r.Index))
		order = append(order, r.Message)
	}
	// Ascending, and records with the same index keep the order they came in.
	if strings.Join(idx, " ") != "0 1 3 3 5 5 9" || strings.Join(order, " ") != "5 3 1 4 0 6 2" {
		t.Errorf("sorted to indexes %v from positions %v", idx, order)
	}
	one := mk(4)
	sortRefusals(one)
	sortRefusals(nil)
	two := mk(2, 1)
	sortRefusals(two)
	if one[0].Index != 4 || two[0].Index != 1 || two[1].Index != 2 {
		t.Errorf("%v %v", one, two)
	}
	sorted := mk(1, 2, 3)
	sortRefusals(sorted)
	if sorted[0].Message != "0" || sorted[1].Message != "1" || sorted[2].Message != "2" {
		t.Errorf("an ordered list was reordered: %v", sorted)
	}
}

// --- the records themselves ---

func TestScanRecordsRefusalsNameTheFault(t *testing.T) {
	trail := bytes.Repeat([]byte{0}, 12) // bytes after the record, so that the section does not simply end.
	cases := []struct {
		name  string
		count int32
		body  []byte
		want  string
	}{
		{"length 0", 1, cat(vb(0), trail), "record 0 of 1 is cut short"},
		{"length negative", 1, cat(vb(-1), trail), "record 0 of 1 has no length"},
		{"key runs past, record ends at the key length", 1,
			cat(withLen(4, cat([]byte{0}, vb(0), vb(0), vb(5))), trail), "record 0 of 1 has a key that runs past the record"},
		{"key fills the record", 1,
			cat(withLen(7, cat([]byte{0}, vb(0), vb(0), vb(3), []byte("abc"))), trail), "record 0 of 1 is cut short"},
		{"key a little long", 1,
			cat(withLen(7, cat([]byte{0}, vb(0), vb(0), vb(4), []byte("abc"))), trail), "record 0 of 1 has a key that runs past the record"},
		{"value runs past, record ends at the value length", 1,
			cat(withLen(5, cat([]byte{0}, vb(0), vb(0), vb(-1), vb(3))), trail), "record 0 of 1 has a value that runs past the record"},
		{"value fills the record", 1,
			cat(withLen(8, cat([]byte{0}, vb(0), vb(0), vb(-1), vb(3), []byte("abc"))), trail), "record 0 of 1 has a header count that cannot be"},
		{"value a little long", 1,
			cat(withLen(8, cat([]byte{0}, vb(0), vb(0), vb(-1), vb(4), []byte("abc"))), trail), "record 0 of 1 has a value that runs past the record"},
		{"negative header count", 1, auto(cat([]byte{0}, vb(0), vb(0), vb(-1), vb(-1), vb(-1))), "has a header count that cannot be"},
		{"header count past the record", 1, auto(cat([]byte{0}, vb(0), vb(0), vb(-1), vb(-1), vb(5))), "has a header count that cannot be"},
		{"header key length negative", 1, auto(cat([]byte{0}, vb(0), vb(0), vb(-1), vb(-1), vb(1), vb(-1), vb(0))), "has a header whose key cannot be"},
		{"header key past the record", 1, auto(cat([]byte{0}, vb(0), vb(0), vb(-1), vb(-1), vb(1), vb(5))), "has a header whose key cannot be"},
		{"header key fills the record", 1,
			cat(withLen(9, cat([]byte{0}, vb(0), vb(0), vb(-1), vb(-1), vb(1), vb(2), []byte("ab"))), trail), "has a header whose value runs past the record"},
		{"header value past the record", 1, auto(cat([]byte{0}, vb(0), vb(0), vb(-1), vb(-1), vb(1), vb(1), []byte("k"), vb(5))), "has a header whose value runs past the record"},
		{"length says more than the fields", 1,
			cat(withLen(20, fields(0, 0, 1, "k", 1, "v", vb(0))), trail), "record 0 of 1 says it is 20 bytes long and its fields are 8"},
		{"bytes after the last record", 1, cat(auto(fields(0, 0, 1, "k", 1, "v", vb(0))), []byte{9}), "the batch says it holds 1 records and more bytes follow them"},
		{"second record missing", 2, auto(fields(0, 0, 1, "k", 1, "v", vb(0))), "record 1 of 2 has no length"},
	}
	for _, tc := range cases {
		b := rawBatch(tc.count, tc.body, 0, 0)
		r := refusedWith(t, tc.name, b, edgeRules(), RefuseInvalidRecord, tc.want)
		if r != nil && r.BeforeLog {
			t.Errorf("%s: refused before the log", tc.name)
		}
	}
}

func TestScanRecordsAcceptsRecordsAtTheirEdges(t *testing.T) {
	ok := [][]byte{
		// Empty (not null) key, value and header key; a null header value.
		auto(fields(0, 0, 0, "", 0, "", cat(vb(1), vb(0), vb(-1)))),
		// Two headers, the last value filling the record to its end.
		auto(fields(0, 0, 1, "k", 1, "v", cat(vb(2), vb(1), []byte("a"), vb(1), []byte("1"), vb(1), []byte("b"), vb(2), []byte("22")))),
		// A negative length other than -1 is a null value, as Kafka reads it.
		auto(fields(0, 0, -7, "", -9, "", vb(0))),
	}
	for i, body := range ok {
		accepted(t, fmt.Sprint("record ", i), rawBatch(1, body, 0, 0), edgeRules())
	}
	// Every kind of header, through the encoder, in every codec.
	recs := []Record{{OffsetDelta: 0, Key: []byte("k"), Value: []byte("v"), Headers: []RecordHeader{{Key: "a", Value: []byte("1")}, {Key: "b"}, {Key: "", Value: []byte("zz")}}}}
	for _, codec := range []int{codecNone, codecGzip, codecSnappy, codecLZ4, codecZstd} {
		p, _ := compress(codec, EncodeRecords(recs))
		accepted(t, fmt.Sprint("headers, codec ", codec), withBody(codec, 1, p), edgeRules())
	}
}

// --- the reader and its sources ---

func TestSliceSourceReadsAndDiscards(t *testing.T) {
	s := &sliceSource{b: []byte{1, 2, 3}}
	for want := byte(1); want <= 3; want++ {
		if c, err := s.ReadByte(); err != nil || c != want {
			t.Fatalf("ReadByte = %d, %v, want %d", c, err, want)
		}
	}
	if c, err := s.ReadByte(); err != io.EOF || c != 0 {
		t.Errorf("ReadByte past the end = %d, %v", c, err)
	}
	s = &sliceSource{b: []byte{1, 2, 3}}
	if n, err := s.Discard(3); n != 3 || err != nil {
		t.Errorf("Discard of exactly what is left = %d, %v", n, err)
	}
	if n, err := s.Discard(0); n != 0 || err != nil {
		t.Errorf("Discard(0) at the end = %d, %v", n, err)
	}
	s = &sliceSource{b: []byte{1, 2, 3}}
	s.ReadByte()
	if n, err := s.Discard(5); n != 2 || err != io.EOF {
		t.Errorf("Discard past the end = %d, %v, want 2, EOF", n, err)
	}
	if n, err := s.Discard(1); n != 0 || err != io.EOF {
		t.Errorf("Discard at the end = %d, %v, want 0, EOF", n, err)
	}
	s = &sliceSource{b: []byte{1, 2, 3, 4}}
	if n, err := s.Discard(2); n != 2 || err != nil {
		t.Fatalf("Discard(2) = %d, %v", n, err)
	}
	if c, _ := s.ReadByte(); c != 3 {
		t.Errorf("after discarding 2, the next byte is %d", c)
	}
}

func TestReaderVarintAndSkip(t *testing.T) {
	read := func(b ...byte) (int64, bool, error) {
		r := &reader{src: &sliceSource{b: b}}
		v, ok := r.varint()
		return v, ok, r.err
	}
	for _, tc := range []struct {
		in   []byte
		want int64
	}{
		{[]byte{0x00}, 0}, {[]byte{0x01}, -1}, {[]byte{0x02}, 1}, {[]byte{0x03}, -2}, {[]byte{0x04}, 2},
		{[]byte{0xac, 0x02}, 150}, {[]byte{0xab, 0x02}, -150},
		// Ten bytes: the last may be 0 or 1.
		{[]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}, 1 << 62},
		{[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, math.MinInt64},
		{[]byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, math.MaxInt64},
		{[]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00}, 0},
	} {
		if v, ok, err := read(tc.in...); v != tc.want || !ok || err != nil {
			t.Errorf("varint % x = %d, %v, %v; want %d", tc.in, v, ok, err, tc.want)
		}
	}
	if _, ok, _ := read(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02); ok {
		t.Error("a varint over 64 bits was taken")
	}
	if _, ok, _ := read(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0xff); ok {
		t.Error("a varint over 64 bits was taken")
	}
	// A source that ends inside one is the reader's error.
	if _, _, err := read(0x80, 0x80); err != io.EOF {
		t.Errorf("a varint cut short: err = %v", err)
	}

	r := &reader{src: &sliceSource{b: []byte{1, 2, 3}}}
	r.skip(0)
	r.skip(-4)
	if r.err != nil || r.n != 0 {
		t.Errorf("skip of nothing: n %d, err %v", r.n, r.err)
	}
	r.skip(2)
	if r.err != nil || r.n != 2 {
		t.Errorf("skip(2): n %d, err %v", r.n, r.err)
	}
	r.skip(5)
	if r.err != io.EOF || r.n != 3 {
		t.Errorf("skip past the end: n %d, err %v, want 3, EOF", r.n, r.err)
	}
	// Once failed, nothing more is read.
	if c := r.byte(); c != 0 || r.n != 3 {
		t.Errorf("byte after a failure = %d, n %d", c, r.n)
	}
}

// skip(0) must return: a loop that never ends on it would hang a produce request.
func TestReaderSkipOfZeroReturns(t *testing.T) {
	done := make(chan struct{})
	go func() {
		r := &reader{src: &sliceSource{b: []byte{1}}}
		r.skip(0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("skip(0) did not return")
	}
}

// --- zstd frames ---

func TestZstdDeclaredWindow(t *testing.T) {
	magic := []byte{0x28, 0xb5, 0x2f, 0xfd}
	frame := func(parts ...[]byte) []byte { return cat(append([][]byte{magic}, parts...)...) }
	type tc struct {
		name  string
		frame []byte
		want  uint64
		ok    bool
	}
	for _, c := range []tc{
		{"empty", nil, 0, false},
		{"five bytes", frame([]byte{0x00}), 0, false},
		{"wrong magic", cat([]byte{0x28, 0xb5, 0x2f, 0xfc}, []byte{0x00, 0x00}), 0, false},
		{"six bytes, w=0", frame([]byte{0x00, 0x00}), 1024, true},
		{"w=0x0d", frame([]byte{0x00, 0x0d}), 2048 + 2048/8*5, true},
		{"w=8", frame([]byte{0x00, 0x08}), 2048, true},
		{"w=0x07", frame([]byte{0x00, 0x07}), 1024 + 1024/8*7, true},
		{"128 MiB", frame([]byte{0x00, 136}), 128 << 20, true},
		{"128 MiB and an eighth", frame([]byte{0x00, 137}), 128<<20 + 16<<20, true},
		// Single segment (0x20): the window is the content size, 1 byte (flag 0).
		{"single, 1 byte", frame([]byte{0x20, 0x7f}), 0x7f, true},
		{"single, 2 bytes", frame([]byte{0x60, 0x10, 0x01}), 0x0110 + 256, true},
		{"single, 4 bytes", frame([]byte{0xa0, 0x01, 0x02, 0x03, 0x04}), 0x04030201, true},
		{"single, 8 bytes", frame([]byte{0xe0, 1, 0, 0, 0, 0, 0, 0, 0x10}), 0x1000000000000001, true},
		// Cut short in the content size.
		{"single, 2 bytes, one given", frame([]byte{0x60, 0x10}), 0, false},
		{"single, 4 bytes, three given", frame([]byte{0xa0, 1, 2, 3}), 0, false},
		{"single, 8 bytes, seven given", frame([]byte{0xe0, 1, 2, 3, 4, 5, 6, 7}), 0, false},
	} {
		got, ok := zstdDeclaredWindow(c.frame)
		if got != c.want || ok != c.ok {
			t.Errorf("%s: %d, %v; want %d, %v", c.name, got, ok, c.want, c.ok)
		}
	}
}

func zstdFrameBatch(desc, window byte) []byte {
	frame := []byte{0x28, 0xb5, 0x2f, 0xfd, desc, window, 1, 2, 3, 4, 5, 6, 7, 8}
	return withBody(codecZstd, 1, frame)
}

// A zstd frame declaring a window past Kafka's 128 MiB is refused as unreadable, saying
// so; one at exactly 128 MiB is not refused for that (this broker's own inflate limit
// refuses it later, as too large).
func TestCheckProduceZstdWindowLimit(t *testing.T) {
	_, r := CheckProduce(zstdFrameBatch(0, 137), edgeRules())
	if r == nil || r.Kind != RefuseUnreadable || !strings.Contains(r.Error(), fmt.Sprintf("a zstd frame that declares a window of %d bytes", 128<<20+16<<20)) {
		t.Errorf("a window past 128 MiB: %+v", r)
	}
	_, r = CheckProduce(zstdFrameBatch(0, 136), edgeRules())
	if r == nil {
		t.Fatal("a garbage zstd frame was accepted")
	}
	if strings.Contains(r.Error(), "declares a window of") {
		t.Errorf("exactly 128 MiB was refused for its window: %q", r.Error())
	}
	// A gzip batch is not looked at for a window, whatever its bytes.
	p, _ := compress(codecGzip, EncodeRecords([]Record{rec(0, "k", "v")}))
	accepted(t, "gzip", withBody(codecGzip, 1, p), edgeRules())
}
