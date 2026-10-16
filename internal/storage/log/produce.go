package log

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
)

// What a Produce request's records for one partition are held to before anything is
// stored. The rule is Apache Kafka's, taken from recordings of 4.3.1
// (conformance/recordings/produce-validation.json and produce-inflated-size.json):
// which batches it refuses and with which error. The log used to keep whatever had a
// plausible header and a matching checksum, on every topic that did not compact, so
// a records section that was not gzip at all, a batch claiming five records and
// holding one, or two batches where one is allowed, were acknowledged and stored,
// and every reader that later looked inside (a timestamp lookup, the cleaner, a
// consumer) failed on what the broker had said yes to.
//
// It is one streaming pass over the records, compressed or not: what is held is a
// decompressor's window and a buffer, never the inflated payload.
//
// Three things are stricter than Kafka, each because this broker's own readers (and
// every consumer) would not be able to read what Kafka stores:
//
//   - a record whose length disagrees with its fields is refused on every topic
//     (Kafka reads a compressed batch it stores as it came with an iterator that
//     skips fields without checking the length, and stores it);
//   - a batch that inflates past MaxInflatedBytes is refused;
//   - a zstd frame declaring a window past zstdMaxWindow is refused, because the
//     decoder allocates the window it is told before reading a byte.

// DefaultMaxMessageBytes is Kafka's default for the broker's message.max.bytes and a
// topic's max.message.bytes: the largest record batch a topic that sets no limit of
// its own accepts (recorded: a batch of 1048588 bytes is stored, one of 1048589 is
// MESSAGE_TOO_LARGE).
const DefaultMaxMessageBytes = 1048588

// DefaultTimestampAfterMaxMs is Kafka's default for message.timestamp.after.max.ms:
// how far ahead of the broker's clock a CreateTime record may be stamped. (The
// default for message.timestamp.before.max.ms is no bound.)
const DefaultTimestampAfterMaxMs = 3_600_000

// kafkaZstdMaxWindow is the largest window Apache Kafka's zstd decoder takes
// (recorded: 128 MiB is accepted, 256 MiB is UNKNOWN_SERVER_ERROR).
const kafkaZstdMaxWindow = 128 << 20

// TargetProducer is ProduceRules.Target for compression.type=producer: the topic
// stores a batch in the codec it arrived in.
const TargetProducer = -1

// RefusalKind is why a batch is refused, as the error Kafka answers.
type RefusalKind int

const (
	RefuseInvalidRecord RefusalKind = iota + 1 // INVALID_RECORD (87)
	RefuseCorrupt                              // CORRUPT_MESSAGE (2)
	RefuseTooLarge                             // MESSAGE_TOO_LARGE (10)
	RefuseUnreadable                           // UNKNOWN_SERVER_ERROR (-1): Kafka's answer for a records section it cannot decompress
	RefuseTimestamp                            // INVALID_TIMESTAMP (32)
	RefuseCompression                          // UNSUPPORTED_COMPRESSION_TYPE (76)
)

// RecordRefusal is one record a refusal names (Produce v8+ carries them).
type RecordRefusal struct {
	Index   int32
	Message string
}

// Refusal is a batch that is not stored, and what the producer is told.
type Refusal struct {
	Kind RefusalKind
	// Reason is this broker's own sentence, for a refusal Kafka gives no text for.
	Reason string
	// KafkaMessage is the error message Kafka's answer carries, word for word, when
	// it carries one; Records are the records it names.
	KafkaMessage string
	Records      []RecordRefusal
	// BeforeLog is set for a refusal Kafka makes before it has asked the partition's
	// log anything (or as a known kind of append failure): the answer then carries
	// no log start offset (-1) where every other answer carries the partition's.
	BeforeLog bool
}

func (r *Refusal) Error() string {
	if r.KafkaMessage != "" {
		return r.KafkaMessage
	}
	return r.Reason
}

// Message is the text for the producer: Kafka's when it has one.
func (r *Refusal) Message() string { return r.Error() }

func refuse(kind RefusalKind, beforeLog bool, format string, args ...any) *Refusal {
	return &Refusal{Kind: kind, BeforeLog: beforeLog, Reason: fmt.Sprintf(format, args...)}
}

// ProduceRules is the topic's say in what a batch must be.
type ProduceRules struct {
	Version int16 // of the Produce request: zstd came with version 7.
	// MaxBatchBytes is max.message.bytes in force: the topic's, else the broker's.
	MaxBatchBytes int64
	// RequireKeys refuses a record with no key (a topic that compacts).
	RequireKeys bool
	// Target is the codec the topic stores (compression.type): TargetProducer, 0 for
	// uncompressed, or a codec. A batch in another codec is written out again in it.
	Target int
	// LogAppendTime is message.timestamp.type=LogAppendTime: records take the broker's
	// clock, so a producer's timestamps are not held to it.
	LogAppendTime bool
	// NowMs is the broker's clock; BeforeMaxMs and AfterMaxMs are how far behind and
	// ahead of it a CreateTime record may be stamped.
	NowMs, BeforeMaxMs, AfterMaxMs int64
	// TopicPartition is "topic-partition", for the messages that name it.
	TopicPartition string
}

// AcceptedBatch is a batch that passed.
type AcceptedBatch struct {
	// Batch is what to append: the one batch of the request, without any bytes that
	// followed it. It aliases the request's buffer unless the topic's codec is not the
	// batch's, when it is the batch written out again in the topic's.
	Batch  []byte
	Header BatchHeader
}

// firstBatch finds the first record batch of a Produce payload as Kafka's reader
// does, and says what follows it. ok is false when there is not a whole batch.
func firstBatch(records []byte) (batch, rest []byte, ok bool, corrupt *Refusal) {
	// Too few bytes for the fields that say what this is: no batch.
	const upToMagic = batchMagicOff + 1
	if len(records) < upToMagic {
		return nil, nil, false, nil
	}
	size := int64(int32(binary.BigEndian.Uint32(records[batchLengthOff:])))
	if size < legacyRecordOverhead {
		r := refuse(RefuseCorrupt, true, "a record batch that says it is %d bytes long, less than any can be", size)
		r.KafkaMessage = fmt.Sprintf("Record size %d is less than the minimum record overhead (%d)", size, legacyRecordOverhead)
		return nil, nil, false, r
	}
	if int64(len(records)) < size+batchOverhead {
		return nil, nil, false, nil // the batch says it is longer than what was sent.
	}
	if magic := int8(records[batchMagicOff]); magic < 0 || magic > batchMagicV2 {
		r := refuse(RefuseCorrupt, true, "a record batch of format version %d", magic)
		r.KafkaMessage = fmt.Sprintf("Invalid magic found in record: %d", magic)
		return nil, nil, false, r
	}
	return records[:size+batchOverhead], records[size+batchOverhead:], true, nil
}

// legacyRecordOverhead is the smallest a message of the formats before version 2 can
// be; Kafka's reader refuses a size below it whatever the format.
const legacyRecordOverhead = 14

// RequestRefusal is what the whole Produce request is answered with, on every
// partition, when the records of any partition cannot even be told apart into
// batches: Kafka looks at every partition's first batch (to see whether the request
// is transactional) before it handles any, and fails the request there.
// transactional reports whether a first batch has the transactional bit.
func RequestRefusal(partitions [][]byte) (transactional bool, refusal *Refusal) {
	for _, records := range partitions {
		batch, _, ok, corrupt := firstBatch(records)
		if corrupt != nil {
			return false, corrupt
		}
		if ok && batch[batchMagicOff] == batchMagicV2 && len(batch) >= batchAttributesOff+2 && attrsOf(batch)&batchTransactionalBit != 0 {
			transactional = true
		}
	}
	return transactional, nil
}

// CheckProduce holds one partition's records to the rules. A refusal means nothing
// may be stored. On success the batch's maximum timestamp has been set to the latest
// of its records', as Kafka sets it (a producer may write anything there, and the
// time index and retention read it).
func CheckProduce(records []byte, rules ProduceRules) (AcceptedBatch, *Refusal) {
	// --- the request's shape: exactly one version 2 batch ---
	batch, rest, ok, corrupt := firstBatch(records)
	switch {
	case corrupt != nil:
		return AcceptedBatch{}, corrupt
	case !ok:
		return AcceptedBatch{}, refuse(RefuseInvalidRecord, true, "the records hold no whole record batch (%d bytes)", len(records))
	case batch[batchMagicOff] != batchMagicV2:
		return AcceptedBatch{}, refuse(RefuseInvalidRecord, true, "a record batch of format version %d; only version 2 is taken", batch[batchMagicOff])
	}
	short := len(batch) < batchFullHeaderBytes
	if !short && rules.Version < 7 && attrsOf(batch)&batchCompressionMask == codecZstd {
		return AcceptedBatch{}, refuse(RefuseCompression, true, "zstd needs Produce version 7 or later, and this is version %d", rules.Version)
	}
	if _, _, more, corrupt := firstBatch(rest); corrupt != nil {
		// Found while looking for a second batch, Kafka's answer has no text.
		corrupt.KafkaMessage = ""
		return AcceptedBatch{}, corrupt
	} else if more {
		return AcceptedBatch{}, refuse(RefuseInvalidRecord, true, "more than one record batch for a partition; a Produce request carries exactly one")
	}

	// --- the batch, as the log takes it ---
	if base := int64(binary.BigEndian.Uint64(batch[batchBaseOffsetOff:])); base != 0 {
		return AcceptedBatch{}, refuse(RefuseInvalidRecord, false, "the batch's base offset is %d; a producer writes 0", base)
	}
	if int64(len(batch)) > rules.MaxBatchBytes {
		return AcceptedBatch{}, refuse(RefuseTooLarge, true, "a record batch of %d bytes is larger than max.message.bytes (%d)", len(batch), rules.MaxBatchBytes)
	}
	if short {
		return AcceptedBatch{}, refuse(RefuseCorrupt, true, "a record batch of %d bytes is shorter than its header (%d)", len(batch), batchFullHeaderBytes)
	}
	if !crcOK(batch) {
		return AcceptedBatch{}, refuse(RefuseCorrupt, true, "the record batch's checksum does not match its contents")
	}
	attrs := attrsOf(batch)
	codec := int(attrs & batchCompressionMask)
	if codec > codecZstd {
		return AcceptedBatch{}, refuse(RefuseUnreadable, false, "unknown compression codec %d", codec)
	}
	count := int32(binary.BigEndian.Uint32(batch[batchRecordCountOff:]))
	fromOffsets := int64(int32(binary.BigEndian.Uint32(batch[batchLastOffsetDeltaOff:]))) + 1
	switch {
	case fromOffsets <= 0:
		return AcceptedBatch{}, refuse(RefuseInvalidRecord, false, "the batch's last offset delta is %d", fromOffsets-1)
	case count <= 0:
		return AcceptedBatch{}, refuse(RefuseInvalidRecord, false, "the batch says it holds %d records", count)
	case fromOffsets != int64(count):
		return AcceptedBatch{}, refuse(RefuseInvalidRecord, false, "the batch says it holds %d records and that its last is record %d", count, fromOffsets-1)
	case attrs&batchControlBit != 0:
		return AcceptedBatch{}, refuse(RefuseInvalidRecord, false, "clients may not write control batches")
	}
	hdr, _ := ParseBatch(batch)
	if hdr.ProducerID >= 0 && hdr.BaseSequence < 0 {
		return AcceptedBatch{}, refuse(RefuseInvalidRecord, false, "a batch with a producer id (%d) and sequence %d", hdr.ProducerID, hdr.BaseSequence)
	}

	// --- the records ---
	scan, refusal := scanRecords(batch, codec, int(count), rules)
	if refusal != nil {
		return AcceptedBatch{}, refusal
	}

	// What Kafka stores is the batch in the topic's codec. Where that is the codec it
	// arrived in, the producer's bytes are kept as they are. Where it is not, Kafka
	// reads every record and writes the batch out again in the topic's codec, and so
	// does this: the records are then numbered 0, 1, 2 whatever the producer wrote,
	// what followed a gzip stream is gone, and the size held to the limit is the
	// rewritten one. (Recorded on a topic of each kind; the compressors here are not
	// the JVM's, so a rewritten size is Kafka's to within what two implementations of
	// a codec differ by.)
	target := rules.Target
	if target == TargetProducer {
		target = codec
	}
	switch {
	case target != codec:
		// Uncompressed, the size is known without writing it: refused before the
		// records are held.
		if target == codecNone && batchFullHeaderBytes+scan.bytes > rules.MaxBatchBytes {
			return AcceptedBatch{}, tooLargeStored(batchFullHeaderBytes+scan.bytes, rules.MaxBatchBytes)
		}
		recs, err := DecodeRecords(batch)
		if err != nil {
			return AcceptedBatch{}, refuse(RefuseInvalidRecord, false, "the records cannot be read: %v", err)
		}
		for i := range recs {
			recs[i].OffsetDelta = int32(i)
		}
		rewritten, err := rebuildBatch(batch, recs, target)
		if err != nil {
			return AcceptedBatch{}, refuse(RefuseUnreadable, false, "the records cannot be written in the topic's codec: %v", err)
		}
		if int64(len(rewritten)) > rules.MaxBatchBytes {
			return AcceptedBatch{}, tooLargeStored(int64(len(rewritten)), rules.MaxBatchBytes)
		}
		batch = rewritten
	case !scan.sequential && codec != codecNone:
		// Offsets inside the batch that are not 0, 1, 2. Kafka keeps an uncompressed
		// batch like that as it came (a consumer then sees those offsets), and fails
		// on a compressed one it would have kept as it came. Both are recorded.
		return AcceptedBatch{}, refuse(RefuseUnreadable, false, "the records of a compressed batch are not numbered 0, 1, 2, ...")
	}
	// The header's maximum timestamp and its timestamp-type bit are the broker's to
	// say, not the producer's: on a CreateTime topic the bit is cleared and the
	// maximum is the latest of the records', whatever the producer wrote there.
	if !rules.LogAppendTime {
		was, max := attrsOf(batch), int64(binary.BigEndian.Uint64(batch[batchMaxTimestampOff:]))
		if was&batchLogAppendTimeBit != 0 || max != scan.maxTimestamp {
			binary.BigEndian.PutUint16(batch[batchAttributesOff:], was&^batchLogAppendTimeBit)
			binary.BigEndian.PutUint64(batch[batchMaxTimestampOff:], uint64(scan.maxTimestamp))
			sealCRCOf(batch)
		}
	}
	hdr.TotalBytes = len(batch)
	return AcceptedBatch{Batch: batch, Header: hdr}, nil
}

// tooLargeStored is the refusal of a batch that is within the limit as it arrived
// and over it as the topic stores it.
func tooLargeStored(stored, limit int64) *Refusal {
	return refuse(RefuseTooLarge, true, "the batch, written as the topic stores it (compression.type), is %d bytes, more than max.message.bytes (%d)", stored, limit)
}

// byteSource is the records section as the scan reads it: a byte at a time for the
// numbers, and whole fields skipped.
type byteSource interface {
	ReadByte() (byte, error)
	Discard(n int) (int, error)
}

// sliceSource is an uncompressed records section: nothing is copied or allocated.
type sliceSource struct {
	b   []byte
	pos int
}

func (s *sliceSource) ReadByte() (byte, error) {
	if s.pos >= len(s.b) {
		return 0, io.EOF
	}
	c := s.b[s.pos]
	s.pos++
	return c, nil
}

func (s *sliceSource) Discard(n int) (int, error) {
	if left := len(s.b) - s.pos; n > left {
		s.pos = len(s.b)
		return left, io.EOF
	}
	s.pos += n
	return n, nil
}

var scanBuffers = sync.Pool{New: func() any { return bufio.NewReaderSize(nil, 32<<10) }}

// recordScan is what one pass over a batch's records found.
type recordScan struct {
	bytes        int64 // the records section, inflated.
	maxTimestamp int64
	sequential   bool // the records' offset deltas are 0, 1, 2, ...
}

// reader counts what it has taken from a source.
type reader struct {
	src byteSource
	n   int64
	err error // the first failure of the source, kept: everything after it is zero.
}

func (r *reader) byte() byte {
	if r.err != nil {
		return 0
	}
	c, err := r.src.ReadByte()
	if err != nil {
		r.err = err
		return 0
	}
	r.n++
	return c
}

// varint reads a zig-zag varint of at most 64 bits. ok is false for one that is
// longer; a source that ends inside it is the reader's err.
func (r *reader) varint() (v int64, ok bool) {
	var u uint64
	for shift := uint(0); ; shift += 7 {
		c := r.byte()
		if r.err != nil {
			return 0, true
		}
		if shift == 63 && c > 1 {
			return 0, false
		}
		u |= uint64(c&0x7f) << shift
		if c&0x80 == 0 {
			return int64(u>>1) ^ -int64(u&1), true
		}
	}
}

func (r *reader) skip(n int64) {
	for n > 0 && r.err == nil {
		step := int(min(n, math.MaxInt32))
		got, err := r.src.Discard(step)
		r.n += int64(got)
		n -= int64(got)
		if err != nil {
			r.err = err
		}
	}
}

// scanRecords reads every record of a batch that has passed the header checks: the
// count the header gives is the count there is, every record is exactly as long as
// it says, a topic that compacts has a key on each, and a CreateTime topic's
// timestamps are within its bounds of the clock.
func scanRecords(batch []byte, codec, count int, rules ProduceRules) (recordScan, *Refusal) {
	body := batch[batchFullHeaderBytes:]
	var src byteSource
	if codec == codecNone {
		src = &sliceSource{b: body}
	} else {
		if codec == codecZstd {
			if w, ok := zstdDeclaredWindow(body); ok && w > kafkaZstdMaxWindow {
				return recordScan{}, refuse(RefuseUnreadable, false, "a zstd frame that declares a window of %d bytes", w)
			}
		}
		stream, closeFn, err := openInflated(codec, body, MaxInflatedBytes)
		if err != nil {
			return recordScan{}, inflateRefusal(err, len(batch))
		}
		defer closeFn()
		br := scanBuffers.Get().(*bufio.Reader)
		br.Reset(stream)
		defer func() { br.Reset(nil); scanBuffers.Put(br) }()
		src = br
	}
	r := &reader{src: src}
	first := int64(binary.BigEndian.Uint64(batch[batchFirstTimestampOff:]))
	stampedByClient := attrsOf(batch)&batchLogAppendTimeBit != 0
	scan := recordScan{maxTimestamp: kafkaNoTimestamp, sequential: true}
	var keyless, badTime []RecordRefusal

	// failed turns the source's failure, or a structural one, into the refusal.
	failed := func(i int, what string) *Refusal {
		switch {
		case r.err == nil || r.err == io.EOF:
			// The records section ended (cleanly, if compressed) where a record's
			// fields should be, or a field is not what a record can hold.
			return refuse(RefuseInvalidRecord, false, "record %d of %d %s", i, count, what)
		default:
			return inflateRefusal(r.err, len(batch))
		}
	}
	for i := 0; i < count; i++ {
		length, ok := r.varint()
		if r.err != nil || !ok || length < 0 {
			return recordScan{}, failed(i, "has no length")
		}
		// Every field is read inside the length the record gives itself: end is
		// where the record stops, and nothing may be read past it.
		start := r.n
		end := start + length
		r.byte() // attributes: unused.
		tsDelta, ok1 := r.varint()
		offsetDelta, ok2 := r.varint()
		// A negative length is a null key or value, whatever the number (Kafka reads
		// any as null; a client writes -1).
		keyLen, ok3 := r.varint()
		if r.err != nil || !ok1 || !ok2 || !ok3 || r.n > end {
			return recordScan{}, failed(i, "is cut short")
		}
		if keyLen > 0 {
			if keyLen > end-r.n {
				return recordScan{}, failed(i, "has a key that runs past the record")
			}
			r.skip(keyLen)
		}
		valueLen, ok := r.varint()
		if r.err != nil || !ok || r.n > end {
			return recordScan{}, failed(i, "is cut short")
		}
		if valueLen > 0 {
			if valueLen > end-r.n {
				return recordScan{}, failed(i, "has a value that runs past the record")
			}
			r.skip(valueLen)
		}
		headers, ok := r.varint()
		if r.err != nil || !ok || headers < 0 || headers > end-r.n {
			return recordScan{}, failed(i, "has a header count that cannot be")
		}
		for h := int64(0); h < headers; h++ {
			n, ok := r.varint()
			if r.err != nil || !ok || n < 0 || n > end-r.n {
				return recordScan{}, failed(i, "has a header whose key cannot be")
			}
			r.skip(n)
			n, ok = r.varint()
			if r.err != nil || !ok || n > end-r.n {
				return recordScan{}, failed(i, "has a header whose value runs past the record")
			}
			if n > 0 {
				r.skip(n)
			}
		}
		if r.err != nil {
			return recordScan{}, failed(i, "is cut short")
		}
		if r.n != end {
			return recordScan{}, refuse(RefuseInvalidRecord, false, "record %d of %d says it is %d bytes long and its fields are %d", i, count, length, r.n-start)
		}

		if offsetDelta != int64(i) {
			scan.sequential = false
		}
		ts := first + tsDelta
		if ts > scan.maxTimestamp {
			scan.maxTimestamp = ts
		}
		switch {
		case rules.RequireKeys && keyLen < 0:
			keyless = append(keyless, RecordRefusal{Index: int32(i),
				Message: "Compacted topic cannot accept message without key in topic partition " + rules.TopicPartition})
		case !rules.LogAppendTime && ts != kafkaNoTimestamp:
			if ts-rules.NowMs > rules.AfterMaxMs || rules.NowMs-ts > rules.BeforeMaxMs {
				badTime = append(badTime, RecordRefusal{Index: int32(i), Message: fmt.Sprintf(
					"Timestamp %d of message with offset %d is out of range. The timestamp should be within [%d, %d]",
					ts, offsetDelta, sub64(rules.NowMs, rules.BeforeMaxMs), add64(rules.NowMs, rules.AfterMaxMs))})
			}
		case stampedByClient:
			// The batch says its timestamps are the broker's, and a producer is not
			// the broker: refused where no producer timestamp stands to be checked.
			badTime = append(badTime, RecordRefusal{Index: int32(i), Message: fmt.Sprintf(
				"Invalid timestamp type in message DefaultRecord(offset=%d, timestamp=%d, key=%d bytes, value=%d bytes). Producer should not set timestamp type to LogAppendTime.",
				offsetDelta, ts, max(keyLen, 0), max(valueLen, 0))})
		}
	}
	// Nothing may follow the last record the header counts.
	r.byte()
	switch {
	case r.err == nil:
		return recordScan{}, refuse(RefuseInvalidRecord, false, "the batch says it holds %d records and more bytes follow them", count)
	case r.err != io.EOF:
		return recordScan{}, inflateRefusal(r.err, len(batch))
	}
	scan.bytes = r.n

	if len(badTime) > 0 {
		all := append(keyless, badTime...)
		sortRefusals(all)
		return recordScan{}, &Refusal{Kind: RefuseTimestamp, Records: all,
			KafkaMessage: "One or more records have been rejected due to invalid timestamp"}
	}
	if len(keyless) > 0 {
		shown := make([]string, 0, 3)
		for _, k := range keyless[:min(len(keyless), 3)] {
			shown = append(shown, fmt.Sprintf("RecordError(batchIndex=%d, message='%s')", k.Index, k.Message))
		}
		return recordScan{}, &Refusal{Kind: RefuseInvalidRecord, Records: keyless, KafkaMessage: fmt.Sprintf(
			"One or more records have been rejected due to %d record errors in total, and only showing the first three errors at most: [%s]",
			len(keyless), strings.Join(shown, ", "))}
	}
	return scan, nil
}

func sortRefusals(rs []RecordRefusal) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && rs[j].Index < rs[j-1].Index; j-- {
			rs[j], rs[j-1] = rs[j-1], rs[j]
		}
	}
}

// add64 and sub64 are a clock plus and minus a bound that may be "no bound"
// (math.MaxInt64): the sum wraps, as Kafka's does, and is what its message prints.
func add64(a, b int64) int64 { return int64(uint64(a) + uint64(b)) }
func sub64(a, b int64) int64 { return int64(uint64(a) - uint64(b)) }

// inflateRefusal is the refusal for a records section that could not be read as its
// codec says: too large for this broker, or not that codec's bytes.
func inflateRefusal(err error, batchBytes int) *Refusal {
	if errors.Is(err, ErrInflatedTooLarge) {
		return refuse(RefuseTooLarge, true, "a record batch of %d bytes inflates past %d bytes, or its zstd frame declares a window past %d: more than this broker will inflate",
			batchBytes, MaxInflatedBytes, zstdMaxWindow)
	}
	return refuse(RefuseUnreadable, false, "the records cannot be decompressed: %v", err)
}

// zstdDeclaredWindow is the window a zstd frame says a decoder needs (RFC 8878, 3.1.1.1).
func zstdDeclaredWindow(frame []byte) (uint64, bool) {
	if len(frame) < 6 || binary.LittleEndian.Uint32(frame) != 0xFD2FB528 {
		return 0, false
	}
	desc := frame[4]
	if desc&0x20 == 0 { // not a single segment: a window descriptor follows.
		w := frame[5]
		base := uint64(1) << (10 + w>>3)
		return base + base/8*uint64(w&7), true
	}
	// A single segment's window is its content size, which is the next field.
	size, at := 1<<(desc>>6), 5
	if len(frame) < at+size {
		return 0, false
	}
	switch size {
	case 1:
		return uint64(frame[at]), true
	case 2:
		return uint64(binary.LittleEndian.Uint16(frame[at:])) + 256, true
	case 4:
		return uint64(binary.LittleEndian.Uint32(frame[at:])), true
	}
	return binary.LittleEndian.Uint64(frame[at:]), true
}

// kafkaNoTimestamp is the timestamp of a record that has none (Kafka's NO_TIMESTAMP):
// it is not held to the clock, and a batch of nothing else has it as its maximum.
const kafkaNoTimestamp = -1
