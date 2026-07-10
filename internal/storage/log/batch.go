package log

import "encoding/binary"

// Kafka record batch v2 header field offsets (bytes from the batch start). The
// engine only ever touches these header fields — records are stored and served
// verbatim, never decoded. See docs/storage-and-durability.md.
const (
	batchBaseOffsetOff      = 0  // int64
	batchLengthOff          = 8  // int32: number of bytes following this field
	batchLastOffsetDeltaOff = 23 // int32
	batchProducerIDOff      = 43 // int64
	batchProducerEpochOff   = 51 // int16
	batchBaseSequenceOff    = 53 // int32
	batchHeaderMinBytes     = 27 // bytes through lastOffsetDelta
	batchFullHeaderBytes    = 61 // bytes through recordCount (the whole v2 header)
	batchOverhead           = 12 // baseOffset(8) + batchLength(4), excluded from batchLength
)

// batchInfo is the minimal, header-only view the engine needs about a batch.
type batchInfo struct {
	baseOffset  int64
	recordCount int32 // lastOffsetDelta + 1
	totalBytes  int64 // on-disk size = batchOverhead + batchLength
}

// parseHeader reads the batch header at the start of b. It requires only the
// header bytes to be present (not the whole batch), so it is usable while
// scanning. ok is false if b is shorter than a header or the length is nonsensical.
func parseHeader(b []byte) (batchInfo, bool) {
	if len(b) < batchHeaderMinBytes {
		return batchInfo{}, false
	}
	length := int32(binary.BigEndian.Uint32(b[batchLengthOff:]))
	if length < batchHeaderMinBytes-batchOverhead {
		return batchInfo{}, false // length can't even cover the header we just read.
	}
	return batchInfo{
		baseOffset:  int64(binary.BigEndian.Uint64(b[batchBaseOffsetOff:])),
		recordCount: int32(binary.BigEndian.Uint32(b[batchLastOffsetDeltaOff:])) + 1,
		totalBytes:  int64(batchOverhead) + int64(length),
	}, true
}

// stampBaseOffset overwrites the batch's base offset in place. Producers send a
// base offset of 0; the broker assigns the real one at append time.
func stampBaseOffset(b []byte, offset int64) {
	binary.BigEndian.PutUint64(b[batchBaseOffsetOff:], uint64(offset))
}

// BatchHeader is the producer/sequence view of a record batch the broker's
// idempotency layer needs. ProducerID is -1 for a non-idempotent batch.
type BatchHeader struct {
	ProducerID    int64
	ProducerEpoch int16
	BaseSequence  int32
	RecordCount   int32
	TotalBytes    int // on-disk size of this batch.
}

// ParseBatch reads the leading record batch's header from b, including the
// idempotent-producer fields. ok is false if b doesn't hold a full v2 header or the
// length is nonsensical. A batch shorter than a full header (or a length that
// doesn't cover it) can't be an idempotent batch, so ProducerID is reported as -1.
func ParseBatch(b []byte) (BatchHeader, bool) {
	info, ok := parseHeader(b)
	if !ok {
		return BatchHeader{}, false
	}
	h := BatchHeader{ProducerID: -1, RecordCount: info.recordCount, TotalBytes: int(info.totalBytes)}
	if len(b) >= batchFullHeaderBytes {
		h.ProducerID = int64(binary.BigEndian.Uint64(b[batchProducerIDOff:]))
		h.ProducerEpoch = int16(binary.BigEndian.Uint16(b[batchProducerEpochOff:]))
		h.BaseSequence = int32(binary.BigEndian.Uint32(b[batchBaseSequenceOff:]))
	}
	return h, true
}
