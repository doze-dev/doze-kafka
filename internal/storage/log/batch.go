package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

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
	batchMagicOff           = 16 // int8: record batch format version (2 for everything we store).
	batchCRCOff             = 17 // uint32: CRC32C of every byte from the attributes field to the end.
	batchAttributesOff      = 21 // int16: first byte covered by the CRC.
	batchRecordCountOff     = 57 // int32: the number of records in the batch.
	batchMagicV2            = 2
	batchHeaderMinBytes     = 27 // bytes through lastOffsetDelta
	batchFullHeaderBytes    = 61 // bytes through recordCount (the whole v2 header)
	batchOverhead           = 12 // baseOffset(8) + batchLength(4), excluded from batchLength
)

// ErrCorruptBatch is returned for a record batch that fails validation: wrong
// format version, inconsistent lengths or counts, or a CRC that does not match.
var ErrCorruptBatch = errors.New("log: corrupt record batch")

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// checkBatchShape validates everything about a complete batch except its CRC:
// it is a v2 batch, its declared length matches its size, and its record count is
// positive and agrees with lastOffsetDelta. The count matters beyond tidiness —
// the log-end offset advances by it, so a negative or lying value would rewind
// or skip offsets permanently.
func checkBatchShape(b []byte) (batchInfo, error) {
	if len(b) < batchFullHeaderBytes {
		return batchInfo{}, fmt.Errorf("%w: %d bytes is shorter than a batch header (%d)", ErrCorruptBatch, len(b), batchFullHeaderBytes)
	}
	if b[batchMagicOff] != batchMagicV2 {
		return batchInfo{}, fmt.Errorf("%w: magic %d, want %d", ErrCorruptBatch, b[batchMagicOff], batchMagicV2)
	}
	info, ok := parseHeader(b)
	if !ok || info.totalBytes != int64(len(b)) {
		return batchInfo{}, fmt.Errorf("%w: declared length does not match the %d bytes present", ErrCorruptBatch, len(b))
	}
	count := int32(binary.BigEndian.Uint32(b[batchRecordCountOff:]))
	if info.recordCount <= 0 || count != info.recordCount {
		return batchInfo{}, fmt.Errorf("%w: record count %d disagrees with lastOffsetDelta (%d records)", ErrCorruptBatch, count, info.recordCount)
	}
	return info, nil
}

// crcOK reports whether a complete batch's stored CRC matches its contents.
func crcOK(b []byte) bool {
	return crc32.Checksum(b[batchAttributesOff:], crcTable) == binary.BigEndian.Uint32(b[batchCRCOff:])
}

// ValidateBatches checks a Produce payload — one or more concatenated record
// batches — before any of it is written: shape and CRC of every batch, with no
// trailing bytes. Rejecting up front means a bad batch can neither be stored
// (and later served to every consumer, who would fail on it) nor leave the
// batches before it half-appended.
func ValidateBatches(records []byte) error {
	for pos := 0; pos < len(records); {
		info, ok := parseHeader(records[pos:])
		if !ok || pos+int(info.totalBytes) > len(records) {
			return fmt.Errorf("%w: truncated batch at byte %d of %d", ErrCorruptBatch, pos, len(records))
		}
		batch := records[pos : pos+int(info.totalBytes)]
		if _, err := checkBatchShape(batch); err != nil {
			return fmt.Errorf("batch at byte %d: %w", pos, err)
		}
		if !crcOK(batch) {
			return fmt.Errorf("%w: CRC mismatch in the batch at byte %d", ErrCorruptBatch, pos)
		}
		pos += int(info.totalBytes)
	}
	return nil
}

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
