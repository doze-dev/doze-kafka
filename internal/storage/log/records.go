package log

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Record-level reading of a stored batch. The log keeps batches verbatim and
// everything else it does works from the batch header; the timestamp lookups need
// the offset and timestamp of individual records, which live in the (possibly
// compressed) body, so they decode them. Only those two fields are read.

const (
	batchFirstTimestampOff = 27 // int64: the timestamp the records' deltas are relative to.

	codecNone   = 0
	codecGzip   = 1
	codecSnappy = 2
	codecLZ4    = 3
	codecZstd   = 4

	// maxDecompressed bounds what one batch may inflate to. A batch is capped by
	// max.message.bytes on the way in, but a compressed one can claim far more
	// than it weighs.
	maxDecompressed = 256 << 20
)

// xerialMagic begins a snappy stream in the framing the Java client's snappy-java
// writes: the first eight bytes of its 16-byte header, then length-prefixed blocks.
var xerialMagic = []byte{0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0}

var errRecords = errors.New("log: cannot read the records of a batch")

// batchRecords calls visit for each record of a complete batch with its offset and
// timestamp, in order, until visit returns false. A batch that cannot be decoded is
// an error: the caller decides whether that stops its lookup.
func batchRecords(batch []byte, visit func(offset, timestamp int64) bool) error {
	if len(batch) < batchFullHeaderBytes {
		return fmt.Errorf("%w: %d bytes is shorter than a header", errRecords, len(batch))
	}
	attrs := binary.BigEndian.Uint16(batch[batchAttributesOff:])
	base := int64(binary.BigEndian.Uint64(batch[batchBaseOffsetOff:]))
	first := int64(binary.BigEndian.Uint64(batch[batchFirstTimestampOff:]))
	max := int64(binary.BigEndian.Uint64(batch[batchMaxTimestampOff:]))
	count := int(int32(binary.BigEndian.Uint32(batch[batchRecordCountOff:])))
	appendTime := attrs&batchLogAppendTimeBit != 0

	body, err := inflate(int(attrs&batchCompressionMask), batch[batchFullHeaderBytes:])
	if err != nil {
		return err
	}
	for i := 0; i < count; i++ {
		length, n := binary.Varint(body)
		if n <= 0 || length < 0 || int64(len(body)-n) < length {
			return fmt.Errorf("%w: record %d is cut short", errRecords, i)
		}
		rec := body[n : n+int(length)]
		body = body[n+int(length):]
		if len(rec) < 1 {
			return fmt.Errorf("%w: record %d is empty", errRecords, i)
		}
		rec = rec[1:] // attributes: unused.
		tsDelta, n := binary.Varint(rec)
		if n <= 0 {
			return fmt.Errorf("%w: record %d has no timestamp", errRecords, i)
		}
		offDelta, n2 := binary.Varint(rec[n:])
		if n2 <= 0 {
			return fmt.Errorf("%w: record %d has no offset", errRecords, i)
		}
		ts := first + tsDelta
		if appendTime {
			ts = max // the broker's clock: every record carries the batch's.
		}
		if !visit(base+offDelta, ts) {
			return nil
		}
	}
	return nil
}

// inflate returns the records section of a batch body, decompressed if the codec
// says it is.
func inflate(codec int, body []byte) ([]byte, error) {
	switch codec {
	case codecNone:
		return body, nil
	case codecGzip:
		r, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("%w: gzip: %v", errRecords, err)
		}
		return readAllBounded(r)
	case codecSnappy:
		if !bytes.HasPrefix(body, xerialMagic) {
			out, err := snappy.Decode(nil, body)
			if err != nil {
				return nil, fmt.Errorf("%w: snappy: %v", errRecords, err)
			}
			return out, nil
		}
		var out []byte
		rest := body[16:] // magic, version and the oldest compatible version.
		for len(rest) > 0 {
			if len(rest) < 4 {
				return nil, fmt.Errorf("%w: snappy stream is cut short", errRecords)
			}
			n := int(binary.BigEndian.Uint32(rest))
			if n < 0 || len(rest)-4 < n {
				return nil, fmt.Errorf("%w: snappy block is cut short", errRecords)
			}
			block, err := snappy.Decode(nil, rest[4:4+n])
			if err != nil {
				return nil, fmt.Errorf("%w: snappy: %v", errRecords, err)
			}
			if len(out)+len(block) > maxDecompressed {
				return nil, fmt.Errorf("%w: inflates past %d bytes", errRecords, maxDecompressed)
			}
			out = append(out, block...)
			rest = rest[4+n:]
		}
		return out, nil
	case codecLZ4:
		return readAllBounded(lz4.NewReader(bytes.NewReader(body)))
	case codecZstd:
		d, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxDecompressed))
		if err != nil {
			return nil, err
		}
		defer d.Close()
		out, err := d.DecodeAll(body, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: zstd: %v", errRecords, err)
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: unknown compression codec %d", errRecords, codec)
}

func readAllBounded(r io.Reader) ([]byte, error) {
	out, err := io.ReadAll(io.LimitReader(r, maxDecompressed+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errRecords, err)
	}
	if len(out) > maxDecompressed {
		return nil, fmt.Errorf("%w: inflates past %d bytes", errRecords, maxDecompressed)
	}
	return out, nil
}
