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

// The windows a zstd frame may declare. A real compressor declares what its level says,
// not what the payload needs: librdkafka and the Java client at their default level ask
// for 2 MiB for a 12 KiB report, so no window under that may be refused. The decoder
// allocates the declared window before reading, so a frame is not allowed more than
// four times what it may inflate to, nor more than zstd's levels reach in practice.
const (
	zstdMinWindow = 2 << 20
	zstdMaxWindow = 64 << 20
)

func zstdWindowFor(limit int) uint64 {
	return uint64(min(max(4*limit, zstdMinWindow), zstdMaxWindow))
}

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
func inflate(codec int, body []byte) ([]byte, error) { return inflateMax(codec, body, maxDecompressed) }

// inflateMax is inflate with a limit on how large the result may be: a compressed
// payload can claim far more than it weighs.
func inflateMax(codec int, body []byte, limit int) ([]byte, error) {
	switch codec {
	case codecNone:
		return body, nil
	case codecGzip:
		r, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("%w: gzip: %v", errRecords, err)
		}
		return readAllBounded(r, limit)
	case codecSnappy:
		if !bytes.HasPrefix(body, xerialMagic) {
			// The block's own header says how large it inflates to, and Decode allocates
			// that before reading a byte: a few bytes can claim four gigabytes.
			if n, err := snappy.DecodedLen(body); err != nil || n > limit {
				return nil, fmt.Errorf("%w: snappy block claims to inflate past %d bytes", errRecords, limit)
			}
			out, err := snappy.Decode(nil, body)
			if err != nil {
				return nil, fmt.Errorf("%w: snappy: %v", errRecords, err)
			}
			return out, nil
		}
		if len(body) < 16 {
			return nil, fmt.Errorf("%w: snappy stream header is cut short", errRecords)
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
			if m, err := snappy.DecodedLen(rest[4 : 4+n]); err != nil || m > limit {
				return nil, fmt.Errorf("%w: snappy block claims to inflate past %d bytes", errRecords, limit)
			}
			block, err := snappy.Decode(nil, rest[4:4+n])
			if err != nil {
				return nil, fmt.Errorf("%w: snappy: %v", errRecords, err)
			}
			if len(out)+len(block) > limit {
				return nil, fmt.Errorf("%w: inflates past %d bytes", errRecords, limit)
			}
			out = append(out, block...)
			rest = rest[4+n:]
		}
		return out, nil
	case codecLZ4:
		return readAllBounded(lz4.NewReader(bytes.NewReader(body)), limit)
	case codecZstd:
		// Streamed through the limit, so what is allocated is what is read, not what the
		// frame claims: DecodeAll inflated a 6 KB frame to 60 MiB before the size was
		// looked at (TRF-2). The window the frame declares is the compressor's setting, not
		// a measure of the payload (librdkafka asks for 2 MiB for a 12 KiB report), so it
		// is allowed up to a few times the limit and no more, because the decoder allocates
		// the window the frame declares before it has read a byte.
		d, err := zstd.NewReader(bytes.NewReader(body),
			zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(zstdWindowFor(limit)))
		if err != nil {
			return nil, fmt.Errorf("%w: zstd: %v", errRecords, err)
		}
		defer d.Close()
		return readAllBounded(d, limit)
	}
	return nil, fmt.Errorf("%w: unknown compression codec %d", errRecords, codec)
}

func readAllBounded(r io.Reader, limit int) ([]byte, error) {
	out, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errRecords, err)
	}
	if len(out) > limit {
		return nil, fmt.Errorf("%w: inflates past %d bytes", errRecords, limit)
	}
	return out, nil
}

// Decompress inflates a payload compressed with one of the record batch codecs (0 none,
// 1 gzip, 2 snappy, 3 lz4, 4 zstd), refusing to produce more than limit bytes. Client
// telemetry uses the same numbering and the same framings (a Java client's snappy is
// xerial-framed, librdkafka's is a plain block; both are read).
func Decompress(codec int, body []byte, limit int) ([]byte, error) {
	return inflateMax(codec, body, limit)
}
