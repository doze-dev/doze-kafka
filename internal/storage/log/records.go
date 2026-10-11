package log

import (
	"bufio"
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

	// MaxInflatedBytes bounds what one batch may inflate to wherever the broker looks
	// inside a compressed batch: 64 times the Java producer's default request size, and
	// the most the cleaner, which holds a batch's records to rewrite it, will hold. Real
	// Kafka has no such bound (recorded: a batch that weighs 20 KB and inflates to 20 MiB
	// is accepted); a batch past it is refused with Kafka's size error, and the
	// difference is in docs/limitations.md.
	MaxInflatedBytes = 64 << 20
	maxDecompressed  = MaxInflatedBytes
)

// xerialMagic begins a snappy stream in the framing the Java client's snappy-java
// writes: the first eight bytes of its 16-byte header, then length-prefixed blocks.
var xerialMagic = []byte{0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0}

// The windows a zstd frame may declare. A real compressor declares what its level says,
// not what the payload needs: librdkafka and the Java client at their default level ask
// for 2 MiB for a 12 KiB report, so no window under that may be refused. The decoder
// allocates the declared window before reading, so a frame is not allowed more than
// four times what it may inflate to, nor more than 8 MiB: the largest window of any of
// zstd's levels up to 19, and the size RFC 8878 asks every decoder to support. A frame
// that declares 64 MiB for a few bytes of payload was taking 64 MiB per request.
const (
	zstdMinWindow = 2 << 20
	zstdMaxWindow = 8 << 20
)

func zstdWindowFor(limit int) uint64 {
	return uint64(min(max(4*limit, zstdMinWindow), zstdMaxWindow))
}

var errRecords = errors.New("log: cannot read the records of a batch")

// countReader counts what has been read through it, so a record's remaining bytes
// are known after its leading fields are.
type countReader struct {
	br *bufio.Reader
	n  int64
}

func (c *countReader) ReadByte() (byte, error) {
	b, err := c.br.ReadByte()
	if err == nil {
		c.n++
	}
	return b, err
}

// recordHead is the leading fields of one record, as far as a caller asked to read.
type recordHead struct {
	TimestampDelta, OffsetDelta int64
	KeyNull                     bool // set only when walkRecords was asked for keys.
}

// walkRecords streams the records of a complete batch through visit, in order, until
// visit returns false or the batch's record count is reached. Only each record's leading
// fields are read and the rest is skipped, so what is held is the decompressor's window
// and a buffer, never the inflated payload; the stream fails with ErrInflatedTooLarge
// once it has produced more than limit bytes. A batch that cannot be read is an error.
func walkRecords(batch []byte, limit int, needKey bool, visit func(i int, h recordHead) bool) error {
	_, err := walkRecordsDrain(batch, limit, needKey, false, visit)
	return err
}

// walkRecordsDrain is walkRecords that, when drain is set and visit never stopped it,
// reads on to the end of the body and returns how many bytes the body inflates to.
func walkRecordsDrain(batch []byte, limit int, needKey, drain bool, visit func(i int, h recordHead) bool) (int64, error) {
	if len(batch) < batchFullHeaderBytes {
		return 0, fmt.Errorf("%w: %d bytes is shorter than a header", errRecords, len(batch))
	}
	attrs := binary.BigEndian.Uint16(batch[batchAttributesOff:])
	count := int(int32(binary.BigEndian.Uint32(batch[batchRecordCountOff:])))
	r, closeFn, err := openInflated(int(attrs&batchCompressionMask), batch[batchFullHeaderBytes:], limit)
	if err != nil {
		return 0, err
	}
	defer closeFn()
	cr := &countReader{br: bufio.NewReaderSize(r, 16<<10)}
	fail := func(i int, what string, err error) (int64, error) {
		switch {
		case errors.Is(err, ErrInflatedTooLarge):
			return 0, fmt.Errorf("%w: %w (limit %d bytes)", errRecords, ErrInflatedTooLarge, limit)
		case errors.Is(err, errRecords):
			return 0, err
		}
		return 0, fmt.Errorf("%w: record %d %s", errRecords, i, what)
	}
	for i := 0; i < count; i++ {
		length, err := binary.ReadVarint(cr)
		if err != nil || length < 0 {
			return fail(i, "is cut short", err)
		}
		recStart := cr.n
		if length < 1 {
			return 0, fmt.Errorf("%w: record %d is empty", errRecords, i)
		}
		if _, err := cr.ReadByte(); err != nil { // attributes: unused.
			return fail(i, "is cut short", err)
		}
		var h recordHead
		if h.TimestampDelta, err = binary.ReadVarint(cr); err != nil {
			return fail(i, "has no timestamp", err)
		}
		if h.OffsetDelta, err = binary.ReadVarint(cr); err != nil {
			return fail(i, "has no offset", err)
		}
		if needKey {
			keyLen, err := binary.ReadVarint(cr)
			if err != nil {
				return fail(i, "has no key length", err)
			}
			h.KeyNull = keyLen == -1
		}
		if used := cr.n - recStart; used > length {
			return 0, fmt.Errorf("%w: record %d is shorter than its own fields", errRecords, i)
		} else if rest := length - used; rest > 0 {
			got, err := io.CopyN(io.Discard, cr.br, rest)
			cr.n += got
			if err != nil {
				return fail(i, "is cut short", err)
			}
		}
		if !visit(i, h) {
			return 0, nil
		}
	}
	if drain {
		got, err := io.Copy(io.Discard, cr.br)
		cr.n += got
		if err != nil {
			return fail(count, "is followed by a body that cannot be read", err)
		}
	}
	return cr.n, nil
}

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
	appendTime := attrs&batchLogAppendTimeBit != 0
	return walkRecords(batch, maxDecompressed, false, func(_ int, h recordHead) bool {
		ts := first + h.TimestampDelta
		if appendTime {
			ts = max // the broker's clock: every record carries the batch's.
		}
		return visit(base+h.OffsetDelta, ts)
	})
}

// inflate returns the records section of a batch body, decompressed if the codec
// says it is.
func inflate(codec int, body []byte) ([]byte, error) { return inflateMax(codec, body, maxDecompressed) }

// ErrInflatedTooLarge is what reading a compressed body returns when it inflates past
// the limit the reader was given. It is a size, not corruption: the caller maps it to
// MESSAGE_TOO_LARGE where Kafka's own answer for a size is that code.
var ErrInflatedTooLarge = errors.New("log: the batch inflates past the limit")

// boundedReader reads from r and fails, rather than truncates, once more than left
// bytes have come out: a stream that stops at the limit would look like a short batch.
type boundedReader struct {
	r    io.Reader
	left int64
}

func (b *boundedReader) Read(p []byte) (int, error) {
	if b.left < 0 {
		return 0, ErrInflatedTooLarge
	}
	if int64(len(p)) > b.left+1 {
		p = p[:b.left+1] // one past the limit is enough to know it was passed.
	}
	n, err := b.r.Read(p)
	if errors.Is(err, zstd.ErrWindowSizeExceeded) {
		// A frame that declares a window past zstdMaxWindow is a size the broker will not
		// take, not corruption: it is what a 64 MiB window over a few bytes is.
		return n, fmt.Errorf("%w: %v", ErrInflatedTooLarge, err)
	}
	b.left -= int64(n)
	if b.left < 0 {
		// Hand back only what is within the limit, and the failure after it.
		return n + int(b.left), nil
	}
	return n, err
}

// snappyBlock decodes one snappy block of at most limit bytes. The block's own header
// says what it inflates to and Decode allocates that before reading a byte, so the
// claim is checked first: against the limit, and against what the block's length could
// possibly produce (a copy is two or three bytes for at most 64, so no block inflates to
// more than about 22 times its size), which keeps a ten byte block from taking the limit.
func snappyBlock(block []byte, limit int) ([]byte, error) {
	n, err := snappy.DecodedLen(block)
	if err != nil {
		return nil, fmt.Errorf("%w: snappy: %v", errRecords, err)
	}
	if n > limit {
		return nil, fmt.Errorf("%w: %w: a snappy block claims %d bytes, past %d", errRecords, ErrInflatedTooLarge, n, limit)
	}
	if n > 22*len(block)+64 {
		return nil, fmt.Errorf("%w: snappy: a %d byte block claims to inflate to %d", errRecords, len(block), n)
	}
	out, err := snappy.Decode(nil, block)
	if err != nil {
		return nil, fmt.Errorf("%w: snappy: %v", errRecords, err)
	}
	return out, nil
}

// xerialReader reads the blocks of a xerial-framed snappy stream one at a time, so what
// is held is one block and not the stream.
type xerialReader struct {
	rest []byte
	cur  []byte
	left int
}

func (x *xerialReader) Read(p []byte) (int, error) {
	for len(x.cur) == 0 {
		if len(x.rest) == 0 {
			return 0, io.EOF
		}
		if len(x.rest) < 4 {
			return 0, fmt.Errorf("%w: snappy stream is cut short", errRecords)
		}
		n := int(binary.BigEndian.Uint32(x.rest))
		if n < 0 || len(x.rest)-4 < n {
			return 0, fmt.Errorf("%w: snappy block is cut short", errRecords)
		}
		block, err := snappyBlock(x.rest[4:4+n], x.left)
		if err != nil {
			return 0, err
		}
		x.left -= len(block)
		x.rest = x.rest[4+n:]
		x.cur = block
	}
	n := copy(p, x.cur)
	x.cur = x.cur[n:]
	return n, nil
}

// openInflated is the records section of a batch body as a stream, failing with
// ErrInflatedTooLarge once it has produced more than limit bytes. What it holds is the
// decompressor's window and buffers, never the inflated payload; the returned function
// releases the decoder.
func openInflated(codec int, body []byte, limit int) (io.Reader, func(), error) {
	nop := func() {}
	switch codec {
	case codecNone:
		return &boundedReader{r: bytes.NewReader(body), left: int64(limit)}, nop, nil
	case codecGzip:
		r, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, nop, fmt.Errorf("%w: gzip: %v", errRecords, err)
		}
		return &boundedReader{r: r, left: int64(limit)}, nop, nil
	case codecSnappy:
		if !bytes.HasPrefix(body, xerialMagic) {
			out, err := snappyBlock(body, limit)
			if err != nil {
				return nil, nop, err
			}
			return bytes.NewReader(out), nop, nil
		}
		if len(body) < 16 {
			return nil, nop, fmt.Errorf("%w: snappy stream header is cut short", errRecords)
		}
		// magic, version and the oldest compatible version, then length-prefixed blocks.
		return &xerialReader{rest: body[16:], left: limit}, nop, nil
	case codecLZ4:
		return &boundedReader{r: lz4.NewReader(bytes.NewReader(body)), left: int64(limit)}, nop, nil
	case codecZstd:
		// The window the frame declares is the compressor's setting, not a measure of the
		// payload (librdkafka asks for 2 MiB for a 12 KiB report), so windows real
		// compressors use are allowed and no more, because the decoder allocates the
		// declared window before it has read a byte (TRF-2).
		d, err := zstd.NewReader(bytes.NewReader(body),
			zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(zstdWindowFor(limit)))
		if err != nil {
			return nil, nop, fmt.Errorf("%w: zstd: %v", errRecords, err)
		}
		return &boundedReader{r: d, left: int64(limit)}, d.Close, nil
	}
	return nil, nop, fmt.Errorf("%w: unknown compression codec %d", errRecords, codec)
}

// inflateMax is inflate with a limit on how large the result may be: a compressed
// payload can claim far more than it weighs. It is the stream read whole, and holds the
// result: callers that only look at records use the stream.
func inflateMax(codec int, body []byte, limit int) ([]byte, error) {
	if codec == codecNone {
		return body, nil
	}
	r, closeFn, err := openInflated(codec, body, limit)
	if err != nil {
		return nil, err
	}
	defer closeFn()
	out, err := readAllInflated(r, limit)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// readAllInflated reads a stream from openInflated whole and says why it stopped when
// it did: past the limit (ErrInflatedTooLarge), or corrupt.
func readAllInflated(r io.Reader, limit int) ([]byte, error) {
	out, err := io.ReadAll(r)
	switch {
	case err == nil:
		return out, nil
	case errors.Is(err, ErrInflatedTooLarge):
		return nil, fmt.Errorf("%w: %w (limit %d bytes)", errRecords, ErrInflatedTooLarge, limit)
	case errors.Is(err, errRecords):
		return nil, err
	}
	return nil, fmt.Errorf("%w: %v", errRecords, err)
}

// Decompress inflates a payload compressed with one of the record batch codecs (0 none,
// 1 gzip, 2 snappy, 3 lz4, 4 zstd), refusing to produce more than limit bytes. Client
// telemetry uses the same numbering and the same framings (a Java client's snappy is
// xerial-framed, librdkafka's is a plain block; both are read).
func Decompress(codec int, body []byte, limit int) ([]byte, error) {
	return inflateMax(codec, body, limit)
}
