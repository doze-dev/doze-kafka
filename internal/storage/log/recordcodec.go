package log

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// The record-level codec. The log stores batches verbatim and does almost everything
// from the batch header; the cleaner (compact.go) has to take a batch apart, drop some
// of its records and put the rest back, so it needs every field of a record, exactly,
// null keys and tombstones included.

// Record is one record of a version-2 batch as it is stored. A nil Key is a null key
// and a nil Value a null value (a tombstone); an empty slice is a present, empty one.
type Record struct {
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32
	Key, Value     []byte
	Headers        []RecordHeader
}

// RecordHeader is one record header; a nil Value is a null header value.
type RecordHeader struct {
	Key   string
	Value []byte
}

// ErrBadRecords is returned for a batch whose records cannot be read.
var ErrBadRecords = errors.New("log: the records of a batch cannot be read")

// cursor reads varints and byte runs from a buffer without ever running past it.
type cursor struct {
	b   []byte
	err error
}

func (c *cursor) fail(what string) {
	if c.err == nil {
		c.err = fmt.Errorf("%w: %s", ErrBadRecords, what)
	}
}

func (c *cursor) varint() int64 {
	if c.err != nil {
		return 0
	}
	v, n := binary.Varint(c.b)
	if n <= 0 {
		c.fail("a varint is cut short")
		return 0
	}
	c.b = c.b[n:]
	return v
}

func (c *cursor) byte() byte {
	if c.err != nil || len(c.b) < 1 {
		c.fail("a record is cut short")
		return 0
	}
	v := c.b[0]
	c.b = c.b[1:]
	return v
}

// bytes reads n bytes; n == -1 is a null value and returns nil.
func (c *cursor) bytes(n int64) []byte {
	if c.err != nil {
		return nil
	}
	if n == -1 {
		return nil
	}
	if n < 0 || n > int64(len(c.b)) {
		c.fail("a length runs past the record")
		return nil
	}
	out := append([]byte{}, c.b[:n]...) // never nil for a present value, even when empty.
	c.b = c.b[n:]
	return out
}

// DecodeRecords returns every record of a complete batch, whatever its compression.
func DecodeRecords(batch []byte) ([]Record, error) {
	if len(batch) < batchFullHeaderBytes {
		return nil, fmt.Errorf("%w: shorter than a header", ErrBadRecords)
	}
	attrs := binary.BigEndian.Uint16(batch[batchAttributesOff:])
	count := int64(int32(binary.BigEndian.Uint32(batch[batchRecordCountOff:])))
	body, err := inflate(int(attrs&batchCompressionMask), batch[batchFullHeaderBytes:])
	if err != nil {
		return nil, err
	}
	// A record is at least a few bytes, so a count the body cannot hold is a lie, and
	// would be an allocation of whatever size the lie names.
	if count < 0 || count > int64(len(body)) {
		return nil, fmt.Errorf("%w: %d records in %d bytes", ErrBadRecords, count, len(body))
	}
	out := make([]Record, 0, count)
	c := &cursor{b: body}
	for i := int64(0); i < count; i++ {
		length := c.varint()
		if c.err != nil || length < 0 || length > int64(len(c.b)) {
			c.fail("a record's length runs past the batch")
			break
		}
		rc := &cursor{b: c.b[:length]}
		c.b = c.b[length:]
		var r Record
		r.Attributes = int8(rc.byte())
		r.TimestampDelta = rc.varint()
		r.OffsetDelta = int32(rc.varint())
		r.Key = rc.bytes(rc.varint())
		r.Value = rc.bytes(rc.varint())
		n := rc.varint()
		if rc.err == nil && (n < 0 || n > int64(len(rc.b))) {
			rc.fail("a header count runs past the record")
		}
		for j := int64(0); j < n && rc.err == nil; j++ {
			k := rc.bytes(rc.varint())
			v := rc.bytes(rc.varint())
			r.Headers = append(r.Headers, RecordHeader{Key: string(k), Value: v})
		}
		if rc.err != nil {
			return nil, rc.err
		}
		out = append(out, r)
	}
	if c.err != nil {
		return nil, c.err
	}
	return out, nil
}

func appendVarint(b []byte, v int64) []byte {
	var put [binary.MaxVarintLen64]byte
	return append(b, put[:binary.PutVarint(put[:], v)]...)
}

func appendBytes(b, v []byte) []byte {
	if v == nil {
		return appendVarint(b, -1)
	}
	return append(appendVarint(b, int64(len(v))), v...)
}

// EncodeRecords is the records section of a batch body for recs, uncompressed.
func EncodeRecords(recs []Record) []byte {
	var body []byte
	for _, r := range recs {
		var rec []byte
		rec = append(rec, byte(r.Attributes))
		rec = appendVarint(rec, r.TimestampDelta)
		rec = appendVarint(rec, int64(r.OffsetDelta))
		rec = appendBytes(rec, r.Key)
		rec = appendBytes(rec, r.Value)
		rec = appendVarint(rec, int64(len(r.Headers)))
		for _, h := range r.Headers {
			rec = appendBytes(rec, []byte(h.Key))
			rec = appendBytes(rec, h.Value)
		}
		body = appendVarint(body, int64(len(rec)))
		body = append(body, rec...)
	}
	return body
}

// compress packs a records section with a codec.
func compress(codec int, data []byte) ([]byte, error) {
	switch codec {
	case codecNone:
		return data, nil
	case codecGzip:
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		if _, err := w.Write(data); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	case codecSnappy:
		return snappy.Encode(nil, data), nil
	case codecLZ4:
		var b bytes.Buffer
		w := lz4.NewWriter(&b)
		if _, err := w.Write(data); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	case codecZstd:
		e, err := zstd.NewWriter(nil)
		if err != nil {
			return nil, err
		}
		defer e.Close()
		return e.EncodeAll(data, nil), nil
	}
	return nil, fmt.Errorf("log: unknown compression codec %d", codec)
}

// CodecByName maps a topic's compression.type to a codec, and whether it names one the
// cleaner should write: "producer", "uncompressed" and anything else mean the records
// are rewritten uncompressed.
func CodecByName(name string) int {
	switch name {
	case "gzip":
		return codecGzip
	case "snappy":
		return codecSnappy
	case "lz4":
		return codecLZ4
	case "zstd":
		return codecZstd
	}
	return codecNone
}

// rebuildBatch builds a batch from orig's header with recs as its records, compressed
// with codec. Everything in the header that says where the batch sits in the log and who
// wrote it stays as it was: the base offset and last offset delta (so offsets never
// move), the producer id, epoch and base sequence (so idempotence and transactions stay
// valid) and the timestamps. The record count is the number kept.
func rebuildBatch(orig []byte, recs []Record, codec int) ([]byte, error) {
	body, err := compress(codec, EncodeRecords(recs))
	if err != nil {
		return nil, err
	}
	out := make([]byte, batchFullHeaderBytes+len(body))
	copy(out, orig[:batchFullHeaderBytes])
	binary.BigEndian.PutUint32(out[batchLengthOff:], uint32(len(out)-batchOverhead))
	attrs := binary.BigEndian.Uint16(out[batchAttributesOff:])&^uint16(batchCompressionMask) | uint16(codec)
	binary.BigEndian.PutUint16(out[batchAttributesOff:], attrs)
	binary.BigEndian.PutUint32(out[batchRecordCountOff:], uint32(len(recs)))
	copy(out[batchFullHeaderBytes:], body)
	sealCRCOf(out)
	return out, nil
}
