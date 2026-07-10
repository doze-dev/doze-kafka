package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrTruncated is reported (via Reader.Err) when a decode reads past the buffer.
var ErrTruncated = errors.New("protocol: buffer truncated")

// Reader decodes Kafka wire primitives from a byte slice, tracking a cursor and
// a sticky error. After the first error every read is a no-op returning the zero
// value, so callers may decode a whole struct and then check Err once.
type Reader struct {
	buf []byte
	pos int
	err error
}

// NewReader returns a Reader positioned at the start of buf.
func NewReader(buf []byte) *Reader { return &Reader{buf: buf} }

func (r *Reader) fail(err error) {
	if r.err == nil {
		r.err = err
	}
}

func (r *Reader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if r.pos+n > len(r.buf) {
		r.fail(fmt.Errorf("%w: need %d bytes at offset %d of %d", ErrTruncated, n, r.pos, len(r.buf)))
		return false
	}
	return true
}

// Int8 reads a signed 8-bit integer.
func (r *Reader) Int8() int8 {
	if !r.need(1) {
		return 0
	}
	v := int8(r.buf[r.pos])
	r.pos++
	return v
}

// Int16 reads a big-endian signed 16-bit integer.
func (r *Reader) Int16() int16 {
	if !r.need(2) {
		return 0
	}
	v := int16(binary.BigEndian.Uint16(r.buf[r.pos:]))
	r.pos += 2
	return v
}

// Int32 reads a big-endian signed 32-bit integer.
func (r *Reader) Int32() int32 {
	if !r.need(4) {
		return 0
	}
	v := int32(binary.BigEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	return v
}

// Int64 reads a big-endian signed 64-bit integer.
func (r *Reader) Int64() int64 {
	if !r.need(8) {
		return 0
	}
	v := int64(binary.BigEndian.Uint64(r.buf[r.pos:]))
	r.pos += 8
	return v
}

// NullableString reads an int16-length-prefixed string. A length of -1 is the
// null encoding and yields ("", false); otherwise ok is true.
func (r *Reader) NullableString() (value string, ok bool) {
	n := r.Int16()
	if r.err != nil || n < 0 {
		return "", false
	}
	if !r.need(int(n)) {
		return "", false
	}
	s := string(r.buf[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return s, true
}

// Remaining returns the bytes not yet consumed (e.g. the request body after the
// header). It returns nil once an error has occurred.
func (r *Reader) Remaining() []byte {
	if r.err != nil {
		return nil
	}
	return r.buf[r.pos:]
}

// Pos returns the current cursor offset into the buffer.
func (r *Reader) Pos() int { return r.pos }

// Err returns the first decode error encountered, if any.
func (r *Reader) Err() error { return r.err }
