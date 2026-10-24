package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// PushTelemetry may be a megabyte of metrics and its header: a frame of exactly the cap
// is served, one byte more is refused, which only the cap for its own API allows.
func TestPushTelemetryFrameCapIsAMegabytePlusItsHeader(t *testing.T) {
	r := echoRouter()
	r.Register(kmsg.PushTelemetry, func(_ context.Context, req *Request) ([]byte, error) {
		return binary.BigEndian.AppendUint32(nil, uint32(len(req.Body))), nil
	})
	addr := listen(t, r, false, nil)

	frame := func(total int) []byte {
		// key, version, correlation id, null client id: 10 bytes; the rest is body, led by
		// the empty tagged fields of a flexible request header.
		hdr, payload := frameFor(kmsg.PushTelemetry, 0, 1, make([]byte, total-10)...)
		return append(hdr, payload...)
	}
	const cap = 1<<20 + 4096
	if maxPushTelemetryFrameBytes != cap {
		t.Fatalf("maxPushTelemetryFrameBytes = %d, want %d", maxPushTelemetryFrameBytes, cap)
	}

	ok := dial(t, addr)
	ok.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := ok.Write(frame(cap)); err != nil {
		t.Fatal(err)
	}
	if resp, err := ReadFrame(ok); err != nil || binary.BigEndian.Uint32(resp) != cap-10 {
		t.Fatalf("a frame of exactly the cap: %v %v", resp, err)
	}

	big := dial(t, addr)
	big.Write(frame(cap + 1))
	expectClosed(t, big, 2*time.Second)
}

// A frame of exactly the chunk is read whole; with nothing after its prefix that is the
// end of the stream, and one byte more is a stream cut short.
func TestReadFrameAtTheChunkSize(t *testing.T) {
	prefix := func(n uint32) *bytes.Reader { return bytes.NewReader(binary.BigEndian.AppendUint32(nil, n)) }
	if _, err := ReadFrameLimit(prefix(frameChunk), 1<<20); !errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("a chunk with no body: %v, want io.EOF", err)
	}
	if _, err := ReadFrameLimit(prefix(frameChunk+1), 1<<20); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("a chunk and a byte with no body: %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestNullableStringOfLengthZeroIsPresent(t *testing.T) {
	if s, ok := NewReader([]byte{0, 0}).NullableString(); s != "" || !ok {
		t.Errorf("empty string: %q %v", s, ok)
	}
	if s, ok := NewReader([]byte{0xff, 0xff}).NullableString(); s != "" || ok {
		t.Errorf("null string: %q %v", s, ok)
	}
	if s, ok := NewReader([]byte{0, 2, 'h', 'i'}).NullableString(); s != "hi" || !ok {
		t.Errorf("string: %q %v", s, ok)
	}
}

func TestReaderKeepsItsFirstError(t *testing.T) {
	r := NewReader([]byte{1})
	r.Int32()
	first := r.Err()
	if first == nil || !errors.Is(first, ErrTruncated) {
		t.Fatalf("first error = %v", first)
	}
	r.Int16()
	if r.Err() != first {
		t.Errorf("the error was replaced: %v", r.Err())
	}
}
