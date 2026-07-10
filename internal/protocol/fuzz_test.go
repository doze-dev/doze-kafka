package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
)

// The request path parses bytes straight off a client socket, so it must never
// panic on malformed input — it must always fail cleanly with an error. These
// fuzz targets feed arbitrary bytes through the frame reader, the header parser,
// and the kmsg request decoder and assert only that nothing panics (and that the
// frame-size cap holds).

// FuzzParseAndDecodeRequest drives the full decode chain a real request takes:
// ParseRequestHeader → DecodeRequest.
func FuzzParseAndDecodeRequest(f *testing.F) {
	// A well-formed-ish Metadata v0 request header + empty body.
	seed := []byte{
		0, 3, // api key = 3 (Metadata)
		0, 0, // api version = 0
		0, 0, 0, 1, // correlation id = 1
		0, 4, 't', 'e', 's', 't', // client id = "test"
	}
	f.Add(seed)
	f.Add(append(append([]byte{}, seed...), 0xff, 0x00, 0x12, 0x34)) // + junk body
	f.Add([]byte{})
	f.Add([]byte{0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		header, off, err := ParseRequestHeader(data)
		if err != nil {
			return
		}
		if off < 0 || off > len(data) {
			t.Fatalf("body offset %d out of range for %d bytes", off, len(data))
		}
		// Decoding untrusted body bytes must also never panic.
		_, _ = DecodeRequest(header, data[off:])
	})
}

// FuzzSkipTaggedFields fuzzes the flexible-version tagged-field skipper directly —
// it walks length-prefixed sections and is the trickiest hand-rolled parser.
func FuzzSkipTaggedFields(f *testing.F) {
	f.Add([]byte{0x00})                   // zero tagged fields
	f.Add([]byte{0x01, 0x00, 0x00})       // one empty field
	f.Add([]byte{0xff, 0xff, 0xff, 0xff}) // garbage varints
	f.Fuzz(func(t *testing.T, data []byte) {
		out := skipTaggedFields(data)
		if len(out) > len(data) {
			t.Fatalf("skipTaggedFields grew the slice: %d > %d", len(out), len(data))
		}
	})
}

// FuzzReadFrame confirms the length-prefixed framing never panics and honors the
// size cap regardless of the prefix.
func FuzzReadFrame(f *testing.F) {
	mkframe := func(payload []byte) []byte {
		buf := make([]byte, 4+len(payload))
		binary.BigEndian.PutUint32(buf, uint32(len(payload)))
		copy(buf[4:], payload)
		return buf
	}
	f.Add(mkframe([]byte("hello")))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0x00}) // huge advertised length, no data
	f.Add([]byte{0, 0, 0, 0})                   // zero-length frame
	f.Fuzz(func(t *testing.T, data []byte) {
		payload, err := ReadFrame(bufio.NewReader(bytes.NewReader(data)))
		if err != nil {
			return
		}
		if len(payload) > maxFrameBytes {
			t.Fatalf("ReadFrame returned %d bytes, above cap %d", len(payload), maxFrameBytes)
		}
	})
}
