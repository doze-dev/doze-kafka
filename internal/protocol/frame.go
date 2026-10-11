package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

const (
	// maxFrameBytes caps a single request to guard against a malformed or hostile
	// length prefix. Kafka's own default max request size is 100 MiB.
	maxFrameBytes = 100 << 20

	// maxPreAuthFrameBytes is the cap on a connection that must still
	// authenticate. Everything such a connection may legitimately send —
	// ApiVersions and the SASL exchange — is well under a kilobyte, so there is
	// no reason to let a stranger ask for more than this.
	maxPreAuthFrameBytes = 1 << 20

	// maxControlFrameBytes caps every request except Produce. Only Produce carries
	// record data; everything else is names, numbers and offsets, and a megabyte
	// of that is thousands of partitions. Decoding turns each small wire element
	// into a larger struct, so the smaller the frame a stranger may send, the
	// less a hostile one can cost.
	maxControlFrameBytes = 1 << 20

	// maxPushTelemetryFrameBytes is PushTelemetry's: the 1 MiB of metrics a client was
	// told it may send (telemetry.max.bytes, Kafka's default and what is advertised), and
	// room for the header and the instance id around it.
	maxPushTelemetryFrameBytes = 1<<20 + 4096

	// frameChunk is how much of a frame is trusted up front. A frame bigger than
	// this is read as the bytes arrive, so memory follows what the peer actually
	// sends rather than what its length prefix claims.
	frameChunk = 64 << 10
)

// ReadFrame reads one length-prefixed Kafka message — a 4-byte big-endian length
// followed by that many bytes — and returns the payload without the prefix.
func ReadFrame(r io.Reader) ([]byte, error) { return ReadFrameLimit(r, maxFrameBytes) }

// ReadFrameLimit is ReadFrame with a caller-chosen size cap, refusing an
// over-limit frame before reading any of its body.
//
// The length prefix is attacker-controlled and arrives before any proof the peer
// will send that much, so allocating n bytes up front lets a four-byte message
// pin up to the cap of memory per connection (and a few hundred connections
// exhaust the machine). Past frameChunk the buffer therefore grows as data
// arrives: a peer that claims 100 MiB and sends nothing costs one chunk.
func ReadFrameLimit(r io.Reader, limit int) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 {
		return nil, fmt.Errorf("protocol: zero-length frame")
	}
	if int64(n) > int64(limit) {
		return nil, fmt.Errorf("protocol: frame of %d bytes exceeds max %d", n, limit)
	}
	if n <= frameChunk {
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		return payload, nil
	}
	var buf bytes.Buffer
	buf.Grow(frameChunk)
	if _, err := io.CopyN(&buf, r, int64(n)); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return buf.Bytes(), nil
}

// WriteFrame writes payload prefixed with its 4-byte big-endian length.
func WriteFrame(w io.Writer, payload []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// readRequestFrame reads one request, applying a size limit chosen from the API
// key (the first two bytes of every request) before any of the body is read. A
// length above every limit is refused on the length alone, without waiting for a
// key that may never come.
func readRequestFrame(r *bufio.Reader, limitFor func(key APIKey) int) ([]byte, error) {
	head, err := r.Peek(4)
	if err != nil {
		return ReadFrameLimit(r, maxControlFrameBytes) // reports the short read.
	}
	n := int64(binary.BigEndian.Uint32(head))
	if n > int64(limitFor(kmsg.Produce)) || n < 2 {
		return ReadFrameLimit(r, limitFor(kmsg.Produce)) // too large for anything, or too short to hold a key.
	}
	head, err = r.Peek(6)
	if err != nil {
		return nil, err
	}
	return ReadFrameLimit(r, limitFor(APIKey(int16(binary.BigEndian.Uint16(head[4:])))))
}
