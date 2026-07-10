package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// maxFrameBytes caps a single request to guard against a malformed or hostile
// length prefix. Kafka's own default max request size is 100 MiB.
const maxFrameBytes = 100 << 20

// ReadFrame reads one length-prefixed Kafka message — a 4-byte big-endian length
// followed by that many bytes — and returns the payload without the prefix.
func ReadFrame(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n == 0 {
		return nil, fmt.Errorf("protocol: zero-length frame")
	}
	if n > maxFrameBytes {
		return nil, fmt.Errorf("protocol: frame of %d bytes exceeds max %d", n, maxFrameBytes)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
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
