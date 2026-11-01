package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// A frame that claims exactly the largest size any request may be (a Produce's) is still
// refused when its key is not a Produce: the size is judged by the key, not by the largest.
func TestAFrameAsLargeAsAProduceMayBeIsRefusedForAnotherKey(t *testing.T) {
	limitFor := func(key APIKey) int {
		if key == kmsg.Produce {
			return maxFrameBytes
		}
		return maxControlFrameBytes
	}
	frame := func(key APIKey) *bufio.Reader {
		b := binary.BigEndian.AppendUint32(nil, maxFrameBytes)
		b = binary.BigEndian.AppendUint16(b, uint16(key))
		return bufio.NewReader(bytes.NewReader(b))
	}
	if _, err := readRequestFrame(frame(kmsg.Metadata), limitFor); err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Errorf("a Metadata frame of the Produce maximum: %v, want it refused as too large", err)
	}
	// For a Produce the same size passes the limit (the body is then simply not there).
	if _, err := readRequestFrame(frame(kmsg.Produce), limitFor); err == nil || strings.Contains(err.Error(), "exceeds max") {
		t.Errorf("a Produce frame of the Produce maximum: %v, want it past the size check", err)
	}
}
