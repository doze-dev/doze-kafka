package broker_test

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestUnhandledAPI_ClosesConnectionNotHang covers the fix for a real DX bug: a
// client that calls an API the broker doesn't serve (one not advertised in
// ApiVersions — e.g. some Kafka UIs probe CreatePartitions) used to get no response
// and hang until its own timeout. The broker must instead close the connection so
// the client fails fast.
func TestUnhandledAPI_ClosesConnectionNotHang(t *testing.T) {
	addr := startBroker(t, 4)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// A well-formed CreatePartitions (api key 37), which this broker does not
	// offer. (This used to send DescribeConfigs, which was unserved then and is
	// served now, with no body: it was closed for being malformed, so the test
	// passed without testing what it claimed to.)
	payload := append([]byte{0, 37, 0, 0, 0, 0, 0, 1, 0xff, 0xff}, kmsg.NewPtrCreatePartitionsRequest().AppendTo(nil)...)
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	copy(frame[4:], payload)
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The broker must close the connection (read returns EOF/reset), not hang.
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 16)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("expected the connection to close on an unhandled API, got data")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("connection hung on an unhandled API instead of closing (the DX bug)")
	}
}
