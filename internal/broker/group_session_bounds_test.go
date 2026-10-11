package broker_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// BAD-29. A classic member's session.timeout.ms outside Kafka's
// group.min.session.timeout.ms (6000) and group.max.session.timeout.ms (1800000),
// both inclusive, is INVALID_SESSION_TIMEOUT on every JoinGroup version, and is
// checked before the member id is handed out. Bounds and order are recorded
// against apache/kafka (conformance/recordings/joingroup-session-timeout.json).
func TestJoinGroupRefusesASessionTimeoutOutsideTheBounds(t *testing.T) {
	addr := startBroker(t, 4)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))

	cases := []struct {
		session int32
		refused bool
	}{
		{-1, true}, {0, true}, {1000, true}, {5999, true},
		{6000, false}, {6001, false}, {45000, false}, {1800000, false},
		{1800001, true}, {2147483647, true},
	}
	n := 0
	for v := int16(0); v <= 9; v++ {
		for _, c := range cases {
			n++
			req := kmsg.NewPtrJoinGroupRequest()
			req.SetVersion(v)
			req.Group = fmt.Sprintf("bounds-v%d-%d", v, n)
			req.SessionTimeoutMillis = c.session
			req.RebalanceTimeoutMillis = 30000
			req.ProtocolType = "consumer"
			p := kmsg.NewJoinGroupRequestProtocol()
			p.Name, p.Metadata = "range", []byte{0, 1, 2, 3}
			req.Protocols = append(req.Protocols, p)
			resp := rawJoin(t, conn, req, v, int32(n))
			switch {
			case c.refused && resp.ErrorCode != kerr.InvalidSessionTimeout.Code:
				t.Errorf("v%d session %d ms: code %d, want INVALID_SESSION_TIMEOUT (26)", v, c.session, resp.ErrorCode)
			case !c.refused && resp.ErrorCode == kerr.InvalidSessionTimeout.Code:
				t.Errorf("v%d session %d ms: refused, but it is inside the bounds", v, c.session)
			case c.refused && resp.MemberID != "":
				t.Errorf("v%d session %d ms: a member id %q was handed out with the refusal", v, c.session, resp.MemberID)
			}
		}
	}
}

// rawJoin sends one JoinGroup at exactly version v (a client library picks the
// highest version both sides know, which would leave v0 to v8 untested).
func rawJoin(t *testing.T, conn net.Conn, req *kmsg.JoinGroupRequest, v int16, corr int32) *kmsg.JoinGroupResponse {
	t.Helper()
	req.SetVersion(v)
	flexible := v >= 6
	hdr := binary.BigEndian.AppendUint16(nil, uint16(kmsg.JoinGroup))
	hdr = binary.BigEndian.AppendUint16(hdr, uint16(v))
	hdr = binary.BigEndian.AppendUint32(hdr, uint32(corr))
	hdr = binary.BigEndian.AppendUint16(hdr, 4)
	hdr = append(hdr, "test"...)
	if flexible {
		hdr = append(hdr, 0) // header tagged fields.
	}
	body := req.AppendTo(nil)
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(hdr)+len(body)))
	frame = append(append(frame, hdr...), body...)
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("v%d: write: %v", v, err)
	}
	var size [4]byte
	if _, err := io.ReadFull(conn, size[:]); err != nil {
		t.Fatalf("v%d: read: %v", v, err)
	}
	buf := make([]byte, binary.BigEndian.Uint32(size[:]))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("v%d: read: %v", v, err)
	}
	skip := 4
	if flexible {
		skip++ // response header tagged fields.
	}
	resp := kmsg.NewPtrJoinGroupResponse()
	resp.SetVersion(v)
	if err := resp.ReadFrom(buf[skip:]); err != nil {
		t.Fatalf("v%d: decode: %v", v, err)
	}
	return resp
}
