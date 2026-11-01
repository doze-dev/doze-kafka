// Package conformance is the real-broker oracle: short scripts of Kafka
// requests, recorded once against Apache Kafka in Docker and replayed against
// doze-kafka in CI, with no Docker, so "what Kafka answers" is a committed file
// and not somebody's memory.
//
// A scenario (scenarios.go) is a Go function that sends raw protocol requests
// at an exact version through Env. The recorder (docker.go) runs it against
// apache/kafka, normalises the parts that are different every run (broker ids,
// hosts and ports, topic ids, member ids, producer ids) and writes
// recordings/<scenario>.json. The replay test (conformance_test.go) runs the
// same script against an in-process doze-kafka and compares step by step. A
// difference is a failure unless the scenario lists it as Known, with the
// sentence in docs/limitations.md that explains it.
//
//	go tool task conformance:record                 # needs Docker; rewrites recordings/
//	go tool task conformance:record -- -only=listoffsets
//	go test ./conformance                           # replay; no Docker
package conformance

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// wire is one raw connection to a broker: a request at a chosen version, one
// response, nothing negotiated, so a script says exactly what goes on the wire.
type wire struct {
	conn net.Conn
	corr int32
	fast bool // a short deadline, for readiness probes.
}

func dial(addr string) (*wire, error) {
	var last error
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			return &wire{conn: c}, nil
		}
		last = err
	}
	return nil, last
}

func (w *wire) close() { w.conn.Close() }

// dialOnce is one connection attempt.
func dialOnce(addr string) (*wire, error) {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &wire{conn: c}, nil
}

// probe asks for the cluster's brokers and reports whether one is there.
func probe(w *wire) bool {
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{}
	w.fast = true
	resp, err := w.roundTrip(req, 9)
	if err != nil {
		return false
	}
	m, ok := resp.(*kmsg.MetadataResponse)
	return ok && len(m.Brokers) > 0
}

// send writes req at version without reading anything back: for a request the
// broker answers with nothing (Produce with acks=0).
func (w *wire) send(req kmsg.Request, version int16) error {
	req.SetVersion(version)
	w.corr++
	w.conn.SetDeadline(time.Now().Add(10 * time.Second))
	_, err := w.conn.Write(w.frame(req, version))
	return err
}

func (w *wire) frame(req kmsg.Request, version int16) []byte {
	var buf []byte
	buf = binary.BigEndian.AppendUint16(buf, uint16(req.Key()))
	buf = binary.BigEndian.AppendUint16(buf, uint16(version))
	buf = binary.BigEndian.AppendUint32(buf, uint32(w.corr))
	const clientID = "doze-conformance"
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(clientID)))
	buf = append(buf, clientID...)
	if req.IsFlexible() {
		buf = append(buf, 0) // header tagged fields.
	}
	buf = req.AppendTo(buf)
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(buf)))
	return append(frame, buf...)
}

// roundTrip sends req at version and reads its response, within a minute (three
// seconds for a probe).
func (w *wire) roundTrip(req kmsg.Request, version int16) (kmsg.Response, error) {
	limit := 60 * time.Second
	if w.fast {
		limit = 3 * time.Second
	}
	return w.roundTripWithin(req, version, limit)
}

// roundTripWithin is roundTrip with the time the answer may take.
func (w *wire) roundTripWithin(req kmsg.Request, version int16, limit time.Duration) (kmsg.Response, error) {
	req.SetVersion(version)
	w.corr++
	var buf []byte
	buf = binary.BigEndian.AppendUint16(buf, uint16(req.Key()))
	buf = binary.BigEndian.AppendUint16(buf, uint16(version))
	buf = binary.BigEndian.AppendUint32(buf, uint32(w.corr))
	const clientID = "doze-conformance"
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(clientID)))
	buf = append(buf, clientID...)
	if req.IsFlexible() {
		buf = append(buf, 0) // header tagged fields.
	}
	buf = req.AppendTo(buf)
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(buf)))
	frame = append(frame, buf...)

	w.conn.SetDeadline(time.Now().Add(limit))
	if _, err := w.conn.Write(frame); err != nil {
		return nil, err
	}
	var size [4]byte
	if _, err := io.ReadFull(w.conn, size[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(size[:])
	if n > 64<<20 {
		return nil, fmt.Errorf("response of %d bytes", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(w.conn, body); err != nil {
		return nil, err
	}
	if len(body) < 4 {
		return nil, errors.New("short response")
	}
	if got := int32(binary.BigEndian.Uint32(body)); got != w.corr {
		return nil, fmt.Errorf("correlation id %d, want %d", got, w.corr)
	}
	body = body[4:]
	resp := req.ResponseKind()
	resp.SetVersion(version)
	// An ApiVersions response always has the old header; every other flexible
	// response has tagged fields after the correlation id.
	if resp.IsFlexible() && req.Key() != int16(kmsg.ApiVersions) {
		var err error
		if body, err = skipTagged(body); err != nil {
			return nil, err
		}
	}
	if err := resp.ReadFrom(body); err != nil {
		return nil, fmt.Errorf("decode %T v%d: %w", resp, version, err)
	}
	return resp, nil
}

func skipTagged(b []byte) ([]byte, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, errors.New("bad tagged field count")
	}
	b = b[n:]
	for i := uint64(0); i < count; i++ {
		_, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, errors.New("bad tag")
		}
		b = b[n:]
		size, n := binary.Uvarint(b)
		if n <= 0 || uint64(len(b)-n) < size {
			return nil, errors.New("bad tag size")
		}
		b = b[n+int(size):]
	}
	return b, nil
}

// saslPlain logs the connection in with SASL/PLAIN: SaslHandshake v1, then
// SaslAuthenticate v2 with the authzid, user and password.
func (w *wire) saslPlain(user, pass string) error {
	h := kmsg.NewPtrSASLHandshakeRequest()
	h.Mechanism = "PLAIN"
	resp, err := w.roundTrip(h, 1)
	if err != nil {
		return err
	}
	if code := resp.(*kmsg.SASLHandshakeResponse).ErrorCode; code != 0 {
		return fmt.Errorf("SaslHandshake: error %d", code)
	}
	a := kmsg.NewPtrSASLAuthenticateRequest()
	a.SASLAuthBytes = []byte("\x00" + user + "\x00" + pass)
	resp, err = w.roundTrip(a, 2)
	if err != nil {
		return err
	}
	if r := resp.(*kmsg.SASLAuthenticateResponse); r.ErrorCode != 0 {
		return fmt.Errorf("SaslAuthenticate: error %d", r.ErrorCode)
	}
	return nil
}
