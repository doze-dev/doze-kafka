package broker_test

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// rawRequest sends one request frame (v0/v1 header: null client id, no tags) and
// reports whether the broker answered or closed the connection.
func rawRequest(t *testing.T, addr string, key, version int16, body []byte) (answered bool, resp []byte) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	hdr := []byte{byte(key >> 8), byte(key), byte(version >> 8), byte(version), 0, 0, 0, 9, 0xff, 0xff}
	frame := make([]byte, 4, 4+len(hdr)+len(body))
	binary.BigEndian.PutUint32(frame, uint32(len(hdr)+len(body)))
	frame = append(append(frame, hdr...), body...)
	c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write(frame); err != nil {
		t.Fatal(err)
	}
	var n [4]byte
	if _, err := io.ReadFull(c, n[:]); err != nil {
		return false, nil
	}
	resp = make([]byte, binary.BigEndian.Uint32(n[:]))
	if _, err := io.ReadFull(c, resp); err != nil {
		return false, nil
	}
	return true, resp
}

// wellFormed is a request with default fields, encoded at the given version.
func wellFormed(r kmsg.Request, version int16) []byte {
	r.SetVersion(version)
	return r.AppendTo(nil)
}

// The broker serves exactly what ApiVersions advertises. It used to answer any
// version kmsg could decode and any API with a handler: Produce v0 (a format the
// log cannot store) was appended, Fetch v0 was answered with batches a v0 client
// cannot read, and InitProducerId v100 handed out a producer id.
func TestOnlyAdvertisedAPIsAndVersionsAreServed(t *testing.T) {
	addr := startBroker(t, 4)
	ipid := func(v int16) []byte {
		r := kmsg.NewPtrInitProducerIDRequest()
		r.SetVersion(v)
		return r.AppendTo(nil)
	}
	for _, tc := range []struct {
		name         string
		key, version int16
		body         []byte
		want         bool
	}{
		{"InitProducerId v1 (advertised)", 22, 1, ipid(1), true},
		{"CreatePartitions v4 (above the maximum)", 37, 4, wellFormed(kmsg.NewPtrCreatePartitionsRequest(), 3), false},
		{"InitProducerId v100", 22, 100, ipid(1), false},
		{"InitProducerId v-1", 22, -1, ipid(1), false},
		// Well-formed requests, so that being refused is about the version and not
		// about a body that would not have decoded anyway.
		{"Produce v2 (below the minimum)", 0, 2, wellFormed(kmsg.NewPtrProduceRequest(), 2), false},
		{"Fetch v3 (below the minimum)", 1, 3, wellFormed(kmsg.NewPtrFetchRequest(), 3), false},
		{"Metadata v13 (above the maximum)", 3, 13, wellFormed(kmsg.NewPtrMetadataRequest(), 13), false},
		{"DescribeLogDirs (no such API here)", 35, 0, wellFormed(kmsg.NewPtrDescribeLogDirsRequest(), 0), false},
		// ApiVersions is the exception: an unknown version is told what is supported.
		{"ApiVersions v99", 18, 99, nil, true},
	} {
		if got, _ := rawRequest(t, addr, tc.key, tc.version, tc.body); got != tc.want {
			t.Errorf("%s: answered=%v, want %v", tc.name, got, tc.want)
		}
	}

	// An API that exists in a newer profile is not served by an older one.
	old := startBroker(t, 1)
	dc := kmsg.NewPtrDescribeClusterRequest()
	if got, _ := rawRequest(t, old, 60, 0, dc.AppendTo(nil)); got {
		t.Error("DescribeCluster was answered by the Kafka 1 profile, which does not advertise it")
	}
}

// The patched kmsg must be the one the broker is compiled against for anyone who
// imports this module, not only when it is the main module. A `replace` directive
// does not travel to importers (and stops `go install pkg@version` working), so
// the fork lives inside the module and broker code must import that copy.
func TestBrokerCodeUsesTheInModuleKmsg(t *testing.T) {
	root := filepath.Join("..", "..")
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(mod), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "replace ") {
			t.Errorf("go.mod has a replace directive (%q): importers do not inherit it", line)
		}
	}
	for _, dir := range []string{"internal/broker", "internal/protocol", "server", "cmd/doze-kafka"} {
		files, _ := filepath.Glob(filepath.Join(root, dir, "*.go"))
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			src, _ := os.ReadFile(f)
			if strings.Contains(string(src), `"github.com/twmb/franz-go/pkg/kmsg"`) {
				t.Errorf("%s imports upstream kmsg; broker code must use internal/kmsg (the patched copy)", f)
			}
		}
	}
}

// rawConn speaks requests with a v1 header (null client id) over one connection.
type rawConn struct {
	t *testing.T
	c net.Conn
}

func openRaw(t *testing.T, addr string) *rawConn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &rawConn{t, c}
}

// do sends req and decodes the answer into resp; ok is false if the connection
// was closed instead.
func (r *rawConn) do(req kmsg.Request, resp kmsg.Response) (ok bool) {
	r.t.Helper()
	body := req.AppendTo(nil)
	hdr := []byte{byte(req.Key() >> 8), byte(req.Key()), byte(req.GetVersion() >> 8), byte(req.GetVersion()), 0, 0, 0, 9, 0xff, 0xff}
	if req.IsFlexible() {
		hdr = append(hdr, 0) // header tagged fields.
	}
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(hdr)+len(body)))
	r.c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := r.c.Write(append(append(frame, hdr...), body...)); err != nil {
		return false
	}
	var n [4]byte
	if _, err := io.ReadFull(r.c, n[:]); err != nil {
		return false
	}
	buf := make([]byte, binary.BigEndian.Uint32(n[:]))
	if _, err := io.ReadFull(r.c, buf); err != nil {
		return false
	}
	resp.SetVersion(req.GetVersion()) // flexibility depends on the version.
	skip := 4                         // correlation id.
	if resp.IsFlexible() && resp.Key() != 18 {
		skip++ // response header tagged fields.
	}
	if err := resp.ReadFrom(buf[skip:]); err != nil {
		r.t.Fatalf("decode %T: %v", resp, err)
	}
	return true
}

func (r *rawConn) plain(user, pass string) (code int16, open bool) {
	r.t.Helper()
	hs := kmsg.NewPtrSASLHandshakeRequest()
	hs.SetVersion(1)
	hs.Mechanism = "PLAIN"
	var hr kmsg.SASLHandshakeResponse
	if !r.do(hs, &hr) {
		return 0, false
	}
	au := kmsg.NewPtrSASLAuthenticateRequest()
	au.SetVersion(1)
	au.SASLAuthBytes = []byte("\x00" + user + "\x00" + pass)
	var ar kmsg.SASLAuthenticateResponse
	if !r.do(au, &ar) {
		return 0, false
	}
	return ar.ErrorCode, true
}

func (r *rawConn) metadataWorks() bool {
	md := kmsg.NewPtrMetadataRequest()
	md.SetVersion(1)
	var mr kmsg.MetadataResponse
	return r.do(md, &mr)
}

// A failed authentication ends the connection, so each guess costs a new one. It
// used to stay open: 5,000 wrong passwords were tried on one connection in 144 ms,
// and a failed re-authentication left an authenticated connection authenticated.
func TestFailedAuthenticationClosesTheConnection(t *testing.T) {
	addr := startAuthBroker(t, []string{"PLAIN"}, map[string]string{"alice": "secret"}, "", "")

	c := openRaw(t, addr)
	if code, open := c.plain("alice", "wrong"); !open || code != 58 {
		t.Fatalf("a wrong password: code %d, answered %v; want SASL_AUTHENTICATION_FAILED (58), answered", code, open)
	}
	if _, open := c.plain("alice", "secret"); open {
		t.Fatal("the connection was still usable after a failed authentication")
	}

	// A good login, then a failed re-authentication: the connection must not stay
	// authenticated.
	c = openRaw(t, addr)
	if code, open := c.plain("alice", "secret"); !open || code != 0 {
		t.Fatalf("a correct login: code %d, answered %v", code, open)
	}
	if !c.metadataWorks() {
		t.Fatal("an authenticated connection should be served")
	}
	c.plain("alice", "wrong")
	if c.metadataWorks() {
		t.Fatal("after a failed re-authentication the connection was still authenticated")
	}
}
