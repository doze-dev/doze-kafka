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
		{"InitProducerId v100", 22, 100, ipid(1), false},
		{"InitProducerId v-1", 22, -1, ipid(1), false},
		{"Produce v2 (below the minimum)", 0, 2, nil, false},
		{"Fetch v3 (below the minimum)", 1, 3, nil, false},
		{"Metadata v13 (above the maximum)", 3, 13, nil, false},
		{"CreatePartitions (no such API here)", 37, 0, nil, false},
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
