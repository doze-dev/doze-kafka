package broker_test

import (
	"encoding/binary"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
	"github.com/doze-dev/doze-kafka/internal/traffic"
)

func askVersions(t *testing.T, c *rawConn, version int16, name, softwareVersion string) *kmsg.ApiVersionsResponse {
	t.Helper()
	req := kmsg.NewPtrApiVersionsRequest()
	req.ClientSoftwareName, req.ClientSoftwareVersion = name, softwareVersion
	return ask(t, c, req, version, kmsg.NewPtrApiVersionsResponse())
}

// From v3 a client names itself in ApiVersions, and Apache Kafka refuses a name or a
// version that is empty or is not letters and digits with dots and dashes between them:
// INVALID_REQUEST and no APIs, on a connection that lives on (recorded:
// conformance/recordings/apiversions-client-software.json, which the replay compares
// step by step). This broker answered 0 and the whole list to all of them, and kept
// whatever it was sent as the client's software.
func TestApiVersionsRefusesTheClientSoftwareKafkaRefuses(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	for _, tc := range []struct {
		version       int16
		name, release string
		refused       bool
	}{
		{3, "apache-kafka-java", "3.7.0", false},
		{4, "librdkafka", "2.15.1", false},
		{3, "a", "1", false},
		{3, "my.app-name", "1.0-rc.1", false},
		{2, "", "", false}, // before v3 there is nothing to refuse.
		{3, "", "", true},
		{3, "", "1.0", true},
		{3, "my-app", "", true},
		{3, "my app", "1.0", true},
		{3, "my_app", "1.0", true},
		{3, "-app", "1.0", true},
		{3, "app.", "1.0", true},
		{3, "café", "1.0", true},
		{3, "my-app", "1.0+build5", true},
		{3, "my-app", "1.0-", true},
		{4, "my app", "1.0", true},
		{3, strings.Repeat("a", 20000), "1.0", false}, // long is not refused.
	} {
		resp := askVersions(t, c, tc.version, tc.name, tc.release)
		switch {
		case tc.refused && (resp.ErrorCode != 42 || len(resp.ApiKeys) != 0):
			t.Errorf("v%d %q %q: error %d with %d APIs, want INVALID_REQUEST (42) and none", tc.version, tc.name, tc.release, resp.ErrorCode, len(resp.ApiKeys))
		case !tc.refused && (resp.ErrorCode != 0 || len(resp.ApiKeys) < 20):
			t.Errorf("v%d %.20q %q: error %d with %d APIs, want 0 and the list", tc.version, tc.name, tc.release, resp.ErrorCode, len(resp.ApiKeys))
		}
	}

	// What a refused request called the client is not what the client is: the name
	// taken before it stands.
	fresh := openRaw(t, addr)
	askVersions(t, fresh, 3, "good-name", "1.0")
	askVersions(t, fresh, 3, "bad name", "9.9")
	ask(t, fresh, kmsg.NewPtrMetadataRequest(), 9, kmsg.NewPtrMetadataResponse())
	eventually(t, 5*time.Second, "the three requests to be recorded", func() bool { return serverhook.Traffic(srv).Requests() >= 20 })
	var software []string
	for _, cl := range serverhook.Traffic(srv).Clients() {
		software = append(software, cl.Software)
	}
	if len(software) != 2 || software[1] != "good-name 1.0" {
		t.Errorf("the clients' software is %q, want the second to be %q", software, "good-name 1.0")
	}
	// And the refusal is a row that says why.
	rows := serverhook.Traffic(srv).Events(traffic.Filter{Conn: 2, ErrorsOnly: true}, 0)
	if len(rows) != 1 || rows[0].Error != "INVALID_REQUEST" || !strings.Contains(rows[0].Detail, "bad name") {
		t.Errorf("the refused request's rows = %+v", rows)
	}
}

// An ApiVersions request at a version the broker does not serve is answered in the v0
// form with UNSUPPORTED_VERSION and exactly one API: ApiVersions itself, with the
// versions served, which is all the client needs to ask again (recorded:
// conformance/recordings/apiversions-unsupported-version.json). This broker sent its
// whole table.
func TestApiVersionsAtAVersionNotServedNamesOnlyItself(t *testing.T) {
	for major, highest := range map[int]int16{1: 1, 2: 2, 3: 3, 4: 4} {
		addr := startBroker(t, major)
		for _, version := range []int16{highest + 1, 99, 32767} {
			// The header of a flexible request ends in an empty tagged section; the body
			// of a version the broker does not know is not read.
			answered, resp := rawRequest(t, addr, 18, version, []byte{0, 7, 'm', 'y', '-', 'a', 'p', 'p', 4, '1', '.', '0', 0})
			if !answered {
				t.Fatalf("Kafka %d, ApiVersions v%d: the connection was closed", major, version)
			}
			body := resp[4:] // after the correlation id: the v0 form has no tagged section.
			if len(body) != 2+4+6 {
				t.Fatalf("Kafka %d, ApiVersions v%d: a body of %d bytes, want 12 (an error, and one API)", major, version, len(body))
			}
			code, n := int16(binary.BigEndian.Uint16(body)), binary.BigEndian.Uint32(body[2:])
			key, lo, hi := int16(binary.BigEndian.Uint16(body[6:])), int16(binary.BigEndian.Uint16(body[8:])), int16(binary.BigEndian.Uint16(body[10:]))
			if code != 35 || n != 1 || key != 18 || lo != 0 || hi != highest {
				t.Errorf("Kafka %d, ApiVersions v%d: error %d, %d APIs, the first %d v%d to v%d; want 35, one, 18 v0 to v%d",
					major, version, code, n, key, lo, hi, highest)
			}
		}
	}
}

// What a client calls itself is kept for as long as its connection is open, and a
// name may be as long as a frame (Kafka refuses none for its length). It is kept
// short: 300 connections each holding a 1 MiB name took the broker from 42 MB to
// 563 MB. Here forty of them hold names that fill a frame.
func TestAClientsSoftwareNameIsKeptShort(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	name := strings.Repeat("a", 1<<20-64) // with the header, a control frame's limit.
	heap := func() uint64 {
		var m runtime.MemStats
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	before := heap()
	const conns = 40
	for range conns {
		c := openRaw(t, addr) // held open until the test ends.
		if resp := askVersions(t, c, 3, name, "1.0"); resp.ErrorCode != 0 {
			t.Fatalf("a name of %d bytes: error %d, want it taken", len(name), resp.ErrorCode)
		}
	}
	if grew := int64(heap()) - int64(before); grew > 8<<20 {
		t.Errorf("%d connections naming themselves with %d bytes each hold %d MiB, want under 8", conns, len(name), grew>>20)
	}
	eventually(t, 5*time.Second, "every connection to be a client", func() bool { return len(serverhook.Traffic(srv).Clients()) == conns })
	clients := serverhook.Traffic(srv).Clients()
	if len(clients) != conns {
		t.Fatalf("%d clients, want %d", len(clients), conns)
	}
	for _, cl := range clients {
		if len(cl.Software) > protocol.MaxClientSoftware || !strings.HasPrefix(cl.Software, "aaaa") {
			t.Fatalf("a client's software is %d bytes (%.20q…), want at most %d", len(cl.Software), cl.Software, protocol.MaxClientSoftware)
		}
	}
}
