package broker_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// The version matrix. For every Kafka major the broker can emulate, every API it
// advertises, at its lowest, a middle and its highest version, a default request
// must be decoded, routed and answered, and the answer must be something a client
// can read back: the right correlation id, a body that decodes at that version and
// encodes to the same bytes. A wrong version range in the table (advertised past
// what the handler can encode) or a handler that panics on an empty request shows
// up here, for the API and version it is wrong at.

// matrixVersions is the versions of a range to try: the ends and the middle.
func matrixVersions(r protocol.VersionRange) []int16 {
	vs := []int16{r.Min}
	if mid := r.Min + (r.Max-r.Min)/2; mid != r.Min && mid != r.Max {
		vs = append(vs, mid)
	}
	if r.Max != r.Min {
		vs = append(vs, r.Max)
	}
	return vs
}

// defaultRequest is a request with every field at its default, for an API, with
// the few settings that would otherwise make the broker wait out a long poll.
func defaultRequest(key int16, version int16) kmsg.Request {
	req := kmsg.RequestForKey(key)
	switch r := req.(type) {
	case *kmsg.FetchRequest:
		r.MaxWaitMillis = 0
	case *kmsg.ProduceRequest:
		r.Acks = 1 // the default, 0, is fire-and-forget: Kafka answers it with nothing.
	case *kmsg.ConsumerGroupHeartbeatRequest:
		r.Group = "matrix"
	}
	req.SetVersion(version)
	return req
}

// matrixRequest sends one request with the header its version calls for: flexible
// versions (request header v2) carry an empty tagged-fields section after the
// client id, which rawRequest, written for the older headers, does not.
func matrixRequest(t *testing.T, addr string, key, version int16, req kmsg.Request) (answered bool, resp []byte) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return matrixRequestOn(t, c, key, version, req)
}

// matrixRequestOn is matrixRequest over a connection the caller made, for a
// request that only means something after another (SaslAuthenticate).
func matrixRequestOn(t *testing.T, c net.Conn, key, version int16, req kmsg.Request) (answered bool, resp []byte) {
	t.Helper()
	hdr := []byte{byte(key >> 8), byte(key), byte(version >> 8), byte(version), 0, 0, 0, 9, 0xff, 0xff}
	if req.IsFlexible() {
		hdr = append(hdr, 0)
	}
	body := req.AppendTo(nil)
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(hdr)+len(body)))
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

// readResponse checks a raw response payload against the request that was sent and
// reports what is wrong with it, or "".
func readResponse(key, version int16, raw []byte) string {
	if len(raw) < 4 {
		return fmt.Sprintf("a %d-byte response has no correlation id", len(raw))
	}
	if got := int32(binary.BigEndian.Uint32(raw)); got != 9 {
		return fmt.Sprintf("correlation id %d, want 9", got)
	}
	body := raw[4:]
	flexible := kmsg.RequestForKey(key)
	flexible.SetVersion(version)
	// Flexible responses carry a tagged-fields section after the correlation id,
	// except ApiVersions, whose header is fixed so a client can read it before it
	// knows the version.
	if flexible.IsFlexible() && key != int16(kmsg.ApiVersions) {
		if len(body) == 0 || body[0] != 0 {
			return "a flexible response has no empty tagged-fields section after the correlation id"
		}
		body = body[1:]
	}
	resp := kmsg.ResponseForKey(key)
	resp.SetVersion(version)
	if err := resp.ReadFrom(body); err != nil {
		return fmt.Sprintf("the body does not decode at this version: %v", err)
	}
	if again := resp.AppendTo(nil); !bytes.Equal(again, body) {
		return fmt.Sprintf("the body is not what the library would write for what it read (%d bytes vs %d): bytes were left over or dropped", len(body), len(again))
	}
	return ""
}

func TestVersionMatrix_EveryAdvertisedAPIAnswersAtEveryProfile(t *testing.T) {
	for _, major := range protocol.SupportedKafkaMajors() {
		t.Run(fmt.Sprintf("kafka%d", major), func(t *testing.T) {
			addr := startBroker(t, major)
			profile, _ := protocol.ProfileFor(major)
			for _, a := range profile.Advertised(false, true) {
				for _, v := range matrixVersions(protocol.VersionRange{Min: a.Min, Max: a.Max}) {
					name := fmt.Sprintf("%s v%d", protocol.APIName(a.Key), v)
					req := defaultRequest(int16(a.Key), v)
					answered, raw := matrixRequest(t, addr, int16(a.Key), v, req)
					if !answered {
						t.Errorf("%s: the broker closed the connection instead of answering a default request", name)
						continue
					}
					if problem := readResponse(int16(a.Key), v, raw); problem != "" {
						t.Errorf("%s: %s", name, problem)
					}
				}
			}
		})
	}
}

// With SASL configured, the two SASL APIs are offered and answer too (before
// authentication, which is when they are used).
func TestVersionMatrix_SASLAPIsAnswerWhenConfigured(t *testing.T) {
	addr := startAuthBroker(t, []string{"PLAIN"}, map[string]string{"u": "p"}, "", "")
	profile, _ := protocol.ProfileFor(4)
	var found int
	for _, a := range profile.Advertised(true, true) {
		if a.Key != protocol.APIKey(kmsg.SASLHandshake) && a.Key != protocol.APIKey(kmsg.SASLAuthenticate) {
			continue
		}
		found++
		for _, v := range matrixVersions(protocol.VersionRange{Min: a.Min, Max: a.Max}) {
			req := defaultRequest(int16(a.Key), v)
			c, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if a.Key == protocol.APIKey(kmsg.SASLHandshake) {
				req.(*kmsg.SASLHandshakeRequest).Mechanism = "PLAIN"
			} else {
				// A SaslAuthenticate with no handshake before it closes the connection,
				// as on Kafka (claim: auth A8): it is sent where it belongs.
				hs := kmsg.NewPtrSASLHandshakeRequest()
				hs.SetVersion(1)
				hs.Mechanism = "PLAIN"
				if answered, _ := matrixRequestOn(t, c, int16(kmsg.SASLHandshake), 1, hs); !answered {
					t.Fatalf("SaslHandshake before SASLAuthenticate v%d: closed the connection", v)
				}
			}
			answered, raw := matrixRequestOn(t, c, int16(a.Key), v, req)
			if !answered {
				t.Errorf("%s v%d: closed the connection", protocol.APIName(a.Key), v)
			} else if problem := readResponse(int16(a.Key), v, raw); problem != "" {
				t.Errorf("%s v%d: %s", protocol.APIName(a.Key), v, problem)
			}
		}
	}
	if found != 2 {
		t.Errorf("with SASL configured the profile advertises %d SASL APIs, want 2", found)
	}
}

// ApiVersions says exactly what the table says, for each profile, and a client
// that reads it can then use every API it lists. Anything not listed is refused by
// closing the connection, whatever the request.
func TestVersionMatrix_ApiVersionsIsTheTableAndTheRestIsRefused(t *testing.T) {
	for _, major := range protocol.SupportedKafkaMajors() {
		addr := startBroker(t, major)
		profile, _ := protocol.ProfileFor(major)
		want := map[int16]protocol.VersionRange{}
		for _, a := range profile.Advertised(false, true) {
			want[int16(a.Key)] = protocol.VersionRange{Min: a.Min, Max: a.Max}
		}

		req := defaultRequest(int16(kmsg.ApiVersions), 0)
		answered, raw := matrixRequest(t, addr, int16(kmsg.ApiVersions), 0, req)
		if !answered {
			t.Fatalf("Kafka %d: ApiVersions closed the connection", major)
		}
		resp := kmsg.NewPtrApiVersionsResponse()
		resp.SetVersion(0)
		if err := resp.ReadFrom(raw[4:]); err != nil {
			t.Fatalf("Kafka %d: ApiVersions: %v", major, err)
		}
		got := map[int16]protocol.VersionRange{}
		for _, k := range resp.ApiKeys {
			got[k.ApiKey] = protocol.VersionRange{Min: k.MinVersion, Max: k.MaxVersion}
		}
		if len(got) != len(want) {
			t.Errorf("Kafka %d: ApiVersions lists %d APIs, the table offers %d", major, len(got), len(want))
		}
		for key, r := range want {
			if got[key] != r {
				t.Errorf("Kafka %d: %s is %v in ApiVersions, %v in the table", major, protocol.APIName(protocol.APIKey(key)), got[key], r)
			}
		}

		for key := int16(0); key < 100; key++ {
			if _, offered := want[key]; offered || kmsg.RequestForKey(key) == nil {
				continue
			}
			r := defaultRequest(key, 0)
			if answered, _ := matrixRequest(t, addr, key, 0, r); answered {
				t.Errorf("Kafka %d: %s is not offered and was answered", major, kmsg.NameForKey(key))
			}
		}
	}
}
