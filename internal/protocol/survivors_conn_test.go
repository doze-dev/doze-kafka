package protocol

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestCloseErrorSaysWhy(t *testing.T) {
	if got := (&CloseError{Reason: "a proof of the wrong length"}).Error(); got != "closing the connection: a proof of the wrong length" {
		t.Errorf("Error = %q", got)
	}
}

// The answer to a request that came in the middle of a login is the error and nothing
// else, at the version asked, and none for a version there is no shape for.
func TestIllegalSASLStateAnswers(t *testing.T) {
	for _, key := range []APIKey{kmsg.SASLHandshake, kmsg.ApiVersions} {
		max := kmsg.RequestForKey(int16(key)).MaxVersion()
		for _, v := range []int16{0, max} {
			h := RequestHeader{APIKey: key, APIVersion: v, CorrelationID: 9}
			got := illegalSASLState(h)
			if got == nil {
				t.Errorf("%s v%d: no answer", APIName(key), v)
				continue
			}
			if binary.BigEndian.Uint32(got) != 9 {
				t.Errorf("%s v%d: correlation id % x", APIName(key), v, got[:4])
			}
			resp := kmsg.ResponseForKey(int16(key))
			resp.SetVersion(v)
			body := got[4:]
			if resp.IsFlexible() && key != kmsg.ApiVersions {
				body = body[1:]
			}
			if err := resp.ReadFrom(body); err != nil {
				t.Errorf("%s v%d: %v", APIName(key), v, err)
				continue
			}
			var code int16
			switch r := resp.(type) {
			case *kmsg.SASLHandshakeResponse:
				code = r.ErrorCode
			case *kmsg.ApiVersionsResponse:
				code = r.ErrorCode
				if len(r.ApiKeys) != 0 {
					t.Errorf("ApiVersions v%d lists %d APIs", v, len(r.ApiKeys))
				}
			}
			if code != kerr.IllegalSaslState.Code {
				t.Errorf("%s v%d: error code %d", APIName(key), v, code)
			}
		}
		for _, v := range []int16{-1, max + 1} {
			if got := illegalSASLState(RequestHeader{APIKey: key, APIVersion: v}); got != nil {
				t.Errorf("%s v%d was answered", APIName(key), v)
			}
		}
	}
	if got := illegalSASLState(RequestHeader{APIKey: kmsg.Metadata}); got != nil {
		t.Errorf("Metadata was answered: % x", got)
	}
}

func TestPreAuthVerdicts(t *testing.T) {
	c := pipeConn(t)
	for key, want := range map[APIKey]preAuthVerdict{
		kmsg.ApiVersions:      preAuthServe,
		kmsg.SASLHandshake:    preAuthServe,
		kmsg.SASLAuthenticate: preAuthClose,
		kmsg.Metadata:         preAuthClose,
	} {
		if got := c.preAuth(key); got != want {
			t.Errorf("before a handshake, %s: %v, want %v", APIName(key), got, want)
		}
	}
	c.AnsweredVersions()
	if got := c.preAuth(kmsg.ApiVersions); got != preAuthClose {
		t.Errorf("a second ApiVersions: %v, want close", got)
	}
	c.saslMechanism = "PLAIN"
	for key, want := range map[APIKey]preAuthVerdict{
		kmsg.SASLAuthenticate: preAuthServe,
		kmsg.SASLHandshake:    preAuthRefuse,
		kmsg.ApiVersions:      preAuthRefuse,
		kmsg.Metadata:         preAuthClose,
	} {
		if got := c.preAuth(key); got != want {
			t.Errorf("in a handshake, %s: %v, want %v", APIName(key), got, want)
		}
	}
}

// Over the wire: an ApiVersions in the middle of the login is answered
// ILLEGAL_SASL_STATE, and the connection is then closed.
func TestAnApiVersionsInTheMiddleOfALoginIsAnsweredThenClosed(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	c := newConn(a, NewRouter(), quietLogger(), true)
	c.saslMechanism = "PLAIN"
	obs := &recordingObserver{}
	c.observer = obs
	done := make(chan struct{})
	go func() { c.serve(context.Background()); close(done) }()

	b.SetDeadline(time.Now().Add(5 * time.Second))
	hdr, payload := frameFor(kmsg.ApiVersions, 0, 4)
	go b.Write(append(hdr, payload...))
	resp, err := ReadFrame(b)
	if err != nil {
		t.Fatal(err)
	}
	if want := illegalSASLState(RequestHeader{APIKey: kmsg.ApiVersions, APIVersion: 0, CorrelationID: 4}); string(resp) != string(want) {
		t.Errorf("answer % x, want % x", resp, want)
	}
	if code := int16(binary.BigEndian.Uint16(resp[4:])); code != kerr.IllegalSaslState.Code {
		t.Errorf("error code %d", code)
	}
	if _, err := ReadFrame(b); !errors.Is(err, io.EOF) {
		t.Errorf("after the answer: %v, want the connection closed", err)
	}
	<-done
	_, _, reqs := obs.snapshot()
	if len(reqs) != 1 || reqs[0].Closed == "" || reqs[0].API != kmsg.ApiVersions {
		t.Errorf("observed %+v", reqs)
	}
}

// A request whose header cannot be read is observed as API -1 with the size of its frame
// and its length prefix.
func TestAnUnreadableHeaderIsObservedWithItsSize(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	c := newConn(a, NewRouter(), quietLogger(), false)
	obs := &recordingObserver{}
	c.observer = obs
	done := make(chan struct{})
	go func() { c.serve(context.Background()); close(done) }()
	b.SetDeadline(time.Now().Add(5 * time.Second))
	go WriteFrame(b, []byte{0, 1, 2})
	<-done
	_, _, reqs := obs.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("observed %+v", reqs)
	}
	if reqs[0].API != -1 || reqs[0].ReqBytes != 7 || reqs[0].Closed == "" {
		t.Errorf("API %d, %d bytes, closed %q; want -1, 7 and a reason", reqs[0].API, reqs[0].ReqBytes, reqs[0].Closed)
	}
}

// Adopting a connection that is not served here waits five seconds for the server to
// have accepted it, as the code says, then gives up.
func TestAdoptWaitsFiveSecondsForAnUnknownConnection(t *testing.T) {
	t.Parallel()
	elsewhere, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer elsewhere.Close()
	go func() {
		for {
			c, err := elsewhere.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	away, err := net.Dial("tcp", elsewhere.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer away.Close()
	start := time.Now()
	if err := Adopt(context.Background(), away); !errors.Is(err, ErrNotOurs) {
		t.Fatalf("Adopt = %v", err)
	}
	if took := time.Since(start); took < 4500*time.Millisecond || took > 8*time.Second {
		t.Errorf("Adopt gave up after %v, want about 5s", took)
	}
}

func TestRouteChecksAdvertisedVersions(t *testing.T) {
	handler := func(context.Context, *Request) ([]byte, error) { return []byte{1}, nil }
	route := func(r *Router, key APIKey, v int16) error {
		_, err := r.Route(context.Background(), &Request{Header: RequestHeader{APIKey: key, APIVersion: v}})
		return err
	}

	// Nothing advertised: anything with a handler is routed.
	open := NewRouter()
	open.Register(kmsg.Metadata, handler)
	if err := route(open, kmsg.Metadata, 99); err != nil {
		t.Errorf("with nothing advertised: %v", err)
	}
	if err := route(open, kmsg.Produce, 0); err == nil || errors.Is(err, ErrNotAdvertised) {
		t.Errorf("no handler: %v", err)
	}

	r := NewRouter()
	r.Register(kmsg.Metadata, handler)
	r.Register(kmsg.ApiVersions, handler)
	r.Register(kmsg.Produce, handler)
	r.Advertise(kmsg.Metadata, 1, 3)
	r.Advertise(kmsg.ApiVersions, 0, 3)
	for v, ok := range map[int16]bool{0: false, 1: true, 2: true, 3: true, 4: false} {
		if err := route(r, kmsg.Metadata, v); ok != (err == nil) || (!ok && !errors.Is(err, ErrNotAdvertised)) {
			t.Errorf("Metadata v%d: %v, allowed %v", v, err, ok)
		}
	}
	if err := route(r, kmsg.ApiVersions, 40); err != nil {
		t.Errorf("ApiVersions at a version never heard of: %v", err)
	}
	if err := route(r, kmsg.Produce, 3); !errors.Is(err, ErrNotAdvertised) {
		t.Errorf("a key not advertised: %v", err)
	}
}

func TestAdvertisedFollowsSASLAndTelemetry(t *testing.T) {
	p, ok := ProfileFor(lastMajor)
	if !ok {
		t.Fatal("no profile")
	}
	has := func(list []AdvertisedAPI, key APIKey) bool {
		return slices.ContainsFunc(list, func(a AdvertisedAPI) bool { return a.Key == key })
	}
	for _, c := range []struct{ sasl, telemetry bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		list := p.Advertised(c.sasl, c.telemetry)
		if has(list, kmsg.SASLHandshake) != c.sasl || has(list, kmsg.SASLAuthenticate) != c.sasl {
			t.Errorf("sasl %v telemetry %v: SASL APIs listed %v", c.sasl, c.telemetry, has(list, kmsg.SASLHandshake))
		}
		if has(list, kmsg.GetTelemetrySubscriptions) != c.telemetry || has(list, kmsg.PushTelemetry) != c.telemetry {
			t.Errorf("sasl %v telemetry %v: telemetry APIs listed %v", c.sasl, c.telemetry, has(list, kmsg.PushTelemetry))
		}
		if !has(list, kmsg.Metadata) || !has(list, kmsg.ApiVersions) {
			t.Errorf("sasl %v telemetry %v: the plain APIs are missing", c.sasl, c.telemetry)
		}
		for i := 1; i < len(list); i++ {
			if list[i-1].Key >= list[i].Key {
				t.Fatalf("not in ascending key order at %d: %d then %d", i, list[i-1].Key, list[i].Key)
			}
		}
	}
}

func TestSupportedKafkaMajorsAreEveryMajorInTheTable(t *testing.T) {
	got := SupportedKafkaMajors()
	want := []int{}
	for m := firstMajor; m <= lastMajor; m++ {
		want = append(want, m)
	}
	if !slices.Equal(got, want) || len(got) != 4 || got[0] != 1 || got[3] != 4 {
		t.Errorf("majors = %v, want %v", got, want)
	}
}
