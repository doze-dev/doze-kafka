package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime"
	"testing"
	"time"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// frameFor builds a length-prefixed request: key, version, correlation id, and a
// null client id — enough for ParseRequestHeader to accept it.
func frameFor(key APIKey, version int16, corr int32, body ...byte) (header, payload []byte) {
	payload = binary.BigEndian.AppendUint16(nil, uint16(key))
	payload = binary.BigEndian.AppendUint16(payload, uint16(version))
	payload = binary.BigEndian.AppendUint32(payload, uint32(corr))
	payload = append(payload, 0xff, 0xff) // client id: null.
	payload = append(payload, body...)
	return binary.BigEndian.AppendUint32(nil, uint32(len(payload))), payload
}

func TestReadFrameDoesNotAllocateWhatThePrefixClaims(t *testing.T) {
	// 100 MiB claimed, 10 bytes sent.
	in := append(binary.BigEndian.AppendUint32(nil, 100<<20), make([]byte, 10)...)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := ReadFrame(bytes.NewReader(in))
	runtime.ReadMemStats(&after)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want unexpected EOF", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 4<<20 {
		t.Fatalf("a frame that claimed 100 MiB and sent 10 bytes allocated %d bytes", grew)
	}
}

func TestReadFrameLimitRefusesBeforeReadingTheBody(t *testing.T) {
	in := append(binary.BigEndian.AppendUint32(nil, 2<<20), make([]byte, 64)...)
	r := bytes.NewReader(in)
	if _, err := ReadFrameLimit(r, maxPreAuthFrameBytes); err == nil {
		t.Fatal("a 2 MiB frame under a 1 MiB limit should be refused")
	}
	if left := r.Len(); left != 64 {
		t.Fatalf("the body was consumed (%d bytes left of 64): refuse first, read never", left)
	}
}

func TestReadFrameLargeRoundTrip(t *testing.T) {
	for _, size := range []int{1, frameChunk, frameChunk + 1, 3*frameChunk + 17, 2 << 20} {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i * 7)
		}
		var wire bytes.Buffer
		if err := WriteFrame(&wire, payload); err != nil {
			t.Fatal(err)
		}
		got, err := ReadFrame(&wire)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("size %d: round trip failed (%v)", size, err)
		}
	}
}

// listen starts a server on a free loopback port and returns its address.
func listen(t *testing.T, router *Router, auth bool, tune func(*Server)) string {
	t.Helper()
	srv := NewServer("127.0.0.1:0", router, quietLogger())
	if auth {
		srv.RequireAuth()
	}
	if tune != nil {
		tune(srv)
	}
	ctx, cancel := context.WithCancel(context.Background())
	addr, err := srv.Bind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { srv.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return addr.String()
}

func echoRouter() *Router {
	r := NewRouter(quietLogger())
	r.Register(APIApiVersions, func(_ context.Context, req *Request) ([]byte, error) {
		return binary.BigEndian.AppendUint32(nil, uint32(req.Header.CorrelationID)), nil
	})
	return r
}

func roundTrip(t *testing.T, addr string, corr int32) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	hdr, payload := frameFor(APIApiVersions, 0, corr)
	c.Write(append(hdr, payload...))
	resp, err := ReadFrame(c)
	if err != nil || len(resp) != 4 || int32(binary.BigEndian.Uint32(resp)) != corr {
		t.Fatalf("round trip on %s: %v %v", addr, resp, err)
	}
}

// expectClosed requires the server to drop the connection promptly.
func expectClosed(t *testing.T, c net.Conn, within time.Duration) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(within))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the connection to be closed, but data arrived")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("connection still open after %v", within)
	}
}

func TestPreAuthConnectionCannotAskForALargeFrame(t *testing.T) {
	addr := listen(t, echoRouter(), true, nil)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Claim 50 MiB and send nothing more. Before authenticating the cap is 1 MiB.
	c.Write(binary.BigEndian.AppendUint32(nil, 50<<20))
	expectClosed(t, c, 2*time.Second)
}

func TestPreAuthIdleConnectionIsDropped(t *testing.T) {
	addr := listen(t, echoRouter(), true, func(s *Server) { s.preAuthTimeout = 150 * time.Millisecond })
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	expectClosed(t, c, 2*time.Second) // connected, said nothing.
}

func TestHandlerPanicCostsTheConnectionNotTheBroker(t *testing.T) {
	r := echoRouter()
	r.Register(APIMetadata, func(context.Context, *Request) ([]byte, error) { panic("handler bug") })
	addr := listen(t, r, false, nil)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	hdr, payload := frameFor(APIMetadata, 0, 7)
	c.Write(append(hdr, payload...))
	expectClosed(t, c, 2*time.Second) // that connection is gone...

	roundTrip(t, addr, 8) // ...and the broker is still serving everyone else.
}

// flakyListener fails the first n Accepts the way a process out of file
// descriptors does, then behaves.
type flakyListener struct {
	net.Listener
	failures int
}

func (f *flakyListener) Accept() (net.Conn, error) {
	if f.failures > 0 {
		f.failures--
		return nil, errors.New("accept tcp 127.0.0.1:9092: accept: too many open files")
	}
	return f.Listener.Accept()
}

func TestAcceptErrorsDoNotEndTheBroker(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer("", echoRouter(), quietLogger())
	srv.UseListener(&flakyListener{Listener: ln, failures: 3})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	roundTrip(t, ln.Addr().String(), 1) // survives three failed accepts.

	select {
	case err := <-done:
		t.Fatalf("Serve returned after an accept error: %v", err)
	default:
	}
}
