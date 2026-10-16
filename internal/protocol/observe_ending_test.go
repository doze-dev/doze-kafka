package protocol

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

func serveObserved(t *testing.T, router *Router) (net.Conn, *recordingObserver) {
	t.Helper()
	obs := &recordingObserver{}
	srv := NewServer("127.0.0.1:0", router, quietLogger())
	srv.SetObserver(obs)
	addr, err := srv.Bind(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, obs
}

func sendRaw(t *testing.T, conn net.Conn, key APIKey, version int16, corr int32, clientID string, body []byte) {
	t.Helper()
	payload := binary.BigEndian.AppendUint16(nil, uint16(key))
	payload = binary.BigEndian.AppendUint16(payload, uint16(version))
	payload = binary.BigEndian.AppendUint32(payload, uint32(corr))
	payload = binary.BigEndian.AppendUint16(payload, uint16(len(clientID)))
	payload = append(payload, clientID...)
	payload = append(payload, body...)
	if err := WriteFrame(conn, payload); err != nil {
		t.Fatal(err)
	}
}

func waitObservations(t *testing.T, obs *recordingObserver, n int) []Observation {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, reqs := obs.snapshot(); len(reqs) >= n {
			return reqs
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, _, reqs := obs.snapshot()
	t.Fatalf("observed %d requests, want %d", len(reqs), n)
	return nil
}

// "Why does my app keep disconnecting?" is the question a request that ends its
// connection raises, and it used to leave no row at all: the loop returned before
// the observer was told (TRF-11).
func TestConnectionEndingRequestsAreObserved(t *testing.T) {
	t.Run("an API the broker does not serve", func(t *testing.T) {
		conn, obs := serveObserved(t, NewRouter())
		sendRaw(t, conn, 99, 0, 41, "stranger", nil)
		got := waitObservations(t, obs, 1)[0]
		// Key 99 is in no version of Kafka's specification: the client asked something
		// no broker answers, and the broker did not fail (A9: the row was red).
		if got.Class != ClassRefused || got.Closed == "" || !strings.Contains(got.Closed, "99") {
			t.Errorf("observed %+v, want a refused row that says the connection was closed because key 99 is not served", got)
		}
		if got.ClientID != "stranger" || got.Correlation != 41 {
			t.Errorf("who and which: %q %d", got.ClientID, got.Correlation)
		}
		if _, closed, _ := obs.snapshot(); len(closed) == 0 {
			time.Sleep(50 * time.Millisecond)
			if _, closed, _ = obs.snapshot(); len(closed) != 1 {
				t.Error("ConnClosed did not follow")
			}
		}
	})
	t.Run("a handler that panics", func(t *testing.T) {
		router := NewRouter()
		router.Register(kmsg.Heartbeat, func(context.Context, *Request) ([]byte, error) { panic("a bug") })
		conn, obs := serveObserved(t, router)
		sendRaw(t, conn, kmsg.Heartbeat, 1, 5, "app", nil)
		got := waitObservations(t, obs, 1)[0]
		if got.Class != ClassFailed || !strings.Contains(got.Closed, "panic") {
			t.Errorf("observed %+v", got)
		}
	})
	t.Run("a header that cannot be read", func(t *testing.T) {
		conn, obs := serveObserved(t, NewRouter())
		if err := WriteFrame(conn, []byte{0, 18}); err != nil { // a key and nothing else.
			t.Fatal(err)
		}
		got := waitObservations(t, obs, 1)[0]
		if got.Class != ClassRefused || got.Closed == "" {
			t.Errorf("observed %+v, want refused: the bytes were the client's", got)
		}
	})
	// A consumer that closes its connection under a long-poll fetch (franz-go does, at a
	// KIP-848 rebalance) leaves the broker an answer it cannot write. The broker did not
	// fail: the row was red, and counted among the minute's errors (the design gate's
	// ledger found the one error no client was answered with).
	t.Run("a client that hangs up before its answer", func(t *testing.T) {
		router := NewRouter()
		gone := make(chan struct{})
		router.Register(kmsg.Heartbeat, func(_ context.Context, req *Request) ([]byte, error) {
			<-gone
			time.Sleep(200 * time.Millisecond) // the reset has arrived.
			q, err := req.Decode()
			if err != nil {
				return nil, err
			}
			resp := q.ResponseKind().(*kmsg.HeartbeatResponse)
			resp.SetVersion(req.Header.APIVersion)
			return req.Reply(resp), nil
		})
		conn, obs := serveObserved(t, router)
		hb := kmsg.NewPtrHeartbeatRequest()
		hb.SetVersion(1)
		sendRaw(t, conn, kmsg.Heartbeat, 1, 9, "leaver", hb.AppendTo(nil))
		time.Sleep(50 * time.Millisecond)
		_ = conn.(*net.TCPConn).SetLinger(0)
		conn.Close()
		close(gone)
		got := waitObservations(t, obs, 1)[0]
		if got.Class != ClassExpected || !strings.Contains(got.Closed, "the client closed the connection before the answer") {
			t.Errorf("observed %+v, want an expected row: the client left before the answer", got)
		}
	})
	t.Run("a request before authentication", func(t *testing.T) {
		router := NewRouter()
		srv := NewServer("127.0.0.1:0", router, quietLogger())
		obs := &recordingObserver{}
		srv.SetObserver(obs)
		srv.RequireAuth() // only the gate matters here.
		addr, err := srv.Bind(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = srv.Serve(ctx); close(done) }()
		defer func() { cancel(); <-done }()
		conn, err := net.Dial("tcp", addr.String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		sendRaw(t, conn, kmsg.Produce, 7, 3, "anon", nil)
		got := waitObservations(t, obs, 1)[0]
		if got.Class != ClassDenied || !strings.Contains(got.Closed, "authenticat") {
			t.Errorf("observed %+v, want a denied row that says the request came before authentication", got)
		}
	})
}

// A request's correlation id is what a client's own log carries, so it is how a person
// finds the request there.
func TestCorrelationIdIsObserved(t *testing.T) {
	router := NewRouter()
	router.Register(kmsg.Heartbeat, func(_ context.Context, req *Request) ([]byte, error) {
		q, err := req.Decode()
		if err != nil {
			return nil, err
		}
		resp := q.ResponseKind().(*kmsg.HeartbeatResponse)
		resp.SetVersion(req.Header.APIVersion)
		return req.Reply(resp), nil
	})
	conn, obs := serveObserved(t, router)
	hb := kmsg.NewPtrHeartbeatRequest()
	hb.SetVersion(1)
	hb.Group = "g"
	sendRaw(t, conn, kmsg.Heartbeat, 1, 8114, "app", hb.AppendTo(nil))
	if got := waitObservations(t, obs, 1)[0]; got.Correlation != 8114 || got.Closed != "" {
		t.Errorf("correlation %d, closed %q", got.Correlation, got.Closed)
	}
}
