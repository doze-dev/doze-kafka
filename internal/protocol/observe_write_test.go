package protocol

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// A client that goes away before its answer is written leaves a row that says so: the
// request was served, the answer could not be sent, and the connection is over. It
// holds for an answer small enough to sit in the write buffer until it is flushed, and
// for one large enough to go straight to the socket.
func TestAnAnswerThatCannotBeWrittenIsObserved(t *testing.T) {
	for name, size := range map[string]int{"a small answer": 16, "an answer larger than the write buffer": 256 << 10} {
		t.Run(name, func(t *testing.T) {
			gone := make(chan struct{})
			router := NewRouter()
			router.Register(kmsg.Heartbeat, func(_ context.Context, req *Request) ([]byte, error) {
				<-gone // answer only once the client has hung up.
				time.Sleep(100 * time.Millisecond)
				return make([]byte, size), nil
			})
			conn, obs := serveObserved(t, router)
			sendRaw(t, conn, kmsg.Heartbeat, 1, 77, "impatient", nil)
			conn.(*net.TCPConn).SetLinger(0) // a reset, so the broker's write fails at once.
			conn.Close()
			close(gone)

			got := waitObservations(t, obs, 1)[0]
			if got.Class != ClassFailed || !strings.Contains(got.Closed, "could not be written") {
				t.Fatalf("observed %+v, want a failed row that says the answer could not be written", got)
			}
			if got.ClientID != "impatient" || got.Correlation != 77 || got.API != kmsg.Heartbeat || got.RespBytes != 0 {
				t.Errorf("who, which and how much: %q %d %v, %d bytes answered", got.ClientID, got.Correlation, got.API, got.RespBytes)
			}
			if !strings.HasPrefix(got.Detail, "closed the connection: ") {
				t.Errorf("detail = %q", got.Detail)
			}
		})
	}
}

// A request is observed with who its connection is at the time: nobody yet (the
// anonymous principal) before it authenticates, and the user from then on. The
// request that authenticates is itself observed as that user's: it is answered after
// the handler has said who.
func TestAnObservationNamesWhoTheConnectionIs(t *testing.T) {
	router := NewRouter()
	router.Register(kmsg.Heartbeat, func(_ context.Context, req *Request) ([]byte, error) {
		return make([]byte, 8), nil
	})
	router.Register(kmsg.SASLAuthenticate, func(_ context.Context, req *Request) ([]byte, error) {
		req.Conn.SetPrincipal("User:alice")
		return make([]byte, 8), nil
	})
	conn, obs := serveObserved(t, router)
	for i, key := range []APIKey{kmsg.Heartbeat, kmsg.SASLAuthenticate, kmsg.Heartbeat} {
		sendRaw(t, conn, key, 1, int32(i), "app", nil)
		if _, err := ReadFrame(conn); err != nil {
			t.Fatal(err)
		}
	}
	got := waitObservations(t, obs, 3)
	for i, want := range []string{AnonymousPrincipal, "User:alice", "User:alice"} {
		if got[i].Principal != want {
			t.Errorf("request %d was observed as %q, want %q", i, got[i].Principal, want)
		}
	}
}
