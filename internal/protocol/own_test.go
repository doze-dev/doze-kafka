package protocol

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// ownServer is a server that answers Heartbeat, with an observer.
func ownServer(t *testing.T) (addr string, obs *recordingObserver) {
	t.Helper()
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
	obs = &recordingObserver{}
	srv := NewServer("127.0.0.1:0", router, quietLogger())
	srv.SetObserver(obs)
	bound, err := srv.Bind(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return bound.String(), obs
}

// beat sends one Heartbeat under a client id and reads the answer.
func beat(t *testing.T, conn net.Conn, clientID string) {
	t.Helper()
	hb := kmsg.NewPtrHeartbeatRequest()
	hb.SetVersion(1)
	hb.Group = "g"
	payload := binary.BigEndian.AppendUint16(nil, uint16(kmsg.Heartbeat))
	payload = binary.BigEndian.AppendUint16(payload, 1)
	payload = binary.BigEndian.AppendUint32(payload, 1)
	payload = binary.BigEndian.AppendUint16(payload, uint16(len(clientID)))
	payload = append(append(payload, clientID...), hb.AppendTo(nil)...)
	if err := WriteFrame(conn, payload); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := ReadFrame(conn); err != nil {
		t.Fatal(err)
	}
}

func observed(t *testing.T, obs *recordingObserver, n int) []Observation {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if _, _, reqs := obs.snapshot(); len(reqs) >= n {
			return reqs
		}
	}
	t.Fatalf("the observer did not hear %d requests", n)
	return nil
}

// A connection this process opened to its own server is marked as its own by saying
// so in memory about that connection, and an observer is told. What a client calls
// itself has nothing to do with it: the same client id from a connection that was not
// adopted is anyone's, and an adopted connection is the process's own under any name.
func TestOnlyAnAdoptedConnectionIsTheProcessesOwn(t *testing.T) {
	addr, obs := ownServer(t)

	own, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer own.Close()
	if err := Adopt(context.Background(), own); err != nil {
		t.Fatalf("adopting a connection to this process's server: %v", err)
	}
	beat(t, own, "anything-at-all")

	foreign, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	beat(t, foreign, "doze-kafka-console") // the name the console uses: it is only a name.
	beat(t, own, "doze-kafka-console")

	reqs := observed(t, obs, 3)
	for i, want := range []bool{true, false, true} {
		if reqs[i].Own != want {
			t.Errorf("request %d (client id %q from %s): own = %v, want %v", i, reqs[i].ClientID, reqs[i].Remote, reqs[i].Own, want)
		}
	}
	// The process's own requests are told and not described; anyone else's are.
	if reqs[0].Req != nil || reqs[0].Group != "" || reqs[1].Req == nil || reqs[1].Group != "g" {
		t.Errorf("described: own %+v, foreign %+v", reqs[0].Summary, reqs[1].Summary)
	}
}

// A connection to somewhere else cannot be adopted, and a closed one leaves nothing
// behind for the next connection from the same address to inherit.
func TestAdoptIsOnlyForConnectionsServedHereAndEndsWithThem(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := Adopt(ctx, away); !errors.Is(err, ErrNotOurs) {
		t.Errorf("adopting a connection to another listener: %v, want ErrNotOurs", err)
	}

	addr, obs := ownServer(t)
	own, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if err := Adopt(context.Background(), own); err != nil {
		t.Fatal(err)
	}
	beat(t, own, "console")
	local := own.LocalAddr().(*net.TCPAddr)
	own.(*net.TCPConn).SetLinger(0) // closed at once, so its address is free to be used again below.
	own.Close()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		if _, closed, _ := obs.snapshot(); len(closed) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the server did not close the connection")
		}
	}
	accepted.Lock()
	left := len(accepted.byEnds)
	accepted.Unlock()
	if left != 0 {
		t.Errorf("%d connections are still on the list after theirs closed", left)
	}

	// Someone else, from the very address the process's own connection had.
	d := net.Dialer{LocalAddr: local, Timeout: 2 * time.Second}
	again, err := d.Dial("tcp", addr)
	if err != nil {
		t.Logf("the address could not be used again at once (%v): the list being empty is what holds", err)
		return
	}
	defer again.Close()
	beat(t, again, "console")
	if reqs := observed(t, obs, 2); reqs[1].Own {
		t.Error("a new connection from the address an adopted one had is taken for the process's own")
	}
}
