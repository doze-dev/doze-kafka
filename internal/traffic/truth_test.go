package traffic

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
)

// What the Traffic page counts must be what happened. Each test here failed on the code
// before its fix (the fix reverted by hand fails it again).

func denied(o *protocol.Observation) {
	o.Group, o.ErrorCode, o.Class = "g-loyalty-etl", kerr.GroupAuthorizationFailed.Code, protocol.ClassDenied
}

// A2: a client that alternates a denied FindCoordinator on one connection and an ok
// Metadata on another was "refused 6 times in a row": the streak was one connection's,
// and the page took the largest of its connections' as the client's. A client's streak
// is its own requests in the order it sent them, whichever connection carried them.
func TestAClientsStreakIsItsRequestsInOrder(t *testing.T) {
	r := New(100, nil)
	r.ConnOpened(1, "a")
	r.ConnOpened(2, "b")
	for i := range 6 {
		at := time.Duration(i) * time.Second
		r.Request(obs(1, "loyalty-etl", kmsg.FindCoordinator, at, denied))
		r.Request(obs(2, "loyalty-etl", kmsg.Metadata, at+time.Millisecond))
	}
	for _, c := range r.Clients() {
		if c.ClientStreak != 0 {
			t.Errorf("connection %d: the client's streak is %d, and its newest request went well", c.Conn, c.ClientStreak)
		}
	}
	for i := range 3 {
		r.Request(obs(1, "loyalty-etl", kmsg.FindCoordinator, 10*time.Second+time.Duration(i)*time.Second, denied))
	}
	for _, c := range r.Clients() {
		if c.ClientStreak != 3 || c.ClientLastError != "GROUP_AUTHORIZATION_FAILED" {
			t.Errorf("connection %d: streak %d (%s), want the client's last 3, all denied", c.Conn, c.ClientStreak, c.ClientLastError)
		}
	}
	// The streak goes with the client's last connection.
	r.ConnClosed(1)
	r.ConnClosed(2)
	r.ConnOpened(3, "c")
	r.Request(obs(3, "loyalty-etl", kmsg.Metadata, 20*time.Second))
	if c := r.Clients(); len(c) != 1 || c[0].ClientStreak != 0 {
		t.Errorf("a new connection of the client after an ok request: %+v", c)
	}
}

func beat(gen string) func(*protocol.Observation) {
	return func(o *protocol.Observation) { o.Group, o.Idle, o.Detail = "g", true, gen }
}

// A4: a folded heartbeat said "169 identical heartbeats ... each one asked the same thing
// and got the same answer" across six rebalances: the generation was not part of what
// folds, and after a refusal of the same kind the fold carried on in the old row. Any
// change breaks the fold, as the page says.
func TestAFoldBreaksWhenAnythingChanges(t *testing.T) {
	r := New(100, nil)
	r.ConnOpened(1, "a")
	for i := range 3 {
		r.Request(obs(1, "app", kmsg.Heartbeat, time.Duration(i)*time.Second, beat("generation 1")))
	}
	r.Request(obs(1, "app", kmsg.Heartbeat, 3*time.Second, beat("generation 2")))
	rows := r.Events(Filter{API: "heartbeat"}, 0)
	if len(rows) != 2 || rows[0].Count != 1 || rows[1].Count != 3 {
		t.Fatalf("a new generation folded into the old: %d rows %+v", len(rows), rows)
	}
	// Same generation, a different answer, then the same request again: the third is a
	// row of its own, not the first row going on.
	r.Request(obs(1, "app", kmsg.Heartbeat, 4*time.Second, beat("generation 2"), func(o *protocol.Observation) {
		o.Idle, o.ErrorCode, o.Class = false, kerr.RebalanceInProgress.Code, protocol.ClassExpected
	}))
	r.Request(obs(1, "app", kmsg.Heartbeat, 5*time.Second, beat("generation 2")))
	rows = r.Events(Filter{API: "heartbeat"}, 0)
	if len(rows) != 4 {
		t.Fatalf("%d rows, want 4: gen 1 ×3, gen 2, the refusal, gen 2 again", len(rows))
	}
	if rows[0].Count != 1 || rows[2].Count != 1 {
		t.Errorf("the fold resumed across a different answer: %+v", rows)
	}
	// A request of another kind on the connection does not break it.
	r.Request(obs(1, "app", kmsg.Produce, 6*time.Second))
	r.Request(obs(1, "app", kmsg.Heartbeat, 7*time.Second, beat("generation 2")))
	if hb := r.Events(Filter{API: "heartbeat"}, 0); len(hb) != 4 || hb[0].Count != 2 {
		t.Errorf("a produce between two beats broke the fold: %+v", hb)
	}
}

// A6: the client link ("its requests") was a substring match: client=checkout listed
// checkout-api's requests, and client=kgo every franz-go client. The filter is the
// client: its id, or for a connection with none, its address.
func TestTheClientFilterIsOneClient(t *testing.T) {
	r := New(100, nil)
	r.ConnOpened(1, "a")
	r.ConnOpened(2, "b")
	r.ConnOpened(3, "127.0.0.1:55190")
	r.Request(obs(1, "checkout", kmsg.Metadata, 0))
	r.Request(obs(2, "checkout-api", kmsg.Metadata, time.Second, func(o *protocol.Observation) { o.Software = "kgo v1.22.1" }))
	r.Request(obs(3, "", kmsg.Metadata, 2*time.Second, func(o *protocol.Observation) { o.Remote = "127.0.0.1:55190" }))
	if got := r.Events(Filter{Client: "checkout"}, 0); len(got) != 1 || got[0].ClientID != "checkout" {
		t.Errorf("client=checkout: %d rows %+v", len(got), got)
	}
	if got := r.Events(Filter{Client: "kgo"}, 0); len(got) != 0 {
		t.Errorf("client=kgo matched by software: %+v", got)
	}
	if got := r.Events(Filter{Client: "127.0.0.1:55190"}, 0); len(got) != 1 || got[0].ClientID != "" {
		t.Errorf("a connection with no client id is named by its address: %+v", got)
	}
	if got := r.Events(Filter{Client: "CHECKOUT-API"}, 0); len(got) != 0 {
		t.Errorf("a client id is matched exactly, case and all: %+v", got)
	}
}

// A7: "requests in the last minute" summed twelve five-second buckets, the newest of
// them the one still filling: 55 to 60 seconds, never a minute. The minute is the
// twelve whole buckets before the one that is filling, and the window says which.
func TestTheLastMinuteIsAWholeMinute(t *testing.T) {
	r := New(1000, nil)
	now := t0.Add(10*time.Minute + 2*time.Second) // 2 s into a bucket.
	r.now = func() time.Time { return now }
	r.ConnOpened(1, "a")
	from := t0.Add(9 * time.Minute) // floor(now, 5 s) - 60 s
	for _, at := range []time.Time{
		from.Add(-time.Millisecond),  // before the minute
		from,                         // its first instant
		from.Add(30 * time.Second),   //
		from.Add(60*time.Second - 1), // its last instant
		from.Add(60 * time.Second),   // the bucket still filling: not yet counted
		now,
	} {
		r.Request(obs(1, "app", kmsg.Metadata, at.Sub(t0)))
	}
	w := r.LastMinute()
	if w.Requests != 3 {
		t.Errorf("%d requests in the minute, want 3", w.Requests)
	}
	if !w.From.Equal(from) || !w.To.Equal(from.Add(time.Minute)) {
		t.Errorf("the minute is %v to %v, want %v to %v", w.From, w.To, from, from.Add(time.Minute))
	}
	for _, c := range r.Clients() {
		if c.Req1m != 3 {
			t.Errorf("a client's minute is %d requests, want 3", c.Req1m)
		}
	}
}

// A client's requests a minute were the sum over its open connections: a client that
// reconnected (a group member whose fetch connection was closed at a rebalance) lost the
// requests of the closed one, and the clients view said 116 a minute for a client that
// had sent 178 (the design gate's ledger). A client's counts are its own, whichever of
// its connections carried them, for as long as it has one open.
func TestAClientsCountsOutliveItsConnections(t *testing.T) {
	r := New(100, nil)
	now := t0.Add(2 * time.Minute)
	r.now = func() time.Time { return now }
	r.ConnOpened(1, "a")
	r.ConnOpened(2, "b")
	for i := range 3 {
		r.Request(obs(1, "depot-router", kmsg.Fetch, 70*time.Second+time.Duration(i)*time.Second))
	}
	r.Request(obs(2, "depot-router", kmsg.Heartbeat, 80*time.Second))
	r.ConnClosed(1)
	r.ConnOpened(3, "c")
	r.Request(obs(3, "depot-router", kmsg.Fetch, 90*time.Second, func(o *protocol.Observation) { o.ErrorCode, o.Class = 1, protocol.ClassRefused }))
	cs := r.Clients()
	if len(cs) != 2 {
		t.Fatalf("%d connections open, want 2", len(cs))
	}
	for _, c := range cs {
		if c.ClientReq1m != 5 || c.ClientReq5m != 5 || c.ClientErr5m != 1 {
			t.Errorf("connection %d: the client's minute %d, five minutes %d with %d errors; want 5, 5 and 1", c.Conn, c.ClientReq1m, c.ClientReq5m, c.ClientErr5m)
		}
	}
	r.ConnClosed(2)
	r.ConnClosed(3)
	if len(r.streaks) != 0 {
		t.Errorf("a client with no connection open is still counted: %d", len(r.streaks))
	}
}
