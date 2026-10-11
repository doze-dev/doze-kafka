package traffic

import (
	"fmt"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

func idle(group string) func(*protocol.Observation) {
	return func(o *protocol.Observation) { o.Group, o.Idle = group, true }
}

// A row folds in repeats and so changes its latest activity, but it is the same row, and
// the inspector, the address and Back all name it. Its id was built from the sequence
// number, which moved at every beat (TRF-12).
func TestAFoldedRowKeepsItsId(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	r.Request(obs(1, "app", kmsg.Heartbeat, 0, idle("g")))
	first := r.Events(Filter{}, 0)[0]
	for i := 1; i <= 5; i++ {
		r.Request(obs(1, "app", kmsg.Heartbeat, time.Duration(i)*3*time.Second, idle("g")))
	}
	got := r.Events(Filter{}, 0)
	if len(got) != 1 || got[0].ID != first.ID || got[0].Count != 6 {
		t.Fatalf("rows = %+v, want one row with the first id %s and a count of 6", got, first.ID)
	}
	if got[0].Seq <= first.Seq {
		t.Errorf("the ordering key did not move: %d then %d", first.Seq, got[0].Seq)
	}
	if e, ok := r.Row(first.ID); !ok || e.Count != 6 {
		t.Errorf("the row is not found by its id: %+v %v", e, ok)
	}
	if e, ok := r.RowBySeq(got[0].Seq); !ok || e.ID != first.ID {
		t.Errorf("a sequence number does not find the row: %+v %v", e, ok)
	}
	if _, ok := r.Row("nope"); ok {
		t.Error("an unknown id found a row")
	}
}

// A folded row used to live and die with its ring slot, so after the ring went round
// the count started again at 1: "412 requests in the last minute" could not be added
// up from rows, and a long-lived heartbeat stream lost its history (TRF-13).
func TestBusyTrafficDoesNotResetAFoldedCount(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	r.Request(obs(1, "app", kmsg.Heartbeat, 0, idle("g")))
	for i := 1; i <= 3000; i++ {
		at := time.Duration(i) * 10 * time.Millisecond
		r.Request(obs(1, "app", kmsg.Produce, at, func(o *protocol.Observation) { o.Topics = []string{fmt.Sprintf("t%d", i)} }))
		if i%10 == 0 {
			r.Request(obs(1, "app", kmsg.Heartbeat, at, idle("g")))
		}
	}
	hb := r.Events(Filter{API: "heartbeat"}, 0)
	if len(hb) != 1 || hb[0].Count != 301 {
		t.Fatalf("heartbeat rows = %+v, want one row with a count of 301 (the beat is active, so it is not the row evicted)", hb)
	}
	if n := len(r.Events(Filter{}, 0)); n != 50 {
		t.Errorf("%d rows, want the capacity, 50", n)
	}
}

// The least recently active row is evicted, not the oldest created (TRF-13).
func TestEvictionIsByLastActivity(t *testing.T) {
	r := New(3, nil)
	r.ConnOpened(1, "a")
	r.Request(obs(1, "app", kmsg.Heartbeat, 0, idle("g")))
	r.Request(obs(1, "app", kmsg.Produce, time.Second, func(o *protocol.Observation) { o.Topics = []string{"a"} }))
	r.Request(obs(1, "app", kmsg.Produce, 2*time.Second, func(o *protocol.Observation) { o.Topics = []string{"b"} }))
	r.Request(obs(1, "app", kmsg.Heartbeat, 3*time.Second, idle("g")))                                           // the beat is active again.
	r.Request(obs(1, "app", kmsg.Produce, 4*time.Second, func(o *protocol.Observation) { o.Topics = []string{"c"} })) // evicts "a".
	got := map[string]bool{}
	for _, e := range r.Events(Filter{}, 0) {
		got[e.API+fmt.Sprint(e.Topics)] = true
	}
	if !got["Heartbeat[]"] || got["Produce[a]"] || !got["Produce[b]"] || !got["Produce[c]"] {
		t.Errorf("kept %v, want the heartbeat, b and c", got)
	}
}

// A transactional id is not a consumer group: the recorder listed it as one, and the
// page linked it to a group page that does not exist (TRF-10).
func TestTransactionalIdIsNotAGroup(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	r.Request(obs(1, "app", kmsg.Produce, 0, func(o *protocol.Observation) {
		o.Topics, o.TxnID, o.Kind = []string{"orders"}, "checkout-1", "produce"
	}))
	cs := r.Clients()
	if len(cs) != 1 || len(cs[0].Groups) != 0 || cs[0].TxnID != "checkout-1" {
		t.Errorf("client = %+v, want no groups and the transactional id", cs)
	}
	e := r.Events(Filter{}, 0)[0]
	if e.Group != "" || e.TxnID != "checkout-1" {
		t.Errorf("row group %q txn %q", e.Group, e.TxnID)
	}
	if got := r.Events(Filter{Group: "checkout-1"}, 0); len(got) != 0 {
		t.Errorf("the group filter found a transaction: %+v", got)
	}
}

// A Fetch carries no group on the wire; the connection that has joined one does. A
// group's page must show its members' fetches, or its story has a hole where the data
// moved (TRF-18).
func TestGroupFilterIncludesItsMembersFetches(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	r.ConnOpened(2, "b")
	r.Request(obs(1, "app", kmsg.JoinGroup, 0, func(o *protocol.Observation) { o.Group, o.Kind = "g-orders", "groups" }))
	r.Request(obs(1, "app", kmsg.Fetch, time.Second, func(o *protocol.Observation) { o.Topics, o.Kind = []string{"orders"}, "fetch" }))
	r.Request(obs(2, "other", kmsg.Fetch, 2*time.Second, func(o *protocol.Observation) { o.Topics, o.Kind = []string{"orders"}, "fetch" }))
	got := r.Events(Filter{Group: "g-orders"}, 0)
	if len(got) != 2 || got[0].API != "Fetch" || got[0].ViaGroup != "g-orders" || got[0].Group != "" || got[1].API != "JoinGroup" {
		t.Fatalf("rows = %+v, want the join and the member's fetch, not the other connection's", got)
	}
}

func TestOnlyTheConsolesOwnIdsAreLeftOut(t *testing.T) {
	r := New(50, nil)
	for i, id := range append([]string{"doze-kafka-console-demo", "doze-kafka-console2"}, ConsoleClientIDs...) {
		r.ConnOpened(uint64(i), "x")
		r.Request(obs(uint64(i), id, kmsg.Metadata, time.Duration(i)*time.Second))
	}
	var left []string
	for _, e := range r.Events(Filter{}, 0) {
		left = append(left, e.ClientID)
	}
	if len(left) != 2 {
		t.Errorf("rows from %v, want the two apps whose ids only begin the same way", left)
	}
	if len(r.Clients()) != 2 {
		t.Errorf("%d clients, want 2", len(r.Clients()))
	}
}

// What a paused page says it missed is counted from a time, not a position: rows move
// when they fold.
func TestNewSinceCountsRequestsAfterACursor(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	for i := 0; i < 5; i++ {
		r.Request(obs(1, "app", kmsg.Produce, time.Duration(i)*time.Second, func(o *protocol.Observation) { o.Topics = []string{"a"} }))
	}
	cursor := r.Query(Query{}).Newest
	if !cursor.Equal(t0.Add(4 * time.Second)) {
		t.Fatalf("newest = %v", cursor)
	}
	r.Request(obs(1, "app", kmsg.Produce, 6*time.Second, func(o *protocol.Observation) { o.Topics = []string{"a"} }))
	for i := 7; i < 10; i++ {
		r.Request(obs(1, "app", kmsg.Heartbeat, time.Duration(i)*time.Second, idle("g")))
	}
	n, atLeast := r.NewSince(Filter{}, cursor)
	if n != 4 || atLeast {
		t.Errorf("new = %d (at least: %v), want 4: one produce and three beats", n, atLeast)
	}
	if n, _ := r.NewSince(Filter{API: "produce"}, cursor); n != 1 {
		t.Errorf("new produces = %d, want 1", n)
	}
	if n, _ := r.NewSince(Filter{}, t0.Add(time.Hour)); n != 0 {
		t.Errorf("new after the future = %d", n)
	}
}

// Each fold keeps the latest 64 ticks, whatever the count, and says when the ticks do not
// reach the cursor.
func TestTickRingKeepsLast64(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	for i := 0; i < 200; i++ {
		r.Request(obs(1, "app", kmsg.Heartbeat, time.Duration(i)*3*time.Second, idle("g"), func(o *protocol.Observation) {
			o.Duration = time.Duration(i) * time.Microsecond
		}))
	}
	e := r.Events(Filter{}, 0)[0]
	if e.Count != 200 || len(e.Ticks) != MaxTicks {
		t.Fatalf("count %d, %d ticks, want 200 and %d", e.Count, len(e.Ticks), MaxTicks)
	}
	if !e.Ticks[len(e.Ticks)-1].At.Equal(t0.Add(199*3*time.Second)) || !e.Ticks[0].At.Equal(t0.Add(136*3*time.Second)) {
		t.Errorf("ticks run %v to %v", e.Ticks[0].At, e.Ticks[len(e.Ticks)-1].At)
	}
	if n, atLeast := r.NewSince(Filter{}, t0.Add(10*time.Second)); n != MaxTicks || !atLeast {
		t.Errorf("new = %d, atLeast %v: the ticks do not reach back, so it is a lower bound", n, atLeast)
	}
	// A request that never folded has no ticks.
	r.Request(obs(1, "app", kmsg.Produce, time.Hour, func(o *protocol.Observation) { o.Topics = []string{"a"} }))
	for _, e := range r.Events(Filter{API: "produce"}, 0) {
		if len(e.Ticks) != 0 {
			t.Errorf("an unfolded row has ticks: %+v", e.Ticks)
		}
	}
}

// Counters: the whole broker's and each connection's, errors by class, in a five
// minute window of five second buckets, from an injected clock.
func TestCountersPerConnectionAndWindow(t *testing.T) {
	r := New(50, nil)
	now := t0.Add(10 * time.Minute)
	r.now = func() time.Time { return now }
	r.ConnOpened(1, "a")
	r.ConnOpened(2, "b")
	r.ConnOpened(3, "c")
	refuse := func(o *protocol.Observation) { o.ErrorCode, o.Class = 3, protocol.ClassRefused }
	expected := func(o *protocol.Observation) { o.ErrorCode, o.Class = 79, protocol.ClassExpected }
	at := func(ago time.Duration) time.Duration { return 10*time.Minute - ago }
	r.Request(obs(1, "app", kmsg.Produce, at(6*time.Minute), refuse)) // out of the window.
	r.Request(obs(1, "app", kmsg.Produce, at(4*time.Minute), refuse)) // in the five minute window only.
	for i := 0; i < 10; i++ {
		r.Request(obs(1, "app", kmsg.Produce, at(30*time.Second)))
	}
	r.Request(obs(1, "app", kmsg.Produce, at(20*time.Second), refuse))
	r.Request(obs(1, "app", kmsg.JoinGroup, at(10*time.Second), expected)) // expected flow is not an error.
	r.Request(obs(2, "other", kmsg.Fetch, at(5*time.Second)))
	r.Request(obs(3, "doze-kafka-console", kmsg.Fetch, at(5*time.Second))) // not counted.

	minute := r.Last(time.Minute)
	if minute.Requests != 13 || minute.Errors != 1 {
		t.Errorf("last minute = %+v, want 13 requests and 1 error", minute)
	}
	five := r.Last(5 * time.Minute)
	if five.Requests != 14 || five.Errors != 2 {
		t.Errorf("last five minutes = %+v, want 14 requests and 2 errors", five)
	}
	cs := r.Clients()
	if len(cs) != 2 {
		t.Fatalf("%d clients", len(cs))
	}
	if c := cs[0]; c.Req1m != 12 || c.Req5m != 13 || c.Err5m != 2 || c.Streak != 0 {
		t.Errorf("client 1 = req/min %d, 5 min %d, errors %d, streak %d", c.Req1m, c.Req5m, c.Err5m, c.Streak)
	}
	if c := cs[1]; c.Req1m != 1 || c.Err5m != 0 {
		t.Errorf("client 2 = %+v", c)
	}
	// Two refusals in a row make a streak, and a success ends it.
	r.Request(obs(2, "other", kmsg.Fetch, at(4*time.Second), refuse))
	r.Request(obs(2, "other", kmsg.Fetch, at(3*time.Second), refuse))
	if c := r.Clients()[1]; c.Streak != 2 || c.LastError != "UNKNOWN_TOPIC_OR_PARTITION" {
		t.Errorf("streak %d, last error %q", c.Streak, c.LastError)
	}
	r.Request(obs(2, "other", kmsg.Fetch, at(2*time.Second)))
	if c := r.Clients()[1]; c.Streak != 0 {
		t.Errorf("a success did not end the streak: %d", c.Streak)
	}
}

func TestNewClampsCapacity(t *testing.T) {
	if got := New(0, nil).Capacity(); got != 1 {
		t.Errorf("capacity %d, want 1", got)
	}
	r := New(-5, nil)
	r.ConnOpened(1, "a")
	r.Request(obs(1, "app", kmsg.Produce, 0))
	r.Request(obs(1, "app", kmsg.Produce, time.Second))
	if len(r.Events(Filter{}, 0)) != 1 {
		t.Error("a recorder of capacity 1 kept more than one row")
	}
}

// Errors grouped by what they said, with who got them.
func TestRefusalsAreGroupedByWhatTheySaid(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	r.ConnOpened(2, "b")
	unknown := func(o *protocol.Observation) { o.ErrorCode, o.Class = 3, protocol.ClassRefused }
	denied := func(o *protocol.Observation) { o.ErrorCode, o.Class = 30, protocol.ClassDenied }
	r.Request(obs(1, "checkout-api", kmsg.Produce, 0, unknown, func(o *protocol.Observation) { o.Topics = []string{"invoices"} }))
	r.Request(obs(1, "checkout-api", kmsg.Produce, time.Second, unknown))
	r.Request(obs(2, "loyalty-etl", kmsg.OffsetFetch, 2*time.Second, denied))
	r.Request(obs(2, "loyalty-etl", kmsg.Produce, 3*time.Second)) // fine.
	got := r.Refusals()
	if len(got) != 2 || got[0].Name != "UNKNOWN_TOPIC_OR_PARTITION" || got[0].Count != 2 || got[1].Name != "GROUP_AUTHORIZATION_FAILED" || got[1].Class != protocol.ClassDenied {
		t.Fatalf("refusals = %+v", got)
	}
	if len(got[0].Clients) != 1 || got[0].Clients[0] != (ClientCount{"checkout-api", 2}) {
		t.Errorf("clients = %+v", got[0].Clients)
	}
	if !got[0].Last.Equal(t0.Add(time.Second)) || !got[0].First.Equal(t0) {
		t.Errorf("first %v last %v", got[0].First, got[0].Last)
	}
	if _, ok := r.Row(got[0].Latest); !ok {
		t.Error("the latest row of a refusal is not found")
	}
}

// A request that closed the connection is a row of its own, with a reason.
func TestAConnectionEndingRequestIsARowThatSaysWhy(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	r.Request(obs(1, "app", 99, 0, func(o *protocol.Observation) {
		o.Closed, o.Class = "API key 99 is not served by this broker", protocol.ClassFailed
		o.Detail = "closed the connection: API key 99 is not served by this broker"
	}))
	e := r.Events(Filter{ErrorsOnly: true}, 0)
	if len(e) != 1 || e[0].Closed == "" || e[0].Error != "CONNECTION_CLOSED" || e[0].Class != protocol.ClassFailed {
		t.Fatalf("rows = %+v", e)
	}
	if w := r.Last(time.Minute); w.Errors != 0 && w.Requests != 1 {
		t.Logf("window %+v", w)
	}
}
