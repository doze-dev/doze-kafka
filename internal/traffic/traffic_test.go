package traffic

import (
	"fmt"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

var t0 = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

func obs(conn uint64, client string, api protocol.APIKey, at time.Duration, mod ...func(*protocol.Observation)) protocol.Observation {
	o := protocol.Observation{
		Conn: conn, Remote: "127.0.0.1:50000", Principal: "User:ANONYMOUS", ClientID: client,
		API: api, Version: 1, Start: t0.Add(at), Duration: time.Millisecond, ReqBytes: 40, RespBytes: 20,
	}
	for _, m := range mod {
		m(&o)
	}
	return o
}

func TestTheRingKeepsTheNewestAndNoMore(t *testing.T) {
	r := New(5, nil)
	if r.Capacity() != 5 {
		t.Errorf("capacity %d, want 5", r.Capacity())
	}
	r.ConnOpened(1, "x")
	for i := 0; i < 12; i++ {
		i := i
		r.Request(obs(1, "app", kmsg.Produce, time.Duration(i)*time.Second, func(o *protocol.Observation) {
			o.Topics = []string{fmt.Sprintf("t%d", i)}
		}))
	}
	got := r.Events(Filter{}, 0)
	if len(got) != 5 {
		t.Fatalf("kept %d rows, want 5", len(got))
	}
	if got[0].Topics[0] != "t11" || got[4].Topics[0] != "t7" {
		t.Errorf("kept %v ... %v, want t11 down to t7", got[0].Topics, got[4].Topics)
	}
	if first := r.Events(Filter{}, 2); len(first) != 2 || first[0].Topics[0] != "t11" {
		t.Errorf("a limit of 2 gave %d rows", len(first))
	}
}

func TestTheConsolesOwnRequestsAreLeftOut(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	r.ConnOpened(2, "b")
	r.Request(obs(1, "doze-kafka-console", kmsg.Metadata, 0))
	r.Request(obs(1, "doze-kafka-console", kmsg.Fetch, time.Second))
	r.Request(obs(2, "billing", kmsg.Produce, 2*time.Second))
	if got := r.Events(Filter{}, 0); len(got) != 1 || got[0].ClientID != "billing" {
		t.Fatalf("rows = %+v, want only billing's", got)
	}
	if cs := r.Clients(); len(cs) != 1 || cs[0].ClientID != "billing" {
		t.Errorf("clients = %+v, want only billing", cs)
	}
}

// A consumer that beats every three seconds is one row that counts, and a row that
// is the same request with a different answer is not folded into it.
func TestIdleRepeatsFoldIntoOneRow(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	idle := func(o *protocol.Observation) { o.Group = "billing"; o.Idle = true }
	r.Request(obs(1, "app", kmsg.Heartbeat, 0, idle))
	r.Request(obs(1, "app", kmsg.Produce, time.Second))
	r.Request(obs(1, "app", kmsg.Heartbeat, 3*time.Second, idle))
	r.Request(obs(1, "app", kmsg.Heartbeat, 6*time.Second, idle))
	r.Request(obs(1, "app", kmsg.Heartbeat, 9*time.Second, idle, func(o *protocol.Observation) { o.ErrorCode = 27 }))

	rows := r.Events(Filter{}, 0)
	if len(rows) != 3 {
		t.Fatalf("%d rows, want 3 (heartbeat x3, produce, heartbeat error): %+v", len(rows), rows)
	}
	var folded *Event
	for i := range rows {
		if rows[i].API == "Heartbeat" && rows[i].Error == "" {
			folded = &rows[i]
		}
	}
	if folded == nil || folded.Count != 3 || !folded.First.Equal(t0) || !folded.Time.Equal(t0.Add(6*time.Second)) {
		t.Fatalf("folded row = %+v, want count 3 from t0 to t0+6s", folded)
	}
	// Rows are in order of latest activity: the error row, then the folded beat
	// (last at +6s), then the produce (at +1s).
	if rows[0].Error != "REBALANCE_IN_PROGRESS" || rows[1].API != "Heartbeat" || rows[2].API != "Produce" {
		t.Errorf("order = %s/%s/%s", rows[0].API, rows[1].API, rows[2].API)
	}

	// Another connection's identical beat is its own row.
	r.ConnOpened(2, "b")
	r.Request(obs(2, "other", kmsg.Heartbeat, 10*time.Second, idle))
	if got := r.Events(Filter{API: "heartbeat"}, 0); len(got) != 3 {
		t.Errorf("heartbeat rows = %d, want 3 (two on conn 1, one on conn 2)", len(got))
	}

	// A fold long after the last one starts a fresh row instead of reviving the old.
	r.Request(obs(1, "app", kmsg.Heartbeat, time.Hour, idle))
	if n := r.Events(Filter{Client: "app", API: "heartbeat"}, 0); len(n) != 3 {
		t.Errorf("after an hour: %d heartbeat rows for app, want 3", len(n))
	}
}

func TestFoldingDoesNotOutliveTheRow(t *testing.T) {
	r := New(3, nil)
	r.ConnOpened(1, "a")
	idle := func(o *protocol.Observation) { o.Group = "g"; o.Idle = true }
	r.Request(obs(1, "app", kmsg.Heartbeat, 0, idle))
	for i := 1; i <= 3; i++ {
		r.Request(obs(1, "app", kmsg.Produce, time.Duration(i)*time.Second))
	}
	// The beat row was pushed out of the ring; the next beat is a new row, not an
	// update to something nobody can see.
	r.Request(obs(1, "app", kmsg.Heartbeat, 5*time.Second, idle))
	for _, e := range r.Events(Filter{API: "heartbeat"}, 0) {
		if e.Count != 1 {
			t.Errorf("a beat row counts %d after the original was evicted", e.Count)
		}
	}
}

func TestFilters(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "127.0.0.1:50000")
	r.Request(obs(1, "billing-app", kmsg.Produce, 0, func(o *protocol.Observation) { o.Topics = []string{"orders"} }))
	r.Request(obs(1, "billing-app", kmsg.Fetch, time.Second, func(o *protocol.Observation) { o.Topics = []string{"orders", "audit"} }))
	r.Request(obs(1, "billing-app", kmsg.OffsetCommit, 2*time.Second, func(o *protocol.Observation) {
		o.Group = "billing"
		o.ErrorCode = 16
	}))
	r.Request(obs(1, "billing-app", kmsg.Produce, 3*time.Second, func(o *protocol.Observation) { o.Software = "apache-kafka-java 3.7.0"; o.Topics = []string{"payments"} }))

	n := func(f Filter) int { return len(r.Events(f, 0)) }
	for name, c := range map[string]struct {
		f    Filter
		want int
	}{
		"all":         {Filter{}, 4},
		"topic":       {Filter{Topic: "orders"}, 2},
		"topic exact": {Filter{Topic: "order"}, 0},
		"group":       {Filter{Group: "billing"}, 1},
		"api":         {Filter{API: "FETCH"}, 1},
		"errors":      {Filter{ErrorsOnly: true}, 1},
		"client":      {Filter{Client: "BILLING"}, 4},
		"software":    {Filter{Client: "java 3.7"}, 1},
		"remote":      {Filter{Client: "127.0.0.1"}, 4},
		"nobody":      {Filter{Client: "inventory"}, 0},
		"combined":    {Filter{Topic: "orders", API: "produce"}, 1},
	} {
		if got := n(c.f); got != c.want {
			t.Errorf("%s: %d rows, want %d", name, got, c.want)
		}
	}
}

func TestTopicIDsAreResolvedWhenRecorded(t *testing.T) {
	id := [16]byte{9}
	r := New(10, func(x [16]byte) string {
		if x == id {
			return "orders"
		}
		return ""
	})
	r.ConnOpened(1, "a")
	r.Request(obs(1, "app", kmsg.Fetch, 0, func(o *protocol.Observation) { o.TopicIDs = [][16]byte{id, {1}} }))
	got := r.Events(Filter{Topic: "orders"}, 0)
	if len(got) != 1 || len(got[0].Topics) != 1 {
		t.Fatalf("rows = %+v, want one with topic orders", got)
	}
}

func TestClientsAreTheLiveConnections(t *testing.T) {
	r := New(10, nil)
	r.ConnOpened(1, "10.0.0.1:1")
	r.ConnOpened(2, "10.0.0.2:2")
	r.Request(obs(2, "billing", kmsg.ApiVersions, 0, func(o *protocol.Observation) { o.Software = "librdkafka 2.3.0" }))
	r.Request(obs(2, "billing", kmsg.JoinGroup, time.Second, func(o *protocol.Observation) { o.Group = "billing" }))
	cs := r.Clients()
	if len(cs) != 2 || cs[0].Conn != 1 || cs[1].Conn != 2 {
		t.Fatalf("clients = %+v", cs)
	}
	c := cs[1]
	if c.ClientID != "billing" || c.Software != "librdkafka 2.3.0" || c.Requests != 2 || len(c.Groups) != 1 || c.Groups[0] != "billing" {
		t.Errorf("client 2 = %+v", c)
	}
	r.ConnClosed(1)
	if cs := r.Clients(); len(cs) != 1 || cs[0].Conn != 2 {
		t.Errorf("after close: %+v", cs)
	}
	// A request on a connection nobody announced still gets a client.
	r.Request(obs(9, "late", kmsg.Metadata, 0))
	if len(r.Clients()) != 2 {
		t.Errorf("a request from an unannounced connection is lost")
	}
}

func TestGroupEventsAreKeptInOrderPerGroup(t *testing.T) {
	r := New(4, nil)
	r.GroupEvent("billing", "joined", "m1", 0, "client a")
	r.GroupEvent("audit", "joined", "x1", 0, "")
	r.GroupEvent("billing", "rebalance", "m1", 0, "a member joined")
	r.GroupEvent("billing", "generation", "m1", 1, "1 member")
	got := r.GroupEvents("billing", 0)
	if len(got) != 3 || got[0].Kind != "joined" || got[1].Kind != "rebalance" || got[2].Kind != "generation" || got[2].Generation != 1 {
		t.Fatalf("billing events = %+v", got)
	}
	if !(got[0].Seq < got[1].Seq && got[1].Seq < got[2].Seq) {
		t.Errorf("not in order: %+v", got)
	}
	if last := r.GroupEvents("billing", 1); len(last) != 1 || last[0].Kind != "generation" {
		t.Errorf("limit 1 = %+v", last)
	}
	// Bounded: a fifth event pushes the oldest out.
	r.GroupEvent("billing", "assigned", "m1", 1, "")
	if got := r.GroupEvents("billing", 0); len(got) != 3 || got[0].Kind != "rebalance" {
		t.Errorf("after overflow: %+v", got)
	}
	if got := r.GroupEvents("nobody", 0); len(got) != 0 {
		t.Errorf("unknown group: %+v", got)
	}
}
