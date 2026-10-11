package traffic

import (
	"fmt"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

func TestRequestsCountsEveryRecordedRequestFoldedOnesToo(t *testing.T) {
	r := New(10, nil)
	r.ConnOpened(1, "a")
	for i := 0; i < 5; i++ {
		r.Request(obs(1, "app", kmsg.Heartbeat, time.Duration(i)*time.Second, idle("g")))
	}
	r.Request(obs(1, "doze-kafka-console", kmsg.Fetch, 0)) // not recorded.
	if got := r.Requests(); got != 5 {
		t.Errorf("requests = %d, want 5", got)
	}
}

// A hundred and one groups: the one quiet longest goes, the noisy one stays.
func TestTheQuietestGroupIsTheOneForgotten(t *testing.T) {
	r := New(10, nil)
	for i := 0; i < MaxGroups; i++ {
		r.GroupEvent(fmt.Sprintf("g%03d", i), "joined", "m", 1, "", 1, "")
	}
	r.GroupEvent("g000", "assigned", "m", 1, "", 1, "") // g000 is active again; g001 is the quietest.
	r.GroupEvent("brand-new", "joined", "m", 1, "", 1, "")
	if len(r.GroupEvents("g001", 0)) != 0 {
		t.Error("the quietest group was kept")
	}
	if len(r.GroupEvents("g000", 0)) != 2 || len(r.GroupEvents("brand-new", 0)) != 1 {
		t.Error("an active group or the new one was dropped")
	}
}

func TestFiltersByKindAndConnectionAndPaging(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	r.ConnOpened(2, "b")
	r.Request(obs(1, "app", kmsg.Produce, 0, func(o *protocol.Observation) { o.Kind, o.Topics = "produce", []string{"t"} }))
	r.Request(obs(2, "other", kmsg.Fetch, time.Second, func(o *protocol.Observation) { o.Kind, o.Topics = "fetch", []string{"t"} }))
	r.Request(obs(2, "other", kmsg.Fetch, 2*time.Second, func(o *protocol.Observation) { o.Kind, o.Topics = "fetch", []string{"u"} }))
	if n := len(r.Events(Filter{Kind: "fetch"}, 0)); n != 2 {
		t.Errorf("fetches = %d", n)
	}
	if n := len(r.Events(Filter{Conn: 1}, 0)); n != 1 {
		t.Errorf("connection 1 rows = %d", n)
	}
	res := r.Query(Query{Filter: Filter{Kind: "fetch"}, Limit: 1})
	if len(res.Rows) != 1 || res.Total != 2 || !res.Newest.Equal(t0.Add(2*time.Second)) {
		t.Errorf("result = %d rows of %d, newest %v", len(res.Rows), res.Total, res.Newest)
	}
	if _, ok := r.RowBySeq(99999); ok {
		t.Error("an unknown sequence number found a row")
	}
	if n, atLeast := r.NewSince(Filter{Kind: "produce"}, t0.Add(-time.Hour)); n != 1 || atLeast {
		t.Errorf("new produces = %d %v", n, atLeast)
	}
}

func TestRefusalsKeepTheNewestRowAndTheEarliestTime(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	bad := func(code int16, class protocol.Class) func(*protocol.Observation) {
		return func(o *protocol.Observation) { o.ErrorCode, o.Class = code, class }
	}
	// The newest is observed first: the older one must still become First.
	r.Request(obs(1, "app", kmsg.Produce, 10*time.Second, bad(3, protocol.ClassRefused), func(o *protocol.Observation) { o.Topics = []string{"new"} }))
	r.Request(obs(1, "other", kmsg.Produce, 2*time.Second, bad(3, protocol.ClassRefused), func(o *protocol.Observation) { o.Topics = []string{"old"} }))
	r.Request(obs(1, "app", kmsg.Fetch, 20*time.Second, bad(30, protocol.ClassDenied)))
	got := r.Refusals()
	if len(got) != 2 || got[0].Count != 2 || !got[0].First.Equal(t0.Add(2*time.Second)) || !got[0].Last.Equal(t0.Add(10*time.Second)) || got[0].Topics[0] != "new" {
		t.Errorf("refusals = %+v", got)
	}
	if len(got[0].Clients) != 2 || got[0].Clients[0].Client != "app" {
		t.Errorf("clients = %+v", got[0].Clients)
	}
	// Equal counts: the newer is first.
	r.Request(obs(1, "app", kmsg.Fetch, 30*time.Second, bad(30, protocol.ClassDenied)))
	r.Request(obs(1, "app", kmsg.Fetch, 31*time.Second, bad(30, protocol.ClassDenied)))
	if got := r.Refusals(); got[0].Name != "GROUP_AUTHORIZATION_FAILED" {
		t.Errorf("first = %s", got[0].Name)
	}
}

func TestACounterWindowThatIsNotWholeBuckets(t *testing.T) {
	var c counters
	now := t0.Add(time.Hour)
	c.add(now.Add(-3*time.Second), false)
	c.add(now.Add(-30*time.Second), true)
	if total, errs := c.sum(now, 8*time.Second); total != 1 || errs != 0 {
		t.Errorf("8 s: %d %d", total, errs)
	}
	if total, errs := c.sum(now, 40*time.Second); total != 2 || errs != 1 {
		t.Errorf("40 s: %d %d", total, errs)
	}
}

// The console's own client is not a client, and a request it sends is not a row, but it
// does not make the real ones vanish.
func TestConsoleClientIsLeftOutOfClients(t *testing.T) {
	r := New(10, nil)
	r.ConnOpened(1, "a")
	r.ConnOpened(2, "b")
	r.Request(obs(1, "doze-kafka-console-fetch", kmsg.Fetch, 0))
	r.Request(obs(2, "app", kmsg.Fetch, 0))
	if cs := r.Clients(); len(cs) != 1 || cs[0].ClientID != "app" {
		t.Errorf("clients = %+v", cs)
	}
}
