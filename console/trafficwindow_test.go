package console

import (
	"strings"
	"testing"
	"time"
)

// Every number on the summary line says what it is counted over. The errors are the
// errors of the minute the requests are counted in, not of the whole ring (which a
// busy broker turns over in seconds and an idle one holds for hours).
func TestTrafficSummaryCountsEveryNumberInItsOwnWindow(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{events: []TrafficEvent{
		{Seq: 5, First: now.Add(-2 * time.Second), Time: now.Add(-2 * time.Second), Count: 1, ClientID: "a", Remote: "r1", API: "Produce", Topics: []string{"orders"}},
		{Seq: 4, First: now.Add(-5 * time.Second), Time: now.Add(-5 * time.Second), Count: 1, ClientID: "a", Remote: "r1", API: "CreateTopics", Error: "TOPIC_ALREADY_EXISTS"},
		// Failed ten minutes ago: in the ring, not in the minute.
		{Seq: 3, First: now.Add(-10 * time.Minute), Time: now.Add(-10 * time.Minute), Count: 1, ClientID: "a", Remote: "r1", API: "Fetch", Error: "UNKNOWN_TOPIC_OR_PARTITION"},
		{Seq: 2, First: now.Add(-11 * time.Minute), Time: now.Add(-11 * time.Minute), Count: 7, ClientID: "a", Remote: "r1", API: "JoinGroup", Error: "REBALANCE_IN_PROGRESS"},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	body := req(t, c, "GET", "/_console/traffic", nil).Body.String()
	sum := between(body, `id="traffic-summary"`, "kept:")
	for _, want := range []string{"2 requests and", "1 error", "in the last minute"} {
		if !strings.Contains(sum, want) {
			t.Errorf("the summary does not say %q:\n%s", want, sum)
		}
	}
	for _, not := range []string{"9 errors", "3 errors", "2 errors"} {
		if strings.Contains(sum, not) {
			t.Errorf("the summary counts errors outside the minute (%q):\n%s", not, sum)
		}
	}
}

// A client's "refused" count is over the requests the recorder kept, and says so.
func TestClientRefusedCountSaysItIsOverKeptRequests(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{
		events: []TrafficEvent{
			{Seq: 2, First: now, Time: now, Count: 1, Conn: 1, ClientID: "a", Remote: "r1", API: "CreateTopics", Error: "TOPIC_ALREADY_EXISTS"},
		},
		clients: []TrafficClient{{Conn: 1, Remote: "r1", ClientID: "a", Opened: now.Add(-time.Hour), Last: now}},
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	body := req(t, c, "GET", "/_console/traffic", nil).Body.String()
	if !strings.Contains(body, "1 of its kept requests refused") {
		t.Errorf("the clients list does not say what its refused count is over:\n%s", between(body, `id="clients-list"`, "</aside>"))
	}
}
