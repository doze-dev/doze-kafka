package console

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The page counts clients, not connections: an app that holds three connections is one
// client with "3 connections" (the summary, the table, the rail), and each connection is
// still listed in its inspector.
func TestClientsAreGroupedByClientID(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{}
	src.events = []TrafficEvent{{ID: "r1", Seq: 1, First: now, Time: now, Count: 1, Conn: 1, ClientID: "checkout-api", API: "Produce", Version: 9, Kind: "produce", Duration: time.Millisecond}}
	src.clients = []TrafficClient{
		{Conn: 1, ClientID: "checkout-api", Software: "Java 4.0", Principal: "User:checkout", Remote: "127.0.0.1:55101", Opened: now.Add(-2 * time.Hour), Last: now.Add(-3 * time.Second), Requests: 10, Req1m: 5, Req5m: 20, Err5m: 1},
		{Conn: 2, ClientID: "checkout-api", Software: "Java 4.0", Principal: "User:checkout", Remote: "127.0.0.1:55102", Opened: now.Add(-time.Hour), Last: now.Add(-1 * time.Second), Requests: 30, Req1m: 7, Req5m: 30, Err5m: 0, Groups: []string{"g-a"}},
		{Conn: 3, ClientID: "checkout-api", Software: "Java 4.0", Principal: "User:checkout", Remote: "127.0.0.1:55103", Opened: now.Add(-time.Minute), Last: now.Add(-2 * time.Second), Requests: 2, Req1m: 1, Req5m: 2},
		{Conn: 4, ClientID: "order-validator", Software: "librdkafka", Principal: "User:validator", Remote: "127.0.0.1:55110", Opened: now.Add(-time.Hour), Last: now, Requests: 4, Req1m: 4, Req5m: 4},
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })

	page := req(t, c, "GET", "/_console/traffic", nil).Body.String()
	if !strings.Contains(page, "2 clients connected") {
		t.Errorf("the summary counts connections, not clients: %s", between(page, `id="traffic-summary"`, "</div>"))
	}
	if !strings.Contains(page, "2 clients · 4 connections") {
		t.Errorf("the inspector's heading does not say clients and connections: %s", between(page, "Connected clients", "</div>"))
	}
	if n := strings.Count(page, `class="insp-client"`); n != 2 {
		t.Errorf("%d clients in the connected-clients list, want 2", n)
	}

	view := req(t, c, "GET", "/_console/traffic?view=clients", nil).Body.String()
	if n := strings.Count(view, `<tr id="cl-`); n != 2 {
		t.Errorf("%d rows in the clients table, want 2 (one per client id)", n)
	}
	if !strings.Contains(view, "3 connections") {
		t.Error("the client row does not say how many connections it has")
	}
	// The row is the one open for any of its connections.
	if !strings.Contains(view, `data-open-keys="client:1 client:2 client:3"`) {
		t.Errorf("the row does not name the connections it opens: %s", between(view, `<tr id="cl-`, ">"))
	}
	// Req/min is the sum, the groups are the union.
	row := between(view, `<tr id="cl-`, "</tr>")
	if !strings.Contains(row, ">13<") || !strings.Contains(row, "g-a") {
		t.Errorf("the grouped row does not add its connections up: %s", row)
	}

	// The rail counts clients.
	if got := c.railCounts(context.Background())["traffic"]; got != 2 {
		t.Errorf("the rail counts %d, want 2 clients", got)
	}

	// The inspector keeps the per-connection detail.
	one := req(t, c, "GET", "/_console/inspect/client?key=2", nil).Body.String()
	for _, want := range []string{"Its connections", "127.0.0.1:55101", "127.0.0.1:55102", "127.0.0.1:55103"} {
		if !strings.Contains(one, want) {
			t.Errorf("the client inspector does not show %q", want)
		}
	}
}

// The row that is open is marked by the shell from what the row names; each row of the
// requests table and of the story names the request it opens.
func TestRowsNameWhatTheyOpen(t *testing.T) {
	src := boardsTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	body := req(t, c, "GET", "/_console/traffic", nil).Body.String()
	if !strings.Contains(body, `id="rq-e5"`) || !strings.Contains(body, `data-open-keys="request:e5"`) {
		t.Error("a request row does not name what it opens")
	}
	story := req(t, c, "GET", "/_console/traffic?group=g-order-validator", nil).Body.String()
	if !strings.Contains(story, `data-open-keys="request:e1"`) {
		t.Error("a story row does not name what it opens")
	}
}

// The story's folded heartbeat reads "every 3 s, since <time>", the time an element the
// browser localises, as the flat table does.
func TestStoryRhythmTimeIsATimeElement(t *testing.T) {
	src := boardsTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	story := req(t, c, "GET", "/_console/traffic?group=g-order-validator", nil).Body.String()
	row := between(story, `id="rq-e3"`, "</div>")
	if !strings.Contains(row, "since <time") {
		t.Errorf("the story's folded row does not draw its time as a <time>: %s", row)
	}
	if !strings.Contains(row, `data-style="clock"`) {
		t.Errorf("the story's time has no style for the browser to localise it by: %s", row)
	}
}

// Nothing has reached the broker (Traf-09): the empty card, the keys that say connect,
// and "up and listening" only while the broker answers (Traf-10).
func TestEmptyBrokerPage(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = &fakeTraffic{} })
	body := req(t, c, "GET", "/_console/traffic", nil).Body.String()
	for _, want := range []string{`id="traffic-empty"`, "Nothing has reached this broker yet", `class="up-only"`, `class="lost-only"`, "connect an app", `data-create`} {
		if !strings.Contains(body, want) {
			t.Errorf("the empty page does not show %q", want)
		}
	}
	strip := between(body, `id="keys"`, "</div>")
	if !strings.Contains(strip, "connect an app") || !strings.Contains(strip, "pause") {
		t.Errorf("the empty page's key strip = %s", strip)
	}
	if strings.Contains(strip, "errors only") {
		t.Errorf("the empty page's key strip offers a key the page does not have: %s", strip)
	}
	if !strings.Contains(strip, `class="keys-lost"`) || !strings.Contains(strip, "retry now") {
		t.Errorf("the strip has no retry hint for a lost broker: %s", strip)
	}
}

// A page with rows has r bound to Retry now, and its strip says so while the broker is
// lost and not otherwise.
func TestRetryKeyAndStrip(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = boardsTraffic() })
	body := req(t, c, "GET", "/_console/traffic", nil).Body.String()
	strip := between(body, `id="keys"`, "</div>")
	for _, want := range []string{`class="keys-live"`, `class="keys-lost"`, "retry now", "errors only"} {
		if !strings.Contains(strip, want) {
			t.Errorf("the traffic strip does not show %q: %s", want, strip)
		}
	}
	var bound bool
	for _, k := range keyMap {
		if k.Key == "r" && k.Action == "retry" {
			bound = true
		}
	}
	if !bound {
		t.Error("r is not Retry now in the key map")
	}
}

// A request with nothing to say shows a dash, not an empty cell; one with a refusal's
// phrase shows it.
func TestEmptyDetailIsADash(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "d1", Seq: 1, First: now, Time: now, Count: 1, Conn: 1, ClientID: "app", API: "ApiVersions", Version: 4, Duration: time.Millisecond},
		{ID: "d2", Seq: 2, First: now, Time: now, Count: 1, Conn: 1, ClientID: "app", API: "Metadata", Version: 12, Detail: "3 topics", Duration: time.Millisecond},
	}, clients: []TrafficClient{{Conn: 1, ClientID: "app", Opened: now, Last: now, Requests: 2}}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	body := req(t, c, "GET", "/_console/traffic", nil).Body.String()
	if d := between(body, `id="rq-d1"`, "</tr>"); !strings.Contains(between(d, "cell-detail", "</td>"), "—") {
		t.Errorf("an empty detail is blank: %s", d)
	}
	if d := between(body, `id="rq-d2"`, "</tr>"); !strings.Contains(between(d, "cell-detail", "</td>"), "3 topics") {
		t.Errorf("the detail is not shown: %s", d)
	}
}
