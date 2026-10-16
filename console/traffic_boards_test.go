package console

import (
	"strings"
	"testing"
	"time"
)

// A source with what the boards show: a folded heartbeat, a refusal Kafka would also
// make, a denial, a first join that is normal flow, and a group that rebalanced.
func boardsTraffic() *fakeTraffic {
	now := time.Now()
	src := &fakeTraffic{}
	src.events = []TrafficEvent{
		{ID: "e5", Seq: 5, First: now.Add(-3 * time.Second), Time: now.Add(-3 * time.Second), Count: 1, Conn: 1, ClientID: "checkout-api", Remote: "127.0.0.1:55102", Principal: "User:checkout",
			API: "Produce", Version: 9, Kind: "produce", Topics: []string{"harbour.checkout.invoices"}, Detail: "no such topic",
			Error: "UNKNOWN_TOPIC_OR_PARTITION", ErrorCode: 3, Class: "refused", Duration: time.Millisecond,
			Facts: TrafficFacts{Acks: -1, HasAcks: true, Decoded: `{"acks": -1, "topics": [{"topic": "harbour.checkout.invoices"}]}`,
				Partitions: []TrafficPart{{Topic: "harbour.checkout.invoices", Partition: 1, ErrorCode: 3}}}},
		{ID: "e4", Seq: 4, First: now.Add(-5 * time.Second), Time: now.Add(-5 * time.Second), Count: 1, Conn: 2, ClientID: "loyalty-etl", Remote: "127.0.0.1:55127", Principal: "User:loyalty",
			API: "OffsetFetch", Version: 8, Kind: "groups", Group: "g-loyalty-etl", Error: "GROUP_AUTHORIZATION_FAILED", ErrorCode: 30, Class: "denied", Duration: time.Millisecond},
		{ID: "e3", Seq: 3, First: now.Add(-40 * time.Second), Time: now.Add(-4 * time.Second), Count: 13, Conn: 3, ClientID: "order-validator", Remote: "127.0.0.1:55110", Principal: "User:validator",
			API: "Heartbeat", Version: 4, Kind: "groups", Group: "g-order-validator", Detail: "generation 7", Duration: time.Millisecond,
			Ticks: []TrafficTick{{At: now.Add(-40 * time.Second), Duration: time.Millisecond}, {At: now.Add(-37 * time.Second), Duration: 2 * time.Millisecond}, {At: now.Add(-4 * time.Second), Duration: time.Millisecond}}},
		{ID: "e2", Seq: 2, First: now.Add(-50 * time.Second), Time: now.Add(-50 * time.Second), Count: 1, Conn: 3, ClientID: "order-validator", Remote: "127.0.0.1:55110", Principal: "User:validator",
			API: "JoinGroup", Version: 9, Kind: "groups", Group: "g-order-validator", Error: "MEMBER_ID_REQUIRED", ErrorCode: 79, Class: "expected", Duration: time.Millisecond},
		{ID: "e1", Seq: 1, First: now.Add(-45 * time.Second), Time: now.Add(-45 * time.Second), Count: 1, Conn: 3, ClientID: "order-validator", Remote: "127.0.0.1:55110", Principal: "User:validator",
			API: "Fetch", Version: 16, Kind: "fetch", ViaGroup: "g-order-validator", Topics: []string{"harbour.checkout.orders"}, Detail: "p0 from 4,812 · 3 records", Duration: time.Millisecond},
		{ID: "e0", Seq: 0, First: now.Add(-20 * time.Second), Time: now.Add(-20 * time.Second), Count: 1, Conn: 9, ClientID: "doze-pump", Remote: "127.0.0.1:1",
			API: "Metadata", Version: 12, Closed: "API key 99 is not served by this broker", Error: "CONNECTION_CLOSED", Class: "failed", Duration: time.Millisecond},
	}
	src.clients = []TrafficClient{
		{Conn: 1, ClientID: "checkout-api", Software: "Apache Kafka Java 4.0.0", Principal: "User:checkout", Remote: "127.0.0.1:55102", Opened: now.Add(-2 * time.Hour), Last: now.Add(-3 * time.Second),
			Requests: 12, Produces: true, TxnID: "checkout-1", Req1m: 186, Req5m: 930, Err5m: 1},
		{Conn: 2, ClientID: "loyalty-etl", Software: "librdkafka 2.5.3", Principal: "User:loyalty", Remote: "127.0.0.1:55127", Opened: now.Add(-time.Hour), Last: now.Add(-5 * time.Second),
			Requests: 4, Groups: []string{"g-loyalty-etl"}, Fetches: true, Req1m: 4, Req5m: 4, Err5m: 4, Streak: 4, LastError: "GROUP_AUTHORIZATION_FAILED"},
		{Conn: 3, ClientID: "order-validator", Software: "librdkafka 2.5.3", Principal: "User:validator", Remote: "127.0.0.1:55110", Opened: now.Add(-2 * time.Hour), Last: now.Add(-4 * time.Second),
			Requests: 15, Groups: []string{"g-order-validator"}, Fetches: true, Req1m: 118, Req5m: 590},
	}
	src.story = TrafficStory{Group: "g-order-validator", Protocol: "classic", Requests: 15, Folded: 12, Topics: []string{"harbour.checkout.orders"},
		Rebalances: []TrafficRebalance{{Start: now.Add(-50 * time.Second), End: now.Add(-44 * time.Second), Done: true, From: 6, To: 7, Cause: "a member joined", Members: 3, MembersBefore: 2,
			Rows: []TrafficEvent{src.events[4], src.events[3]}}},
		Before:      []TrafficEvent{src.events[2]},
		Generations: []TrafficGeneration{{Gen: 6, Cause: "second member joined", Members: 2, At: now.Add(-time.Hour)}, {Gen: 7, Cause: "a member joined", Members: 3, At: now.Add(-44 * time.Second)}},
	}
	return src
}

func TestTrafficBoardsRenderWhatTheRecorderHolds(t *testing.T) {
	src := boardsTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "harbour.checkout.orders", 1, 0)

	// Main: a folded heartbeat reads as a rhythm, a refusal in the board's purple, normal
	// flow is not an error, a closed connection says why.
	body := strings.ReplaceAll(req(t, c, "GET", "/_console/traffic", nil).Body.String(), "<wbr>", "") // a result wraps at its underscores.
	for _, want := range []string{"every 3 s", `class="errrow refused"`, `class="errrow denied"`, `class="errrow failed"`, "MEMBER_ID_REQUIRED", "CONNECTION_CLOSED", "3 clients connected", "last 4 requests refused", "producer · tx checkout-1"} {
		if !strings.Contains(body, want) {
			t.Errorf("Main does not show %q", want)
		}
	}
	if strings.Contains(between(body, "MEMBER_ID_REQUIRED", "</tr>"), "errrow") {
		t.Error("a first join is shown as an error")
	}

	// Errors only: expected flow is out, the explainer is the inspector's default.
	errs := req(t, c, "GET", "/_console/traffic?kind=errors", nil).Body.String()
	if strings.Contains(errs, "MEMBER_ID_REQUIRED") {
		t.Error("errors only lists a first join")
	}
	for _, want := range []string{"Most frequent refusal", "In plain words", "Create harbour.checkout.invoices", "See the 1 topic that exists", "The other refusals"} {
		if !strings.Contains(errs, want) {
			t.Errorf("the errors view does not show %q", want)
		}
	}

	// A topic: who touched it.
	tp := req(t, c, "GET", "/_console/traffic?topic=harbour.checkout.invoices", nil).Body.String()
	for _, want := range []string{"Who touched harbour.checkout.invoices", "Produce requests", "of 18 requests match"} {
		if !strings.Contains(tp, want) {
			t.Errorf("a topic filter does not show %q", want)
		}
	}

	// A group: the story, nested under its rebalance.
	st := req(t, c, "GET", "/_console/traffic?group=g-order-validator", nil).Body.String()
	for _, want := range []string{`id="traffic-story"`, "a member joined", "generation <span class=\"b\">6 → 7</span>", "Why it rebalanced", "had 2 members at generation 6 and 3 at generation 7", "Show them as a plain list"} {
		if !strings.Contains(st, want) {
			t.Errorf("the group story does not show %q", want)
		}
	}
	// Traf-12 draws one row above the story: the field, the kinds, the group's pill, clear
	// and the count; no summary line, no Filters row; its foot note and the way out.
	for _, want := range []string{`class="seg"`, `class="pill pill-name"`, `class="b">clear</a>`, `id="story-count"`} {
		if !strings.Contains(st, want) {
			t.Errorf("the story's filter row does not show %q", want)
		}
	}
	for _, gone := range []string{`id="traffic-summary"`, `id="filter-pills"`} {
		if strings.Contains(st, gone) {
			t.Errorf("the story page still draws %s", gone)
		}
	}
	if !strings.Contains(st, "Indented rows belong to the rebalance above them.") || !strings.Contains(st, "Idle repeats fold; any change breaks the fold.") {
		t.Error("the story's foot note is not drawn")
	}
	if flat := req(t, c, "GET", "/_console/traffic?group=g-order-validator&flat=1", nil).Body.String(); strings.Contains(flat, `id="traffic-story"`) {
		t.Error("flat still shows the story")
	} else if !strings.Contains(flat, `id="traffic-summary"`) || strings.Contains(flat, `class="filterrow story"`) {
		t.Error("a plain list is the Traffic page with its summary and its own filter row")
	}
	// The generations table draws the newest generation as the board's .on row, and only that.
	gens := between(st, `id="group-summary"`, "What the sequence means")
	if n := strings.Count(gens, `<div class="r on">`); n != 1 || !strings.Contains(gens, `<div class="r on"><span class="m b">7</span>`) {
		t.Errorf("generations: %d rows are .on, want only generation 7: %s", n, gens)
	}
	// The folded heartbeat of the generation before is the unindented last row, at the
	// time it was first seen (not when it was last seen, with milliseconds).
	story := between(st, `id="traffic-story"`, "</main>")
	if !strings.Contains(story, `<div class="r kid `) {
		t.Errorf("the generation before is not an unindented row: %s", story)
	}
	// Result names wrap at their underscores: a break may follow each, never fall inside a word.
	if main := req(t, c, "GET", "/_console/traffic", nil).Body.String(); !strings.Contains(main, "UNKNOWN_<wbr>TOPIC_<wbr>OR_<wbr>PARTITION") {
		t.Error("a refusal's name does not offer a break after each underscore")
	}
	// The frozen line is the row under the header, not above the table.
	if main := req(t, c, "GET", "/_console/traffic", nil).Body.String(); !strings.Contains(main, `</tr>
      <tr class="frozen-line lost-only">`) {
		t.Error("the frozen line is not the table's second header row")
	}

	// Clients: needs a look, the callout with what it means.
	cl := req(t, c, "GET", "/_console/traffic?view=clients", nil).Body.String()
	for _, want := range []string{"1 client needs a look", "loyalty-etl has been refused 4 times in a row", "100 %", "none (producer)", "g-loyalty-etl, refused"} {
		if !strings.Contains(cl, want) {
			t.Errorf("the clients view does not show %q", want)
		}
	}
}

func TestRequestInspectorShowsPartitionsDecodedFieldsAndWhyARefusalHappened(t *testing.T) {
	src := boardsTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	one := req(t, c, "GET", "/_console/inspect/request?key=e5", nil).Body.String()
	for _, want := range []string{"Produce v9", "Refused", "Real Kafka would refuse the same call.", "Result per partition", "harbour.checkout.invoices",
		"Decoded fields", "Acks", "-1 (all)", "Record bytes are shown on the topic page, not here.", "correlation"} {
		if !strings.Contains(one, want) {
			t.Errorf("the request inspector does not show %q", want)
		}
	}
	folded := req(t, c, "GET", "/_console/inspect/request?key=e3&unfold=1", nil).Body.String()
	for _, want := range []string{"Folded row", "13 identical heartbeats are shown as one row", "Why it folded", "Each tick is one heartbeat", `id="unfolded"`, "Open g-order-validator"} {
		if !strings.Contains(folded, want) {
			t.Errorf("the folded inspector does not show %q", want)
		}
	}
	closed := req(t, c, "GET", "/_console/inspect/request?key=e0", nil).Body.String()
	if !strings.Contains(closed, "The broker closed the connection") || !strings.Contains(closed, "API key 99 is not served by this broker") {
		t.Errorf("a request that ended its connection does not say why:\n%s", closed)
	}
	expected := req(t, c, "GET", "/_console/inspect/request?key=e2", nil).Body.String()
	if !strings.Contains(expected, "normal flow") {
		t.Error("a first join is not explained as normal flow")
	}
}

// The paused page asks, cheaply, how many requests it missed.
func TestPausedPageCountsWhatItMissed(t *testing.T) {
	src := boardsTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	rec := req(t, c, "GET", "/_console/traffic/new?since=0", nil)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"n":`) || rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("new: %d %s", rec.Code, rec.Body)
	}
	if bad := req(t, c, "GET", "/_console/traffic/new?since=soon", nil); bad.Code != 400 {
		t.Errorf("a bad cursor answered %d", bad.Code)
	}
}

// An empty recorder is the board's "nothing yet": the line to point an app at is the
// running configuration's.
func TestEmptyTrafficPointsAnAppAtTheRunningBootstrap(t *testing.T) {
	src := &fakeTraffic{}
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "orders", 1, 0) // not first run: a topic exists.
	body := req(t, c, "GET", "/_console/traffic", nil).Body.String()
	for _, want := range []string{"Nothing has reached this broker yet", "bootstrap.servers=", "Three first steps", "Connect an app", "Create a topic", "0 requests so far"} {
		if !strings.Contains(body, want) {
			t.Errorf("the empty state does not show %q", want)
		}
	}
	_ = addr
}

func TestNearestTopicAndDistance(t *testing.T) {
	topics := []TopicSummary{{Name: "harbour.checkout.orders"}, {Name: "harbour.depot.dispatch"}}
	if got, ok := nearestTopic("harbour.checkout.order", topics); !ok || got != "harbour.checkout.orders" {
		t.Errorf("nearest = %q %v", got, ok)
	}
	if _, ok := nearestTopic("something-else-entirely", topics); ok {
		t.Error("a name that is nothing like any was suggested")
	}
	if got := intervalText(TrafficEvent{Count: 3, First: tt0, Time: tt0.Add(90 * time.Second)}); got != "every 45 s" {
		t.Errorf("interval = %q", got)
	}
	if got := intervalText(TrafficEvent{Count: 3, First: tt0, Time: tt0.Add(300 * time.Millisecond)}); got != "every 150 ms" {
		t.Errorf("interval = %q", got)
	}
	if got := intervalText(TrafficEvent{Count: 3, First: tt0, Time: tt0.Add(10 * time.Minute)}); got != "every 5 min" {
		t.Errorf("interval = %q", got)
	}
	for class, want := range map[string]string{"": "ok", "failed": "bad", "not-implemented": "warn", "refused": "deny", "denied": "deny", "expected": "deny"} {
		if got := resultTone(class); got != want {
			t.Errorf("tone(%q) = %q, want %q", class, got, want)
		}
	}
	for class, want := range map[string]string{"expected": "Expected", "refused": "Refused", "denied": "Denied", "not-implemented": "Not implemented", "failed": "Failed", "": ""} {
		if got := classWord(class); got != want {
			t.Errorf("word(%q) = %q, want %q", class, got, want)
		}
	}
	if got := acksText(0) + acksText(1) + acksText(7); got != "0 (none)1 (leader)7" {
		t.Errorf("acks = %q", got)
	}
	if got := repeatUnit("Fetch") + repeatUnit("PushTelemetry") + repeatUnit("OffsetFetch") + repeatUnit("Metadata"); got != "fetchreportoffset fetchrequest" {
		t.Errorf("units = %q", got)
	}
	if n := timelineKindsCovered(); n < 8 {
		t.Errorf("%d timeline kinds", n)
	}
}

func timelineKindsCovered() int {
	n := 0
	for _, k := range []string{"joined", "left", "expired", "dropped", "rebalance", "generation", "assigned", "epoch", "other"} {
		if l, _ := timelineKind(k); l != "" {
			n++
		}
	}
	return n
}
