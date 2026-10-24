package console

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A search that has its hundred matches stops there and offers the rest, and does not
// read the next chunk to find more than it will show.
func TestSearchStopsAtItsHundredthMatch(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "hund", 1, nil); err != nil {
		t.Fatal(err)
	}
	// 3000 records: the oldest 2000 never match; the newest thousand hold exactly 100 matches.
	bulkProduce(t, cl, "hund", 0, 3000, func(i int) string {
		if i >= 2000 && i%10 == 0 {
			return fmt.Sprintf("hit-%d", i)
		}
		return fmt.Sprintf("miss-%d", i)
	}, time.Time{})
	hits, from, to, earliest, err := c.scanPartition(ctx, "hund", 0, []byte("hit-"), -1, 5000)
	if err != nil || len(hits) != 100 || from != 2000 || to != 3000 || earliest != 0 {
		t.Errorf("%d hits, read [%d,%d) of a log from %d (%v); want 100 hits from [2000,3000)", len(hits), from, to, earliest, err)
	}
	sv, err := c.searchTopic(ctx, "hund", 0, 1, "hit-", false, -1)
	if err != nil || len(sv.Rows) != 100 || !strings.Contains(sv.Note, "Read offsets 2000–2999 of partition 0 (1000 records): 100 matches.") ||
		!strings.HasSuffix(sv.OlderURL, "&q=hit-&before=2000") {
		t.Errorf("%d rows, %q, %q (%v)", len(sv.Rows), sv.Note, sv.OlderURL, err)
	}
}

func TestMetricRowsAreOrderedAndFolded(t *testing.T) {
	c := newTestConsole(t)
	t0 := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	mk := func(name, attrs string) MetricSeries {
		return MetricSeries{Name: "org.apache.kafka.producer." + name, Attrs: attrs, Unit: "1", Samples: []MetricSample{{T: t0, V: 1}, {T: t0.Add(5 * time.Second), V: 2}}}
	}
	in := ClientInstance{Series: []MetricSeries{
		mk("record.send.rate", ""), mk("connection.count", ""), mk("request.latency.avg", ""), mk("fetch.manager.bytes.consumed.rate", ""),
		mk("poll.idle.ratio.avg", ""), mk("zz.unknown", ""),
		mk("request.error.rate", "node=1"), mk("request.error.rate", "node=2"), mk("request.error.rate", "node=3"),
		mk("request.retry.rate", "node=1"), mk("request.retry.rate", "node=2"), mk("record.error.rate", ""),
	}}
	m := c.clientMetricsView(in, false)
	var rest []string
	for _, r := range m.Rest {
		rest = append(rest, r.Short)
	}
	if strings.Join(rest, " ") != "request.latency.avg poll.idle.ratio.avg connection.count fetch.manager.bytes.consumed.rate" {
		t.Errorf("the metrics the boards name come first, in their order: %v", rest)
	}
	if len(m.Worth) != 4 || m.More != (6-4)+(2) {
		t.Errorf("worth %d, more %d; want 4 shown of 6, and 2 more of the rest (6 others, 4 shown)", len(m.Worth), m.More)
	}
	all := c.clientMetricsView(in, true)
	if len(all.Worth) != 6 || len(all.Rest) != 6 || all.More != 0 {
		t.Errorf("all: %d + %d, more %d", len(all.Worth), len(all.Rest), all.More)
	}
}

func TestRequestInspectorLinksAndLabels(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{
		events: []TrafficEvent{
			{ID: "meta", Seq: 6, Conn: 1, First: now, Time: now, Count: 1, ClientID: "svc", Remote: "10.0.0.1:5", API: "Metadata", Version: 12, Kind: "metadata", Topics: []string{"orders"}},
			{ID: "nokind", Seq: 5, Conn: 1, First: now, Time: now, Count: 1, ClientID: "svc", Remote: "10.0.0.1:5", API: "Fetch", Version: 13, Kind: "fetch"},
			{ID: "closed", Seq: 4, Conn: 77, First: now, Time: now, Count: 1, ClientID: "svc", Remote: "10.0.0.1:5", API: "Produce", Version: 9, Kind: "produce", Topics: []string{"orders"}},
			{ID: "open", Seq: 3, Conn: 1, First: now, Time: now, Count: 1, ClientID: "svc", Remote: "10.0.0.1:5", API: "Produce", Version: 9, Kind: "produce", Topics: []string{"orders"}},
			{ID: "via", Seq: 2, Conn: 1, First: now, Time: now, Count: 1, ClientID: "svc", API: "Fetch", Version: 13, Kind: "fetch", ViaGroup: "g", Topics: []string{"orders"}},
			{ID: "own", Seq: 1, Conn: 1, First: now, Time: now, Count: 1, ClientID: "svc", API: "Heartbeat", Version: 4, Group: "g"},
		},
		clients: []TrafficClient{{Conn: 1, ClientID: "svc", Remote: "10.0.0.1:5"}},
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "orders", 1, 0)
	get := func(id string) string {
		rec := req(t, c, "GET", "/_console/inspect/request?key="+id, nil)
		if rec.Code != 200 {
			t.Fatalf("%s: %d", id, rec.Code)
		}
		return rec.Body.String()
	}
	open, closed := get("open"), get("closed")
	if !strings.Contains(open, `hx-get="/_console/inspect/client?key=1"`) || !strings.Contains(open, `href="/_console/traffic?client=svc"`) {
		t.Errorf("a request over an open connection links to its client and its requests:\n%s", readable(open))
	}
	if strings.Contains(closed, "/_console/inspect/client?key=77") || !strings.Contains(closed, `href="/_console/traffic?client=svc"`) {
		t.Errorf("a request over a closed connection offers its client, or does not link its requests")
	}
	if !strings.Contains(open, "Follow in Topics") || !strings.Contains(open, `href="/_console/topics/orders"`) {
		t.Error("a produce to a topic that is there does not offer to follow it")
	}
	if strings.Contains(get("meta"), "Follow in Topics") {
		t.Error("a Metadata request offers to follow a topic")
	}
	if body := get("nokind"); strings.Contains(body, "Follow in Topics") {
		t.Error("a fetch that names no topic offers to follow one")
	}
	if !strings.Contains(readable(get("via")), "(the group of its connection)") || strings.Contains(readable(get("own")), "(the group of its connection)") {
		t.Error("only a group borrowed from the connection says so")
	}
	if !strings.Contains(get("via"), `href="/_console/groups/g"`) {
		t.Error("no link to the group")
	}
}

func TestClientInspectorNamesAndPointsToOthers(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{clients: []TrafficClient{
		{Conn: 1, ClientID: "quiet", Remote: "10.0.0.1:1", Opened: now, Last: now},
		{Conn: 2, ClientID: "also-quiet", Remote: "10.0.0.2:2", Opened: now, Last: now},
		{Conn: 3, ClientID: "reporter", Remote: "10.0.0.3:3", Opened: now, Last: now},
		{Conn: 4, Remote: "10.0.0.4:4", Opened: now, Last: now},
		{Conn: 5, Remote: "10.0.0.5:5", Software: "librdkafka", Opened: now, Last: now},
		{Conn: 6, ClientID: "two", Remote: "10.0.0.6:6", Opened: now, Last: now},
		{Conn: 7, ClientID: "two", Remote: "10.0.0.7:7", Opened: now, Last: now},
	}}
	metrics := fakeMetrics{instances: []ClientInstance{{ID: "i", ClientID: "reporter", Conn: 3, Pushes: 2, First: now, Last: now}}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src; o.ClientMetrics = metrics })
	get := func(key string) string {
		return req(t, c, "GET", "/_console/inspect/client?key="+key, nil).Body.String()
	}
	// A client that does not report points to one that does, even when it is not the first.
	if body := get("2"); !strings.Contains(body, `hx-get="/_console/inspect/client?key=3"`) || !strings.Contains(body, "open=client:3") {
		t.Errorf("the client two rows down is not found:\n%s", readable(body))
	}
	if body := get("3"); strings.Contains(body, "See a client that reports") {
		t.Error("a client that reports points elsewhere")
	}
	// Without a client id, the software and the address say which; with neither, the address alone.
	if body := readable(get("5")); !strings.Contains(body, "librdkafka · 10.0.0.5:5") {
		t.Errorf("software and address:\n%s", body)
	}
	if body := readable(get("4")); strings.Contains(body, "10.0.0.4:4 · 10.0.0.4:4") || !strings.Contains(body, "10.0.0.4:4") {
		t.Errorf("address only:\n%s", body)
	}
	// Connections are listed only for a client that has several.
	if body := readable(get("6")); !strings.Contains(body, "Its 2 connections") || !strings.Contains(body, "10.0.0.7:7") {
		t.Errorf("two connections:\n%s", body)
	}
	if body := readable(get("1")); strings.Contains(body, "connections") && strings.Contains(body, "Its 1 connections") {
		t.Errorf("one connection is listed as several:\n%s", body)
	}
	if body := get("1"); strings.Contains(body, `id="client-connections"`) {
		t.Error("a list of connections for a client with one")
	}
}
