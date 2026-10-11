package console

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeTraffic is a TrafficSource with what a test puts in it.
type fakeTraffic struct {
	mu      sync.Mutex
	events  []TrafficEvent
	clients []TrafficClient
	groups  map[string][]TrafficGroupEvent
	story   TrafficStory
	asked   []TrafficFilter
}

func (f *fakeTraffic) Events(flt TrafficFilter, limit int) []TrafficEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, flt)
	var out []TrafficEvent
	for _, e := range f.events {
		if flt.Topic != "" && !slices.Contains(e.Topics, flt.Topic) {
			continue
		}
		if flt.ErrorsOnly && (e.Class == "" || e.Class == "expected") && !(e.Error != "" && e.Class == "") {
			continue
		}
		out = append(out, e)
	}
	return out
}
func (f *fakeTraffic) Row(id string) (TrafficEvent, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.events {
		if e.ID == id {
			return e, true
		}
	}
	return TrafficEvent{}, false
}

func (f *fakeTraffic) NewSince(flt TrafficFilter, t time.Time) (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, e := range f.events {
		if e.Time.After(t) {
			n += max(e.Count, 1)
		}
	}
	return n, false
}

func (f *fakeTraffic) Story(group string) TrafficStory {
	if f.story.Group == group {
		return f.story
	}
	return TrafficStory{Group: group}
}

func (f *fakeTraffic) Window(d time.Duration) TrafficWindow {
	f.mu.Lock()
	defer f.mu.Unlock()
	var w TrafficWindow
	for _, e := range f.events {
		if time.Since(e.Time) > d {
			continue
		}
		w.Requests += max(e.Count, 1)
		if e.Class != "" && e.Class != "expected" {
			w.Errors += max(e.Count, 1)
		}
	}
	return w
}

func (f *fakeTraffic) Refusals() []TrafficRefusal {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []TrafficRefusal
	for _, e := range f.events {
		if e.Class != "" && e.Class != "expected" {
			out = append(out, TrafficRefusal{Code: e.ErrorCode, Name: e.Error, Class: e.Class, Count: 1,
				Clients: []TrafficClientCount{{e.ClientID, 1}}, First: e.Time, Last: e.Time, Latest: e.ID, API: e.API, Topics: e.Topics})
		}
	}
	return out
}

func (f *fakeTraffic) Capacity() int    { return 2000 }
func (f *fakeTraffic) Since() time.Time { return tt0 }

func (f *fakeTraffic) Clients() []TrafficClient { return f.clients }
func (f *fakeTraffic) GroupEvents(g string, limit int) []TrafficGroupEvent {
	return f.groups[g]
}

var tt0 = time.Date(2026, 10, 10, 12, 30, 15, 250_000_000, time.Local)

func sampleTraffic() *fakeTraffic {
	return &fakeTraffic{
		events: []TrafficEvent{
			{ID: "r9", Seq: 9, First: tt0, Time: tt0.Add(5 * time.Second), Count: 4, ClientID: "billing", Remote: "127.0.0.1:50001", Principal: "User:ANONYMOUS",
				API: "Heartbeat", Version: 4, Group: "billing-workers", Duration: 800 * time.Microsecond, ReqBytes: 60, RespBytes: 12},
			{ID: "r8", Seq: 8, First: tt0, Time: tt0, Count: 1, ClientID: "billing", Software: "apache-kafka-java 3.7.0", Remote: "127.0.0.1:50001", Principal: "User:ANONYMOUS",
				API: "Produce", Version: 9, Topics: []string{"orders"}, Detail: "1 partition, 2.0 KiB", Duration: 12 * time.Millisecond, ReqBytes: 2100, RespBytes: 80},
			{ID: "r7", Seq: 7, First: tt0, Class: "refused", ErrorCode: 3, Time: tt0, Count: 1, ClientID: "audit", Remote: "127.0.0.1:50002", Principal: "User:ANONYMOUS",
				API: "Fetch", Version: 13, Topics: []string{"payments"}, Error: "UNKNOWN_TOPIC_OR_PARTITION", Duration: 2 * time.Millisecond, ReqBytes: 90, RespBytes: 50},
		},
		clients: []TrafficClient{{Conn: 1, Remote: "127.0.0.1:50001", ClientID: "billing", Software: "apache-kafka-java 3.7.0", Principal: "User:ANONYMOUS",
			Opened: time.Now().Add(-time.Minute), Last: time.Now().Add(-2 * time.Second), Requests: 41, Groups: []string{"billing-workers"}}},
		groups: map[string][]TrafficGroupEvent{
			"billing-workers": {
				{Seq: 1, Time: tt0, Kind: "joined", Member: "member-aaaaaaaaaaaaaaaaaaaa", Detail: "client billing"},
				{Seq: 2, Time: tt0.Add(time.Second), Kind: "generation", Member: "member-aaaaaaaaaaaaaaaaaaaa", Generation: 1, Detail: "1 member, leader member-a, protocol range"},
				{Seq: 3, Time: tt0.Add(40 * time.Second), Kind: "expired", Member: "member-aaaaaaaaaaaaaaaaaaaa", Detail: "client billing sent no heartbeat within its session timeout"},
			},
		},
	}
}

func TestTrafficPageShowsRequestsAndClients(t *testing.T) {
	src := sampleTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })

	rec := req(t, c, "GET", "/_console/traffic", nil)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := strings.ReplaceAll(rec.Body.String(), "<wbr>", "") // a result wraps at its underscores.
	for _, want := range []string{
		"Heartbeat", "v4", "×4", "billing-workers", // a folded row, with its count and its group.
		"Produce", "orders", "1 partition, 2.0 KiB", "12 ms",
		"UNKNOWN_TOPIC_OR_PARTITION", // the failed fetch, named.
		"apache-kafka-java 3.7.0",    // in the clients table.
		`id="clients-list"`, `id="traffic-table"`,
		`data-live="/_console/traffic/live"`, // the tail polls itself.
		`hx-get="/_console/inspect/request?key=`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the page does not show %q", want)
		}
	}
	// The rail offers it and marks it current.
	if !strings.Contains(body, `href="/_console/traffic" class="nav on"`) {
		t.Error("the rail does not mark Traffic as the current section")
	}
}

func TestTrafficFiltersReachTheSourceAndSurviveThePoll(t *testing.T) {
	src := sampleTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })

	rec := req(t, c, "GET", "/_console/traffic?topic=orders&errors=1&client=bill+ing", nil)
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	last := src.asked[len(src.asked)-1]
	if last.Topic != "orders" || !last.ErrorsOnly || last.Client != "bill ing" {
		t.Errorf("the source was asked for %+v", last)
	}
	body := rec.Body.String()
	// The poll URL carries the filter, or the first refresh would undo it.
	if !strings.Contains(body, `data-live="/_console/traffic/live?client=bill%20ing&amp;topic=orders&amp;errors=1"`) {
		t.Errorf("the live URL loses the filter:\n%s", between(body, `data-live=`, ` data-hash`))
	}
	// Errors-only over this fake leaves nothing matching topic=orders.
	if !strings.Contains(body, "No request matches the filter") {
		t.Error("an empty result for a filter does not say so")
	}
}

func between(s, a, b string) string {
	i := strings.Index(s, a)
	if i < 0 {
		return ""
	}
	j := strings.Index(s[i:], b)
	if j < 0 {
		return s[i:]
	}
	return s[i : i+j]
}

// The poll answers 204 while nothing moved, and a full region once something did.
func TestTrafficLiveOnlyAnswersWhenSomethingChanged(t *testing.T) {
	src := sampleTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })

	first := req(t, c, "GET", "/_console/traffic/live", nil)
	if first.Code != 200 || !strings.Contains(first.Body.String(), `id="traffic-live"`) {
		t.Fatalf("live: %d %s", first.Code, first.Body)
	}
	hash := first.Header().Get("HX-Live-Hash")
	if hash == "" {
		t.Fatal("no hash to poll with")
	}
	if rec := req(t, c, "GET", "/_console/traffic/live?h="+hash, nil); rec.Code != 204 {
		t.Errorf("nothing changed, but the poll answered %d", rec.Code)
	}
	src.mu.Lock()
	src.events = append([]TrafficEvent{{Seq: 10, Time: tt0, Count: 1, ClientID: "billing", API: "Metadata", Version: 12, Duration: time.Millisecond}}, src.events...)
	src.mu.Unlock()
	rec := req(t, c, "GET", "/_console/traffic/live?h="+hash, nil)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "Metadata") {
		t.Errorf("a new request did not refresh the region: %d", rec.Code)
	}
	// A folded row that gained a repeat is also a change: same row, new count.
	src.mu.Lock()
	src.events[0].Count = 11
	src.events[0].Seq = 11
	src.mu.Unlock()
	h2 := rec.Header().Get("HX-Live-Hash")
	if rec := req(t, c, "GET", "/_console/traffic/live?h="+h2, nil); rec.Code != 200 {
		t.Errorf("a repeat folded into a row did not refresh the region: %d", rec.Code)
	}
}

func TestTrafficInspectorShowsOneRequestAndSaysWhenItMovedOn(t *testing.T) {
	src := sampleTraffic()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })

	rec := req(t, c, "GET", "/_console/traffic/request?seq=8", nil)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{"Produce v9", "billing", "apache-kafka-java 3.7.0", "127.0.0.1:50001", "User:ANONYMOUS", "orders", "1 partition, 2.0 KiB", "12 ms", "2.1 KB in"} {
		if !strings.Contains(body, want) {
			t.Errorf("the inspector is missing %q:\n%s", want, body)
		}
	}
	if gone := req(t, c, "GET", "/_console/traffic/request?seq=999", nil); gone.Code != 200 || !strings.Contains(gone.Body.String(), "aged out") {
		t.Errorf("a vanished row: %d %s", gone.Code, gone.Body)
	}
	if bad := req(t, c, "GET", "/_console/traffic/request?seq=abc", nil); bad.Code != 400 {
		t.Errorf("a non-numeric seq answered %d, want 400", bad.Code)
	}
}

func TestGroupPageShowsItsTimelineNewestFirst(t *testing.T) {
	src := sampleTraffic()
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "orders", 1, 1)
	commitFor(t, addr, "billing-workers", "orders", map[int32]int64{0: 1})

	rec := req(t, c, "GET", "/_console/groups/billing-workers?tab=timeline", nil)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{`id="group-timeline"`, "session expired", "new generation", "gen 1", "no heartbeat within its session timeout"} {
		if !strings.Contains(body, want) {
			t.Errorf("the group page's timeline is missing %q", want)
		}
	}
	expired, generation, joined := strings.Index(body, "session expired"), strings.Index(body, "new generation"), strings.Index(body, ">joined<")
	if !(expired > 0 && expired < generation && generation < joined) {
		t.Errorf("not newest first: expired@%d generation@%d joined@%d", expired, generation, joined)
	}

	// A group the coordinator has said nothing about says so, rather than showing a blank.
	commitFor(t, addr, "quiet", "orders", map[int32]int64{0: 1})
	if q := req(t, c, "GET", "/_console/groups/quiet?tab=timeline", nil); !strings.Contains(q.Body.String(), "Nothing has happened in this group since the broker started") {
		t.Error("a group with no events does not say so")
	}
}

// With nothing to show it from, the console does not pretend: no rail entry, no page.
func TestWithoutATrafficSourceThereIsNoTrafficSection(t *testing.T) {
	c := newTestConsole(t)
	home := req(t, c, "GET", "/_console/topics", nil)
	if strings.Contains(home.Body.String(), "/_console/traffic") {
		t.Error("the rail offers Traffic with nothing behind it")
	}
	for _, p := range []string{"/_console/traffic", "/_console/traffic/live", "/_console/traffic/request?seq=1"} {
		if rec := req(t, c, "GET", p, nil); rec.Code != 404 {
			t.Errorf("GET %s = %d, want 404", p, rec.Code)
		}
	}
	if rec := req(t, c, "GET", "/_console/api/palette", nil); strings.Contains(rec.Body.String(), "Traffic") {
		t.Error("the palette offers Traffic with nothing behind it")
	}
}

func TestShortDuration(t *testing.T) {
	for in, want := range map[time.Duration]string{
		400 * time.Microsecond: "0.4 ms", 12 * time.Millisecond: "12 ms", 2500 * time.Millisecond: "2.5 s",
		190 * time.Second: "3m10s", 0: "0 µs",
		// Under a tenth of a millisecond is microseconds, never "0.0 ms".
		42 * time.Microsecond: "42 µs", 49 * time.Microsecond: "49 µs", 99*time.Microsecond + 900*time.Nanosecond: "99 µs",
		100 * time.Microsecond: "0.1 ms", 960 * time.Microsecond: "1.0 ms", -3 * time.Microsecond: "0 µs",
		time.Millisecond: "1 ms", 999 * time.Millisecond: "999 ms",
	} {
		if got := shortDuration(in); got != want {
			t.Errorf("shortDuration(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestSpanText(t *testing.T) {
	for in, want := range map[time.Duration]string{
		0: "just now", time.Second: "just now", 12 * time.Second: "12s", 190 * time.Second: "3m 10s", 125 * time.Minute: "2h 05m",
		// A record's age in the table is a span too: a day or more counts in days.
		47 * time.Hour: "47h 00m", 48 * time.Hour: "2d 00h", 77 * time.Hour: "3d 05h",
	} {
		if got := spanText(in); got != want {
			t.Errorf("spanText(%v) = %q, want %q", in, got, want)
		}
	}
}

// ---- client metrics (KIP-714) ----

type fakeMetrics struct{ instances []ClientInstance }

func (f fakeMetrics) Instances() []ClientInstance { return f.instances }

func ramp(n int, from, step float64) []MetricSample {
	out := make([]MetricSample, n)
	for i := range out {
		out[i] = MetricSample{T: tt0.Add(time.Duration(i) * 5 * time.Second), V: from + float64(i)*step}
	}
	return out
}

func sampleMetrics() fakeMetrics {
	// The ones worth looking at come LAST, so only sorting puts them first.
	var series []MetricSeries
	for i := 0; i < 10; i++ { // enough to need the fold.
		series = append(series, MetricSeries{Name: fmt.Sprintf("org.apache.kafka.producer.filler.%02d", i), Kind: "gauge", Samples: ramp(5, 1, 1)})
	}
	series = append(series,
		MetricSeries{Name: "org.apache.kafka.producer.node.request.latency.avg", Attrs: "node_id=1", Unit: "ms", Kind: "gauge", Samples: ramp(20, 4, 0.5)},
		MetricSeries{Name: "org.apache.kafka.producer.connection.creation.total", Kind: "sum", Samples: ramp(20, 3, 0)},
		MetricSeries{Name: "org.apache.kafka.producer.somethingelse.total", Kind: "sum", Samples: ramp(3, 1000, 1000)},
	)
	return fakeMetrics{[]ClientInstance{
		{ID: "0d7a8e3c-1b1e-4c2f-9a77-3b7c2f6a9d10", Conn: 1, ClientID: "billing", Software: "apache-kafka-java 3.7.1", Principal: "User:ANONYMOUS", Host: "/127.0.0.1",
			First: tt0, Last: time.Now(), Pushes: 20, Resource: map[string]string{"client_rack": "r1"}, Series: series},
		{ID: "11111111-2222-4333-8444-555555555555", ClientID: "audit", Last: time.Now(), Pushes: 1, Terminated: true},
	}}
}

// A client's own metrics are the client's page (Traf-07), not a section under the
// requests: the inspector says what they are and puts the ones worth a look first.
func TestClientInspectorShowsWhatItReportsAboutItself(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = sampleTraffic(); o.ClientMetrics = sampleMetrics() })
	body := req(t, c, "GET", "/_console/inspect/client?key=1", nil).Body.String()
	for _, want := range []string{
		"Metrics reported by the client itself", "metrics, not traces", "20 reports", "last report",
		"Request latency, average", "node_id=1", // the label a person writes, and the attributes it is for.
		"13.5 ms", // 4 + 19*0.5, with its unit and without float noise.
		"Sparklines cover", "one point per report",
		"more metrics are in the full list",
		"client_rack=r1", // what the client said about itself.
		"connected",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the client's inspector does not show %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "one bar per report") {
		t.Error("the copy says bars; the chart is a line of points")
	}
	if n := strings.Count(body, `<polyline points=`); n < 1 {
		t.Errorf("%d sparklines drawn", n)
	}
	// The interesting ones come first, and the rest fold away until asked for.
	if strings.Contains(body, "producer.filler.09") {
		t.Error("the filler is not folded away")
	}
	all := req(t, c, "GET", "/_console/inspect/client?key=1&all=1", nil).Body.String()
	if !strings.Contains(all, "producer.filler.09") || strings.Contains(all, "more metrics are in the full list") {
		t.Error("asking for all of them does not list them all")
	}
}

func TestClientWithoutMetricsSaysWhyAndWhatToFlip(t *testing.T) {
	src := sampleTraffic()
	src.clients = append(src.clients, TrafficClient{Conn: 2, ClientID: "seed-loader", Software: "kgo", Principal: "User:ANONYMOUS", Remote: "127.0.0.1:55190",
		Opened: time.Now().Add(-9 * time.Second), Last: time.Now(), Requests: 3})
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src; o.ClientMetrics = sampleMetrics() })
	body := req(t, c, "GET", "/_console/inspect/client?key=2", nil).Body.String()
	for _, want := range []string{"This client does not report metrics", "The client is too old", "The client turned it off", "enable.metrics.push=", "--client-telemetry", "See a client that reports"} {
		if !strings.Contains(body, want) {
			t.Errorf("the no-metrics inspector does not show %q", want)
		}
	}
	// With telemetry off on the broker it says that instead of blaming the client.
	c2, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	if b := req(t, c2, "GET", "/_console/inspect/client?key=2", nil).Body.String(); strings.Contains(b, "does not report metrics") || !strings.Contains(b, "does not collect client metrics") {
		t.Errorf("without a store the page still blames the client:\n%s", b)
	}
}

// The requests page no longer carries a section of metric cards: the open connections
// are the inspector's default, with the sentence that points at a client.
func TestTrafficPageHasNoMetricCardsAndPointsAtTheClient(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = sampleTraffic(); o.ClientMetrics = sampleMetrics() })
	body := req(t, c, "GET", "/_console/traffic", nil).Body.String()
	if strings.Contains(body, `class="mcard"`) {
		t.Error("the page still draws metric cards under the requests")
	}
	if !strings.Contains(body, "Open a client to see them.") {
		t.Error("the connected clients do not say where the metrics are")
	}
}

// A new report changes nothing in the request region: the metrics are not part of it.
func TestClientMetricsDoNotRefreshTheRequestRegion(t *testing.T) {
	src := sampleMetrics()
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = sampleTraffic(); o.ClientMetrics = src })
	first := req(t, c, "GET", "/_console/traffic/live", nil)
	hash := first.Header().Get("HX-Live-Hash")
	src.instances[0].Pushes++
	if rec := req(t, c, "GET", "/_console/traffic/live?h="+hash, nil); rec.Code != 204 {
		t.Errorf("a metrics report refreshed the whole request region: %d", rec.Code)
	}
}

func TestFormatMetric(t *testing.T) {
	for _, tc := range []struct {
		v    float64
		unit string
		want string
	}{
		{12.500000000000002, "ms", "12.5 ms"}, {0.043123, "1", "0.0431"}, {8812.4, "", "8,812"}, {0, "ms", "0 ms"},
		{250, "B", "250 B"}, {3, "1", "3"}, {1.0, "", "1"}, {math.NaN(), "", "—"}, {-4.25, "ms", "-4.25 ms"},
	} {
		if got := formatMetric(tc.v, tc.unit); got != tc.want {
			t.Errorf("formatMetric(%v, %q) = %q, want %q", tc.v, tc.unit, got, tc.want)
		}
	}
}

func TestSparklineScalesTheLastSamplesAndDrawsNothingForOne(t *testing.T) {
	if got := sparkline(ramp(1, 1, 1)); got != "" {
		t.Errorf("one sample drew %q", got)
	}
	flat := string(sparkline(ramp(5, 7, 0)))
	if !strings.Contains(flat, `points="1.0,12.0 `) {
		t.Errorf("a flat series is not drawn along the middle: %s", flat)
	}
	rising := string(sparkline(ramp(5, 0, 1)))
	// Oldest at the left and the bottom, newest at the right and the top.
	if !strings.HasPrefix(between(rising, `points="`, `"/>`), `points="1.0,22.0 `) || !strings.HasSuffix(between(rising, `points="`, `"/>`), `119.0,2.0`) {
		t.Errorf("a rising series does not run bottom-left to top-right: %s", rising)
	}
	long := string(sparkline(ramp(200, 0, 1)))
	if n := strings.Count(between(long, `points="`, `"/>`), ","); n != sparkPoints {
		t.Errorf("%d points drawn for 200 samples, want the last %d", n, sparkPoints)
	}
	if !strings.Contains(long, "the last 60 samples, from 140 to 199") {
		t.Errorf("the chart's description is wrong: %s", long)
	}
}

func TestMetricRankPutsWhatAnswersIsThisClientHealthyFirst(t *testing.T) {
	order := []string{
		"org.apache.kafka.producer.node.request.latency.avg",
		"org.apache.kafka.producer.record.send.rate",
		"org.apache.kafka.producer.record.error.rate", // zero: good news, after the throughput.
		"org.apache.kafka.consumer.poll.idle.ratio.avg",
		"org.apache.kafka.producer.connection.count",
		"org.apache.kafka.producer.bufferpool.wait.ratio", // not in the list: last.
	}
	for i := 1; i < len(order); i++ {
		if metricRank(order[i-1], 0) >= metricRank(order[i], 0) {
			t.Errorf("%s (%d) should rank before %s (%d)", order[i-1], metricRank(order[i-1], 0), order[i], metricRank(order[i], 0))
		}
	}
	if metricRank("org.apache.kafka.producer.batch.split.rate", 0) != metricRank("org.apache.kafka.producer.waiting.threads", 0) {
		t.Error("metrics outside the list do not share the last place")
	}
	// An error or retry rate above zero is urgent, wherever it would otherwise sit.
	if metricRank("org.apache.kafka.producer.record.error.rate", 0.5) >= metricRank("org.apache.kafka.producer.node.request.latency.avg", 99) {
		t.Error("a non-zero error rate does not come first")
	}
	if metricRank("org.apache.kafka.producer.topic.record.retry.rate", 2) >= 0 {
		t.Error("a non-zero retry rate does not come first")
	}
}
