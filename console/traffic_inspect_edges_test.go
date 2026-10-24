package console

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestRequestExplainByClass(t *testing.T) {
	c := newTestConsole(t)
	if c.requestExplain(TrafficEvent{}, false) != nil {
		t.Error("an explanation of a request that went well")
	}
	for class, want := range map[string]string{
		"expected": "This is part of the protocol's normal flow, not a problem.", "refused": "Real Kafka would refuse the same call.",
		"denied": "An ACL or a login decided this.", "not-implemented": "This broker does not do this.", "failed": "The broker could not do this; its log says why.", "": "",
	} {
		v := c.requestExplain(TrafficEvent{Error: "X", ErrorCode: 3, Class: class}, false)
		if v.Verdict != want || v.Name != "X" || v.Word != classWord(class) || v.Tone != resultTone(class) || v.Closed != "" || v.Meaning == "" {
			t.Errorf("%q: %+v", class, v)
		}
	}
	closed := c.requestExplain(TrafficEvent{Error: "CONNECTION_CLOSED", Class: "refused", Closed: "the frame was larger than allowed"}, false)
	if closed.Word != "Failed" || closed.Tone != "bad" || closed.Cause != "the frame was larger than allowed." || closed.Closed == "" || closed.Meaning != "The broker closed the connection because of this request." || closed.Fix != "" || closed.Verdict != "" {
		t.Errorf("%+v", closed)
	}
	// Unknown topic: what to do depends on what the broker does about auto-create.
	on := c.requestExplain(TrafficEvent{Error: "U", ErrorCode: 3, Class: "refused"}, true)
	off := c.requestExplain(TrafficEvent{Error: "U", ErrorCode: 3, Class: "refused"}, false)
	if on.Fix == "" || off.Fix == "" {
		t.Errorf("no fix: %q / %q", on.Fix, off.Fix)
	}
}

func TestFoldViewAndRepeatUnits(t *testing.T) {
	c := newTestConsole(t)
	t0 := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for api, want := range map[string]string{"Heartbeat": "heartbeat", "ConsumerGroupHeartbeat": "heartbeat", "Fetch": "fetch", "PushTelemetry": "report", "OffsetFetch": "offset fetch", "Produce": "request"} {
		if got := repeatUnit(api); got != want {
			t.Errorf("repeatUnit(%q) = %q", api, got)
		}
	}
	ticks := []TrafficTick{{At: t0, Duration: 2 * time.Millisecond}, {At: t0.Add(3 * time.Second), Duration: 3 * time.Millisecond}}
	e := TrafficEvent{API: "Heartbeat", Detail: "generation 4", Count: 9, First: t0, Time: t0.Add(24 * time.Second), Duration: time.Millisecond, Ticks: ticks}
	f := c.foldView(e, true)
	if f.Count != 9 || f.Interval != "every 3 s" || f.Unit != "heartbeat" || !f.Unfold || f.Each != "ok, 1 ms each" || f.Kept != 2 || len(f.Ticks) != 2 ||
		f.SameLabel != "Generation" || f.Same != "generation 4 throughout" || f.Note != "Each tick is one heartbeat: the last 2 of 9" {
		t.Errorf("%+v", f)
	}
	if f.Ticks[0].Took != "2 ms" || f.Ticks[0].Title != "12:00:00.000 UTC · 2 ms" {
		t.Errorf("tick %+v", f.Ticks[0])
	}
	e.Detail, e.Count = "epoch 7", 2
	f = c.foldView(e, false)
	if f.SameLabel != "Epoch" || f.Same != "epoch 7 throughout" || f.Unfold || f.Note != "Each tick is one heartbeat" {
		t.Errorf("%+v", f)
	}
	e.Detail, e.Ticks = "wait 500 ms", nil
	f = c.foldView(e, false)
	if f.SameLabel != "" || f.Same != "" || f.Kept != 0 || f.Note != "Each tick is one heartbeat" {
		t.Errorf("no ticks: %+v", f)
	}
	e.Count, e.Ticks = 1500, ticks
	if f = c.foldView(e, false); f.Note != "Each tick is one heartbeat: the last 2 of 1,500" {
		t.Errorf("%q", f.Note)
	}
}

func TestDecodedViewAndSmallInspectorText(t *testing.T) {
	if h, raw := decodedView(""); h != "" || raw != "" {
		t.Error("nothing decoded")
	}
	h, raw := decodedView(`{"a":[1,2]}`)
	if raw != "{\n  \"a\": [\n    1,\n    2\n  ]\n}" || !strings.Contains(string(h), `class="jt-k"`) {
		t.Errorf("%q %q", raw, h)
	}
	if h, raw = decodedView("not json <b>"); raw != "not json <b>" || strings.Contains(string(h), "<b>") {
		t.Errorf("%q %q", raw, h)
	}
	for a, want := range map[int]string{-1: "-1 (all)", 0: "0 (none)", 1: "1 (leader)", 2: "2", -2: "-2"} {
		if got := acksText(a); got != want {
			t.Errorf("acksText(%d) = %q", a, got)
		}
	}
	if plural2(1, "record") != "1 record" || plural2(0, "record") != "0 records" || plural2(7, "record") != "7 records" {
		t.Error("plural2")
	}
	if got := string(requestTitle(TrafficEvent{API: "Fetch<x>", Version: 13})); got != `Fetch&lt;x&gt; <span class="mut">v13</span>` {
		t.Errorf("%q", got)
	}
	if got := string(requestTitle(TrafficEvent{API: "Produce", Version: 9, Count: 4})); got != `Produce <span class="mut">v9</span> <span class="tag">×4</span>` {
		t.Errorf("%q", got)
	}
	if got := string(requestTitle(TrafficEvent{API: "Produce", Version: 9, Count: 1})); strings.Contains(got, "tag") {
		t.Errorf("%q", got)
	}
	if firstNonEmpty() != "" || firstNonEmpty("", "b", "c") != "b" || firstNonEmpty("a", "b") != "a" || joinNonEmpty("-", "", "a", "", "b") != "a-b" || joinNonEmpty("-") != "" {
		t.Error("string helpers")
	}
}

func TestPartViews(t *testing.T) {
	c := newTestConsole(t)
	got := c.partViews(TrafficFacts{Partitions: []TrafficPart{
		{Topic: "orders", Partition: 2, HasOffset: true, Offset: 14407, Records: 1},
		{Topic: "orders", Partition: 3, Records: 5, ErrorCode: 3},
		{Topic: "", Partition: 0, HasOffset: true, Offset: 0},
	}})
	if len(got) != 3 {
		t.Fatalf("%d", len(got))
	}
	if p := got[0]; p.Topic != "orders" || p.TopicURL != "/_console/topics/orders" || p.Offset != "14,407" || p.Records != "1 record" || p.Result != "ok" || p.Tone != "ok" || p.Partition != 2 {
		t.Errorf("%+v", p)
	}
	if p := got[1]; p.Offset != "" || p.Records != "5 records" || p.Result != "UNKNOWN_TOPIC_OR_PARTITION" || p.Tone != "deny" {
		t.Errorf("%+v", p)
	}
	if p := got[2]; p.TopicURL != "" || p.Offset != "0" || p.Records != "" {
		t.Errorf("%+v", p)
	}
}

func TestMetricNamesOrderAndValues(t *testing.T) {
	for in, want := range map[string]string{
		"org.apache.kafka.producer.request.latency.avg": "request.latency.avg", "org.apache.kafka.consumer.connection.count": "connection.count",
		"org.apache.kafka.client.foo": "foo", "org.apache.kafka.bar": "bar", "other.name": "other.name",
	} {
		if got := metricShort(in); got != want {
			t.Errorf("metricShort(%q) = %q", in, got)
		}
	}
	if metricLabel("org.apache.kafka.producer.request.latency.avg") != "Request latency, average" || metricLabel("unknown.metric") != "" {
		t.Error("metricLabel")
	}
	for _, n := range []string{"org.apache.kafka.producer.request.error.rate", "request.retry.rate", "record.error.rate", "org.apache.kafka.producer.record.retry.rate"} {
		if !worthALook(n) {
			t.Errorf("%s is not worth a look", n)
		}
	}
	if worthALook("request.latency.avg") || worthALook("my.request.error.rate.custom") {
		t.Error("worthALook is too generous")
	}
	if metricRank("x.error.rate", 1) != -1 || metricRank("x.retry.rate", 0.5) != -1 || metricRank("x.error.rate", 0) != 6 && metricRank("x.error.rate", 0) != len(metricPriority) {
		t.Errorf("ranks %d %d %d", metricRank("x.error.rate", 1), metricRank("x.retry.rate", 0.5), metricRank("x.error.rate", 0))
	}
	if metricRank("request.latency.avg", 5) != 0 || metricRank("nothing.known", 0) != len(metricPriority) || metricRank("connection.count", 3) != 16 {
		t.Errorf("ranks %d %d %d", metricRank("request.latency.avg", 5), metricRank("nothing.known", 0), metricRank("connection.count", 3))
	}
	for _, tc := range []struct {
		v    float64
		unit string
		want string
	}{
		{1.4, "ms", "1.4 ms"}, {0, "ms", "0 ms"}, {0.94, "", "0.94"}, {1, "1", "1"}, {0, "1/s", "0 /s"}, {12.5, "1/s", "12.5 /s"},
		{1234, "By/s", "1.2 KiB/s"}, {500, "By", "500 B"}, {1536, "By", "1.5 KiB"}, {math.NaN(), "By/s", "—"}, {math.Inf(1), "By", "—"}, {2 << 20, "By/s", "2.0 MiB/s"},
	} {
		if got := metricValue(tc.v, tc.unit); got != tc.want {
			t.Errorf("metricValue(%v, %q) = %q, want %q", tc.v, tc.unit, got, tc.want)
		}
	}
	for _, tc := range []struct {
		v    float64
		unit string
		want string
	}{
		{1234.4, "", "1,234"}, {100, "", "100"}, {99.5, "", "99.5"}, {1, "", "1"}, {0.043, "", "0.043"}, {0, "", "0"}, {math.NaN(), "", "—"}, {12.5, "ms", "12.5 ms"}, {1000, "", "1,000"}, {999.9, "", "1000"},
	} {
		if got := formatMetric(tc.v, tc.unit); got != tc.want {
			t.Errorf("formatMetric(%v, %q) = %q, want %q", tc.v, tc.unit, got, tc.want)
		}
	}
}

func TestSparkBars(t *testing.T) {
	at := time.Now()
	mk := func(vs ...float64) []MetricSample {
		var out []MetricSample
		for i, v := range vs {
			out = append(out, MetricSample{T: at.Add(time.Duration(i) * time.Second), V: v})
		}
		return out
	}
	bars := func(vs ...float64) string {
		h := string(sparkBarsHTML(mk(vs...)))
		var out []string
		for _, p := range strings.Split(h, `style="height: `)[1:] {
			out = append(out, p[:strings.Index(p, "px")])
		}
		return strings.Join(out, ",")
	}
	if sparkBarsHTML(nil) != "" {
		t.Error("bars of nothing")
	}
	if got := bars(0, 0); got != "2,2" {
		t.Errorf("all zero: %s", got)
	}
	if got := bars(0, 5, 10); got != "2,9,16" {
		t.Errorf("0, 5, 10: %s", got)
	}
	if got := bars(-10, 10); got != "16,16" {
		t.Errorf("negative values are drawn by size: %s", got)
	}
	if got := bars(math.NaN(), 4, math.Inf(1)); got != "2,16,2" {
		t.Errorf("NaN and Inf: %s", got)
	}
	long := make([]float64, 20)
	for i := range long {
		long[i] = float64(i + 1)
	}
	got := bars(long...)
	if n := len(strings.Split(got, ",")); n != 12 || !strings.HasSuffix(got, ",16") {
		t.Errorf("twenty samples draw the last twelve: %s", got)
	}
	if !strings.HasPrefix(string(sparkBarsHTML(mk(1))), `<span class="spark">`) || !strings.HasSuffix(string(sparkBarsHTML(mk(1))), `</span>`) {
		t.Error("the wrapper")
	}
}

func TestInstanceForPicksTheReportingConnection(t *testing.T) {
	ins := []ClientInstance{
		{ID: "silent", ClientID: "app", Conn: 7, Pushes: 0},
		{ID: "other-principal", ClientID: "app", Principal: "User:other", Conn: 8, Pushes: 2},
		{ID: "same-client", ClientID: "app", Principal: "User:me", Conn: 9, Pushes: 3},
		{ID: "anon", ClientID: "app", Conn: 10, Pushes: 1},
		{ID: "exact", ClientID: "app", Principal: "User:me", Conn: 5, Pushes: 4},
	}
	if got, ok := instanceFor(TrafficClient{Conn: 5, ClientID: "app", Principal: "User:me"}, ins); !ok || got.ID != "exact" {
		t.Errorf("the connection it pushed over: %+v", got)
	}
	if got, ok := instanceFor(TrafficClient{Conn: 99, ClientID: "app", Principal: "User:me"}, ins); !ok || got.ID != "same-client" {
		t.Errorf("same client and principal: %+v", got)
	}
	if got, ok := instanceFor(TrafficClient{Conn: 99, ClientID: "app", Principal: "User:zed"}, ins); !ok || got.ID != "anon" {
		t.Errorf("a push with no principal fits any: %+v", got)
	}
	if _, ok := instanceFor(TrafficClient{Conn: 99, ClientID: "nobody"}, ins); ok {
		t.Error("a client that has not reported")
	}
	if _, ok := instanceFor(TrafficClient{Conn: 7, ClientID: ""}, ins); ok {
		t.Error("a silent instance counts as reporting")
	}
	if _, ok := instanceFor(TrafficClient{Conn: 0, ClientID: ""}, []ClientInstance{{Conn: 0, Pushes: 1}}); ok {
		t.Error("connection 0 matches connection 0")
	}
	var none []ClientInstance
	if _, ok := instanceFor(TrafficClient{Conn: 1}, none); ok {
		t.Error("no instances")
	}
}

func TestClientMetricsView(t *testing.T) {
	c := newTestConsole(t)
	t0 := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	samples := func(n int, v float64) []MetricSample {
		var out []MetricSample
		for i := 0; i < n; i++ {
			out = append(out, MetricSample{T: t0.Add(time.Duration(i*5) * time.Second), V: v})
		}
		return out
	}
	series := func(name string, n int, v float64) MetricSeries {
		return MetricSeries{Name: "org.apache.kafka.producer." + name, Unit: "1", Samples: samples(n, v)}
	}
	in := ClientInstance{ID: "i", Pushes: 5, First: t0, Last: t0.Add(20 * time.Second), Dropped: 1, Skipped: 2, Terminated: true,
		Series: []MetricSeries{
			series("record.error.rate", 13, 0.2), series("request.error.rate", 13, 0), series("record.retry.rate", 3, 0), series("request.retry.rate", 3, 0),
			series("zz.one", 2, 1), series("aa.two", 2, 2), series("request.latency.avg", 2, 1.5), series("poll.idle.ratio.avg", 2, 0.9), series("connection.count", 2, 2),
			series("fetch.manager.bytes.consumed.rate", 2, 10), series("b.three", 2, 3), series("c.four", 2, 4),
		}}
	m := c.clientMetricsView(in, false)
	if m.Reports != 5 || m.Metrics != 12 || m.Dropped != 1 || m.Skipped != 2 || !m.Terminated || m.All || m.Every != "5 s" {
		t.Errorf("%+v", m)
	}
	var worth []string
	for _, r := range m.Worth {
		worth = append(worth, r.Short+":"+r.Tone)
	}
	if strings.Join(worth, " ") != "request.error.rate:ok request.retry.rate:ok record.error.rate:warn record.retry.rate:ok" {
		t.Errorf("worth a look: %v", worth)
	}
	var rest []string
	for _, r := range m.Rest {
		rest = append(rest, r.Short)
	}
	if strings.Join(rest, " ") != "request.latency.avg poll.idle.ratio.avg connection.count fetch.manager.bytes.consumed.rate" {
		t.Errorf("the rest: %v", rest)
	}
	if m.More != 4 {
		t.Errorf("more: %d", m.More)
	}
	if m.Worth[0].Last != "0" || m.Worth[2].Last != "0.2" || m.Worth[2].N != 13 || m.Worth[2].Label != "Record error rate" || m.Rest[0].Label != "Request latency, average" || !strings.Contains(string(m.Worth[2].Spark), "spark") {
		t.Errorf("rows %+v", m.Worth[2])
	}
	// The bars cover the last twelve reports of the longest series: 11 gaps of 5 s.
	if m.Span != "the last 55 seconds" && m.Span != "the last minute" {
		t.Errorf("span %q", m.Span)
	}
	all := c.clientMetricsView(in, true)
	if len(all.Worth) != 4 || len(all.Rest) != 8 || all.More != 0 || !all.All {
		t.Errorf("all: worth %d rest %d more %d", len(all.Worth), len(all.Rest), all.More)
	}
	// More worth-a-look rows than fit: the surplus is counted, not dropped silently.
	many := ClientInstance{Series: []MetricSeries{series("request.error.rate", 2, 0), series("request.retry.rate", 2, 0), series("record.error.rate", 2, 0), series("record.retry.rate", 2, 0)}}
	many.Series = append(many.Series, MetricSeries{Name: "x.request.error.rate", Samples: samples(2, 0)})
	if mm := c.clientMetricsView(many, false); len(mm.Worth) != 4 || mm.More != 0 {
		t.Errorf("four worth a look: %d, more %d", len(mm.Worth), mm.More)
	}
	// Span wording and the cadence.
	spanOf := func(secs int) string {
		in := ClientInstance{Series: []MetricSeries{{Name: "x", Samples: []MetricSample{{T: t0}, {T: t0.Add(time.Duration(secs) * time.Second)}}}}}
		return c.clientMetricsView(in, false).Span
	}
	for secs, want := range map[int]string{0: "", 1: "the last 1 seconds", 25: "the last 25 seconds", 54: "the last 54 seconds", 55: "the last minute", 65: "the last minute", 66: "the last 66 seconds", 119: "the last 119 seconds", 120: "the last 2 minutes", 300: "the last 5 minutes"} {
		if got := spanOf(secs); got != want {
			t.Errorf("span of %d s: %q, want %q", secs, got, want)
		}
	}
	every := func(pushes int, secs int) string {
		return c.clientMetricsView(ClientInstance{Pushes: pushes, First: t0, Last: t0.Add(time.Duration(secs) * time.Second)}, false).Every
	}
	if every(1, 10) != "" || every(2, 0) != "" || every(2, 10) != "10 s" || every(3, 10) != "5 s" {
		t.Errorf("cadence: %q %q %q %q", every(1, 10), every(2, 0), every(2, 10), every(3, 10))
	}
	if got := c.clientMetricsView(ClientInstance{Metrics: 7, Series: make([]MetricSeries, 2)}, false).Metrics; got != 7 {
		t.Errorf("metrics %d", got)
	}
	if got := c.clientMetricsView(ClientInstance{Series: make([]MetricSeries, 2)}, false).Metrics; got != 2 {
		t.Errorf("metrics %d", got)
	}
}

func TestInspectRequestPage(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{
		events: []TrafficEvent{
			{ID: "r5", Seq: 5, Conn: 1, First: now, Time: now, Count: 1, ClientID: "svc", API: "Produce", Version: 9, Kind: "produce", Topics: []string{"orders"}, Duration: 12 * time.Millisecond,
				Correlation: 4812, Facts: TrafficFacts{HasAcks: true, Acks: -1, Decoded: `{"a":1}`, PartitionsMore: 3, Partitions: []TrafficPart{{Topic: "orders", Partition: 0, HasOffset: true, Offset: 1, Records: 2}}}},
			{ID: "r4", Seq: 4, Conn: 8, First: now.Add(-time.Minute), Time: now, Count: 6, ClientID: "svc", API: "Heartbeat", Version: 4, Group: "g", Detail: "generation 3", Duration: time.Millisecond},
			{ID: "r3", Seq: 3, Conn: 8, First: now, Time: now, Count: 1, ClientID: "svc", API: "OffsetCommit", Version: 8, Group: "g", Topics: []string{"orders"}, Parts: "p0–p1"},
			{ID: "r2", Seq: 2, Conn: 8, First: now, Time: now, Count: 1, API: "Fetch", Version: 13, Kind: "fetch", ViaGroup: "g", Remote: "10.0.0.5:99", Topics: []string{"gone"}, Class: "refused", Error: "UNKNOWN_TOPIC_OR_PARTITION", ErrorCode: 3},
		},
		clients: []TrafficClient{{Conn: 1, ClientID: "svc", Remote: "r:1", Opened: now, Last: now}},
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "orders", 1, 0)
	get := func(key string) string {
		rec := req(t, c, "GET", "/_console/inspect/request?key="+key, nil)
		if rec.Code != 200 {
			t.Fatalf("%s: %d %s", key, rec.Code, rec.Body)
		}
		return readable(rec.Body.String())
	}
	body := get("r5")
	for _, want := range []string{"Produce", "v9", "correlation 4,812", "ok · 12 ms", "-1 (all)", "3 more partitions are not listed.", "Decoded fields", "Follow in Topics"} {
		if !strings.Contains(body, want) {
			t.Errorf("r5 lacks %q:\n%s", want, body)
		}
	}
	raw := req(t, c, "GET", "/_console/inspect/request?key=r5", nil).Body.String()
	for _, want := range []string{`/_console/topics/orders`, `/_console/traffic?client=svc`, `/_console/inspect/client?key=1`} {
		if !strings.Contains(raw, want) {
			t.Errorf("r5 does not link to %s", want)
		}
	}
	folded := get("r4")
	if !strings.Contains(folded, "Folded row") || !strings.Contains(folded, "×6") || !strings.Contains(folded, "svc · g") || !strings.Contains(folded, "all ok") || strings.Contains(folded, "correlation") {
		t.Errorf("folded:\n%s", folded)
	}
	commit := req(t, c, "GET", "/_console/inspect/request?key=r3", nil).Body.String()
	if strings.Contains(readable(commit), "correlation") || !strings.Contains(readable(commit), "orders p0–p1") {
		t.Errorf("a commit's head carries a time alone, and the partitions it commits:\n%s", readable(commit))
	}
	refused := req(t, c, "GET", "/_console/inspect/request?key=r2", nil).Body.String()
	if !strings.Contains(refused, "UNKNOWN_TOPIC_OR_PARTITION") || strings.Contains(refused, `/_console/topics/gone`) || !strings.Contains(refused, "/_console/groups/g") {
		t.Errorf("refused fetch via a group, of a topic that is not there:\n%s", readable(refused))
	}
	if strings.Contains(refused, `/_console/inspect/client?key=8`) {
		t.Error("a link to a client connection that has closed")
	}
	if gone := get("zzz"); !strings.Contains(gone, "aged out") {
		t.Errorf("a row that has aged out:\n%s", gone)
	}
	for q, want := range map[string]string{"seq=5": "Produce", "seq=99": "aged out"} {
		rec := req(t, c, "GET", "/_console/inspect/request?"+q, nil)
		if rec.Code != 200 || !strings.Contains(readable(rec.Body.String()), want) {
			t.Errorf("%s: %d", q, rec.Code)
		}
	}
	if rec := req(t, c, "GET", "/_console/inspect/request?seq=x", nil); rec.Code != 400 {
		t.Errorf("seq=x: %d", rec.Code)
	}
	if rec := req(t, c, "GET", "/_console/inspect/request", nil); rec.Code != 400 {
		t.Errorf("no key: %d", rec.Code)
	}
}
