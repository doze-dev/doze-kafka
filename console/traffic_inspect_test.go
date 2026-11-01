package console

import (
	"strings"
	"testing"
	"time"
)

func TestInstanceIsFoundByConnectionThenByClient(t *testing.T) {
	ins := []ClientInstance{
		{ID: "a", ClientID: "app", Principal: "User:a", Conn: 4, Pushes: 3},
		{ID: "b", ClientID: "app", Principal: "User:a", Conn: 9, Pushes: 0}, // never reported.
		{ID: "c", ClientID: "other", Principal: "User:b", Conn: 5, Pushes: 1},
	}
	if in, ok := instanceFor(TrafficClient{Conn: 5, ClientID: "x"}, ins); !ok || in.ID != "c" {
		t.Errorf("by connection: %+v %v", in, ok)
	}
	// Another connection of the same client and principal: the client's report.
	if in, ok := instanceFor(TrafficClient{Conn: 77, ClientID: "app", Principal: "User:a"}, ins); !ok || in.ID != "a" {
		t.Errorf("by client: %+v %v", in, ok)
	}
	// The same client id under another principal is another client.
	if _, ok := instanceFor(TrafficClient{Conn: 77, ClientID: "app", Principal: "User:z"}, ins); ok {
		t.Error("another principal's client was given the report")
	}
	if _, ok := instanceFor(TrafficClient{Conn: 77}, ins); ok {
		t.Error("a client with no id was matched by it")
	}
}

func TestExplainingEveryKindOfNo(t *testing.T) {
	c := &Console{}
	for class, want := range map[string]string{
		"expected":        "normal flow",
		"refused":         "Real Kafka would refuse",
		"denied":          "ACL or a login",
		"not-implemented": "does not do this",
		"failed":          "log says why",
	} {
		x := c.requestExplain(TrafficEvent{Error: "UNKNOWN_SERVER_ERROR", ErrorCode: -1, Class: class}, true)
		if x == nil || !strings.Contains(x.Verdict, want) {
			t.Errorf("%s: %+v, want a verdict with %q", class, x, want)
		}
	}
	if c.requestExplain(TrafficEvent{}, true) != nil {
		t.Error("a request that went well is explained as a refusal")
	}
}

func TestPartsAndDecodedFields(t *testing.T) {
	c := &Console{prefix: "/_console"}
	parts := c.partViews(TrafficFacts{Partitions: []TrafficPart{
		{Topic: "orders", Partition: 1, Offset: 4812, HasOffset: true, Records: 3},
		{Topic: "orders", Partition: 2, ErrorCode: 3},
	}})
	if len(parts) != 2 || parts[0].Offset != "4,812" || parts[0].Records != "3 records" || parts[0].TopicURL == "" ||
		parts[1].Result != "UNKNOWN_TOPIC_OR_PARTITION" || parts[1].Tone != "deny" {
		t.Errorf("parts = %+v", parts)
	}
	html, raw := decodedView(`{"a": 1}`)
	if !strings.Contains(string(html), "jt-k") || !strings.Contains(raw, "\n") {
		t.Errorf("decoded = %q %q", html, raw)
	}
	if h, r := decodedView("not json at all"); string(h) != "not json at all" || r != "not json at all" {
		t.Errorf("a bad text = %q %q", h, r)
	}
	if h, r := decodedView(""); h != "" || r != "" {
		t.Error("nothing decoded is something")
	}
	if got := plural2(1, "record") + plural2(2, "record"); got != "1 record2 records" {
		t.Errorf("plural = %q", got)
	}
}

func TestFoldViewSaysWhatEveryRepeatShared(t *testing.T) {
	c := &Console{}
	now := time.Now()
	f := c.foldView(TrafficEvent{API: "Heartbeat", Count: 300, First: now.Add(-time.Hour), Time: now, Duration: time.Millisecond, Detail: "generation 7",
		Ticks: []TrafficTick{{At: now.Add(-2 * time.Second), Duration: time.Millisecond}, {At: now, Duration: 4 * time.Millisecond}}}, false)
	if len(f.Ticks) != 2 || f.SameLabel != "Generation" || f.Same != "generation 7 throughout" {
		t.Errorf("fold = %+v", f)
	}
	if !strings.Contains(f.Note, "the last 2 of 300") || f.Unit != "heartbeat" {
		t.Errorf("note %q unit %q", f.Note, f.Unit)
	}
}

// A3: "Request error rate" and "Request retries" were the labels of any metric whose
// name held error.rate or retry.rate: a Java producer's record.error.rate and
// record.retry.rate, which count records, not requests. A label is the metric's own
// name in words, matched on the whole name; anything else is shown by its name.
func TestMetricLabelsAreTheMetricsOwnName(t *testing.T) {
	for name, want := range map[string]string{
		"org.apache.kafka.producer.record.error.rate":        "Record error rate",
		"org.apache.kafka.producer.record.retry.rate":        "Record retries",
		"org.apache.kafka.consumer.request.latency.avg":      "Request latency, average",
		"org.apache.kafka.consumer.node.request.latency.avg": "Request latency to a node, average",
		"org.apache.kafka.consumer.poll.idle.ratio.avg":      "Poll idle ratio",
		"org.apache.kafka.producer.some.other.error.rate":    "",
		"org.apache.kafka.consumer.coordinator.commit.rate":  "",
		"something.else": "",
	} {
		if got := metricLabel(name); got != want {
			t.Errorf("%s: %q, want %q", name, got, want)
		}
	}
	if worthALook("org.apache.kafka.producer.some.other.error.rate") || !worthALook("org.apache.kafka.producer.record.error.rate") {
		t.Error("worth a look is not the error and retry rates by name")
	}
	// A value carries the unit the client gave it, and no unit it did not.
	for _, tc := range []struct {
		v    float64
		unit string
		want string
	}{{1.4, "ms", "1.4 ms"}, {0, "1/s", "0 /s"}, {1229, "By/s", "1.2 KiB/s"}, {0.94, "", "0.94"}, {418, "", "418"}} {
		if got := metricValue(tc.v, tc.unit); got != tc.want {
			t.Errorf("%v %q: %q, want %q", tc.v, tc.unit, got, tc.want)
		}
	}
}

func TestMetricSpanIsWhatTheBarsCover(t *testing.T) {
	c := &Console{}
	t0 := time.Now()
	var samples []MetricSample
	for i := range 20 {
		samples = append(samples, MetricSample{T: t0.Add(time.Duration(i) * 5 * time.Second), V: float64(i)})
	}
	in := ClientInstance{Pushes: 20, First: t0, Last: samples[19].T, Series: []MetricSeries{
		{Name: "org.apache.kafka.consumer.request.latency.avg", Samples: samples},
		{Name: "org.apache.kafka.producer.record.error.rate", Samples: []MetricSample{{T: t0, V: 0}}},
	}}
	m := c.clientMetricsView(in, false)
	// Twelve bars, five seconds apart: 55 s from the first to the last, "the last minute".
	if m.Span != "the last minute" || len(m.Worth) != 1 || len(m.Rest) != 1 || m.Metrics != 2 || m.Every != "5 s" {
		t.Errorf("view = %+v", m)
	}
	if n := strings.Count(string(m.Rest[0].Spark), "<i "); n != sparkBars {
		t.Errorf("%d bars, want %d", n, sparkBars)
	}
}
