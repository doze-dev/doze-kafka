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
		x := c.requestExplain(TrafficEvent{Error: "UNKNOWN_SERVER_ERROR", ErrorCode: -1, Class: class})
		if x == nil || !strings.Contains(x.Verdict, want) {
			t.Errorf("%s: %+v, want a verdict with %q", class, x, want)
		}
	}
	if c.requestExplain(TrafficEvent{}) != nil {
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

func TestFoldViewDrawsTicksByDuration(t *testing.T) {
	c := &Console{}
	now := time.Now()
	f := c.foldView(TrafficEvent{API: "Heartbeat", Count: 300, First: now.Add(-time.Hour), Time: now, Duration: time.Millisecond,
		Ticks: []TrafficTick{{At: now.Add(-2 * time.Second), Duration: time.Millisecond}, {At: now, Duration: 4 * time.Millisecond, Class: "failed"}}}, false)
	if len(f.Ticks) != 2 || f.Ticks[1].Height <= f.Ticks[0].Height || f.Ticks[1].Tone != "bad" {
		t.Errorf("ticks = %+v", f.Ticks)
	}
	if !strings.Contains(f.Note, "the last 2 of 300") || f.Unit != "heartbeat" {
		t.Errorf("note %q unit %q", f.Note, f.Unit)
	}
}

func TestMetricLabelsAndSpan(t *testing.T) {
	if metricLabel("org.apache.kafka.producer.record.error.rate") != "Request error rate" || metricLabel("something.else") != "" {
		t.Error("labels are wrong")
	}
	c := &Console{}
	t0 := time.Now()
	in := ClientInstance{Pushes: 2, Series: []MetricSeries{
		{Name: "org.apache.kafka.producer.request.latency.avg", Samples: []MetricSample{{T: t0, V: 1}, {T: t0.Add(5 * time.Minute), V: 2}}},
		{Name: "org.apache.kafka.producer.x.error.rate", Samples: []MetricSample{{T: t0, V: 0}}},
	}}
	m := c.clientMetricsView(in, false)
	if m.Span != "the last 5 minutes" || len(m.Worth) != 1 || len(m.Rest) != 1 || m.Metrics != 2 {
		t.Errorf("view = %+v", m)
	}
	in.Series[0].Samples[1].T = t0.Add(40 * time.Second)
	if m := c.clientMetricsView(in, true); m.Span != "the last 40 seconds" {
		t.Errorf("span = %q", m.Span)
	}
}
