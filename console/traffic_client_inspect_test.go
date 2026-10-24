package console

import (
	"strings"
	"testing"
	"time"
)

func TestInspectClientPage(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{clients: []TrafficClient{
		{Conn: 1, ClientID: "app", Remote: "10.0.0.1:1", Software: "kafka-java 3.7", Principal: "User:u", Opened: now.Add(-2 * time.Hour), Last: now, Groups: []string{"g1", "g2"}, Requests: 10, AskedTelemetry: true},
		{Conn: 2, ClientID: "app", Remote: "10.0.0.1:2", Opened: now.Add(-time.Hour), Last: now.Add(-time.Minute), Requests: 4},
		{Conn: 3, Remote: "10.0.0.9:3", Software: "librdkafka", Opened: now.Add(-time.Minute), Last: now},
		{Conn: 4, Remote: "10.0.0.8:4", Opened: now.Add(-time.Minute), Last: now},
		{Conn: 5, ClientID: "quiet", Remote: "10.0.0.5:5", Opened: now.Add(-time.Minute), Last: now, AskedTelemetry: true},
	}}
	metrics := fakeMetrics{instances: []ClientInstance{
		{ID: "i1", ClientID: "app", Conn: 2, Pushes: 3, First: now.Add(-time.Minute), Last: now.Add(-10 * time.Second), Series: []MetricSeries{{Name: "org.apache.kafka.producer.request.latency.avg", Unit: "ms", Samples: ramp(3, 1, 1)}}},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src; o.ClientMetrics = metrics })
	get := func(key string) string {
		rec := req(t, c, "GET", "/_console/inspect/client?key="+key, nil)
		if rec.Code != 200 {
			t.Fatalf("%s: %d", key, rec.Code)
		}
		return rec.Body.String()
	}
	body := get("1")
	text := readable(body)
	for _, want := range []string{"app", "kafka-java 3.7", "User:u", "10.0.0.1:1", "connected 2 h 00 m", "2 connections", "g1", "g2", "Request latency, average"} {
		if !strings.Contains(text, want) {
			t.Errorf("client 1 lacks %q:\n%s", want, text)
		}
	}
	for _, want := range []string{`/_console/groups/g1`, `/_console/traffic?client=app`} {
		if !strings.Contains(body, want) {
			t.Errorf("client 1 lacks a link to %s", want)
		}
	}
	// Without a client id the software and the address say which.
	if t3 := readable(get("3")); !strings.Contains(t3, "librdkafka · 10.0.0.9:3") {
		t.Errorf("client 3:\n%s", t3)
	}
	if t4 := readable(get("4")); !strings.Contains(t4, "10.0.0.8:4") || !strings.Contains(t4, "software not reported") {
		t.Errorf("client 4:\n%s", t4)
	}
	// A client that does not report points at one that does.
	q := get("5")
	if !strings.Contains(q, "/_console/inspect/client?key=1") || !strings.Contains(q, "open=client:1") {
		i := strings.Index(q, "See a client")
		t.Errorf("client 5 does not point to a client that reports:\n%s", q[max(0, i-400):i+40])
	}
	if strings.Contains(get("2"), "See a client that reports") {
		t.Error("a client that reports points at itself")
	}
	if gone := readable(get("99")); !strings.Contains(gone, "This connection has closed.") {
		t.Errorf("a closed connection:\n%s", gone)
	}
	if rec := req(t, c, "GET", "/_console/inspect/client?key=x", nil); rec.Code != 400 {
		t.Errorf("a bad key: %d", rec.Code)
	}
	// All of a client's metrics when asked.
	if rec := req(t, c, "GET", "/_console/inspect/client?key=1&all=1", nil); rec.Code != 200 {
		t.Errorf("all: %d", rec.Code)
	}
}

func TestMetricsForAClient(t *testing.T) {
	now := time.Now()
	full := ClientInstance{ID: "i1", ClientID: "app", Pushes: 2, Series: []MetricSeries{{Name: "m"}}}
	sum := ClientInstance{ID: "i1", ClientID: "app", Pushes: 2}
	c, _ := newTestConsoleAddr(t)
	if _, ok := c.metricsFor(TrafficClient{ClientID: "app"}); ok || c.hasMetrics(TrafficClient{ClientID: "app"}) {
		t.Error("metrics with no source")
	}
	c.metrics = fakeMetrics{instances: []ClientInstance{full}}
	if in, ok := c.metricsFor(TrafficClient{ClientID: "app", Opened: now}); !ok || in.ID != "i1" || len(in.Series) != 1 || !c.hasMetrics(TrafficClient{ClientID: "app"}) {
		t.Errorf("%+v %v", in, ok)
	}
	if _, ok := c.metricsFor(TrafficClient{ClientID: "other"}); ok || c.hasMetrics(TrafficClient{ClientID: "other"}) {
		t.Error("another client's metrics")
	}
	// A source that can say who reports cheaply, and give one whole.
	c.metrics = summarySource{sums: []ClientInstance{sum}, full: map[string]ClientInstance{"i1": full}}
	if in, ok := c.metricsFor(TrafficClient{ClientID: "app"}); !ok || len(in.Series) != 1 {
		t.Errorf("the whole instance is fetched: %+v %v", in, ok)
	}
	c.metrics = summarySource{sums: []ClientInstance{sum}}
	if in, ok := c.metricsFor(TrafficClient{ClientID: "app"}); !ok || len(in.Series) != 0 {
		t.Errorf("the summary stands when the whole is not there: %+v %v", in, ok)
	}
	withSeries := sum
	withSeries.Series = []MetricSeries{{Name: "kept"}}
	c.metrics = summarySource{sums: []ClientInstance{withSeries}, full: map[string]ClientInstance{"i1": full}}
	if in, ok := c.metricsFor(TrafficClient{ClientID: "app"}); !ok || in.Series[0].Name != "kept" {
		t.Errorf("a summary that carries its series is not replaced: %+v", in)
	}
}

type summarySource struct {
	sums []ClientInstance
	full map[string]ClientInstance
}

func (s summarySource) Instances() []ClientInstance { return nil }
func (s summarySource) Summaries() []ClientInstance { return s.sums }
func (s summarySource) ByID(id string) (ClientInstance, bool) {
	in, ok := s.full[id]
	return in, ok
}
