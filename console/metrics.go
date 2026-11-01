package console

// Client metrics: what each client says about itself.
//
// With KIP-714 a Kafka client (Java 3.7+, librdkafka 2.5+) pushes its own metrics to the
// broker it is connected to every few seconds: request latency, poll idle ratio,
// connection counts. These are METRICS the client reports, not traces of anything it did,
// and the page says so. They are held in memory for a few minutes and reach the console
// through the same kind of interface as the traffic: defined here, implemented where the
// broker is.

import (
	"html/template"
	"math"
	"strconv"
	"strings"
	"time"
)

// ClientMetricsSource is what clients have reported about themselves.
type ClientMetricsSource interface {
	// Instances are the clients that have reported, most recently heard from first.
	Instances() []ClientInstance
}

// ClientInstance is one client, as its telemetry describes it.
type ClientInstance struct {
	ID                                  string // the client instance id, a UUID.
	ClientID, Software, Principal, Host string
	// Conn is the connection it reports over (the traffic recorder's connection id), 0
	// when the source does not know.
	Conn        uint64
	First, Last time.Time
	Pushes      int
	// Metrics is how many series it has, Dropped how many beyond the cap were not kept
	// and Skipped how many pushes were not read for coming too fast.
	Metrics, Dropped, Skipped int
	Terminated                bool // its last push said it was closing.
	Resource                  map[string]string
	Series                    []MetricSeries
}

// MetricSeries is one metric for one set of attributes, oldest sample first.
type MetricSeries struct {
	Name, Attrs, Unit, Kind string
	Samples                 []MetricSample
}

// MetricSample is one point of a series.
type MetricSample struct {
	T time.Time
	V float64
}

// metricRow is a series, ready to show.
type metricRow struct {
	Name  string
	Short string
	// Label is the metric as a person writes it, where it is known; "" otherwise.
	Label string
	Attrs string
	Unit  string
	Last  string
	N     int
	Spark template.HTML
	// Tone is the value's colour, for an error or retry rate: ok at zero, warn above.
	Tone string
}

// metricPriority is what to put first, most useful first: errors and retries, how long
// requests take, how much is being sent, whether the consumer is keeping up. A real Java
// client reports well over a hundred metrics a producer; these are the ones that answer
// "is this client healthy", and the rest follow in name order, folded away (the fold is
// clientMetricsView's, in traffic_inspect.go: four of each list, then "All N metrics").
var metricPriority = []string{
	"request.latency.avg", "request.latency.max", "fetch.latency.avg",
	"record.send.rate", "record.queue.time.avg", "requests.in.flight",
	"record.error.rate", "record.retry.rate",
	"poll.idle.ratio", "last.heartbeat", "rebalance.latency", "commit.latency", "records.lag",
	"request.rate", "outgoing.byte.rate", "incoming.byte.rate",
	"connection.count", "connection.creation.rate",
	"throttle.time.avg", "io.wait.ratio", "batch.size.avg", "compression.rate.avg",
}

// metricRank is where a metric sits in metricPriority; those not in it share the last place.
// An error or retry rate above zero is the first thing anyone opening a client's numbers
// wants, so it jumps the queue (a zero one stays where it is: good news, not urgent).
func metricRank(name string, last float64) int {
	if last > 0 && (strings.Contains(name, "error.rate") || strings.Contains(name, "retry.rate")) {
		return -1
	}
	for i, k := range metricPriority {
		if strings.Contains(name, k) {
			return i
		}
	}
	return len(metricPriority)
}

func lastSample(s MetricSeries) float64 {
	if len(s.Samples) == 0 {
		return 0
	}
	return s.Samples[len(s.Samples)-1].V
}

// formatMetric writes a value with the digits that mean something: 12.5, 0.043, 8,812 and
// not 12.500000000000002; with its unit unless the unit is the dimensionless "1".
func formatMetric(v float64, unit string) string {
	var s string
	switch a := math.Abs(v); {
	case math.IsNaN(v) || math.IsInf(v, 0):
		return "—"
	case a >= 1000:
		s = groupDigits(int64(math.Round(v)))
	case a >= 100:
		s = strconv.FormatFloat(v, 'f', 0, 64)
	case a >= 1:
		s = strconv.FormatFloat(v, 'f', 2, 64)
	case a == 0:
		s = "0"
	default:
		s = strconv.FormatFloat(v, 'g', 3, 64)
	}
	if strings.Contains(s, ".") && !strings.ContainsAny(s, "e") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	if unit != "" && unit != "1" {
		s += " " + unit
	}
	return s
}
