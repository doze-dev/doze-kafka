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
	"fmt"
	"html/template"
	"math"
	"sort"
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
	First, Last                         time.Time
	Pushes                              int
	Terminated                          bool // its last push said it was closing.
	Resource                            map[string]string
	Series                              []MetricSeries
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
	Attrs string
	Unit  string
	Last  string
	N     int
	Spark template.HTML
}

// metricCard is a client, ready to show.
type metricCard struct {
	ClientInstance
	Who      string
	IDShort  string // the first part of the instance id: enough to tell two apart.
	Age      string
	Closing  bool
	Shown    []metricRow
	More     []metricRow
	Resource string
}

// metricsShown is how many series a card lists before folding the rest away.
const metricsShown = 10

// metricPriority is what to put first, most useful first: errors and retries, how long
// requests take, how much is being sent, whether the consumer is keeping up. A real Java
// client reports well over a hundred metrics a producer; these are the ones that answer
// "is this client healthy", and the rest follow in name order, folded away.
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

func (c *Console) metricCards() []metricCard {
	if c.metrics == nil {
		return nil
	}
	now := time.Now()
	var cards []metricCard
	for _, in := range c.metrics.Instances() {
		card := metricCard{ClientInstance: in, Who: in.ClientID, Age: spanText(now.Sub(in.Last)), Closing: in.Terminated}
		if card.Who == "" {
			card.Who = in.Host
		}
		card.IDShort = in.ID
		if len(card.IDShort) > 8 {
			card.IDShort = card.IDShort[:8]
		}
		var keys []string
		for k, v := range in.Resource {
			keys = append(keys, k+"="+v)
		}
		sort.Strings(keys)
		card.Resource = strings.Join(keys, " · ")

		rows := make([]metricRow, 0, len(in.Series))
		for _, s := range in.Series {
			rows = append(rows, metricRow{
				Name: s.Name, Short: strings.TrimPrefix(s.Name, "org.apache.kafka."), Attrs: s.Attrs, Unit: s.Unit,
				Last: formatMetric(lastSample(s), s.Unit), N: len(s.Samples), Spark: sparkline(s.Samples),
			})
		}
		lastOf := map[string]float64{}
		for _, ser := range in.Series {
			lastOf[ser.Name+"\x00"+ser.Attrs] = lastSample(ser)
		}
		rank := func(r metricRow) int { return metricRank(r.Name, lastOf[r.Name+"\x00"+r.Attrs]) }
		sort.SliceStable(rows, func(a, b int) bool { return rank(rows[a]) < rank(rows[b]) })
		if len(rows) > metricsShown {
			card.Shown, card.More = rows[:metricsShown], rows[metricsShown:]
		} else {
			card.Shown = rows
		}
		cards = append(cards, card)
	}
	return cards
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

const (
	sparkW, sparkH = 120, 24
	sparkPoints    = 60
)

// sparkline draws a series as a small inline SVG: its last sparkPoints samples scaled to
// fit, so the shape is what shows. Nothing to draw (fewer than two samples) is a dot's
// worth of nothing, not an empty chart.
func sparkline(samples []MetricSample) template.HTML {
	if len(samples) < 2 {
		return ""
	}
	if len(samples) > sparkPoints {
		samples = samples[len(samples)-sparkPoints:]
	}
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, s := range samples {
		lo, hi = math.Min(lo, s.V), math.Max(hi, s.V)
	}
	var pts strings.Builder
	for i, s := range samples {
		x := float64(i) * float64(sparkW-2) / float64(len(samples)-1)
		y := float64(sparkH) / 2
		if hi > lo {
			y = 2 + (hi-s.V)/(hi-lo)*float64(sparkH-4)
		}
		fmt.Fprintf(&pts, "%.1f,%.1f ", x+1, y)
	}
	return template.HTML(fmt.Sprintf(
		`<svg class="spark" viewBox="0 0 %d %d" width="%d" height="%d" role="img" aria-label="the last %d samples, from %s to %s"><polyline points="%s"/></svg>`,
		sparkW, sparkH, sparkW, sparkH, len(samples), template.HTMLEscapeString(formatMetric(samples[0].V, "")), template.HTMLEscapeString(formatMetric(samples[len(samples)-1].V, "")),
		strings.TrimSpace(pts.String())))
}
