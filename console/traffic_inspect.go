package console

// The inspector's bodies for Traffic: one request (Traf-04), a folded row (Traf-05),
// a client that reports metrics (Traf-07) and one that does not (Traf-11).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// partView is one partition of a request, ready to show.
type partView struct {
	Topic, TopicURL string
	Partition       int32
	Offset          string
	Result, Tone    string
	Records         string
}

// foldView is what a folded row says about its repeats (Traf-05).
type foldView struct {
	Count    int
	First    time.Time
	Last     time.Time
	Interval string
	Each     string // "ok, 0 ms each"
	Result   string
	Ticks    []tickView
	Kept     int
	Note     string // "Each tick is one heartbeat" / "the last 64 of 310"
	Unit     string // "heartbeat", "fetch", "push"
	Unfold   bool
}

type tickView struct {
	Height int
	Tone   string
	Title  string
	At     time.Time
	Took   string
	Result string
}

// explainView is what a refusal means, for the inspector of the request that got it.
type explainView struct {
	Word, Tone, Name, Meaning, Cause, Fix, Verdict string
	Closed                                         string
}

func (c *Console) requestExplain(e TrafficEvent) *explainView {
	if e.Error == "" {
		return nil
	}
	v := &explainView{Word: classWord(e.Class), Tone: resultTone(e.Class), Name: e.Error}
	if e.Closed != "" {
		v.Word, v.Tone = "Failed", "bad"
		v.Meaning = "The broker closed the connection because of this request."
		v.Cause = e.Closed + "."
		v.Closed = e.Closed
		return v
	}
	info := protocol.Explain(e.ErrorCode)
	v.Meaning, v.Cause, v.Fix = info.Meaning, info.Cause, info.Fix
	switch e.Class {
	case "expected":
		v.Verdict = "This is part of the protocol's normal flow, not a problem."
	case "refused":
		v.Verdict = "Real Kafka would refuse the same call."
	case "denied":
		v.Verdict = "An ACL or a login decided this."
	case "not-implemented":
		v.Verdict = "This broker does not do this."
	case "failed":
		v.Verdict = "The broker could not do this; its log says why."
	}
	return v
}

// periodic repeats are named by what they are.
func repeatUnit(api string) string {
	switch api {
	case "Heartbeat", "ConsumerGroupHeartbeat":
		return "heartbeat"
	case "Fetch":
		return "fetch"
	case "PushTelemetry":
		return "report"
	case "OffsetFetch":
		return "offset fetch"
	}
	return "request"
}

func (c *Console) foldView(e TrafficEvent, unfold bool) *foldView {
	f := &foldView{Count: e.Count, First: e.First, Last: e.Time, Interval: intervalText(e), Unit: repeatUnit(e.API), Unfold: unfold}
	each := shortDuration(e.Duration)
	f.Each = "ok, " + each + " each"
	f.Result = "ok"
	var maxDur time.Duration
	for _, t := range e.Ticks {
		maxDur = max(maxDur, t.Duration)
	}
	for _, t := range e.Ticks {
		h := 14
		if maxDur > 0 {
			h = 6 + int(16*float64(t.Duration)/float64(maxDur))
		}
		f.Ticks = append(f.Ticks, tickView{Height: h, Tone: resultTone(t.Class), At: t.At, Took: shortDuration(t.Duration),
			Title: whenText(t.At, StyleClockMs) + " · " + shortDuration(t.Duration)})
	}
	f.Kept = len(f.Ticks)
	f.Note = "Each tick is one " + f.Unit
	if e.Count > f.Kept && f.Kept > 0 {
		f.Note += ": the last " + strconv.Itoa(f.Kept) + " of " + groupDigits(int64(e.Count))
	}
	return f
}

// decodedView pretty-prints the request's fields and tints them.
func decodedView(raw string) (template.HTML, string) {
	if raw == "" {
		return "", ""
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, []byte(raw), "", "  "); err != nil {
		return jsonTint(raw), raw
	}
	return jsonTint(buf.String()), buf.String()
}

func (c *Console) partViews(f TrafficFacts) []partView {
	var out []partView
	for _, p := range f.Partitions {
		pv := partView{Topic: p.Topic, Partition: p.Partition, Result: "ok", Tone: "ok"}
		if p.Topic != "" {
			pv.TopicURL = c.topicURL(p.Topic)
		}
		if p.HasOffset {
			pv.Offset = groupInt(p.Offset)
		}
		if p.Records > 0 {
			pv.Records = plural2(p.Records, "record")
		}
		if p.ErrorCode != 0 {
			pv.Result, pv.Tone = protocol.ErrorName(p.ErrorCode), resultTone(string(protocol.CodeClass(p.ErrorCode)))
		}
		out = append(out, pv)
	}
	return out
}

func plural2(n int, one string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + one + "s"
}

// groupInt writes 4812 as "4,812".
func groupInt(n int64) string { return groupDigits(n) }

func (c *Console) inspectRequest(w http.ResponseWriter, r *http.Request, key string) {
	if c.trafficOff(w, r) {
		return
	}
	q := r.URL.Query()
	if raw := q.Get("seq"); key == "" && raw != "" {
		// The earlier address named a sequence number, which moves whenever a row folds.
		seq, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			c.fail(w, r, badInput("seq must be a number"))
			return
		}
		key = "seq-" + raw // no row: it has moved on or aged out.
		for _, e := range c.traffic.Events(TrafficFilter{}, 0) {
			if e.Seq == seq {
				key = e.ID
			}
		}
	}
	if key == "" {
		c.fail(w, r, badInput("a request is named by its row id"))
		return
	}
	v := inspectorView{Kind: KindRequest, Key: key, Title: "Request", Body: map[string]any{}}
	e, ok := c.traffic.Row(key)
	if !ok {
		v.Gone = true
		v.GoneMsg = "This request has aged out of what the broker remembers. The last couple of thousand are kept, in memory only."
		c.inspect(w, v)
		return
	}
	row := c.trafficRow(e)
	v.Title = fmt.Sprintf("%s v%d", e.API, e.Version)
	if row.Folded {
		v.Cap = "Folded row"
		v.Title += " ×" + strconv.Itoa(e.Count)
		v.Sub = row.Who
		if g := firstNonEmpty(e.Group, e.ViaGroup); g != "" {
			v.Sub += " · " + g
		}
		if e.Error == "" {
			v.Status, v.StatusTone = "all ok", "ok"
		}
	} else {
		v.Status, v.StatusTone = "ok · "+row.Dur, "ok"
		if e.Error != "" {
			v.Status, v.StatusTone = e.Error+" · "+row.Dur, row.Tone
		}
		v.SubTime = e.Time
		v.Sub = "correlation " + groupInt(int64(e.Correlation))
	}
	body := v.Body
	body["R"] = row
	body["Explain"] = c.requestExplain(e)
	body["Parts"] = c.partViews(e.Facts)
	body["PartsMore"] = e.Facts.PartitionsMore
	body["HasAcks"], body["Acks"] = e.Facts.HasAcks, acksText(e.Facts.Acks)
	html, raw := decodedView(e.Facts.Decoded)
	body["Decoded"], body["DecodedRaw"] = html, raw
	if row.Folded {
		body["Fold"] = c.foldView(e, q.Get("unfold") != "")
	}
	if e.ClientID != "" || e.Remote != "" {
		body["WhoFilter"] = c.prefix + "/traffic?" + TrafficFilter{Client: row.Who}.query()
	}
	if row.ConnKey != "" || c.hasOpenConn(e.Conn) {
		body["ClientOpen"] = c.inspectURL(KindClient, strconv.FormatUint(e.Conn, 10))
	}
	if g := firstNonEmpty(e.Group, e.ViaGroup); g != "" {
		body["GroupName"], body["GroupLink"] = g, c.groupURL(g)
		body["GroupVia"] = e.Group == "" && e.ViaGroup != ""
	}
	c.inspect(w, v)
}

func acksText(a int) string {
	switch a {
	case -1:
		return "-1 (all)"
	case 0:
		return "0 (none)"
	case 1:
		return "1 (leader)"
	}
	return strconv.Itoa(a)
}

func (c *Console) hasOpenConn(conn uint64) bool {
	for _, cl := range c.traffic.Clients() {
		if cl.Conn == conn {
			return true
		}
	}
	return false
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

// ---- clients ----

// metricLabel names a metric as a person would, for the ones everyone looks at. Any
// other is shown as its short name: a label is only written where it is known to say
// what the metric is.
var metricLabels = []struct{ Match, Label string }{
	{"error.rate", "Request error rate"},
	{"retry.rate", "Request retries"},
	{"request.latency.avg", "Request latency, average"},
	{"request.latency.max", "Request latency, maximum"},
	{"poll.idle.ratio", "Poll idle ratio"},
	{"connection.count", "Connection count"},
}

func metricLabel(name string) string {
	for _, l := range metricLabels {
		if strings.Contains(name, l.Match) {
			return l.Label
		}
	}
	return ""
}

// clientMetrics is a client's metrics, ready to show (Traf-07).
type clientMetrics struct {
	Who         string
	Reports     int
	Metrics     int
	Dropped     int
	Skipped     int
	Last        time.Time
	Worth, Rest []metricRow
	More        int
	All         bool
	Span        string
	Terminated  bool
	Resource    string
	IDShort     string
}

const restShown = 4

func (c *Console) metricsFor(cl TrafficClient) (ClientInstance, bool) {
	if c.metrics == nil {
		return ClientInstance{}, false
	}
	if src, ok := c.metrics.(interface {
		ForConn(uint64) (ClientInstance, bool)
	}); ok {
		in, found := src.ForConn(cl.Conn)
		return in, found && in.Pushes > 0
	}
	for _, in := range c.metrics.Instances() {
		if (in.Conn != 0 && in.Conn == cl.Conn) || (in.Conn == 0 && cl.ClientID != "" && in.ClientID == cl.ClientID) {
			return in, in.Pushes > 0
		}
	}
	return ClientInstance{}, false
}

// hasMetrics says a client has reported (without copying its series).
func (c *Console) hasMetrics(cl TrafficClient) bool {
	if c.metrics == nil {
		return false
	}
	if src, ok := c.metrics.(interface{ Summaries() []ClientInstance }); ok {
		for _, in := range src.Summaries() {
			if in.Conn == cl.Conn && in.Pushes > 0 {
				return true
			}
		}
		return false
	}
	_, ok := c.metricsFor(cl)
	return ok
}

func (c *Console) clientMetricsView(in ClientInstance, all bool) clientMetrics {
	m := clientMetrics{Reports: in.Pushes, Metrics: in.Metrics, Dropped: in.Dropped, Skipped: in.Skipped, Last: in.Last, All: all, Terminated: in.Terminated}
	if m.Metrics == 0 {
		m.Metrics = len(in.Series)
	}
	m.IDShort = in.ID
	if len(m.IDShort) > 8 {
		m.IDShort = m.IDShort[:8]
	}
	var keys []string
	for k, v := range in.Resource {
		keys = append(keys, k+"="+v)
	}
	sort.Strings(keys)
	m.Resource = strings.Join(keys, " · ")
	var span time.Duration
	rows := make([]metricRow, 0, len(in.Series))
	lastOf := map[string]float64{}
	for _, s := range in.Series {
		if len(s.Samples) > 1 {
			span = max(span, s.Samples[len(s.Samples)-1].T.Sub(s.Samples[0].T))
		}
		short := strings.TrimPrefix(s.Name, "org.apache.kafka.")
		row := metricRow{Name: s.Name, Short: short, Attrs: s.Attrs, Unit: s.Unit, Last: formatMetric(lastSample(s), s.Unit), N: len(s.Samples), Spark: sparkline(s.Samples)}
		if l := metricLabel(s.Name); l != "" {
			row.Label = l
		}
		rows = append(rows, row)
		lastOf[s.Name+"\x00"+s.Attrs] = lastSample(s)
	}
	rank := func(r metricRow) int { return metricRank(r.Name, lastOf[r.Name+"\x00"+r.Attrs]) }
	sort.SliceStable(rows, func(a, b int) bool { return rank(rows[a]) < rank(rows[b]) })
	for _, r := range rows {
		if strings.Contains(r.Name, "error.rate") || strings.Contains(r.Name, "retry.rate") {
			m.Worth = append(m.Worth, r)
		} else {
			m.Rest = append(m.Rest, r)
		}
	}
	if len(m.Worth) > 4 && !all {
		m.More += len(m.Worth) - 4
		m.Worth = m.Worth[:4]
	}
	if !all && len(m.Rest) > restShown {
		m.More += len(m.Rest) - restShown
		m.Rest = m.Rest[:restShown]
	}
	// The chart covers what the samples cover: the span from the first sample to the
	// last of the longest series, written in whole minutes ("the last 5 minutes").
	mins := int(span.Round(time.Minute) / time.Minute)
	switch {
	case mins >= 2:
		m.Span = "the last " + strconv.Itoa(mins) + " minutes"
	case span > 0:
		m.Span = "the last " + strconv.Itoa(int(span.Round(time.Second)/time.Second)) + " seconds"
	}
	return m
}

func (c *Console) inspectClient(w http.ResponseWriter, r *http.Request, key string) {
	if c.trafficOff(w, r) {
		return
	}
	conn, err := strconv.ParseUint(key, 10, 64)
	if err != nil {
		c.fail(w, r, badInput("a client is named by its connection number"))
		return
	}
	v := inspectorView{Kind: KindClient, Key: key, Title: "Client", Body: map[string]any{}}
	for _, cl := range c.traffic.Clients() {
		if cl.Conn != conn {
			continue
		}
		now := time.Now()
		cr := c.clientRow(cl, now)
		who := cl.ClientID
		if who == "" { // no client id: the software and the address say which (TRF-31).
			who = firstNonEmpty(cl.Software, cl.Remote)
			if cl.Software != "" {
				who += " · " + cl.Remote
			}
		}
		v.Title = who
		v.Sub = joinNonEmpty(" · ", firstNonEmpty(cl.Software, "software not reported"), cl.Principal, cl.Remote)
		v.Status, v.StatusKind = "connected "+spanText(now.Sub(cl.Opened)), "pill"
		body := v.Body
		body["C"] = cr
		body["Reqs"] = c.prefix + "/traffic?" + cr.Filter
		in, ok := c.metricsFor(cl)
		all := r.URL.Query().Get("all") != ""
		if ok {
			if in.Series == nil && len(in.Series) == 0 {
				// A source that offers a summary first: fetch the series for this one.
				if src, has := c.metrics.(interface {
					ForConn(uint64) (ClientInstance, bool)
				}); has {
					in, _ = src.ForConn(cl.Conn)
				}
			}
			m := c.clientMetricsView(in, all)
			m.Who = who
			body["M"] = m
			body["HasMetrics"] = true
		} else if c.metrics != nil {
			body["NoMetrics"] = true
			body["Asked"] = cl.AskedTelemetry
			// A client that does report, to point at.
			var other *clientRow
			for _, ocl := range c.traffic.Clients() {
				if ocl.Conn != cl.Conn && c.hasMetrics(ocl) {
					x := c.clientRow(ocl, now)
					other = &x
					break
				}
			}
			if other != nil {
				body["Other"] = other
				body["OtherURL"] = c.inspectURL(KindClient, strconv.FormatUint(other.Conn, 10))
			}
		}
		body["MetricsOn"] = c.metrics != nil
		var gl []struct{ Name, URL string }
		for _, g := range cl.Groups {
			gl = append(gl, struct{ Name, URL string }{g, c.groupURL(g)})
		}
		body["GroupLinks"] = gl
		c.inspect(w, v)
		return
	}
	v.Gone = true
	v.GoneMsg = "This connection has closed."
	c.inspect(w, v)
}

func joinNonEmpty(sep string, ss ...string) string {
	var out []string
	for _, s := range ss {
		if s != "" {
			out = append(out, s)
		}
	}
	return strings.Join(out, sep)
}
