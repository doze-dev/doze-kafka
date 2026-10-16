package console

// The inspector's bodies for Traffic: one request (Traf-04; an OffsetCommit as Traf-08
// draws it), a folded row (Traf-05), a client that reports metrics (Traf-07) and one
// that does not (Traf-11). Their template is traffic_inspector.html.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
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
	Ticks    []tickView
	Kept     int
	Note     string // "Each tick is one heartbeat" / "the last 64 of 310"
	Unit     string // "heartbeat", "fetch", "report"
	Unfold   bool
	// Same is what every repeat had in common beyond the request (Traf-05's
	// "Generation: epoch 12 throughout"): the recorder's fold breaks when it changes, so
	// it holds for all of them. SameLabel is its row's label.
	Same, SameLabel string
}

type tickView struct {
	Title string
	At    time.Time
	Took  string
}

// explainView is what a refusal means, for the inspector of the request that got it.
type explainView struct {
	Word, Tone, Name, Meaning, Cause, Fix, Verdict string
	Closed                                         string
}

func (c *Console) requestExplain(e TrafficEvent, autoCreate bool) *explainView {
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
	info := protocol.ExplainFor(e.ErrorCode, protocol.BrokerFacts{AutoCreateTopics: autoCreate})
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

// repeatUnit names a folded row's repeats by what they are.
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
	f.Each = "ok, " + shortDuration(e.Duration) + " each"
	for _, t := range e.Ticks {
		f.Ticks = append(f.Ticks, tickView{At: t.At, Took: shortDuration(t.Duration), Title: whenText(t.At, StyleClockMs) + " · " + shortDuration(t.Duration)})
	}
	f.Kept = len(f.Ticks)
	// The row's detail is part of what folds (the recorder's fold key): a heartbeat's
	// generation, a fetch's wait. It was the same for every repeat.
	switch {
	case strings.HasPrefix(e.Detail, "generation "):
		f.SameLabel, f.Same = "Generation", e.Detail+" throughout"
	case strings.HasPrefix(e.Detail, "epoch "):
		f.SameLabel, f.Same = "Epoch", e.Detail+" throughout"
	}
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

// inspectTraffic renders a Traffic selection: the boards' head (.hd) and the kind's
// body. title is the head's title as the boards write it (the request, its version
// muted); tone is what the body is ("request", "folded", "client-metrics",
// "client-none"), for the page to draw its state by (shell.js sets data-traffic-open).
func (c *Console) inspectTraffic(w http.ResponseWriter, v inspectorView, title template.HTML, tone string) {
	data := map[string]any{}
	for k, val := range v.Body {
		data[k] = val
	}
	data["InspKind"], data["InspKey"], data["InspTitle"], data["InspSub"] = v.Kind, v.Key, v.Title, v.Sub
	data["InspTitleHTML"] = title
	if title == "" {
		data["InspTitleHTML"] = template.HTML(template.HTMLEscapeString(v.Title)) //nolint:gosec // escaped here.
	}
	data["InspKindLabel"] = inspectorKindLabels[v.Kind]
	if v.Cap != "" {
		data["InspKindLabel"] = v.Cap
	}
	data["InspStatus"], data["InspStatusTone"], data["InspStatusKind"], data["InspSubTime"] = v.Status, v.StatusTone, v.StatusKind, v.SubTime
	data["InspGone"], data["InspGoneMsg"], data["InspOpen"], data["InspTone"] = v.Gone, v.GoneMsg, openParam(v.Kind, v.Key), tone
	c.partial(w, "traffic_inspector", data)
}

// requestTitle is the boards' head: the request, its version muted, and the fold count.
func requestTitle(e TrafficEvent) template.HTML {
	t := template.HTMLEscapeString(e.API) + ` <span class="mut">v` + strconv.Itoa(int(e.Version)) + `</span>`
	if e.Count > 1 {
		t += ` <span class="tag">×` + strconv.Itoa(e.Count) + `</span>`
	}
	return template.HTML(t) //nolint:gosec // the API name is escaped; the rest is ours.
}

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
		c.inspectTraffic(w, v, "", "request")
		return
	}
	row := c.trafficRow(e, trafficView{})
	v.Title = fmt.Sprintf("%s v%d", e.API, e.Version)
	tone := "request"
	body := v.Body
	commit := e.API == "OffsetCommit" && e.Error == "" && !row.Folded
	if row.Folded {
		tone = "folded"
		v.Cap = "Folded row"
		v.Title += " ×" + strconv.Itoa(e.Count)
		v.Sub = row.Who
		if g := firstNonEmpty(e.Group, e.ViaGroup); g != "" {
			v.Sub += " · " + g
		}
		v.Status, v.StatusTone = "all ok", "ok"
	} else {
		v.Status, v.StatusTone = "ok · "+row.Dur, "ok"
		if e.Error != "" {
			v.Status, v.StatusTone = e.Error+" · "+row.Dur, row.Tone
		}
		v.SubTime = e.Time
		// Traf-04 draws a request's head with its time and correlation id; Traf-08 draws a
		// commit's with its time alone.
		if !commit {
			v.Sub = "correlation " + groupInt(int64(e.Correlation))
		}
	}
	body["R"] = row
	body["Explain"] = c.requestExplain(e, c.autoCreateTopics(r.Context()))
	body["Parts"] = c.partViews(e.Facts)
	body["PartsMore"] = e.Facts.PartitionsMore
	body["HasAcks"], body["Acks"] = e.Facts.HasAcks, acksText(e.Facts.Acks)
	html, raw := decodedView(e.Facts.Decoded)
	body["Decoded"], body["DecodedRaw"] = html, raw
	if row.Folded {
		body["Fold"] = c.foldView(e, q.Get("unfold") != "")
	}
	if commit {
		body["Commit"] = true
		body["Commits"] = row.TopicParts
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
	// "Follow in Topics" goes to the topic the request wrote or read, when there is one
	// to go to: a refused Produce names a topic that does not exist (A8).
	if len(e.Topics) > 0 && (e.Kind == "produce" || e.Kind == "fetch") && c.topicExists(r.Context(), e.Topics[0]) {
		body["FollowURL"] = c.topicURL(e.Topics[0])
	}
	c.inspectTraffic(w, v, requestTitle(e), tone)
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

// metricLabels name a metric in words, for the ones the boards name: each is the
// metric's own name written out (the client said what it measures; the label says it
// in words and claims nothing more), matched on the whole name below the client's
// prefix, so a record rate is not called a request rate (A3). Any other metric is
// shown by its short name.
var metricLabels = map[string]string{
	"request.latency.avg":               "Request latency, average",
	"request.latency.max":               "Request latency, maximum",
	"node.request.latency.avg":          "Request latency to a node, average",
	"node.request.latency.max":          "Request latency to a node, maximum",
	"poll.idle.ratio.avg":               "Poll idle ratio",
	"connection.count":                  "Connection count",
	"request.error.rate":                "Request error rate",
	"request.retry.rate":                "Request retries",
	"record.error.rate":                 "Record error rate",
	"record.retry.rate":                 "Record retries",
	"fetch.manager.bytes.consumed.rate": "Bytes fetched",
}

// metricShort is a metric's name below the client's own prefix
// ("org.apache.kafka.producer.").
func metricShort(name string) string {
	for _, p := range []string{"org.apache.kafka.producer.", "org.apache.kafka.consumer.", "org.apache.kafka.client.", "org.apache.kafka."} {
		if strings.HasPrefix(name, p) {
			return strings.TrimPrefix(name, p)
		}
	}
	return name
}

func metricLabel(name string) string { return metricLabels[metricShort(name)] }

// metricOrder is the order Traf-07 lists the metrics it names in.
var metricOrder = map[string]int{
	"request.error.rate": 0, "request.retry.rate": 1, "record.error.rate": 2, "record.retry.rate": 3,
	"request.latency.avg": 4, "node.request.latency.avg": 5, "poll.idle.ratio.avg": 6, "connection.count": 7,
	"fetch.manager.bytes.consumed.rate": 8, "request.latency.max": 9, "node.request.latency.max": 10,
}

// worthALook are the metrics the boards put first ("Worth a look first"): the error
// and retry rates, by their whole name.
func worthALook(name string) bool {
	switch metricShort(name) {
	case "request.error.rate", "request.retry.rate", "record.error.rate", "record.retry.rate":
		return true
	}
	return false
}

// clientMetrics is a client's metrics, ready to show (Traf-07).
type clientMetrics struct {
	Who         string
	Reports     int
	Metrics     int
	Dropped     int
	Skipped     int
	Last        time.Time
	Every       string // "5 s": how often it has reported, from its reports.
	Worth, Rest []metricRow
	More        int
	All         bool
	Span        string
	Terminated  bool
}

const restShown = 4

// instanceFor finds the instance that reports for a connection: the one that pushed over
// it, else the one a connection of the same client id and principal pushed over (a
// client may subscribe and push on different connections, and each of its connections
// is a row of the clients table). ok is false when none has reported.
func instanceFor(cl TrafficClient, ins []ClientInstance) (ClientInstance, bool) {
	var byClient *ClientInstance
	for i, in := range ins {
		if in.Pushes == 0 {
			continue
		}
		if in.Conn != 0 && in.Conn == cl.Conn {
			return in, true
		}
		if byClient == nil && cl.ClientID != "" && in.ClientID == cl.ClientID && (in.Principal == "" || in.Principal == cl.Principal) {
			byClient = &ins[i]
		}
	}
	if byClient != nil {
		return *byClient, true
	}
	return ClientInstance{}, false
}

func (c *Console) metricsFor(cl TrafficClient) (ClientInstance, bool) {
	if c.metrics == nil {
		return ClientInstance{}, false
	}
	// Who reports is known from the summaries (no series copied); the one found is then
	// fetched whole, which is the one the inspector asked for.
	idx := c.metrics.Instances
	if src, ok := c.metrics.(interface{ Summaries() []ClientInstance }); ok {
		idx = src.Summaries
	}
	sum, found := instanceFor(cl, idx())
	if !found {
		return ClientInstance{}, false
	}
	if src, ok := c.metrics.(interface {
		ByID(string) (ClientInstance, bool)
	}); ok && len(sum.Series) == 0 {
		if full, ok := src.ByID(sum.ID); ok {
			return full, true
		}
	}
	return sum, true
}

// hasMetrics says a client has reported (without copying its series).
func (c *Console) hasMetrics(cl TrafficClient) bool {
	if c.metrics == nil {
		return false
	}
	idx := c.metrics.Instances
	if src, ok := c.metrics.(interface{ Summaries() []ClientInstance }); ok {
		idx = src.Summaries
	}
	_, ok := instanceFor(cl, idx())
	return ok
}

// sparkBars is how many of a series' latest reports its sparkline draws, one bar each
// (Traf-07: twelve bars).
const sparkBars = 12

// sparkBarsHTML draws a series' latest reports as the board's bars: the board's .spark,
// a 16 px box, each bar 2 px at the least and as tall as its value is of the largest
// drawn.
func sparkBarsHTML(samples []MetricSample) template.HTML {
	if len(samples) == 0 {
		return ""
	}
	if len(samples) > sparkBars {
		samples = samples[len(samples)-sparkBars:]
	}
	hi := 0.0
	for _, s := range samples {
		if !math.IsNaN(s.V) && !math.IsInf(s.V, 0) {
			hi = math.Max(hi, math.Abs(s.V))
		}
	}
	var b strings.Builder
	b.WriteString(`<span class="spark">`)
	for _, s := range samples {
		h := 2
		if hi > 0 && !math.IsNaN(s.V) && !math.IsInf(s.V, 0) {
			h = 2 + int(math.Round(14*math.Abs(s.V)/hi))
		}
		fmt.Fprintf(&b, `<i style="height: %dpx"></i>`, h)
	}
	b.WriteString(`</span>`)
	return template.HTML(b.String()) //nolint:gosec // numbers only.
}

// kibText is a number of bytes as the boards write it: "526 B", "1.2 KiB", "3.4 MiB".
func kibText(n int) string {
	switch {
	case n < 1024:
		return strconv.Itoa(n) + " B"
	case n < 1<<20:
		return strconv.FormatFloat(float64(n)/1024, 'f', 1, 64) + " KiB"
	}
	return strconv.FormatFloat(float64(n)/(1<<20), 'f', 1, 64) + " MiB"
}

// metricValue writes a metric's latest value with the unit the client gave it, in the
// boards' way: "1.4 ms", "0 /s", "1.2 KiB/s", "0.94", "1". With no unit it is the number
// alone: a unit is never guessed from a name.
func metricValue(v float64, unit string) string {
	switch unit {
	case "By/s", "By":
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return "—"
		}
		s := kibText(int(math.Round(v)))
		if unit == "By/s" {
			s += "/s"
		}
		return s
	case "1/s":
		return formatMetric(v, "") + " /s"
	}
	return formatMetric(v, unit)
}

func (c *Console) clientMetricsView(in ClientInstance, all bool) clientMetrics {
	m := clientMetrics{Reports: in.Pushes, Metrics: in.Metrics, Dropped: in.Dropped, Skipped: in.Skipped, Last: in.Last, All: all, Terminated: in.Terminated}
	if m.Metrics == 0 {
		m.Metrics = len(in.Series)
	}
	if in.Pushes > 1 && in.Last.After(in.First) {
		m.Every = everyText(in.Last.Sub(in.First) / time.Duration(in.Pushes-1))
	}
	var span time.Duration
	rows := make([]metricRow, 0, len(in.Series))
	lastOf := map[string]float64{}
	for _, s := range in.Series {
		drawn := s.Samples
		if len(drawn) > sparkBars {
			drawn = drawn[len(drawn)-sparkBars:]
		}
		if len(drawn) > 1 {
			span = max(span, drawn[len(drawn)-1].T.Sub(drawn[0].T))
		}
		row := metricRow{Name: s.Name, Short: metricShort(s.Name), Label: metricLabel(s.Name), Attrs: s.Attrs, Unit: s.Unit, Last: metricValue(lastSample(s), s.Unit), N: len(s.Samples), Spark: sparkBarsHTML(s.Samples)}
		if worthALook(s.Name) {
			row.Tone = "ok"
			if lastSample(s) > 0 {
				row.Tone = "warn"
			}
		}
		rows = append(rows, row)
		lastOf[s.Name+"\x00"+s.Attrs] = lastSample(s)
	}
	// The metrics the boards name come first, in the boards' order (Traf-07: request
	// latency, poll idle ratio, connection count, bytes fetched); then the rest by what
	// answers "is this client healthy" first (metricRank).
	rank := func(r metricRow) int {
		if i, ok := metricOrder[metricShort(r.Name)]; ok {
			return i - len(metricOrder)
		}
		return metricRank(r.Name, lastOf[r.Name+"\x00"+r.Attrs])
	}
	sort.SliceStable(rows, func(a, b int) bool { return rank(rows[a]) < rank(rows[b]) })
	for _, r := range rows {
		if worthALook(r.Name) {
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
	// The bars cover what the drawn samples cover: from the first bar's report to the
	// last ("the last minute", "the last 25 seconds").
	switch secs := int(span.Round(time.Second) / time.Second); {
	case secs >= 55 && secs <= 65:
		m.Span = "the last minute"
	case secs >= 120:
		m.Span = "the last " + strconv.Itoa(int(span.Round(time.Minute)/time.Minute)) + " minutes"
	case secs > 0:
		m.Span = "the last " + strconv.Itoa(secs) + " seconds"
	}
	return m
}

// everyText is how often something happens, as the boards write it: "5 s", "1.5 s",
// "2 min".
func everyText(d time.Duration) string {
	switch {
	case d < time.Second:
		return strconv.Itoa(int(d.Milliseconds())) + " ms"
	case d < time.Minute:
		s := d.Seconds()
		if math.Abs(s-math.Round(s)) < 0.05 || s >= 10 {
			return strconv.Itoa(int(math.Round(s))) + " s"
		}
		return strconv.FormatFloat(s, 'f', 1, 64) + " s"
	}
	return strconv.Itoa(int(math.Round(d.Minutes()))) + " min"
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
	all := c.traffic.Clients()
	for _, cl := range all {
		if cl.Conn != conn {
			continue
		}
		now := time.Now()
		// The page counts clients, not connections: what is shown is the client the
		// connection belongs to, with each of its connections listed below.
		cr := c.clientRow(cl, now)
		for _, g := range c.groupClients(all, now) {
			for _, mem := range g.Members {
				if mem.Conn == cl.Conn {
					cr = g
				}
			}
		}
		who := cl.ClientID
		if who == "" { // no client id: the software and the address say which (TRF-31).
			who = firstNonEmpty(cl.Software, cl.Remote)
			if cl.Software != "" {
				who += " · " + cl.Remote
			}
		}
		v.Title = who
		v.Sub = joinNonEmpty(" · ", firstNonEmpty(cr.Software, "software not reported"), cr.Principal, cl.Remote)
		v.Status, v.StatusKind = "connected "+spanBoard(now.Sub(cr.Opened)), "pill"
		body := v.Body
		body["C"] = cr
		if cr.Conns > 1 {
			body["Members"] = cr.Members
		}
		body["Reqs"] = c.prefix + "/traffic?" + cr.Filter
		// Of the client's connections, the instance that reported last is the one shown.
		in, ok := c.metricsFor(cl)
		for _, mem := range cr.Members {
			if mi, mok := c.metricsFor(mem.TrafficClient); mok && (!ok || mi.Last.After(in.Last)) {
				in, ok = mi, true
			}
		}
		tone := "client-none"
		allMetrics := r.URL.Query().Get("all") != ""
		if ok {
			tone = "client-metrics"
			// Traf-07's client that reports wears the ok tint; Traf-11's does not.
			v.StatusTone = "ok"
			m := c.clientMetricsView(in, allMetrics)
			m.Who = who
			body["M"] = m
			body["HasMetrics"] = true
		} else if c.metrics != nil {
			body["NoMetrics"] = true
			body["Asked"] = cr.AskedTelemetry
			// A client that does report, to point at.
			for _, g := range c.groupClients(all, now) {
				if g.Who == cr.Who {
					continue
				}
				for _, mem := range g.Members {
					if c.hasMetrics(mem.TrafficClient) {
						body["OtherURL"] = c.inspectURL(KindClient, strconv.FormatUint(mem.Conn, 10))
						body["OtherHref"] = c.prefix + "/traffic?view=clients&open=client:" + strconv.FormatUint(mem.Conn, 10)
						break
					}
				}
				if body["OtherURL"] != nil {
					break
				}
			}
		}
		body["MetricsOn"] = c.metrics != nil
		var gl []struct{ Name, URL string }
		for _, g := range cr.TrafficClient.Groups {
			gl = append(gl, struct{ Name, URL string }{g, c.groupURL(g)})
		}
		body["GroupLinks"] = gl
		c.inspectTraffic(w, v, "", tone)
		return
	}
	v.Gone = true
	v.GoneMsg = "This connection has closed."
	c.inspectTraffic(w, v, "", "client-none")
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
