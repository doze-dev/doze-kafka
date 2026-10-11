package console

// Traffic: "what did my app just do?"
//
// The console is a wire client and has no business inside the broker, so what the
// broker saw is handed to it through the small interface below, implemented where the
// broker is (cmd/doze-kafka). With no source (the console running apart from the
// broker, say) the Traffic section is not offered at all, rather than offered empty.

import (
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// TrafficSource is what the broker remembers of its clients' requests and of what its
// consumer groups did.
type TrafficSource interface {
	// Events are the requests matching f, latest activity first, at most limit.
	Events(f TrafficFilter, limit int) []TrafficEvent
	// Clients are the open connections, oldest first.
	Clients() []TrafficClient
	// GroupEvents are what a group's coordinator did, oldest first, at most the latest limit.
	GroupEvents(group string, limit int) []TrafficGroupEvent
}

// TrafficFilter narrows the requests. Zero fields match everything.
type TrafficFilter struct {
	Client     string
	API        string
	Topic      string
	Group      string
	ErrorsOnly bool
}

// TrafficEvent is one row: a request, or a run of identical idle ones folded together.
type TrafficEvent struct {
	Seq                                   uint64
	Conn                                  uint64 // the connection it came over (a TrafficClient's Conn, while it is open).
	First, Time                           time.Time
	Count                                 int
	Remote, ClientID, Software, Principal string
	API                                   string
	Version                               int16
	Topics                                []string
	Group, Detail                         string
	Error                                 string
	Duration                              time.Duration
	ReqBytes, RespBytes                   int
}

// TrafficClient is an open connection.
type TrafficClient struct {
	Conn                                  uint64
	Remote, ClientID, Software, Principal string
	Opened, Last                          time.Time
	Requests                              int64
	Groups                                []string
}

// TrafficGroupEvent is something a group's coordinator did.
type TrafficGroupEvent struct {
	Seq        uint64
	Time       time.Time
	Kind       string
	Member     string
	Generation int32
	Detail     string
}

// trafficRows is how many rows the page shows. The source keeps more.
const trafficRows = 200

func trafficFilterFrom(r *http.Request) TrafficFilter {
	q := r.URL.Query()
	return TrafficFilter{
		Client: strings.TrimSpace(q.Get("client")), API: strings.TrimSpace(q.Get("api")),
		Topic: strings.TrimSpace(q.Get("topic")), Group: strings.TrimSpace(q.Get("group")),
		ErrorsOnly: q.Get("errors") != "",
	}
}

// trafficView is what the Traffic page was asked for: the source's own filter, the one
// field of the board ("client, topic, group or request", matched by the console over
// every field at once) and the kind of request.
type trafficView struct {
	TrafficFilter
	Q    string
	Kind string // "", "errors", "produce", "fetch", "groups" or "admin".
}

func trafficViewFrom(r *http.Request) trafficView {
	v := trafficView{TrafficFilter: trafficFilterFrom(r), Q: strings.TrimSpace(r.URL.Query().Get("q")), Kind: r.URL.Query().Get("kind")}
	switch v.Kind {
	case "errors", "produce", "fetch", "groups", "admin":
	default:
		v.Kind = ""
	}
	return v
}

func (v trafficView) query() string {
	s := v.TrafficFilter.query()
	add := func(k, val string) {
		if val == "" {
			return
		}
		if s != "" {
			s += "&"
		}
		s += k + "=" + queryEscape(val)
	}
	add("q", v.Q)
	add("kind", v.Kind)
	return s
}

func (v trafficView) empty() bool { return v == trafficView{} }

// trafficKinds are the board's segmented control, in its order.
var trafficKinds = []struct{ Key, Label string }{
	{"", "all"}, {"errors", "errors"}, {"produce", "produce"}, {"fetch", "fetch"}, {"groups", "groups"}, {"admin", "admin"},
}

// requestKind says which of the board's kinds an API belongs to. An API that is none
// of them (Metadata, ApiVersions, InitProducerId...) is listed only under "all".
func requestKind(api string) string {
	switch api {
	case "Produce":
		return "produce"
	case "Fetch", "ShareFetch":
		return "fetch"
	case "JoinGroup", "SyncGroup", "Heartbeat", "LeaveGroup", "OffsetCommit", "OffsetFetch", "OffsetDelete", "FindCoordinator",
		"ConsumerGroupHeartbeat", "ConsumerGroupDescribe", "DescribeGroups", "ListGroups", "DeleteGroups", "TxnOffsetCommit":
		return "groups"
	case "CreateTopics", "DeleteTopics", "CreatePartitions", "AlterConfigs", "IncrementalAlterConfigs", "DescribeConfigs",
		"CreateAcls", "DeleteAcls", "DescribeAcls", "DeleteRecords", "DescribeCluster", "DescribeLogDirs", "ListTransactions":
		return "admin"
	}
	return ""
}

// matches is the board's one field: any of the client, its software, the address, the
// request, a topic or the group contains what was typed.
func (v trafficView) matches(e TrafficEvent) bool {
	if v.Kind != "" && v.Kind != "errors" && requestKind(e.API) != v.Kind {
		return false
	}
	if v.Q == "" {
		return true
	}
	q := strings.ToLower(v.Q)
	hay := []string{e.ClientID, e.Software, e.Remote, e.API, e.Group, e.Principal}
	hay = append(hay, e.Topics...)
	for _, h := range hay {
		if strings.Contains(strings.ToLower(h), q) {
			return true
		}
	}
	return false
}

// query is the filter as a URL query (without the leading "?"), so the live poll and
// the inspector links keep what the person asked for.
func (f TrafficFilter) query() string {
	var parts []string
	add := func(k, v string) {
		if v != "" {
			parts = append(parts, k+"="+queryEscape(v))
		}
	}
	add("client", f.Client)
	add("api", f.API)
	add("topic", f.Topic)
	add("group", f.Group)
	if f.ErrorsOnly {
		parts = append(parts, "errors=1")
	}
	return strings.Join(parts, "&")
}

func queryEscape(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// trafficRow is a request, ready to show.
type trafficRow struct {
	TrafficEvent
	ConnKey  string // the open connection it came over, to open that client; "" when it has closed.
	Target   string // the topics, or the group.
	Who      string // the client id, else the address.
	Dur      string
	Sizes    string
	OpenKey  string
	Folded   bool
	SameSpan string // "for 2m10s" on a folded row.
}

func (c *Console) trafficRow(e TrafficEvent) trafficRow {
	row := trafficRow{TrafficEvent: e, Folded: e.Count > 1}
	switch {
	case len(e.Topics) > 0:
		row.Target = strings.Join(e.Topics, ", ")
	case e.Group != "":
		row.Target = e.Group
	}
	row.Who = e.ClientID
	if row.Who == "" {
		row.Who = e.Remote
	}
	row.Dur = shortDuration(e.Duration)
	row.Sizes = humanBytes(e.ReqBytes) + " in · " + humanBytes(e.RespBytes) + " out"
	if row.Folded {
		row.SameSpan = "over " + shortDuration(e.Time.Sub(e.First))
	}
	return row
}

// shortDuration is a duration a person reads at a glance: 0.4 ms, 12 ms, 2.5 s, 3m10s.
func shortDuration(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%.1f ms", float64(d)/float64(time.Millisecond))
	case d < time.Second:
		return fmt.Sprintf("%d ms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1f s", d.Seconds())
	default:
		d = d.Round(time.Second)
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	}
}

// spanText is how long ago something was, for a column of ages: "just now", 12s, 3m 10s, 2h 5m.
func spanText(d time.Duration) string {
	switch {
	case d < 2*time.Second:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm %02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh %02dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

// clientRow is an open connection, ready to show.
type clientRow struct {
	TrafficClient
	Who    string
	Age    string
	Idle   string
	Filter string // the query that shows only this client's requests.
	// Doing is what its requests in the tail say it is: producing, consuming, in which
	// groups. Refused is how many of its kept requests the broker answered with an error.
	Doing   string
	Refused int
}

// capacityOf is how many rows the source keeps, when it says.
func capacityOf(src TrafficSource) int {
	if c, ok := src.(interface{ Capacity() int }); ok {
		return c.Capacity()
	}
	return 0
}

func (c *Console) trafficData(r *http.Request) map[string]any {
	v := trafficViewFrom(r)
	// Everything the recorder holds, newest first: the summary counts it all, and the
	// table is what is left after the page's filter.
	all := c.traffic.Events(TrafficFilter{}, 0)
	src := v.TrafficFilter
	if v.Kind == "errors" {
		src.ErrorsOnly = true
	}
	events := c.traffic.Events(src, 0)
	now := time.Now()
	clients := c.traffic.Clients()
	open := map[uint64]bool{}
	for _, cl := range clients {
		open[cl.Conn] = true
	}

	rows := make([]trafficRow, 0, trafficRows)
	h := fnv.New64a()
	for _, e := range events {
		if !v.matches(e) {
			continue
		}
		if len(rows) == trafficRows {
			break
		}
		row := c.trafficRow(e)
		row.OpenKey = requestKey(e)
		if open[e.Conn] {
			row.ConnKey = strconv.FormatUint(e.Conn, 10)
		}
		rows = append(rows, row)
		fmt.Fprint(h, e.Seq, e.Count, ",")
	}

	// The summary: what each number is counted over is in its words. A row that folded
	// repeats counts them all; one that began before the minute counts only its latest
	// request in it, so the number is then "at least".
	var lastMinute, errs int
	lower := false
	byRemote := map[string]*remoteActivity{}
	for _, e := range all {
		if e.Error != "" {
			errs += max(e.Count, 1)
		}
		switch {
		case !e.First.Before(now.Add(-time.Minute)):
			lastMinute += max(e.Count, 1)
		case !e.Time.Before(now.Add(-time.Minute)):
			lastMinute++
			if e.Count > 1 {
				lower = true
			}
		}
		a := byRemote[e.Remote]
		if a == nil {
			a = &remoteActivity{}
			byRemote[e.Remote] = a
		}
		switch requestKind(e.API) {
		case "produce":
			a.produces = true
		case "fetch":
			a.fetches = true
		}
		if e.Error != "" {
			a.refused += max(e.Count, 1)
		}
	}

	var crows []clientRow
	for _, cl := range clients {
		cr := clientRow{TrafficClient: cl, Who: cl.ClientID, Age: spanText(now.Sub(cl.Opened)), Idle: spanText(now.Sub(cl.Last))}
		if cr.Who == "" {
			cr.Who = cl.Remote
		}
		cr.Filter = TrafficFilter{Client: cr.Who}.query()
		var does []string
		if a := byRemote[cl.Remote]; a != nil {
			if a.produces {
				does = append(does, "producer")
			}
			if a.fetches {
				does = append(does, "consumer")
			}
			cr.Refused = a.refused
		}
		for _, g := range cl.Groups {
			does = append(does, g)
		}
		cr.Doing = strings.Join(does, " · ")
		crows = append(crows, cr)
		fmt.Fprint(h, cl.Conn, cl.Requests, cl.ClientID, cl.Software, ";")
	}
	cards := c.metricCards()
	for _, card := range cards {
		fmt.Fprint(h, card.ID, card.Pushes, card.Terminated, ";")
	}
	fmt.Fprint(h, "|", lastMinute, errs, len(all))

	kept := ""
	if n := capacityOf(c.traffic); n > 0 {
		kept = groupDigits(int64(n))
	} else {
		kept = "few thousand requests"
	}
	var kinds []map[string]any
	for _, k := range trafficKinds {
		kv := v
		kv.Kind = k.Key
		kv.ErrorsOnly = false // the segmented control says it; an old ?errors=1 does not stay beside it.
		qs := kv.query()
		u := c.prefix + "/traffic"
		if qs != "" {
			u += "?" + qs
		}
		kinds = append(kinds, map[string]any{"Key": k.Key, "Label": k.Label, "URL": u, "On": v.Kind == k.Key})
	}
	return map[string]any{
		"HasClientMetrics": c.metrics != nil, "Metrics": cards,
		"Rows": rows, "Clients": crows, "Filter": v, "FilterQ": v.query(), "Kinds": kinds,
		"Filtered": !v.empty(), "Hash": fmt.Sprintf("%x", h.Sum64()),
		"LastMinute": lastMinute, "LastMinuteLowerBound": lower, "ErrorCount": errs, "KeptText": kept,
		"InspDefault": "traffic",
	}
}

// remoteActivity is what one connection's kept requests say it does.
type remoteActivity struct {
	produces, fetches bool
	refused           int
}

func (c *Console) trafficPage(w http.ResponseWriter, r *http.Request) {
	if c.traffic == nil {
		c.fail(w, r, fmt.Errorf("%w: there is no traffic view in this console", errNotFound))
		return
	}
	data := c.trafficData(r)
	data["Page"], data["Title"] = "traffic", "Traffic"
	c.render(w, r, "traffic_detail", data)
}

// trafficLive is the polled region: connected clients and the request tail.
func (c *Console) trafficLive(w http.ResponseWriter, r *http.Request) {
	if c.traffic == nil {
		c.fail(w, r, fmt.Errorf("%w: there is no traffic view in this console", errNotFound))
		return
	}
	data := c.trafficData(r)
	if liveUnchanged(w, r, data["Hash"].(string)) {
		return
	}
	// The summary line and the open connections are outside this region: they come
	// back with it, out of band.
	data["OOB"] = true
	c.partial(w, "traffic_live", data)
}

// trafficRequest is the inspector for one request, at its earlier address (the
// inspector route is the one the page uses). A row is named by its row id; a
// sequence number, which moves whenever the row folds in a repeat, is still
// accepted and is turned into the id of the row that has it now.
func (c *Console) trafficRequest(w http.ResponseWriter, r *http.Request) {
	if c.trafficOff(w, r) {
		return
	}
	c.inspectRequest(w, r, r.URL.Query().Get("key"))
}

// timelineRow is one thing a group did, ready to show.
type timelineRow struct {
	TrafficGroupEvent
	Label  string
	Tone   string
	Member string
}

// groupTimeline is what the group's coordinator did, newest first. It is what answers
// "why does my consumer keep rebalancing": the order of joins, expiries and
// generations. nil with no traffic source.
func (c *Console) groupTimeline(group string) []timelineRow {
	if c.traffic == nil {
		return nil
	}
	events := c.traffic.GroupEvents(group, 60)
	rows := make([]timelineRow, 0, len(events))
	for i := len(events) - 1; i >= 0; i-- {
		e := events[i]
		label, tone := timelineKind(e.Kind)
		rows = append(rows, timelineRow{TrafficGroupEvent: e, Label: label, Tone: tone, Member: shortID(e.Member)})
	}
	return rows
}

func timelineKind(kind string) (label, tone string) {
	switch kind {
	case "joined":
		return "joined", "ok"
	case "left":
		return "left", "dim"
	case "expired":
		return "session expired", "bad"
	case "dropped":
		return "dropped", "bad"
	case "rebalance":
		return "rebalance", "warn"
	case "generation":
		return "new generation", "ok"
	case "assigned":
		return "assigned", "ok"
	case "epoch":
		return "new epoch", "ok"
	}
	return kind, "dim"
}
