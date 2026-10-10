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

// query is the filter as a URL query (without the leading "?"), so the live poll and
// the drawer links keep what the person asked for.
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
	TimeStr  string // 15:04:05.000
	Target   string // the topics, or the group.
	Who      string // the client id, else the address.
	Dur      string
	Sizes    string
	DrawerQ  string
	Folded   bool
	SameSpan string // "for 2m10s" on a folded row.
}

func (c *Console) trafficRow(e TrafficEvent) trafficRow {
	row := trafficRow{TrafficEvent: e, TimeStr: e.Time.Format("15:04:05.000"), Folded: e.Count > 1}
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
}

func (c *Console) trafficData(r *http.Request) map[string]any {
	f := trafficFilterFrom(r)
	events := c.traffic.Events(f, trafficRows)
	rows := make([]trafficRow, 0, len(events))
	h := fnv.New64a()
	for _, e := range events {
		row := c.trafficRow(e)
		row.DrawerQ = "seq=" + strconv.FormatUint(e.Seq, 10)
		rows = append(rows, row)
		fmt.Fprint(h, e.Seq, e.Count, ",")
	}
	now := time.Now()
	var clients []clientRow
	for _, cl := range c.traffic.Clients() {
		cr := clientRow{TrafficClient: cl, Who: cl.ClientID, Age: spanText(now.Sub(cl.Opened)), Idle: spanText(now.Sub(cl.Last))}
		if cr.Who == "" {
			cr.Who = cl.Remote
		}
		cr.Filter = TrafficFilter{Client: cr.Who}.query()
		clients = append(clients, cr)
		fmt.Fprint(h, cl.Conn, cl.Requests, cl.ClientID, cl.Software, ";")
	}
	return map[string]any{
		"Rows": rows, "Clients": clients, "Filter": f, "FilterQ": f.query(),
		"Filtered": f != (TrafficFilter{}), "Hash": fmt.Sprintf("%x", h.Sum64()),
	}
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
	c.partial(w, "traffic_live", data)
}

// trafficRequest is the drawer for one request. A row that has folded in another
// request since the page was drawn has a new sequence number; the drawer says so
// rather than showing a different request than the one clicked.
func (c *Console) trafficRequest(w http.ResponseWriter, r *http.Request) {
	if c.traffic == nil {
		c.fail(w, r, fmt.Errorf("%w: there is no traffic view in this console", errNotFound))
		return
	}
	seq, err := strconv.ParseUint(r.URL.Query().Get("seq"), 10, 64)
	if err != nil {
		c.fail(w, r, badInput("seq must be a number"))
		return
	}
	for _, e := range c.traffic.Events(TrafficFilter{}, 0) {
		if e.Seq == seq {
			row := c.trafficRow(e)
			c.partial(w, "traffic_drawer", map[string]any{"R": row})
			return
		}
	}
	c.partial(w, "traffic_drawer", map[string]any{"Gone": true})
}

// timelineRow is one thing a group did, ready to show.
type timelineRow struct {
	TrafficGroupEvent
	TimeStr string
	Label   string
	Tone    string
	Member  string
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
		rows = append(rows, timelineRow{TrafficGroupEvent: e, TimeStr: e.Time.Format("15:04:05.000"), Label: label, Tone: tone, Member: shortID(e.Member)})
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
