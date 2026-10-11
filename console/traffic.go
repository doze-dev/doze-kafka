package console

// Traffic: "what did my app just do?"
//
// The console is a wire client and has no business inside the broker, so what the
// broker saw is handed to it through the small interface below, implemented where the
// broker is (cmd/doze-kafka). With no source (the console running apart from the
// broker, say) the Traffic section is not offered at all, rather than offered empty.
//
// The page is the boards Main and Traf-02 to Traf-12: the requests (Main), filtered
// (Traf-02), errors only with their explainers (Traf-03), one request (Traf-04) or a
// folded row (Traf-05) in the inspector, the clients (Traf-06, Traf-07, Traf-11),
// paused (Traf-08), empty (Traf-09), disconnected (Traf-10) and the story of a group's
// rebalance (Traf-12). The page is this file; the inspector's bodies are in
// traffic_inspect.go and the explainers and the story in traffic_story.go.

import (
	"fmt"
	"hash/fnv"
	"html/template"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// TrafficSource is what the broker remembers of its clients' requests and of what its
// consumer groups did.
type TrafficSource interface {
	// Events are the requests matching f, latest activity first, at most limit.
	Events(f TrafficFilter, limit int) []TrafficEvent
	// Row is one request by its id, while the recorder still holds it.
	Row(id string) (TrafficEvent, bool)
	// NewSince counts the requests matching f that began after t: what a paused page
	// has missed. atLeast says the count is a lower bound.
	NewSince(f TrafficFilter, t time.Time) (n int, atLeast bool)
	// Clients are the open connections that have sent a request, oldest first.
	Clients() []TrafficClient
	// GroupEvents are what a group's coordinator did, oldest first, at most the latest limit.
	GroupEvents(group string, limit int) []TrafficGroupEvent
	// Story is a group's requests nested under the rebalances they belong to.
	Story(group string) TrafficStory
	// Window counts the requests and the errors of the last d (at most five minutes).
	Window(d time.Duration) TrafficWindow
	// Refusals groups the error rows held by what they said, most often first.
	Refusals() []TrafficRefusal
	// Capacity is how many request rows are kept.
	Capacity() int
	// Since is when the recorder began, which is when the broker did.
	Since() time.Time
}

// TrafficFilter narrows the requests. Zero fields match everything.
type TrafficFilter struct {
	Client string
	API    string
	Topic  string
	// Group matches the rows that name the group and the fetches of its members.
	Group string
	// Kind is produce, fetch, groups or admin.
	Kind       string
	ErrorsOnly bool
}

// TrafficEvent is one row: a request, or a run of identical idle ones folded together.
type TrafficEvent struct {
	ID                                    string // the row's id: it does not change when the row folds.
	Seq                                   uint64
	Conn                                  uint64 // the connection it came over (a TrafficClient's Conn, while it is open).
	First, Time                           time.Time
	Count                                 int
	Remote, ClientID, Software, Principal string
	API                                   string
	Version                               int16
	Kind                                  string
	Correlation                           int32
	Topics                                []string
	TopicsMore                            int // topics the request named that the recorder did not keep.
	Group, ViaGroup, TxnID, Detail        string
	Error                                 string // the Kafka error's name; empty when it went well.
	ErrorCode                             int16
	// Class is what the error means: "expected", "refused", "denied", "not-implemented"
	// or "failed"; "" when there was none.
	Class string
	// Closed says the broker closed the connection because of this request, and why.
	Closed              string
	Duration            time.Duration
	ReqBytes, RespBytes int
	Facts               TrafficFacts
	// Ticks are the latest repeats of a folded row, oldest first.
	Ticks []TrafficTick
}

// TrafficFacts is what the inspector shows beyond a row.
type TrafficFacts struct {
	Acks           int
	HasAcks        bool
	Partitions     []TrafficPart
	PartitionsMore int
	Decoded        string
}

// TrafficPart is how one partition of a request went.
type TrafficPart struct {
	Topic          string
	Partition      int32
	Offset         int64
	HasOffset      bool
	Records, Bytes int
	ErrorCode      int16
	Error          string
}

// TrafficTick is one repeat of a folded row.
type TrafficTick struct {
	At       time.Time
	Duration time.Duration
	Class    string
}

// TrafficClient is an open connection.
type TrafficClient struct {
	Conn                                  uint64
	Remote, ClientID, Software, Principal string
	Opened, Last                          time.Time
	Requests                              int64
	Groups                                []string
	TxnID                                 string
	Produces, Fetches, AskedTelemetry     bool
	Req1m, Req5m, Err5m                   int
	Streak                                int
	LastError                             string
}

// TrafficGroupEvent is something a group's coordinator did.
type TrafficGroupEvent struct {
	Seq        uint64
	Time       time.Time
	Kind       string
	Member     string
	Generation int32
	Detail     string
	Members    int
	Cause      string
}

// TrafficWindow is what the counters say about a stretch of time.
type TrafficWindow struct{ Requests, Errors int }

// TrafficRefusal is one error as the recorder has seen it.
type TrafficRefusal struct {
	Code        int16
	Name, Class string
	Count       int
	Clients     []TrafficClientCount
	First, Last time.Time
	Latest      string // the id of its newest row.
	API         string
	Topics      []string
}

// TrafficClientCount is a client and how many times.
type TrafficClientCount struct {
	Client string
	Count  int
}

// TrafficStory is a group's recent past: its rebalances, newest first, each with the
// requests that belong to it.
type TrafficStory struct {
	Group, Protocol  string
	Rebalances       []TrafficRebalance
	Before           []TrafficEvent
	Generations      []TrafficGeneration
	Requests, Folded int
	Topics           []string
}

// TrafficRebalance is one rebalance of a group.
type TrafficRebalance struct {
	Start, End             time.Time
	Done                   bool
	From, To               int32
	Cause                  string
	Members, MembersBefore int
	Rows                   []TrafficEvent
}

// TrafficGeneration is a row of the generations table.
type TrafficGeneration struct {
	Gen     int32
	Cause   string
	Members int
	At      time.Time
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
// every field at once), the kind of request, and which of the two views it is.
type trafficView struct {
	TrafficFilter
	Q    string
	Kind string // "", "errors", "produce", "fetch", "groups" or "admin".
	// View is "requests" (Main) or "clients" (Traf-06).
	View string
	// Flat turns the rebalance story of a group filter back into a plain list.
	Flat bool
}

func trafficViewFrom(r *http.Request) trafficView {
	q := r.URL.Query()
	v := trafficView{TrafficFilter: trafficFilterFrom(r), Q: strings.TrimSpace(q.Get("q")), Kind: q.Get("kind"), View: "requests", Flat: q.Get("flat") != ""}
	switch v.Kind {
	case "errors", "produce", "fetch", "groups", "admin":
	default:
		v.Kind = ""
	}
	if q.Get("view") == "clients" {
		v.View = "clients"
	}
	return v
}

// query is the view as a URL query (without the leading "?"), so the live poll and
// the inspector links keep what the person asked for.
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
	if v.View == "clients" {
		add("view", "clients")
	}
	if v.Flat {
		add("flat", "1")
	}
	return s
}

func (v trafficView) empty() bool {
	return v.TrafficFilter == (TrafficFilter{}) && v.Q == "" && v.Kind == ""
}

// source is the filter the recorder is asked: the kind chip is its own filter, except
// "errors", which is the recorder's errors-only.
func (v trafficView) source() TrafficFilter {
	f := v.TrafficFilter
	switch v.Kind {
	case "errors":
		f.ErrorsOnly = true
	case "produce", "fetch", "groups", "admin":
		f.Kind = v.Kind
	}
	return f
}

// trafficKinds are the board's segmented control, in its order.
var trafficKinds = []struct{ Key, Label string }{
	{"", "all"}, {"errors", "errors"}, {"produce", "produce"}, {"fetch", "fetch"}, {"groups", "groups"}, {"admin", "admin"},
}

// matches is the board's one field: any of the client, its software, the address, the
// request, a topic or the group contains what was typed.
func (v trafficView) matches(e TrafficEvent) bool {
	if v.Q == "" {
		return true
	}
	q := strings.ToLower(v.Q)
	hay := []string{e.ClientID, e.Software, e.Remote, e.API, e.Group, e.ViaGroup, e.TxnID, e.Principal, e.Error}
	hay = append(hay, e.Topics...)
	for _, h := range hay {
		if strings.Contains(strings.ToLower(h), q) {
			return true
		}
	}
	return false
}

// query is the filter as a URL query (without the leading "?").
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
	// OpenKey is the id the inspector names the row by.
	OpenKey string
	// ConnKey is the open connection it came over, to open that client; "" once it has
	// closed.
	ConnKey  string
	Target   string // the topics, or the group, or the transactional id.
	Who      string // the client id, else the address.
	Dur      string
	Sizes    string
	Folded   bool
	SameSpan string // "over 2m10s" on a folded row.
	// Tone is the colour word of the result: "ok", "deny" (refused, denied, expected),
	// "warn" (not implemented) or "bad" (failed).
	Tone string
	// RowClass is the table row's class: the tint of a refusal.
	RowClass string
	// Result is what the Result column reads: "ok" or the error's name.
	Result string
	// ResultTitle is the hover on the result.
	ResultTitle string
	// DetailText is what the Detail column reads: the row's detail, or for a periodic
	// repeat its rhythm ("every 3 s, since 14:34:15").
	DetailText string
	Interval   string
	// Rhythm says the detail is the row's rhythm ("every 3 s, since 14:34:15"), the time
	// being drawn by the template so that it is in the browser's zone.
	Rhythm bool
}

// resultTone maps an error class to the board's four colours: purple for what real
// Kafka would also refuse and for what an ACL decided, amber for not implemented and
// red only for the broker being unhappy.
func resultTone(class string) string {
	switch class {
	case "":
		return "ok"
	case "failed":
		return "bad"
	case "not-implemented":
		return "warn"
	}
	return "deny"
}

// classWord is the class as the person reads it.
func classWord(class string) string {
	switch class {
	case "expected":
		return "Expected"
	case "refused":
		return "Refused"
	case "denied":
		return "Denied"
	case "not-implemented":
		return "Not implemented"
	case "failed":
		return "Failed"
	}
	return ""
}

// periodic are the APIs whose repeats are a rhythm, and are written as one.
var periodic = map[string]bool{"Heartbeat": true, "ConsumerGroupHeartbeat": true, "PushTelemetry": true}

func (c *Console) trafficRow(e TrafficEvent) trafficRow {
	row := trafficRow{TrafficEvent: e, Folded: e.Count > 1, OpenKey: e.ID}
	switch {
	case len(e.Topics) > 0:
		row.Target = strings.Join(e.Topics, ", ")
		if e.TopicsMore > 0 {
			row.Target += " and " + strconv.Itoa(e.TopicsMore) + " more"
		}
	case e.Group != "":
		row.Target = e.Group
	case e.TxnID != "":
		row.Target = "tx " + e.TxnID
	case e.ViaGroup != "":
		row.Target = ""
	}
	row.Who = e.ClientID
	if row.Who == "" {
		row.Who = e.Remote
	}
	row.Dur = shortDuration(e.Duration)
	row.Sizes = humanBytes(e.ReqBytes) + " in · " + humanBytes(e.RespBytes) + " out"
	row.DetailText = e.Detail
	if row.Folded {
		row.SameSpan = "over " + shortDuration(e.Time.Sub(e.First))
		row.Interval = intervalText(e)
		if periodic[e.API] && row.Interval != "" {
			row.DetailText = row.Interval + ", since " + whenText(e.First, StyleClock)
			row.Rhythm = true
		}
	}
	row.Tone = resultTone(e.Class)
	row.Result = "ok"
	if e.Error != "" {
		row.Result = e.Error
		row.ResultTitle = "The response carried this error"
		switch e.Class {
		case "expected":
			row.ResultTitle = "Part of the normal flow, not an error: the client does this next"
		case "refused":
			row.ResultTitle = "Refused: real Kafka would say no too"
		case "denied":
			row.ResultTitle = "Denied: an ACL or a login decided"
		case "not-implemented":
			row.ResultTitle = "Not implemented by this broker"
		case "failed":
			row.ResultTitle = "Failed: the broker could not do it"
		}
		if e.Class != "expected" {
			row.RowClass = "errrow " + e.Class
		}
	}
	return row
}

// intervalText is how often a folded row repeated: "every 3 s".
func intervalText(e TrafficEvent) string {
	if e.Count < 2 {
		return ""
	}
	each := e.Time.Sub(e.First) / time.Duration(e.Count-1)
	switch {
	case each < 100*time.Millisecond:
		return "every " + shortDuration(each)
	case each < time.Second:
		return "every " + strconv.Itoa(int(each.Milliseconds())) + " ms"
	case each < time.Minute:
		s := each.Seconds()
		if math.Abs(s-math.Round(s)) < 0.05 || s >= 10 {
			return "every " + strconv.Itoa(int(math.Round(s))) + " s"
		}
		return "every " + strconv.FormatFloat(s, 'f', 1, 64) + " s"
	}
	return "every " + strconv.Itoa(int(each.Minutes()+0.5)) + " min"
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
	Who  string
	Age  string
	Idle string
	// Filter is the query that shows only this client's requests, and Href the address
	// that carries it (a URL, so that it is not escaped a second time: TRF-9).
	Filter string
	Href   template.URL
	// Doing is what its requests say it is: "producer · tx checkout-1", the groups it
	// is in, "none (producer)".
	Doing string
	// Note is the line under it in the connected-clients list.
	Note string
	// Rate is the error rate over its last five minutes, as the Clients view writes it.
	Rate    string
	RateTon string
	// NeedsALook marks a client the page raises: refused several times in a row, or
	// most of what it asked was refused.
	NeedsALook bool
	Groups     string
	Metrics    bool
	// Conns says how many connections the client has open (a client is its client id:
	// one app often holds several connections, and the page counts apps). Members are
	// those connections, oldest first, and Keys the inspector keys of them, so that a
	// row knows it is the one that is open whichever of its connections was opened.
	Conns   int
	More    int // the connections beyond the one whose address is shown.
	Members []clientRow
	Keys    string
}

// groupClients folds the connections of one client id into one row, in the order the
// clients first connected. A connection with no client id is named by its address, so
// it stays a row of its own (TRF-31).
func (c *Console) groupClients(cls []TrafficClient, now time.Time) []clientRow {
	var order []string
	by := map[string][]TrafficClient{}
	for _, cl := range cls {
		who := cl.ClientID
		if who == "" {
			who = cl.Remote
		}
		if _, ok := by[who]; !ok {
			order = append(order, who)
		}
		by[who] = append(by[who], cl)
	}
	out := make([]clientRow, 0, len(order))
	for _, who := range order {
		conns := by[who]
		if len(conns) == 1 {
			cr := c.clientRow(conns[0], now)
			cr.Conns, cr.Members, cr.Keys = 1, []clientRow{cr}, "client:"+strconv.FormatUint(conns[0].Conn, 10)
			out = append(out, cr)
			continue
		}
		// The connection that spoke last stands for the client; the rest of the row is the
		// sum of all of them.
		m := conns[0]
		for _, cl := range conns[1:] {
			if cl.Last.After(m.Last) {
				m = cl
			}
		}
		m.Opened, m.Requests, m.Req1m, m.Req5m, m.Err5m, m.Streak = conns[0].Opened, 0, 0, 0, 0, 0
		m.Groups, m.Produces, m.Fetches, m.AskedTelemetry = nil, false, false, false
		var members []clientRow
		var keys []string
		var principals []string
		for _, cl := range conns {
			if cl.Opened.Before(m.Opened) {
				m.Opened = cl.Opened
			}
			m.Requests += cl.Requests
			m.Req1m += cl.Req1m
			m.Req5m += cl.Req5m
			m.Err5m += cl.Err5m
			if cl.Streak > m.Streak {
				m.Streak, m.LastError = cl.Streak, cl.LastError
			}
			for _, g := range cl.Groups {
				if !slices.Contains(m.Groups, g) {
					m.Groups = append(m.Groups, g)
				}
			}
			m.Produces = m.Produces || cl.Produces
			m.Fetches = m.Fetches || cl.Fetches
			m.AskedTelemetry = m.AskedTelemetry || cl.AskedTelemetry
			if m.Software == "" {
				m.Software = cl.Software
			}
			if m.TxnID == "" {
				m.TxnID = cl.TxnID
			}
			if cl.Principal != "" && !slices.Contains(principals, cl.Principal) {
				principals = append(principals, cl.Principal)
			}
			members = append(members, c.clientRow(cl, now))
			keys = append(keys, "client:"+strconv.FormatUint(cl.Conn, 10))
		}
		m.Principal = strings.Join(principals, ", ")
		cr := c.clientRow(m, now)
		cr.NeedsALook = false
		for _, mem := range members {
			cr.NeedsALook = cr.NeedsALook || mem.NeedsALook
		}
		cr.Conns, cr.More, cr.Members, cr.Keys = len(conns), len(conns)-1, members, strings.Join(keys, " ")
		out = append(out, cr)
	}
	return out
}

// clientCount is how many clients (distinct client ids, or an address for a connection
// with none) the connections are.
func clientCount(cls []TrafficClient) int {
	seen := map[string]bool{}
	for _, cl := range cls {
		who := cl.ClientID
		if who == "" {
			who = cl.Remote
		}
		seen[who] = true
	}
	return len(seen)
}

// needsALook is the rule behind "1 client needs a look": three refusals in a row, or
// three or more in five minutes making up at least half of what it asked.
func needsALook(c TrafficClient) bool {
	return c.Streak >= 3 || (c.Err5m >= 3 && c.Req5m > 0 && c.Err5m*2 >= c.Req5m)
}

func (c *Console) clientRow(cl TrafficClient, now time.Time) clientRow {
	cr := clientRow{TrafficClient: cl, Who: cl.ClientID, Age: spanText(now.Sub(cl.Opened)), Idle: spanText(now.Sub(cl.Last))}
	if cr.Who == "" {
		cr.Who = cl.Remote
	}
	cr.Filter = TrafficFilter{Client: cr.Who}.query()
	cr.Href = template.URL(c.prefix + "/traffic?" + cr.Filter)
	var does []string
	switch {
	case cl.Produces && cl.TxnID != "":
		does = append(does, "producer · tx "+cl.TxnID)
	case cl.Produces:
		does = append(does, "producer")
	}
	if cl.Fetches && len(cl.Groups) == 0 {
		does = append(does, "consumer")
	}
	does = append(does, cl.Groups...)
	cr.Doing = strings.Join(does, " · ")
	cr.NeedsALook = needsALook(cl)
	switch {
	case cl.Req5m == 0:
		cr.Rate, cr.RateTon = "—", "mut"
	default:
		pct := float64(cl.Err5m) * 100 / float64(cl.Req5m)
		switch {
		case cl.Err5m == 0:
			cr.Rate, cr.RateTon = "0 %", "ok"
		case pct < 10:
			cr.Rate, cr.RateTon = strconv.FormatFloat(pct, 'f', 1, 64)+" %", ""
		default:
			cr.Rate, cr.RateTon = strconv.Itoa(int(pct+0.5))+" %", "warn"
		}
	}
	switch {
	case len(cl.Groups) > 0:
		cr.Groups = strings.Join(cl.Groups, ", ")
		if cl.Streak > 0 && cl.Streak >= 3 {
			cr.Groups += ", refused"
		}
	case cl.Produces:
		cr.Groups = "none (producer)"
	default:
		cr.Groups = "none"
	}
	return cr
}

// ringText says where a "written by" comes from and how far back it reaches: the
// requests the broker keeps in memory, the oldest of which is so old. short is the
// words beside the number, long is what the tooltip says.
func (c *Console) ringText(now time.Time) (short, long string) {
	events := c.traffic.Events(TrafficFilter{}, 0)
	kept := "the recent requests the broker keeps in memory"
	if n := c.traffic.Capacity(); n > 0 {
		kept = "the last " + groupDigits(int64(n)) + " requests the broker keeps in memory"
	}
	if len(events) == 0 {
		return "from recent requests; none are kept yet",
			"Written by is counted from " + kept + ", and none are kept yet. It does not survive a restart."
	}
	oldest := events[0].First
	for _, e := range events {
		if e.First.Before(oldest) {
			oldest = e.First
		}
	}
	age := spanText(now.Sub(oldest))
	return "from recent requests, the oldest " + age + " old",
		"Written by is counted from " + kept + ". It reaches back " + age + ": a client that wrote earlier, or whose requests have been pushed out, is not listed. It does not survive a restart."
}

// trafficData is everything the Traffic page, and its poll, draw.
func (c *Console) trafficData(r *http.Request) map[string]any {
	v := trafficViewFrom(r)
	now := time.Now()
	clients := c.traffic.Clients()
	open := map[uint64]bool{}
	for _, cl := range clients {
		open[cl.Conn] = true
	}
	win := c.traffic.Window(time.Minute)

	h := fnv.New64a()
	data := map[string]any{
		"Filter": v, "FilterQ": v.query(), "Filtered": !v.empty(), "InspDefault": "traffic",
		"HasClientMetrics": c.metrics != nil, "OOB": false,
		"TView": v.View,
	}

	// The clients, in the order they connected.
	crows := c.groupClients(clients, now)
	needLook := 0
	for i := range crows {
		if c.metrics != nil {
			for _, mem := range crows[i].Members {
				if c.hasMetrics(mem.TrafficClient) {
					crows[i].Metrics = true
				}
			}
		}
		if crows[i].NeedsALook {
			needLook++
		}
	}
	for _, cl := range clients {
		fmt.Fprint(h, cl.Conn, cl.Requests, cl.ClientID, cl.Software, cl.Streak, cl.Req5m, ";")
	}
	data["Clients"], data["NeedLook"] = crows, needLook
	data["ClientCount"], data["ConnCount"] = len(crows), len(clients)

	kept := "few thousand requests"
	if n := c.traffic.Capacity(); n > 0 {
		kept = groupDigits(int64(n))
	}
	data["KeptText"] = kept
	data["LastMinute"], data["ErrorCount"] = win.Requests, win.Errors
	data["Since"] = c.traffic.Since()
	fmt.Fprint(h, "|", len(clients), win.Requests, win.Errors, v.View)

	// The tabs' Clients badge is the number of connected clients.
	if v.View == "clients" {
		data["Attention"] = c.attention(crows)
		// Traf-06 draws no panel: the table has the clients, so the inspector's own
		// list of them would show them twice and take 400 px the table's nine
		// columns need. A selected client opens it.
		data["InspDefault"] = ""
		data["Hash"] = fmt.Sprintf("%x", h.Sum64())
		data["Rows"] = []trafficRow(nil)
		c.trafficKindsData(data, v)
		return data
	}

	// The rows: everything the recorder holds that the page's filter lets through,
	// newest first; the table shows the first trafficRows of them, and the heading says
	// how many matched.
	all := c.traffic.Events(TrafficFilter{}, 0)
	events := c.traffic.Events(v.source(), 0)
	totalReq := 0
	for _, e := range all {
		totalReq += max(e.Count, 1)
	}
	var rows []trafficRow
	matchedRows, matchedReq := 0, 0
	for _, e := range events {
		if !v.matches(e) {
			continue
		}
		matchedRows++
		matchedReq += max(e.Count, 1)
		if len(rows) >= trafficRows {
			continue
		}
		row := c.trafficRow(e)
		if open[e.Conn] {
			row.ConnKey = strconv.FormatUint(e.Conn, 10)
		}
		rows = append(rows, row)
		fmt.Fprint(h, e.ID, e.Seq, e.Count, ",")
	}
	data["Rows"] = rows
	data["Matched"], data["MatchedRows"], data["TotalRequests"] = matchedReq, matchedRows, totalReq
	data["Cut"] = matchedRows > len(rows)
	data["Shown"] = len(rows)
	// The cursor a paused page counts from: the start of the newest row shown.
	var newest time.Time
	for _, row := range rows {
		if row.Time.After(newest) {
			newest = row.Time
		}
	}
	data["Newest"] = newest
	data["NewestMs"] = newest.UnixMilli()

	// The refusals of what the page lists: the errors view's explainers, and the line
	// under the table.
	refusals := c.traffic.Refusals()
	data["Refused"] = 0
	failed := 0
	refusedRows := 0
	for _, rf := range refusals {
		if rf.Class == "failed" {
			failed += rf.Count
		}
		refusedRows += rf.Count
	}
	data["RefusalCount"], data["FailedCount"], data["RefusedMatching"] = refusedRows, failed, c.countErrors(rows)
	fmt.Fprint(h, "|", refusedRows, len(refusals))
	for _, rf := range refusals {
		fmt.Fprint(h, rf.Name, rf.Count, ";")
	}

	// Which summary the inspector shows until a row or a client replaces it.
	switch {
	case v.Kind == "errors" || v.ErrorsOnly:
		data["InspDefault"] = "errors"
		data["ErrorSummary"] = c.errorSummary(r.Context(), refusals)
	case v.Topic != "":
		data["InspDefault"] = "touched"
		data["Touched"] = c.touched(v.Topic)
	case v.Group != "" && !v.Flat:
		data["InspDefault"] = "story"
	}
	if v.Group != "" && !v.Flat {
		fv := v
		fv.Flat = true
		data["FlatURL"] = c.prefix + "/traffic?" + fv.query()
		st := c.traffic.Story(v.Group)
		data["Story"] = c.storyView(st, open)
		data["HasStory"] = true
		fmt.Fprint(h, "|story", st.Requests, len(st.Rebalances))
		for _, rb := range st.Rebalances {
			fmt.Fprint(h, rb.Start.UnixNano(), rb.Done, len(rb.Rows), ";")
		}
		if data["InspDefault"] == "story" {
			data["GroupSummary"] = c.groupSummary(r.Context(), v.Group, st)
		}
	}
	data["Hash"] = fmt.Sprintf("%x", h.Sum64())
	c.trafficKindsData(data, v)
	return data
}

// countErrors counts the error rows in what is shown.
func (c *Console) countErrors(rows []trafficRow) int {
	n := 0
	for _, r := range rows {
		if r.Error != "" && r.Class != "expected" {
			n++
		}
	}
	return n
}

// trafficKindsData adds the segmented control and the filter pills.
func (c *Console) trafficKindsData(data map[string]any, v trafficView) {
	var kinds []map[string]any
	for _, k := range trafficKinds {
		kv := v
		kv.Kind = k.Key
		kv.ErrorsOnly = false // the segmented control says it; an old ?errors=1 does not stay beside it.
		kv.View = "requests"
		qs := kv.query()
		u := c.prefix + "/traffic"
		if qs != "" {
			u += "?" + qs
		}
		kinds = append(kinds, map[string]any{"Key": k.Key, "Label": k.Label, "URL": u, "On": v.Kind == k.Key || (k.Key == "errors" && v.Kind == "" && v.ErrorsOnly)})
	}
	data["Kinds"] = kinds

	// The pills of a structured filter (Traf-02): each says what it narrows and removes
	// itself; "clear" removes them all.
	type pill struct{ Name, Value, URL string }
	var pills []pill
	without := func(mod func(*trafficView)) string {
		x := v
		mod(&x)
		qs := x.query()
		if qs == "" {
			return c.prefix + "/traffic"
		}
		return c.prefix + "/traffic?" + qs
	}
	if v.Topic != "" {
		pills = append(pills, pill{"topic", v.Topic, without(func(x *trafficView) { x.Topic = "" })})
	}
	if v.Group != "" {
		pills = append(pills, pill{"group", v.Group, without(func(x *trafficView) { x.Group = "" })})
	}
	if v.Client != "" {
		pills = append(pills, pill{"client", v.Client, without(func(x *trafficView) { x.Client = "" })})
	}
	if v.API != "" {
		pills = append(pills, pill{"request", v.API, without(func(x *trafficView) { x.API = "" })})
	}
	data["Pills"] = pills
	kind := v.Kind
	if kind == "" && v.ErrorsOnly {
		kind = "errors"
	}
	if kind == "" {
		kind = "all"
	}
	data["KindWord"] = kind
	data["HasPills"] = len(pills) > 0
	data["TabRequests"] = c.prefix + "/traffic"
	data["TabClients"] = c.prefix + "/traffic?view=clients"
}

// attention is the line under the clients table that names the client needing a look
// (Traf-06): who, how many refusals in a row, and what the error means.
func (c *Console) attention(rows []clientRow) map[string]any {
	for _, r := range rows {
		if !r.NeedsALook {
			continue
		}
		a := map[string]any{"Who": r.Who, "Streak": r.Streak, "Err": r.LastError, "URL": c.prefix + "/traffic?kind=errors&" + TrafficFilter{Client: r.Who}.query()}
		a["Why"] = refusalWhy(r.LastError)
		return a
	}
	return nil
}

// trafficPage is the home page of the Traffic section.
func (c *Console) trafficPage(w http.ResponseWriter, r *http.Request) {
	if c.traffic == nil {
		c.fail(w, r, fmt.Errorf("%w: there is no traffic view in this console", errNotFound))
		return
	}
	data := c.trafficData(r)
	data["Page"], data["Title"] = "traffic", "Traffic"
	if v, _ := data["TView"].(string); v == "clients" {
		data["Title"] = "Traffic, clients"
	}
	// An empty broker with no filter is the board's "nothing yet": the line to point an
	// app at, from the running configuration.
	if data["ClientCount"].(int) == 0 && data["TView"] == "requests" && !data["Filtered"].(bool) && data["TotalRequests"].(int) == 0 {
		data["Empty"] = true
		data["Facts"] = c.connectFacts(r)
		data["Hints"] = pageHints("traffic_empty")
	}
	c.render(w, r, "traffic_detail", data)
}

// trafficLive is the polled region: the request tail (or the clients table) with the
// summary and the inspector's default riding along out of band.
func (c *Console) trafficLive(w http.ResponseWriter, r *http.Request) {
	if c.traffic == nil {
		c.fail(w, r, fmt.Errorf("%w: there is no traffic view in this console", errNotFound))
		return
	}
	data := c.trafficData(r)
	if liveUnchanged(w, r, data["Hash"].(string)) {
		return
	}
	// The summary line and the inspector's default are outside this region: they come
	// back with it, out of band.
	data["OOB"] = true
	if data["ClientCount"].(int) == 0 && data["TView"] == "requests" && !data["Filtered"].(bool) && data["TotalRequests"].(int) == 0 {
		data["Empty"] = true
		data["Facts"] = c.connectFacts(r)
		data["Hints"] = pageHints("traffic_empty")
	} else {
		data["Hints"] = pageHints("traffic")
	}
	c.partial(w, "traffic_live", data)
}

// trafficNew answers the paused page's one question, cheaply: how many requests came
// after the one it stopped at. It is polled only while the page is paused.
func (c *Console) trafficNew(w http.ResponseWriter, r *http.Request) {
	if c.trafficOff(w, r) {
		return
	}
	v := trafficViewFrom(r)
	ms, err := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	if err != nil {
		c.fail(w, r, badInput("since is the time the page stopped at, in milliseconds"))
		return
	}
	n, atLeast := c.traffic.NewSince(v.source(), time.UnixMilli(ms))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	fmt.Fprintf(w, `{"n":%d,"atLeast":%t}`, n, atLeast)
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
