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
// traffic_inspect.go and the explainers and the story in traffic_story.go. The design
// gate (e2e/design) measures each board against the page.

import (
	"context"
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
	// Events are the requests matching f, newest first by when they began, at most limit
	// (0: all of them).
	Events(f TrafficFilter, limit int) []TrafficEvent
	// Query is Events with what matched in all: one walk of what the recorder holds,
	// which copies only the rows it returns. A page that lists the newest rows and
	// counts the rest asks this once.
	Query(f TrafficFilter, limit int) TrafficResult
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
	// Window counts the requests and the errors of the last d of whole five-second
	// buckets (at most five minutes), and says which stretch that is.
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
	// Client is one client: its id exactly, or the address of a connection with none.
	Client string
	API    string
	Topic  string
	// Group matches the rows that name the group and the fetches of its members.
	Group string
	// Kind is produce, fetch, groups or admin.
	Kind       string
	ErrorsOnly bool
	// Any is the page's one search field: a row whose client, software, address,
	// principal, request, topics, group, transactional id or error contains it. It is
	// not part of a filter's address (the page carries it as q).
	Any string
}

// TrafficResult is the rows a query found and what matched in all.
type TrafficResult struct {
	Rows []TrafficEvent
	// Total is how many rows matched, whatever the limit; Requests how many requests
	// they stand for; Held how many requests everything the recorder holds stands for.
	Total, Requests, Held int
	// Newest is when the newest matching row began; Oldest when the oldest matching
	// row's first request did.
	Newest, Oldest time.Time
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
	// Parts are the partitions the request named ("p0–p2") where the row writes them
	// beside a topic or a group; Gen its generation, when HasGen; AutoCreate that a
	// Metadata request allowed the broker to create the topics it named.
	Parts      string
	Gen        int32
	HasGen     bool
	AutoCreate bool
	Error      string // the Kafka error's name; empty when it went well.
	ErrorCode  int16
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
	// ClientStreak, ClientLastError and ClientLastClass are the client's (its client id's)
	// run of errors, across its connections in the order its requests came.
	ClientStreak    int
	ClientLastError string
	ClientLastClass string
	// ClientReq1m, ClientReq5m and ClientErr5m are the client's counts, across every
	// connection it has had while one of them stayed open.
	ClientReq1m, ClientReq5m, ClientErr5m int
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

// TrafficWindow is what the counters say about a stretch of time: the requests that
// began in [From, To) and the errors among them.
type TrafficWindow struct {
	Requests, Errors int
	From, To         time.Time
}

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
// field of the board ("client, topic, group or request", matched by the recorder over
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
	// The field shows the topic or group a pill names (Traf-02, Traf-12); sent back
	// unchanged it is that pill, not a second filter of the same words.
	if v.Q != "" && (v.Q == v.Topic || v.Q == v.Group) {
		v.Q = ""
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
// "errors", which is the recorder's errors-only, and the one search field is its Any.
// The recorder does all of the narrowing, so it can count what matched without
// handing over the rows.
func (v trafficView) source() TrafficFilter {
	f := v.TrafficFilter
	f.Any = v.Q
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
	ConnKey     string
	Target      string // the topics, or the group, or the transactional id.
	TargetTitle string // what Target counts, by name ("5 topics" is these five).
	Who         string // the client id, else the address.
	WhoQ        string // Who as a query value.
	// Dur is the board's duration ("2 ms", "0 ms", "3.0 s"); DurExact the measured one,
	// for the tooltip ("42 µs").
	Dur, DurExact string
	Sizes         string // "526 B / 71 B": in, out.
	Folded        bool
	// Tone is the colour word of the result: "ok", "deny" (refused, denied, expected),
	// "warn" (not implemented) or "bad" (failed).
	Tone string
	// RowClass is the row's class: the tint of a refusal.
	RowClass string
	// Result is what the Result column reads: "ok" or the error's name.
	Result string
	// ResultTitle is the hover on the result.
	ResultTitle string
	// DetailText is what the Detail column reads: the row's detail, or for a periodic
	// repeat its rhythm ("every 3 s, since 14:34:15").
	DetailText string
	Interval   string
	// Rhythm says the detail is the row's rhythm, the time being drawn by the template
	// so that it is in the browser's zone.
	Rhythm bool
	// TopicParts is a commit's topic and partitions: "harbour.depot.dispatch p0–p2".
	TopicParts string
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

// groupAPIs are the requests a group's row names the group for (Main: an OffsetCommit
// is "g-depot-router" with its partitions in the detail).
var groupAPIs = map[string]bool{
	"OffsetCommit": true, "OffsetFetch": true, "Heartbeat": true, "JoinGroup": true, "SyncGroup": true, "LeaveGroup": true,
	"ConsumerGroupHeartbeat": true, "FindCoordinator": true, "TxnOffsetCommit": true, "OffsetDelete": true,
}

// trafficRow writes a request the way the boards do. What the Target and Detail say
// follows what the page is filtered by: beside a topic filter (Traf-02) a commit names
// its topic and the group goes to the detail; otherwise (Main) a commit names its group
// and its topic and partitions are the detail.
func (c *Console) trafficRow(e TrafficEvent, v trafficView) trafficRow {
	row := trafficRow{TrafficEvent: e, Folded: e.Count > 1, OpenKey: e.ID}
	topics := strings.Join(e.Topics, ", ")
	if e.TopicsMore > 0 {
		topics += " and " + strconv.Itoa(e.TopicsMore) + " more"
	}
	if len(e.Topics) == 1 && e.Parts != "" {
		row.TopicParts = e.Topics[0] + " " + e.Parts
	} else {
		row.TopicParts = strings.TrimSpace(topics + " " + e.Parts)
	}
	row.DetailText = e.Detail
	switch {
	case v.Topic != "" && len(e.Topics) > 0:
		// Beside a topic filter every row names the topic it is about (Traf-02).
		row.Target = topics
		if e.API == "OffsetCommit" && e.Group != "" {
			row.DetailText = joinNonEmpty(" · ", e.Parts, e.Group)
		}
		if e.API == "Metadata" {
			row.DetailText = c.metadataDetail(e)
		}
	case e.API == "Metadata":
		switch n := len(e.Topics) + e.TopicsMore; {
		case n == 1:
			row.Target = e.Topics[0]
		case n > 1:
			row.Target = strconv.Itoa(n) + " topics"
			row.TargetTitle = topics
		default:
			row.Target = "all topics"
		}
		row.DetailText = c.metadataDetail(e)
	case groupAPIs[e.API] && e.Group != "":
		row.Target = e.Group
		if e.API == "OffsetCommit" {
			row.DetailText = row.TopicParts
		}
	case len(e.Topics) > 0:
		row.Target = topics
	case e.Group != "":
		row.Target = e.Group
	case e.TxnID != "":
		row.Target = "tx " + e.TxnID
	}
	row.Who = e.ClientID
	if row.Who == "" {
		row.Who = e.Remote
	}
	row.WhoQ = queryEscape(row.Who)
	row.Dur, row.DurExact = shortDuration(e.Duration), exactDuration(e.Duration)
	row.Sizes = kibText(e.ReqBytes) + " / " + kibText(e.RespBytes)
	if row.Folded {
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

// metadataDetail is what a Metadata row's detail says: the answer, and what the broker
// does with a request that allows it to create the topics it named. "auto-create on"
// was the request's flag, written on a broker started with auto-create off (A8).
func (c *Console) metadataDetail(e TrafficEvent) string {
	detail := e.Detail
	if strings.HasSuffix(detail, " topic") || strings.HasSuffix(detail, " topics") {
		detail = "" // the Target column says how many.
	}
	if e.AutoCreate && c.facts.KafkaMajor > 0 {
		if c.facts.AutoCreateTopics {
			detail = joinNonEmpty(" · ", detail, "auto-create on")
		} else {
			detail = joinNonEmpty(" · ", detail, "asks to auto-create; off here")
		}
	}
	if detail == "" {
		detail = e.Detail
	}
	return detail
}

// intervalText is how often a folded row repeated: "every 3 s".
func intervalText(e TrafficEvent) string {
	if e.Count < 2 {
		return ""
	}
	return "every " + everyText(e.Time.Sub(e.First)/time.Duration(e.Count-1))
}

// shortDuration is a duration as the boards write it: whole milliseconds, rounded
// ("0 ms", "2 ms", "41 ms", "500 ms"), seconds to a tenth from one second ("3.0 s"), and
// minutes and seconds from a minute ("2 m 10 s"). The boards print "0 ms" for what took
// under half a millisecond (Main, Traf-05, Traf-12); the measured time is the
// tooltip (exactDuration).
func shortDuration(d time.Duration) string {
	switch {
	case d < 0:
		d = 0
		fallthrough
	case d < 999500*time.Microsecond:
		return strconv.FormatInt(int64(math.Round(float64(d)/float64(time.Millisecond))), 10) + " ms"
	case d < 59950*time.Millisecond:
		return strconv.FormatFloat(d.Seconds(), 'f', 1, 64) + " s"
	default:
		d = d.Round(time.Second)
		return fmt.Sprintf("%d m %d s", int(d.Minutes()), int(d.Seconds())%60)
	}
}

// exactDuration is the measured duration, for the tooltip: "42 µs", "0.4 ms", "22.8 ms".
func exactDuration(d time.Duration) string {
	switch {
	case d < 100*time.Microsecond:
		return fmt.Sprintf("took %d µs", max(d.Microseconds(), 0))
	case d < time.Second:
		return fmt.Sprintf("took %.1f ms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("took %.3f s", d.Seconds())
}

// spanText is how long ago something was, for a column of ages: "just now", 12s, 3m 10s, 2h 05m, 3d 05h.
func spanText(d time.Duration) string {
	switch {
	case d < 2*time.Second:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm %02ds", int(d.Minutes()), int(d.Seconds())%60)
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh %02dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd %02dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

// spanBoard is how long something has been, as the Traffic boards write it: "9 s",
// "38 s", "4 m 10 s", "1 h 02 m", "2 h 14 m", "3 d 05 h".
func spanBoard(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%d s", max(int(d.Seconds()), 0))
	case d < time.Hour:
		return fmt.Sprintf("%d m %02d s", int(d.Minutes()), int(d.Seconds())%60)
	case d < 48*time.Hour:
		return fmt.Sprintf("%d h %02d m", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%d d %02d h", int(d.Hours())/24, int(d.Hours())%24)
}

// clientRow is an open connection, ready to show.
type clientRow struct {
	TrafficClient
	Who string
	// Age is how long it has been open, and Idle since its last request, as the boards
	// write them ("2 h 14 m", "38 s").
	Age  string
	Idle string
	// Filter is the query that shows only this client's requests, and Href the address
	// that carries it (a URL, so that it is not escaped a second time: TRF-9).
	Filter string
	Href   template.URL
	// Doing is what its requests say it is: "producer · tx checkout-1", the groups it
	// is in, "none (producer)".
	Doing string
	// Rate is the error rate over its last five minutes, as the Clients view writes it;
	// RateTitle what it is of.
	Rate      string
	RateTon   string
	RateTitle string
	// NeedsALook marks a client the page raises: refused several times in a row, or
	// most of what it asked was refused.
	NeedsALook bool
	// LookHead and LookNote say why, when it does (lookWords): the callout's sentence
	// after the client's name, and the line under it in the connected-clients list.
	LookHead, LookNote string
	Groups             string
	Metrics            bool
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
		// A client's counts are its own, whichever connection carried them: the sum over
		// the connections open now lost those of one that closed (a fetch connection
		// closed at a rebalance), so a client said 116 a minute and had sent 178.
		for i := range conns {
			conns[i].Req1m, conns[i].Req5m, conns[i].Err5m = conns[i].ClientReq1m, conns[i].ClientReq5m, conns[i].ClientErr5m
		}
		if len(conns) == 1 {
			cr := c.clientRow(conns[0], now)
			cr.Conns, cr.Members, cr.Keys = 1, []clientRow{cr}, "client:"+strconv.FormatUint(conns[0].Conn, 10)
			out = append(out, cr)
			continue
		}
		// The connection that spoke last stands for the client; the rest of the row is the
		// sum of all of them. The streak is the client's own, the same on each (A2).
		m := conns[0]
		for _, cl := range conns[1:] {
			if cl.Last.After(m.Last) {
				m = cl
			}
		}
		m.Opened, m.Requests = conns[0].Opened, 0
		m.Groups, m.Produces, m.Fetches, m.AskedTelemetry = nil, false, false, false
		var members []clientRow
		var keys []string
		var principals []string
		for _, cl := range conns {
			if cl.Opened.Before(m.Opened) {
				m.Opened = cl.Opened
			}
			m.Requests += cl.Requests
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

// needsALook is the rule behind "1 client needs a look": three errors in a row, the
// client's latest requests, or three or more in five minutes making up at least half of
// what it asked.
func needsALook(c TrafficClient) bool {
	return c.ClientStreak >= 3 || (c.Err5m >= 3 && c.Req5m > 0 && c.Err5m*2 >= c.Req5m)
}

// noWord is the word for a kind of no, in a sentence: "refused", "denied", "failed".
func noWord(class string) string {
	switch class {
	case "denied":
		return "denied"
	case "failed":
		return "failed"
	case "not-implemented":
		return "not served"
	}
	return "refused"
}

// lookWords say why a client needs a look, as the callout's sentence (after the
// client's name) and as the line under it in the connected-clients list. The board
// (Traf-06) draws the streak: "has been refused 4 times in a row", "last 4 requests
// refused"; the word is the class of the no (an ACL's is "denied": A2 of the
// foundation). A client raised by the five-minute rule alone has no such streak, so it
// is told by the count the rule read.
func lookWords(c TrafficClient) (head, note string) {
	if c.ClientStreak >= 3 {
		w := noWord(c.ClientLastClass)
		if w == "failed" {
			return fmt.Sprintf("has failed %d times in a row", c.ClientStreak), fmt.Sprintf("last %d requests failed", c.ClientStreak)
		}
		return fmt.Sprintf("has been %s %d times in a row", w, c.ClientStreak), fmt.Sprintf("last %d requests %s", c.ClientStreak, w)
	}
	return fmt.Sprintf("had %d errors among its %d requests in the last five minutes", c.Err5m, c.Req5m),
		fmt.Sprintf("%d of %d requests were errors in the last 5 minutes", c.Err5m, c.Req5m)
}

func (c *Console) clientRow(cl TrafficClient, now time.Time) clientRow {
	cr := clientRow{TrafficClient: cl, Who: cl.ClientID, Age: spanBoard(now.Sub(cl.Opened)), Idle: spanBoard(now.Sub(cl.Last))}
	if cr.Who == "" {
		cr.Who = cl.Remote
	}
	cr.Filter = TrafficFilter{Client: cr.Who}.query()
	cr.Href = template.URL(c.prefix + "/traffic?" + cr.Filter) //nolint:gosec // built from an escaped query.
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
	cr.LookHead, cr.LookNote = lookWords(cl)
	switch {
	case cl.Req5m == 0:
		cr.Rate, cr.RateTon = "—", "mut"
		cr.RateTitle = "No request in the last 5 minutes"
	default:
		pct := float64(cl.Err5m) * 100 / float64(cl.Req5m)
		cr.RateTitle = fmt.Sprintf("%d of %d requests in the last 5 minutes were errors", cl.Err5m, cl.Req5m)
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
		if cl.ClientStreak >= 3 {
			cr.Groups += ", " + noWord(cl.ClientLastClass)
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
	held := c.traffic.Query(TrafficFilter{}, 1) // how far back it reaches; the rows are not wanted.
	kept := "the recent requests the broker keeps in memory"
	if n := c.traffic.Capacity(); n > 0 {
		kept = "the last " + groupDigits(int64(n)) + " requests the broker keeps in memory"
	}
	if held.Total == 0 {
		return "from recent requests; none are kept yet",
			"Written by is counted from " + kept + ", and none are kept yet. It does not survive a restart."
	}
	age := spanText(now.Sub(held.Oldest))
	return "from recent requests, the oldest " + age + " old",
		"Written by is counted from " + kept + ". It reaches back " + age + ": a client that wrote earlier, or whose requests have been pushed out, is not listed. It does not survive a restart."
}

// autoCreateTopics is the broker's setting, when the console knows it.
func (c *Console) autoCreateTopics(context.Context) bool { return c.facts.AutoCreateTopics }

// topicExists says the topic is one the broker has now.
func (c *Console) topicExists(ctx context.Context, name string) bool {
	topics, err := c.be.ListTopics(ctx)
	if err != nil {
		return false
	}
	for _, t := range topics {
		if t.Name == name {
			return true
		}
	}
	return false
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
		"HasClientMetrics": c.metrics != nil, "MetricsOn": c.metrics != nil, "OOB": false,
		"TView": v.View, "ClearURL": c.prefix + "/traffic",
	}
	// Clearing the filter (Esc, "clear") keeps the kind of request asked for, as the
	// field and the pills are the filter and the kinds are not.
	if v.Kind != "" && (v.Group == "" || v.Flat) {
		data["ClearURL"] = c.prefix + "/traffic?kind=" + v.Kind
	}
	field := v.Q
	if field == "" {
		field = firstNonEmpty(v.Topic, v.Group)
	}
	data["FieldValue"] = field

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
		fmt.Fprint(h, cl.Conn, cl.Requests, cl.ClientID, cl.Software, cl.ClientStreak, cl.Req5m, ";")
	}
	data["Clients"], data["NeedLook"] = crows, needLook
	data["ClientCount"], data["ConnCount"] = len(crows), len(clients)

	kept := "few thousand requests"
	if n := c.traffic.Capacity(); n > 0 {
		kept = groupDigits(int64(n))
	}
	data["KeptText"] = kept
	data["LastMinute"], data["ErrorCount"] = win.Requests, win.Errors
	data["WindowFrom"], data["WindowTo"] = win.From.UnixMilli(), win.To.UnixMilli()
	data["WindowFromT"], data["WindowToT"] = win.From, win.To
	data["Since"] = c.traffic.Since()
	fmt.Fprint(h, "|", len(clients), win.Requests, win.Errors, win.From.UnixMilli(), v.View)

	if v.View == "clients" {
		data["Attention"] = c.attention(crows)
		data["ClientNote"] = clientsNote(crows)
		// Traf-06 draws no panel: the table has the clients, so the inspector's own
		// list of them would show them twice and take 400 px the table's nine
		// columns need. A selected client opens it.
		data["InspDefault"] = ""
		// A paused clients view counts the requests since it paused (Traf-08), from now.
		data["NewestMs"] = msText(now)
		data["Hash"] = fmt.Sprintf("%x", h.Sum64())
		data["Rows"] = []trafficRow(nil)
		data["Hints"] = trafficHints("clients", false)
		c.trafficKindsData(data, v)
		return data
	}

	// The rows: everything the recorder holds that the page's filter lets through,
	// newest first; the table shows the first trafficRows of them, and the heading says
	// how many matched. One question to the recorder, which hands over the rows shown and
	// counts the rest where they lie.
	res := c.traffic.Query(v.source(), trafficRows)
	totalReq, matchedRows, matchedReq := res.Held, res.Total, res.Requests
	var rows []trafficRow
	for _, e := range res.Rows {
		row := c.trafficRow(e, v)
		if open[e.Conn] {
			row.ConnKey = strconv.FormatUint(e.Conn, 10)
		}
		rows = append(rows, row)
		fmt.Fprint(h, e.ID, e.Seq, e.Count, ",")
	}
	data["Rows"] = rows
	data["RowCount"] = len(rows) + 1
	data["Matched"], data["MatchedRows"], data["TotalRequests"] = matchedReq, matchedRows, totalReq
	data["Cut"] = matchedRows > len(rows)
	data["Shown"] = len(rows)
	// The cursor a paused page counts from: the start of the newest row shown, to the
	// millisecond it is written in (a row that began later in the same millisecond is
	// counted by the recorder, which has the nanoseconds).
	var newest time.Time
	for _, row := range rows {
		if row.Time.After(newest) {
			newest = row.Time
		}
	}
	if newest.IsZero() {
		newest = now
	}
	data["Newest"] = newest
	data["NewestMs"] = msText(newest)

	// The refusals of what the page lists: the errors view's explainers, and the line
	// under the table (A9: they were of everything held, so the panel named a refusal
	// the filtered list did not have).
	refusals := c.traffic.Refusals()
	if v.Kind == "errors" || v.ErrorsOnly {
		refusals = refusalsOf(c.traffic.Events(v.source(), 0))
	}
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
	state := "requests"
	switch {
	case v.Kind == "errors" || v.ErrorsOnly:
		data["InspDefault"] = "errors"
		state = "errors"
		// The most frequent refusal of what the page lists, not of everything held (A9).
		data["ErrorSummary"] = c.errorSummary(r.Context(), refusals)
	case v.Topic != "":
		data["InspDefault"] = "touched"
		state = "filtered"
		data["Touched"] = c.touched(r.Context(), v.Topic)
	case v.Group != "" && !v.Flat:
		data["InspDefault"] = "story"
		state = "story"
	case !v.empty():
		state = "filtered"
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
	// The line under the table has the board's 8 px under it on Main, Traf-02, Traf-04,
	// Traf-05, Traf-08 and Traf-10; the errors view and the story put a spacer there.
	data["FootPad"] = state != "errors" && state != "story"
	data["Hints"] = trafficHints(state, false)
	data["Hash"] = fmt.Sprintf("%x", h.Sum64())
	c.trafficKindsData(data, v)
	return data
}

// msText is a time as milliseconds to the microsecond ("1792148375593.043"): the
// paused page's cursor, which the browser reads as a number and sends back.
func msText(t time.Time) string {
	us := t.UnixMicro()
	return strconv.FormatInt(us/1000, 10) + "." + fmt.Sprintf("%03d", us%1000)
}

// refusalsOf groups error rows by what they said, most often first (ties: newest
// first), as the recorder's Refusals does for all it holds.
func refusalsOf(rows []TrafficEvent) []TrafficRefusal {
	by := map[string]*TrafficRefusal{}
	per := map[string]map[string]int{}
	var order []string
	for _, e := range rows {
		if e.Error == "" || e.Class == "expected" || e.Class == "" {
			continue
		}
		g := by[e.Error]
		if g == nil {
			g = &TrafficRefusal{Code: e.ErrorCode, Name: e.Error, Class: e.Class, First: e.Time, Last: e.Time, Latest: e.ID, API: e.API, Topics: e.Topics}
			by[e.Error], per[e.Error] = g, map[string]int{}
			order = append(order, e.Error)
		}
		n := max(e.Count, 1)
		g.Count += n
		if e.Time.Before(g.First) {
			g.First = e.Time
		}
		if e.Time.After(g.Last) {
			g.Last, g.Latest, g.API, g.Topics = e.Time, e.ID, e.API, e.Topics
		}
		who := firstNonEmpty(e.ClientID, e.Remote)
		per[e.Error][who] += n
	}
	out := make([]TrafficRefusal, 0, len(order))
	for _, k := range order {
		g := by[k]
		for who, n := range per[k] {
			g.Clients = append(g.Clients, TrafficClientCount{who, n})
		}
		slices.SortFunc(g.Clients, func(a, b TrafficClientCount) int {
			if a.Count != b.Count {
				return b.Count - a.Count
			}
			return strings.Compare(a.Client, b.Client)
		})
		out = append(out, *g)
	}
	slices.SortStableFunc(out, func(a, b TrafficRefusal) int {
		if a.Count != b.Count {
			return b.Count - a.Count
		}
		return b.Last.Compare(a.Last)
	})
	return out
}

// clientsNote is the line under the clients table (Traf-06): what the error rate is of,
// and, as the board does, the one client with errors that does not need a look.
func clientsNote(rows []clientRow) string {
	s := "Error rate is the requests refused, denied, not served or failed over all requests in the last 5 minutes."
	for _, r := range rows {
		if r.NeedsALook || r.Err5m == 0 {
			continue
		}
		s += fmt.Sprintf(" %s shows %s among %s", r.Who, pluralOf(r.Err5m, "error"), groupDigits(int64(r.Req5m)))
		if r.LastError != "" {
			s += " (the latest " + r.LastError + ")"
		}
		s += "."
		break
	}
	return s
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
		on := v.Kind == k.Key || (k.Key == "errors" && v.Kind == "" && v.ErrorsOnly)
		// A group's story is its group requests (Traf-12 draws "groups" on).
		if v.Group != "" && !v.Flat && v.Kind == "" {
			on = k.Key == "groups"
		}
		kinds = append(kinds, map[string]any{"Key": k.Key, "Label": k.Label, "URL": u, "On": on})
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
// (Traf-06): who, why it is raised (lookWords), and what the error means.
func (c *Console) attention(rows []clientRow) map[string]any {
	for _, r := range rows {
		if !r.NeedsALook {
			continue
		}
		a := map[string]any{"Who": r.Who, "Head": r.LookHead, "URL": c.prefix + "/traffic?kind=errors&" + TrafficFilter{Client: r.Who}.query()}
		a["Why"] = refusalWhy(firstNonEmpty(r.ClientLastError, r.LastError))
		return a
	}
	return nil
}

// trafficHints are the keys strip of a Traffic page in the state the server draws it
// (the boards' strips: Main, Traf-02, Traf-03, Traf-06, Traf-09, Traf-12). What the
// browser changes (a row opened, paused, the broker lost) the shell redraws from the
// same words (shell.js, trafficKeys).
func trafficHints(state string, empty bool) []keyHint {
	k := func(label string, keys ...string) keyHint { return keyHint{Keys: keys, Label: label} }
	move, open, filter, pause, jump := k("move", "j", "k"), k("open", "↵"), k("filter", "/"), k("pause", "p"), k("jump or act", "⌘K")
	lostKeys := []keyHint{lost(k("retry now", "r")), lost(move), lost(jump)}
	var hs []keyHint
	switch {
	case empty:
		hs = []keyHint{k("connect an app", "c"), pause, jump}
		lostKeys = []keyHint{lost(k("retry now", "r")), lost(jump)}
	case state == "clients":
		hs = []keyHint{move, k("open client", "↵"), k("requests", "g", "r"), jump}
	case state == "errors":
		hs = []keyHint{move, open, filter, k("errors only", "e"), pause, jump}
	case state == "filtered":
		hs = []keyHint{move, open, filter, k("clear filter", "esc"), pause, jump}
	case state == "story":
		hs = []keyHint{move, open, k("clear filter", "esc"), jump}
	default:
		hs = []keyHint{move, open, filter, pause, jump}
	}
	for i := range hs {
		hs[i] = live(hs[i])
	}
	return append(hs, lostKeys...)
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
	c.markEmpty(r, data)
	c.render(w, r, "traffic_detail", data)
}

// markEmpty makes an empty broker with no filter the board's "nothing yet" (Traf-09):
// the line to point an app at, from the running configuration.
func (c *Console) markEmpty(r *http.Request, data map[string]any) {
	if data["ClientCount"].(int) == 0 && data["TView"] == "requests" && !data["Filtered"].(bool) && data["TotalRequests"].(int) == 0 {
		data["Empty"] = true
		data["SoFar"] = 0
		data["Facts"] = c.connectFacts(r)
		data["Hints"] = trafficHints("", true)
		data["InspDefault"] = ""
	}
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
	c.markEmpty(r, data)
	c.partial(w, "traffic_live", data)
}

// trafficNew answers the paused page's one question, cheaply: how many requests came
// after the one it stopped at. It is polled only while the page is paused; it also
// tells a paused page that the broker is still there.
func (c *Console) trafficNew(w http.ResponseWriter, r *http.Request) {
	if c.trafficOff(w, r) {
		return
	}
	v := trafficViewFrom(r)
	ms, err := strconv.ParseFloat(r.URL.Query().Get("since"), 64)
	if err != nil || math.IsNaN(ms) || math.IsInf(ms, 0) {
		c.fail(w, r, badInput("since is the time the page stopped at, in milliseconds"))
		return
	}
	// "since" is the start of the newest row the page shows, to the microsecond
	// (msText): what began after that microsecond is new.
	n, atLeast := c.traffic.NewSince(v.source(), time.UnixMicro(int64(math.Round(ms*1000))).Add(time.Microsecond-1))
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
