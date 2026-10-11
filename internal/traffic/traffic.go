// Package traffic remembers what clients have asked the broker, and what the
// consumer-group coordinators did about it, for the console to show.
//
// It answers "what did my app just do?" and "why does my consumer keep
// rebalancing?". It is a bounded, in-memory recorder: nothing is written to disk,
// the least recently active rows are dropped when it is full, and a restart starts
// it empty. That is the right size for a tool that looks at the last few minutes of
// a laptop's traffic, and it keeps the recorder from ever being a cost worth
// measuring.
//
// The bounds, all of them constants here:
//
//   - 2,000 rows (the capacity the server gives New), each with its strings clipped
//     (MaxText and friends: a client chose them) and its facts bounded;
//   - 64 ticks on a folded row (MaxTicks), kept only for rows that fold;
//   - 200 events per group (MaxGroupEvents) and at most 100 groups (MaxGroups);
//   - one counter ring per connection, 60 buckets of five seconds (five minutes),
//     and one for the whole broker.
//
// A Recorder is a protocol.Observer; the broker feeds it group events through
// GroupEvent. The console never sees this package: it is handed an interface of its
// own, implemented in cmd/doze-kafka.
package traffic

import (
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// ConsoleClientIDs are the client ids of the console's own Kafka clients, whose
// requests are left out: the page that shows traffic would otherwise be mostly its
// own polling. They are exact: an app that begins its client id the same way is an
// app (TRF-21).
var ConsoleClientIDs = []string{"doze-kafka-console", "doze-kafka-console-fetch", "doze-kafka-console-trace"}

// collapseWindow is how long a repeated request may keep folding into one row. A
// consumer's heartbeat every three seconds is one row for as long as the consumer
// lives; this only stops a row that went quiet an hour ago from swallowing the next.
const collapseWindow = 10 * time.Minute

// What is kept of what a client said about itself. The recorder holds up to
// 2,000 rows for as long as the broker runs, and everything in a row was chosen by
// a client that may be anyone, so every string is clipped on the way in: a 900 KiB
// join reason kept whole, in each of 2,000 rows, was 1.8 GiB (TRF-1). A clipped
// value ends in an ellipsis and is a copy, so it does not keep its original alive.
const (
	// MaxText bounds a detail and the reason a connection was closed.
	MaxText = 256
	// MaxName bounds what is a name: a client id, a software name, a principal, a group,
	// a transactional id, a member id.
	MaxName = 128
	// MaxRemote bounds a peer address, which is never longer than an IPv6 address and a
	// port.
	MaxRemote = 64
	// MaxTopics is how many topic names a row keeps, and MaxTopicBytes how many bytes
	// of them; the rest are counted. Twenty ordinary names fit; twenty at the longest a
	// name may be do not, and four of them do.
	MaxTopics     = 20
	MaxTopicBytes = 1024
	// MaxTopicName is the longest a topic may be named.
	MaxTopicName = 249

	// MaxTicks is how many of a folded row's latest repeats are kept as ticks.
	MaxTicks = 64
	// MaxGroupEvents is how many events one group keeps, and MaxGroups how many groups
	// are kept at all: a noisy group cannot push out a quiet group's story (TRF-19).
	MaxGroupEvents = 200
	MaxGroups      = 100

	// bucketWidth and bucketCount make a connection's counters: five minutes in five
	// second steps. The whole broker's is the same shape.
	bucketWidth = 5 * time.Second
	bucketCount = 60
)

// clip returns s cut to at most n bytes, on a character boundary, ending in an
// ellipsis when it was cut.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	const dots = "…"
	cut := n - len(dots)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return strings.Clone(s[:cut]) + dots
}

// clipTopics keeps the first topics that fit the row's budget, each clipped, and says
// how many it left out.
func clipTopics(topics []string) (kept []string, more int) {
	size := 0
	for i, tp := range topics {
		tp = clip(tp, MaxTopicName)
		if len(kept) == MaxTopics || size+len(tp) > MaxTopicBytes {
			return kept, len(topics) - i
		}
		kept = append(kept, tp)
		size += len(tp)
	}
	return kept, 0
}

// Tick is one repeat of a folded row: when it began, how long it took and how it went.
type Tick struct {
	At       time.Time
	Duration time.Duration
	Class    protocol.Class
}

// Event is one row: a request, or a run of identical ones folded together.
type Event struct {
	// ID names the row for as long as the recorder holds it, and does not change when
	// the row folds in a repeat. It is not the same after a restart.
	ID string
	// Seq orders rows by their latest activity: a row that folds in a repeat takes a
	// new one.
	Seq uint64
	// First and Time are when the row's first and most recent request began.
	First, Time time.Time
	// Count is how many identical requests the row stands for.
	Count int

	Conn                                  uint64
	Remote, ClientID, Software, Principal string

	API     string
	Key     int16
	Version int16
	// Kind is the family the API belongs to: produce, fetch, groups, admin, or "".
	Kind string
	// Correlation is the request's correlation id.
	Correlation int32

	Topics []string
	// TopicsMore is how many topics the request named beyond the MaxTopics kept.
	TopicsMore int
	Group      string
	// ViaGroup is the group of the connection a Fetch came over, which the request
	// itself does not say: it lets a group's rows include its members' fetches (TRF-18).
	ViaGroup string
	// TxnID is the transactional id the request named, if it did. It is not a group.
	TxnID  string
	Detail string
	// Error is the Kafka name of the first error in the response ("" when it went
	// well), ErrorCode its number, and Class what it means (expected, refused,
	// denied, not implemented, failed).
	Error     string
	ErrorCode int16
	Class     protocol.Class
	// Closed says the broker closed the connection because of this request, and why.
	Closed string
	// Idle marks a request that moved nothing. These are the rows that fold.
	Idle bool

	// Duration and the byte counts are the row's most recent request's.
	Duration            time.Duration
	ReqBytes, RespBytes int

	// Facts are the first request's partitions and decoded fields.
	Facts protocol.Facts
	// Ticks are the last MaxTicks repeats of a folded row, oldest first. Count says
	// how many there were in all.
	Ticks []Tick
}

// IsError reports whether the row's answer counts as an error.
func (e Event) IsError() bool { return e.Class.IsError() }

// row is an Event on the recency list.
type row struct {
	Event
	prev, next *row
	foldKey    string
	lastReq    uint64
}

// Client is a live connection that has sent a request.
type Client struct {
	Conn                                  uint64
	Remote, ClientID, Software, Principal string
	Opened, Last                          time.Time
	Requests                              int64
	// Groups are the consumer groups this connection has spoken for, newest last.
	Groups []string
	// TxnID is the transactional id it has used, the latest.
	TxnID string
	// Produces and Fetches say what it has been seen doing.
	Produces, Fetches bool
	// AskedTelemetry says it has called GetTelemetrySubscriptions: a client that never
	// does is one that does not report metrics.
	AskedTelemetry bool
	// Req1m, Req5m and Err5m are what its counters say now: requests in the last
	// minute (to the nearest five seconds) and requests and errors in the last five.
	Req1m, Req5m, Err5m int
	// Streak is how many requests in a row, the latest ones, were errors; LastError
	// is the name of the latest.
	Streak    int
	LastError string
}

// GroupEvent is something a coordinator did: a member joined, a generation began.
type GroupEvent struct {
	Seq        uint64
	Time       time.Time
	Group      string
	Kind       string // joined, left, expired, rebalance, generation, assigned, ...
	Member     string
	Generation int32
	Detail     string
	// Members is how many members the group had when it happened, and Cause why a
	// rebalance or an epoch began, where there is a phrase for it.
	Members int
	Cause   string
}

// Filter narrows the rows. Zero fields match everything. Text fields match a
// substring, case-insensitively, except Group, Topic and Kind, which match exactly.
type Filter struct {
	Client string // matches the client id, the software or the peer address.
	API    string
	Topic  string
	// Group matches the rows that name the group and the fetches of its members.
	Group string
	// Kind is produce, fetch, groups or admin.
	Kind string
	// Conn matches one connection.
	Conn       uint64
	ErrorsOnly bool
}

// Query is a Filter with a limit.
type Query struct {
	Filter
	Limit int
}

// Result is the rows a query found, with how many matched in all.
type Result struct {
	Rows []Event
	// Total is how many rows matched the filter, whatever the limit.
	Total int
	// Newest is the start time of the newest matching row; the cursor a paused page
	// holds on to.
	Newest time.Time
}

// counters is a ring of fixed-width buckets of request and error counts.
type counters struct {
	b [bucketCount]bucket
}

// bucket is eight bytes: a connection holds sixty of them, and a broker may have
// thousands of connections.
type bucket struct {
	idx         int32
	total, errs uint16
}

func (c *counters) add(t time.Time, isErr bool) {
	i := int32(t.UnixNano() / int64(bucketWidth))
	b := &c.b[int(i%bucketCount+bucketCount)%bucketCount]
	if b.idx != i {
		*b = bucket{idx: i}
	}
	if b.total < 0xffff {
		b.total++
	}
	if isErr && b.errs < 0xffff {
		b.errs++
	}
}

// sum is the requests and errors in the window ending at now, at most five minutes.
func (c *counters) sum(now time.Time, window time.Duration) (total, errs int) {
	hi := int32(now.UnixNano() / int64(bucketWidth))
	n := int32(window / bucketWidth)
	if window%bucketWidth != 0 {
		n++
	}
	lo := hi - n
	for i := range c.b {
		if b := c.b[i]; b.idx > lo && b.idx <= hi && b.total > 0 {
			total += int(b.total)
			errs += int(b.errs)
		}
	}
	return total, errs
}

// conn is what the recorder keeps per open connection.
type conn struct {
	Client
	counts counters
	// seen is false until the connection sends a request: one that never does (a port
	// probe, a pre-authentication connection that went quiet) is not a client (TRF-30).
	seen bool
	// recent is the foldable row each kind of request is currently folding into.
	recent map[string]*row
}

// groupLog is one group's events.
type groupLog struct {
	events []GroupEvent
	last   uint64
}

// Recorder is the bounded store. The zero value is not usable; use New.
type Recorder struct {
	mu      sync.Mutex
	resolve func([16]byte) string
	now     func() time.Time
	epoch   string

	seq  uint64 // orders rows and group events.
	reqs uint64 // requests recorded, folded ones included.

	byID       map[string]*row
	head, tail *row // most recently active first.
	limit      int
	conns      map[uint64]*conn
	all        counters // the whole broker, console clients left out.

	groups map[string]*groupLog
}

// Capacity is how many rows the recorder keeps.
func (r *Recorder) Capacity() int { return r.limit }

// New returns a recorder holding at most capacity rows. resolve turns a topic id into
// its name, for the requests that carry only the id; it may be nil.
func New(capacity int, resolve func([16]byte) string) *Recorder {
	if capacity < 1 {
		capacity = 1
	}
	return &Recorder{
		resolve: resolve, limit: capacity, now: time.Now,
		epoch: strconv.FormatInt(time.Now().Unix(), 36),
		byID:  map[string]*row{}, conns: map[uint64]*conn{}, groups: map[string]*groupLog{},
	}
}

func isConsole(clientID string) bool { return slices.Contains(ConsoleClientIDs, clientID) }

// ConnOpened implements protocol.Observer.
func (r *Recorder) ConnOpened(id uint64, remote string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.conns[id] = &conn{Client: Client{Conn: id, Remote: clip(remote, MaxRemote), Opened: now, Last: now}, recent: map[string]*row{}}
}

// ConnClosed implements protocol.Observer.
func (r *Recorder) ConnClosed(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.conns, id)
}

func (r *Recorder) unlink(x *row) {
	if x.prev != nil {
		x.prev.next = x.next
	} else if r.head == x {
		r.head = x.next
	}
	if x.next != nil {
		x.next.prev = x.prev
	} else if r.tail == x {
		r.tail = x.prev
	}
	x.prev, x.next = nil, nil
}

func (r *Recorder) pushFront(x *row) {
	x.prev, x.next = nil, r.head
	if r.head != nil {
		r.head.prev = x
	}
	r.head = x
	if r.tail == nil {
		r.tail = x
	}
}

// Request implements protocol.Observer.
func (r *Recorder) Request(o protocol.Observation) {
	r.mu.Lock()
	defer r.mu.Unlock()

	c := r.conns[o.Conn]
	if c == nil {
		c = &conn{Client: Client{Conn: o.Conn, Remote: clip(o.Remote, MaxRemote), Opened: o.Start}, recent: map[string]*row{}}
		r.conns[o.Conn] = c
	}
	c.seen = true
	if o.ClientID != "" {
		c.ClientID = clip(o.ClientID, MaxName)
	}
	if o.Software != "" {
		c.Software = clip(o.Software, MaxName)
	}
	c.Principal = clip(o.Principal, MaxName)
	c.Last = o.Start
	c.Requests++
	group := clip(o.Group, MaxName)
	if group != "" && !slices.Contains(c.Groups, group) {
		c.Groups = append(c.Groups, group)
		if len(c.Groups) > 4 {
			c.Groups = c.Groups[1:]
		}
	}
	if isConsole(c.ClientID) {
		return
	}
	if o.TxnID != "" {
		c.TxnID = clip(o.TxnID, MaxName)
	}
	switch o.Kind {
	case "produce":
		c.Produces = true
	case "fetch":
		c.Fetches = true
	}
	if o.API == kmsg.GetTelemetrySubscriptions {
		c.AskedTelemetry = true
	}
	if o.Class == "" && o.ErrorCode != 0 {
		o.Class = protocol.Classify(o.API, o.ErrorCode)
	}
	isErr := o.Class.IsError() || o.Closed != ""
	c.counts.add(o.Start, isErr)
	r.all.add(o.Start, isErr)
	r.reqs++
	if isErr {
		c.Streak++
		c.LastError = protocol.ErrorName(o.ErrorCode)
		if o.Closed != "" {
			c.LastError = "CONNECTION_CLOSED"
		}
	} else {
		c.Streak, c.LastError = 0, ""
	}

	topics := slices.Clone(o.Topics)
	for _, id := range o.TopicIDs {
		if r.resolve != nil {
			if n := r.resolve(id); n != "" && !slices.Contains(topics, n) {
				topics = append(topics, n)
			}
		}
	}
	topics, more := clipTopics(topics)
	detail := o.Detail
	errName := protocol.ErrorName(o.ErrorCode)
	if o.Closed != "" {
		errName = "CONNECTION_CLOSED"
	}
	ev := Event{
		First: o.Start, Time: o.Start, Count: 1, Conn: o.Conn, Remote: c.Remote,
		ClientID: c.ClientID, Software: c.Software, Principal: c.Principal,
		API: protocol.APIName(o.API), Key: int16(o.API), Version: o.Version, Kind: o.Kind, Correlation: o.Correlation,
		Topics: topics, TopicsMore: more, Group: group, TxnID: clip(o.TxnID, MaxName), Detail: clip(detail, MaxText),
		Error: errName, ErrorCode: o.ErrorCode, Class: o.Class, Closed: clip(o.Closed, MaxText),
		Idle: o.Idle, Duration: o.Duration, ReqBytes: o.ReqBytes, RespBytes: o.RespBytes,
	}
	if o.Closed != "" {
		ev.Class = protocol.ClassFailed
		if o.Class != "" {
			ev.Class = o.Class
		}
	}
	// A fetch does not say which group it belongs to; the connection knows.
	if ev.Group == "" && ev.TxnID == "" && o.Kind == "fetch" && len(c.Groups) > 0 {
		ev.ViaGroup = c.Groups[len(c.Groups)-1]
	}

	r.seq++
	if ev.Idle && ev.Error == "" {
		key := foldKey(ev)
		if prev := c.recent[key]; prev != nil && o.Start.Sub(prev.Time) < collapseWindow {
			if len(prev.Ticks) == 0 {
				// The row's first request is a tick too, so the strip shows all of them.
				prev.Ticks = append(prev.Ticks, Tick{At: prev.First, Duration: prev.Duration, Class: prev.Class})
			}
			prev.Count++
			prev.Time, prev.Seq, prev.lastReq = o.Start, r.seq, r.reqs
			prev.Duration, prev.ReqBytes, prev.RespBytes = o.Duration, o.ReqBytes, o.RespBytes
			prev.Detail = ev.Detail
			prev.Correlation = o.Correlation
			if len(prev.Ticks) >= MaxTicks {
				copy(prev.Ticks, prev.Ticks[1:])
				prev.Ticks = prev.Ticks[:MaxTicks-1]
			}
			prev.Ticks = append(prev.Ticks, Tick{At: o.Start, Duration: o.Duration, Class: o.Class})
			r.unlink(prev)
			r.pushFront(prev)
			return
		}
	}
	ev.Seq = r.seq
	ev.ID = r.epoch + "." + strconv.FormatUint(r.seq, 36)
	if o.Req != nil {
		ev.Facts = protocol.DescribeFacts(o.Req, o.Resp)
	}
	stored := &row{Event: ev, foldKey: foldKey(ev), lastReq: r.reqs}
	if len(r.byID) >= r.limit && r.tail != nil {
		old := r.tail
		r.unlink(old)
		delete(r.byID, old.ID)
		if oc := r.conns[old.Conn]; oc != nil && oc.recent[old.foldKey] == old {
			delete(oc.recent, old.foldKey)
		}
	}
	r.byID[ev.ID] = stored
	r.pushFront(stored)
	if ev.Idle && ev.Error == "" {
		c.recent[stored.foldKey] = stored
	}
}

func foldKey(e Event) string {
	return e.API + "\x00" + strings.Join(e.Topics, ",") + "\x00" + strconv.Itoa(e.TopicsMore) + "\x00" + e.Group + "\x00" + e.TxnID
}

func (r *Recorder) copyRow(x *row) Event {
	e := x.Event
	e.Ticks = slices.Clone(x.Ticks)
	return e
}

// Query returns the rows matching the filter, newest first by when they began (the
// column the page shows), at most Limit, and how many matched in all.
func (r *Recorder) Query(q Query) Result {
	r.mu.Lock()
	var out []Event
	total := 0
	var newest time.Time
	for x := r.head; x != nil; x = x.next {
		if !q.matches(x.Event) {
			continue
		}
		total++
		if x.Time.After(newest) {
			newest = x.Time
		}
		out = append(out, r.copyRow(x))
	}
	r.mu.Unlock()
	slices.SortFunc(out, func(a, b Event) int {
		switch {
		case a.Time.After(b.Time):
			return -1
		case a.Time.Before(b.Time):
			return 1
		case a.Seq > b.Seq:
			return -1
		case a.Seq < b.Seq:
			return 1
		}
		return 0
	})
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return Result{Rows: out, Total: total, Newest: newest}
}

// Events returns the rows matching f, newest first, at most limit.
func (r *Recorder) Events(f Filter, limit int) []Event {
	return r.Query(Query{Filter: f, Limit: limit}).Rows
}

// Row returns one row by its ID; ok is false once it has aged out.
func (r *Recorder) Row(id string) (Event, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	x := r.byID[id]
	if x == nil {
		return Event{}, false
	}
	return r.copyRow(x), true
}

// RowBySeq finds a row by the sequence number it last had, for an address that
// predates ids.
func (r *Recorder) RowBySeq(seq uint64) (Event, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for x := r.head; x != nil; x = x.next {
		if x.Seq == seq {
			return r.copyRow(x), true
		}
	}
	return Event{}, false
}

// NewSince counts the requests matching f that began after t, which is what a paused
// page says it has missed. A folded row counts the repeats it kept ticks for, so when
// the ticks do not reach back to t the count is a lower bound, and atLeast says so.
func (r *Recorder) NewSince(f Filter, t time.Time) (n int, atLeast bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for x := r.head; x != nil; x = x.next {
		if !x.Time.After(t) {
			continue // the list is by activity, not by start: keep looking.
		}
		if !f.matches(x.Event) {
			continue
		}
		if x.Count == 1 || len(x.Ticks) == 0 {
			n++
			continue
		}
		newer := 0
		for _, tk := range x.Ticks {
			if tk.At.After(t) {
				newer++
			}
		}
		n += newer
		if newer == len(x.Ticks) && x.Count > len(x.Ticks) {
			atLeast = true
		}
	}
	return n, atLeast
}

func (f Filter) matches(e Event) bool {
	if f.ErrorsOnly && !e.IsError() {
		return false
	}
	if f.Kind != "" && e.Kind != f.Kind {
		return false
	}
	if f.Conn != 0 && e.Conn != f.Conn {
		return false
	}
	if f.API != "" && !strings.Contains(strings.ToLower(e.API), strings.ToLower(f.API)) {
		return false
	}
	if f.Topic != "" && !slices.Contains(e.Topics, f.Topic) {
		return false
	}
	if f.Group != "" && e.Group != f.Group && e.ViaGroup != f.Group {
		return false
	}
	if f.Client != "" {
		q := strings.ToLower(f.Client)
		if !strings.Contains(strings.ToLower(e.ClientID), q) &&
			!strings.Contains(strings.ToLower(e.Software), q) &&
			!strings.Contains(strings.ToLower(e.Remote), q) {
			return false
		}
	}
	return true
}

func (q Query) matches(e Event) bool { return q.Filter.matches(e) }

// Clients returns the open connections that have sent a request, oldest first. The
// console's own are left out.
func (r *Recorder) Clients() []Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	out := make([]Client, 0, len(r.conns))
	for _, c := range r.conns {
		if !c.seen || isConsole(c.ClientID) {
			continue
		}
		cc := c.Client
		cc.Groups = slices.Clone(c.Groups)
		cc.Req1m, _ = c.counts.sum(now, time.Minute)
		cc.Req5m, cc.Err5m = c.counts.sum(now, 5*time.Minute)
		out = append(out, cc)
	}
	slices.SortFunc(out, func(a, b Client) int {
		switch {
		case a.Conn < b.Conn:
			return -1
		case a.Conn > b.Conn:
			return 1
		}
		return 0
	})
	return out
}

// Window is what the whole broker's counters say about a stretch of time.
type Window struct {
	Requests, Errors int
}

// Last returns the requests and errors of the last d (at most five minutes, to the
// nearest five seconds), folded repeats counted one by one. Errors are the answers that
// are refused, denied, not implemented or failed: expected flow is not one.
func (r *Recorder) Last(d time.Duration) Window {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, e := r.all.sum(r.now(), d)
	return Window{Requests: t, Errors: e}
}

// Requests is how many requests the recorder has seen since it started.
func (r *Recorder) Requests() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reqs
}

// Refusal is one error as the recorder has seen it: how often, from whom, and when.
type Refusal struct {
	Code  int16
	Name  string
	Class protocol.Class
	// Count is how many requests were answered with it (folded rows count whole).
	Count int
	// Clients are who got it, with how many times each, most first.
	Clients []ClientCount
	// First and Last bound when, and Latest is the ID of the newest row.
	First, Last time.Time
	Latest      string
	API         string
	Topics      []string
}

// ClientCount is a client and how many times.
type ClientCount struct {
	Client string
	Count  int
}

// Refusals groups the error rows the recorder holds by what they said, most often
// first (ties: newest first).
func (r *Recorder) Refusals() []Refusal {
	r.mu.Lock()
	defer r.mu.Unlock()
	by := map[string]*Refusal{}
	per := map[string]map[string]int{}
	for x := r.head; x != nil; x = x.next {
		if !x.IsError() {
			continue
		}
		key := x.Error
		g := by[key]
		if g == nil {
			g = &Refusal{Code: x.ErrorCode, Name: x.Error, Class: x.Class, First: x.Time, Last: x.Time, Latest: x.ID, API: x.API, Topics: x.Topics}
			by[key] = g
			per[key] = map[string]int{}
		}
		n := max(x.Count, 1)
		g.Count += n
		if x.Time.Before(g.First) {
			g.First = x.Time
		}
		if x.Time.After(g.Last) {
			g.Last, g.Latest, g.API, g.Topics = x.Time, x.ID, x.API, x.Topics
		}
		who := x.ClientID
		if who == "" {
			who = x.Remote
		}
		per[key][who] += n
	}
	out := make([]Refusal, 0, len(by))
	for key, g := range by {
		for who, n := range per[key] {
			g.Clients = append(g.Clients, ClientCount{who, n})
		}
		slices.SortFunc(g.Clients, func(a, b ClientCount) int {
			if a.Count != b.Count {
				return b.Count - a.Count
			}
			return strings.Compare(a.Client, b.Client)
		})
		out = append(out, *g)
	}
	slices.SortFunc(out, func(a, b Refusal) int {
		if a.Count != b.Count {
			return b.Count - a.Count
		}
		return b.Last.Compare(a.Last)
	})
	return out
}

// GroupEvent records something a coordinator did. It implements the broker's sink.
func (r *Recorder) GroupEvent(group, kind, member string, generation int32, detail string, members int, cause string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	group = clip(group, MaxName)
	g := r.groups[group]
	if g == nil {
		if len(r.groups) >= MaxGroups {
			// The group that has been quiet longest goes, not the one that is noisy.
			var oldest string
			var at uint64
			for name, l := range r.groups {
				if oldest == "" || l.last < at {
					oldest, at = name, l.last
				}
			}
			delete(r.groups, oldest)
		}
		g = &groupLog{}
		r.groups[group] = g
	}
	g.last = r.seq
	g.events = append(g.events, GroupEvent{
		Seq: r.seq, Time: r.now(), Group: group, Kind: clip(kind, MaxName),
		Member: clip(member, MaxName), Generation: generation, Detail: clip(detail, MaxText),
		Members: members, Cause: clip(cause, MaxName),
	})
	if len(g.events) > MaxGroupEvents {
		g.events = slices.Delete(g.events, 0, len(g.events)-MaxGroupEvents)
	}
}

// GroupEvents returns the events of one group, oldest first, at most the latest limit.
func (r *Recorder) GroupEvents(group string, limit int) []GroupEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	g := r.groups[group]
	if g == nil {
		return nil
	}
	out := slices.Clone(g.events)
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}
