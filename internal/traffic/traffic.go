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
	"cmp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

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

	// bucketWidth and bucketCount make a connection's counters: five minutes of whole
	// five-second buckets, and the one that is filling. The whole broker's is the same
	// shape.
	bucketWidth = 5 * time.Second
	bucketCount = 61
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
	// Parts are the partitions the request named, as ranges ("p0–p2"), where its row
	// writes them beside a topic or a group (an offset commit); Gen its generation, when
	// HasGen; AutoCreate that a Metadata request allowed the broker to create a topic.
	Parts      string
	Gen        int32
	HasGen     bool
	AutoCreate bool
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
	// ClientStreak and ClientLastError are the same for the client this connection
	// belongs to (its client id, or its address when it has none): its requests in the
	// order they came, whichever of its connections carried them (A2).
	ClientStreak    int
	ClientLastClass protocol.Class
	ClientLastError string
	// ClientReq1m, ClientReq5m and ClientErr5m are the client's counts, across every
	// connection it has had while one of them has stayed open.
	ClientReq1m, ClientReq5m, ClientErr5m int
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

// Filter narrows the rows. Zero fields match everything. API and Any match a
// substring, case-insensitively; Client, Group, Topic and Kind match exactly.
type Filter struct {
	Client string // one client: its id exactly, or the address of a connection with none.
	API    string
	Topic  string
	// Group matches the rows that name the group and the fetches of its members.
	Group string
	// Kind is produce, fetch, groups or admin.
	Kind string
	// Conn matches one connection.
	Conn       uint64
	ErrorsOnly bool
	// Any is the page's one search field: it matches a row whose client, software,
	// address, principal, request, topics, group, transactional id or error contains it.
	Any string
}

// Query is a Filter with a limit.
type Query struct {
	Filter
	Limit int
}

// Result is the rows a query found, with how many matched in all.
type Result struct {
	Rows []Event
	// Total is how many rows matched the filter, whatever the limit, and Requests how
	// many requests those rows stand for (a folded row is one row and many requests).
	Total, Requests int
	// Held is how many requests all the rows the recorder holds stand for, whatever the
	// filter: the M of "N of M requests match".
	Held int
	// Newest is the start time of the newest matching row; the cursor a paused page
	// holds on to. Oldest is when the oldest matching row's first request began: how far
	// back what is held reaches.
	Newest, Oldest time.Time
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

// span is the window of whole buckets that ends where the bucket now filling begins:
// [from, to), window long (rounded up to whole buckets, at most five minutes). A count
// of it is of a whole minute (A7: the twelve buckets that ended with the filling one
// were 55 to 60 seconds).
func span(now time.Time, window time.Duration) (lo, hi int32, from, to time.Time) {
	hi = int32(now.UnixNano() / int64(bucketWidth))
	n := int32(window / bucketWidth)
	if window%bucketWidth != 0 {
		n++
	}
	n = min(n, bucketCount-1)
	lo = hi - n
	return lo, hi, time.Unix(0, int64(lo)*int64(bucketWidth)), time.Unix(0, int64(hi)*int64(bucketWidth))
}

// sum is the requests and errors in the window of whole buckets before now.
func (c *counters) sum(now time.Time, window time.Duration) (total, errs int) {
	lo, hi, _, _ := span(now, window)
	for i := range c.b {
		if b := c.b[i]; b.idx >= lo && b.idx < hi && b.total > 0 {
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
	// skey is the client this connection is counted under in the recorder's streaks:
	// its client id, or its address when it has none.
	skey string
}

// streak is a client's run of errors, its latest requests: the client's, across its
// connections, in the order they came (A2).
type streak struct {
	n     int
	class protocol.Class
	last  string
	conns int
	// counts are the client's requests and errors, whichever connection carried them.
	counts counters
}

// groupLog is one group's events.
type groupLog struct {
	events []GroupEvent
	last   uint64
}

// Recorder is the bounded store. The zero value is not usable; use New.
type Recorder struct {
	// mu is taken by every connection after every request (Request) and by every page
	// that reads. What is done under it is what all of them wait for: nothing that
	// grows with what a client sent (see Request), and readers share it.
	mu      sync.RWMutex
	resolve func([16]byte) string
	// describe is protocol.DescribeFactsNamed; a test replaces it to see where it runs.
	describe func(kmsg.Request, kmsg.Response, func([16]byte) string) protocol.Facts
	now      func() time.Time
	epoch    string

	seq  uint64 // orders rows and group events.
	reqs uint64 // requests recorded, folded ones included.
	held uint64 // requests the rows now held stand for.

	byID       map[string]*row
	head, tail *row // most recently active first.
	limit      int
	conns      map[uint64]*conn
	streaks    map[string]*streak // by client: one entry per client with an open connection.
	all        counters           // the whole broker, console clients left out.

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
		resolve: resolve, describe: protocol.DescribeFactsNamed, limit: capacity, now: time.Now,
		epoch: strconv.FormatInt(time.Now().Unix(), 36),
		byID:  map[string]*row{}, conns: map[uint64]*conn{}, groups: map[string]*groupLog{}, streaks: map[string]*streak{},
	}
}

// countUnder moves a connection's place in the streaks to the client it now says it is.
// The lock is held.
func (r *Recorder) countUnder(c *conn, key string) {
	if c.skey == key {
		return
	}
	r.release(c)
	c.skey = key
	s := r.streaks[key]
	if s == nil {
		s = &streak{}
		r.streaks[key] = s
	}
	s.conns++
}

// release takes a connection out of the streaks; a client's goes with its last
// connection. The lock is held.
func (r *Recorder) release(c *conn) {
	if c.skey == "" {
		return
	}
	if s := r.streaks[c.skey]; s != nil {
		if s.conns--; s.conns <= 0 {
			delete(r.streaks, c.skey)
		}
	}
	c.skey = ""
}

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
	if c := r.conns[id]; c != nil {
		r.release(c)
	}
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

// prepared is what a request says about itself that needs no lock to work out: its
// strings clipped, its topic ids named, its error classed.
type prepared struct {
	remote, clientID, software, principal string
	group, txnID, detail, closed, errName string
	api                                   string
	topics                                []string
	more                                  int
	class                                 protocol.Class
	isErr                                 bool
}

// prepare does the work of a request that does not need the recorder's state. It runs
// before the lock is taken: clipping copies strings, and naming a topic id asks the
// broker's metadata, which has a lock of its own.
func (r *Recorder) prepare(o *protocol.Observation) prepared {
	p := prepared{
		remote: clip(o.Remote, MaxRemote), clientID: clip(o.ClientID, MaxName), software: clip(o.Software, MaxName),
		principal: clip(o.Principal, MaxName), group: clip(o.Group, MaxName), txnID: clip(o.TxnID, MaxName),
		detail: clip(o.Detail, MaxText), closed: clip(o.Closed, MaxText),
		api: protocol.APIName(o.API), class: o.Class,
	}
	if p.class == "" && o.ErrorCode != 0 {
		p.class = protocol.Classify(o.API, o.ErrorCode)
	}
	p.errName = protocol.ErrorName(o.ErrorCode)
	if o.Closed != "" {
		p.errName = "CONNECTION_CLOSED"
		if p.class == "" {
			p.class = protocol.ClassFailed
		}
	}
	// A connection that ended is an error by its class: a client that hung up before its
	// answer is not one (the broker did not fail).
	p.isErr = p.class.IsError()
	topics := o.Topics
	if len(o.TopicIDs) > 0 && r.resolve != nil {
		topics = slices.Clone(o.Topics)
		for _, id := range o.TopicIDs {
			if n := r.resolve(id); n != "" && !slices.Contains(topics, n) {
				topics = append(topics, n)
			}
		}
	}
	p.topics, p.more = clipTopics(topics)
	return p
}

// Request implements protocol.Observer.
//
// Every connection calls it after every request, so what it does with the lock held is
// what every connection waits for: finding the connection, counting, and putting one
// row on the list. Describing the request (prepare, and the partitions and decoded
// fields of DescribeFacts) is done without it.
func (r *Recorder) Request(o protocol.Observation) {
	// The process's own requests (the console beside the broker, polling) are not
	// traffic. Which they are is said by the connection (protocol.Adopt), in memory:
	// nothing a client sends, its client id least of all, makes a request one of them.
	if o.Own {
		return
	}
	p := r.prepare(&o)

	// A request that moved nothing may fold into the row before it, and then it needs no
	// description at all (a heartbeat every three seconds from every consumer). Whether
	// it folds is only known under the lock, so such a request is described after the
	// lock has said it starts a row, with the lock let go. Every other request is
	// described now.
	mayFold := o.Idle && p.errName == ""
	var facts protocol.Facts
	described := o.Req == nil
	if !mayFold && !described {
		facts, described = r.describe(o.Req, o.Resp, r.resolve), true
	}

	r.mu.Lock()
	c := r.conns[o.Conn]
	if c == nil {
		c = &conn{Client: Client{Conn: o.Conn, Remote: p.remote, Opened: o.Start}, recent: map[string]*row{}}
		r.conns[o.Conn] = c
	}
	r.count(c, &o, &p)
	ev := Event{
		First: o.Start, Time: o.Start, Count: 1, Conn: o.Conn, Remote: c.Remote,
		ClientID: c.ClientID, Software: c.Software, Principal: c.Principal,
		API: p.api, Key: int16(o.API), Version: o.Version, Kind: o.Kind, Correlation: o.Correlation,
		Topics: p.topics, TopicsMore: p.more, Group: p.group, TxnID: p.txnID, Detail: p.detail,
		Error: p.errName, ErrorCode: o.ErrorCode, Class: p.class, Closed: p.closed,
		Idle: o.Idle, Duration: o.Duration, ReqBytes: o.ReqBytes, RespBytes: o.RespBytes,
		Parts: clip(o.Parts, MaxText), Gen: o.Gen, HasGen: o.HasGen, AutoCreate: o.AutoCreate,
	}
	// A fetch does not say which group it belongs to; the connection knows.
	if ev.Group == "" && ev.TxnID == "" && o.Kind == "fetch" && len(c.Groups) > 0 {
		ev.ViaGroup = c.Groups[len(c.Groups)-1]
	}

	if mayFold && r.fold(c, &ev) {
		r.mu.Unlock()
		return
	}
	if !described {
		r.mu.Unlock()
		facts = r.describe(o.Req, o.Resp, r.resolve)
		r.mu.Lock()
		// A connection's requests come one at a time, so nothing of this connection
		// changed meanwhile; if something did (a caller that is not a connection), the
		// row it made is the one to fold into.
		if r.fold(c, &ev) {
			r.mu.Unlock()
			return
		}
	}
	ev.Facts = facts
	r.insert(c, ev)
	r.mu.Unlock()
}

// count notes a request against its connection and the counters. The lock is held.
func (r *Recorder) count(c *conn, o *protocol.Observation, p *prepared) {
	c.seen = true
	if p.clientID != "" {
		c.ClientID = p.clientID
	}
	if p.software != "" {
		c.Software = p.software
	}
	c.Principal = p.principal
	c.Last = o.Start
	c.Requests++
	if p.group != "" && !slices.Contains(c.Groups, p.group) {
		c.Groups = append(c.Groups, p.group)
		if len(c.Groups) > 4 {
			c.Groups = c.Groups[1:]
		}
	}
	if p.txnID != "" {
		c.TxnID = p.txnID
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
	c.counts.add(o.Start, p.isErr)
	r.all.add(o.Start, p.isErr)
	r.reqs++
	if p.isErr {
		c.Streak++
		c.LastError = p.errName
	} else {
		c.Streak, c.LastError = 0, ""
	}
	key := c.ClientID
	if key == "" {
		key = c.Remote
	}
	r.countUnder(c, key)
	s := r.streaks[key]
	s.counts.add(o.Start, p.isErr)
	if p.isErr {
		s.n++
		s.last, s.class = p.errName, p.class
	} else {
		s.n, s.last, s.class = 0, "", ""
	}
}

// fold adds the request to the row its connection is folding requests like it into,
// and reports whether there was one. The lock is held.
//
// A connection folds one row per request kind, the latest of that kind: any other
// request of the kind (another generation, a different answer) ends the fold, and a
// repeat after it is a row of its own, not the old row going on (A4).
func (r *Recorder) fold(c *conn, ev *Event) bool {
	prev := c.recent[ev.API]
	if prev == nil || !sameRequest(&prev.Event, ev) || ev.Time.Sub(prev.Time) >= collapseWindow {
		return false
	}
	if len(prev.Ticks) == 0 {
		// The row's first request is a tick too, so the strip shows all of them.
		prev.Ticks = append(prev.Ticks, Tick{At: prev.First, Duration: prev.Duration, Class: prev.Class})
	}
	r.seq++
	r.held++
	prev.Count++
	prev.Time, prev.Seq = ev.Time, r.seq
	prev.Duration, prev.ReqBytes, prev.RespBytes = ev.Duration, ev.ReqBytes, ev.RespBytes

	prev.Correlation = ev.Correlation
	if len(prev.Ticks) >= MaxTicks {
		copy(prev.Ticks, prev.Ticks[1:])
		prev.Ticks = prev.Ticks[:MaxTicks-1]
	}
	prev.Ticks = append(prev.Ticks, Tick{At: ev.Time, Duration: ev.Duration, Class: ev.Class})
	r.unlink(prev)
	r.pushFront(prev)
	return true
}

// insert puts a new row at the front, dropping the least recently active one when the
// recorder is full. The lock is held.
func (r *Recorder) insert(c *conn, ev Event) {
	r.seq++
	ev.Seq = r.seq
	ev.ID = r.epoch + "." + strconv.FormatUint(r.seq, 36)
	stored := &row{Event: ev}
	if len(r.byID) >= r.limit && r.tail != nil {
		old := r.tail
		r.unlink(old)
		delete(r.byID, old.ID)
		r.held -= uint64(max(old.Count, 1))
		if oc := r.conns[old.Conn]; oc != nil && oc.recent[old.API] == old {
			delete(oc.recent, old.API)
		}
	}
	r.byID[ev.ID] = stored
	r.held++
	r.pushFront(stored)
	if ev.Idle && ev.Error == "" {
		c.recent[ev.API] = stored
	} else {
		delete(c.recent, ev.API) // a different request of the kind breaks the fold.
	}
}

// sameRequest is what two requests must share to be one row: the request, what it
// names and its detail (a heartbeat's generation, a fetch's wait). The page says each
// repeat "asked the same thing and got the same answer": without the detail a new
// generation folded into the old one's row, across six rebalances (A4). It compares the
// fields where they lie (a key built of them was a copy of the topics in every row).
func sameRequest(a, b *Event) bool {
	return a.API == b.API && a.TopicsMore == b.TopicsMore && a.Group == b.Group && a.TxnID == b.TxnID &&
		a.Detail == b.Detail && slices.Equal(a.Topics, b.Topics)
}

func (r *Recorder) copyRow(x *row) Event {
	e := x.Event
	e.Ticks = slices.Clone(x.Ticks)
	return e
}

// newer orders rows as the page lists them: by when they began, the later first, and
// by latest activity among those that began together.
func newer(a, b *Event) bool {
	if !a.Time.Equal(b.Time) {
		return a.Time.After(b.Time)
	}
	return a.Seq > b.Seq
}

// Query returns the rows matching the filter, newest first by when they began (the
// column the page shows), at most Limit, and what matched in all.
//
// It reads under the lock every connection's requests wait on, so it copies only the
// rows it returns: with a limit, the newest Limit of them are picked out by pointer
// while the list is walked and copied at the end. (It copied every matching row, 3.5 MB
// for a full recorder, and cut the copy afterwards, several times a poll.) Readers
// share the lock with each other.
func (r *Recorder) Query(q Query) Result {
	var res Result
	f := q.Filter.lowered()
	r.mu.RLock()
	res.Held = int(r.held)
	var top []*row
	if q.Limit > 0 {
		top = make([]*row, 0, min(q.Limit, len(r.byID)))
	}
	for x := r.head; x != nil; x = x.next {
		if !f.matches(&x.Event) {
			continue
		}
		res.Total++
		res.Requests += max(x.Count, 1)
		if x.Time.After(res.Newest) {
			res.Newest = x.Time
		}
		if res.Oldest.IsZero() || x.First.Before(res.Oldest) {
			res.Oldest = x.First
		}
		switch {
		case q.Limit <= 0 || len(top) < q.Limit:
			top = append(top, x)
		case newer(&x.Event, &top[len(top)-1].Event):
			top[len(top)-1] = x
		default:
			continue
		}
		if q.Limit > 0 {
			// The list is by latest activity, which is nearly by start, so a row seldom
			// moves far from the end it was put at.
			for i := len(top) - 1; i > 0 && newer(&top[i].Event, &top[i-1].Event); i-- {
				top[i], top[i-1] = top[i-1], top[i]
			}
		}
	}
	res.Rows = make([]Event, len(top))
	if q.Limit <= 0 {
		// Everything was asked for. The pointers are put in order, which is quick (they
		// are nearly in it) and moves eight bytes where sorting the copies moved a row.
		slices.SortFunc(top, func(a, b *row) int {
			if newer(&a.Event, &b.Event) {
				return -1
			}
			return 1 // two rows never share a sequence number.
		})
	}
	for i, x := range top {
		res.Rows[i] = r.copyRow(x)
	}
	r.mu.RUnlock()
	if len(res.Rows) == 0 {
		res.Rows = nil
	}
	return res
}

// Events returns the rows matching f, newest first, at most limit.
func (r *Recorder) Events(f Filter, limit int) []Event {
	return r.Query(Query{Filter: f, Limit: limit}).Rows
}

// Row returns one row by its ID; ok is false once it has aged out.
func (r *Recorder) Row(id string) (Event, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	x := r.byID[id]
	if x == nil {
		return Event{}, false
	}
	return r.copyRow(x), true
}

// RowBySeq finds a row by the sequence number it last had, for an address that
// predates ids.
func (r *Recorder) RowBySeq(seq uint64) (Event, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
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
	f = f.lowered()
	r.mu.RLock()
	defer r.mu.RUnlock()
	for x := r.head; x != nil; x = x.next {
		if !x.Time.After(t) {
			continue // the list is by activity, not by start: keep looking.
		}
		if !f.matches(&x.Event) {
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

// lowered is the filter with its substring fields in lower case, done once for a walk
// of the list rather than once a row.
func (f Filter) lowered() Filter {
	f.API, f.Any = strings.ToLower(f.API), strings.ToLower(f.Any)
	return f
}

// containsFold reports whether s contains lower, which is already in lower case,
// whatever the case of s. It allocates nothing for the plain names this is asked about
// (a walk of the list asks it thousands of times with the lock held).
func containsFold(s, lower string) bool {
	if lower == "" {
		return true
	}
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return strings.Contains(strings.ToLower(s), lower)
		}
	}
	for i := 0; i+len(lower) <= len(s); i++ {
		j := 0
		for ; j < len(lower); j++ {
			c := s[i+j]
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != lower[j] {
				break
			}
		}
		if j == len(lower) {
			return true
		}
	}
	return false
}

// matches reports whether a row passes the filter, which must be lowered. The row is
// read where it lies: this runs for every row with the lock held.
func (f *Filter) matches(e *Event) bool {
	if f.ErrorsOnly && !e.IsError() {
		return false
	}
	if f.Kind != "" && e.Kind != f.Kind {
		return false
	}
	if f.Conn != 0 && e.Conn != f.Conn {
		return false
	}
	if f.API != "" && !containsFold(e.API, f.API) {
		return false
	}
	if f.Topic != "" && !slices.Contains(e.Topics, f.Topic) {
		return false
	}
	if f.Group != "" && e.Group != f.Group && e.ViaGroup != f.Group {
		return false
	}
	// The client is one client: its id, or for a connection with none, its address. It
	// was a substring of the id, the software or the address, so "checkout" listed
	// checkout-api's requests too (A6). The page's one field (Any) is the search.
	if f.Client != "" && e.ClientID != f.Client && (e.ClientID != "" || e.Remote != f.Client) {
		return false
	}
	if f.Any != "" && !f.any(e) {
		return false
	}
	return true
}

// any is the page's one search field: the client, its software, the address, the
// principal, the request, a topic, the group or transactional id, or the error
// contains what was typed.
func (f *Filter) any(e *Event) bool {
	for _, s := range [...]string{e.ClientID, e.Software, e.Remote, e.API, e.Group, e.ViaGroup, e.TxnID, e.Principal, e.Error} {
		if containsFold(s, f.Any) {
			return true
		}
	}
	for _, s := range e.Topics {
		if containsFold(s, f.Any) {
			return true
		}
	}
	return false
}

// Clients returns the open connections that have sent a request, oldest first. The
// console's own are left out.
func (r *Recorder) Clients() []Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := r.now()
	out := make([]Client, 0, len(r.conns))
	for _, c := range r.conns {
		if !c.seen {
			continue
		}
		cc := c.Client
		cc.Groups = slices.Clone(c.Groups)
		cc.Req1m, _ = c.counts.sum(now, time.Minute)
		cc.Req5m, cc.Err5m = c.counts.sum(now, 5*time.Minute)
		if s := r.streaks[c.skey]; s != nil {
			cc.ClientStreak, cc.ClientLastError, cc.ClientLastClass = s.n, s.last, s.class
			cc.ClientReq1m, _ = s.counts.sum(now, time.Minute)
			cc.ClientReq5m, cc.ClientErr5m = s.counts.sum(now, 5*time.Minute)
		}
		out = append(out, cc)
	}
	slices.SortFunc(out, func(a, b Client) int { return cmp.Compare(a.Conn, b.Conn) })
	return out
}

// Window is what the whole broker's counters say about a stretch of time: the requests
// that began in [From, To), and the errors among them.
type Window struct {
	Requests, Errors int
	From, To         time.Time
}

// LastMinute is the requests and errors of the last whole minute counted.
func (r *Recorder) LastMinute() Window {
	return r.Last(time.Minute)
}

// Last returns the requests and errors of the last d of whole five-second buckets (at
// most five minutes), folded repeats counted one by one, and the stretch they are of.
// Errors are the answers that are refused, denied, not implemented or failed: expected
// flow is not one.
func (r *Recorder) Last(d time.Duration) Window {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := r.now()
	t, e := r.all.sum(now, d)
	_, _, from, to := span(now, d)
	return Window{Requests: t, Errors: e, From: from.In(now.Location()), To: to.In(now.Location())}
}

// Requests is how many requests the recorder has seen since it started.
func (r *Recorder) Requests() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
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
	r.mu.RLock()
	defer r.mu.RUnlock()
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
	r.mu.RLock()
	defer r.mu.RUnlock()
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
