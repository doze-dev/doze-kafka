// Package traffic remembers what clients have asked the broker, and what the
// consumer-group coordinators did about it, for the console to show.
//
// It answers "what did my app just do?" and "why does my consumer keep
// rebalancing?". It is a bounded, in-memory recorder: nothing is written to disk,
// the oldest rows are dropped when it is full, and a restart starts it empty. That
// is the right size for a tool that looks at the last few minutes of a laptop's
// traffic, and it keeps the recorder from ever being a cost worth measuring.
//
// A Recorder is a protocol.Observer; the broker feeds it group events through
// GroupEvent. The console never sees this package: it is handed an interface of its
// own, implemented in cmd/doze-kafka.
package traffic

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// ConsoleClientPrefix marks the console's own Kafka clients, whose requests are
// left out: the page that shows traffic would otherwise be mostly its own polling.
const ConsoleClientPrefix = "doze-kafka-console"

// collapseWindow is how long a repeated request may keep folding into one row. A
// consumer's heartbeat every three seconds is one row for as long as the consumer
// lives; this only stops a row that went quiet an hour ago from swallowing the next.
const collapseWindow = 10 * time.Minute

// Event is one row: a request, or a run of identical ones folded together.
type Event struct {
	// Seq orders rows by their latest activity. A row that folds in another request
	// moves to the front, so a tail ordered by Seq is ordered by what happened last.
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

	Topics []string
	Group  string
	Detail string
	// Error is the Kafka name of the first error in the response ("" when it went well).
	Error string
	// Idle marks a request that moved nothing. These are the rows that fold.
	Idle bool

	// Duration and the byte counts are the row's most recent request's.
	Duration            time.Duration
	ReqBytes, RespBytes int
}

// Client is a live connection.
type Client struct {
	Conn                                  uint64
	Remote, ClientID, Software, Principal string
	Opened, Last                          time.Time
	Requests                              int64
	// Groups are the consumer groups this connection has spoken for, newest last.
	Groups []string
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
}

// Filter narrows the rows. Zero fields match everything. Text fields match a
// substring, case-insensitively, except Group and Topic, which match a name exactly.
type Filter struct {
	Client     string // matches the client id, the software or the peer address.
	API        string
	Topic      string
	Group      string
	ErrorsOnly bool
}

// Recorder is the bounded store. The zero value is not usable; use New.
type Recorder struct {
	mu      sync.Mutex
	resolve func([16]byte) string

	seq   uint64
	ring  []*Event
	next  int // where the next row goes once the ring is full.
	limit int
	// recent is, per connection, the foldable row each kind of request is currently
	// folding into.
	recent map[uint64]map[string]*Event

	conns map[uint64]*Client

	groupLimit  int
	groupEvents []GroupEvent
}

// New returns a recorder holding at most capacity rows (and as many group events).
// resolve turns a topic id into its name, for the requests that carry only the id;
// it may be nil.
func New(capacity int, resolve func([16]byte) string) *Recorder {
	if capacity < 1 {
		capacity = 1
	}
	return &Recorder{
		resolve: resolve, limit: capacity, groupLimit: capacity,
		recent: map[uint64]map[string]*Event{}, conns: map[uint64]*Client{},
	}
}

func isConsole(clientID string) bool { return strings.HasPrefix(clientID, ConsoleClientPrefix) }

// ConnOpened implements protocol.Observer.
func (r *Recorder) ConnOpened(id uint64, remote string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.conns[id] = &Client{Conn: id, Remote: remote, Opened: time.Now(), Last: time.Now()}
}

// ConnClosed implements protocol.Observer.
func (r *Recorder) ConnClosed(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.conns, id)
	delete(r.recent, id)
}

// Request implements protocol.Observer.
func (r *Recorder) Request(o protocol.Observation) {
	r.mu.Lock()
	defer r.mu.Unlock()

	c := r.conns[o.Conn]
	if c == nil {
		c = &Client{Conn: o.Conn, Remote: o.Remote, Opened: o.Start}
		r.conns[o.Conn] = c
	}
	if o.ClientID != "" {
		c.ClientID = o.ClientID
	}
	if o.Software != "" {
		c.Software = o.Software
	}
	c.Principal = o.Principal
	c.Last = o.Start
	c.Requests++
	if g := o.Group; g != "" && !slices.Contains(c.Groups, g) {
		c.Groups = append(c.Groups, g)
		if len(c.Groups) > 4 {
			c.Groups = c.Groups[1:]
		}
	}
	if isConsole(c.ClientID) {
		return
	}

	topics := slices.Clone(o.Topics)
	for _, id := range o.TopicIDs {
		if r.resolve != nil {
			if n := r.resolve(id); n != "" && !slices.Contains(topics, n) {
				topics = append(topics, n)
			}
		}
	}
	ev := Event{
		First: o.Start, Time: o.Start, Count: 1, Conn: o.Conn, Remote: c.Remote,
		ClientID: c.ClientID, Software: c.Software, Principal: o.Principal,
		API: protocol.APIName(o.API), Key: int16(o.API), Version: o.Version,
		Topics: topics, Group: o.Group, Detail: o.Detail, Error: protocol.ErrorName(o.ErrorCode),
		Idle: o.Idle, Duration: o.Duration, ReqBytes: o.ReqBytes, RespBytes: o.RespBytes,
	}

	r.seq++
	if ev.Idle && ev.Error == "" {
		key := foldKey(ev)
		if prev := r.recent[o.Conn][key]; prev != nil && o.Start.Sub(prev.Time) < collapseWindow {
			prev.Count++
			prev.Time, prev.Seq = o.Start, r.seq
			prev.Duration, prev.ReqBytes, prev.RespBytes = o.Duration, o.ReqBytes, o.RespBytes
			prev.Detail = ev.Detail
			return
		}
	}
	ev.Seq = r.seq
	stored := &ev
	if len(r.ring) < r.limit {
		r.ring = append(r.ring, stored)
	} else {
		old := r.ring[r.next]
		if m := r.recent[old.Conn]; m != nil {
			if k := foldKey(*old); m[k] == old {
				delete(m, k)
			}
		}
		r.ring[r.next] = stored
		r.next = (r.next + 1) % r.limit
	}
	if ev.Idle && ev.Error == "" {
		m := r.recent[o.Conn]
		if m == nil {
			m = map[string]*Event{}
			r.recent[o.Conn] = m
		}
		m[foldKey(ev)] = stored
	}
}

func foldKey(e Event) string {
	return e.API + "\x00" + strings.Join(e.Topics, ",") + "\x00" + e.Group
}

// Events returns the rows matching f, newest activity first, at most limit.
func (r *Recorder) Events(f Filter, limit int) []Event {
	r.mu.Lock()
	out := make([]Event, 0, min(limit, len(r.ring)))
	for _, e := range r.ring {
		if f.matches(*e) {
			out = append(out, *e)
		}
	}
	r.mu.Unlock()
	slices.SortFunc(out, func(a, b Event) int {
		switch {
		case a.Seq > b.Seq:
			return -1
		case a.Seq < b.Seq:
			return 1
		}
		return 0
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (f Filter) matches(e Event) bool {
	if f.ErrorsOnly && e.Error == "" {
		return false
	}
	if f.API != "" && !strings.Contains(strings.ToLower(e.API), strings.ToLower(f.API)) {
		return false
	}
	if f.Topic != "" && !slices.Contains(e.Topics, f.Topic) {
		return false
	}
	if f.Group != "" && e.Group != f.Group {
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

// Clients returns the live connections, oldest first. The console's own are left out.
func (r *Recorder) Clients() []Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Client, 0, len(r.conns))
	for _, c := range r.conns {
		if isConsole(c.ClientID) {
			continue
		}
		cc := *c
		cc.Groups = slices.Clone(c.Groups)
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

// GroupEvent records something a coordinator did. It implements the broker's sink.
func (r *Recorder) GroupEvent(group, kind, member string, generation int32, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	ev := GroupEvent{Seq: r.seq, Time: time.Now(), Group: group, Kind: kind, Member: member, Generation: generation, Detail: detail}
	r.groupEvents = append(r.groupEvents, ev)
	if len(r.groupEvents) > r.groupLimit {
		r.groupEvents = slices.Delete(r.groupEvents, 0, len(r.groupEvents)-r.groupLimit)
	}
}

// GroupEvents returns the events of one group, oldest first, at most the latest limit.
func (r *Recorder) GroupEvents(group string, limit int) []GroupEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []GroupEvent
	for _, e := range r.groupEvents {
		if e.Group == group {
			out = append(out, e)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}
