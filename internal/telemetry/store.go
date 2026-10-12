package telemetry

import (
	"crypto/rand"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// samplesKept is how many points each series keeps: at the five-second push interval
	// that is five minutes, "a few minutes per client instance".
	samplesKept = 60
	// instanceTTL is how long an instance that stopped pushing is remembered. A client
	// that said it was terminating is forgotten sooner.
	instanceTTL   = 10 * time.Minute
	terminatedTTL = time.Minute
	maxInstances  = 100
	// maxSeriesEach holds a Java client's 136 series with room. samplesKept x
	// maxSeriesEach x maxInstances is what WorstCaseBytes budgets (TRF-6).
	maxSeriesEach = 150
	// maxPerConn is how many instances one connection may own: a flood of fresh ids from
	// one socket does not push real clients out (TRF-27).
	maxPerConn = 8
	// maxFuture and maxPast are how far a point's own timestamp may be from the broker's
	// clock before the broker's is used instead (TRF-25).
	maxFuture = time.Minute
	maxPast   = 10 * time.Minute

	// What a client may make the broker hold about it besides its numbers. Every one is
	// the client's own choice, so each is bounded on the way in (TRF-4).
	maxResourceKeys  = 32
	maxResourceKey   = 128
	maxResourceValue = 1024
	maxNameBytes     = 256 // a metric's name, and its attributes.
	maxUnitBytes     = 32
)

// clipText cuts s to at most n bytes on a character boundary, as a copy so that it
// does not keep the payload it was read from alive.
func clipText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return strings.Clone(s[:n])
}

// setResource adds one attribute, bounded: a key it has is updated, a new key is kept
// only while there are fewer than maxResourceKeys.
func setResource(m map[string]string, k, v string) {
	k, v = clipText(k, maxResourceKey), clipText(v, maxResourceValue)
	if _, ok := m[k]; !ok && len(m) >= maxResourceKeys {
		return
	}
	m[k] = v
}

// Sample is one point of a series.
type Sample struct {
	T time.Time
	V float64
}

// Series is one metric for one set of attributes, newest last.
type Series struct {
	Name    string
	Attrs   string
	Unit    string
	Kind    Kind
	Samples []Sample
}

// Last is the newest value.
func (s Series) Last() float64 {
	if len(s.Samples) == 0 {
		return 0
	}
	return s.Samples[len(s.Samples)-1].V
}

// Instance is one client as the broker knows it from its telemetry: the id the broker
// gave it, who it said it was, and what it has reported.
type Instance struct {
	ID        [16]byte
	ClientID  string // the client.id on its requests.
	Software  string // from ApiVersions, if the client sent it on this connection.
	Principal string
	Host      string
	// Conn is the connection that owns the instance (the first to claim it, or the one
	// that took it over once that closed): the traffic recorder's connection id, so a
	// client can be linked to its metrics.
	Conn       uint64
	First      time.Time
	Last       time.Time
	Pushes     int
	Terminated bool
	// Metrics is how many series are held, and Dropped how many more the client sent
	// that the per-instance cap kept out. Skipped is how many pushes came before the
	// interval had passed and were answered THROTTLING_QUOTA_EXCEEDED, as Apache Kafka
	// does, and so were not read.
	Metrics, Dropped, Skipped int
	Resource                  map[string]string
	// Series is every series held; empty in a summary (Summaries).
	Series []Series
}

// series holds one series compactly: the offsets are milliseconds from base, so a
// sample is 12 bytes, not the 32 of a time and a float.
type series struct {
	name, attrs, unit string
	kind              Kind
	base              time.Time
	off               []int32
	val               []float64
}

func (s *series) last() time.Time {
	if len(s.off) == 0 {
		return time.Time{}
	}
	return s.base.Add(time.Duration(s.off[len(s.off)-1]) * time.Millisecond)
}

func (s *series) add(t time.Time, v float64) {
	if len(s.off) == 0 {
		s.base = t
	}
	s.off = append(s.off, int32(t.Sub(s.base)/time.Millisecond))
	s.val = append(s.val, v)
	if n := len(s.off); n > samplesKept {
		drop := n - samplesKept
		shift := s.off[drop]
		s.base = s.base.Add(time.Duration(shift) * time.Millisecond)
		s.off = slices.Delete(s.off, 0, drop)
		for i := range s.off {
			s.off[i] -= shift
		}
		s.val = slices.Delete(s.val, 0, drop)
	}
}

func (s *series) export() Series {
	out := Series{Name: s.name, Attrs: s.attrs, Unit: s.unit, Kind: s.kind, Samples: make([]Sample, len(s.off))}
	for i := range s.off {
		out.Samples[i] = Sample{T: s.base.Add(time.Duration(s.off[i]) * time.Millisecond), V: s.val[i]}
	}
	return out
}

type instance struct {
	Instance
	series map[string]*series
	order  []string
	owner  func() bool // reports whether the owning connection is still open.

	// What the gate (Subscribe, Arrive) remembers, as Apache Kafka's
	// ClientMetricsInstance does: when a subscription was last given and a push
	// last taken, and that the last push said it was the client's last, which the
	// next push is refused for (recorded: conformance/recordings/telemetry-terminating.json).
	lastGet, lastPush time.Time
	hasGet, hasPush   bool
	saidTerminating   bool
	askAgain          bool // the last push was refused in a way that tells the client to ask again.
}

// Store keeps the instances, bounded: at most maxInstances, each with at most
// maxSeriesEach series of samplesKept points.
type Store struct {
	mu        sync.Mutex
	instances map[[16]byte]*instance
	now       func() time.Time
	interval  time.Duration
}

// NewStore returns an empty store. Its pushes are expected every five seconds; the
// broker says otherwise with SetPushInterval.
func NewStore() *Store {
	return &Store{instances: map[[16]byte]*instance{}, now: time.Now, interval: 5 * time.Second}
}

// SetPushInterval is how often clients were told to push, for the rate Subscribe and
// Arrive allow.
func (s *Store) SetPushInterval(d time.Duration) {
	s.mu.Lock()
	s.interval = d
	s.mu.Unlock()
}

// WorstCaseBytes estimates the most memory the store can hold: every instance with
// every series full. The budget test holds it to 32 MB, and measures the real thing.
func WorstCaseBytes() int {
	perSample := 4 + 8 // an offset and a value.
	perSeries := 128 + 2*maxNameBytes + maxUnitBytes
	perInstance := maxResourceKeys*(maxResourceKey+maxResourceValue) + 1024
	return maxInstances * (maxSeriesEach*(perSeries+samplesKept*perSample) + perInstance)
}

// NewID is a fresh client instance id (a version 4 UUID), as the broker assigns to a
// client that has none.
func NewID() [16]byte {
	var id [16]byte
	_, _ = rand.Read(id[:])
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	return id
}

// Who is what the broker knows of a client from the request that carried its telemetry.
type Who struct {
	ClientID, Software, Principal, Host string
	// Conn is the number of the connection the request came over, which is where the
	// instance was last heard from (it is not the instance's owner: that is the
	// principal). Open reports whether that connection is still open.
	Conn uint64
	Open func() bool
}

// Touch records that an instance asked for its subscription (or pushed), creating it if
// it is new. A client the broker has never heard of, whose id survived a broker restart,
// is adopted. An instance is not tied to a principal or a connection: Apache Kafka
// 4.3.1 lets any connection speak for any instance id (recorded:
// conformance/recordings/telemetry-instance-binding-spaced.json), and the instance
// follows the connection that spoke last, which is how the console links a client's
// requests to its metrics.
func (s *Store) Touch(id [16]byte, who Who) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touch(id, who)
}

func (s *Store) touch(id [16]byte, who Who) *instance {
	now := s.now()
	s.prune(now)
	in := s.instances[id]
	if in == nil {
		if who.Conn != 0 {
			owned := 0
			for _, o := range s.instances {
				if o.Conn == who.Conn {
					owned++
				}
			}
			if owned >= maxPerConn {
				// Never refused (Apache Kafka refuses nothing): the connection's own
				// oldest goes, so a flood of fresh ids from one socket costs the flood's
				// maker and nobody else.
				s.evictOf(who.Conn)
			}
		}
		if len(s.instances) >= maxInstances {
			s.evict()
		}
		in = &instance{Instance: Instance{ID: id, First: now, Resource: map[string]string{}}, series: map[string]*series{}}
		s.instances[id] = in
	}
	in.Conn, in.owner = who.Conn, who.Open
	if who.ClientID != "" {
		in.ClientID = clipText(who.ClientID, maxNameBytes/2)
	}
	if who.Software != "" {
		in.Software = clipText(who.Software, maxNameBytes/2)
	}
	if who.Principal != "" {
		in.Principal = clipText(who.Principal, maxNameBytes)
	}
	if who.Host != "" {
		in.Host = clipText(who.Host, maxNameBytes)
	}
	in.Last = now
	return in
}

// Known reports whether the broker has an instance with this id.
func (s *Store) Known(id [16]byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.instances[id]
	return ok
}

// Verdict is what the gate says to a request.
type Verdict int

const (
	// Accepted: the request is within the rules.
	Accepted Verdict = iota
	// Early: it came before the interval had passed (THROTTLING_QUOTA_EXCEEDED).
	Early
	// AfterTerminating: it is the push that follows one that said it was the last
	// (INVALID_REQUEST).
	AfterTerminating
)

// Subscribe is the gate for a client asking for its subscription. Apache Kafka answers
// a second request within the push interval of the last request it took from the
// instance, a subscription or a push, THROTTLING_QUOTA_EXCEEDED; a refused request does
// not postpone the next (recorded: conformance/recordings/telemetry-interval.json).
//
// One request is answered although it is early: the one that follows a push refused
// for its subscription id or its compression type (AskAgain), which is the client
// doing what the refusal told it to. It is answered once and sets no clock, so the push
// straight after it is still early (recorded:
// conformance/recordings/telemetry-ask-again.json).
func (s *Store) Subscribe(id [16]byte, who Who) Verdict {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.touch(id, who)
	now := s.now()
	asked := in.askAgain
	in.askAgain = false
	if last, ok := in.lastRequest(); ok && now.Sub(last) < s.interval {
		if asked {
			return Accepted
		}
		return Early
	}
	in.lastGet, in.hasGet = now, true
	return Accepted
}

// AskAgain notes that the push just let through by Arrive was refused for its
// subscription id or its compression type. Both tell the client to ask for its
// subscription again; see Subscribe.
func (s *Store) AskAgain(id [16]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in := s.instances[id]; in != nil {
		in.askAgain = true
	}
}

// Arrive is the gate for a push, applied before anything else is looked at, as Apache
// Kafka's ClientMetricsManager applies it (recorded:
// conformance/recordings/telemetry-terminating.json, telemetry-throttle.json and
// telemetry-interval.json).
//
// The instance remembers whether the last push it was sent said it was terminating,
// whatever became of that push: taken, refused further on for its subscription id or
// its compression, or refused here. While it does, a push is refused
// (AfterTerminating), and the push's own word replaces the memory. So terminating
// pushes in a row are refused from the second on for as long as they come, a
// terminating push that was itself refused still closes the instance, and an ordinary
// push after one is refused once.
//
// A push is on time when a subscription was given since the last push that set the
// clock, or that push is an interval old; a push on time sets the clock, whatever
// becomes of it after (an unknown subscription id still counts). One that is not on
// time is early and refused, unless it is terminating, which is the client's last word
// and is taken without setting the clock.
func (s *Store) Arrive(id [16]byte, who Who, terminating bool) Verdict {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.touch(id, who)
	was := in.saidTerminating
	in.saidTerminating = terminating
	in.askAgain = false // this push's own outcome decides it (AskAgain).
	if was {
		return AfterTerminating
	}
	now := s.now()
	onTime := !in.hasPush || (in.hasGet && in.lastPush.Before(in.lastGet)) || now.Sub(in.lastPush) >= s.interval
	if onTime {
		in.lastPush, in.hasPush = now, true
		return Accepted
	}
	if terminating {
		return Accepted
	}
	in.Skipped++
	return Early
}

// lastRequest is when the instance's last accepted request, a subscription or a push,
// was.
func (in *instance) lastRequest() (time.Time, bool) {
	switch {
	case in.hasGet && in.hasPush:
		return maxTime(in.lastGet, in.lastPush), true
	case in.hasGet:
		return in.lastGet, true
	case in.hasPush:
		return in.lastPush, true
	}
	return time.Time{}, false
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// Push records one decoded push.
func (s *Store) Push(id [16]byte, who Who, p Payload, terminating bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.touch(id, who)
	in.Pushes++
	in.Terminated = terminating
	for k, v := range p.Resource {
		setResource(in.Resource, k, v)
	}
	at := s.now()
	for _, m := range p.Metrics {
		for _, pt := range m.Points {
			name, attrs := clipText(m.Name, maxNameBytes), clipText(pt.Attrs, maxNameBytes)
			key := name + "\x00" + attrs
			ser := in.series[key]
			if ser == nil {
				if len(in.series) >= maxSeriesEach {
					in.Dropped++
					continue
				}
				ser = &series{name: name, attrs: attrs, unit: clipText(m.Unit, maxUnitBytes), kind: m.Kind}
				in.series[key] = ser
				in.order = append(in.order, key)
			}
			t := pt.Time
			if t.IsZero() || t.After(at.Add(maxFuture)) || t.Before(at.Add(-maxPast)) {
				t = at
			}
			if l := ser.last(); t.Before(l) {
				t = l // a series is in time order.
			}
			ser.add(t, pt.Value)
		}
	}
	in.Metrics = len(in.series)
}

func (s *Store) prune(now time.Time) {
	for id, in := range s.instances {
		ttl := instanceTTL
		if in.Terminated {
			ttl = terminatedTTL
		}
		if now.Sub(in.Last) > ttl {
			delete(s.instances, id)
		}
	}
}

// evictOf drops the connection's least recently heard instance.
func (s *Store) evictOf(conn uint64) {
	var victim [16]byte
	var best *instance
	for id, in := range s.instances {
		if in.Conn == conn && (best == nil || in.Last.Before(best.Last)) {
			victim, best = id, in
		}
	}
	if best != nil {
		delete(s.instances, victim)
	}
}

// evict makes room for one: a client that said it was closing goes first, then one
// whose connection has closed, then the least recently heard from.
func (s *Store) evict() {
	var victim [16]byte
	var best *instance
	rank := func(in *instance) int {
		switch {
		case in.Terminated:
			return 0
		case in.owner == nil || !in.owner():
			return 1
		}
		return 2
	}
	for id, in := range s.instances {
		if best == nil || rank(in) < rank(best) || (rank(in) == rank(best) && in.Last.Before(best.Last)) {
			victim, best = id, in
		}
	}
	delete(s.instances, victim)
}

func (in *instance) copy(withSeries bool) Instance {
	cp := in.Instance
	cp.Resource = make(map[string]string, len(in.Resource))
	for k, v := range in.Resource {
		cp.Resource[k] = v
	}
	cp.Series = nil
	if withSeries {
		for _, key := range in.order {
			cp.Series = append(cp.Series, in.series[key].export())
		}
		sort.SliceStable(cp.Series, func(a, b int) bool {
			if cp.Series[a].Name != cp.Series[b].Name {
				return cp.Series[a].Name < cp.Series[b].Name
			}
			return cp.Series[a].Attrs < cp.Series[b].Attrs
		})
	}
	return cp
}

func (s *Store) list(withSeries bool) []Instance {
	s.prune(s.now())
	out := make([]Instance, 0, len(s.instances))
	for _, in := range s.instances {
		out = append(out, in.copy(withSeries))
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Last.After(out[b].Last) })
	return out
}

// Instances returns copies of what is held, most recently heard from first, each with
// every series.
func (s *Store) Instances() []Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.list(true)
}

// Summaries is Instances without the series: who each client is and how much it has
// reported, which is all a list needs, at the cost of a few fields each.
func (s *Store) Summaries() []Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.list(false)
}

// Instance returns one instance with its series, and whether it is held.
func (s *Store) Instance(id [16]byte) (Instance, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prune(s.now())
	in := s.instances[id]
	if in == nil {
		return Instance{}, false
	}
	return in.copy(true), true
}

// ForConn returns the summary of the instance a connection owns (the most recently
// heard from, if it owns several), and whether there is one.
func (s *Store) ForConn(conn uint64) (Instance, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prune(s.now())
	var best *instance
	for _, in := range s.instances {
		if in.Conn == conn && (best == nil || in.Last.After(best.Last)) {
			best = in
		}
	}
	if best == nil {
		return Instance{}, false
	}
	return best.copy(false), true
}

// ShortName is a metric's name without the "org.apache.kafka." prefix the standard ones
// share: "producer.node.request.latency.avg".
func ShortName(name string) string { return strings.TrimPrefix(name, "org.apache.kafka.") }
