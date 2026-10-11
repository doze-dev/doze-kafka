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
	// pushBurst is how many pushes an instance may make in a burst; after it, pushes are
	// admitted at one per third of the interval it was told to push at (TRF-5).
	pushBurst = 20
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
	// that the per-instance cap kept out. Skipped is how many pushes came faster than
	// the interval allows and were not decoded.
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
	series  map[string]*series
	order   []string
	owner   func() bool // reports whether the owning connection is still open.
	tokens  float64
	refill  time.Time
	hasPush bool
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

// SetPushInterval is how often clients were told to push, for the rate Admit allows.
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
// is adopted. It reports false when the instance belongs to another principal: the
// caller must not serve it as that instance. An instance belongs to a principal, not to
// a connection: another connection of the same principal may speak for it, and the
// instance follows the connection that spoke last (TRF-27, narrowed).
func (s *Store) Touch(id [16]byte, who Who) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.touch(id, who) != nil
}

// Owned reports whether id exists and belongs to somebody else than who: the claim
// Touch would refuse. The broker uses it to give a client that quotes another's id a
// fresh one.
func (s *Store) Owned(id [16]byte, who Who) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.instances[id]
	return in != nil && !s.mayClaim(in, who)
}

func (s *Store) mayClaim(in *instance, who Who) bool {
	if who.Principal != "" && in.Principal != "" && who.Principal != in.Principal {
		return false
	}
	// Not the connection: a client may speak to the broker over more than one (the Java
	// client subscribes on its bootstrap connection and pushes on the one metadata gave
	// it), and what real Kafka does about another connection's push is unrecorded (queue
	// R4). The instance follows whichever connection last spoke for it.
	return true
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
				return nil
			}
		}
		if len(s.instances) >= maxInstances {
			s.evict()
		}
		in = &instance{Instance: Instance{ID: id, First: now, Resource: map[string]string{}}, series: map[string]*series{}, tokens: pushBurst}
		s.instances[id] = in
	} else if !s.mayClaim(in, who) {
		return nil
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

// Admit says whether a push from this instance is within the rate its interval allows:
// a burst of pushBurst, then one per third of the interval. A push that is not is
// counted (Instance.Skipped) and need not be decoded. A terminating push is always
// admitted: it is the client's last word.
func (s *Store) Admit(id [16]byte, terminating bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.instances[id]
	if in == nil || terminating {
		return true
	}
	now := s.now()
	if in.hasPush {
		in.tokens = min(pushBurst, in.tokens+float64(now.Sub(in.refill))/float64(s.interval/3))
	}
	in.refill, in.hasPush = now, true
	if in.tokens < 1 {
		in.Skipped++
		return false
	}
	in.tokens--
	return true
}

// Push records one decoded push. It reports false when the instance is another
// connection's or principal's, and nothing was recorded.
func (s *Store) Push(id [16]byte, who Who, p Payload, terminating bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.touch(id, who)
	if in == nil {
		return false
	}
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
	return true
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
