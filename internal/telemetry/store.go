package telemetry

import (
	"crypto/rand"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// samplesKept is how many points each series keeps: at the five-second push interval
	// that is ten minutes, "a few minutes per client instance".
	samplesKept = 120
	// instanceTTL is how long an instance that stopped pushing is remembered. A client
	// that said it was terminating is forgotten sooner.
	instanceTTL   = 10 * time.Minute
	terminatedTTL = time.Minute
	maxInstances  = 200
	maxSeriesEach = 400
)

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
	ID         [16]byte
	ClientID   string // the client.id on its requests.
	Software   string // from ApiVersions, if the client sent it on this connection.
	Principal  string
	Host       string
	First      time.Time
	Last       time.Time
	Pushes     int
	Terminated bool
	Resource   map[string]string
	Series     []Series
}

type instance struct {
	Instance
	series map[string]*Series
	order  []string
}

// Store keeps the instances, bounded: at most maxInstances (the least recently heard
// from is dropped), each with at most maxSeriesEach series of samplesKept points.
type Store struct {
	mu        sync.Mutex
	instances map[[16]byte]*instance
	now       func() time.Time
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{instances: map[[16]byte]*instance{}, now: time.Now}
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
}

// Touch records that an instance asked for its subscription (or pushed), creating it if
// it is new. A client the broker has never heard of, whose id survived a broker restart,
// is adopted.
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
		if len(s.instances) >= maxInstances {
			s.dropOldest()
		}
		in = &instance{Instance: Instance{ID: id, First: now, Resource: map[string]string{}}, series: map[string]*Series{}}
		s.instances[id] = in
	}
	if who.ClientID != "" {
		in.ClientID = who.ClientID
	}
	if who.Software != "" {
		in.Software = who.Software
	}
	if who.Principal != "" {
		in.Principal = who.Principal
	}
	if who.Host != "" {
		in.Host = who.Host
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

// Push records one decoded push.
func (s *Store) Push(id [16]byte, who Who, p Payload, terminating bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.touch(id, who)
	in.Pushes++
	in.Terminated = terminating
	for k, v := range p.Resource {
		in.Resource[k] = v
	}
	at := s.now()
	for _, m := range p.Metrics {
		for _, pt := range m.Points {
			key := m.Name + "\x00" + pt.Attrs
			ser := in.series[key]
			if ser == nil {
				if len(in.series) >= maxSeriesEach {
					continue
				}
				ser = &Series{Name: m.Name, Attrs: pt.Attrs, Unit: m.Unit, Kind: m.Kind}
				in.series[key] = ser
				in.order = append(in.order, key)
			}
			t := pt.Time
			if t.IsZero() || t.After(at.Add(time.Minute)) {
				t = at
			}
			ser.Samples = append(ser.Samples, Sample{T: t, V: pt.Value})
			if n := len(ser.Samples); n > samplesKept {
				ser.Samples = slices.Delete(ser.Samples, 0, n-samplesKept)
			}
		}
	}
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

func (s *Store) dropOldest() {
	var oldest [16]byte
	var at time.Time
	first := true
	for id, in := range s.instances {
		if first || in.Last.Before(at) {
			oldest, at, first = id, in.Last, false
		}
	}
	delete(s.instances, oldest)
}

// Instances returns copies of what is held, most recently heard from first.
func (s *Store) Instances() []Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prune(s.now())
	out := make([]Instance, 0, len(s.instances))
	for _, in := range s.instances {
		cp := in.Instance
		cp.Resource = map[string]string{}
		for k, v := range in.Resource {
			cp.Resource[k] = v
		}
		for _, key := range in.order {
			ser := *in.series[key]
			ser.Samples = slices.Clone(ser.Samples)
			cp.Series = append(cp.Series, ser)
		}
		sort.SliceStable(cp.Series, func(a, b int) bool {
			if cp.Series[a].Name != cp.Series[b].Name {
				return cp.Series[a].Name < cp.Series[b].Name
			}
			return cp.Series[a].Attrs < cp.Series[b].Attrs
		})
		out = append(out, cp)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Last.After(out[b].Last) })
	return out
}

// ShortName is a metric's name without the "org.apache.kafka." prefix the standard ones
// share: "producer.node.request.latency.avg".
func ShortName(name string) string { return strings.TrimPrefix(name, "org.apache.kafka.") }
