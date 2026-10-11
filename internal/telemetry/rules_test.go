package telemetry

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

func gauge(name string, at time.Time, v float64) Payload {
	return Payload{Metrics: []Metric{{Name: name, Kind: Gauge, Points: []Point{{Time: at, Value: v}}}}}
}

// A point stamped 1970 or an hour ago was kept as written, and series were appended in
// arrival order (TRF-25).
func TestPastTimestampsAreClamped(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	for i, at := range []time.Time{{}, t0.Add(-time.Hour), time.Unix(0, 0), t0.Add(time.Hour)} {
		s.Push(id(byte(10+i)), Who{}, gauge("m", at, 1), false) // one instance each: order does not hide it.
	}
	for _, in := range s.Instances() {
		if sm := in.Series[0].Samples[0]; !sm.T.Equal(t0) {
			t.Errorf("a sample is stamped %v, want the broker's clock %v", sm.T, t0)
		}
	}
	// In order: a point that claims to be earlier than the one before it is placed at it.
	now = t0.Add(time.Minute)
	s.Push(id(2), Who{}, gauge("m", t0.Add(50*time.Second), 1), false)
	s.Push(id(2), Who{}, gauge("m", t0.Add(40*time.Second), 2), false)
	sm := s.Instances()
	var got []Sample
	for _, in := range sm {
		if in.ID == id(2) {
			got = in.Series[0].Samples
		}
	}
	if len(got) != 2 || got[1].T.Before(got[0].T) {
		t.Errorf("samples out of order: %+v", got)
	}
}

// New series past the cap are dropped, and the card says how many (TRF-26).
func TestDroppedSeriesAreCounted(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0 }
	var pts []Point
	for i := 0; i < maxSeriesEach+7; i++ {
		pts = append(pts, Point{Attrs: string(rune('a'+i%26)) + string(rune('a'+i/26)), Time: t0, Value: 1})
	}
	s.Push(id(1), Who{}, Payload{Metrics: []Metric{{Name: "m", Kind: Gauge, Points: pts}}}, false)
	in := s.Instances()[0]
	if in.Metrics != maxSeriesEach || in.Dropped != 7 || len(in.Series) != maxSeriesEach {
		t.Errorf("held %d, dropped %d, want %d and 7", in.Metrics, in.Dropped, maxSeriesEach)
	}
}

// An instance is nobody's: any principal and any connection may speak for an instance id,
// and the instance follows whoever spoke last. That is what Apache Kafka 4.3.1 does
// (recorded: conformance/recordings/telemetry-instance-binding-spaced.json), and the Java
// client relies on part of it: it subscribes on its bootstrap connection and pushes on
// the one the metadata gave it.
func TestAnyConnectionAndPrincipalMaySpeakForAnInstance(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0 }
	a := Who{ClientID: "a", Principal: "User:a", Conn: 1, Open: func() bool { return true }}
	s.Touch(id(1), a)
	b := Who{ClientID: "a", Principal: "User:a", Conn: 2, Open: func() bool { return true }}
	s.Push(id(1), b, gauge("m", t0, 1), false)
	if in := s.Instances()[0]; in.Conn != 2 || in.Pushes != 1 {
		t.Errorf("the instance did not follow the connection that spoke: %+v", in)
	}
	other := Who{ClientID: "other", Principal: "User:other", Conn: 3, Open: func() bool { return true }}
	s.Push(id(1), other, gauge("m", t0, 1), false)
	if in := s.Instances()[0]; in.Conn != 3 || in.Principal != "User:other" || in.Pushes != 2 {
		t.Errorf("another principal's push was not taken for the instance: %+v", in)
	}
}

func TestOneConnectionCannotFillTheStoreAndFreshIDsEvictTheClosedFirst(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	flood := Who{Conn: 9, Open: func() bool { return true }}
	for i := 0; i < 50; i++ {
		now = t0.Add(time.Duration(i) * time.Second)
		s.Touch(id2(i), flood)
	}
	if n := len(s.Instances()); n != maxPerConn {
		t.Errorf("one connection owns %d instances, want at most %d", n, maxPerConn)
	}
	if !s.Known(id2(49)) || s.Known(id2(0)) {
		t.Error("a flood from one connection did not cost it its own oldest instances")
	}

	// A full store of live clients with one that has closed: the closed one goes.
	s = NewStore()
	s.now = func() time.Time { return now }
	for i := 0; i < maxInstances; i++ {
		now = t0.Add(time.Duration(i) * time.Second)
		open := i != maxInstances/2
		s.Touch(id2(i), Who{Conn: uint64(1000 + i), Open: func() bool { return open }})
	}
	now = t0.Add(time.Hour / 10)
	s.Touch(id2(5000), Who{Conn: 77, Open: func() bool { return true }})
	if s.Known(id2(maxInstances/2)) || !s.Known(id2(0)) || !s.Known(id2(5000)) {
		t.Errorf("eviction took the wrong one: closed known=%v, 0 known=%v", s.Known(id2(maxInstances/2)), s.Known(id2(0)))
	}
}

// The gate answers as Apache Kafka 4.3.1 does (recorded:
// conformance/recordings/telemetry-throttle.json and telemetry-interval.json): the first
// push after a subscription is taken whenever it comes; after that a push is early, and
// refused, until an interval has passed since the last one taken; a refused request does
// not postpone the next; a terminating push is never early; and the push after it is
// refused once.
func TestPushesBeforeTheIntervalAreEarlyAndCounted(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	at := func(d time.Duration) { now = t0.Add(d) }
	w := Who{}
	if v := s.Subscribe(id(1), w); v != Accepted {
		t.Fatalf("first subscription: %v", v)
	}
	at(10 * time.Millisecond)
	if v := s.Arrive(id(1), w, false); v != Accepted {
		t.Errorf("the push straight after the subscription: %v, want Accepted", v)
	}
	at(2 * time.Second)
	if v := s.Arrive(id(1), w, false); v != Early {
		t.Errorf("a push 2 s after the last, interval 5 s: %v, want Early", v)
	}
	at(4900 * time.Millisecond)
	if v := s.Arrive(id(1), w, false); v != Early {
		t.Errorf("a push 4.89 s after the last: %v, want Early (no tolerance)", v)
	}
	// The refused ones did not postpone it: 5.01 s after the one that was taken.
	at(5020 * time.Millisecond)
	if v := s.Arrive(id(1), w, false); v != Accepted {
		t.Errorf("a push 5.01 s after the last taken: %v, want Accepted", v)
	}
	if in := s.Instances()[0]; in.Skipped != 2 {
		t.Errorf("skipped %d pushes, want 2", in.Skipped)
	}
	// A subscription request right after a push is early too, and one after the interval is not.
	at(5100 * time.Millisecond)
	if v := s.Subscribe(id(1), w); v != Early {
		t.Errorf("a subscription request 80 ms after a push: %v, want Early", v)
	}
	at(11 * time.Second)
	if v := s.Subscribe(id(1), w); v != Accepted {
		t.Errorf("a subscription request after the interval: %v, want Accepted", v)
	}
	// A terminating push is never early, and the push after it is refused once.
	at(11100 * time.Millisecond)
	if v := s.Arrive(id(1), w, true); v != Accepted {
		t.Errorf("a terminating push straight after: %v, want Accepted", v)
	}
	s.Push(id(1), w, Payload{}, true) // the broker stores it, which is what arms the refusal.
	if v := s.Arrive(id(1), w, false); v != AfterTerminating {
		t.Errorf("the push after a terminating one: %v, want AfterTerminating", v)
	}
	if v := s.Arrive(id(1), w, false); v != Early {
		t.Errorf("the one after that, still inside the interval: %v, want Early", v)
	}
	// An instance never seen is made by the push and its first push is taken.
	if v := s.Arrive(id(200), w, false); v != Accepted || !s.Known(id(200)) {
		t.Errorf("an unknown instance: %v, known %v", v, s.Known(id(200)))
	}
}

// A point cut off mid-way was stored as a zero value and the payload accepted (TRF-24).
func TestTruncatedPointIsMalformed(t *testing.T) {
	full := Encode(samplePayload())
	for n := 0; n < len(full); n++ {
		p, err := Decode(full[:n])
		if err != nil {
			if !errors.Is(err, ErrMalformed) {
				t.Errorf("cut at %d: error %v is not ErrMalformed", n, err)
			}
			continue
		}
		for _, m := range p.Metrics {
			for _, pt := range m.Points {
				if pt.Value == 0 && pt.Time.IsZero() && m.Name != "" && n < len(full)-1 {
					t.Errorf("cut at %d: a point with no value and no time was accepted: %+v", n, pt)
				}
			}
		}
	}
	// A cut inside a number point itself, built by hand: a double field with 3 of 8 bytes.
	pt := protowire.AppendTag(nil, 4, protowire.Fixed64Type)
	pt = append(pt, 1, 2, 3)
	if _, err := decodePointForTest(pt); err == nil {
		t.Error("a truncated double was read as a value")
	}
}

func decodePointForTest(b []byte) (Point, error) { return decodeNumberPoint(b) }
