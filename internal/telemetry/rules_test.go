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

// An instance is a principal's: another principal cannot take it over or replace its
// who. Another connection of the same principal can speak for it, and the instance then
// follows that connection: the Java client subscribes on its bootstrap connection and
// pushes on the one the metadata gave it. What real Kafka does about the rest is
// unrecorded (queue R4).
func TestInstanceBelongsToItsPrincipalAndFollowsItsConnection(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0 }
	open := true
	a := Who{ClientID: "a", Principal: "User:a", Conn: 1, Open: func() bool { return open }}
	if !s.Touch(id(1), a) {
		t.Fatal("the first claim was refused")
	}
	b := Who{ClientID: "a", Principal: "User:a", Conn: 2, Open: func() bool { return true }}
	if !s.Push(id(1), b, gauge("m", t0, 1), false) {
		t.Fatal("the same principal's other connection was refused")
	}
	if in := s.Instances()[0]; in.Conn != 2 || in.Pushes != 1 {
		t.Errorf("the instance did not follow the connection that spoke: %+v", in)
	}
	evil := Who{ClientID: "evil", Principal: "User:evil", Conn: 3, Open: func() bool { return true }}
	if s.Touch(id(1), evil) || s.Push(id(1), evil, gauge("m", t0, 1), false) {
		t.Error("another principal took the instance")
	}
	if !s.Owned(id(1), evil) || s.Owned(id(1), a) || s.Owned(id(5), evil) {
		t.Error("Owned does not say who the instance belongs to")
	}
	if in := s.Instances()[0]; in.ClientID != "a" || in.Principal != "User:a" {
		t.Errorf("the refused claim changed the instance: %+v", in)
	}
	_ = open
}

func TestOneConnectionCannotFillTheStoreAndFreshIDsEvictTheClosedFirst(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	flood := Who{Conn: 9, Open: func() bool { return true }}
	accepted := 0
	for i := 0; i < 50; i++ {
		if s.Touch(id2(i), flood) {
			accepted++
		}
	}
	if accepted != maxPerConn {
		t.Errorf("one connection owns %d instances, want at most %d", accepted, maxPerConn)
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

// Pushes faster than the interval allows are not decoded and are counted; a terminating
// push always is (TRF-5).
func TestPushesPastTheRateAreSkippedAndCounted(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	s.Touch(id(1), Who{})
	admitted := 0
	for i := 0; i < 100; i++ {
		now = t0.Add(time.Duration(i) * time.Millisecond)
		if s.Admit(id(1), false) {
			admitted++
		}
	}
	if admitted != pushBurst {
		t.Errorf("%d of 100 pushes in 100 ms admitted, want the burst of %d", admitted, pushBurst)
	}
	if in := s.Instances()[0]; in.Skipped != 100-pushBurst {
		t.Errorf("skipped = %d, want %d", in.Skipped, 100-pushBurst)
	}
	if !s.Admit(id(1), true) {
		t.Error("a terminating push was skipped")
	}
	// After a pause it is admitted again, at the interval's third.
	now = t0.Add(10 * time.Second)
	if !s.Admit(id(1), false) {
		t.Error("a push after the interval was skipped")
	}
	if !s.Admit(id(200), false) {
		t.Error("an unknown instance is the broker's to refuse, not the throttle's")
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
