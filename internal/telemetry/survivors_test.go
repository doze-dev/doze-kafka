package telemetry

import (
	"fmt"
	"math"
	"testing"
	"time"
)

// A histogram point's own time is read (field 3 is a fixed64) and kept apart from its count.
func TestAHistogramPointKeepsItsTime(t *testing.T) {
	histPoint := cat(fixed64(3, uint64(t0.UnixNano())), fixed64(4, 4), fixed64(5, math.Float64bits(10)))
	hist := bytesField(9, bytesField(1, histPoint))
	m := bytesField(2, cat(bytesField(1, []byte("h")), hist))
	p, err := Decode(bytesField(1, bytesField(2, m)))
	if err != nil {
		t.Fatal(err)
	}
	pt := p.Metrics[0].Points[0]
	if !pt.Time.Equal(t0) || pt.Count != 4 {
		t.Errorf("point = %+v, want time %v and count 4", pt, t0)
	}
}

// A push exactly one interval after the last is on time.
func TestAPushExactlyOneIntervalLaterIsOnTime(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	if v := s.Arrive(id(1), Who{}, false); v != Accepted {
		t.Fatal(v)
	}
	now = t0.Add(5*time.Second - time.Nanosecond)
	if v := s.Arrive(id(1), Who{}, false); v != Early {
		t.Errorf("1ns short of the interval: %v, want Early", v)
	}
	now = t0.Add(5 * time.Second)
	if v := s.Arrive(id(1), Who{}, false); v != Accepted {
		t.Errorf("exactly one interval later: %v, want Accepted", v)
	}
}

// A point five minutes old is kept as stamped; ten minutes and a nanosecond old is not.
func TestAPointWithinTheWindowKeepsItsOwnTime(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0 }
	old := t0.Add(-5 * time.Minute)
	edge := t0.Add(-10 * time.Minute)
	soon := t0.Add(30 * time.Second)
	for i, at := range []time.Time{old, edge, soon, t0.Add(-10*time.Minute - time.Nanosecond)} {
		s.Push(id(byte(i+1)), Who{}, gauge("m", at, 1), false)
	}
	want := []time.Time{old, edge, soon, t0}
	for i, w := range want {
		in, _ := s.Instance(id(byte(i + 1)))
		if got := in.Series[0].Samples[0].T; !got.Equal(w) {
			t.Errorf("point %d stamped %v, want %v", i, got, w)
		}
	}
}

// Software and host are remembered when a later request says nothing of them.
func TestWhoNotSaidIsNotForgotten(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0 }
	s.Touch(id(1), Who{ClientID: "c", Software: "sw", Host: "h", Principal: "p"})
	s.Touch(id(1), Who{})
	in := s.Instances()[0]
	if in.ClientID != "c" || in.Software != "sw" || in.Host != "h" || in.Principal != "p" {
		t.Errorf("instance = %+v", in)
	}
	s.Touch(id(1), Who{Software: "sw2", Host: "h2"})
	in = s.Instances()[0]
	if in.Software != "sw2" || in.Host != "h2" {
		t.Errorf("instance = %+v", in)
	}
}

// An instance is held for exactly its TTL after it was last heard from; a terminated
// one for the shorter.
func TestInstancesAreHeldForExactlyTheirTTL(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	s.Touch(id(1), Who{})
	s.Push(id(2), Who{}, Payload{}, true)
	now = t0.Add(terminatedTTL)
	s.Instances() // prunes
	if !s.Known(id(1)) || !s.Known(id(2)) {
		t.Error("an instance was forgotten at exactly its TTL")
	}
	now = t0.Add(terminatedTTL + time.Nanosecond)
	s.Touch(id(3), Who{})
	if s.Known(id(2)) || !s.Known(id(1)) {
		t.Errorf("terminated known %v, silent known %v", s.Known(id(2)), s.Known(id(1)))
	}
	now = t0.Add(instanceTTL)
	s.Instances()
	if !s.Known(id(1)) {
		t.Error("a silent instance was forgotten at exactly its TTL")
	}
	now = t0.Add(instanceTTL + time.Nanosecond)
	s.Instances()
	if s.Known(id(1)) {
		t.Error("a silent instance outlived its TTL")
	}
}

// When the store is full, among instances of one kind the longest silent goes.
func TestTheLongestSilentOfOneKindIsEvictedFirst(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	open := func() bool { return true }
	for i := 0; i < maxInstances; i++ {
		now = t0.Add(time.Duration(i) * time.Second)
		s.Touch(id2(i+1), Who{Open: open})
	}
	now = t0.Add(time.Duration(maxInstances) * time.Second)
	s.Touch(id2(maxInstances+1), Who{Open: open})
	if s.Known(id2(1)) {
		t.Error("the longest silent was kept")
	}
	for i := 2; i <= maxInstances+1; i++ {
		if !s.Known(id2(i)) {
			t.Fatalf("instance %d was evicted instead", i)
		}
	}
}

// Series come out ordered by name, then by attributes, whatever order they arrived in.
func TestSeriesAreOrderedByNameThenAttributes(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0 }
	pt := func(a string) []Point { return []Point{{Attrs: a, Time: t0, Value: 1}} }
	s.Push(id(1), Who{}, Payload{Metrics: []Metric{
		{Name: "b", Kind: Gauge, Points: pt("y=1")},
		{Name: "a", Kind: Gauge, Points: append(pt("z=1"), pt("x=1")...)},
		{Name: "b", Kind: Gauge, Points: pt("x=1")},
	}}, false)
	var got []string
	for _, se := range s.Instances()[0].Series {
		got = append(got, se.Name+"/"+se.Attrs)
	}
	want := []string{"a/x=1", "a/z=1", "b/x=1", "b/y=1"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("order = %v, want %v", got, want)
	}
}

// A series keeps exactly samplesKept points, with their own times, the oldest going first.
func TestASeriesKeepsItsLastSamplesWithTheirTimes(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0.Add(40 * time.Second) }
	push := func(i int) {
		s.Push(id(1), Who{}, gauge("m", t0.Add(time.Duration(i)*1100*time.Millisecond), float64(i)), false)
	}
	for i := 0; i < samplesKept; i++ {
		push(i)
	}
	sm := s.Instances()[0].Series[0].Samples
	if len(sm) != samplesKept || sm[0].V != 0 {
		t.Fatalf("after %d points: %d samples, first %v", samplesKept, len(sm), sm[0])
	}
	for i := samplesKept; i < samplesKept+5; i++ {
		push(i)
	}
	sm = s.Instances()[0].Series[0].Samples
	if len(sm) != samplesKept {
		t.Fatalf("%d samples, want %d", len(sm), samplesKept)
	}
	for k, x := range sm {
		i := k + 5
		if x.V != float64(i) || !x.T.Equal(t0.Add(time.Duration(i)*1100*time.Millisecond)) {
			t.Fatalf("sample %d = %+v, want point %d", k, x, i)
		}
	}
}

// The worst case the budget is held to, from the documented bounds.
func TestWorstCaseBytesFollowsTheBounds(t *testing.T) {
	perSample := 4 + 8
	perSeries := 128 + 2*256 + 32
	perInstance := 32*(128+1024) + 1024
	want := 100 * (150*(perSeries+60*perSample) + perInstance)
	if got := WorstCaseBytes(); got != want || got != 24668800 {
		t.Errorf("WorstCaseBytes = %d, want %d", got, want)
	}
}

// clipText at its edges: a string exactly n long is whole, and a cut at 0 on a
// continuation byte is empty.
func TestClipTextEdges(t *testing.T) {
	if got := clipText("abcde", 5); got != "abcde" {
		t.Errorf("got %q", got)
	}
	if got := clipText("\x80\x80", 0); got != "" {
		t.Errorf("got %q", got)
	}
}
