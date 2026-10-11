package telemetry

import (
	"testing"
	"time"
)

// The clock a subscription request and a push share is the newer of the two, whichever
// came first and whichever the instance has had at all (recorded:
// conformance/recordings/telemetry-interval.json).
func TestSubscriptionAndPushShareOneClock(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	at := func(d time.Duration) { now = t0.Add(d) }
	w := Who{}

	// Only a subscription so far: asking again inside the interval is early.
	s.Subscribe(id(1), w)
	at(time.Second)
	if v := s.Subscribe(id(1), w); v != Early {
		t.Errorf("a second subscription request: %v, want Early", v)
	}
	// Only a push so far (an instance the broker was never asked about): the same.
	if v := s.Arrive(id(2), w, false); v != Accepted {
		t.Fatalf("the push of an instance never seen: %v", v)
	}
	if v := s.Subscribe(id(2), w); v != Early {
		t.Errorf("a subscription request right after the only push: %v, want Early", v)
	}
	// A subscription taken after the push is the newer of the two.
	at(6 * time.Second)
	if v := s.Subscribe(id(2), w); v != Accepted {
		t.Fatalf("a subscription request after the interval: %v", v)
	}
	at(7 * time.Second)
	if v := s.Subscribe(id(2), w); v != Early {
		t.Errorf("a subscription request 1 s after the last taken: %v, want Early", v)
	}
	at(12 * time.Second)
	if v := s.Subscribe(id(2), w); v != Accepted {
		t.Errorf("a subscription request 6 s after the last taken: %v, want Accepted", v)
	}
}
