package telemetry

import (
	"testing"
	"time"
)

// A push refused for its subscription id or its compression type tells the client to
// ask for its subscription again, and Apache Kafka answers that request although it
// comes straight after a push (recorded:
// conformance/recordings/telemetry-ask-again.json). It answers it once, and the
// request does not count as a subscription given: a push straight after it is early,
// and a second request straight after it is too. This store throttled the request.
func TestTheSubscriptionRequestAfterARefusedPushIsAnswered(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	w := Who{}
	s.Subscribe(id(1), w)
	now = now.Add(time.Millisecond)
	if v := s.Arrive(id(1), w, false); v != Accepted {
		t.Fatalf("the first push: %v", v)
	}
	s.AskAgain(id(1)) // the broker found its subscription id wrong.
	now = now.Add(time.Millisecond)
	if v := s.Subscribe(id(1), w); v != Accepted {
		t.Errorf("the subscription request after a push refused for its subscription id: %v, want Accepted", v)
	}
	now = now.Add(time.Millisecond)
	if v := s.Arrive(id(1), w, false); v != Early {
		t.Errorf("the push straight after that request: %v, want Early (the request set no clock)", v)
	}
	now = now.Add(time.Millisecond)
	if v := s.Subscribe(id(1), w); v != Early {
		t.Errorf("a request after the early push: %v, want Early", v)
	}

	// Once: the second request straight after is early.
	s.Subscribe(id(2), w)
	s.Arrive(id(2), w, false)
	s.AskAgain(id(2))
	if v := s.Subscribe(id(2), w); v != Accepted {
		t.Errorf("the first request: %v", v)
	}
	if v := s.Subscribe(id(2), w); v != Early {
		t.Errorf("the second request straight after: %v, want Early", v)
	}

	// A push that arrives after the refused one takes the permission away, whatever
	// becomes of it: here it is refused for following a terminating one.
	s.Subscribe(id(3), w)
	s.Arrive(id(3), w, true)
	s.AskAgain(id(3))
	if v := s.Arrive(id(3), w, false); v != AfterTerminating {
		t.Fatalf("the push after a terminating one: %v", v)
	}
	if v := s.Subscribe(id(3), w); v != Early {
		t.Errorf("a request after a push refused for another reason: %v, want Early", v)
	}

	// An instance the store does not hold is not made by it.
	s.AskAgain(id(9))
	if s.Known(id(9)) {
		t.Error("AskAgain made an instance")
	}
}
