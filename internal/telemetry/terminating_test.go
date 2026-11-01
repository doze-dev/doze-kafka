package telemetry

import (
	"testing"
	"time"
)

// What a broker remembers of a client that said it was closing, as Apache Kafka 4.3.1
// answers (recorded: conformance/recordings/telemetry-terminating.json).
//
// Kafka keeps one thing per instance: whether the last push it was sent said
// "terminating", whatever became of that push (taken, or refused for its subscription
// id, its compression, or for following a terminating one). While that is so the next
// push is INVALID_REQUEST, and the next push's own word replaces it. So a run of
// terminating pushes is refused from the second on, for as long as it runs; a refused
// terminating push still makes the next one INVALID_REQUEST; and an ordinary push
// after one is refused once.
//
// This store used to arm the refusal only when a terminating push was stored, and
// disarm it at the next push: the third terminating push in a row was taken, a
// terminating push that was refused (or on an instance never seen) armed nothing, and
// the push after it was told it was early.
func TestATerminatedInstanceStaysTerminatedWhileItIsToldSo(t *testing.T) {
	type push struct {
		terminating bool
		want        Verdict
		what        string
	}
	const T, P = true, false
	for name, script := range map[string][]push{
		"terminating pushes in a row": {
			{T, Accepted, "the first"},
			{T, AfterTerminating, "the second"},
			{T, AfterTerminating, "the third"},
			{T, AfterTerminating, "the fourth"},
			{P, AfterTerminating, "an ordinary push after them"},
			{P, Early, "the one after that: the refusal is spent, the interval is not"},
		},
		"an ordinary push between": {
			{P, Accepted, "a push"},
			{T, Accepted, "terminating at once: never early"},
			{P, AfterTerminating, "the push after"},
			{T, Accepted, "terminating again: the refused push was not terminating"},
			{T, AfterTerminating, "and once more"},
		},
	} {
		s := NewStore()
		s.now = func() time.Time { return t0 }
		if v := s.Subscribe(id(1), Who{}); v != Accepted {
			t.Fatalf("%s: subscribe: %v", name, v)
		}
		for _, p := range script {
			if got := s.Arrive(id(1), Who{}, p.terminating); got != p.want {
				t.Errorf("%s: %s: %v, want %v", name, p.what, got, p.want)
			}
		}
	}
}

// The broker refuses a push for other reasons after the gate has passed it (a wrong
// subscription id, an unsupported compression type), and then stores nothing. A
// terminating push refused that way has still said the client is closing, also on an
// instance the broker had never seen.
func TestARefusedTerminatingPushStillTerminates(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0 }
	// Never seen, never stored: the gate passes it and the broker refuses it.
	if v := s.Arrive(id(7), Who{}, true); v != Accepted {
		t.Fatalf("a terminating push for an instance never seen: %v", v)
	}
	if v := s.Arrive(id(7), Who{}, false); v != AfterTerminating {
		t.Errorf("the push after a refused terminating one: %v, want AfterTerminating", v)
	}
	// Subscribed, then a terminating push the broker refuses, then another.
	s.Subscribe(id(8), Who{})
	s.Arrive(id(8), Who{}, true)
	if v := s.Arrive(id(8), Who{}, true); v != AfterTerminating {
		t.Errorf("a terminating push after a refused terminating one: %v, want AfterTerminating", v)
	}
	// A subscription request does not clear it.
	s.Subscribe(id(9), Who{})
	s.Arrive(id(9), Who{}, true)
	s.Subscribe(id(9), Who{})
	if v := s.Arrive(id(9), Who{}, false); v != AfterTerminating {
		t.Errorf("the push after a terminating one and a subscription request: %v, want AfterTerminating", v)
	}
}

// A terminating push that comes inside the interval is taken, but it does not set the
// clock: the next ordinary push is early or not by the push before it.
func TestAnEarlyTerminatingPushDoesNotSetTheClock(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	s.Subscribe(id(1), Who{})
	if v := s.Arrive(id(1), Who{}, false); v != Accepted {
		t.Fatalf("the first push: %v", v)
	}
	now = t0.Add(3 * time.Second)
	if v := s.Arrive(id(1), Who{}, true); v != Accepted {
		t.Fatalf("a terminating push 3 s later: %v", v)
	}
	if v := s.Arrive(id(1), Who{}, false); v != AfterTerminating {
		t.Fatalf("the push after it: %v", v)
	}
	now = t0.Add(5500 * time.Millisecond)
	if v := s.Arrive(id(1), Who{}, false); v != Accepted {
		t.Errorf("a push 5.5 s after the last one that set the clock (2.5 s after the terminating one): %v, want Accepted", v)
	}
	// A terminating push that is not early does set it.
	now = t0.Add(11 * time.Second)
	s.Arrive(id(1), Who{}, true)
	s.Arrive(id(1), Who{}, false) // refused once.
	now = t0.Add(14 * time.Second)
	if v := s.Arrive(id(1), Who{}, false); v != Early {
		t.Errorf("a push 3 s after a terminating one that was on time: %v, want Early", v)
	}
}
