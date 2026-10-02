package broker

import (
	"testing"
	"time"
)

func ready(s *subscription) bool {
	select {
	case <-s.C:
		return true
	case <-time.After(30 * time.Millisecond):
		return false
	}
}

func TestNotifierIsPerTopic(t *testing.T) {
	n := newNotifier()
	a := n.subscribe([]string{"a"})
	ab := n.subscribe([]string{"a", "b"})
	b := n.subscribe([]string{"b"})
	defer a.Close()
	defer ab.Close()
	defer b.Close()

	n.signal("a")
	if !ready(a) || !ready(ab) {
		t.Fatal("a produce to a must wake the fetches reading a")
	}
	if ready(b) {
		t.Fatal("a produce to a woke a fetch that reads only b")
	}

	n.signal("b")
	if !ready(b) || !ready(ab) {
		t.Fatal("a produce to b must wake the fetches reading b")
	}
	if ready(a) {
		t.Fatal("a produce to b woke a fetch that reads only a")
	}
}

// A signal that arrives between a fetch's read and its wait must not be lost: it
// is buffered in the subscription, which exists before the read.
func TestNotifierDoesNotLoseASignalSentBeforeTheWait(t *testing.T) {
	n := newNotifier()
	s := n.subscribe([]string{"t"})
	defer s.Close()
	n.signal("t") // lands "between the read and the wait".
	n.signal("t") // and a second one must not block or pile up.
	if !ready(s) {
		t.Fatal("the signal sent before the wait was lost")
	}
	if ready(s) {
		t.Fatal("two signals should coalesce into one wakeup")
	}
}

func TestNotifierCloseUnsubscribes(t *testing.T) {
	n := newNotifier()
	s := n.subscribe([]string{"t"})
	s.Close()
	n.signal("t")
	if ready(s) {
		t.Fatal("a closed subscription still received a signal")
	}
	if len(n.subs) != 0 {
		t.Fatalf("the notifier retains %d topics after every subscriber left", len(n.subs))
	}
}
