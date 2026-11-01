package broker

import (
	"testing"
	"time"
)

// GRP-19: the time each offset expires is the one the sweep acts on.
func TestOffsetExpiryShowsWhenTheSweepWouldTakeEachOffset(t *testing.T) {
	t0 := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	r := newExpiryRig(t, t.TempDir(), t0, "a", "b")

	// A group that never had a member: each offset from its own commit.
	r.commit(t, "solo", "a", 5)
	r.advance(3 * time.Minute)
	r.commit(t, "solo", "b", 7)
	got := r.b.OffsetExpiry("solo")
	if got.Retention != testRetention || !got.EmptySince.IsZero() || len(got.Offsets) != 2 {
		t.Fatalf("solo = %+v", got)
	}
	byTopic := map[string]time.Time{}
	for _, o := range got.Offsets {
		byTopic[o.Topic] = o.At
	}
	if !byTopic["a"].Equal(t0.Add(testRetention)) || !byTopic["b"].Equal(t0.Add(3*time.Minute+testRetention)) {
		t.Errorf("expiry times = %v", byTopic)
	}

	// A group that had a member and lost it: all offsets from the moment it became empty,
	// however late the last commit.
	member, _ := r.join(t, "team", "a")
	r.commit(t, "team", "a", 1)
	r.advance(time.Minute)
	r.leave(t, "team", member)
	emptyAt := r.now
	r.advance(2 * time.Minute)
	r.commit(t, "team", "b", 2) // a commit into an empty group does not extend it.
	got = r.b.OffsetExpiry("team")
	if !got.EmptySince.Equal(emptyAt) || len(got.Offsets) != 2 {
		t.Fatalf("team = %+v", got)
	}
	for _, o := range got.Offsets {
		if !o.At.Equal(emptyAt.Add(testRetention)) {
			t.Errorf("%s expires %v, want %v", o.Topic, o.At, emptyAt.Add(testRetention))
		}
	}
	// And the sweep takes exactly what it said, when it said.
	r.advance(testRetention - 2*time.Minute - time.Millisecond)
	r.exp.sweep()
	if !r.has("team", "a") {
		t.Fatal("the offsets went before the time the view gave")
	}
	r.advance(2 * time.Millisecond)
	r.exp.sweep()
	if r.has("team", "a") || r.has("team", "b") {
		t.Fatal("the offsets are still held after the time the view gave")
	}

	// A group with a member keeps the topic it reads and loses, from its commit, one it does not.
	member, _ = r.join(t, "busy", "a")
	_ = member
	r.commit(t, "busy", "a", 1)
	r.commit(t, "busy", "b", 1)
	got = r.b.OffsetExpiry("busy")
	if len(got.Offsets) != 1 || got.Offsets[0].Topic != "b" {
		t.Errorf("busy = %+v, want only b to expire", got)
	}
	if none := r.b.OffsetExpiry("nobody"); len(none.Offsets) != 0 {
		t.Errorf("an unknown group = %+v", none)
	}
}

func TestOffsetExpiryIsEmptyWhenOffsetsAreKeptForEver(t *testing.T) {
	r := newExpiryRigRetaining(t, t.TempDir(), time.Now(), 0, "a")
	r.commit(t, "g", "a", 1)
	if got := r.b.OffsetExpiry("g"); got.Retention != 0 || len(got.Offsets) != 0 {
		t.Errorf("expiry with no retention = %+v", got)
	}
}
