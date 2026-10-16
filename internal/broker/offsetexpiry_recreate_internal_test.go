package broker

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// What is left of "when this group became empty" once the group is gone and its id is
// used again: nothing. Recorded in conformance/recordings/offsets-retention-recreate.json.

// A deleted group's id, used by a standalone committer, is a group nobody ever joined:
// its offsets are as old as their commits (recorded: g-del). The moment the old group
// emptied used to stay in memory and expire the new commits early.
func TestOffsetRetentionADeletedGroupLeavesNothingBehind(t *testing.T) {
	for _, protocol := range []string{"classic", "consumer"} {
		t.Run(protocol, func(t *testing.T) {
			r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
			if protocol == "classic" {
				member, _ := r.join(t, "g", "a")
				r.commit(t, "g", "a", 5)
				r.leave(t, "g", member)
			} else {
				r.join848(t, "g", "n1", "a")
				r.commit(t, "g", "a", 5)
				r.hb848("g", "n1", -1, nil)
			}
			r.advance(testRetention - time.Minute)
			if code := r.b.deleteGroup("g"); code != 0 {
				t.Fatalf("delete: %d", code)
			}
			r.commit(t, "g", "a", 6)
			r.advance(2 * time.Minute)
			r.exp.sweep()
			if !r.has("g", "a") {
				t.Fatal("an offset committed two minutes ago expired: the deleted group's emptiness was still counted")
			}
			r.advance(testRetention)
			r.exp.sweep()
			if r.has("g", "a") {
				t.Fatal("the new offset was kept past its own retention")
			}
		})
	}
}

// The same on the far side of a restart: what the store has of a deleted group is
// nothing, whatever was waiting to be written when it was deleted.
func TestOffsetRetentionADeletedGroupIsNotWrittenBackToTheStore(t *testing.T) {
	dir := t.TempDir()
	r := newExpiryRig(t, dir, time.Now(), "a")
	member, _ := r.join(t, "g", "a")
	r.commit(t, "g", "a", 5)
	r.leave(t, "g", member) // that it emptied is waiting to be written.
	r.advance(testRetention - time.Minute)
	if code := r.b.deleteGroup("g"); code != 0 {
		t.Fatalf("delete: %d", code)
	}
	r.exp.flush()
	if known, _ := r.m.GroupLiveness(); len(known) != 0 {
		t.Fatalf("the store still has a record of the deleted group: %+v", known)
	}
	r.commit(t, "g", "a", 6)
	r.stopped()

	r2 := newExpiryRig(t, dir, r.now.Add(2*time.Minute), "a")
	r2.exp.sweep()
	if !r2.has("g", "a") {
		t.Fatal("after a restart the deleted group's emptiness expired a new commit")
	}
}

// OffsetDelete of an empty group's last offset leaves the group, empty since it was,
// until the next sweep finds it has nothing and drops it. A commit before that sweep is
// a commit into the old group (recorded: g-offdel); one after it starts a new group
// (recorded: g-offdel-late).
func TestOffsetRetentionAnEmptyGroupWithNoOffsetsIsDroppedAtTheNextSweep(t *testing.T) {
	emptied := func(t *testing.T) *expiryRig {
		r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
		member, _ := r.join(t, "g", "a")
		r.commit(t, "g", "a", 5)
		r.leave(t, "g", member)
		r.advance(testRetention - time.Minute)
		if err := r.m.DeleteOffset("g", "a", 0); err != nil {
			t.Fatal(err)
		}
		return r
	}
	t.Run("a commit before the sweep", func(t *testing.T) {
		r := emptied(t)
		r.commit(t, "g", "a", 6)
		r.advance(2 * time.Minute)
		r.exp.sweep()
		if r.has("g", "a") {
			t.Fatal("a commit into the empty group, before any sweep, extended it")
		}
	})
	t.Run("a commit after the sweep", func(t *testing.T) {
		r := emptied(t)
		r.exp.sweep()
		for _, g := range r.b.listAllGroups() {
			if g.id == "g" {
				t.Errorf("the empty group with no offsets is still listed after a sweep: %+v", g)
			}
		}
		r.commit(t, "g", "a", 6)
		r.advance(2 * time.Minute)
		r.exp.sweep()
		if !r.has("g", "a") {
			t.Fatal("an offset committed two minutes ago expired: the dropped group's emptiness was still counted")
		}
	})
}

// A group whose only member left without committing has nothing to keep it: once a sweep
// has dropped it, a commit by hand under its id is a standalone commit.
func TestOffsetRetentionAGroupThatEmptiedWithNoOffsetsLeavesNothingBehind(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	member, _ := r.join(t, "g", "a")
	r.leave(t, "g", member)
	r.advance(testRetention - time.Minute)
	r.exp.sweep()
	r.commit(t, "g", "a", 6)
	r.advance(2 * time.Minute)
	r.exp.sweep()
	if !r.has("g", "a") {
		t.Fatal("an offset committed two minutes ago expired: the forgotten group's emptiness was still counted")
	}
}

// A classic group that emptied, joined and left again by a consumer-protocol member, is
// a consumer group with no members: the classic group's emptiness is not what its
// offsets are judged by any more.
func TestOffsetRetentionAnEmptyGroupThatChangedProtocolForgetsWhenItEmptied(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	member, _ := r.join(t, "g", "a")
	r.leave(t, "g", member)
	r.advance(testRetention - time.Minute)
	r.join848(t, "g", "n1", "a")
	r.commit(t, "g", "a", 6)
	r.hb848("g", "n1", -1, nil)
	r.advance(2 * time.Minute)
	r.exp.sweep()
	if !r.has("g", "a") {
		t.Fatal("the offset of an emptied consumer group was judged by when the classic group before it emptied")
	}
}

// failingDeletes is a store whose DeleteGroupOffsets fails.
type failingDeletes struct{ meta.Store }

func (failingDeletes) DeleteGroupOffsets(string) (int, error) { return 0, errStoreDown }

// A DeleteGroups that could not remove the offsets has deleted nothing: the group is
// still the group that emptied when it did, not one nobody ever joined.
func TestOffsetRetentionAFailedDeleteKeepsWhenTheGroupEmptied(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	member, _ := r.join(t, "g", "a")
	r.commit(t, "g", "a", 5)
	r.leave(t, "g", member)
	emptied := r.now

	r.advance(testRetention - time.Minute)
	healthy := r.b.meta
	r.b.meta = failingDeletes{healthy}
	if code := r.b.deleteGroup("g"); code == 0 {
		t.Fatal("the delete succeeded with the store refusing it")
	}
	r.b.meta = healthy
	r.exp.mu.Lock()
	st := r.exp.groups["g"]
	r.exp.mu.Unlock()
	if st.EmptySince != emptied.UnixMilli() {
		t.Fatalf("after the failed delete the group is recorded as %+v, want empty since %d", st, emptied.UnixMilli())
	}
	r.commit(t, "g", "a", 6) // a commit into the empty group does not extend it.
	r.advance(2 * time.Minute)
	r.exp.sweep()
	if r.has("g", "a") {
		t.Fatal("a failed delete gave the group's offsets a new retention")
	}
}

// listed reports whether the group is in what ListGroups would answer.
func (r *expiryRig) listed(group string) bool {
	for _, g := range r.b.listAllGroups() {
		if g.id == group {
			return true
		}
	}
	return false
}

// A group with no members and no offsets is gone at the next sweep whichever protocol it
// was (recorded: g-none and g-c-none are not there 45 s after their members left), and
// until that sweep it is listed, Empty.
func TestAGroupWithNoMembersAndNoOffsetsIsGoneAtTheNextSweep(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	member, _ := r.join(t, "classic", "a")
	r.leave(t, "classic", member)
	r.join848(t, "consumer", "n1", "a")
	r.hb848("consumer", "n1", -1, nil)
	for _, g := range []string{"classic", "consumer"} {
		if !r.listed(g) {
			t.Errorf("%s: not listed after its member left and before any sweep", g)
		}
	}
	r.exp.sweep()
	for _, g := range []string{"classic", "consumer"} {
		if r.listed(g) {
			t.Errorf("%s: still listed after a sweep, with no members and no offsets", g)
		}
	}
	if n := len(r.b.groups.list()) + len(r.b.cg848.list()); n != 0 {
		t.Errorf("%d group objects are left in the coordinators", n)
	}
}

// The same for a group the coordinator still holds and the record has lost, and for one
// only the record knows (after a restart nothing else does).
func TestTheSweepDropsWhatHoldsNothingWhoeverKnowsOfIt(t *testing.T) {
	dir := t.TempDir()
	r := newExpiryRig(t, dir, time.Now(), "a")
	member, _ := r.join(t, "object-only", "a")
	r.leave(t, "object-only", member)
	r.exp.mu.Lock()
	delete(r.exp.groups, "object-only")
	r.exp.mu.Unlock()
	r.exp.sweep()
	if r.listed("object-only") {
		t.Error("a classic group with no members, no offsets and no record is still listed after a sweep")
	}

	member, _ = r.join(t, "record-only", "a")
	r.commit(t, "record-only", "a", 5)
	r.leave(t, "record-only", member)
	r.advance(testRetention - time.Minute)
	r.stopped()
	r2 := newExpiryRig(t, dir, r.now.Add(time.Second), "a")
	if err := r2.m.DeleteOffset("record-only", "a", 0); err != nil {
		t.Fatal(err)
	}
	r2.exp.sweep()
	r2.commit(t, "record-only", "a", 6)
	r2.advance(2 * time.Minute)
	r2.exp.sweep()
	if !r2.has("record-only", "a") {
		t.Fatal("after a restart, the emptiness of a group that then held nothing was still counted against a new commit")
	}
}
