package meta

import (
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

// Offsets carry the time they were committed, so a retention can be applied to them
// (GRP-19). The clock is the store's own, replaceable for a test.
func TestOffsetsRecordWhenTheyWereCommitted(t *testing.T) {
	s := openStore(t).(*boltStore)
	now := time.UnixMilli(1_700_000_000_000)
	s.clock = func() time.Time { return now }
	if _, err := s.CreateTopic("t", 2, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitOffset("g", "t", 0, 5, ""); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	if err := s.CommitOffset("g", "t", 1, 6, "m"); err != nil {
		t.Fatal(err)
	}
	entries, err := s.ListOffsets("g")
	if err != nil || len(entries) != 2 {
		t.Fatalf("%v %v", entries, err)
	}
	if entries[0].CommittedAt != 1_700_000_000_000 || entries[1].CommittedAt != 1_700_000_000_000+3_600_000 {
		t.Errorf("commit times %d, %d", entries[0].CommittedAt, entries[1].CommittedAt)
	}
	// A commit again is a new time, with the offset it carries.
	now = now.Add(time.Hour)
	if err := s.CommitOffset("g", "t", 0, 9, ""); err != nil {
		t.Fatal(err)
	}
	entries, _ = s.ListOffsets("g")
	if entries[0].Offset != 9 || entries[0].CommittedAt != now.UnixMilli() {
		t.Errorf("after the second commit: %+v", entries[0])
	}
}

// ExpireOffsets deletes what its predicate says, in one transaction with the commits it
// races with, and says how many. An offset written before commit times were kept (no
// time) starts its clock at the sweep that first sees it instead of being expired at
// once.
func TestExpireOffsets(t *testing.T) {
	s := openStore(t).(*boltStore)
	now := time.UnixMilli(1_700_000_000_000)
	s.clock = func() time.Time { return now }
	if _, err := s.CreateTopic("a", 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateTopic("b", 1, nil); err != nil {
		t.Fatal(err)
	}
	s.CommitOffset("g", "a", 0, 1, "")
	now = now.Add(2 * time.Hour)
	s.CommitOffset("g", "b", 0, 2, "")
	s.CommitOffset("other", "a", 0, 3, "")

	cutoff := now.Add(-time.Hour).UnixMilli()
	n, err := s.ExpireOffsets("g", func(topic string, partition int32, at int64) bool { return at <= cutoff })
	if err != nil || n != 1 {
		t.Fatalf("expired %d, %v", n, err)
	}
	if _, ok, _ := s.FetchOffset("g", "a", 0); ok {
		t.Error("the old offset survived")
	}
	if v, ok, _ := s.FetchOffset("g", "b", 0); !ok || v != 2 {
		t.Error("the new offset was removed")
	}
	if v, ok, _ := s.FetchOffset("other", "a", 0); !ok || v != 3 {
		t.Error("another group's offset was removed")
	}

	// A record from a store written before commit times: its time is stamped, not 0.
	s.update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketOffsets).Put(offsetKey("old", "a", 0), []byte(`{"offset":7}`))
	})
	var seen int64 = -1
	if n, err := s.ExpireOffsets("old", func(_ string, _ int32, at int64) bool { seen = at; return false }); err != nil || n != 0 {
		t.Fatalf("%d %v", n, err)
	}
	if seen != now.UnixMilli() {
		t.Errorf("the predicate saw time %d for an offset with none, want the sweep's %d", seen, now.UnixMilli())
	}
	entries, _ := s.ListOffsets("old")
	if len(entries) != 1 || entries[0].CommittedAt != now.UnixMilli() {
		t.Errorf("the stamp was not kept: %+v", entries)
	}
}

// What is kept about a group's members survives a reopen: when it became empty, or that
// it has members and their session, because the broker forgets the members themselves
// when it stops and the offsets retention must not take that for a group nobody joined.
func TestGroupLivenessSurvivesAReopen(t *testing.T) {
	path := t.TempDir() + "/meta.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	set := func(group string, st *GroupLiveness) {
		t.Helper()
		if err := s.SetGroupLiveness(group, st); err != nil {
			t.Fatal(err)
		}
	}
	set("g1", &GroupLiveness{EmptySince: 1000})
	set("g2", &GroupLiveness{EmptySince: 2000})
	set("g2", nil) // forgotten.
	set("g3", &GroupLiveness{Live: true, Classic: true, Session: 45 * time.Second})
	set("g4", &GroupLiveness{Live: true, Session: 6 * time.Second})
	set("g5", &GroupLiveness{}) // nothing to keep is nothing kept.
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.GroupLiveness()
	want := map[string]GroupLiveness{
		"g1": {EmptySince: 1000},
		"g3": {Live: true, Classic: true, Session: 45 * time.Second},
		"g4": {Live: true, Session: 6 * time.Second},
	}
	if err != nil || len(got) != len(want) {
		t.Fatalf("%v %v", got, err)
	}
	for g, w := range want {
		if got[g] != w {
			t.Errorf("%s: %+v, want %+v", g, got[g], w)
		}
	}
	// Deleting a group's offsets forgets it too: nothing is left to expire.
	for _, g := range []string{"g1", "g3"} {
		if _, err := s.DeleteGroupOffsets(g); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := s.GroupLiveness(); len(got) != 1 || !got["g4"].Live {
		t.Errorf("after deleting two groups: %v", got)
	}
}

// A store written before groups with members were recorded: every group with offsets
// and no record is taken, once, to have had members, because which of them had is not
// known. A group that is recorded keeps its record, and a second open adopts nothing.
func TestAdoptGroupsWithOffsetsRunsOnce(t *testing.T) {
	s := openStore(t)
	if _, err := s.CreateTopic("t", 1, nil); err != nil {
		t.Fatal(err)
	}
	for _, g := range []string{"old-live", "old-empty"} {
		if err := s.CommitOffset(g, "t", 0, 1, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetGroupLiveness("old-empty", &GroupLiveness{EmptySince: 5}); err != nil {
		t.Fatal(err)
	}
	n, err := s.AdoptGroupsWithOffsets(45 * time.Second)
	if err != nil || n != 1 {
		t.Fatalf("adopted %d, %v", n, err)
	}
	got, _ := s.GroupLiveness()
	if st := got["old-live"]; !st.Live || !st.Classic || st.Session != 45*time.Second {
		t.Errorf("the group with no record: %+v", st)
	}
	if st := got["old-empty"]; st.Live || st.EmptySince != 5 {
		t.Errorf("the group with a record was changed: %+v", st)
	}
	// From now on a group with offsets and no record is one nobody joined.
	if err := s.CommitOffset("standalone", "t", 0, 1, ""); err != nil {
		t.Fatal(err)
	}
	if n, err := s.AdoptGroupsWithOffsets(45 * time.Second); err != nil || n != 0 {
		t.Fatalf("a second run adopted %d, %v", n, err)
	}
	if got, _ := s.GroupLiveness(); len(got) != 2 {
		t.Errorf("after the second run: %v", got)
	}
}

// ValidateGroupID accepts "", so a group "" with committed offsets is adopted like any other.
func TestAdoptGroupsWithOffsetsAdoptsTheEmptyGroupID(t *testing.T) {
	s := openStore(t)
	if _, err := s.CreateTopic("t", 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitOffset("", "t", 0, 1, ""); err != nil {
		t.Fatal(err)
	}
	if n, err := s.AdoptGroupsWithOffsets(45 * time.Second); err != nil || n != 1 {
		t.Fatalf("adopted %d, %v", n, err)
	}
	if got, _ := s.GroupLiveness(); !got[""].Live {
		t.Errorf("group \"\" was not adopted: %v", got)
	}
}
