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

// When a group became empty is kept across a restart, because offsets that outlive
// their members are expired from that moment and the broker forgets its members when
// it stops.
func TestGroupEmptySinceSurvivesAReopen(t *testing.T) {
	path := t.TempDir() + "/meta.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetGroupEmptySince("g1", 1000); err != nil {
		t.Fatal(err)
	}
	if err := s.SetGroupEmptySince("g2", 2000); err != nil {
		t.Fatal(err)
	}
	if err := s.SetGroupEmptySince("g2", 0); err != nil { // cleared: a member joined.
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.GroupsEmptySince()
	if err != nil || len(got) != 1 || got["g1"] != 1000 {
		t.Errorf("%v %v", got, err)
	}
	// Deleting a group's offsets forgets when it was empty too: nothing is left to expire.
	if _, err := s.DeleteGroupOffsets("g1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GroupsEmptySince(); len(got) != 0 {
		t.Errorf("the empty-since of a group with no offsets was kept: %v", got)
	}
}
