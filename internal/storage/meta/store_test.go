package meta

import "testing"

func openStore(t *testing.T) Store {
	t.Helper()
	s, err := Open(t.TempDir() + "/meta.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// Deleting a topic must take its committed offsets with it, atomically, and
// leave every other topic's offsets alone — including a topic whose name merely
// shares a prefix or suffix with the deleted one.
func TestDeleteTopicRemovesItsOffsetsOnly(t *testing.T) {
	s := openStore(t)
	for _, name := range []string{"a", "ab", "ba", "a.b"} {
		if _, err := s.CreateTopic(name, 2, nil); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(group, topic string, partition int32, off int64) {
		t.Helper()
		if err := s.CommitOffset(group, topic, partition, off, ""); err != nil {
			t.Fatal(err)
		}
	}
	commit("g1", "a", 0, 10)
	commit("g1", "a", 1, 11)
	commit("g2", "a", 0, 12)
	commit("g1", "ab", 0, 20)
	commit("g1", "ba", 0, 30)
	commit("g1", "a.b", 0, 40)
	commit("a", "ab", 0, 50) // a group NAMED like the deleted topic must survive.

	if err := s.DeleteTopic("a"); err != nil {
		t.Fatal(err)
	}

	if _, ok, _ := s.FetchOffset("g1", "a", 0); ok {
		t.Error("g1's offset on the deleted topic survived")
	}
	if _, ok, _ := s.FetchOffset("g2", "a", 0); ok {
		t.Error("g2's offset on the deleted topic survived")
	}
	for _, c := range []struct {
		group, topic string
		part         int32
		want         int64
	}{{"g1", "ab", 0, 20}, {"g1", "ba", 0, 30}, {"g1", "a.b", 0, 40}, {"a", "ab", 0, 50}} {
		got, ok, err := s.FetchOffset(c.group, c.topic, c.part)
		if err != nil || !ok || got != c.want {
			t.Errorf("FetchOffset(%s,%s,%d) = %d,%v,%v; an unrelated offset was disturbed (want %d)", c.group, c.topic, c.part, got, ok, err, c.want)
		}
	}

	// Recreating the name starts clean.
	if _, err := s.CreateTopic("a", 2, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.FetchOffset("g1", "a", 0); ok {
		t.Error("a recreated topic inherited the old topic's offsets")
	}
}

func TestGroupOffsetHelpers(t *testing.T) {
	s := openStore(t)
	for _, c := range []struct {
		g, topic string
		p        int32
	}{{"g1", "a", 0}, {"g1", "a", 1}, {"g1", "b", 0}, {"g2", "a", 0}, {"g10", "a", 0}} {
		if err := s.CommitOffset(c.g, c.topic, c.p, 7, ""); err != nil {
			t.Fatal(err)
		}
	}
	groups, err := s.OffsetGroups()
	if err != nil || len(groups) != 3 || groups[0] != "g1" || groups[1] != "g10" || groups[2] != "g2" {
		t.Fatalf("OffsetGroups = %v, %v; want [g1 g10 g2] each once", groups, err)
	}
	// "g1" must not match "g10": the group id is delimited, not a prefix.
	n, err := s.DeleteGroupOffsets("g1")
	if err != nil || n != 3 {
		t.Fatalf("DeleteGroupOffsets(g1) = %d, %v; want 3", n, err)
	}
	if es, _ := s.ListOffsets("g10"); len(es) != 1 {
		t.Errorf("deleting g1 disturbed g10: %v", es)
	}
	if err := s.DeleteOffset("g2", "a", 0); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteOffset("g2", "a", 0); err != nil { // already gone: a no-op.
		t.Fatalf("deleting a missing offset should be a no-op: %v", err)
	}
	if groups, _ := s.OffsetGroups(); len(groups) != 1 || groups[0] != "g10" {
		t.Errorf("OffsetGroups after deletes = %v, want [g10]", groups)
	}
}
