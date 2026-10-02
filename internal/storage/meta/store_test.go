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
