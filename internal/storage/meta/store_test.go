package meta

import (
	"errors"
	"strings"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

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
	mustTopics(t, s, "a", "b")
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

// Two brokers on one metadata file used to block forever in silence (bbolt waits
// for the lock with no timeout). It must be an error that says why.
func TestSecondOpenOfTheSameFileFailsWithAReason(t *testing.T) {
	old := lockTimeout
	lockTimeout = 200 * time.Millisecond
	defer func() { lockTimeout = old }()

	path := t.TempDir() + "/meta.db"
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	start := time.Now()
	_, err = Open(path)
	if err == nil {
		t.Fatal("a second Open of a locked metadata file should fail")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("it took %v to fail; it must not wait indefinitely", took)
	}
	if !strings.Contains(err.Error(), "another doze-kafka") || !strings.Contains(err.Error(), path) {
		t.Fatalf("the error should name the file and the likely cause, got: %v", err)
	}
}

func TestCommitOffsetsIsOneAtomicWrite(t *testing.T) {
	s := openStore(t)
	mustTopics(t, s, "a", "b")
	writes := func() int64 { return s.(*boltStore).writeTxns.Load() }
	before := writes()
	defer func() {
		// One request, one transaction (one fsync): that is the whole point.
		if n := writes() - before; n != 1 {
			t.Errorf("CommitOffsets used %d write transactions for 3 offsets, want 1", n)
		}
	}()
	err := s.CommitOffsets("g", []OffsetCommit{
		{Topic: "a", Partition: 0, Offset: 1}, {Topic: "a", Partition: 1, Offset: 2, Metadata: "m"}, {Topic: "b", Partition: 0, Offset: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	es, _ := s.ListOffsets("g")
	if len(es) != 3 {
		t.Fatalf("ListOffsets = %v, want 3 entries", es)
	}
	if off, ok, _ := s.FetchOffset("g", "a", 1); !ok || off != 2 {
		t.Errorf("a/1 = %d,%v want 2", off, ok)
	}
	if es, _ := s.ListOffsets("g"); es[1].Metadata != "m" {
		t.Errorf("metadata lost: %+v", es[1])
	}
	if err := s.CommitOffsets("g", nil); err != nil {
		t.Errorf("an empty commit should be a no-op: %v", err)
	}
}

// Ids must never be reissued: not within a process, and not after a restart that
// skipped the unused rest of a block.
func TestReserveProducerIDsNeverReissues(t *testing.T) {
	path := t.TempDir() + "/meta.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := s.ReserveProducerIDs(1000)
	b, _ := s.ReserveProducerIDs(1000)
	if b != a+1000 {
		t.Fatalf("blocks %d and %d should be adjacent and disjoint", a, b)
	}
	s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	c, _ := s2.NextProducerID()
	if c < b+1000 {
		t.Fatalf("after a restart the next id %d overlaps the reserved range ending %d", c, b+1000)
	}
}

// The registry is served from memory, so it must track every change and reload
// from disk, and a caller must not be able to corrupt it through what it is handed.
func TestTopicCacheIsCoherent(t *testing.T) {
	path := t.TempDir() + "/meta.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	created, err := s.CreateTopic("orders", 3, map[string]string{"retention.ms": "1000"})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s.Topic("orders"); !ok || got.ID != created.ID || got.Partitions != 3 {
		t.Fatalf("Topic after create: %+v %v", got, ok)
	}
	if got, ok, _ := s.TopicByID(created.ID); !ok || got.Name != "orders" {
		t.Fatalf("TopicByID after create: %+v %v", got, ok)
	}

	got, _, _ := s.Topic("orders")
	got.Config["retention.ms"] = "tampered"
	got.Partitions = 99
	if again, _, _ := s.Topic("orders"); again.Config["retention.ms"] != "1000" || again.Partitions != 3 {
		t.Fatalf("a caller changed the cached topic: %+v", again)
	}

	s.CreateTopic("a", 1, nil)
	s.CreateTopic("z", 1, nil)
	list, _ := s.ListTopics()
	if len(list) != 3 || list[0].Name != "a" || list[1].Name != "orders" || list[2].Name != "z" {
		t.Fatalf("ListTopics should be sorted by name: %+v", list)
	}
	s.Close()

	s2, err := Open(path) // a restart reloads the registry from disk.
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got, ok, _ := s2.Topic("orders"); !ok || got.ID != created.ID || got.Config["retention.ms"] != "1000" {
		t.Fatalf("Topic after reopen: %+v %v", got, ok)
	}
	if err := s2.DeleteTopic("orders"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s2.Topic("orders"); ok {
		t.Error("a deleted topic is still served from the cache")
	}
	if _, ok, _ := s2.TopicByID(created.ID); ok {
		t.Error("a deleted topic is still served by id")
	}
}

func mustTopics(t *testing.T, s Store, names ...string) {
	t.Helper()
	for _, n := range names {
		if _, err := s.CreateTopic(n, 2, nil); err != nil {
			t.Fatal(err)
		}
	}
}

// "Orders" and "orders" are one directory on a case-insensitive filesystem, so two
// such topics would share a log. The second is refused, everywhere.
func TestTopicNamesDifferingOnlyInCaseAreRefused(t *testing.T) {
	s := openStore(t)
	mustTopics(t, s, "Orders")
	_, err := s.CreateTopic("orders", 1, nil)
	if !errors.Is(err, ErrTopicCollides) {
		t.Fatalf("CreateTopic(orders) with Orders present = %v, want ErrTopicCollides", err)
	}
	if _, err := s.CreateTopic("Orders", 1, nil); !errors.Is(err, ErrTopicExists) {
		t.Errorf("the same name again = %v, want ErrTopicExists", err)
	}
	// Once the first is gone the other spelling is free.
	if err := s.DeleteTopic("Orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateTopic("orders", 1, nil); err != nil {
		t.Errorf("after deleting Orders, orders should be creatable: %v", err)
	}
}

// The cache and the database must agree whatever order a concurrent create and
// delete of one name commit in. They used to be updated in separate critical
// sections: a delete could commit and update the cache between a create's commit
// and its cache update, leaving a topic the cache had and the database did not.
func TestCreateAndDeleteCannotInterleaveBetweenCommitAndCache(t *testing.T) {
	s := openStore(t)
	bs := s.(*boltStore)
	committed, release := make(chan struct{}), make(chan struct{})
	bs.afterCreateCommit = func() { close(committed); <-release }

	created := make(chan struct{})
	go func() { defer close(created); s.CreateTopic("t", 1, nil) }()
	<-committed // the create is committed; its cache update has not happened yet.

	deleted := make(chan struct{})
	go func() { defer close(deleted); s.DeleteTopic("t") }()
	select {
	case <-deleted:
		// The delete ran in the gap. Let the create finish and see what is left.
	case <-time.After(300 * time.Millisecond):
		// The delete is waiting for the create, as it should.
	}
	close(release)
	<-created
	<-deleted

	_, cached, _ := s.Topic("t")
	var stored bool
	bs.db.View(func(tx *bbolt.Tx) error { stored = tx.Bucket(bucketTopics).Get([]byte("t")) != nil; return nil })
	if cached != stored {
		t.Fatalf("the cache says exists=%v, the database says exists=%v", cached, stored)
	}
}

// NUL separates group, topic and partition in an offset key, so a group id with
// one in it would address another group's offsets.
func TestGroupIDWithNULIsRefused(t *testing.T) {
	s := openStore(t)
	mustTopics(t, s, "orders")
	if err := s.CommitOffsets("g\x00x", []OffsetCommit{{Topic: "orders", Offset: 7}}); !errors.Is(err, ErrInvalidGroupID) {
		t.Fatalf("commit for a group id containing NUL = %v, want ErrInvalidGroupID", err)
	}
	if es, _ := s.ListOffsets("g"); len(es) != 0 {
		t.Errorf("group g sees offsets it never committed: %+v", es)
	}
}

// A commit that reaches the store after its topic was deleted must not be kept:
// the next topic of that name would start with a committed offset it never had.
func TestCommitForADeletedTopicIsNotKept(t *testing.T) {
	s := openStore(t)
	mustTopics(t, s, "orders")
	if err := s.DeleteTopic("orders"); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitOffsets("g", []OffsetCommit{{Topic: "orders", Offset: 500}}); err != nil {
		t.Fatal(err)
	}
	mustTopics(t, s, "orders")
	if off, ok, _ := s.FetchOffset("g", "orders", 0); ok {
		t.Errorf("the re-created topic inherited committed offset %d from the deleted one", off)
	}
}

// Deleting a topic removes its id mapping too, or the id would resolve to a name
// that no longer exists after a restart.
func TestDeleteTopicRemovesTheIDMapping(t *testing.T) {
	path := t.TempDir() + "/meta.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	topic, err := s.CreateTopic("gone", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteTopic("gone"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if s, err = Open(path); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var left bool
	s.(*boltStore).db.View(func(tx *bbolt.Tx) error { left = tx.Bucket(bucketTopicsByID).Get(topic.ID[:]) != nil; return nil })
	if left {
		t.Error("the topic id is still mapped after the topic was deleted")
	}
}

// A topic's configuration is replaced as a whole, is what the cache serves at
// once, is what a reopened store reads, and cannot be written for a topic that is
// not there.
func TestSetTopicConfigReplacesPersistsAndRefusesUnknownTopics(t *testing.T) {
	path := t.TempDir() + "/meta.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	created, err := s.CreateTopic("t", 3, map[string]string{"retention.ms": "1000", "segment.bytes": "4096"})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := s.SetTopicConfig("t", map[string]string{"retention.bytes": "2048"})
	if err != nil {
		t.Fatal(err)
	}
	if len(updated.Config) != 1 || updated.Config["retention.bytes"] != "2048" || updated.ID != created.ID || updated.Partitions != 3 {
		t.Errorf("after replacing the config: %+v (id and partitions must be untouched)", updated)
	}
	if got, _, _ := s.Topic("t"); len(got.Config) != 1 || got.Config["retention.ms"] != "" {
		t.Errorf("the cache serves %v, want only retention.bytes", got.Config)
	}
	// The topic owns its map: changing the caller's copy changes nothing.
	updated.Config["retention.bytes"] = "mutated"
	if got, _, _ := s.Topic("t"); got.Config["retention.bytes"] != "2048" {
		t.Error("a returned topic shares its config map with the store")
	}
	if cleared, err := s.SetTopicConfig("t", nil); err != nil || len(cleared.Config) != 0 {
		t.Errorf("clearing the config: %+v %v", cleared, err)
	}
	if _, err := s.SetTopicConfig("t", map[string]string{"flush.ms": "7"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetTopicConfig("missing", map[string]string{"a": "b"}); !errors.Is(err, ErrNoSuchTopic) {
		t.Errorf("config for a topic that does not exist: %v, want ErrNoSuchTopic", err)
	}
	s.Close()

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got, ok, _ := s.Topic("t"); !ok || len(got.Config) != 1 || got.Config["flush.ms"] != "7" || got.ID != created.ID {
		t.Errorf("after reopening: %+v", got)
	}
}
