package meta

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

// Every character the topic-name rule names is accepted at both ends of its
// range, and the characters just outside each range are refused.
func TestTopicNameRangesAreInclusiveAtBothEnds(t *testing.T) {
	for _, ok := range []string{"a", "z", "A", "Z", "0", "9", "a.b_c-d"} {
		if err := ValidateTopicName(ok); err != nil {
			t.Errorf("ValidateTopicName(%q) = %v, want accepted", ok, err)
		}
	}
	// '`' is just below 'a', '{' just above 'z', '@' below 'A', '[' above 'Z',
	// '/' below '0', ':' above '9'.
	for _, bad := range []string{"`", "{", "@", "[", "/", ":", "a b", "a\x00"} {
		if err := ValidateTopicName(bad); !errors.Is(err, ErrInvalidTopicName) {
			t.Errorf("ValidateTopicName(%q) = %v, want ErrInvalidTopicName", bad, err)
		}
	}
	if err := ValidateTopicName(strings.Repeat("a", MaxTopicNameLen)); err != nil {
		t.Errorf("a name of exactly the maximum length: %v", err)
	}
	if err := ValidateTopicName(strings.Repeat("a", MaxTopicNameLen+1)); !errors.Is(err, ErrInvalidTopicName) {
		t.Errorf("a name one past the maximum length: %v", err)
	}
}

// A group id is refused for a NUL anywhere, the first byte included, and for
// being past MaxGroupIDBytes, but not at exactly that many.
func TestGroupIDBounds(t *testing.T) {
	for _, bad := range []string{"\x00", "\x00g", "g\x00", "g\x00h"} {
		if err := ValidateGroupID(bad); !errors.Is(err, ErrInvalidGroupID) {
			t.Errorf("ValidateGroupID(%q) = %v", bad, err)
		}
	}
	if err := ValidateGroupID(strings.Repeat("g", MaxGroupIDBytes)); err != nil {
		t.Errorf("a group id of exactly %d bytes: %v", MaxGroupIDBytes, err)
	}
	if err := ValidateGroupID(strings.Repeat("g", MaxGroupIDBytes+1)); !errors.Is(err, ErrInvalidGroupID) {
		t.Errorf("a group id one byte too long: %v", err)
	}
}

// A topic with no partitions is refused, and nothing of it is stored.
func TestCreateTopicRefusesZeroPartitions(t *testing.T) {
	s := openStore(t)
	for _, n := range []int32{0, -1} {
		if _, err := s.CreateTopic("t", n, nil); err == nil {
			t.Errorf("CreateTopic with %d partitions succeeded", n)
		}
	}
	if _, ok, _ := s.Topic("t"); ok {
		t.Error("a refused topic is registered")
	}
}

// Creating a topic writes its record and its id mapping, both on disk: after a
// reopen the topic is found by name and by id.
func TestCreateTopicWritesNameAndIDMappingToDisk(t *testing.T) {
	path := t.TempDir() + "/meta.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	topic, err := s.CreateTopic("orders", 3, map[string]string{"retention.ms": "1000"})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	byID, ok, err := s.TopicByID(topic.ID)
	if err != nil || !ok || byID.Name != "orders" || byID.Partitions != 3 {
		t.Fatalf("TopicByID after reopen = %+v, %v, %v", byID, ok, err)
	}
	var mapped []byte
	s.(*boltStore).db.View(func(tx *bbolt.Tx) error {
		mapped = append(mapped, tx.Bucket(bucketTopicsByID).Get(topic.ID[:])...)
		return nil
	})
	if string(mapped) != "orders" {
		t.Errorf("id mapping on disk = %q, want orders", mapped)
	}
}

// Removing several ACLs removes every one of them, not only the first.
func TestRemoveACLsRemovesEveryOneNamed(t *testing.T) {
	s := openStore(t)
	mk := func(p string) ACL {
		return ACL{ResourceType: 2, Name: "orders", PatternType: 3, Principal: p, Host: "*", Operation: 3, Permission: 3}
	}
	a, b, c := mk("User:a"), mk("User:b"), mk("User:c")
	if err := s.AddACLs([]ACL{a, b, c}); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveACLs([]ACL{a, c}); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListACLs()
	if err != nil || len(got) != 1 || got[0] != b {
		t.Fatalf("after removing a and c: %+v, %v, want only b", got, err)
	}
}

// A registry operation writes all its puts and all its deletes, and they are on
// disk after a reopen.
func TestRegistryApplyAppliesEveryPutAndDelete(t *testing.T) {
	path := t.TempDir() + "/meta.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RegistryApply(map[string][]byte{"d1": []byte("x"), "d2": []byte("x"), "d3": []byte("x"), "keep": []byte("k")}, nil); err != nil {
		t.Fatal(err)
	}
	puts := map[string][]byte{"p1": []byte("one"), "p2": []byte("two"), "p3": []byte("three")}
	if err := s.RegistryApply(puts, []string{"d1", "d2", "d3", "absent"}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	all, err := s.RegistryAll()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"keep": "k", "p1": "one", "p2": "two", "p3": "three"}
	if len(all) != len(want) {
		t.Fatalf("registry = %v, want %v", all, want)
	}
	for k, v := range want {
		if string(all[k]) != v {
			t.Errorf("registry[%q] = %q, want %q", k, all[k], v)
		}
	}
}

// Transactional state is stored, listed, replaced by id, and survives a reopen.
func TestTxnRecordsRoundTrip(t *testing.T) {
	path := t.TempDir() + "/meta.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.ListTxns(); err != nil || len(got) != 0 {
		t.Fatalf("empty store: %v, %v", got, err)
	}
	r1 := TxnRecord{ID: "t1", ProducerID: 7, Epoch: 2, State: "Ongoing", TimeoutMs: 5000, Groups: []string{"g"},
		Partitions: []TxnPartition{{Topic: "a", Partition: 1}}, Offsets: []TxnOffset{{Group: "g", Topic: "a", Partition: 1, Offset: 9}}, UpdatedMs: 11}
	r2 := TxnRecord{ID: "t2", ProducerID: 8, State: "Empty", UpdatedMs: 12}
	for _, r := range []TxnRecord{r1, r2} {
		if err := s.PutTxn(r); err != nil {
			t.Fatal(err)
		}
	}
	r1b := r1
	r1b.State, r1b.UpdatedMs = "PrepareCommit", 13
	if err := s.PutTxn(r1b); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.ListTxns()
	if err != nil || len(got) != 2 {
		t.Fatalf("ListTxns = %+v, %v, want two", got, err)
	}
	if got[0].ID != "t1" || got[0].State != "PrepareCommit" || got[0].ProducerID != 7 || got[0].Epoch != 2 ||
		len(got[0].Offsets) != 1 || got[0].Offsets[0].Offset != 9 || len(got[0].Partitions) != 1 {
		t.Errorf("t1 = %+v", got[0])
	}
	if got[1].ID != "t2" || got[1].ProducerID != 8 {
		t.Errorf("t2 = %+v", got[1])
	}
}

// A group whose id is empty has offsets like any other and is listed.
func TestEmptyGroupIDIsListedAmongOffsetGroups(t *testing.T) {
	s := openStore(t)
	mustTopics(t, s, "a")
	if err := s.CommitOffset("", "a", 0, 4, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitOffset("g", "a", 0, 5, ""); err != nil {
		t.Fatal(err)
	}
	groups, err := s.OffsetGroups()
	if err != nil || len(groups) != 2 || groups[0] != "" || groups[1] != "g" {
		t.Fatalf("OffsetGroups = %q, %v, want [\"\" g]", groups, err)
	}
}

// An offset key is built whatever the lengths of group and topic: a long topic
// under a short group id commits and reads back.
func TestOffsetOfALongTopicUnderAShortGroup(t *testing.T) {
	s := openStore(t)
	long := strings.Repeat("t", 200)
	mustTopics(t, s, long)
	if err := s.CommitOffset("g", long, 5, 77, "m"); err != nil {
		t.Fatal(err)
	}
	if off, ok, err := s.FetchOffset("g", long, 5); err != nil || !ok || off != 77 {
		t.Fatalf("FetchOffset = %d, %v, %v", off, ok, err)
	}
	k := offsetKey("g", long, 5)
	want := append(append(append([]byte("g\x00"), long...), 0), 0, 0, 0, 5)
	if !bytes.Equal(k, want) {
		t.Errorf("offsetKey = %q, want %q", k, want)
	}
}

// The producer-id block is as large as asked, and a block of one is one id.
func TestReserveProducerIDsBlockSizes(t *testing.T) {
	s := openStore(t)
	a, _ := s.ReserveProducerIDs(1)
	b, _ := s.ReserveProducerIDs(0) // below one is taken as one.
	c, _ := s.ReserveProducerIDs(-5)
	d, _ := s.ReserveProducerIDs(10)
	e, _ := s.ReserveProducerIDs(1)
	if b != a+1 || c != b+1 || d != c+1 || e != d+10 {
		t.Fatalf("blocks start at %d %d %d %d %d", a, b, c, d, e)
	}
}

// An offset key for the empty topic name is still an offset the group holds: it
// is listed and expires like another.
func TestOffsetKeysForAnEmptyTopicNameAreListedAndExpired(t *testing.T) {
	s := openStore(t).(*boltStore)
	if err := s.update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketOffsets).Put(offsetKey("g", "", 3), []byte(`{"offset":5,"at":100}`))
	}); err != nil {
		t.Fatal(err)
	}
	es, err := s.ListOffsets("g")
	if err != nil || len(es) != 1 || es[0].Topic != "" || es[0].Partition != 3 || es[0].Offset != 5 {
		t.Fatalf("ListOffsets = %+v, %v", es, err)
	}
	var seen []int32
	n, err := s.ExpireOffsets("g", func(topic string, p int32, at int64) bool { seen = append(seen, p); return true })
	if err != nil || n != 1 || len(seen) != 1 || seen[0] != 3 {
		t.Fatalf("ExpireOffsets = %d, %v (saw %v)", n, err, seen)
	}
	if es, _ := s.ListOffsets("g"); len(es) != 0 {
		t.Errorf("left after expiry: %+v", es)
	}
}

// A sweep applies every change it decides on: all the expired offsets are
// deleted and all the legacy ones stamped, not only the first of each.
func TestExpireOffsetsAppliesEveryChange(t *testing.T) {
	s := openStore(t).(*boltStore)
	now := time.UnixMilli(1_700_000_000_000)
	s.clock = func() time.Time { return now }
	mustTopics(t, s, "a", "b", "c")
	put := func(topic, rec string) {
		t.Helper()
		if err := s.update(func(tx *bbolt.Tx) error {
			return tx.Bucket(bucketOffsets).Put(offsetKey("g", topic, 0), []byte(rec))
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Two legacy records (no time) that are kept: both are stamped.
	put("a", `{"offset":1}`)
	put("b", `{"offset":2}`)
	if n, err := s.ExpireOffsets("g", func(string, int32, int64) bool { return false }); err != nil || n != 0 {
		t.Fatalf("%d, %v", n, err)
	}
	es, _ := s.ListOffsets("g")
	if len(es) != 2 || es[0].CommittedAt != now.UnixMilli() || es[1].CommittedAt != now.UnixMilli() {
		t.Fatalf("stamps: %+v, want both at %d", es, now.UnixMilli())
	}
	// Three expired records go together.
	put("c", `{"offset":3,"at":5}`)
	if n, err := s.ExpireOffsets("g", func(string, int32, int64) bool { return true }); err != nil || n != 3 {
		t.Fatalf("expired %d, %v, want 3", n, err)
	}
	if es, _ := s.ListOffsets("g"); len(es) != 0 {
		t.Errorf("left after expiry: %+v", es)
	}
}

// The default wait for the file lock is bounded: with the lock held, a second
// open gives up within seconds.
func TestDefaultLockTimeoutIsBounded(t *testing.T) {
	if lockTimeout != 2*time.Second {
		t.Fatalf("lockTimeout = %v, want 2s", lockTimeout)
	}
	path := t.TempDir() + "/meta.db"
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	done := make(chan error, 1)
	go func() { _, err := Open(path); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a second open of a locked file succeeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a second open of a locked file was still waiting after 10s")
	}
}
