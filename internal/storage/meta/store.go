// Package meta is the broker's metadata store: the topic registry, committed
// consumer-group offsets, and the producer-id counter. It is kept entirely
// separate from the message log and is durable across restarts, backed by a
// single embedded bbolt database. Committed offsets here replace Kafka's internal
// __consumer_offsets topic. See docs/storage-and-durability.md.
package meta

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
)

// lockTimeout bounds the wait for bbolt's exclusive file lock (a variable so a
// test need not wait the full two seconds).
var lockTimeout = 2 * time.Second

// ErrTopicExists is returned by CreateTopic when the name is already registered.
var ErrTopicExists = errors.New("meta: topic already exists")

// ErrTopicCollides is returned by CreateTopic when the name differs from an
// existing topic's only in letter case. Partition directories are named after the
// topic, and on a case-insensitive filesystem (the macOS and Windows defaults)
// "Orders" and "orders" are one directory: two topics would share a log, read each
// other's records, and delete each other's data. Refused on every platform so a
// data directory behaves the same wherever it is opened.
var ErrTopicCollides = errors.New("meta: topic name collides with an existing topic")

// ErrInvalidGroupID is returned for a group id that cannot be a key: NUL is the
// separator between group, topic and partition in the offsets bucket, so a group
// id containing one would read and delete another group's offsets.
var ErrInvalidGroupID = errors.New("meta: invalid group id")

// ValidateGroupID reports whether a group id can be stored.
func ValidateGroupID(group string) error {
	if strings.IndexByte(group, 0) >= 0 {
		return fmt.Errorf("%w: contains a NUL byte", ErrInvalidGroupID)
	}
	return nil
}

var (
	bucketTopics     = []byte("topics")       // name -> topicRecord (JSON)
	bucketTopicsByID = []byte("topics_by_id") // uuid -> name
	bucketOffsets    = []byte("offsets")      // group\0topic\0partition -> offsetRecord (JSON)
	bucketMeta       = []byte("meta")         // singletons (producer-id counter, ...)
	keyProducerID    = []byte("producer_id")
)

// TopicID is a Kafka topic UUID. Modern Fetch/Metadata versions address topics by
// this rather than by name.
type TopicID [16]byte

// Topic is a registered topic's metadata.
type Topic struct {
	Name       string
	ID         TopicID
	Partitions int32
	Config     map[string]string
}

// Store is the metadata persistence contract: single-writer, small, durable.
type Store interface {
	CreateTopic(name string, partitions int32, config map[string]string) (Topic, error)
	Topic(name string) (Topic, bool, error)
	TopicByID(id TopicID) (Topic, bool, error)
	ListTopics() ([]Topic, error)
	DeleteTopic(name string) error
	// SetTopicConfig replaces a topic's own configuration (its overrides of the
	// broker's defaults) and returns the topic as it now is.
	SetTopicConfig(name string, config map[string]string) (Topic, error)

	CommitOffset(group, topic string, partition int32, offset int64, metadata string) error
	FetchOffset(group, topic string, partition int32) (offset int64, ok bool, err error)
	ListOffsets(group string) ([]OffsetEntry, error)
	// OffsetGroups lists every group id that has at least one committed offset.
	OffsetGroups() ([]string, error)
	// DeleteGroupOffsets removes every committed offset of a group and reports how many.
	DeleteGroupOffsets(group string) (int, error)
	// DeleteOffset removes one committed offset (a no-op if there is none).
	DeleteOffset(group, topic string, partition int32) error

	// CommitOffsets stores several offsets of one group in a single transaction.
	// Each bolt transaction is an fsync, so committing 20 partitions one at a time
	// was 20 fsyncs; this is one.
	CommitOffsets(group string, offsets []OffsetCommit) error

	NextProducerID() (int64, error)
	// ReserveProducerIDs hands out a block of n consecutive producer ids in one
	// transaction and returns the first. The block is durable before it is used,
	// so ids are never reissued after a crash (a few may be skipped).
	ReserveProducerIDs(n int64) (first int64, err error)

	Close() error
}

// Open opens (creating if needed) the metadata database at path and ensures its
// buckets exist.
func Open(path string) (Store, error) {
	// bbolt takes an exclusive file lock. With no timeout a second broker on the
	// same file (a forgotten terminal, a crashed run still shutting down) blocks
	// forever in silence; a short timeout turns it into an error that says why.
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: lockTimeout})
	if err != nil {
		if errors.Is(err, bbolt.ErrTimeout) {
			return nil, fmt.Errorf("meta: %s is locked by another process; is another doze-kafka already using this --meta-db (or --data-dir)?", path)
		}
		return nil, fmt.Errorf("meta: open %s: %w", path, err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, name := range [][]byte{bucketTopics, bucketTopicsByID, bucketOffsets, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("meta: init buckets: %w", err)
	}
	s := &boltStore{db: db, byName: map[string]Topic{}, byID: map[TopicID]string{}}
	if err := s.loadCache(); err != nil {
		db.Close()
		return nil, fmt.Errorf("meta: load topic registry: %w", err)
	}
	return s, nil
}

func (s *boltStore) loadCache() error {
	return s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketTopics).ForEach(func(k, v []byte) error {
			var rec topicRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			t := Topic{Name: string(k), ID: rec.ID, Partitions: rec.Partitions, Config: rec.Config}
			s.byName[t.Name] = t
			s.byID[t.ID] = t.Name
			return nil
		})
	})
}

// cloneTopic copies the one mutable field so a caller cannot change the cache.
func cloneTopic(t Topic) Topic {
	if t.Config != nil {
		t.Config = maps.Clone(t.Config)
	}
	return t
}

// OffsetCommit is one committed offset, for CommitOffsets.
type OffsetCommit struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
}

// boltStore is the bbolt-backed Store. The topic registry is also held in memory:
// this process is the only writer of the file (bbolt enforces that with its lock),
// so a write-through cache is always coherent, and it takes a produce, fetch or
// commit from a read transaction plus a JSON decode (~3 µs) to a map lookup.
type boltStore struct {
	db *bbolt.DB

	writeTxns atomic.Int64

	afterCreateCommit func() // test hook: runs between the commit and the cache update.

	topicMu sync.Mutex // serializes registry writes together with their cache update.
	mu      sync.RWMutex
	byName  map[string]Topic
	byID    map[TopicID]string
}

// update runs one write transaction (one fsync) and counts it, so a test can pin
// "this operation is a single transaction".
func (s *boltStore) update(fn func(tx *bbolt.Tx) error) error {
	s.writeTxns.Add(1)
	return s.db.Update(fn)
}

type topicRecord struct {
	ID         TopicID           `json:"id"`
	Partitions int32             `json:"partitions"`
	Config     map[string]string `json:"config,omitempty"`
}

type offsetRecord struct {
	Offset   int64  `json:"offset"`
	Metadata string `json:"metadata,omitempty"`
}

// OffsetEntry is one committed (topic, partition) → offset for a group, returned
// by ListOffsets (the "fetch all offsets for a group" path).
type OffsetEntry struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
}

func (s *boltStore) CreateTopic(name string, partitions int32, config map[string]string) (Topic, error) {
	if err := ValidateTopicName(name); err != nil {
		return Topic{}, err
	}
	if partitions <= 0 {
		return Topic{}, fmt.Errorf("meta: partitions must be positive, got %d", partitions)
	}
	var id TopicID
	if _, err := rand.Read(id[:]); err != nil {
		return Topic{}, err
	}
	rec := topicRecord{ID: id, Partitions: partitions, Config: config}
	// The registry write and the cache update are one step under topicMu: with
	// two separate critical sections a concurrent create and delete of the same
	// name could commit in one order and update the cache in the other, leaving a
	// topic the store has and the cache does not (invisible, yet uncreatable).
	s.topicMu.Lock()
	defer s.topicMu.Unlock()
	s.mu.RLock()
	for existing := range s.byName {
		if existing != name && strings.EqualFold(existing, name) {
			s.mu.RUnlock()
			return Topic{}, fmt.Errorf("%w: %q and %q differ only in letter case", ErrTopicCollides, name, existing)
		}
	}
	s.mu.RUnlock()
	err := s.update(func(tx *bbolt.Tx) error {
		topics := tx.Bucket(bucketTopics)
		if topics.Get([]byte(name)) != nil {
			return ErrTopicExists
		}
		val, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if err := topics.Put([]byte(name), val); err != nil {
			return err
		}
		return tx.Bucket(bucketTopicsByID).Put(id[:], []byte(name))
	})
	if err != nil {
		return Topic{}, err
	}
	t := Topic{Name: name, ID: id, Partitions: partitions, Config: config}
	if s.afterCreateCommit != nil {
		s.afterCreateCommit()
	}
	s.mu.Lock()
	s.byName[name] = cloneTopic(t)
	s.byID[id] = name
	s.mu.Unlock()
	return t, nil
}

func (s *boltStore) Topic(name string) (Topic, bool, error) {
	s.mu.RLock()
	t, ok := s.byName[name]
	s.mu.RUnlock()
	return cloneTopic(t), ok, nil
}

func (s *boltStore) TopicByID(id TopicID) (Topic, bool, error) {
	s.mu.RLock()
	name, ok := s.byID[id]
	t := s.byName[name]
	s.mu.RUnlock()
	return cloneTopic(t), ok, nil
}

func (s *boltStore) ListTopics() ([]Topic, error) {
	s.mu.RLock()
	topics := make([]Topic, 0, len(s.byName))
	for _, t := range s.byName {
		topics = append(topics, cloneTopic(t))
	}
	s.mu.RUnlock()
	slices.SortFunc(topics, func(a, b Topic) int { return strings.Compare(a.Name, b.Name) })
	return topics, nil
}

// ErrNoSuchTopic is returned by SetTopicConfig for a topic that is not registered.
var ErrNoSuchTopic = errors.New("meta: no such topic")

func (s *boltStore) SetTopicConfig(name string, config map[string]string) (Topic, error) {
	if len(config) == 0 {
		config = nil
	}
	// Under topicMu, like create and delete, so the stored record and the cache
	// change together and a concurrent delete cannot be undone by this write.
	s.topicMu.Lock()
	defer s.topicMu.Unlock()
	var rec topicRecord
	err := s.update(func(tx *bbolt.Tx) error {
		topics := tx.Bucket(bucketTopics)
		v := topics.Get([]byte(name))
		if v == nil {
			return ErrNoSuchTopic
		}
		if err := json.Unmarshal(v, &rec); err != nil {
			return err
		}
		rec.Config = config
		val, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return topics.Put([]byte(name), val)
	})
	if err != nil {
		return Topic{}, err
	}
	t := Topic{Name: name, ID: rec.ID, Partitions: rec.Partitions, Config: config}
	s.mu.Lock()
	s.byName[name] = cloneTopic(t)
	s.mu.Unlock()
	return cloneTopic(t), nil
}

func (s *boltStore) DeleteTopic(name string) error {
	s.topicMu.Lock()
	defer s.topicMu.Unlock()
	err := s.update(func(tx *bbolt.Tx) error {
		topics := tx.Bucket(bucketTopics)
		v := topics.Get([]byte(name))
		if v == nil {
			return nil
		}
		var rec topicRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			return err
		}
		if err := topics.Delete([]byte(name)); err != nil {
			return err
		}
		if err := tx.Bucket(bucketTopicsByID).Delete(rec.ID[:]); err != nil {
			return err
		}
		// Committed offsets belong to the topic's life, not the name's. Left behind,
		// they point into a log that no longer exists: after delete-and-recreate (a
		// test teardown, or the console's Purge) a consumer resumes from an offset
		// the new topic has not reached, and silently skips everything below it.
		// Same transaction, so a topic is never gone with its offsets still there.
		offsets := tx.Bucket(bucketOffsets)
		var stale [][]byte
		c := offsets.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if offsetKeyTopic(k) == name {
				stale = append(stale, append([]byte(nil), k...))
			}
		}
		for _, k := range stale {
			if err := offsets.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	if t, ok := s.byName[name]; ok {
		delete(s.byID, t.ID)
		delete(s.byName, name)
	}
	s.mu.Unlock()
	return nil
}

// offsetKeyTopic extracts the topic from a bucketOffsets key
// (group \0 topic \0 partition). Topic names contain no NUL, so the topic is
// whatever follows the last NUL before the 4-byte partition.
func offsetKeyTopic(k []byte) string {
	if len(k) < 6 {
		return ""
	}
	rest := k[:len(k)-5] // drop \0 + partition.
	return string(rest[bytes.LastIndexByte(rest, 0)+1:])
}

func (s *boltStore) CommitOffsets(group string, offsets []OffsetCommit) error {
	if len(offsets) == 0 {
		return nil
	}
	if err := ValidateGroupID(group); err != nil {
		return err
	}
	return s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketOffsets)
		topics := tx.Bucket(bucketTopics)
		for _, o := range offsets {
			// The topic is checked inside the transaction that writes the offset.
			// DeleteTopic removes a topic's offsets with it; a commit that had
			// already passed the handler's lookup would otherwise land after the
			// delete and be inherited by the next topic of that name.
			if topics.Get([]byte(o.Topic)) == nil {
				continue
			}
			val, err := json.Marshal(offsetRecord{Offset: o.Offset, Metadata: o.Metadata})
			if err != nil {
				return err
			}
			if err := b.Put(offsetKey(group, o.Topic, o.Partition), val); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *boltStore) CommitOffset(group, topic string, partition int32, offset int64, metadata string) error {
	return s.CommitOffsets(group, []OffsetCommit{{Topic: topic, Partition: partition, Offset: offset, Metadata: metadata}})
}

func (s *boltStore) FetchOffset(group, topic string, partition int32) (int64, bool, error) {
	var (
		offset int64
		found  bool
	)
	err := s.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketOffsets).Get(offsetKey(group, topic, partition))
		if v == nil {
			return nil
		}
		var rec offsetRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			return err
		}
		offset, found = rec.Offset, true
		return nil
	})
	return offset, found, err
}

// ListOffsets returns every committed offset for a group. Keys are
// group\0topic\0partition(4B); we scan the group\0 prefix and split each key back
// into (topic, partition). The trailing \0 in the prefix keeps group "foo" from
// matching group "foobar".
// OffsetGroups lists the distinct group ids that hold committed offsets, sorted.
func (s *boltStore) OffsetGroups() ([]string, error) {
	var out []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucketOffsets).Cursor()
		var last string
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			i := bytes.IndexByte(k, 0)
			if i < 0 {
				continue
			}
			if g := string(k[:i]); g != last || len(out) == 0 {
				out = append(out, g)
				last = g
			}
		}
		return nil
	})
	return out, err
}

func (s *boltStore) DeleteGroupOffsets(group string) (int, error) {
	prefix := append([]byte(group), 0)
	var n int
	err := s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketOffsets)
		var keys [][]byte
		c := b.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			keys = append(keys, append([]byte(nil), k...))
		}
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		n = len(keys)
		return nil
	})
	return n, err
}

func (s *boltStore) DeleteOffset(group, topic string, partition int32) error {
	return s.update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketOffsets).Delete(offsetKey(group, topic, partition))
	})
}

func (s *boltStore) ListOffsets(group string) ([]OffsetEntry, error) {
	prefix := append([]byte(group), 0)
	var out []OffsetEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucketOffsets).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			rem := k[len(prefix):]
			if len(rem) < 5 { // topic(>=0) + \0 + partition(4)
				continue
			}
			topic := string(rem[:len(rem)-5])
			partition := int32(binary.BigEndian.Uint32(rem[len(rem)-4:]))
			var rec offsetRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			out = append(out, OffsetEntry{Topic: topic, Partition: partition, Offset: rec.Offset, Metadata: rec.Metadata})
		}
		return nil
	})
	return out, err
}

func (s *boltStore) NextProducerID() (int64, error) { return s.ReserveProducerIDs(1) }

func (s *boltStore) ReserveProducerIDs(n int64) (int64, error) {
	if n < 1 {
		n = 1
	}
	var first int64
	err := s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		var next int64
		if v := b.Get(keyProducerID); len(v) == 8 {
			next = int64(binary.BigEndian.Uint64(v))
		}
		first = next
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(next+n))
		return b.Put(keyProducerID, buf[:])
	})
	return first, err
}

func (s *boltStore) Close() error { return s.db.Close() }

// offsetKey builds the composite key group\0topic\0<partition:be32>.
func offsetKey(group, topic string, partition int32) []byte {
	key := make([]byte, 0, len(group)+len(topic)+6)
	key = append(key, group...)
	key = append(key, 0)
	key = append(key, topic...)
	key = append(key, 0)
	var pb [4]byte
	binary.BigEndian.PutUint32(pb[:], uint32(partition))
	return append(key, pb[:]...)
}
