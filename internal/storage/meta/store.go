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
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

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

// CollisionError is the error ErrTopicCollides stands for, with the two names.
type CollisionError struct {
	Name, Existing string
	CaseOnly       bool // differs only in letter case; otherwise only in '.' against '_'.
}

func (e *CollisionError) Error() string {
	if e.CaseOnly {
		return fmt.Sprintf("%v: %q and %q differ only in letter case", ErrTopicCollides, e.Name, e.Existing)
	}
	return fmt.Sprintf("%v: %q and %q differ only in '.' against '_'", ErrTopicCollides, e.Name, e.Existing)
}

func (e *CollisionError) Is(target error) bool { return target == ErrTopicCollides }

// FindCollision reports the first of existing that name may not live beside, or nil.
// It is the rule spelled out over a list; the store answers the same question for the
// topics it holds from an index (TopicCollision), without looking at each of them.
func FindCollision(name string, existing []string) *CollisionError {
	for _, e := range existing {
		if e == name {
			continue
		}
		caseOnly := strings.EqualFold(e, name)
		if caseOnly || metricKey(e) == metricKey(name) {
			return &CollisionError{Name: name, Existing: e, CaseOnly: caseOnly}
		}
	}
	return nil
}

// metricKey is a name with '.' written as '_': Kafka's metric names cannot tell
// the two apart, so it refuses a topic whose key is one that exists.
func metricKey(name string) string { return strings.ReplaceAll(name, ".", "_") }

// foldKey is a name with every letter replaced by the least of the letters it equals
// without regard to case, so two names have one key exactly when strings.EqualFold
// says they are equal.
func foldKey(name string) string {
	return strings.Map(func(r rune) rune {
		least := r
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			least = min(least, f)
		}
		return least
	}, name)
}

// nameIndex answers "which topic may this name not live beside" without looking at
// every topic: the names under each key the two rules compare. Creating n topics
// used to compare each with all that existed, n squared comparisons for a batch.
type nameIndex struct {
	byFold, byMetric map[string][]string
}

func newNameIndex() nameIndex {
	return nameIndex{byFold: map[string][]string{}, byMetric: map[string][]string{}}
}

func (x nameIndex) add(name string) {
	x.byFold[foldKey(name)] = append(x.byFold[foldKey(name)], name)
	x.byMetric[metricKey(name)] = append(x.byMetric[metricKey(name)], name)
}

func (x nameIndex) remove(name string) {
	drop := func(m map[string][]string, key string) {
		names := m[key]
		if i := slices.Index(names, name); i >= 0 {
			names = slices.Delete(names, i, i+1)
		}
		if len(names) == 0 {
			delete(m, key)
			return
		}
		m[key] = names
	}
	drop(x.byFold, foldKey(name))
	drop(x.byMetric, metricKey(name))
}

// collision is the topic name may not live beside, or nil. Names in beside are let
// off the '.' against '_' rule (and only that one): see Store.CreateTopicBeside.
func (x nameIndex) collision(name string, beside map[string]struct{}) *CollisionError {
	for _, e := range x.byFold[foldKey(name)] {
		if e != name {
			return &CollisionError{Name: name, Existing: e, CaseOnly: true}
		}
	}
	// The newest first: of several topics a name collides with (they can only have
	// come from one request), Kafka names the one created last.
	for _, e := range slices.Backward(x.byMetric[metricKey(name)]) {
		if _, let := beside[e]; e != name && !let {
			return &CollisionError{Name: name, Existing: e}
		}
	}
	return nil
}

// ErrInvalidGroupID is returned for a group id that cannot be a key: NUL is the
// separator between group, topic and partition in the offsets bucket, so a group
// id containing one would read and delete another group's offsets.
var ErrInvalidGroupID = errors.New("meta: invalid group id")

// MaxGroupIDBytes is the longest group id that is stored. A group id is part of a key
// of the offsets bucket (group, topic, partition), and a key there cannot be longer
// than 32,768 bytes: a longer id could be joined and never committed for. It is also
// kept in memory for as long as the group is, and a flexible request can carry one a
// megabyte long. 4,096 bytes is far past any real group id and leaves a key room.
const MaxGroupIDBytes = 4096

// ValidateGroupID reports whether a group id can be stored.
func ValidateGroupID(group string) error {
	if strings.IndexByte(group, 0) >= 0 {
		return fmt.Errorf("%w: contains a NUL byte", ErrInvalidGroupID)
	}
	if len(group) > MaxGroupIDBytes {
		return fmt.Errorf("%w: %d bytes, more than %d", ErrInvalidGroupID, len(group), MaxGroupIDBytes)
	}
	return nil
}

var (
	bucketTopics     = []byte("topics")       // name -> topicRecord (JSON)
	bucketTopicsByID = []byte("topics_by_id") // uuid -> name
	bucketOffsets    = []byte("offsets")      // group\0topic\0partition -> offsetRecord (JSON)
	bucketMeta       = []byte("meta")         // singletons (producer-id counter, ...)
	bucketACLs       = []byte("acls")         // acl key -> ACL (JSON).
	bucketTxns       = []byte("txns")         // transactional.id -> TxnRecord (JSON).
	bucketRegistry   = []byte("registry")     // schema registry records, opaque to this package.
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
	// CreateTopicBeside is CreateTopic for one topic of a CreateTopics request that
	// names several. Apache Kafka checks each name of a request against the topics
	// that existed before it, not against the others in it, so "a.b" and "a_b" sent
	// together are both created (recorded: createtopics-collisions): beside is the
	// names this request has already created, which the '.' against '_' rule does
	// not count. Names that differ only in letter case are refused all the same:
	// that rule is about the directory two topics would share.
	CreateTopicBeside(name string, partitions int32, config map[string]string, beside map[string]struct{}) (Topic, error)
	// TopicCollision is the registered topic a new topic called name could not live
	// beside (CreateTopicBeside's rule and its beside), or nil: what CreateTopics
	// with validate_only has to say without creating anything.
	TopicCollision(name string, beside map[string]struct{}) *CollisionError
	Topic(name string) (Topic, bool, error)
	TopicByID(id TopicID) (Topic, bool, error)
	ListTopics() ([]Topic, error)
	DeleteTopic(name string) error
	// SetTopicConfig replaces a topic's own configuration (its overrides of the
	// broker's defaults) and returns the topic as it now is.
	SetTopicConfig(name string, config map[string]string) (Topic, error)
	// SetPartitions raises a topic's partition count and returns the topic as it
	// now is. A count that is not above the current one is ErrPartitionsNotIncreased:
	// partitions are never removed.
	SetPartitions(name string, count int32) (Topic, error)

	CommitOffset(group, topic string, partition int32, offset int64, metadata string) error
	FetchOffset(group, topic string, partition int32) (offset int64, ok bool, err error)
	ListOffsets(group string) ([]OffsetEntry, error)
	// OffsetGroups lists every group id that has at least one committed offset.
	OffsetGroups() ([]string, error)
	// DeleteGroupOffsets removes every committed offset of a group and reports how many.
	DeleteGroupOffsets(group string) (int, error)
	// DeleteOffset removes one committed offset (a no-op if there is none).
	DeleteOffset(group, topic string, partition int32) error

	// ExpireOffsets deletes, in one transaction, the offsets of group that expire says
	// to (given the topic, partition and the time the offset was committed, in unix
	// milliseconds), and reports how many. Run inside the write transaction, so a commit
	// that races with it is either before (and judged) or after (and kept). An offset
	// that has no commit time, written before they were kept, is given the time of this
	// call, so a retention starts counting from the first sweep that saw it.
	ExpireOffsets(group string, expire func(topic string, partition int32, committedAt int64) bool) (int, error)
	// SetGroupLiveness records what the offsets retention must still know about a
	// group after a restart, which forgets the members themselves: that it has
	// members (and the longest session any of them asked for), or when it lost the
	// last one. A nil state forgets the group.
	SetGroupLiveness(group string, state *GroupLiveness) error
	// GroupLiveness is every recorded group.
	GroupLiveness() (map[string]GroupLiveness, error)
	// AdoptGroupsWithOffsets is run when the store is opened by a broker: the first
	// time, on a store written before groups with members were recorded, it records
	// every group that has offsets and no state as one that has members with this
	// session, because which of them had is not known and an offset is kept when
	// that is in doubt. It reports how many it recorded; afterwards it does nothing.
	AdoptGroupsWithOffsets(session time.Duration) (int, error)

	// CommitOffsets stores several offsets of one group in a single transaction.
	// Each bolt transaction is an fsync, so committing 20 partitions one at a time
	// was 20 fsyncs; this is one.
	CommitOffsets(group string, offsets []OffsetCommit) error

	// AddACLs stores access-control entries. An entry already present is left as it
	// is: adding twice is not an error, as in Kafka. They are kept for tooling that
	// provisions and reads them back; nothing in the broker consults them.
	AddACLs(acls []ACL) error
	// ListACLs returns every stored entry, in a stable order.
	ListACLs() ([]ACL, error)
	// RemoveACLs deletes exactly these entries (any that are not there are ignored).
	RemoveACLs(acls []ACL) error

	// PutTxn stores one transactional producer's coordinator state, durably. The
	// state of a transaction is written before the broker acts on it, so a crash
	// part-way through ending one is finished on the next start.
	PutTxn(rec TxnRecord) error
	// ListTxns returns every stored transactional state.
	ListTxns() ([]TxnRecord, error)

	// RegistryAll returns every record the schema registry keeps. The registry owns
	// the key space and the encoding; this package only stores bytes.
	RegistryAll() (map[string][]byte, error)
	// RegistryApply writes and deletes records in one transaction, so a registry
	// operation that touches several (a schema and its version) is all or nothing.
	RegistryApply(puts map[string][]byte, deletes []string) error

	NextProducerID() (int64, error)
	// ReserveProducerIDs hands out a block of n consecutive producer ids in one
	// transaction and returns the first. The block is durable before it is used,
	// so ids are never reissued after a crash (a few may be skipped).
	ReserveProducerIDs(n int64) (first int64, err error)

	Close() error
}

// Open opens (creating if needed) the metadata database at path and ensures its
// buckets exist.
func Open(path string) (Store, error) { return OpenWithClock(path, time.Now) }

// OpenWithClock is Open with the clock offsets are stamped with, for a test that
// has to say what time a commit was made.
func OpenWithClock(path string, clock func() time.Time) (Store, error) {
	// bbolt takes an exclusive file lock. With no timeout a second broker on the
	// same file (a forgotten terminal, a crashed run still shutting down) blocks
	// forever in silence; a short timeout turns it into an error that says why.
	_, statErr := os.Stat(path)
	isNew := errors.Is(statErr, fs.ErrNotExist)
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: lockTimeout})
	if err != nil {
		if errors.Is(err, bbolt.ErrTimeout) {
			return nil, fmt.Errorf("meta: %s is locked by another process; is another doze-kafka already using this --meta-db (or --data-dir)?", path)
		}
		return nil, fmt.Errorf("meta: open %s: %w", path, err)
	}
	s := &boltStore{db: db, byName: map[string]Topic{}, byID: map[TopicID]string{}, names: newNameIndex(), clock: clock}
	err = s.commit(func(tx *bbolt.Tx) error {
		for _, name := range [][]byte{bucketTopics, bucketTopicsByID, bucketOffsets, bucketMeta, bucketACLs, bucketTxns, bucketRegistry} {
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
	if isNew {
		// bbolt flushes the file on every commit and never its directory. A database
		// created a moment ago is a name in a directory nobody has flushed: every
		// topic, offset and transaction committed to it since would go with that
		// name in a power cut.
		dir := filepath.Dir(path)
		if err := syncDirectory(dir); err != nil {
			db.Close()
			return nil, fmt.Errorf("meta: make %s durable: %w", path, err)
		}
		durable("dirsync", dir)
	}
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
			s.names.add(t.Name)
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

	clock func() time.Time // the time an offset is stamped with; replaceable for a test.

	topicMu sync.Mutex // serializes registry writes together with their cache update.
	mu      sync.RWMutex
	byName  map[string]Topic
	byID    map[TopicID]string
	names   nameIndex // the names of byName, by what the collision rules compare.
}

// update runs one write transaction (one fsync) and counts it, so a test can pin
// "this operation is a single transaction".
func (s *boltStore) update(fn func(tx *bbolt.Tx) error) error {
	s.writeTxns.Add(1)
	if err := faultAt("meta.write"); err != nil {
		return err
	}
	return s.commit(fn)
}

// commit runs one bbolt write transaction. bbolt fsyncs the file before Update
// returns unless it was opened with NoSync, and every answer this broker gives
// about a committed offset, a transaction's state or a topic rests on that; the
// hook is told so only when it is true, which is how a power-cut test notices a
// store that was opened not to sync.
func (s *boltStore) commit(fn func(tx *bbolt.Tx) error) error {
	if err := s.db.Update(fn); err != nil {
		return err
	}
	if !s.db.NoSync {
		durable("commit", s.db.Path())
	}
	return nil
}

var durabilityHook atomic.Pointer[func(event, path string)]

// SetDurabilityHookForTest installs a function that is told each time the store
// has made something durable: "commit" with the database's path after a
// transaction it fsynced, "dirsync" with a directory after flushing it. It
// returns a func that removes the hook. Nothing in the broker calls it.
func SetDurabilityHookForTest(fn func(event, path string)) (restore func()) {
	old := durabilityHook.Swap(&fn)
	return func() { durabilityHook.Store(old) }
}

func durable(event, path string) {
	if h := durabilityHook.Load(); h != nil && *h != nil {
		(*h)(event, path)
	}
}

type topicRecord struct {
	ID         TopicID           `json:"id"`
	Partitions int32             `json:"partitions"`
	Config     map[string]string `json:"config,omitempty"`
}

type offsetRecord struct {
	Offset   int64  `json:"offset"`
	Metadata string `json:"metadata,omitempty"`
	// At is when it was committed, unix milliseconds; 0 for one written before they
	// were kept.
	At int64 `json:"at,omitempty"`
}

// OffsetEntry is one committed (topic, partition) → offset for a group, returned
// by ListOffsets (the "fetch all offsets for a group" path).
type OffsetEntry struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
	// CommittedAt is when the offset was committed, unix milliseconds (0: not known).
	CommittedAt int64
}

func (s *boltStore) CreateTopic(name string, partitions int32, config map[string]string) (Topic, error) {
	return s.CreateTopicBeside(name, partitions, config, nil)
}

func (s *boltStore) TopicCollision(name string, beside map[string]struct{}) *CollisionError {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.names.collision(name, beside)
}

func (s *boltStore) CreateTopicBeside(name string, partitions int32, config map[string]string, beside map[string]struct{}) (Topic, error) {
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
	// Kafka's own rule: "a.b" and "a_b" would share metric names, so they may not
	// both exist. Ours, beside it: names that differ only in letter case would share
	// a directory on a case-insensitive file system.
	if c := s.TopicCollision(name, beside); c != nil {
		return Topic{}, c
	}
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
	s.names.add(name)
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
		s.names.remove(name)
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
	now := s.clock().UnixMilli()
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
			val, err := json.Marshal(offsetRecord{Offset: o.Offset, Metadata: o.Metadata, At: now})
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
		// A group with nothing left has nothing to expire: when it was empty goes too.
		if err := tx.Bucket(bucketMeta).Delete(emptySinceKey(group)); err != nil {
			return err
		}
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
			out = append(out, OffsetEntry{Topic: topic, Partition: partition, Offset: rec.Offset, Metadata: rec.Metadata, CommittedAt: rec.At})
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

// ErrPartitionsNotIncreased is returned by SetPartitions for a count that is not
// above the topic's current one.
var ErrPartitionsNotIncreased = errors.New("meta: partition count must increase")

func (s *boltStore) SetPartitions(name string, count int32) (Topic, error) {
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
		if count <= rec.Partitions {
			return ErrPartitionsNotIncreased
		}
		rec.Partitions = count
		val, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return topics.Put([]byte(name), val)
	})
	if err != nil {
		return Topic{}, err
	}
	t := Topic{Name: name, ID: rec.ID, Partitions: rec.Partitions, Config: rec.Config}
	s.mu.Lock()
	s.byName[name] = cloneTopic(t)
	s.mu.Unlock()
	return cloneTopic(t), nil
}

// GroupLiveness is what is kept about a group's members across a restart.
type GroupLiveness struct {
	// EmptySince is when the group lost its last member, in unix milliseconds; 0 for
	// a group that is Live.
	EmptySince int64
	// Live is a group that has members. Classic says one of them joined by the classic
	// protocol, and Session is the longest session timeout any of them has.
	Live    bool
	Classic bool
	Session time.Duration

	// What kind of group it is, which ListGroups and the describes show of an empty
	// group after a restart as before it (recorded: groups-listed-after-restart):
	// ProtocolType is a classic group's protocol type ("consumer", "connect", ...).
	// Consumer is a consumer-protocol group with no members, which has no EmptySince
	// (its offsets expire from their commits), and Epoch and Assignor are what it was
	// at when it emptied.
	ProtocolType string
	Consumer     bool
	Epoch        int32
	Assignor     string
}

// emptySincePrefix keys, in the meta bucket, what is kept about a group's members.
// Written by an earlier version: 8 bytes are when it lost the last one, 9 bytes are a
// group that has some (the kind of group, then its session in milliseconds). Written
// now: livenessV2, flags, empty since (8), session in milliseconds (8), epoch (4),
// then the protocol type and the assignor, each a 2-byte length and its bytes.
var emptySincePrefix = []byte("empty_since\x00")

// keyGroupsAdopted is set once AdoptGroupsWithOffsets has run.
var keyGroupsAdopted = []byte("groups_adopted")

const (
	liveClassic  byte = 'c'
	liveConsumer byte = 'n'
	livenessV2   byte = 'V'

	flagLive     byte = 1
	flagClassic  byte = 2
	flagConsumer byte = 4
)

func emptySinceKey(group string) []byte {
	return append(append([]byte(nil), emptySincePrefix...), group...)
}

func encodeGroupLiveness(st GroupLiveness) []byte {
	var flags byte
	if st.Live {
		flags |= flagLive
	}
	if st.Classic {
		flags |= flagClassic
	}
	if st.Consumer {
		flags |= flagConsumer
	}
	b := []byte{livenessV2, flags}
	b = binary.BigEndian.AppendUint64(b, uint64(st.EmptySince))
	b = binary.BigEndian.AppendUint64(b, uint64(st.Session.Milliseconds()))
	b = binary.BigEndian.AppendUint32(b, uint32(st.Epoch))
	b = binary.BigEndian.AppendUint16(b, uint16(len(st.ProtocolType)))
	b = append(b, st.ProtocolType...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(st.Assignor)))
	return append(b, st.Assignor...)
}

func decodeGroupLiveness(v []byte) (GroupLiveness, bool) {
	switch {
	case len(v) == 8:
		return GroupLiveness{EmptySince: int64(binary.BigEndian.Uint64(v))}, true
	case len(v) == 9 && (v[0] == liveClassic || v[0] == liveConsumer):
		return GroupLiveness{Live: true, Classic: v[0] == liveClassic,
			Session: time.Duration(binary.BigEndian.Uint64(v[1:])) * time.Millisecond}, true
	case len(v) >= 24 && v[0] == livenessV2:
		flags := v[1]
		st := GroupLiveness{Live: flags&flagLive != 0, Classic: flags&flagClassic != 0, Consumer: flags&flagConsumer != 0,
			EmptySince: int64(binary.BigEndian.Uint64(v[2:])),
			Session:    time.Duration(binary.BigEndian.Uint64(v[10:])) * time.Millisecond,
			Epoch:      int32(binary.BigEndian.Uint32(v[18:]))}
		rest := v[22:]
		str := func() (string, bool) {
			if len(rest) < 2 || len(rest) < 2+int(binary.BigEndian.Uint16(rest)) {
				return "", false
			}
			n := int(binary.BigEndian.Uint16(rest))
			out := string(rest[2 : 2+n])
			rest = rest[2+n:]
			return out, true
		}
		var ok1, ok2 bool
		st.ProtocolType, ok1 = str()
		st.Assignor, ok2 = str()
		return st, ok1 && ok2 && len(rest) == 0
	}
	return GroupLiveness{}, false
}

func (s *boltStore) SetGroupLiveness(group string, state *GroupLiveness) error {
	return s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if state == nil || !state.Live && state.EmptySince == 0 && !state.Consumer {
			return b.Delete(emptySinceKey(group))
		}
		return b.Put(emptySinceKey(group), encodeGroupLiveness(*state))
	})
}

func (s *boltStore) GroupLiveness() (map[string]GroupLiveness, error) {
	out := map[string]GroupLiveness{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucketMeta).Cursor()
		for k, v := c.Seek(emptySincePrefix); k != nil && bytes.HasPrefix(k, emptySincePrefix); k, v = c.Next() {
			if st, ok := decodeGroupLiveness(v); ok {
				out[string(k[len(emptySincePrefix):])] = st
			}
		}
		return nil
	})
	return out, err
}

func (s *boltStore) AdoptGroupsWithOffsets(session time.Duration) (int, error) {
	done := false
	if err := s.db.View(func(tx *bbolt.Tx) error {
		done = tx.Bucket(bucketMeta).Get(keyGroupsAdopted) != nil
		return nil
	}); err != nil || done {
		return 0, err
	}
	n := 0
	err := s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		var last []byte
		c := tx.Bucket(bucketOffsets).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			i := bytes.IndexByte(k, 0)
			if i < 0 || bytes.Equal(k[:i], last) {
				continue
			}
			last = append(last[:0], k[:i]...)
			if b.Get(emptySinceKey(string(last))) != nil {
				continue
			}
			if err := b.Put(emptySinceKey(string(last)), encodeGroupLiveness(GroupLiveness{Live: true, Classic: true, Session: session})); err != nil {
				return err
			}
			n++
		}
		return b.Put(keyGroupsAdopted, []byte{1})
	})
	return n, err
}

func (s *boltStore) ExpireOffsets(group string, expire func(topic string, partition int32, committedAt int64) bool) (int, error) {
	prefix := append([]byte(group), 0)
	now := s.clock().UnixMilli()
	var n int
	err := s.update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketOffsets)
		type change struct {
			key []byte
			val []byte // nil: delete.
		}
		var changes []change
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			rem := k[len(prefix):]
			if len(rem) < 5 {
				continue
			}
			var rec offsetRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			stamped := false
			if rec.At == 0 {
				rec.At, stamped = now, true
			}
			topic := string(rem[:len(rem)-5])
			partition := int32(binary.BigEndian.Uint32(rem[len(rem)-4:]))
			switch {
			case expire(topic, partition, rec.At):
				changes = append(changes, change{key: append([]byte(nil), k...)})
				n++
			case stamped:
				val, err := json.Marshal(rec)
				if err != nil {
					return err
				}
				changes = append(changes, change{key: append([]byte(nil), k...), val: val})
			}
		}
		for _, ch := range changes {
			if ch.val == nil {
				if err := b.Delete(ch.key); err != nil {
					return err
				}
			} else if err := b.Put(ch.key, ch.val); err != nil {
				return err
			}
		}
		return nil
	})
	return n, err
}
