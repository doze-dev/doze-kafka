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

	"go.etcd.io/bbolt"
)

// ErrTopicExists is returned by CreateTopic when the name is already registered.
var ErrTopicExists = errors.New("meta: topic already exists")

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

	CommitOffset(group, topic string, partition int32, offset int64, metadata string) error
	FetchOffset(group, topic string, partition int32) (offset int64, ok bool, err error)
	ListOffsets(group string) ([]OffsetEntry, error)

	NextProducerID() (int64, error)

	Close() error
}

// Open opens (creating if needed) the metadata database at path and ensures its
// buckets exist.
func Open(path string) (Store, error) {
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
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
	return &boltStore{db: db}, nil
}

type boltStore struct{ db *bbolt.DB }

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
	err := s.db.Update(func(tx *bbolt.Tx) error {
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
	return Topic{Name: name, ID: id, Partitions: partitions, Config: config}, nil
}

func (s *boltStore) Topic(name string) (Topic, bool, error) {
	var (
		topic Topic
		found bool
	)
	err := s.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bucketTopics).Get([]byte(name))
		if v == nil {
			return nil
		}
		var rec topicRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			return err
		}
		topic = Topic{Name: name, ID: rec.ID, Partitions: rec.Partitions, Config: rec.Config}
		found = true
		return nil
	})
	return topic, found, err
}

func (s *boltStore) TopicByID(id TopicID) (Topic, bool, error) {
	var name string
	err := s.db.View(func(tx *bbolt.Tx) error {
		if v := tx.Bucket(bucketTopicsByID).Get(id[:]); v != nil {
			name = string(v)
		}
		return nil
	})
	if err != nil || name == "" {
		return Topic{}, false, err
	}
	return s.Topic(name)
}

func (s *boltStore) ListTopics() ([]Topic, error) {
	var topics []Topic
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketTopics).ForEach(func(k, v []byte) error {
			var rec topicRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			topics = append(topics, Topic{Name: string(k), ID: rec.ID, Partitions: rec.Partitions, Config: rec.Config})
			return nil
		})
	})
	return topics, err
}

func (s *boltStore) DeleteTopic(name string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
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
		return tx.Bucket(bucketTopicsByID).Delete(rec.ID[:])
	})
}

func (s *boltStore) CommitOffset(group, topic string, partition int32, offset int64, metadata string) error {
	val, err := json.Marshal(offsetRecord{Offset: offset, Metadata: metadata})
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketOffsets).Put(offsetKey(group, topic, partition), val)
	})
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

func (s *boltStore) NextProducerID() (int64, error) {
	var id int64
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		var next int64
		if v := b.Get(keyProducerID); len(v) == 8 {
			next = int64(binary.BigEndian.Uint64(v))
		}
		id = next
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(next+1))
		return b.Put(keyProducerID, buf[:])
	})
	return id, err
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
