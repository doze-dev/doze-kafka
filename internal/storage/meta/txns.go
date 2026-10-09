package meta

import (
	"encoding/json"

	"go.etcd.io/bbolt"
)

// TxnRecord is the coordinator's durable state for one transactional.id: which
// producer id and epoch own it, where its transaction stands, and what it has touched
// so far. The partitions and offsets are what ending the transaction acts on.
type TxnRecord struct {
	ID         string         `json:"id"`
	ProducerID int64          `json:"producer_id"`
	Epoch      int16          `json:"epoch"`
	State      string         `json:"state"` // Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit or CompleteAbort.
	TimeoutMs  int32          `json:"timeout_ms"`
	Partitions []TxnPartition `json:"partitions,omitempty"`
	Groups     []string       `json:"groups,omitempty"`
	Offsets    []TxnOffset    `json:"offsets,omitempty"` // committed when the transaction commits.
	StartedMs  int64          `json:"started_ms,omitempty"`
	UpdatedMs  int64          `json:"updated_ms"`
}

// TxnPartition is a topic-partition a transaction has written to.
type TxnPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// TxnOffset is a consumer offset a transaction will commit with it.
type TxnOffset struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Metadata  string `json:"metadata,omitempty"`
}

func (s *boltStore) PutTxn(rec TxnRecord) error {
	val, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.update(func(tx *bbolt.Tx) error { return tx.Bucket(bucketTxns).Put([]byte(rec.ID), val) })
}

func (s *boltStore) ListTxns() ([]TxnRecord, error) {
	var out []TxnRecord
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketTxns).ForEach(func(_, v []byte) error {
			var r TxnRecord
			if err := json.Unmarshal(v, &r); err != nil {
				return err
			}
			out = append(out, r)
			return nil
		})
	})
	return out, err
}
