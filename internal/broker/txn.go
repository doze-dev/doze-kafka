package broker

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The transaction coordinator: the classic protocol (before KIP-890), which is what
// current Java, librdkafka and franz-go clients negotiate when the broker offers
// Produce no newer than v9.
//
// One entry per transactional.id holds the producer id and epoch that own it and where
// its transaction stands. The state is written to the metadata store BEFORE the broker
// acts on it, so a crash in the middle of ending a transaction is finished on the next
// start:
//
//	Empty            no transaction in progress
//	Ongoing          partitions and offsets are being added to one
//	PrepareCommit    the decision is made; markers are being written
//	PrepareAbort     (same, for an abort)
//	CompleteCommit   the markers are written and the offsets applied
//	CompleteAbort    the markers are written
//
// Ending a transaction appends a commit or abort marker to every partition it wrote to
// (the log skips a partition the producer has no open transaction on, so repeating this
// after a crash writes nothing twice), applies the consumer offsets it carried if it
// committed, and records the Complete state.
//
// Locking: each entry has a read-write lock. Producing takes it for reading across the
// check and the append, so a transaction cannot be ended between the two and leave a
// record behind its own marker; everything that changes the entry takes it for writing.
// The order is always entry, then idempotency state, then the partition.

const (
	txnEmpty          = "Empty"
	txnOngoing        = "Ongoing"
	txnPrepareCommit  = "PrepareCommit"
	txnPrepareAbort   = "PrepareAbort"
	txnCompleteCommit = "CompleteCommit"
	txnCompleteAbort  = "CompleteAbort"

	// maxTxnTimeoutMs is the longest transaction.timeout.ms a producer may ask for,
	// Kafka's transaction.max.timeout.ms.
	maxTxnTimeoutMs = 15 * 60 * 1000

	// txnCheckEvery is how often open transactions are checked against their timeout.
	txnCheckEvery = 500 * time.Millisecond
)

type txnEntry struct {
	mu  sync.RWMutex
	rec meta.TxnRecord
}

type txnManager struct {
	mu      sync.Mutex
	entries map[string]*txnEntry
}

func newTxnManager(recs []meta.TxnRecord) *txnManager {
	m := &txnManager{entries: make(map[string]*txnEntry, len(recs))}
	for _, r := range recs {
		m.entries[r.ID] = &txnEntry{rec: r}
	}
	return m
}

func (m *txnManager) get(id string) *txnEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entries[id]
}

func (m *txnManager) getOrCreate(id string) *txnEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[id]
	if e == nil {
		e = &txnEntry{rec: meta.TxnRecord{ID: id}}
		m.entries[id] = e
	}
	return e
}

func (m *txnManager) all() []*txnEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*txnEntry, 0, len(m.entries))
	for _, e := range m.entries {
		out = append(out, e)
	}
	return out
}

func nowMs() int64 { return time.Now().UnixMilli() }

// persist writes the entry's state. Callers hold e.mu for writing.
func (b *Broker) persistTxn(e *txnEntry) int16 {
	e.rec.UpdatedMs = nowMs()
	if err := b.meta.PutTxn(e.rec); err != nil {
		b.log.Error("transactions: could not store the state of a transaction", "transactional_id", e.rec.ID, "err", err)
		return kerr.CoordinatorNotAvailable.Code
	}
	return 0
}

// owns checks that a request's producer id and epoch are the ones that own the
// transactional id. A lower epoch has been fenced by a newer producer.
func (e *txnEntry) owns(pid int64, epoch int16) int16 {
	switch {
	case e.rec.State == "":
		return kerr.InvalidProducerIDMapping.Code
	case pid != e.rec.ProducerID:
		return kerr.InvalidProducerIDMapping.Code
	case epoch < e.rec.Epoch:
		return kerr.ProducerFenced.Code
	case epoch > e.rec.Epoch:
		return kerr.InvalidProducerEpoch.Code
	}
	return 0
}

// canAdd reports whether partitions or offsets may be added now: not while the
// transaction is being ended.
func (e *txnEntry) canAdd() int16 {
	switch e.rec.State {
	case txnPrepareCommit, txnPrepareAbort:
		return kerr.ConcurrentTransactions.Code
	}
	return 0
}

// begin moves a settled entry to Ongoing, starting a transaction.
func (e *txnEntry) begin() {
	if e.rec.State != txnOngoing {
		e.rec.State = txnOngoing
		e.rec.StartedMs = nowMs()
		e.rec.Partitions, e.rec.Groups, e.rec.Offsets = nil, nil, nil
	}
}

// initTxnProducer is InitProducerId for a transactional id: it makes the caller the
// owner, fencing whoever held the id before and deciding what was left open.
func (b *Broker) initTxnProducer(id string, timeoutMs int32) (pid int64, epoch int16, code int16) {
	if timeoutMs <= 0 || timeoutMs > maxTxnTimeoutMs {
		return -1, -1, kerr.InvalidTransactionTimeout.Code
	}
	e := b.txns.getOrCreate(id)
	e.mu.Lock()
	defer e.mu.Unlock()

	switch e.rec.State {
	case "":
		first, err := b.nextProducerID()
		if err != nil {
			b.log.Error("transactions: could not allocate a producer id", "err", err)
			return -1, -1, kerr.CoordinatorNotAvailable.Code
		}
		e.rec.ProducerID, e.rec.Epoch = first, -1 // bumped to 0 below.
	case txnOngoing, txnPrepareAbort:
		// The previous owner's transaction is aborted; its markers carry the new epoch.
		if c := b.abortFencing(e); c != 0 {
			return -1, -1, c
		}
	case txnPrepareCommit:
		if c := b.finishTxn(e, true); c != 0 {
			return -1, -1, c
		}
	}
	if e.rec.Epoch >= math.MaxInt16-1 {
		// Epochs are exhausted: a new producer id, and epoch 0 again.
		first, err := b.nextProducerID()
		if err != nil {
			return -1, -1, kerr.CoordinatorNotAvailable.Code
		}
		e.rec.ProducerID, e.rec.Epoch = first, -1
	}
	e.rec.Epoch++
	e.rec.State, e.rec.TimeoutMs = txnEmpty, timeoutMs
	e.rec.Partitions, e.rec.Groups, e.rec.Offsets = nil, nil, nil
	if c := b.persistTxn(e); c != 0 {
		return -1, -1, c
	}
	return e.rec.ProducerID, e.rec.Epoch, 0
}

// abortFencing aborts an open transaction and fences its producer by raising the epoch
// first, so the markers (and whatever the old producer tries next) carry the new one.
// Callers hold e.mu for writing.
func (b *Broker) abortFencing(e *txnEntry) int16 {
	if e.rec.Epoch < math.MaxInt16 {
		e.rec.Epoch++
	}
	e.rec.State = txnPrepareAbort
	if c := b.persistTxn(e); c != 0 {
		return c
	}
	return b.finishTxn(e, false)
}

// finishTxn writes the markers for a transaction whose decision is already stored
// (PrepareCommit or PrepareAbort), applies a commit's offsets, and records the
// Complete state. Safe to repeat. Callers hold e.mu for writing.
func (b *Broker) finishTxn(e *txnEntry, commit bool) int16 {
	now := nowMs()
	var written []meta.TxnPartition
	for _, tp := range e.rec.Partitions {
		plog, err := b.logs.ExistingPartition(tp.Topic, tp.Partition)
		if errors.Is(err, log.ErrNoSuchPartition) {
			continue // the topic was deleted: there is nothing to end there.
		}
		if err != nil {
			b.log.Error("transactions: could not open a partition to end a transaction", "transactional_id", e.rec.ID, "topic", tp.Topic, "partition", tp.Partition, "err", err)
			return kerr.CoordinatorNotAvailable.Code
		}
		_, wrote, err := plog.AppendMarker(e.rec.ProducerID, e.rec.Epoch, commit, 0, now)
		if err != nil {
			b.log.Error("transactions: could not write a transaction marker", "transactional_id", e.rec.ID, "topic", tp.Topic, "partition", tp.Partition, "err", err)
			return kerr.CoordinatorNotAvailable.Code
		}
		if wrote {
			written = append(written, tp)
		}
	}
	// A marker is the decision readers act on, so it is on disk before the transaction
	// is called complete.
	for _, tp := range written {
		if err := b.logs.SyncPartition(tp.Topic, tp.Partition); err != nil {
			b.log.Error("transactions: could not flush a transaction marker", "transactional_id", e.rec.ID, "topic", tp.Topic, "partition", tp.Partition, "err", err)
			return kerr.CoordinatorNotAvailable.Code
		}
		b.produce.signal(tp.Topic) // fetchers waiting on the last stable offset can go.
	}
	if commit {
		byGroup := map[string][]meta.OffsetCommit{}
		for _, o := range e.rec.Offsets {
			byGroup[o.Group] = append(byGroup[o.Group], meta.OffsetCommit{Topic: o.Topic, Partition: o.Partition, Offset: o.Offset, Metadata: o.Metadata})
		}
		for group, commits := range byGroup {
			if err := b.meta.CommitOffsets(group, commits); err != nil {
				b.log.Error("transactions: could not apply a transaction's offsets", "transactional_id", e.rec.ID, "group", group, "err", err)
				return kerr.CoordinatorNotAvailable.Code
			}
		}
		e.rec.State = txnCompleteCommit
	} else {
		e.rec.State = txnCompleteAbort
	}
	e.rec.Partitions, e.rec.Groups, e.rec.Offsets = nil, nil, nil
	return b.persistTxn(e)
}

// endTxn is EndTxn: the producer's decision to commit or abort.
func (b *Broker) endTxn(id string, pid int64, epoch int16, commit bool) int16 {
	e := b.txns.get(id)
	if e == nil {
		return kerr.InvalidProducerIDMapping.Code
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if c := e.owns(pid, epoch); c != 0 {
		return c
	}
	switch e.rec.State {
	case txnOngoing:
		e.rec.State = txnPrepareAbort
		if commit {
			e.rec.State = txnPrepareCommit
		}
		if c := b.persistTxn(e); c != 0 {
			return c
		}
		return b.finishTxn(e, commit)
	case txnPrepareCommit, txnPrepareAbort:
		// A repeat of an end that did not finish; the decision stands.
		return b.finishTxn(e, e.rec.State == txnPrepareCommit)
	case txnCompleteCommit:
		if commit {
			return 0 // a retry of an end that succeeded.
		}
	case txnCompleteAbort:
		if !commit {
			return 0
		}
	}
	return kerr.InvalidTxnState.Code
}

// RecoverTransactions finishes what a stop left half done: a transaction whose decision
// was stored is carried out, and one still open belonged to a producer that is gone, so
// it is aborted and the id fenced.
func (b *Broker) RecoverTransactions() {
	for _, e := range b.txns.all() {
		e.mu.Lock()
		var code int16
		switch e.rec.State {
		case txnPrepareCommit:
			code = b.finishTxn(e, true)
		case txnPrepareAbort:
			code = b.finishTxn(e, false)
		case txnOngoing:
			code = b.abortFencing(e)
		}
		if code != 0 {
			b.log.Error("transactions: could not finish a transaction left from before the restart", "transactional_id", e.rec.ID, "code", code)
		} else if s := e.rec.State; s == txnCompleteCommit || s == txnCompleteAbort {
			b.log.Debug("transactions: finished after a restart", "transactional_id", e.rec.ID, "state", s)
		}
		e.mu.Unlock()
	}
}

// RunTransactions aborts transactions that outlive their timeout, until ctx ends. The
// abort raises the epoch, so a producer that was only slow finds itself fenced rather
// than writing into a transaction that is already decided.
func (b *Broker) RunTransactions(ctx context.Context) {
	t := time.NewTicker(txnCheckEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := nowMs()
		for _, e := range b.txns.all() {
			e.mu.RLock()
			expired := e.rec.State == txnOngoing && now-e.rec.UpdatedMs > int64(e.rec.TimeoutMs)
			e.mu.RUnlock()
			if !expired {
				continue
			}
			e.mu.Lock()
			if e.rec.State == txnOngoing && nowMs()-e.rec.UpdatedMs > int64(e.rec.TimeoutMs) {
				b.log.Info("transactions: aborting a transaction that timed out", "transactional_id", e.rec.ID, "timeout_ms", e.rec.TimeoutMs)
				if code := b.abortFencing(e); code != 0 {
					b.log.Error("transactions: could not abort a timed-out transaction", "transactional_id", e.rec.ID, "code", code)
				}
			}
			e.mu.Unlock()
		}
	}
}

// lockForProduce is the check a transactional batch has to pass, held across its
// append. The producer must own the transactional id, a transaction must be open, and
// the partition must have been added to it; otherwise the batch would sit in the log
// with no marker ever coming for it, holding the last stable offset still for ever.
// The returned func releases the lock.
func (b *Broker) lockForProduce(txnID *string, h log.BatchHeader, topic string, partition int32) (unlock func(), code int16, msg string) {
	if txnID == nil || *txnID == "" {
		return nil, kerr.InvalidTxnState.Code, "a transactional batch must come in a produce request that names the transactional id"
	}
	e := b.txns.get(*txnID)
	if e == nil {
		return nil, kerr.InvalidProducerIDMapping.Code, "no producer owns that transactional id"
	}
	e.mu.RLock()
	if c := e.owns(h.ProducerID, h.ProducerEpoch); c != 0 {
		e.mu.RUnlock()
		if c == kerr.ProducerFenced.Code {
			c = kerr.InvalidProducerEpoch.Code // what a partition reports; the client treats both as fenced.
		}
		return nil, c, "the producer id and epoch are not the ones that own the transactional id"
	}
	if e.rec.State == txnOngoing {
		for _, p := range e.rec.Partitions {
			if p.Topic == topic && p.Partition == partition {
				return e.mu.RUnlock, 0, ""
			}
		}
	}
	e.mu.RUnlock()
	return nil, kerr.InvalidTxnState.Code, "the partition was not added to an open transaction"
}
