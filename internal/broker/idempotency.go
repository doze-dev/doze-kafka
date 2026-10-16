package broker

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/doze-dev/doze-kafka/internal/storage/log"
)

// The idempotency tracker deduplicates an idempotent producer's retried batches.
// A producer stamps each record batch with (producerID, producerEpoch, baseSequence);
// on a timeout it resends the same batch. Without tracking, the resend appends a
// second copy — so "idempotent" would be a lie. The tracker remembers each
// producer's recent sequence numbers per partition and, on a retry, returns the
// batch's original offset instead of appending it again.
//
// State is per (partition, producerID), in memory, and rebuilt from the log the first
// time a partition is produced to after a restart (loadFrom): the log holds every batch
// with its producer id and sequence, so a batch whose acknowledgement was lost in a
// crash and is sent again is recognised rather than appended twice.

// maxTrackedBatches is how many recent batches per producer we keep for dedup. It
// matches Kafka's five-in-flight window for idempotent producers (claim: api V9).
const maxTrackedBatches = 5

// idempotency holds per-partition producer sequence state.
type idempotency struct {
	mu         sync.Mutex
	partitions map[topicPartition]*partitionProducers
}

type topicPartition struct {
	topic     string
	partition int32
}

func newIdempotency() *idempotency {
	return &idempotency{partitions: make(map[topicPartition]*partitionProducers)}
}

// forPartition returns (creating if needed) the producer state for a partition.
// Callers lock the returned value's mu across their check→append→record sequence so
// two concurrent retries can't both append.
func (i *idempotency) forPartition(topic string, partition int32) *partitionProducers {
	i.mu.Lock()
	defer i.mu.Unlock()
	tp := topicPartition{topic, partition}
	pp := i.partitions[tp]
	if pp == nil {
		pp = &partitionProducers{producers: make(map[int64]*producerSequence)}
		i.partitions[tp] = pp
	}
	return pp
}

// dropPartition forgets a partition's producer state (used on DeleteTopics).
func (i *idempotency) dropPartitions(topic string, partitions int32) {
	i.mu.Lock()
	defer i.mu.Unlock()
	for p := int32(0); p < partitions; p++ {
		delete(i.partitions, topicPartition{topic, p})
	}
}

type partitionProducers struct {
	mu        sync.Mutex
	producers map[int64]*producerSequence
	sweepAt   int  // producer count after the last idle sweep.
	loaded    bool // the log's recent history has been read in (see loadFrom).
}

// historyBytes is how much of a partition's log is read to relearn its producers after a
// restart: the last 16 MiB, by whole segments. A producer retries within its delivery
// timeout, which is minutes, so what matters is what was written just before the stop.
const historyBytes = 16 << 20

// loadFrom reads the producers' recent sequence numbers back from the log, once. The
// tracker lives in memory, and a restart used to wipe it: a producer retrying a batch
// whose acknowledgement was lost in the stop was believed new and appended twice. The
// log is the record of what was written, so the history is rebuilt from it. Callers
// hold pp.mu.
func (pp *partitionProducers) loadFrom(plog *log.Partition) error {
	if pp.loaded {
		return nil
	}
	history, err := plog.ProducerHistory(historyBytes)
	if err != nil {
		return err
	}
	for _, h := range history {
		pp.record(h.ProducerID, h.Epoch, h.FirstSeq, h.LastSeq, h.BaseOffset)
	}
	pp.loaded = true
	return nil
}

type producerSequence struct {
	epoch    int16
	recent   []batchSeq // ascending by sequence; most recent last.
	lastUsed time.Time
}

// Producer state is dropped by idle time, as Kafka does (claim: api V9), not by count. A producer
// retries a batch within its delivery timeout (two minutes by default); state
// that has been idle far longer than that can go. Evicting by count instead meant
// that after a thousand other producers had written, an early producer's retry
// was taken for a new batch and appended a second time. The hard ceiling exists
// only so memory stays bounded if a great many producers are active at once.
const (
	producerIdleExpiry       = 15 * time.Minute
	producerSweepEvery       = 1000  // sweep the idle out after this many producers accumulate.
	maxProducersPerPartition = 50000 // hard ceiling per partition.
)

type batchSeq struct {
	firstSeq, lastSeq int32
	baseOffset        int64
}

// verdict is the outcome of checking a batch against a producer's sequence history.
type verdict struct {
	kind   verdictKind
	offset int64 // for a duplicate: the batch's original base offset.
}

type verdictKind int

const (
	seqAccept       verdictKind = iota // in order; append it.
	seqDuplicate                       // already applied; return offset, don't append.
	seqDuplicateOld                    // already applied, but too long ago to say where.
	seqOutOfOrder                      // a gap or an overlap; reject.
	seqFenced                          // an older producer epoch; reject.
)

// nextSeq is the sequence that follows last. Sequences are 31-bit and wrap to 0.
func nextSeq(last int32) int32 {
	if last == math.MaxInt32 {
		return 0
	}
	return last + 1
}

// lastSeqOf is the sequence of the last record in a batch of count records
// starting at first, wrapping as nextSeq does.
func lastSeqOf(first, count int32) int32 {
	return int32((int64(first) + int64(count) - 1) & math.MaxInt32)
}

// check evaluates a batch (caller holds pp.mu). It does not mutate state; on
// seqAccept the caller appends and then calls record.
func (pp *partitionProducers) check(pid int64, epoch int16, firstSeq, lastSeq int32) verdict {
	st := pp.producers[pid]
	if st == nil {
		// A producer this broker has no memory of (new, or from before a restart):
		// accept the batch as its baseline.
		return verdict{kind: seqAccept}
	}
	if epoch < st.epoch {
		return verdict{kind: seqFenced}
	}
	if epoch > st.epoch {
		// A new epoch is a new session, and a session starts at sequence 0.
		if firstSeq != 0 {
			return verdict{kind: seqOutOfOrder}
		}
		return verdict{kind: seqAccept}
	}
	n := len(st.recent)
	if n == 0 || firstSeq == nextSeq(st.recent[n-1].lastSeq) {
		return verdict{kind: seqAccept}
	}
	// Not the next batch. An exact repeat of a remembered batch is a retry: answer
	// with the offset it was given. Something older than everything remembered was
	// applied long ago, but where is no longer known. Anything else — a gap, or a
	// batch that overlaps what is already there — is out of order. It used to be
	// answered as a successful duplicate, silently dropping records the producer
	// believed were written.
	for _, b := range st.recent {
		if b.firstSeq == firstSeq && b.lastSeq == lastSeq {
			return verdict{kind: seqDuplicate, offset: b.baseOffset}
		}
	}
	if lastSeq < st.recent[0].firstSeq && firstSeq <= lastSeq {
		return verdict{kind: seqDuplicateOld}
	}
	return verdict{kind: seqOutOfOrder}
}

// record remembers an accepted batch's sequence range and assigned offset (caller
// holds pp.mu).
func (pp *partitionProducers) record(pid int64, epoch int16, firstSeq, lastSeq int32, baseOffset int64) {
	st := pp.producers[pid]
	if st == nil || epoch > st.epoch {
		st = &producerSequence{epoch: epoch}
		pp.producers[pid] = st
	}
	st.recent = append(st.recent, batchSeq{firstSeq, lastSeq, baseOffset})
	if len(st.recent) > maxTrackedBatches {
		st.recent = st.recent[len(st.recent)-maxTrackedBatches:]
	}
	st.lastUsed = now()
	if len(pp.producers) >= pp.sweepAt+producerSweepEvery || len(pp.producers) > maxProducersPerPartition {
		pp.expireIdle(pid)
		pp.sweepAt = len(pp.producers)
	}
}

// now is the clock for producer expiry (a variable so tests can move it).
var now = time.Now

// expireIdle drops producers idle longer than producerIdleExpiry and, if the
// partition is still over its ceiling, the least recently used beyond it.
func (pp *partitionProducers) expireIdle(keep int64) {
	cutoff := now().Add(-producerIdleExpiry)
	for id, st := range pp.producers {
		if id != keep && st.lastUsed.Before(cutoff) {
			delete(pp.producers, id)
		}
	}
	if over := len(pp.producers) - maxProducersPerPartition; over > 0 {
		// Still over the ceiling with everyone recently active: drop the least
		// recently used, a tenth of the ceiling at a time so this pass is rare.
		pp.evictOldest(keep, over+maxProducersPerPartition/10)
	}
}

// evictOldest drops the n least recently used producers other than keep.
func (pp *partitionProducers) evictOldest(keep int64, n int) {
	type entry struct {
		id int64
		at time.Time
	}
	all := make([]entry, 0, len(pp.producers))
	for id, st := range pp.producers {
		if id != keep {
			all = append(all, entry{id, st.lastUsed})
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.Before(all[j].at) })
	if n > len(all) {
		n = len(all)
	}
	for _, e := range all[:n] {
		delete(pp.producers, e.id)
	}
}

// producerView is one producer's state on a partition, for DescribeProducers.
type producerView struct {
	pid        int64
	epoch      int16
	lastSeq    int32
	lastUsedMs int64
}

// snapshot lists the producers the tracker remembers on a partition.
func (i *idempotency) snapshot(topic string, partition int32) []producerView {
	i.mu.Lock()
	pp := i.partitions[topicPartition{topic, partition}]
	i.mu.Unlock()
	if pp == nil {
		return nil
	}
	pp.mu.Lock()
	defer pp.mu.Unlock()
	out := make([]producerView, 0, len(pp.producers))
	for pid, st := range pp.producers {
		v := producerView{pid: pid, epoch: st.epoch, lastSeq: -1, lastUsedMs: st.lastUsed.UnixMilli()}
		if n := len(st.recent); n > 0 {
			v.lastSeq = st.recent[n-1].lastSeq
		}
		out = append(out, v)
	}
	return out
}
