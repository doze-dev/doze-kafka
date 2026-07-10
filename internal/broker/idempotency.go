package broker

import "sync"

// The idempotency tracker deduplicates an idempotent producer's retried batches.
// A producer stamps each record batch with (producerID, producerEpoch, baseSequence);
// on a timeout it resends the same batch. Without tracking, the resend appends a
// second copy — so "idempotent" would be a lie. The tracker remembers each
// producer's recent sequence numbers per partition and, on a retry, returns the
// batch's original offset instead of appending it again.
//
// State is per (partition, producerID), in memory. It survives for the life of the
// broker process; it is not persisted, so a batch that was in flight exactly across
// a broker restart is the one residual gap (and durable acks make even that rare —
// an acked batch was fsynced, so the producer won't resend it). See
// docs/limitations.md.

// maxTrackedBatches is how many recent batches per producer we keep for dedup. It
// matches Kafka's five-in-flight window for idempotent producers.
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
}

type producerSequence struct {
	epoch  int16
	recent []batchSeq // ascending by sequence; most recent last.
}

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
	seqAccept     verdictKind = iota // in order; append it.
	seqDuplicate                     // already applied; return offset, don't append.
	seqOutOfOrder                    // a gap; reject.
	seqFenced                        // an older producer epoch; reject.
)

// check evaluates a batch (caller holds pp.mu). It does not mutate state; on
// seqAccept the caller appends and then calls record.
func (pp *partitionProducers) check(pid int64, epoch int16, firstSeq, lastSeq int32) verdict {
	st := pp.producers[pid]
	if st == nil || epoch > st.epoch {
		// A producer we've never seen, or one that bumped its epoch (a new session):
		// accept this batch as the new baseline.
		return verdict{kind: seqAccept}
	}
	if epoch < st.epoch {
		return verdict{kind: seqFenced}
	}
	// Same epoch: compare against the last accepted sequence.
	expected := int32(0)
	if n := len(st.recent); n > 0 {
		expected = st.recent[n-1].lastSeq + 1
	}
	switch {
	case firstSeq == expected:
		return verdict{kind: seqAccept}
	case firstSeq < expected:
		// A retry of an already-applied batch. Return its original offset if we
		// still remember it; otherwise it's too old to place.
		for _, b := range st.recent {
			if b.firstSeq == firstSeq && b.lastSeq == lastSeq {
				return verdict{kind: seqDuplicate, offset: b.baseOffset}
			}
		}
		return verdict{kind: seqDuplicate, offset: -1}
	default: // firstSeq > expected
		return verdict{kind: seqOutOfOrder}
	}
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
}
