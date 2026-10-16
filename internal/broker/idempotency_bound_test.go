package broker

import (
	"math"
	"testing"
	"time"
)

// stepClock replaces the producer-expiry clock with one that advances a
// millisecond per reading, so "least recently used" is well defined in a tight loop.
func stepClock(t *testing.T) (advance func(time.Duration)) {
	t.Helper()
	old := now
	at := time.Unix(1_700_000_000, 0)
	now = func() time.Time { at = at.Add(time.Millisecond); return at }
	t.Cleanup(func() { now = old })
	return func(d time.Duration) { at = at.Add(d) }
}

func newProducers() *partitionProducers {
	return &partitionProducers{producers: map[int64]*producerSequence{}}
}

// A retried batch is recognised however many other producers have written since.
// State used to be evicted by count (the 1,000 most recent producers), so after a
// thousand others an early producer's retry was appended a second time.
func TestRetryIsRecognisedAfterManyOtherProducers(t *testing.T) {
	stepClock(t)
	pp := newProducers()
	pp.record(7, 0, 0, 0, 42, 0)
	for pid := int64(1000); pid < 4000; pid++ {
		pp.record(pid, 0, 0, 0, pid, 0)
	}
	if v := pp.check(7, 0, 0, 0); v.kind != seqDuplicate || v.offset != 42 {
		t.Fatalf("producer 7's retry after 3000 other producers: verdict %+v, want a duplicate at offset 42", v)
	}
}

// State goes when it has been idle for longer than any retry could take, and the
// hard ceiling holds even if nothing is idle.
func TestProducerStateExpiresByIdleTimeAndIsBounded(t *testing.T) {
	advance := stepClock(t)
	pp := newProducers()
	pp.record(1, 0, 0, 0, 1, 0)
	advance(producerIdleExpiry + time.Minute)
	for pid := int64(2); pid < 2+producerSweepEvery+1; pid++ {
		pp.record(pid, 0, 0, 0, pid, 0)
	}
	if _, ok := pp.producers[1]; ok {
		t.Error("a producer idle past the expiry was kept through a sweep")
	}
	if _, ok := pp.producers[2]; !ok {
		t.Error("a producer that wrote after the idle one was swept with it")
	}

	pp = newProducers()
	const n = maxProducersPerPartition + 250
	for pid := int64(1); pid <= n; pid++ {
		pp.record(pid, 0, 0, 0, pid, 0)
	}
	if got := len(pp.producers); got > maxProducersPerPartition {
		t.Fatalf("tracking %d producers, want at most %d", got, maxProducersPerPartition)
	}
	if _, ok := pp.producers[n]; !ok {
		t.Error("the most recent producer must be retained")
	}
	if _, ok := pp.producers[1]; ok {
		t.Error("with everything recent, the least recently used should be the one dropped")
	}
}

func TestSequenceVerdicts(t *testing.T) {
	stepClock(t)
	pp := newProducers()
	// Batches seq 0-4 and 5-9 at offsets 100 and 105.
	pp.record(1, 0, 0, 4, 100, 0)
	pp.record(1, 0, 5, 9, 105, 0)

	for _, tc := range []struct {
		name        string
		epoch       int16
		first, last int32
		want        verdictKind
		offset      int64
	}{
		{"the next batch", 0, 10, 12, seqAccept, 0},
		{"an exact retry", 0, 5, 9, seqDuplicate, 105},
		{"a gap", 0, 12, 13, seqOutOfOrder, 0},
		// Two new records hidden behind three old ones: accepting this as a
		// "duplicate" dropped the new ones and reported success.
		{"an overlap with new records", 0, 7, 11, seqOutOfOrder, 0},
		{"a different batch over old sequences", 0, 3, 6, seqOutOfOrder, 0},
		{"an older epoch", -1, 10, 10, seqFenced, 0},
		{"a new epoch starting at 0", 1, 0, 2, seqAccept, 0},
		{"a new epoch not starting at 0", 1, 40, 41, seqOutOfOrder, 0},
	} {
		if v := pp.check(1, tc.epoch, tc.first, tc.last); v.kind != tc.want || (tc.want == seqDuplicate && v.offset != tc.offset) {
			t.Errorf("%s (seq %d-%d, epoch %d): verdict %+v, want kind %d", tc.name, tc.first, tc.last, tc.epoch, v, tc.want)
		}
	}

	// Older than everything remembered: applied, but where is no longer known.
	for i := int32(2); i < 10; i++ {
		pp.record(1, 0, i*5, i*5+4, int64(100+i*5), 0)
	}
	if v := pp.check(1, 0, 0, 4); v.kind != seqDuplicateOld {
		t.Errorf("a retry older than the remembered window: verdict %+v, want seqDuplicateOld", v)
	}
}

// Sequence numbers are 31-bit and wrap to 0; the batch after MaxInt32 is in order.
func TestSequenceWrapsToZero(t *testing.T) {
	stepClock(t)
	pp := newProducers()
	pp.record(1, 0, math.MaxInt32-1, math.MaxInt32, 10, 0)
	if v := pp.check(1, 0, 0, 1); v.kind != seqAccept {
		t.Errorf("sequence 0 after MaxInt32: verdict %+v, want accept", v)
	}
	if got := lastSeqOf(math.MaxInt32-1, 4); got != 1 {
		t.Errorf("a 4-record batch starting at MaxInt32-1 ends at sequence %d, want 1", got)
	}
	pp.record(1, 0, math.MaxInt32-1, lastSeqOf(math.MaxInt32-1, 4), 20, 0)
	if v := pp.check(1, 0, 2, 2); v.kind != seqAccept {
		t.Errorf("the batch after one that spanned the wrap: verdict %+v, want accept", v)
	}
}
