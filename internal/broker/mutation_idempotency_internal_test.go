package broker

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"
)

// The producer tracker's edges: what is a retry, what is too old to place, how many
// batches and producers are kept, and when idle ones go.

func newPP() *partitionProducers {
	return &partitionProducers{producers: map[int64]*producerSequence{}}
}

// at moves the tracker's clock to t for the rest of the test.
func at(t *testing.T, tm time.Time) {
	t.Helper()
	old := now
	now = func() time.Time { return tm }
	t.Cleanup(func() { now = old })
}

func TestCheckPlacesABatchBySequence(t *testing.T) {
	pp := newPP()
	// One producer, epoch 2, which has been given sequences 10-14 and then 15-19.
	pp.record(1, 2, 10, 14, 100, 1000)
	pp.record(1, 2, 15, 19, 105, 2000)
	for _, tc := range []struct {
		name            string
		pid             int64
		epoch           int16
		first, last     int32
		kind            verdictKind
		offset, timeMs  int64
		offsetTimeCheck bool
	}{
		{"an unknown producer starts anywhere", 9, 0, 40, 44, seqAccept, 0, 0, false},
		{"the next sequence", 1, 2, 20, 24, seqAccept, 0, 0, false},
		{"an exact repeat of the first", 1, 2, 10, 14, seqDuplicate, 100, 1000, true},
		{"an exact repeat of the last", 1, 2, 15, 19, seqDuplicate, 105, 2000, true},
		{"older than everything remembered", 1, 2, 0, 4, seqDuplicateOld, 0, 0, false},
		{"older, a single record", 1, 2, 9, 9, seqDuplicateOld, 0, 0, false},
		{"older but touching the oldest remembered", 1, 2, 8, 10, seqOutOfOrder, 0, 0, false},
		{"older and overlapping", 1, 2, 12, 14, seqOutOfOrder, 0, 0, false},
		{"a wrapped range is not older", 1, 2, 5, 2, seqOutOfOrder, 0, 0, false},
		{"a gap", 1, 2, 22, 24, seqOutOfOrder, 0, 0, false},
		{"an older epoch is fenced", 1, 1, 20, 24, seqFenced, 0, 0, false},
		{"a newer epoch starts at 0", 1, 3, 0, 4, seqAccept, 0, 0, false},
		{"a newer epoch cannot start elsewhere", 1, 3, 20, 24, seqOutOfOrder, 0, 0, false},
	} {
		v := pp.check(tc.pid, tc.epoch, tc.first, tc.last)
		if v.kind != tc.kind {
			t.Errorf("%s: verdict %d, want %d", tc.name, v.kind, tc.kind)
		}
		if tc.offsetTimeCheck && (v.offset != tc.offset || v.timestamp != tc.timeMs) {
			t.Errorf("%s: answered offset %d time %d, want %d and %d", tc.name, v.offset, v.timestamp, tc.offset, tc.timeMs)
		}
	}
	// A sequence wraps at 31 bits: after the largest comes 0.
	wrap := newPP()
	wrap.record(1, 0, math.MaxInt32-1, 2, 0, 0)
	if v := wrap.check(1, 0, 3, 5); v.kind != seqAccept {
		t.Errorf("the sequence after a wrapped batch: verdict %d, want accept", v.kind)
	}
}

func TestRecordKeepsFiveBatchesAndAnEpochIsASession(t *testing.T) {
	pp := newPP()
	for i := int32(0); i < 5; i++ {
		pp.record(1, 0, i*10, i*10+9, int64(i)*10, 0)
	}
	if got := len(pp.producers[1].recent); got != maxTrackedBatches {
		t.Fatalf("%d batches kept after %d, want all of them", got, maxTrackedBatches)
	}
	if v := pp.check(1, 0, 0, 9); v.kind != seqDuplicate {
		t.Errorf("the oldest of five batches: verdict %d, want duplicate", v.kind)
	}
	pp.record(1, 0, 50, 59, 50, 0)
	if got := len(pp.producers[1].recent); got != maxTrackedBatches {
		t.Fatalf("%d batches kept after six, want %d", got, maxTrackedBatches)
	}
	if v := pp.check(1, 0, 0, 9); v.kind != seqDuplicateOld {
		t.Errorf("the batch pushed out by a sixth: verdict %d, want too old to place", v.kind)
	}
	// The same epoch keeps what it has; a newer one starts again.
	pp.record(1, 0, 60, 69, 60, 0)
	if pp.producers[1].epoch != 0 || len(pp.producers[1].recent) != maxTrackedBatches {
		t.Errorf("the same epoch reset the history: epoch %d, %d batches", pp.producers[1].epoch, len(pp.producers[1].recent))
	}
	pp.record(1, 1, 0, 4, 70, 0)
	if st := pp.producers[1]; st.epoch != 1 || len(st.recent) != 1 {
		t.Errorf("a new epoch: epoch %d with %d batches, want 1 with 1", st.epoch, len(st.recent))
	}
}

func producerCount(pp *partitionProducers) int { return len(pp.producers) }

// Idle producers are swept out once producerSweepEvery have accumulated since the
// last sweep, and only then.
func TestIdleProducersAreSweptEveryThousand(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at(t, start)
	pp := newPP()
	for i := int64(0); i < producerSweepEvery-1; i++ {
		pp.record(i, 0, 0, 0, 0, 0)
	}
	if got := producerCount(pp); got != producerSweepEvery-1 {
		t.Fatalf("%d producers, want %d", got, producerSweepEvery-1)
	}
	// All of them have been idle for longer than the expiry when the thousandth arrives.
	at(t, start.Add(producerIdleExpiry+time.Minute))
	pp.record(int64(producerSweepEvery), 0, 0, 0, 0, 0)
	if got := producerCount(pp); got != 1 {
		t.Fatalf("%d producers after the thousandth, want only it", got)
	}
	if pp.sweepAt != 1 {
		t.Errorf("sweepAt %d, want the count after the sweep, 1", pp.sweepAt)
	}
	// The next sweep is a thousand producers away, not at the next record.
	at(t, start.Add(2*(producerIdleExpiry+time.Minute)))
	pp.record(-1, 0, 0, 0, 0, 0)
	if got := producerCount(pp); got != 2 {
		t.Errorf("%d producers, want 2: an idle one was swept before a thousand had accumulated", got)
	}
}

// The ceiling is a backstop for a partition whose producers are all active: past
// maxProducersPerPartition the idle go and then the least recently used.
func TestProducerCeilingEvictsIdleThenLeastRecentlyUsed(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at(t, start.Add(time.Hour))
	idle := start // an hour before now.
	fill := func(n int, lastUsed time.Time) *partitionProducers {
		pp := newPP()
		pp.sweepAt = 1 << 30 // the periodic sweep is not due.
		for i := 0; i < n; i++ {
			pp.producers[int64(i)] = &producerSequence{lastUsed: lastUsed}
		}
		return pp
	}

	// Exactly at the ceiling nothing is swept; one over it, the idle go.
	pp := fill(maxProducersPerPartition-1, idle)
	pp.record(-1, 0, 0, 0, 0, 0)
	if got := producerCount(pp); got != maxProducersPerPartition {
		t.Errorf("at the ceiling: %d producers, want %d with nothing swept", got, maxProducersPerPartition)
	}
	pp.record(-2, 0, 0, 0, 0, 0)
	if got := producerCount(pp); got != 2 {
		t.Errorf("over the ceiling: %d producers, want the 2 recent ones", got)
	}

	// All active: exactly at the ceiling nothing goes; over it, the excess and a
	// tenth of the ceiling more, the least recently used first.
	recent := start.Add(time.Hour)
	pp = fill(maxProducersPerPartition, recent)
	pp.expireIdle(0)
	if got := producerCount(pp); got != maxProducersPerPartition {
		t.Errorf("expireIdle at the ceiling: %d producers, want %d", got, maxProducersPerPartition)
	}
	pp = fill(maxProducersPerPartition+1, recent)
	for i := 0; i < 10; i++ { // the ten least recently used.
		pp.producers[int64(i)].lastUsed = recent.Add(-time.Duration(10-i) * time.Second)
	}
	pp.expireIdle(1 << 40)
	want := maxProducersPerPartition + 1 - (1 + maxProducersPerPartition/10)
	if got := producerCount(pp); got != want {
		t.Errorf("expireIdle one over the ceiling: %d producers, want %d", got, want)
	}
	for i := 0; i < 10; i++ {
		if _, ok := pp.producers[int64(i)]; ok {
			t.Errorf("producer %d, among the least recently used, was kept", i)
		}
	}
}

func TestEvictOldestDropsTheOldestOthersAndNeverMoreThanThereAre(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	build := func() *partitionProducers {
		pp := newPP()
		for i := int64(1); i <= 5; i++ {
			pp.producers[i] = &producerSequence{lastUsed: base.Add(time.Duration(i) * time.Minute)}
		}
		return pp
	}
	ids := func(pp *partitionProducers) []int {
		var out []int
		for id := range pp.producers {
			out = append(out, int(id))
		}
		sort.Ints(out)
		return out
	}
	pp := build()
	pp.evictOldest(1, 2) // producer 1 is the oldest, and is kept; 2 and 3 go.
	if got := fmt.Sprint(ids(pp)); got != "[1 4 5]" {
		t.Errorf("evicting 2 keeping 1: %s, want [1 4 5]", got)
	}
	pp = build()
	pp.evictOldest(3, 4) // exactly the others.
	if got := fmt.Sprint(ids(pp)); got != "[3]" {
		t.Errorf("evicting 4 of 4 others: %s, want [3]", got)
	}
	pp = build()
	pp.evictOldest(3, 99) // more than there are.
	if got := fmt.Sprint(ids(pp)); got != "[3]" {
		t.Errorf("evicting 99: %s, want [3]", got)
	}
}

func TestDropPartitionsForgetsOnlyThoseOfTheTopic(t *testing.T) {
	i := newIdempotency()
	for _, tp := range []topicPartition{{"x", 0}, {"x", 1}, {"x", 2}, {"y", 0}} {
		i.forPartition(tp.topic, tp.partition)
	}
	i.dropPartitions("x", 2)
	var left []string
	for tp := range i.partitions {
		left = append(left, fmt.Sprintf("%s-%d", tp.topic, tp.partition))
	}
	sort.Strings(left)
	if got := fmt.Sprint(left); got != "[x-2 y-0]" {
		t.Errorf("after dropping x's 2 partitions: %s, want [x-2 y-0]", got)
	}
}

func TestSnapshotNamesEachProducersLastSequence(t *testing.T) {
	i := newIdempotency()
	if got := i.snapshot("t", 0); got != nil {
		t.Errorf("a partition never produced to: %v", got)
	}
	pp := i.forPartition("t", 0)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at(t, start)
	pp.record(7, 3, 0, 4, 0, 0)
	pp.record(7, 3, 5, 9, 5, 0)
	pp.producers[8] = &producerSequence{epoch: 1, lastUsed: start} // no batch remembered.
	got := map[int64]producerView{}
	for _, v := range i.snapshot("t", 0) {
		got[v.pid] = v
	}
	if v := got[7]; v.epoch != 3 || v.lastSeq != 9 || v.lastUsedMs != start.UnixMilli() {
		t.Errorf("producer 7: %+v, want epoch 3, last sequence 9, last used %d", v, start.UnixMilli())
	}
	if v := got[8]; v.epoch != 1 || v.lastSeq != -1 {
		t.Errorf("producer 8: %+v, want epoch 1 and last sequence -1", v)
	}
}
