package broker

import "testing"

// Every idempotent producer session gets a fresh producer id, so state kept per
// producer must be bounded or a long-running broker driven by short-lived clients
// grows without limit.
func TestProducerStateIsBoundedAndKeepsTheRecent(t *testing.T) {
	pp := &partitionProducers{producers: map[int64]*producerSequence{}}
	const n = maxProducersPerPartition + 250
	for pid := int64(1); pid <= n; pid++ {
		pp.record(pid, 0, 0, 0, pid)
	}
	if got := len(pp.producers); got > maxProducersPerPartition {
		t.Fatalf("tracking %d producers, want at most %d", got, maxProducersPerPartition)
	}
	if _, ok := pp.producers[n]; !ok {
		t.Error("the most recent producer must be retained")
	}
	if _, ok := pp.producers[1]; ok {
		t.Error("the least recently used producer should have been evicted")
	}

	// A producer that keeps writing is not the one evicted, however old its id.
	pp = &partitionProducers{producers: map[int64]*producerSequence{}}
	for pid := int64(1); pid <= maxProducersPerPartition; pid++ {
		pp.record(pid, 0, 0, 0, pid)
	}
	pp.record(1, 0, 1, 1, 99) // producer 1 is active again.
	pp.record(maxProducersPerPartition+1, 0, 0, 0, 100)
	if _, ok := pp.producers[1]; !ok {
		t.Error("an active producer was evicted ahead of idle ones")
	}
	if _, ok := pp.producers[2]; ok {
		t.Error("the idlest producer should have been the one evicted")
	}
}
