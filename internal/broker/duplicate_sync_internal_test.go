package broker

// BAD-1 of plan/stages/bad-days.md: what an acks=all acknowledgement means when
// the answer rests on bytes this request did not write.
//
// These tests used to assert a counter of SyncPartition calls. That counter rose
// by one for a call that returned at once because the partition believed it had
// nothing to flush, which is exactly the hole: after a kill and a restart the
// retry was acknowledged with one call and no fsync. They now count the fsyncs
// the disk saw and read back what a power cut leaves.

import (
	"testing"

	"github.com/doze-dev/doze-kafka/internal/powercut"

	"github.com/twmb/franz-go/pkg/kerr"
)

// mustBeOnDisk cuts the power under every mode and checks that the batch
// acknowledged at offset is there, whole.
func mustBeOnDisk(t *testing.T, r *rig, topic string, partition int32, offset int64, payload string) {
	t.Helper()
	for _, mode := range powercut.Modes {
		after := r.cut(mode, 7)
		got, ok := payloadAt(after.read(topic, partition, false), offset)
		if !ok || got != payload {
			t.Fatalf("ACKNOWLEDGED AND LOST (power cut, %s): %s-%d offset %d was acknowledged at acks=all; after the cut it holds %q (present=%v), want %q", mode, topic, partition, offset, got, ok, payload)
		}
		after.shutdown()
	}
}

// BAD-1 (P0). A retry of an idempotent acks=all batch the log already holds is
// answered from the log. If the first send was never fsynced (the sync failed, or
// the connection dropped before it) those bytes are still in the page cache, so
// the retry must fsync before it is acknowledged: acks=all is a promise about the
// disk, not about who wrote the bytes.
func TestDuplicateRetryAtAcksAllIsSyncedBeforeTheAck(t *testing.T) {
	r := newRig(t, 1<<20)
	r.mustCreateTopic("t", 1, nil)

	// First send: appended, then the sync fails and the producer is told so.
	r.failSync.Store(true)
	first := r.produce("t", 0, -1, nil, payloadBatch(7, 0, 0, 0, "first send"))
	if first.ErrorCode != kerr.KafkaStorageError.Code {
		t.Fatalf("first send with a failing sync: code %d, want KAFKA_STORAGE_ERROR", first.ErrorCode)
	}

	// The producer retries the same sequence once the disk is back.
	r.failSync.Store(false)
	before := r.pc.FsyncsOf(".log")
	retry := r.produce("t", 0, -1, nil, payloadBatch(7, 0, 0, 0, "first send"))
	if retry.ErrorCode != 0 || retry.BaseOffset != 0 {
		t.Fatalf("retry: code %d offset %d, want 0 at the original offset 0", retry.ErrorCode, retry.BaseOffset)
	}
	if got := r.pc.FsyncsOf(".log") - before; got != 1 {
		t.Fatalf("a duplicate retry at acks=all was acknowledged after %d fsync(s) of the log, want 1: acknowledged and not durable", got)
	}
	mustBeOnDisk(t, r, "t", 0, 0, "first send")
}

// The same hole through acks=1: the first send is never synced by design, and the
// acks=all retry of it is the first request to promise durability.
func TestDuplicateAtAcksAllAfterAcksOneIsSynced(t *testing.T) {
	r := newRig(t, 1<<20)
	r.mustCreateTopic("t", 1, nil)
	if p := r.produce("t", 0, 1, nil, payloadBatch(7, 0, 0, 0, "acks=1 first")); p.ErrorCode != 0 {
		t.Fatalf("acks=1: code %d", p.ErrorCode)
	}
	before := r.pc.FsyncsOf(".log")
	if p := r.produce("t", 0, -1, nil, payloadBatch(7, 0, 0, 0, "acks=1 first")); p.ErrorCode != 0 || p.BaseOffset != 0 {
		t.Fatalf("acks=all duplicate: code %d offset %d", p.ErrorCode, p.BaseOffset)
	}
	if got := r.pc.FsyncsOf(".log") - before; got != 1 {
		t.Fatalf("the acks=all duplicate was acknowledged after %d fsync(s) of the log, want 1", got)
	}
	mustBeOnDisk(t, r, "t", 0, 0, "acks=1 first")
}

// BAD-1 across a restart. The first send reaches the log and the broker is killed
// before any fsync. It restarts and rebuilds the producer's sequence from the
// log; the producer retries at acks=all; the retry is a duplicate. The new
// process has written nothing, and that is not the same as having nothing to
// flush: what it read back was never on disk.
func TestDuplicateRetryAfterAKillAndRestartIsDurable(t *testing.T) {
	r := newRig(t, 1<<20)
	r.mustCreateTopic("t", 1, nil)
	if p := r.produce("t", 0, -1, nil, payloadBatch(11, 0, 0, 0, "baseline")); p.ErrorCode != 0 {
		t.Fatalf("baseline: %d", p.ErrorCode)
	}
	// In the page cache only: the process dies before the fsync.
	if p := r.produce("t", 0, 1, nil, payloadBatch(12, 0, 0, 0, "first send, never flushed")); p.ErrorCode != 0 || p.BaseOffset != 1 {
		t.Fatalf("first send: %d at %d", p.ErrorCode, p.BaseOffset)
	}
	r.restart()

	before := r.pc.FsyncsOf(".log")
	p := r.produce("t", 0, -1, nil, payloadBatch(12, 0, 0, 0, "first send, never flushed"))
	if p.ErrorCode != 0 || p.BaseOffset != 1 {
		t.Fatalf("retry after the restart: code %d offset %d, want a duplicate acknowledged at 1", p.ErrorCode, p.BaseOffset)
	}
	if got := r.pc.FsyncsOf(".log") - before; got != 1 {
		t.Errorf("the retry was acknowledged after %d fsync(s) of the log, want 1", got)
	}
	mustBeOnDisk(t, r, "t", 0, 1, "first send, never flushed")
}

// And when the fsync it needs cannot be had, the retry is refused, like any other
// acks=all produce. (This is the real-binary reproduction of the hole, in
// process: with every flush failing, a new batch was refused and a duplicate was
// acknowledged.)
func TestDuplicateRetryAfterARestartIsRefusedWhenItCannotBeFlushed(t *testing.T) {
	r := newRig(t, 1<<20)
	r.mustCreateTopic("t", 1, nil)
	if p := r.produce("t", 0, 1, nil, payloadBatch(12, 0, 0, 0, "x")); p.ErrorCode != 0 {
		t.Fatalf("first send: %d", p.ErrorCode)
	}
	r.restart()
	r.failSync.Store(true)
	if p := r.produce("t", 0, -1, nil, payloadBatch(12, 0, 0, 0, "x")); p.ErrorCode != kerr.KafkaStorageError.Code || p.BaseOffset != -1 {
		t.Fatalf("a duplicate retry with every flush failing: code %d offset %d, want KAFKA_STORAGE_ERROR and -1", p.ErrorCode, p.BaseOffset)
	}
}

// One Produce, two partitions: one holds a duplicate of a send nobody flushed, the
// other takes a new batch. Each is acknowledged on its own partition's flush.
// (Every test that watched an fsync used a one-partition topic, so flushing
// partition 0 whatever was written passed them all.)
func TestEveryPartitionOfAProduceIsFlushedBeforeItIsAcknowledged(t *testing.T) {
	r := newRig(t, 1<<20)
	r.mustCreateTopic("t", 3, nil)
	if p := r.produce("t", 2, 1, nil, payloadBatch(12, 0, 0, 0, "dup on p2")); p.ErrorCode != 0 {
		t.Fatalf("first send: %d", p.ErrorCode)
	}
	resp := r.produceMany("t", -1, nil, []int32{1, 2}, map[int32][]byte{
		1: payloadBatch(12, 0, 0, 0, "new on p1"),
		2: payloadBatch(12, 0, 0, 0, "dup on p2"),
	})
	for _, p := range resp {
		if p.ErrorCode != 0 || p.BaseOffset != 0 {
			t.Fatalf("partition %d: code %d offset %d", p.Partition, p.ErrorCode, p.BaseOffset)
		}
	}
	mustBeOnDisk(t, r, "t", 1, 0, "new on p1")
	mustBeOnDisk(t, r, "t", 2, 0, "dup on p2")
}

// A duplicate whose original sits in a segment that has since rolled, before and
// after a restart.
func TestDuplicateOfABatchInASealedSegmentIsDurable(t *testing.T) {
	r := newRig(t, 300)
	r.mustCreateTopic("t", 1, nil)
	r.produce("t", 0, 1, nil, payloadBatch(12, 0, 0, 0, "the original"))
	for range 10 {
		r.produce("t", 0, 1, nil, payloadBatch(-1, -1, -1, 0, "filler, to roll the segment a few times"))
	}
	if p := r.produce("t", 0, -1, nil, payloadBatch(12, 0, 0, 0, "the original")); p.ErrorCode != 0 || p.BaseOffset != 0 {
		t.Fatalf("duplicate: %d at %d", p.ErrorCode, p.BaseOffset)
	}
	mustBeOnDisk(t, r, "t", 0, 0, "the original")

	r.produce("t", 0, 1, nil, payloadBatch(12, 0, 1, 0, "the second original"))
	for range 10 {
		r.produce("t", 0, 1, nil, payloadBatch(-1, -1, -1, 0, "filler, to roll the segment a few times"))
	}
	r.restart()
	if p := r.produce("t", 0, -1, nil, payloadBatch(12, 0, 1, 0, "the second original")); p.ErrorCode != 0 || p.BaseOffset != 11 {
		t.Fatalf("duplicate after a restart: %d at %d", p.ErrorCode, p.BaseOffset)
	}
	mustBeOnDisk(t, r, "t", 0, 11, "the second original")
}
