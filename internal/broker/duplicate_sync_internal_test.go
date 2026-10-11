package broker

// BAD-1 of plan/stages/bad-days.md: what an acks=all acknowledgement means when the
// first send of a batch was never synced.

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

// BAD-1 (P0). A retry of an idempotent acks=all batch the log already holds is
// answered from the log. If the first send was never fsynced (the sync failed, or
// the connection dropped before it) those bytes are still in the page cache, so
// the retry must fsync before it is acknowledged: acks=all is a promise about the
// disk, not about who wrote the bytes.
func TestDuplicateRetryAtAcksAllIsSyncedBeforeTheAck(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)

	// First send: appended, then the sync fails and the producer is told so.
	failSync = true
	first := produceOnce(t, b, -1, idemBatch(7, 0, 0, 1, 0)).Topics[0].Partitions[0]
	if first.ErrorCode != kerr.KafkaStorageError.Code {
		t.Fatalf("first send with a failing sync: code %d, want KAFKA_STORAGE_ERROR", first.ErrorCode)
	}

	// The producer retries the same sequence once the disk is back.
	failSync = false
	before := logs.SyncOps()
	retry := produceOnce(t, b, -1, idemBatch(7, 0, 0, 1, 0)).Topics[0].Partitions[0]
	if retry.ErrorCode != 0 || retry.BaseOffset != 0 {
		t.Fatalf("retry: code %d offset %d, want 0 at the original offset 0", retry.ErrorCode, retry.BaseOffset)
	}
	if got := logs.SyncOps(); got != before+1 {
		t.Fatalf("a duplicate retry at acks=all was acknowledged with %d fsync(s), want 1: acknowledged and not durable", got-before)
	}
}

// The same hole through acks=1: the first send is never synced by design, and the
// acks=all retry of it is the first request to promise durability.
func TestDuplicateAtAcksAllAfterAcksOneIsSynced(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)
	if p := produceOnce(t, b, 1, idemBatch(7, 0, 0, 1, 0)).Topics[0].Partitions[0]; p.ErrorCode != 0 {
		t.Fatalf("acks=1: code %d", p.ErrorCode)
	}
	before := logs.SyncOps()
	if p := produceOnce(t, b, -1, idemBatch(7, 0, 0, 1, 0)).Topics[0].Partitions[0]; p.ErrorCode != 0 || p.BaseOffset != 0 {
		t.Fatalf("acks=all duplicate: code %d offset %d", p.ErrorCode, p.BaseOffset)
	}
	if got := logs.SyncOps(); got != before+1 {
		t.Fatalf("the acks=all duplicate was acknowledged with %d fsync(s), want 1", got-before)
	}
}
