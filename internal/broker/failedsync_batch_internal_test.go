package broker

// BAD-2 of plan/stages/bad-days.md: what a failed acks=all fsync leaves in the log.

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

// BAD-2 (decision: document and show, as Kafka's NOT_ENOUGH_REPLICAS_AFTER_APPEND
// does). When the acks=all fsync fails the batch is already in the log. The
// producer is told KAFKA_STORAGE_ERROR (retriable) with no offset, and the batch
// stays: a consumer can read it, and a non-idempotent retry stores it again,
// exactly as a retry after NOT_ENOUGH_REPLICAS_AFTER_APPEND does. An idempotent
// retry is deduplicated. This test pins that choice so a change to it is a
// decision, not an accident.
func TestFsyncFailedBatchIsEitherRolledBackOrDocumented(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)
	part, _ := logs.Partition("t", 0)

	failSync = true
	p := produceOnce(t, b, -1, plainBatch()).Topics[0].Partitions[0]
	if p.ErrorCode != kerr.KafkaStorageError.Code || p.BaseOffset != -1 {
		t.Fatalf("failed sync: code %d offset %d, want KAFKA_STORAGE_ERROR and -1", p.ErrorCode, p.BaseOffset)
	}
	if got := part.LogEndOffset(); got != 1 {
		t.Fatalf("log end %d after a failed sync: the documented behaviour is that the batch stays (1)", got)
	}
	data, err := part.Read(0, 1<<20)
	if err != nil || len(data) == 0 {
		t.Fatalf("a consumer reads %d bytes, err %v: the documented behaviour is that the batch is visible", len(data), err)
	}

	failSync = false
	if p := produceOnce(t, b, -1, plainBatch()).Topics[0].Partitions[0]; p.ErrorCode != 0 || p.BaseOffset != 1 {
		t.Fatalf("non-idempotent retry: code %d offset %d, want it stored again at 1", p.ErrorCode, p.BaseOffset)
	}
	// The idempotent producer's retry is the same record, not a second copy.
	failSync = true
	if p := produceOnce(t, b, -1, idemBatch(5, 0, 0, 1, 0)).Topics[0].Partitions[0]; p.ErrorCode != kerr.KafkaStorageError.Code {
		t.Fatalf("idempotent first send: code %d", p.ErrorCode)
	}
	failSync = false
	if p := produceOnce(t, b, -1, idemBatch(5, 0, 0, 1, 0)).Topics[0].Partitions[0]; p.ErrorCode != 0 || p.BaseOffset != 2 {
		t.Fatalf("idempotent retry: code %d offset %d, want the original offset 2", p.ErrorCode, p.BaseOffset)
	}
	if got := part.LogEndOffset(); got != 3 {
		t.Fatalf("log end %d, want 3: the idempotent retry must not be stored twice", got)
	}
}
