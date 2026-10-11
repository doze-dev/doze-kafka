package broker

// BAD-3 of plan/stages/bad-days.md: a failed fsync is not retried into a false success.

import (
	"os"
	"sync"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/storage/log"

	"github.com/twmb/franz-go/pkg/kerr"
)

// BAD-3 at the broker. The kernel's behaviour after a failed fsync: the unsynced
// pages are dropped and the next fsync succeeds. A broker that retries then
// acknowledges acks=all for bytes that are gone. It must refuse instead, for
// acks=all and for acks=1 (the partition's writes are offline until a restart).
func TestFsyncFailureDoesNotFalselySucceedAtTheBroker(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)
	var once sync.Once
	restore := log.SetFsyncForTest(func(f *os.File) error {
		fail := false
		once.Do(func() { fail = true })
		if fail {
			_ = f.Truncate(0) // the dirty pages are dropped...
			return os.ErrInvalid
		}
		return f.Sync() // ...and the retry "succeeds".
	})
	defer restore()

	if p := produceOnce(t, b, -1, plainBatch()).Topics[0].Partitions[0]; p.ErrorCode != kerr.KafkaStorageError.Code {
		t.Fatalf("the failing fsync: code %d, want KAFKA_STORAGE_ERROR", p.ErrorCode)
	}
	for _, acks := range []int16{-1, 1} {
		p := produceOnce(t, b, acks, plainBatch()).Topics[0].Partitions[0]
		if p.ErrorCode != kerr.KafkaStorageError.Code {
			t.Fatalf("acks=%d after a failed fsync: code %d, want KAFKA_STORAGE_ERROR (the retry would have \"succeeded\" for data the kernel dropped)", acks, p.ErrorCode)
		}
	}
	if err := logs.Sync(); err == nil {
		t.Fatal("the periodic sync reported success on a partition whose fsync failed")
	}
}
