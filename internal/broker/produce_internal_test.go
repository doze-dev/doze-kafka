package broker

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// plainBatch is a minimal header-valid, non-idempotent (producer id -1) batch.
func plainBatch() []byte {
	b := make([]byte, 61+8)
	binary.BigEndian.PutUint32(b[8:], uint32(len(b)-12)) // batchLength
	b[16] = 2                                            // magic v2
	// lastOffsetDelta 0 (one record); producerId defaults to 0 here, so force -1:
	for i := 43; i < 51; i++ {
		b[i] = 0xff // producerId = -1 (non-idempotent)
	}
	binary.BigEndian.PutUint32(b[57:], 1) // recordsCount
	return b
}

// TestProduceAcksFsync verifies the durability contract of acks: acks=all fsyncs the
// written partition before replying, while acks=1 does not. The background fsync
// timer is disabled so the sync count reflects only produce-triggered syncs.
func TestProduceAcksFsync(t *testing.T) {
	dir := t.TempDir()
	metaStore, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metaStore.Close()
	logs, err := log.OpenManager(filepath.Join(dir, "data"), log.ManagerOptions{
		SegmentBytes:       1 << 20,
		IndexIntervalBytes: 4096,
		FsyncInterval:      0, // no timer: SyncOps reflects only durable-ack fsyncs.
	})
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()

	cfg := config.Default()
	cfg.FsyncInterval = 0
	b, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), logs, metaStore)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metaStore.CreateTopic("t", 1, nil); err != nil {
		t.Fatal(err)
	}

	produce := func(acks int16) {
		req := kmsg.NewPtrProduceRequest()
		req.SetVersion(7) // non-flexible: body encodes without a tagged-fields header.
		req.Acks = acks
		req.TimeoutMillis = 1000
		pt := kmsg.NewProduceRequestTopic()
		pt.Topic = "t"
		pp := kmsg.NewProduceRequestTopicPartition()
		pp.Partition = 0
		pp.Records = plainBatch()
		pt.Partitions = append(pt.Partitions, pp)
		req.Topics = append(req.Topics, pt)

		r := &protocol.Request{
			Header: protocol.RequestHeader{APIKey: protocol.APIProduce, APIVersion: 7, CorrelationID: 1},
			Body:   req.AppendTo(nil),
		}
		if _, err := b.handleProduce(context.Background(), r); err != nil {
			t.Fatalf("handleProduce(acks=%d): %v", acks, err)
		}
	}

	// acks=1 (leader/page-cache): must not fsync.
	before := logs.SyncOps()
	produce(1)
	if got := logs.SyncOps(); got != before {
		t.Fatalf("acks=1 fsynced (%d -> %d); it should stay in the page cache", before, got)
	}

	// acks=all (-1): must fsync the written partition before acknowledging.
	before = logs.SyncOps()
	produce(-1)
	if got := logs.SyncOps(); got <= before {
		t.Fatalf("acks=all did not fsync (%d -> %d); the acknowledgement is not durable", before, got)
	}
}
