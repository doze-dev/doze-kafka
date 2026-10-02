package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
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
	binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], crc32.MakeTable(crc32.Castagnoli)))
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

// produceOnce runs handleProduce directly and returns the decoded response, so a
// test can read the per-partition error code and message.
func produceOnce(t *testing.T, b *Broker, acks int16, records []byte) *kmsg.ProduceResponse {
	t.Helper()
	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(8) // v8 is the first with an error message, still non-flexible.
	req.Acks = acks
	req.TimeoutMillis = 1000
	pt := kmsg.NewProduceRequestTopic()
	pt.Topic = "t"
	pp := kmsg.NewProduceRequestTopicPartition()
	pp.Records = records
	pt.Partitions = append(pt.Partitions, pp)
	req.Topics = append(req.Topics, pt)
	out, err := b.handleProduce(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: protocol.APIProduce, APIVersion: 8, CorrelationID: 1},
		Body:   req.AppendTo(nil),
	})
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(8)
	if err := resp.ReadFrom(out[4:]); err != nil { // skip the correlation id.
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

// newTestBroker builds a broker over a real log manager whose fsync can be made
// to fail on demand.
func newTestBroker(t *testing.T, failSync *bool) (*Broker, *log.Manager) {
	t.Helper()
	dir := t.TempDir()
	metaStore, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metaStore.Close() })
	logs, err := log.OpenManager(filepath.Join(dir, "data"), log.ManagerOptions{
		SegmentBytes:       1 << 20,
		IndexIntervalBytes: 4096,
		BeforeSync: func() error {
			if *failSync {
				return errDiskOnFire
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logs.Close() })
	cfg := config.Default()
	cfg.FsyncInterval = 0
	b, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), logs, metaStore)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metaStore.CreateTopic("t", 1, nil); err != nil {
		t.Fatal(err)
	}
	return b, logs
}

var errDiskOnFire = errors.New("input/output error")

// acks=all promises the record is on disk. When the fsync fails the producer
// must be told; it used to be acknowledged as a success.
func TestProduceFsyncFailureIsReportedNotAcknowledged(t *testing.T) {
	var failSync bool
	b, _ := newTestBroker(t, &failSync)

	failSync = true
	resp := produceOnce(t, b, -1, plainBatch())
	p := resp.Topics[0].Partitions[0]
	if p.ErrorCode != int16(protocol.ErrKafkaStorageError) {
		t.Fatalf("acks=all with a failing fsync: error code %d, want KAFKA_STORAGE_ERROR (%d)", p.ErrorCode, protocol.ErrKafkaStorageError)
	}
	if p.ErrorMessage == nil || !strings.Contains(*p.ErrorMessage, "input/output error") {
		t.Errorf("the failure should say why, got %v", p.ErrorMessage)
	}

	// acks=1 never promised durability, so it must not fail on a sync it never makes.
	if p := produceOnce(t, b, 1, plainBatch()).Topics[0].Partitions[0]; p.ErrorCode != 0 {
		t.Errorf("acks=1 failed with %d though it does not fsync", p.ErrorCode)
	}

	// And a healthy disk acknowledges normally.
	failSync = false
	if p := produceOnce(t, b, -1, plainBatch()).Topics[0].Partitions[0]; p.ErrorCode != 0 {
		t.Errorf("acks=all on a healthy disk: error code %d", p.ErrorCode)
	}
}

// A batch that fails validation is refused whole, before anything is written —
// including the good batches that precede it in the same payload.
func TestProduceRefusesCorruptBatches(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)

	good := plainBatch()
	flip := func(batch []byte, i int) []byte { c := append([]byte{}, batch...); c[i] ^= 0xFF; return c }
	cases := map[string][]byte{
		"payload byte flipped (CRC)": flip(good, len(good)-1),
		"header byte flipped (CRC)":  flip(good, 22),
		"wrong magic":                flip(good, 16),
		"good batch then a bad one":  append(append([]byte{}, good...), flip(good, len(good)-1)...),
		"truncated batch":            good[:len(good)-4],
		"trailing junk":              append(append([]byte{}, good...), 1, 2, 3),
	}
	for name, records := range cases {
		p := produceOnce(t, b, 1, records).Topics[0].Partitions[0]
		if p.ErrorCode != int16(protocol.ErrCorruptMessage) {
			t.Errorf("%s: error code %d, want CORRUPT_MESSAGE (%d)", name, p.ErrorCode, protocol.ErrCorruptMessage)
		}
	}
	part, err := logs.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	if leo := part.LogEndOffset(); leo != 0 {
		t.Fatalf("log end offset = %d after only invalid produces; something was written", leo)
	}

	// The partition is untouched, so the next valid batch is offset 0.
	if p := produceOnce(t, b, 1, good).Topics[0].Partitions[0]; p.ErrorCode != 0 || p.BaseOffset != 0 {
		t.Fatalf("a valid batch after the refusals: code %d offset %d, want 0 at 0", p.ErrorCode, p.BaseOffset)
	}
}

// Creating a topic, and asking for the offsets of all its partitions, must not
// open them: each open partition holds descriptors for the life of the broker.
func TestTopicCreationAndOffsetListingDoNotOpenPartitions(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)

	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = "wide", 40, 1
	if got := b.createTopic(ct, false); got.ErrorCode != 0 {
		t.Fatalf("createTopic: %d", got.ErrorCode)
	}
	if n := logs.OpenPartitions(); n != 0 {
		t.Fatalf("creating a 40-partition topic opened %d partitions", n)
	}

	lo := kmsg.NewPtrListOffsetsRequest()
	lo.SetVersion(5)
	lo.ReplicaID = -1
	for _, ts := range []int64{-1, -2} {
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = "wide"
		for p := int32(0); p < 40; p++ {
			rp := kmsg.NewListOffsetsRequestTopicPartition()
			rp.Partition, rp.Timestamp = p, ts
			rt.Partitions = append(rt.Partitions, rp)
		}
		lo.Topics = []kmsg.ListOffsetsRequestTopic{rt}
		out, err := b.handleListOffsets(context.Background(), &protocol.Request{
			Header: protocol.RequestHeader{APIKey: protocol.APIListOffsets, APIVersion: 5, CorrelationID: 1},
			Body:   lo.AppendTo(nil),
		})
		if err != nil {
			t.Fatal(err)
		}
		resp := kmsg.NewPtrListOffsetsResponse()
		resp.SetVersion(5)
		if err := resp.ReadFrom(out[4:]); err != nil {
			t.Fatal(err)
		}
		for _, p := range resp.Topics[0].Partitions {
			if p.ErrorCode != 0 || p.Offset != 0 {
				t.Fatalf("partition %d: code %d offset %d, want an empty partition", p.Partition, p.ErrorCode, p.Offset)
			}
		}
	}
	if n := logs.OpenPartitions(); n != 0 {
		t.Fatalf("listing offsets opened %d partitions", n)
	}

	// Producing is what opens one.
	produceOnce(t, b, 1, plainBatch())
	if n := logs.OpenPartitions(); n != 1 {
		t.Fatalf("after one produce %d partitions are open, want exactly 1", n)
	}
}
