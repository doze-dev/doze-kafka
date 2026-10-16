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

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"

	"github.com/twmb/franz-go/pkg/kerr"
)

// testRecord is the smallest record there is, eight bytes: no key, the value "v",
// no headers, at offset delta i (below 64, so that it is one byte).
func testRecord(i int32) []byte {
	return []byte{0x0e, 0, 0, byte(i << 1), 0x01, 0x02, 'v', 0}
}

// plainBatch is a minimal valid, non-idempotent (producer id -1) batch of one record.
func plainBatch() []byte {
	b := make([]byte, 61+8)
	binary.BigEndian.PutUint32(b[8:], uint32(len(b)-12)) // batchLength
	b[16] = 2                                            // magic v2
	// lastOffsetDelta 0 (one record); producerId defaults to 0 here, so force -1:
	for i := 43; i < 51; i++ {
		b[i] = 0xff // producerId = -1 (non-idempotent)
	}
	binary.BigEndian.PutUint32(b[57:], 1) // recordsCount
	copy(b[61:], testRecord(0))
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
	if err := logs.EnsurePartitionDirs("t", 1); err != nil { // as createTopic does.
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
			Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.Produce), APIVersion: 7, CorrelationID: 1},
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
		Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.Produce), APIVersion: 8, CorrelationID: 1},
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
	if err := logs.EnsurePartitionDirs("t", 1); err != nil { // as createTopic does.
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
	if p.ErrorCode != kerr.KafkaStorageError.Code {
		t.Fatalf("acks=all with a failing fsync: error code %d, want KAFKA_STORAGE_ERROR (%d)", p.ErrorCode, kerr.KafkaStorageError.Code)
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

// A batch that fails validation is refused whole, before anything is written, with
// the error Apache Kafka answers for it (recorded: produce-validation). A second
// batch in the same partition's records, and a batch cut short, are INVALID_RECORD:
// a Produce request carries exactly one whole batch a partition.
func TestProduceRefusesCorruptBatches(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)

	good := plainBatch()
	flip := func(batch []byte, i int) []byte { c := append([]byte{}, batch...); c[i] ^= 0xFF; return c }
	cases := map[string]struct {
		records []byte
		want    int16
	}{
		"payload byte flipped (CRC)": {flip(good, len(good)-1), kerr.CorruptMessage.Code},
		"header byte flipped (CRC)":  {flip(good, 30), kerr.CorruptMessage.Code},
		"wrong magic":                {flip(good, 16), kerr.CorruptMessage.Code},
		"good batch then a bad one":  {append(append([]byte{}, good...), flip(good, len(good)-1)...), kerr.InvalidRecord.Code},
		"two good batches":           {append(append([]byte{}, good...), good...), kerr.InvalidRecord.Code},
		"truncated batch":            {good[:len(good)-4], kerr.InvalidRecord.Code},
		"no records at all":          {nil, kerr.InvalidRecord.Code},
		// The transactional bit, in a request that names no transactional id.
		"attributes flipped": {flip(good, 22), kerr.TransactionalIDAuthorizationFailed.Code},
	}
	for name, tc := range cases {
		p := produceOnce(t, b, 1, tc.records).Topics[0].Partitions[0]
		if p.ErrorCode != tc.want || p.BaseOffset != -1 {
			t.Errorf("%s: error code %d at offset %d, want %d at -1", name, p.ErrorCode, p.BaseOffset, tc.want)
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
	// Bytes after the batch that are too few to be another are not the batch's, and
	// not an error: Kafka takes the batch and drops them, and so does this.
	if p := produceOnce(t, b, 1, append(append([]byte{}, good...), 1, 2, 3)).Topics[0].Partitions[0]; p.ErrorCode != 0 || p.BaseOffset != 1 {
		t.Fatalf("a batch followed by three bytes: code %d offset %d, want 0 at 1", p.ErrorCode, p.BaseOffset)
	}
	if stored, err := part.Read(0, 1<<20); err != nil || len(stored) != 2*len(good) {
		t.Errorf("the log holds %d bytes after two batches of %d (%v): the three bytes after the second were stored", len(stored), len(good), err)
	}
}

// Creating a topic, and asking for the offsets of all its partitions, must not
// open them: each open partition holds descriptors for the life of the broker.
func TestTopicCreationAndOffsetListingDoNotOpenPartitions(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)

	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = "wide", 40, 1
	if got := b.createTopic(ct, false, nil); got.ErrorCode != 0 {
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
		out, err := typed(b.handleListOffsets)(context.Background(), &protocol.Request{
			Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.ListOffsets), APIVersion: 5, CorrelationID: 1},
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

// idemBatch is plainBatch for an idempotent producer: n records' worth of body
// from producer pid at the given epoch and base sequence, with attributes attrs.
func idemBatch(pid int64, epoch int16, seq, n int32, attrs uint16) []byte {
	b := make([]byte, 61+int(n)*8)
	binary.BigEndian.PutUint32(b[8:], uint32(len(b)-12))
	b[16] = 2
	binary.BigEndian.PutUint16(b[21:], attrs)
	binary.BigEndian.PutUint32(b[23:], uint32(n-1)) // lastOffsetDelta
	binary.BigEndian.PutUint64(b[43:], uint64(pid))
	binary.BigEndian.PutUint16(b[51:], uint16(epoch))
	binary.BigEndian.PutUint32(b[53:], uint32(seq))
	binary.BigEndian.PutUint32(b[57:], uint32(n))
	for i := int32(0); i < n; i++ {
		copy(b[61+8*i:], testRecord(i))
	}
	binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], crc32.MakeTable(crc32.Castagnoli)))
	return b
}

func TestIdempotentProduceRefusesWhatItCannotPlace(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)
	part, _ := logs.Partition("t", 0)
	code := func(records []byte) (int16, int64) {
		p := produceOnce(t, b, 1, records).Topics[0].Partitions[0]
		return p.ErrorCode, p.BaseOffset
	}
	if c, off := code(idemBatch(9, 0, 0, 5, 0)); c != 0 || off != 0 {
		t.Fatalf("seq 0-4: code %d offset %d", c, off)
	}
	if c, off := code(idemBatch(9, 0, 0, 5, 0)); c != 0 || off != 0 {
		t.Errorf("an exact retry: code %d offset %d, want success at the original offset 0", c, off)
	}
	end := part.LogEndOffset()
	// Sequences 3-6: two new records behind two old ones.
	if c, _ := code(idemBatch(9, 0, 3, 4, 0)); c != kerr.OutOfOrderSequenceNumber.Code {
		t.Errorf("an overlapping batch: code %d, want OUT_OF_ORDER_SEQUENCE_NUMBER (45)", c)
	}
	if c, _ := code(idemBatch(9, 0, 9, 1, 0)); c != kerr.OutOfOrderSequenceNumber.Code {
		t.Errorf("a gap: code %d, want 45", c)
	}
	if got := part.LogEndOffset(); got != end {
		t.Errorf("refused batches moved the log end from %d to %d", end, got)
	}
}

func TestProduceValidatesAcksControlBatchesAndRecordCounts(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)
	part, _ := logs.Partition("t", 0)

	for _, acks := range []int16{2, -2, 100} {
		if c := produceOnce(t, b, acks, plainBatch()).Topics[0].Partitions[0].ErrorCode; c != kerr.InvalidRequiredAcks.Code {
			t.Errorf("acks=%d: code %d, want INVALID_REQUIRED_ACKS (21)", acks, c)
		}
	}
	// A control batch (attributes bit 5) is the broker's to write, not a client's.
	if c := produceOnce(t, b, 1, idemBatch(-1, 0, 0, 1, 0x20)).Topics[0].Partitions[0].ErrorCode; c != kerr.InvalidRecord.Code {
		t.Errorf("a client-sent control batch: code %d, want INVALID_RECORD (87), as Kafka answers", c)
	}
	// A header that claims two billion records over an 8-byte body.
	huge := plainBatch()
	binary.BigEndian.PutUint32(huge[23:], uint32(1<<31-2))
	binary.BigEndian.PutUint32(huge[57:], uint32(1<<31-1))
	binary.BigEndian.PutUint32(huge[17:], crc32.Checksum(huge[21:], crc32.MakeTable(crc32.Castagnoli)))
	if c := produceOnce(t, b, 1, huge).Topics[0].Partitions[0].ErrorCode; c != kerr.InvalidRecord.Code {
		t.Errorf("a batch claiming 2^31-1 records in 8 bytes: code %d, want INVALID_RECORD (87)", c)
	}
	if end := part.LogEndOffset(); end != 0 {
		t.Errorf("the log end is %d after only refused produces", end)
	}
}

// acks=0 has no response to carry an error, so an invalid batch ends the
// connection: that is how the producer finds out.
func TestAcksZeroWithAnInvalidBatchFailsTheRequest(t *testing.T) {
	var failSync bool
	b, _ := newTestBroker(t, &failSync)
	bad := plainBatch()
	bad[len(bad)-1] ^= 0xff
	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(8)
	req.Acks = 0
	pt := kmsg.NewProduceRequestTopic()
	pt.Topic = "t"
	pp := kmsg.NewProduceRequestTopicPartition()
	pp.Records = bad
	pt.Partitions = append(pt.Partitions, pp)
	req.Topics = append(req.Topics, pt)
	out, err := b.handleProduce(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.Produce), APIVersion: 8}, Body: req.AppendTo(nil)})
	if err == nil || out != nil {
		t.Fatalf("acks=0 with a corrupt batch returned (%v, %v); want an error so the connection closes", out, err)
	}
	pp.Records = plainBatch()
	req.Topics[0].Partitions[0] = pp
	if out, err := b.handleProduce(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.Produce), APIVersion: 8}, Body: req.AppendTo(nil)}); err != nil || out != nil {
		t.Fatalf("acks=0 with a valid batch returned (%v, %v); want silence", out, err)
	}
}

// Deleting a topic forgets its producers. Otherwise the same producer writing
// sequence 0 to the re-created topic is taken for a retry and nothing is appended.
func TestDeletingATopicForgetsItsProducerState(t *testing.T) {
	var failSync bool
	b, logs := newTestBroker(t, &failSync)
	if p := produceOnce(t, b, 1, idemBatch(5, 0, 0, 3, 0)).Topics[0].Partitions[0]; p.ErrorCode != 0 {
		t.Fatal(p.ErrorCode)
	}
	del := kmsg.NewPtrDeleteTopicsRequest()
	del.SetVersion(3)
	del.TopicNames = []string{"t"}
	if _, err := typed(b.handleDeleteTopics)(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.DeleteTopics), APIVersion: 3}, Body: del.AppendTo(nil)}); err != nil {
		t.Fatal(err)
	}
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = "t", 1, 1
	if got := b.createTopic(ct, false, nil); got.ErrorCode != 0 {
		t.Fatal(got.ErrorCode)
	}
	p := produceOnce(t, b, 1, idemBatch(5, 0, 0, 3, 0)).Topics[0].Partitions[0]
	part, _ := logs.Partition("t", 0)
	if p.ErrorCode != 0 || p.BaseOffset != 0 || part.LogEndOffset() != 3 {
		t.Errorf("the same producer on the re-created topic: code %d offset %d, log end %d; want its 3 records appended at 0",
			p.ErrorCode, p.BaseOffset, part.LogEndOffset())
	}
}

// Producer ids handed out before a restart are never handed out again, including
// the ones served from memory out of a reserved block.
func TestProducerIDsAreNotReusedAfterARestart(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Broker, func()) {
		metaStore, err := meta.Open(filepath.Join(dir, "meta.db"))
		if err != nil {
			t.Fatal(err)
		}
		logs, err := log.OpenManager(filepath.Join(dir, "data"), log.ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		b, err := New(config.Default(), slog.New(slog.NewTextHandler(io.Discard, nil)), logs, metaStore)
		if err != nil {
			t.Fatal(err)
		}
		return b, func() { logs.Close(); metaStore.Close() }
	}
	seen := map[int64]bool{}
	for run := 0; run < 3; run++ {
		b, closeAll := open()
		for i := 0; i < 5; i++ {
			id, err := b.nextProducerID()
			if err != nil {
				t.Fatal(err)
			}
			if seen[id] {
				t.Fatalf("producer id %d was issued twice (second time in run %d)", id, run)
			}
			seen[id] = true
		}
		closeAll()
	}
}
