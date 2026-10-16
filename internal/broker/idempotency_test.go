package broker_test

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// idempotentBatch builds a minimal but header-valid record batch v2 stamped with
// the idempotent-producer fields, holding as many eight-byte records as it says (the
// broker reads them: a batch that claims records it does not hold is refused). A
// test can forge an exact retry with it; the CRC is sealed last, as a real producer
// does, because the broker rejects a batch that does not check out.
func idempotentBatch(pid int64, epoch int16, baseSeq, records int32) []byte {
	bodyLen := 8 * int(records)
	total := 61 + bodyLen
	b := make([]byte, total)
	for i := int32(0); i < records; i++ {
		copy(b[61+8*i:], []byte{0x0e, 0, 0, byte(i << 1), 0x01, 0x02, 'v', 0}) // no key, the value "v", offset delta i.
	}
	binary.BigEndian.PutUint32(b[8:], uint32(total-12))   // batchLength (bytes after this field)
	b[16] = 2                                             // magic = v2
	binary.BigEndian.PutUint32(b[23:], uint32(records-1)) // lastOffsetDelta
	binary.BigEndian.PutUint64(b[43:], uint64(pid))       // producerId
	binary.BigEndian.PutUint16(b[51:], uint16(epoch))     // producerEpoch
	binary.BigEndian.PutUint32(b[53:], uint32(baseSeq))   // baseSequence
	binary.BigEndian.PutUint32(b[57:], uint32(records))   // recordsCount
	sealCRC(b)
	return b
}

// sealCRC writes a batch's CRC32C (over every byte from the attributes field on).
func sealCRC(b []byte) {
	binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], crc32.MakeTable(crc32.Castagnoli)))
}

// TestIdempotence_DeduplicatesRetries drives the idempotent-producer sequence logic
// at the wire level: a retried batch returns its original offset without appending a
// second copy, in-order batches advance, and a gap is rejected OUT_OF_ORDER.
func TestIdempotence_DeduplicatesRetries(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	mustCreateTopic(t, ctx, cl, "idem", 1)

	produce := func(batch []byte) kmsg.ProduceResponseTopicPartition {
		req := kmsg.NewPtrProduceRequest()
		req.Acks = -1
		req.TimeoutMillis = 5000
		rt := kmsg.NewProduceRequestTopic()
		rt.Topic = "idem"
		rp := kmsg.NewProduceRequestTopicPartition()
		rp.Partition = 0
		rp.Records = batch
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("produce: %v", err)
		}
		return resp.Topics[0].Partitions[0]
	}
	leo := func() int64 { return listOffset(t, ctx, cl, "idem", 0, -1) }

	const pid = int64(7777)
	const epoch = int16(0)

	// First batch (sequence 0) → appended at offset 0.
	r0 := produce(idempotentBatch(pid, epoch, 0, 1))
	if r0.ErrorCode != 0 {
		t.Fatalf("first produce error: %d", r0.ErrorCode)
	}
	if r0.BaseOffset != 0 || leo() != 1 {
		t.Fatalf("first produce: offset=%d leo=%d, want 0/1", r0.BaseOffset, leo())
	}

	// Exact retry of sequence 0 → deduplicated: original offset, no second append.
	rDup := produce(idempotentBatch(pid, epoch, 0, 1))
	if rDup.ErrorCode != 0 {
		t.Fatalf("duplicate produce should succeed, got error %d", rDup.ErrorCode)
	}
	if rDup.BaseOffset != 0 {
		t.Fatalf("duplicate returned offset %d, want the original 0", rDup.BaseOffset)
	}
	if leo() != 1 {
		t.Fatalf("duplicate appended a second copy (leo=%d, want 1)", leo())
	}

	// In-order next batch (sequence 1) → appended at offset 1.
	r1 := produce(idempotentBatch(pid, epoch, 1, 1))
	if r1.ErrorCode != 0 || r1.BaseOffset != 1 || leo() != 2 {
		t.Fatalf("in-order produce: code=%d offset=%d leo=%d, want 0/1/2", r1.ErrorCode, r1.BaseOffset, leo())
	}

	// A gap (sequence 5 when 2 is expected) → OUT_OF_ORDER, nothing appended.
	rGap := produce(idempotentBatch(pid, epoch, 5, 1))
	if rGap.ErrorCode != 45 { // OUT_OF_ORDER_SEQUENCE_NUMBER
		t.Fatalf("out-of-order produce error = %d, want 45", rGap.ErrorCode)
	}
	if leo() != 2 {
		t.Fatalf("out-of-order batch was appended (leo=%d, want 2)", leo())
	}
}
