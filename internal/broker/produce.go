package broker

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
)

// handleProduce appends each partition's record batches to its log: stamp the base
// offset, append the raw bytes verbatim, and advance the LEO by the record count
// from the batch header.
//
// Two guarantees beyond a plain append:
//   - Idempotence. An idempotent producer's retried batch (same producer id, epoch,
//     and base sequence) is deduplicated — it returns the original offset instead of
//     appending a second copy. See idempotency.go.
//   - Durable acks. acks=all (-1) fsyncs the partitions it wrote before replying, so
//     an acknowledged record is on disk, not merely in the OS page cache. acks=1 is
//     the fast path (page cache); acks=0 asks for no reply, so we send none.
func (b *Broker) handleProduce(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	pr := kreq.(*kmsg.ProduceRequest)

	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(req.Header.APIVersion)

	var produced bool
	written := map[topicPartition]struct{}{} // partitions to fsync for a durable ack.

	for _, t := range pr.Topics {
		rt := kmsg.NewProduceResponseTopic()
		rt.Topic = t.Topic
		rt.TopicID = t.TopicID
		name, topic, ok := b.resolveTopic(t.Topic, t.TopicID)

		for _, part := range t.Partitions {
			rp := kmsg.NewProduceResponseTopicPartition()
			rp.Partition = part.Partition

			switch {
			case !ok || part.Partition < 0 || part.Partition >= topic.Partitions:
				rp.ErrorCode = int16(protocol.ErrUnknownTopicOrPart)
			default:
				if b.appendRecords(name, part, &rp) {
					produced = true
					written[topicPartition{name, part.Partition}] = struct{}{}
				}
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}

	if produced {
		b.produce.signal() // wake long-polling fetchers.
	}
	if pr.Acks == 0 {
		return nil, nil // fire-and-forget: the client expects no response.
	}
	if pr.Acks == -1 { // acks=all: make the acknowledgement durable.
		for tp := range written {
			if err := b.logs.SyncPartition(tp.topic, tp.partition); err != nil {
				b.log.Warn("durable ack fsync failed", "topic", tp.topic, "partition", tp.partition, "err", err)
			}
		}
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// appendRecords appends one partition's records and fills rp. A non-idempotent
// payload (producer id -1) takes the plain fast path; an idempotent one goes through
// sequence dedup. It reports whether any new data was written.
func (b *Broker) appendRecords(topic string, part kmsg.ProduceRequestTopicPartition, rp *kmsg.ProduceResponseTopicPartition) bool {
	plog, err := b.logs.Partition(topic, part.Partition)
	if err != nil {
		rp.ErrorCode = int16(protocol.ErrUnknownServer)
		return false
	}

	hdr, ok := log.ParseBatch(part.Records)
	if !ok || hdr.ProducerID < 0 {
		return b.appendPlain(plog, part.Records, rp)
	}
	return b.appendIdempotent(topic, part.Partition, plog, part.Records, rp)
}

// appendPlain appends the payload verbatim (no dedup) — the path for
// non-idempotent producers.
func (b *Broker) appendPlain(plog *log.Partition, records []byte, rp *kmsg.ProduceResponseTopicPartition) bool {
	baseOffset, err := plog.Append(records)
	if err != nil {
		rp.ErrorCode = int16(protocol.ErrUnknownServer)
		msg := err.Error()
		rp.ErrorMessage = &msg
		return false
	}
	rp.BaseOffset = baseOffset
	rp.LogAppendTime = -1 // CreateTime: the record's own timestamp stands.
	rp.LogStartOffset = plog.LogStartOffset()
	return len(records) > 0
}

// appendIdempotent walks the payload batch by batch under the partition's producer
// lock, deduplicating retries and rejecting out-of-order or fenced batches. The
// check→append→record sequence is atomic per partition so two concurrent retries
// can't both append.
func (b *Broker) appendIdempotent(topic string, partition int32, plog *log.Partition, records []byte, rp *kmsg.ProduceResponseTopicPartition) bool {
	pp := b.idem.forPartition(topic, partition)
	pp.mu.Lock()
	defer pp.mu.Unlock()

	var (
		firstOffset int64 = -1
		wrote       bool
	)
	for pos := 0; pos < len(records); {
		hdr, ok := log.ParseBatch(records[pos:])
		if !ok || hdr.TotalBytes <= 0 || pos+hdr.TotalBytes > len(records) {
			rp.ErrorCode = int16(protocol.ErrUnknownServer)
			return wrote
		}
		batch := records[pos : pos+hdr.TotalBytes]
		firstSeq := hdr.BaseSequence
		lastSeq := hdr.BaseSequence + hdr.RecordCount - 1

		v := pp.check(hdr.ProducerID, hdr.ProducerEpoch, firstSeq, lastSeq)
		switch v.kind {
		case seqAccept:
			off, err := plog.Append(batch)
			if err != nil {
				rp.ErrorCode = int16(protocol.ErrUnknownServer)
				msg := err.Error()
				rp.ErrorMessage = &msg
				return wrote
			}
			pp.record(hdr.ProducerID, hdr.ProducerEpoch, firstSeq, lastSeq, off)
			if firstOffset < 0 {
				firstOffset = off
			}
			wrote = true
		case seqDuplicate:
			if firstOffset < 0 {
				firstOffset = v.offset
			}
		case seqOutOfOrder:
			rp.ErrorCode = int16(protocol.ErrOutOfOrderSequence)
			return wrote
		case seqFenced:
			rp.ErrorCode = int16(protocol.ErrInvalidProducerEpoch)
			return wrote
		}
		pos += hdr.TotalBytes
	}

	rp.BaseOffset = firstOffset
	rp.LogAppendTime = -1
	rp.LogStartOffset = plog.LogStartOffset()
	return wrote
}
