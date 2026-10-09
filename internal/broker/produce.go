package broker

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
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

	// acks is -1 (all), 0 (none) or 1 (leader). Anything else is a client bug that
	// used to be treated as acks=1 without a word.
	if pr.Acks < -1 || pr.Acks > 1 {
		for _, t := range pr.Topics {
			rt := kmsg.NewProduceResponseTopic()
			rt.Topic, rt.TopicID = t.Topic, t.TopicID
			for _, part := range t.Partitions {
				rp := kmsg.NewProduceResponseTopicPartition()
				rp.Partition, rp.BaseOffset = part.Partition, -1
				rp.ErrorCode = kerr.InvalidRequiredAcks.Code
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
		return protocol.EncodeResponse(req.Header, resp), nil
	}

	producedTopics := map[string]struct{}{}
	refused := false // a partition's payload was rejected as invalid.
	// Partitions written by this request, with where their response entries sit,
	// so an fsync failure can be reported against exactly those entries.
	type slot struct{ topic, part int }
	written := map[topicPartition][]slot{}

	for _, t := range pr.Topics {
		rt := kmsg.NewProduceResponseTopic()
		rt.Topic = t.Topic
		rt.TopicID = t.TopicID
		name, topic, ok := b.resolveTopic(t.Topic, t.TopicID)
		// Authorisation comes before existence, as in Kafka: a principal that may not
		// write to a topic is not told whether it exists.
		authorized := b.canTopic(req.Conn, cmp.Or(name, t.Topic), opWrite)

		for _, part := range t.Partitions {
			rp := kmsg.NewProduceResponseTopicPartition()
			rp.Partition = part.Partition

			switch {
			case !authorized:
				rp.ErrorCode = kerr.TopicAuthorizationFailed.Code
				rp.BaseOffset = -1
				refused = true
			case !ok || part.Partition < 0 || part.Partition >= topic.Partitions:
				rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
			default:
				wrote := b.appendRecords(req.Conn, pr.TransactionID, name, topic.Config, part, &rp)
				refused = refused || rp.ErrorCode == kerr.CorruptMessage.Code || rp.ErrorCode == kerr.MessageTooLarge.Code
				if wrote {
					producedTopics[name] = struct{}{}
					tp := topicPartition{name, part.Partition}
					written[tp] = append(written[tp], slot{len(resp.Topics), len(rt.Partitions)})
				}
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}

	for topic := range producedTopics {
		b.produce.signal(topic) // wake the fetchers reading this topic.
	}
	if pr.Acks == 0 {
		if refused {
			// acks=0 has no response to carry the error, so the only way to tell
			// the producer its data was not taken is to drop the connection, as a
			// real broker does. Staying silent lost the records unnoticed.
			return nil, fmt.Errorf("produce with acks=0 carried a record batch the broker refused")
		}
		return nil, nil // fire-and-forget: the client expects no response.
	}
	if pr.Acks == -1 { // acks=all: make the acknowledgement durable.
		for tp, slots := range written {
			err := b.logs.SyncPartition(tp.topic, tp.partition)
			if err == nil {
				continue
			}
			// acks=all is a promise that the record is on disk. If the disk says
			// otherwise the producer must hear it, not be told "success" about data
			// that may vanish: it will retry (idempotently), and the failure is
			// visible instead of silent.
			b.log.Error("durable ack: fsync failed; reporting the produce as failed", "topic", tp.topic, "partition", tp.partition, "err", err)
			msg := "fsync failed: " + err.Error()
			for _, s := range slots {
				rp := &resp.Topics[s.topic].Partitions[s.part]
				rp.ErrorCode = kerr.KafkaStorageError.Code
				rp.ErrorMessage = &msg
				rp.BaseOffset = -1
			}
		}
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// appendRecords appends one partition's records and fills rp. A non-idempotent
// payload (producer id -1) takes the plain fast path; an idempotent one goes through
// sequence dedup. It reports whether any new data was written.
func (b *Broker) appendRecords(conn *protocol.Conn, txnID *string, topic string, topicConfig map[string]string, part kmsg.ProduceRequestTopicPartition, rp *kmsg.ProduceResponseTopicPartition) bool {
	plog, err := b.logs.ExistingPartition(topic, part.Partition)
	if errors.Is(err, log.ErrNoSuchPartition) {
		// The topic was deleted between resolving it and opening its log.
		rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
		return false
	}
	if err != nil {
		rp.ErrorCode = b.storageFault("produce: open partition", err, "topic", topic, "partition", part.Partition)
		return false
	}

	// Validate every batch before writing any: shape and CRC. A bad batch must not
	// be stored (every consumer would then fail on it, forever) and must not leave
	// the batches before it half-appended.
	if err := log.ValidateBatches(part.Records); err != nil {
		rp.ErrorCode = kerr.CorruptMessage.Code
		msg := err.Error()
		rp.ErrorMessage = &msg
		rp.BaseOffset = -1
		return false
	}

	// The topic's own limit on one record batch, if it has set one. (There is no
	// broker-wide default: a development broker that refused every batch over
	// Kafka's 1 MB would surprise more people than it helped.)
	if limit, ok := topicInt(topicConfig, "max.message.bytes"); ok {
		if largest := log.LargestBatch(part.Records); largest > limit {
			rp.ErrorCode = kerr.MessageTooLarge.Code
			msg := fmt.Sprintf("a record batch of %d bytes is larger than this topic's max.message.bytes (%d)", largest, limit)
			rp.ErrorMessage = &msg
			rp.BaseOffset = -1
			return false
		}
	}
	// message.timestamp.type=LogAppendTime: the records take the broker's clock.
	appendTime := int64(-1)
	if topicConfig["message.timestamp.type"] == "LogAppendTime" {
		appendTime = time.Now().UnixMilli()
		log.StampLogAppendTime(part.Records, appendTime)
	}

	var wrote bool
	if hdr, ok := log.ParseBatch(part.Records); !ok || hdr.ProducerID < 0 {
		wrote = b.appendPlain(plog, part.Records, rp)
	} else if hdr.Transactional {
		// A transactional producer needs Write on its transactional id as well, and
		// the transaction must be open on this partition for the length of the append.
		if txnID != nil && !b.canTxnID(conn, *txnID, opWrite) {
			rp.ErrorCode, rp.BaseOffset = kerr.TransactionalIDAuthorizationFailed.Code, -1
			return false
		}
		unlock, code, msg := b.lockForProduce(txnID, hdr, topic, part.Partition)
		if code != 0 {
			rp.ErrorCode, rp.BaseOffset = code, -1
			rp.ErrorMessage = &msg
			return false
		}
		defer unlock()
		wrote = b.appendIdempotent(topic, part.Partition, plog, part.Records, rp)
	} else {
		wrote = b.appendIdempotent(topic, part.Partition, plog, part.Records, rp)
	}
	if rp.ErrorCode == 0 {
		rp.LogAppendTime = appendTime // -1 under CreateTime: the record's own timestamp stands.
	}
	return wrote
}

// topicInt reads a numeric topic config, if the topic has set it.
func topicInt(topicConfig map[string]string, key string) (int64, bool) {
	v, ok := topicConfig[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	return n, err == nil
}

// appendPlain appends the payload verbatim (no dedup) — the path for
// non-idempotent producers.
func (b *Broker) appendPlain(plog *log.Partition, records []byte, rp *kmsg.ProduceResponseTopicPartition) bool {
	baseOffset, err := plog.Append(records)
	if err != nil {
		rp.ErrorCode = b.storageFault("produce: append", err)
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
			rp.ErrorCode = kerr.UnknownServerError.Code
			return wrote
		}
		batch := records[pos : pos+hdr.TotalBytes]
		firstSeq := hdr.BaseSequence
		lastSeq := lastSeqOf(hdr.BaseSequence, hdr.RecordCount)

		v := pp.check(hdr.ProducerID, hdr.ProducerEpoch, firstSeq, lastSeq)
		switch v.kind {
		case seqAccept:
			off, err := plog.Append(batch)
			if err != nil {
				rp.ErrorCode = b.storageFault("produce: append (idempotent)", err, "topic", topic, "partition", partition)
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
		case seqDuplicateOld:
			rp.ErrorCode = kerr.DuplicateSequenceNumber.Code
			return wrote
		case seqOutOfOrder:
			rp.ErrorCode = kerr.OutOfOrderSequenceNumber.Code
			return wrote
		case seqFenced:
			rp.ErrorCode = kerr.InvalidProducerEpoch.Code
			return wrote
		}
		pos += hdr.TotalBytes
	}

	rp.BaseOffset = firstOffset
	rp.LogAppendTime = -1
	rp.LogStartOffset = plog.LogStartOffset()
	return wrote
}
