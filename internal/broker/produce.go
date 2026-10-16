package broker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"

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
	kreq, err := req.Decode()
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
		return req.Reply(resp), nil
	}

	// Before any partition is handled Kafka looks at the first batch of each, to see
	// whether the request is transactional, and a request it cannot even take apart
	// there fails whole; so does one with a transactional batch and no transactional
	// id (recorded: produce-validation, records-20-zero-bytes, magic-3-in-v2-layout,
	// transactional-bit-no-producer-id).
	var first [][]byte
	for _, t := range pr.Topics {
		for _, part := range t.Partitions {
			first = append(first, part.Records)
		}
	}
	transactional, refusal := log.RequestRefusal(first)
	if refusal == nil && transactional && pr.TransactionID == nil {
		refusal = &log.Refusal{Kind: refuseTransactionalID, BeforeLog: true,
			Reason: "a transactional record batch in a request that names no transactional id"}
	}
	if refusal != nil {
		for _, t := range pr.Topics {
			rt := kmsg.NewProduceResponseTopic()
			rt.Topic, rt.TopicID = t.Topic, t.TopicID
			for _, part := range t.Partitions {
				rp := kmsg.NewProduceResponseTopicPartition()
				rp.Partition = part.Partition
				refuseProduce(&rp, refusal, nil)
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
		if pr.Acks == 0 {
			return nil, fmt.Errorf("produce with acks=0 was refused (%s)", refusal.Reason)
		}
		return req.Reply(resp), nil
	}

	producedTopics := map[string]struct{}{}
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
			case !ok || part.Partition < 0 || part.Partition >= topic.Partitions:
				rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				rp.BaseOffset = -1 // as Apache Kafka answers it (claim: traffic R7).
			default:
				wrote, answered := b.appendRecords(req.Conn, req.Header.APIVersion, pr.TransactionID, name, topic.Config, part, &rp)
				if wrote {
					producedTopics[name] = struct{}{}
				}
				// A partition is synced for acks=all when this request wrote to it
				// or when its answer rests on bytes an earlier request wrote (a
				// duplicate retry): acknowledging those is the same promise.
				if wrote || (answered && rp.ErrorCode == 0) {
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
		// acks=0 has no response to carry an error, so the only way to tell the
		// producer its data was not taken (a refused batch, an unknown topic or
		// partition, storage that could not take it) is to drop the connection.
		// Apache Kafka does so for any partition's error (recorded:
		// conformance/recordings/produce-acks-zero.json); staying silent lost the
		// records unnoticed.
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if p.ErrorCode != 0 {
					return nil, fmt.Errorf("produce with acks=0 was refused for %s-%d (%s)", t.Topic, p.Partition, kerr.ErrorForCode(p.ErrorCode))
				}
			}
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
	return req.Reply(resp), nil
}

// refuseTransactionalID is the one refusal of a Produce request that the log does
// not make: a transactional batch with no transactional id to authorise.
const refuseTransactionalID log.RefusalKind = -1

// refusalCode is the Kafka error for each kind of refusal.
func refusalCode(k log.RefusalKind) int16 {
	switch k {
	case log.RefuseInvalidRecord:
		return kerr.InvalidRecord.Code
	case log.RefuseCorrupt:
		return kerr.CorruptMessage.Code
	case log.RefuseTooLarge:
		return kerr.MessageTooLarge.Code
	case log.RefuseTimestamp:
		return kerr.InvalidTimestamp.Code
	case log.RefuseCompression:
		return kerr.UnsupportedCompressionType.Code
	case refuseTransactionalID:
		return kerr.TransactionalIDAuthorizationFailed.Code
	}
	// A records section that cannot be decompressed: Kafka has no better name for it
	// than this (recorded: garbage, a truncated stream and a codec number nothing has
	// are all UNKNOWN_SERVER_ERROR).
	return kerr.UnknownServerError.Code
}

// refuseProduce fills a partition's answer for a batch that is not stored. plog is
// the partition's log, nil when the refusal is of the whole request.
func refuseProduce(rp *kmsg.ProduceResponseTopicPartition, r *log.Refusal, plog *log.Partition) {
	rp.ErrorCode = refusalCode(r.Kind)
	rp.BaseOffset = -1
	rp.LogAppendTime = -1
	// Kafka's answer carries the partition's log start offset once the log has been
	// asked, and -1 for a refusal made before that.
	rp.LogStartOffset = -1
	if plog != nil && !r.BeforeLog {
		rp.LogStartOffset = plog.LogStartOffset()
	}
	// Kafka's answer carries its text where it has one and null where it does not
	// (recorded: every refusal in produce-validation without a text of Kafka's).
	if r.KafkaMessage != "" {
		msg := r.KafkaMessage
		rp.ErrorMessage = &msg
	}
	for _, rec := range r.Records {
		er := kmsg.NewProduceResponseTopicPartitionErrorRecord()
		er.RelativeOffset = rec.Index
		m := rec.Message
		er.ErrorMessage = &m
		rp.ErrorRecords = append(rp.ErrorRecords, er)
	}
}

// produceRules is what a topic's configuration holds a batch to.
func (b *Broker) produceRules(version int16, topic string, partition int32, topicConfig map[string]string) log.ProduceRules {
	rules := log.ProduceRules{
		Version:        version,
		MaxBatchBytes:  b.cfg.MessageMaxBytes,
		RequireKeys:    compacts(topicConfig),
		Target:         log.TargetProducer,
		LogAppendTime:  topicConfig["message.timestamp.type"] == "LogAppendTime",
		NowMs:          time.Now().UnixMilli(),
		BeforeMaxMs:    math.MaxInt64,
		AfterMaxMs:     log.DefaultTimestampAfterMaxMs,
		TopicPartition: topic + "-" + strconv.Itoa(int(partition)),
	}
	if rules.MaxBatchBytes <= 0 {
		rules.MaxBatchBytes = log.DefaultMaxMessageBytes
	}
	// The topic's own limit on one record batch replaces the broker's
	// (message.max.bytes, Kafka's default of 1048588 unless set).
	if limit, ok := topicInt(topicConfig, "max.message.bytes"); ok {
		rules.MaxBatchBytes = limit
	}
	if ms, ok := topicInt(topicConfig, "message.timestamp.before.max.ms"); ok {
		rules.BeforeMaxMs = ms
	}
	if ms, ok := topicInt(topicConfig, "message.timestamp.after.max.ms"); ok {
		rules.AfterMaxMs = ms
	}
	switch c := topicConfig["compression.type"]; c {
	case "", "producer":
	case "uncompressed":
		rules.Target = 0
	default:
		rules.Target = log.CodecByName(c)
	}
	return rules
}

// appendRecords appends one partition's records and fills rp. A non-idempotent
// payload (producer id -1) takes the plain fast path; an idempotent one goes through
// sequence dedup. It reports whether any new data was written, and whether the
// answer rests on data already in the log without having written it (a duplicate
// retry): that data may still be in the page cache, so an acks=all answer must
// sync it too (BAD-1).
func (b *Broker) appendRecords(conn *protocol.Conn, version int16, txnID *string, topic string, topicConfig map[string]string, part kmsg.ProduceRequestTopicPartition, rp *kmsg.ProduceResponseTopicPartition) (wrote, answered bool) {
	plog, err := b.logs.ExistingPartition(topic, part.Partition)
	if errors.Is(err, log.ErrNoSuchPartition) {
		// The topic was deleted between resolving it and opening its log.
		rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
		return false, false
	}
	if err != nil {
		rp.ErrorCode = b.storageFault("produce: open partition", err, "topic", topic, "partition", part.Partition)
		return false, false
	}

	// Everything Kafka holds a batch to, before a byte of it is written: one batch,
	// its header, its checksum, and every record of it, read through whatever it is
	// compressed with (log.CheckProduce, which is a stream and holds no payload). A
	// batch that is refused is not stored, on any kind of topic; a batch that was
	// stored used to be refused here only on a topic that compacts.
	rules := b.produceRules(version, topic, part.Partition, topicConfig)
	accepted, refusal := log.CheckProduce(part.Records, rules)
	if refusal != nil {
		refuseProduce(rp, refusal, plog)
		return false, false
	}
	batch, hdr := accepted.Batch, accepted.Header

	// message.timestamp.type=LogAppendTime: the records take the broker's clock.
	appendTime := int64(-1)
	if rules.LogAppendTime {
		appendTime = rules.NowMs
		log.StampLogAppendTime(batch, appendTime)
	}

	switch {
	case hdr.ProducerID < 0:
		wrote = b.appendPlain(plog, batch, rp)
		answered = wrote
	case hdr.Transactional:
		// A transactional producer needs Write on its transactional id as well, and
		// the transaction must be open on this partition for the length of the append.
		if txnID != nil && !b.canTxnID(conn, *txnID, opWrite) {
			rp.ErrorCode, rp.BaseOffset = kerr.TransactionalIDAuthorizationFailed.Code, -1
			return false, false
		}
		unlock, code, msg := b.lockForProduce(txnID, hdr, topic, part.Partition)
		if code != 0 {
			rp.ErrorCode, rp.BaseOffset = code, -1
			rp.ErrorMessage = &msg
			return false, false
		}
		defer unlock()
		wrote, answered = b.appendIdempotent(topic, part.Partition, plog, batch, rp)
	default:
		wrote, answered = b.appendIdempotent(topic, part.Partition, plog, batch, rp)
	}
	if rp.ErrorCode == 0 && wrote {
		rp.LogAppendTime = appendTime // -1 under CreateTime: the record's own timestamp stands.
	}
	return wrote, answered
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
func (b *Broker) appendIdempotent(topic string, partition int32, plog *log.Partition, records []byte, rp *kmsg.ProduceResponseTopicPartition) (wrote, answered bool) {
	pp := b.idem.forPartition(topic, partition)
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if err := pp.loadFrom(plog); err != nil {
		rp.ErrorCode = b.storageFault("produce: read the producers' history", err, "topic", topic, "partition", partition)
		return false, false
	}

	// A batch refused for its sequence or its epoch has no offset (-1), and the
	// answer carries the partition's log start offset, as Kafka's does.
	refused := func(code int16) (bool, bool) {
		rp.ErrorCode, rp.BaseOffset, rp.LogAppendTime = code, -1, -1
		rp.LogStartOffset = plog.LogStartOffset()
		return wrote, answered
	}
	var firstOffset, duplicateTime int64 = -1, -1
	for pos := 0; pos < len(records); {
		hdr, ok := log.ParseBatch(records[pos:])
		if !ok || hdr.TotalBytes <= 0 || pos+hdr.TotalBytes > len(records) {
			rp.ErrorCode = kerr.UnknownServerError.Code
			return wrote, answered
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
				return wrote, answered
			}
			pp.record(hdr.ProducerID, hdr.ProducerEpoch, firstSeq, lastSeq, off, log.MaxTimestampOf(batch))
			if firstOffset < 0 {
				firstOffset = off
			}
			wrote = true
		case seqDuplicate:
			answered = true
			if firstOffset < 0 {
				firstOffset = v.offset
				// A duplicate is answered as the batch it repeats was: its offset, and
				// the timestamp that batch was stored with (recorded: produce-validation,
				// unknown-pid-seq-5-again).
				duplicateTime = v.timestamp
			}
		case seqDuplicateOld:
			return refused(kerr.DuplicateSequenceNumber.Code)
		case seqOutOfOrder:
			return refused(kerr.OutOfOrderSequenceNumber.Code)
		case seqFenced:
			return refused(kerr.InvalidProducerEpoch.Code)
		}
		pos += hdr.TotalBytes
	}

	rp.BaseOffset = firstOffset
	rp.LogAppendTime = duplicateTime
	rp.LogStartOffset = plog.LogStartOffset()
	return wrote, answered
}
