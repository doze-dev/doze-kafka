package broker

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
)

const defaultFetchMaxBytes = 1 << 20 // 1 MiB when a client sets no bound.

// handleFetch serves raw record batches from each partition's fetch offset,
// resolving topic IDs (modern versions address topics by UUID). It long-polls: if
// nothing is available it waits up to MaxWaitMillis for a produce, then returns
// whatever accumulated. highWatermark and logStartOffset come straight from the
// partition (single-node: highWatermark == LEO).
func (b *Broker) handleFetch(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	fr := kreq.(*kmsg.FetchRequest)

	minBytes := max(int(fr.MinBytes), 1)
	deadline := time.Now().Add(time.Duration(fr.MaxWaitMillis) * time.Millisecond)

	for {
		resp, total := b.buildFetchResponse(fr, req.Header.APIVersion)
		if total >= minBytes || fr.MaxWaitMillis <= 0 || !time.Now().Before(deadline) {
			return protocol.EncodeResponse(req.Header, resp), nil
		}
		// Wait for a produce or the deadline, then re-read.
		select {
		case <-b.produce.wait():
		case <-time.After(time.Until(deadline)):
		case <-ctx.Done():
			return protocol.EncodeResponse(req.Header, resp), nil
		}
	}
}

// buildFetchResponse reads every requested partition once and returns the response
// plus the total record bytes gathered (used to decide long-poll satisfaction).
func (b *Broker) buildFetchResponse(fr *kmsg.FetchRequest, version int16) (*kmsg.FetchResponse, int) {
	resp := kmsg.NewPtrFetchResponse()
	resp.SetVersion(version)

	total := 0
	for _, t := range fr.Topics {
		rt := kmsg.NewFetchResponseTopic()
		rt.TopicID = t.TopicID
		name, topic, ok := b.resolveTopic(t.Topic, t.TopicID)
		rt.Topic = name

		for _, part := range t.Partitions {
			rp := kmsg.NewFetchResponseTopicPartition()
			rp.Partition = part.Partition

			if !ok || part.Partition < 0 || part.Partition >= topic.Partitions {
				rp.ErrorCode = int16(b.unknownTopicErr(t.Topic))
				rt.Partitions = append(rt.Partitions, rp)
				continue
			}
			total += b.readPartition(name, part, fr, &rp)
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return resp, total
}

// readPartition fills rp from one partition and returns the record bytes read.
func (b *Broker) readPartition(topic string, part kmsg.FetchRequestTopicPartition, fr *kmsg.FetchRequest, rp *kmsg.FetchResponseTopicPartition) int {
	plog, err := b.logs.Partition(topic, part.Partition)
	if err != nil {
		rp.ErrorCode = int16(protocol.ErrUnknownServer)
		return 0
	}

	maxBytes := int64(part.PartitionMaxBytes)
	if maxBytes <= 0 {
		maxBytes = int64(fr.MaxBytes)
	}
	if maxBytes <= 0 {
		maxBytes = defaultFetchMaxBytes
	}

	data, err := plog.Read(part.FetchOffset, maxBytes)
	rp.HighWatermark = plog.LogEndOffset()
	rp.LastStableOffset = rp.HighWatermark
	rp.LogStartOffset = plog.LogStartOffset()

	switch {
	case errors.Is(err, log.ErrOffsetOutOfRange):
		rp.ErrorCode = int16(protocol.ErrOffsetOutOfRange)
		return 0
	case err != nil:
		rp.ErrorCode = int16(protocol.ErrUnknownServer)
		return 0
	}
	rp.RecordBatches = data
	return len(data)
}

// unknownTopicErr picks the right "unknown topic" code depending on whether the
// request addressed the topic by name or by id.
func (b *Broker) unknownTopicErr(nameInRequest string) protocol.ErrorCode {
	if nameInRequest == "" {
		return protocol.ErrUnknownTopicID
	}
	return protocol.ErrUnknownTopicOrPart
}
