package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// Special ListOffsets timestamps.
const (
	listOffsetsLatest   = -1 // newest offset (the LEO).
	listOffsetsEarliest = -2 // oldest retained offset (the log-start offset).
)

// handleListOffsets answers earliest/latest offset queries. A real timestamp is
// served coarsely as the earliest offset — a per-record timestamp index is not
// implemented (local dev consumers use earliest/latest almost exclusively).
func (b *Broker) handleListOffsets(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	lr := kreq.(*kmsg.ListOffsetsRequest)

	resp := kmsg.NewPtrListOffsetsResponse()
	resp.SetVersion(req.Header.APIVersion)

	for _, t := range lr.Topics {
		rt := kmsg.NewListOffsetsResponseTopic()
		rt.Topic = t.Topic
		name, topic, ok := b.resolveTopic(t.Topic, [16]byte{})

		for _, part := range t.Partitions {
			rp := kmsg.NewListOffsetsResponseTopicPartition()
			rp.Partition = part.Partition
			rp.Timestamp = -1
			rp.LeaderEpoch = -1

			if !ok || part.Partition < 0 || part.Partition >= topic.Partitions {
				rp.ErrorCode = int16(protocol.ErrUnknownTopicOrPart)
				rt.Partitions = append(rt.Partitions, rp)
				continue
			}
			// Bounds reads an unopened partition's files without opening it: a topic
			// listing asks about every partition, and opening them all for that held
			// two file descriptors per partition for the life of the broker.
			start, end, err := b.logs.Bounds(name, part.Partition)
			if err != nil {
				rp.ErrorCode = b.storageFault("list offsets: read bounds", err, "topic", name, "partition", part.Partition)
				rt.Partitions = append(rt.Partitions, rp)
				continue
			}
			switch part.Timestamp {
			case listOffsetsLatest:
				rp.Offset = end
			default: // earliest, or a real timestamp (coarse: earliest).
				rp.Offset = start
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}
