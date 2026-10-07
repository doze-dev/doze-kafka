package broker

import (
	"context"
	"errors"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"

	"github.com/twmb/franz-go/pkg/kerr"
)

// handleDeleteRecords moves each named partition's log-start offset up to the
// offset given (-1: the high watermark, which empties it). Kafka Streams uses it to
// purge repartition topics it has finished with, and test suites use it to clear a
// topic between tests without the delete-and-recreate that changes the topic's id
// and drops its consumers' committed offsets.
//
// An offset at or below the current start is not an error: the answer is the start
// as it is. One above the log end is OFFSET_OUT_OF_RANGE, and the broker does not
// guess what was meant. A fetch below the new start is OFFSET_OUT_OF_RANGE from then
// on, as it is after retention.
func (b *Broker) handleDeleteRecords(ctx context.Context, req *protocol.Request, dr *kmsg.DeleteRecordsRequest, resp *kmsg.DeleteRecordsResponse) error {
	for _, t := range dr.Topics {
		rt := kmsg.NewDeleteRecordsResponseTopic()
		rt.Topic = t.Topic
		name, topic, ok := b.resolveTopic(t.Topic, [16]byte{})
		for _, p := range t.Partitions {
			rp := kmsg.NewDeleteRecordsResponseTopicPartition()
			rp.Partition = p.Partition
			rp.LowWatermark = -1
			switch {
			case !ok || p.Partition < 0 || p.Partition >= topic.Partitions:
				rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
			default:
				start, err := b.logs.DeleteBefore(name, p.Partition, p.Offset)
				switch {
				case errors.Is(err, log.ErrOffsetOutOfRange):
					rp.ErrorCode = kerr.OffsetOutOfRange.Code
				case errors.Is(err, log.ErrNoSuchPartition):
					rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				case err != nil:
					rp.ErrorCode = b.storageFault("delete records", err, "topic", name, "partition", p.Partition)
				default:
					rp.LowWatermark = start
				}
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return nil
}
