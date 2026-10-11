package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Special ListOffsets timestamps.
const (
	listOffsetsLatest   = -1 // newest offset (the LEO).
	listOffsetsEarliest = -2 // oldest retained offset (the log-start offset).
	listOffsetsMaxTime  = -3 // the offset of the record with the largest timestamp (v7+).
)

// handleListOffsets answers earliest, latest and max-timestamp queries and, for a real
// timestamp, the offset of the first record at or after it (offsetsForTimes, "replay
// from ten minutes ago", Flink's start-from-timestamp). A timestamp after every
// record is offset -1 with timestamp -1 for v1 and later, as in Kafka, and the log end
// for v0, which has no way to say "none".
func (b *Broker) handleListOffsets(ctx context.Context, req *protocol.Request, lr *kmsg.ListOffsetsRequest, resp *kmsg.ListOffsetsResponse) error {
	for _, t := range lr.Topics {
		rt := kmsg.NewListOffsetsResponseTopic()
		rt.Topic = t.Topic
		name, topic, ok := b.resolveTopic(t.Topic, [16]byte{})
		authorized := b.canTopic(req.Conn, t.Topic, opDescribe)

		// (Apache Kafka answers a partition named twice in one topic once, with
		// INVALID_REQUEST. This broker answers each entry, because the console asks
		// for a partition's earliest and latest offset as two entries of one request;
		// docs/limitations.md lists it, and conformance/scenarios.go records it.)
		for _, part := range t.Partitions {
			rp := kmsg.NewListOffsetsResponseTopicPartition()
			rp.Partition = part.Partition
			rp.Timestamp = -1
			rp.LeaderEpoch = -1

			if !authorized {
				rp.ErrorCode = kerr.TopicAuthorizationFailed.Code
				rt.Partitions = append(rt.Partitions, rp)
				continue
			}

			if !ok || part.Partition < 0 || part.Partition >= topic.Partitions {
				rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				rt.Partitions = append(rt.Partitions, rp)
				continue
			}
			// The isolation level is read_uncommitted (0) or read_committed (1);
			// Kafka's answer to anything else is an unexplained server error.
			if lr.IsolationLevel != 0 && lr.IsolationLevel != 1 {
				rp.ErrorCode = kerr.UnknownServerError.Code
				rt.Partitions = append(rt.Partitions, rp)
				continue
			}
			// A leader epoch from the future is one this broker has not had; epoch 0
			// is the only one a single node ever has.
			if part.CurrentLeaderEpoch > 0 {
				rp.ErrorCode = kerr.UnknownLeaderEpoch.Code
				rt.Partitions = append(rt.Partitions, rp)
				continue
			}
			// -4 (earliest local) and -5 (latest tiered) came with v8 and v9, which
			// are not offered; anything else below -3 is not a timestamp at all.
			if part.Timestamp < listOffsetsMaxTime || part.Timestamp == listOffsetsMaxTime && req.Header.APIVersion < 7 {
				rp.ErrorCode = kerr.UnsupportedVersion.Code
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
				// A read-committed consumer's "latest" is the last stable offset: what it
				// may read, not what has been written.
				if lr.IsolationLevel == 1 {
					if lso, err := b.lastStableOffset(name, part.Partition, end); err != nil {
						rp.ErrorCode = b.storageFault("list offsets: last stable offset", err, "topic", name, "partition", part.Partition)
					} else {
						rp.Offset = lso
					}
				}
			case listOffsetsEarliest:
				rp.Offset = start
			default:
				off, ts, found, err := b.offsetByTime(name, part.Partition, part.Timestamp, start, end)
				if err != nil {
					rp.ErrorCode = b.storageFault("list offsets: timestamp lookup", err, "topic", name, "partition", part.Partition)
					rt.Partitions = append(rt.Partitions, rp)
					continue
				}
				if found && lr.IsolationLevel == 1 {
					// Under read-committed nothing at or above the last stable offset
					// exists yet, whatever its timestamp.
					if lso, err := b.lastStableOffset(name, part.Partition, end); err == nil && off >= lso {
						found = false
					}
				}
				switch {
				case found:
					rp.Offset, rp.Timestamp = off, ts
				case req.Header.APIVersion == 0:
					rp.Offset = end
				default:
					rp.Offset = -1
				}
			}
			// v0 answers with a list of offsets ("up to MaxNumOffsets before the
			// timestamp") instead of a single field; leaving it empty told a v0
			// client the partition had no offsets at all.
			if req.Header.APIVersion == 0 {
				rp.OldStyleOffsets = []int64{rp.Offset}
			}
			// An answer that found an offset carries the leader epoch it was found
			// under: 0, the only one a single node has. "Nothing there" has none.
			if rp.ErrorCode == 0 && rp.Offset >= 0 {
				rp.LeaderEpoch = 0
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return nil
}

// offsetByTime answers a ListOffsets timestamp query for one partition. An empty
// partition (or one with nothing at or after the timestamp) is not found; a partition
// with no data to open has nothing to ask.
func (b *Broker) offsetByTime(topic string, partition int32, timestamp, start, end int64) (offset, ts int64, found bool, err error) {
	if start >= end {
		return -1, -1, false, nil
	}
	p, err := b.logs.ExistingPartition(topic, partition)
	if err != nil {
		return 0, 0, false, err
	}
	if timestamp == listOffsetsMaxTime {
		return p.OffsetOfMaxTimestamp()
	}
	return p.OffsetForTimestamp(timestamp)
}

// lastStableOffset is a partition's last stable offset. A partition that has no
// data on disk to open has none open, and its answer is the end.
func (b *Broker) lastStableOffset(topic string, partition int32, end int64) (int64, error) {
	if end == 0 {
		return 0, nil
	}
	p, err := b.logs.ExistingPartition(topic, partition)
	if err != nil {
		return 0, err
	}
	return p.LastStableOffset()
}
