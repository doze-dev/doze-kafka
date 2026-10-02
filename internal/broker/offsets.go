package broker

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// handleOffsetCommit persists a group's committed offsets for topic-partitions to
// the metadata store (this replaces Kafka's internal __consumer_offsets topic). The
// commit is fenced by generation/memberID — a stale generation or unknown member is
// rejected per-partition (validateCommit lets simple consumers with no group
// through). Committed offsets are the only durable consumer-group state.
func (b *Broker) handleOffsetCommit(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	oc := kreq.(*kmsg.OffsetCommitRequest)

	fence := b.groups.validateCommit(oc.Group, oc.MemberID, oc.Generation)

	resp := kmsg.NewPtrOffsetCommitResponse()
	resp.SetVersion(req.Header.APIVersion)

	for _, t := range oc.Topics {
		rt := kmsg.NewOffsetCommitResponseTopic()
		rt.Topic = t.Topic
		rt.TopicID = t.TopicID
		name, topic, ok := b.resolveTopic(t.Topic, t.TopicID)

		for _, p := range t.Partitions {
			rp := kmsg.NewOffsetCommitResponseTopicPartition()
			rp.Partition = p.Partition
			switch {
			case fence != int16(protocol.ErrNone):
				rp.ErrorCode = fence
			case !ok || p.Partition < 0 || p.Partition >= topic.Partitions:
				rp.ErrorCode = int16(protocol.ErrUnknownTopicOrPart)
			default:
				md := ""
				if p.Metadata != nil {
					md = *p.Metadata
				}
				if err := b.meta.CommitOffset(oc.Group, name, p.Partition, p.Offset, md); err != nil {
					rp.ErrorCode = b.storageFault("offset commit", err, "group", oc.Group, "topic", name, "partition", p.Partition)
				}
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// handleOffsetFetch returns a group's committed offsets, or -1 for any partition
// with no commit so the client applies its auto.offset.reset. It answers both the
// v0-v7 single-group form (top-level Group + Topics) and the v8+ batched form
// (Groups). A nil topic list means "all committed offsets for the group".
func (b *Broker) handleOffsetFetch(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	of := kreq.(*kmsg.OffsetFetchRequest)

	resp := kmsg.NewPtrOffsetFetchResponse()
	resp.SetVersion(req.Header.APIVersion)

	if len(of.Groups) > 0 { // v8+ batched form.
		for _, g := range of.Groups {
			rg := kmsg.NewOffsetFetchResponseGroup()
			rg.Group = g.Group
			partitions, err := b.groupOffsets(g.Group, groupTopicNames(g.Topics), g.Topics == nil)
			if err != nil {
				return nil, err
			}
			for _, po := range partitions {
				rt := findOrAppendGroupTopic(&rg, po.topic)
				gp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
				gp.Partition = po.partition
				gp.Offset = po.offset
				gp.Metadata = po.metadata
				rt.Partitions = append(rt.Partitions, gp) // rt points into rg.Topics; mutation sticks.
			}
			resp.Groups = append(resp.Groups, rg)
		}
		return protocol.EncodeResponse(req.Header, resp), nil
	}

	// v0-v7 single-group form.
	partitions, err := b.groupOffsets(of.Group, requestTopicNames(of.Topics), of.Topics == nil)
	if err != nil {
		return nil, err
	}
	for _, po := range partitions {
		rt := findOrAppendTopic(resp, po.topic)
		tp := kmsg.NewOffsetFetchResponseTopicPartition()
		tp.Partition = po.partition
		tp.Offset = po.offset
		tp.Metadata = po.metadata
		rt.Partitions = append(rt.Partitions, tp) // rt points into resp.Topics; mutation sticks.
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// partitionOffset is one resolved (topic, partition) committed offset. A partition
// with no commit carries offset -1 (the client then applies auto.offset.reset).
type partitionOffset struct {
	topic     string
	partition int32
	offset    int64
	metadata  *string
}

// groupOffsets resolves the committed offsets for a group. When fetchAll is true
// (the request named no topics) it returns every committed offset via
// meta.ListOffsets; otherwise it returns one entry per requested (topic, partition),
// filling in -1 where nothing is committed.
func (b *Broker) groupOffsets(group string, topics map[string][]int32, fetchAll bool) ([]partitionOffset, error) {
	if fetchAll {
		entries, err := b.meta.ListOffsets(group)
		if err != nil {
			return nil, err
		}
		out := make([]partitionOffset, 0, len(entries))
		for _, e := range entries {
			po := partitionOffset{topic: e.Topic, partition: e.Partition, offset: e.Offset}
			if e.Metadata != "" {
				md := e.Metadata
				po.metadata = &md
			}
			out = append(out, po)
		}
		return out, nil
	}

	var out []partitionOffset
	for topic, parts := range topics {
		for _, part := range parts {
			po := partitionOffset{topic: topic, partition: part, offset: -1}
			offset, ok, err := b.meta.FetchOffset(group, topic, part)
			if err != nil {
				return nil, err
			}
			if ok {
				po.offset = offset
			}
			out = append(out, po)
		}
	}
	return out, nil
}

func requestTopicNames(topics []kmsg.OffsetFetchRequestTopic) map[string][]int32 {
	m := make(map[string][]int32, len(topics))
	for _, t := range topics {
		m[t.Topic] = t.Partitions
	}
	return m
}

func groupTopicNames(topics []kmsg.OffsetFetchRequestGroupTopic) map[string][]int32 {
	m := make(map[string][]int32, len(topics))
	for _, t := range topics {
		m[t.Topic] = t.Partitions
	}
	return m
}

// findOrAppendTopic returns the v0-v7 response topic entry for name, appending a
// new one if absent, and returns a pointer to it. Callers must use the pointer
// before the next append to resp.Topics (a reallocation would strand it).
func findOrAppendTopic(resp *kmsg.OffsetFetchResponse, name string) *kmsg.OffsetFetchResponseTopic {
	for i := range resp.Topics {
		if resp.Topics[i].Topic == name {
			return &resp.Topics[i]
		}
	}
	rt := kmsg.NewOffsetFetchResponseTopic()
	rt.Topic = name
	resp.Topics = append(resp.Topics, rt)
	return &resp.Topics[len(resp.Topics)-1]
}

// findOrAppendGroupTopic returns the response topic entry for name, appending a new
// one if absent, and returns a pointer to it.
func findOrAppendGroupTopic(rg *kmsg.OffsetFetchResponseGroup, name string) *kmsg.OffsetFetchResponseGroupTopic {
	for i := range rg.Topics {
		if rg.Topics[i].Topic == name {
			return &rg.Topics[i]
		}
	}
	rt := kmsg.NewOffsetFetchResponseGroupTopic()
	rt.Topic = name
	rg.Topics = append(rg.Topics, rt)
	return &rg.Topics[len(rg.Topics)-1]
}
