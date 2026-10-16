package broker

import (
	"cmp"
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"

	"github.com/twmb/franz-go/pkg/kerr"
)

// handleOffsetCommit persists a group's committed offsets for topic-partitions to
// the metadata store (this replaces Kafka's internal __consumer_offsets topic). The
// commit is fenced by generation/memberID — a stale generation or unknown member is
// rejected per-partition (validateCommit lets simple consumers with no group
// through). Committed offsets are the only durable consumer-group state.
func (b *Broker) handleOffsetCommit(ctx context.Context, req *protocol.Request, oc *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse) error {
	fence := b.commitFence(oc)
	if !b.canGroup(req.Conn, oc.Group, opRead) {
		fence = kerr.GroupAuthorizationFailed.Code
	}

	// Validate every partition first and write the valid ones in ONE transaction:
	// a bolt transaction is an fsync, so committing a 20-partition poll one
	// partition at a time cost 20 of them (about 140 ms); this costs one.
	type slot struct{ topic, part int }
	var (
		commits []meta.OffsetCommit
		slots   []slot
	)
	for _, t := range oc.Topics {
		rt := kmsg.NewOffsetCommitResponseTopic()
		rt.Topic = t.Topic
		rt.TopicID = t.TopicID
		name, topic, ok := b.resolveTopic(t.Topic, t.TopicID)
		topicRead := b.canTopic(req.Conn, cmp.Or(name, t.Topic), opRead)

		for _, p := range t.Partitions {
			rp := kmsg.NewOffsetCommitResponseTopicPartition()
			rp.Partition = p.Partition
			switch {
			case fence != errNone:
				rp.ErrorCode = fence
			case !topicRead:
				rp.ErrorCode = kerr.TopicAuthorizationFailed.Code
			case !ok || p.Partition < 0 || p.Partition >= topic.Partitions:
				rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
			case p.Metadata != nil && len(*p.Metadata) > maxOffsetMetadataBytes:
				rp.ErrorCode = kerr.OffsetMetadataTooLarge.Code
			default:
				md := ""
				if p.Metadata != nil {
					md = *p.Metadata
				}
				commits = append(commits, meta.OffsetCommit{Topic: name, Partition: p.Partition, Offset: p.Offset, Metadata: md})
				slots = append(slots, slot{len(resp.Topics), len(rt.Partitions)})
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	if err := b.meta.CommitOffsets(oc.Group, commits); err != nil {
		code := b.coordinatorFault("offset commit", err, "group", oc.Group, "partitions", len(commits))
		for _, s := range slots {
			resp.Topics[s.topic].Partitions[s.part].ErrorCode = code
		}
	}
	return nil
}

// handleOffsetFetch returns a group's committed offsets, or -1 for any partition
// with no commit so the client applies its auto.offset.reset. It answers both the
// v0-v7 single-group form (top-level Group + Topics) and the v8+ batched form
// (Groups). A nil topic list means "all committed offsets for the group".
func (b *Broker) handleOffsetFetch(ctx context.Context, req *protocol.Request, of *kmsg.OffsetFetchRequest, resp *kmsg.OffsetFetchResponse) error {
	if len(of.Groups) > 0 { // v8+ batched form.
		for _, g := range of.Groups {
			rg := kmsg.NewOffsetFetchResponseGroup()
			rg.Group = g.Group
			if !b.canGroup(req.Conn, g.Group, opDescribe) {
				rg.ErrorCode = kerr.GroupAuthorizationFailed.Code
				resp.Groups = append(resp.Groups, rg)
				continue
			}
			// A KIP-848 member names itself and its epoch: one the coordinator no
			// longer knows, or knows at another epoch, must not be handed offsets
			// to resume from as if it still held the partitions.
			if g.MemberID != nil && *g.MemberID != "" && b.claims.ownerOf(g.Group) == protoConsumer {
				if code := b.cg848.validateCommit(g.Group, *g.MemberID, g.MemberEpoch); code != 0 {
					rg.ErrorCode = code
					resp.Groups = append(resp.Groups, rg)
					continue
				}
			}
			partitions, err := b.groupOffsets(g.Group, groupTopicNames(g.Topics), g.Topics == nil)
			if err != nil {
				return err
			}
			for _, po := range partitions {
				// A topic the principal may not describe is left out of an "all
				// offsets" answer and refused by name in an explicit one.
				denied := !b.canTopic(req.Conn, po.topic, opDescribe)
				if denied && g.Topics == nil {
					continue
				}
				rt := findOrAppendGroupTopic(&rg, po.topic)
				gp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
				gp.Partition = po.partition
				gp.Offset = po.offset
				gp.Metadata = metadataOrEmpty(po.metadata)
				if denied {
					gp.Offset, gp.Metadata = -1, metadataOrEmpty(nil)
					gp.ErrorCode = kerr.TopicAuthorizationFailed.Code
				}
				rt.Partitions = append(rt.Partitions, gp) // rt points into rg.Topics; mutation sticks.
			}
			resp.Groups = append(resp.Groups, rg)
		}
		return nil
	}

	// v0-v7 single-group form.
	if !b.canGroup(req.Conn, of.Group, opDescribe) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return nil
	}
	partitions, err := b.groupOffsets(of.Group, requestTopicNames(of.Topics), of.Topics == nil)
	if err != nil {
		return err
	}
	for _, po := range partitions {
		denied := !b.canTopic(req.Conn, po.topic, opDescribe)
		if denied && of.Topics == nil {
			continue
		}
		rt := findOrAppendTopic(resp, po.topic)
		tp := kmsg.NewOffsetFetchResponseTopicPartition()
		tp.Partition = po.partition
		tp.Offset = po.offset
		tp.Metadata = metadataOrEmpty(po.metadata)
		if denied {
			tp.Offset, tp.Metadata = -1, metadataOrEmpty(nil)
			tp.ErrorCode = kerr.TopicAuthorizationFailed.Code
		}
		rt.Partitions = append(rt.Partitions, tp) // rt points into resp.Topics; mutation sticks.
	}
	return nil
}

// maxOffsetMetadataBytes is Kafka's offset.metadata.max.bytes default: the longest
// metadata string a commit may carry before OFFSET_METADATA_TOO_LARGE.
const maxOffsetMetadataBytes = 4096

// metadataOrEmpty is what OffsetFetch sends for a partition's metadata: Kafka
// always sends a string, empty when none was committed, never null.
func metadataOrEmpty(m *string) *string {
	if m != nil {
		return m
	}
	empty := ""
	return &empty
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

	// One read of the group's offsets answers every requested partition, with its
	// metadata. Looking each one up separately returned only the number.
	entries, err := b.meta.ListOffsets(group)
	if err != nil {
		return nil, err
	}
	committed := make(map[topicPartition]meta.OffsetEntry, len(entries))
	for _, e := range entries {
		committed[topicPartition{e.Topic, e.Partition}] = e
	}
	var out []partitionOffset
	for topic, parts := range topics {
		for _, part := range parts {
			po := partitionOffset{topic: topic, partition: part, offset: -1}
			if e, ok := committed[topicPartition{topic, part}]; ok {
				po.offset = e.Offset
				if e.Metadata != "" { // the metadata committed with the offset comes back with it.
					md := e.Metadata
					po.metadata = &md
				}
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

// commitFence decides whether a group's OffsetCommit may be stored at all. The
// group id must be storable, and the committer must be entitled to commit for the
// group as it stands now, under whichever protocol the group runs.
func (b *Broker) commitFence(oc *kmsg.OffsetCommitRequest) int16 {
	if oc.Group == "" || meta.ValidateGroupID(oc.Group) != nil {
		return kerr.InvalidGroupID.Code
	}
	if b.claims.ownerOf(oc.Group) == protoConsumer {
		return b.cg848.validateCommit(oc.Group, oc.MemberID, oc.Generation)
	}
	return b.groups.validateCommit(oc.Group, oc.MemberID, oc.InstanceID, oc.Generation)
}
