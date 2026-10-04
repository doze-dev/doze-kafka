package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
)

// DeleteGroups and OffsetDelete are what teardown scripts and admin tools use to
// remove a consumer group or reset its position. A group's committed offsets are
// the only durable consumer state here, so deleting a group means deleting them.

// groupIsActive reports whether any live member belongs to the group, under
// either coordinator.
func (b *Broker) groupIsActive(id string) bool {
	return b.groups.hasMembers(id) || b.cg848.hasMembers(id)
}

// handleDeleteGroups removes empty groups. A group with live members is refused
// (NON_EMPTY_GROUP), an unknown one is GROUP_ID_NOT_FOUND, as on Kafka.
func (b *Broker) handleDeleteGroups(ctx context.Context, req *protocol.Request, dr *kmsg.DeleteGroupsRequest, resp *kmsg.DeleteGroupsResponse) error {
	for _, id := range dr.Groups {
		rg := kmsg.NewDeleteGroupsResponseGroup()
		rg.Group = id
		rg.ErrorCode = b.deleteGroup(id)
		resp.Groups = append(resp.Groups, rg)
	}
	return nil
}

func (b *Broker) deleteGroup(id string) int16 {
	if b.groupIsActive(id) {
		return kerr.NonEmptyGroup.Code
	}
	// Hold the id for the length of the deletion. Checking for members and then
	// deleting left a gap in which a consumer could join and have the offsets it
	// was about to read removed underneath it.
	const deleting = "deleting"
	if !b.claims.claim(id, deleting) {
		return kerr.NonEmptyGroup.Code
	}
	defer b.claims.release(id, deleting)
	existed := b.groups.forget(id)
	if b.cg848.forget(id) {
		existed = true
	}
	n, err := b.meta.DeleteGroupOffsets(id)
	if err != nil {
		b.log.Warn("delete group: could not remove committed offsets", "group", id, "err", err)
		return kerr.UnknownServerError.Code
	}
	if !existed && n == 0 {
		return kerr.GroupIDNotFound.Code
	}
	return errNone
}

// handleOffsetDelete removes a group's committed offsets for named partitions —
// how a tool "resets" a consumer to the auto.offset.reset position. The classic
// protocol's subscriptions are opaque bytes to a relay, so whether a live member
// reads a topic cannot be told; any group with live members is treated as
// subscribed and refused (GROUP_SUBSCRIBED_TO_TOPIC), which is the safe answer.
func (b *Broker) handleOffsetDelete(ctx context.Context, req *protocol.Request, odr *kmsg.OffsetDeleteRequest, resp *kmsg.OffsetDeleteResponse) error {
	known := b.groupIsActive(odr.Group) || b.groupHasOffsets(odr.Group)
	if !known {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return nil
	}
	active := b.groupIsActive(odr.Group)

	for _, t := range odr.Topics {
		rt := kmsg.NewOffsetDeleteResponseTopic()
		rt.Topic = t.Topic
		_, topic, topicOK := b.resolveTopic(t.Topic, [16]byte{})
		for _, p := range t.Partitions {
			rp := kmsg.NewOffsetDeleteResponseTopicPartition()
			rp.Partition = p.Partition
			switch {
			case active:
				rp.ErrorCode = kerr.GroupSubscribedToTopic.Code
			case (!topicOK || p.Partition < 0 || p.Partition >= topic.Partitions) && !b.hasOffset(odr.Group, t.Topic, p.Partition):
				rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
			default:
				// Including an offset whose topic is gone. Kafka answers
				// UNKNOWN_TOPIC_OR_PARTITION there, but it also never keeps such an
				// offset; a data directory from an earlier version of this broker
				// can hold one, and refusing left no way to remove it short of
				// deleting the whole group.
				if err := b.meta.DeleteOffset(odr.Group, t.Topic, p.Partition); err != nil {
					rp.ErrorCode = kerr.UnknownServerError.Code
				}
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return nil
}

// hasOffset reports whether the group has a committed offset stored for the partition.
func (b *Broker) hasOffset(group, topic string, partition int32) bool {
	_, ok, err := b.meta.FetchOffset(group, topic, partition)
	return err == nil && ok
}
