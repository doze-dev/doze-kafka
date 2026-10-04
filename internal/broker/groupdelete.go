package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
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
func (b *Broker) handleDeleteGroups(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	dr := kreq.(*kmsg.DeleteGroupsRequest)

	resp := kmsg.NewPtrDeleteGroupsResponse()
	resp.SetVersion(req.Header.APIVersion)

	for _, id := range dr.Groups {
		rg := kmsg.NewDeleteGroupsResponseGroup()
		rg.Group = id
		rg.ErrorCode = b.deleteGroup(id)
		resp.Groups = append(resp.Groups, rg)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

func (b *Broker) deleteGroup(id string) int16 {
	if b.groupIsActive(id) {
		return int16(protocol.ErrNonEmptyGroup)
	}
	// Hold the id for the length of the deletion. Checking for members and then
	// deleting left a gap in which a consumer could join and have the offsets it
	// was about to read removed underneath it.
	const deleting = "deleting"
	if !b.claims.claim(id, deleting) {
		return int16(protocol.ErrNonEmptyGroup)
	}
	defer b.claims.release(id, deleting)
	existed := b.groups.forget(id)
	n, err := b.meta.DeleteGroupOffsets(id)
	if err != nil {
		b.log.Warn("delete group: could not remove committed offsets", "group", id, "err", err)
		return int16(protocol.ErrUnknownServer)
	}
	if !existed && n == 0 {
		return int16(protocol.ErrGroupIDNotFound)
	}
	return int16(protocol.ErrNone)
}

// handleOffsetDelete removes a group's committed offsets for named partitions —
// how a tool "resets" a consumer to the auto.offset.reset position. The classic
// protocol's subscriptions are opaque bytes to a relay, so whether a live member
// reads a topic cannot be told; any group with live members is treated as
// subscribed and refused (GROUP_SUBSCRIBED_TO_TOPIC), which is the safe answer.
func (b *Broker) handleOffsetDelete(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	odr := kreq.(*kmsg.OffsetDeleteRequest)

	resp := kmsg.NewPtrOffsetDeleteResponse()
	resp.SetVersion(req.Header.APIVersion)

	known := b.groupIsActive(odr.Group) || b.groupHasOffsets(odr.Group)
	if !known {
		resp.ErrorCode = int16(protocol.ErrGroupIDNotFound)
		return protocol.EncodeResponse(req.Header, resp), nil
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
				rp.ErrorCode = int16(protocol.ErrGroupSubscribedToTopic)
			case !topicOK || p.Partition < 0 || p.Partition >= topic.Partitions:
				rp.ErrorCode = int16(protocol.ErrUnknownTopicOrPart)
			default:
				if err := b.meta.DeleteOffset(odr.Group, t.Topic, p.Partition); err != nil {
					rp.ErrorCode = int16(protocol.ErrUnknownServer)
				}
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}
