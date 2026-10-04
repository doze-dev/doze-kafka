package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The KIP-848 request handlers: ConsumerGroupHeartbeat and ConsumerGroupDescribe.
// The coordinator they call is in group848.go, as groups.go is to group.go for the
// classic protocol.

// handleConsumerGroupHeartbeat is the single KIP-848 API that carries join, poll,
// and leave. The broker computes and drives the assignment (see cg848Manager).
func (b *Broker) handleConsumerGroupHeartbeat(ctx context.Context, req *protocol.Request, hr *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse) error {
	res := b.cg848.heartbeat(hr, clientRef{req.Header.ClientID, req.Conn.RemoteHost()})

	resp.ErrorCode = res.errorCode
	if res.errorMsg != "" {
		resp.ErrorMessage = &res.errorMsg
	}
	if res.errorCode == errNone {
		mid := res.memberID
		resp.MemberID = &mid
		resp.MemberEpoch = res.memberEpoch
		resp.HeartbeatIntervalMillis = cg848HeartbeatMs
		resp.Assignment = res.assignment
	}
	return nil
}

// handleConsumerGroupDescribe reports the broker-driven group's state, epoch, and
// members with their current assignments — the data `kafka-consumer-groups
// --describe` shows. Unknown groups get GROUP_ID_NOT_FOUND.
func (b *Broker) handleConsumerGroupDescribe(ctx context.Context, req *protocol.Request, dr *kmsg.ConsumerGroupDescribeRequest, resp *kmsg.ConsumerGroupDescribeResponse) error {
	for _, group := range dr.Groups {
		rg := kmsg.NewConsumerGroupDescribeResponseGroup()
		rg.Group = group
		state, epoch, assignor, members, ok := b.cg848.describe(group)
		if !ok || (len(members) == 0 && b.claims.ownerOf(group) == protoClassic) {
			rg.ErrorCode = kerr.GroupIDNotFound.Code
			resp.Groups = append(resp.Groups, rg)
			continue
		}
		rg.State = state
		rg.Epoch = epoch
		rg.AssignmentEpoch = epoch
		rg.AssignorName = assignor
		for _, mem := range members {
			rm := kmsg.NewConsumerGroupDescribeResponseGroupMember()
			rm.MemberID = mem.id
			rm.ClientID = mem.client.clientID
			rm.ClientHost = mem.client.host
			rm.InstanceID = mem.instanceID
			rm.RackID = mem.rack
			rm.MemberEpoch = mem.epoch
			rm.SubscribedTopics = mem.subscribed
			if mem.regex != "" {
				rm.SubscribedTopicRegex = &mem.regex
			}
			rm.Assignment = b.describeAssignment(mem.assigned)
			rm.TargetAssignment = b.describeAssignment(mem.target)
			rg.Members = append(rg.Members, rm)
		}
		resp.Groups = append(resp.Groups, rg)
	}
	return nil
}

// describeAssignment renders a set of partitions as a ConsumerGroupDescribe
// assignment (topic name, id and partitions). A topic that no longer exists is
// shown by id alone.
func (b *Broker) describeAssignment(a tpSet) kmsg.Assignment {
	out := kmsg.NewAssignment()
	for _, t := range a.byTopic() {
		at := kmsg.NewAssignmentTopicPartition()
		at.TopicID = t.topic
		if topic, ok, err := b.meta.TopicByID(t.topic); err == nil && ok {
			at.Topic = topic.Name
		}
		at.Partitions = t.parts
		out.TopicPartitions = append(out.TopicPartitions, at)
	}
	return out
}

func encodeAssignment(a tpSet) *kmsg.ConsumerGroupHeartbeatResponseAssignment {
	out := kmsg.NewConsumerGroupHeartbeatResponseAssignment()
	for _, t := range a.byTopic() {
		at := kmsg.NewConsumerGroupHeartbeatResponseAssignmentTopic()
		at.TopicID = t.topic
		at.Partitions = t.parts
		out.Topics = append(out.Topics, at)
	}
	return &out
}
