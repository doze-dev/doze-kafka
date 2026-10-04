package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"

	"github.com/twmb/franz-go/pkg/kerr"
)

// handleMetadata describes the single-node cluster: brokers=[self],
// controller=self, cluster id, and topics. A request with no named topics returns
// all registered topics; named topics are resolved individually, with unknown
// ones reported as UNKNOWN_TOPIC_OR_PARTITION.
func (b *Broker) handleMetadata(ctx context.Context, req *protocol.Request, mr *kmsg.MetadataRequest, resp *kmsg.MetadataResponse) error {
	self := kmsg.NewMetadataResponseBroker()
	self.NodeID = b.nodeID()
	self.Host = b.cfg.AdvertisedHost
	self.Port = b.advertisedPort()
	resp.Brokers = append(resp.Brokers, self)

	clusterID := b.clusterID()
	resp.ClusterID = &clusterID
	resp.ControllerID = b.nodeID()

	// A null topic list means "all topics" and an empty one means "none" — from v1.
	// v0 cannot express null, so there an empty list is how a client asks for
	// everything. Treating v1+ empty as "all" made a client that asked for nothing
	// (to learn the brokers, say) pay for and parse the whole topic registry.
	if mr.Topics == nil || (len(mr.Topics) == 0 && req.Header.APIVersion == 0) {
		// All topics.
		topics, err := b.meta.ListTopics()
		if err != nil {
			return err
		}
		for _, t := range topics {
			resp.Topics = append(resp.Topics, b.topicMetadata(t.Name, t.ID, t.Partitions, errNone))
		}
		return nil
	}

	for _, reqTopic := range mr.Topics {
		if reqTopic.Topic == nil {
			// Addressed by id (Metadata v10+).
			if reqTopic.TopicID == ([16]byte{}) {
				continue
			}
			if t, ok, err := b.meta.TopicByID(meta.TopicID(reqTopic.TopicID)); err == nil && ok {
				resp.Topics = append(resp.Topics, b.topicMetadata(t.Name, t.ID, t.Partitions, errNone))
			} else {
				rt := b.topicMetadata("", meta.TopicID(reqTopic.TopicID), 0, kerr.UnknownTopicID.Code)
				rt.Topic = nil
				resp.Topics = append(resp.Topics, rt)
			}
			continue
		}
		name := *reqTopic.Topic
		if err := meta.ValidateTopicName(name); err != nil && !b.topicRegistered(name) {
			// A name Kafka would refuse is INVALID_TOPIC_EXCEPTION, not "unknown":
			// the difference tells a client its request can never succeed. Topics
			// already in the registry are exempt so ones created before names were
			// validated stay reachable.
			resp.Topics = append(resp.Topics, b.topicMetadata(name, meta.TopicID{}, 0, kerr.InvalidTopicException.Code))
			continue
		}
		t, found, err := b.meta.Topic(name)
		if err != nil {
			return err
		}
		if !found {
			// AllowAutoTopicCreation arrived in v4; before that a metadata request
			// for an unknown topic always meant "create it if the broker allows".
			allow := mr.AllowAutoTopicCreation || req.Header.APIVersion < 4
			if created, ok := b.maybeAutoCreate(name, allow); ok {
				resp.Topics = append(resp.Topics, b.topicMetadata(created.Name, created.ID, created.Partitions, errNone))
				continue
			}
			resp.Topics = append(resp.Topics, b.topicMetadata(name, meta.TopicID{}, 0, kerr.UnknownTopicOrPartition.Code))
			continue
		}
		resp.Topics = append(resp.Topics, b.topicMetadata(t.Name, t.ID, t.Partitions, errNone))
	}

	return nil
}

// topicMetadata builds a Metadata topic entry. Every partition is led by us, with
// replicas=isr=[self] and leaderEpoch 0 (single node).
func (b *Broker) topicMetadata(name string, id meta.TopicID, partitions int32, errCode int16) kmsg.MetadataResponseTopic {
	rt := kmsg.NewMetadataResponseTopic()
	topicName := name
	rt.Topic = &topicName
	rt.TopicID = id
	rt.ErrorCode = int16(errCode)
	for p := range partitions {
		pt := kmsg.NewMetadataResponseTopicPartition()
		pt.Partition = p
		pt.Leader = b.nodeID()
		pt.Replicas = []int32{b.nodeID()}
		pt.ISR = []int32{b.nodeID()}
		rt.Partitions = append(rt.Partitions, pt)
	}
	return rt
}

// topicRegistered reports whether the registry already holds a topic by this name.
func (b *Broker) topicRegistered(name string) bool {
	_, ok, err := b.meta.Topic(name)
	return err == nil && ok
}
