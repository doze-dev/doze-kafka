package broker

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// handleMetadata describes the single-node cluster: brokers=[self],
// controller=self, cluster id, and topics. A request with no named topics returns
// all registered topics; named topics are resolved individually, with unknown
// ones reported as UNKNOWN_TOPIC_OR_PARTITION.
func (b *Broker) handleMetadata(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	mr := kreq.(*kmsg.MetadataRequest)

	resp := kmsg.NewPtrMetadataResponse()
	resp.SetVersion(req.Header.APIVersion)

	self := kmsg.NewMetadataResponseBroker()
	self.NodeID = b.nodeID()
	self.Host = b.cfg.AdvertisedHost
	self.Port = b.advertisedPort()
	resp.Brokers = append(resp.Brokers, self)

	clusterID := b.clusterID()
	resp.ClusterID = &clusterID
	resp.ControllerID = b.nodeID()

	if len(mr.Topics) == 0 {
		// All topics.
		topics, err := b.meta.ListTopics()
		if err != nil {
			return nil, err
		}
		for _, t := range topics {
			resp.Topics = append(resp.Topics, b.topicMetadata(t.Name, t.ID, t.Partitions, protocol.ErrNone))
		}
		return protocol.EncodeResponse(req.Header, resp), nil
	}

	for _, reqTopic := range mr.Topics {
		if reqTopic.Topic == nil {
			continue
		}
		name := *reqTopic.Topic
		if err := meta.ValidateTopicName(name); err != nil && !b.topicRegistered(name) {
			// A name Kafka would refuse is INVALID_TOPIC_EXCEPTION, not "unknown":
			// the difference tells a client its request can never succeed. Topics
			// already in the registry are exempt so ones created before names were
			// validated stay reachable.
			resp.Topics = append(resp.Topics, b.topicMetadata(name, meta.TopicID{}, 0, protocol.ErrInvalidTopic))
			continue
		}
		t, found, err := b.meta.Topic(name)
		if err != nil {
			return nil, err
		}
		if !found {
			if created, ok := b.maybeAutoCreate(name, mr.AllowAutoTopicCreation); ok {
				resp.Topics = append(resp.Topics, b.topicMetadata(created.Name, created.ID, created.Partitions, protocol.ErrNone))
				continue
			}
			resp.Topics = append(resp.Topics, b.topicMetadata(name, meta.TopicID{}, 0, protocol.ErrUnknownTopicOrPart))
			continue
		}
		resp.Topics = append(resp.Topics, b.topicMetadata(t.Name, t.ID, t.Partitions, protocol.ErrNone))
	}

	return protocol.EncodeResponse(req.Header, resp), nil
}

// topicMetadata builds a Metadata topic entry. Every partition is led by us, with
// replicas=isr=[self] and leaderEpoch 0 (single node).
func (b *Broker) topicMetadata(name string, id meta.TopicID, partitions int32, errCode protocol.ErrorCode) kmsg.MetadataResponseTopic {
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
