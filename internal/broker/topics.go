package broker

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// handleCreateTopics registers each topic (assigning a UUID) and materializes its
// partition directories with LEO=0. Single-node: replication factor is always 1.
func (b *Broker) handleCreateTopics(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	cr := kreq.(*kmsg.CreateTopicsRequest)

	resp := kmsg.NewPtrCreateTopicsResponse()
	resp.SetVersion(req.Header.APIVersion)

	for _, t := range cr.Topics {
		resp.Topics = append(resp.Topics, b.createTopic(t, cr.ValidateOnly))
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// createTopic handles a single topic and returns its response entry.
func (b *Broker) createTopic(t kmsg.CreateTopicsRequestTopic, validateOnly bool) kmsg.CreateTopicsResponseTopic {
	rt := kmsg.NewCreateTopicsResponseTopic()
	rt.Topic = t.Topic

	partitions := t.NumPartitions
	if partitions <= 0 {
		partitions = int32(b.cfg.DefaultPartitions)
	}
	if validateOnly {
		rt.NumPartitions = partitions
		rt.ReplicationFactor = 1
		return rt
	}

	created, err := b.meta.CreateTopic(t.Topic, partitions, configMap(t.Configs))
	if err != nil {
		if errors.Is(err, meta.ErrTopicExists) {
			rt.ErrorCode = int16(protocol.ErrTopicAlreadyExists)
		} else {
			rt.ErrorCode = int16(protocol.ErrUnknownServer)
			msg := err.Error()
			rt.ErrorMessage = &msg
		}
		return rt
	}

	// Materialize the partition directories so they recover after a restart even
	// if nothing is ever produced to them.
	for p := range created.Partitions {
		if _, err := b.logs.Partition(created.Name, p); err != nil {
			rt.ErrorCode = int16(protocol.ErrUnknownServer)
			msg := err.Error()
			rt.ErrorMessage = &msg
			return rt
		}
	}

	rt.TopicID = created.ID
	rt.NumPartitions = created.Partitions
	rt.ReplicationFactor = 1
	return rt
}

// configMap flattens request topic configs into a plain map (nil values dropped).
func configMap(configs []kmsg.CreateTopicsRequestTopicConfig) map[string]string {
	if len(configs) == 0 {
		return nil
	}
	out := make(map[string]string, len(configs))
	for _, c := range configs {
		if c.Value != nil {
			out[c.Name] = *c.Value
		}
	}
	return out
}

// maybeAutoCreate creates a topic on demand when the metadata request permits it
// and config enables it, materializing its partition directories. It returns the
// created topic and whether creation happened (a lost race falls back to a lookup).
func (b *Broker) maybeAutoCreate(name string, requested bool) (meta.Topic, bool) {
	if !requested || !b.cfg.AutoCreateTopics {
		return meta.Topic{}, false
	}
	created, err := b.meta.CreateTopic(name, int32(b.cfg.DefaultPartitions), nil)
	if err != nil {
		if t, ok, lerr := b.meta.Topic(name); lerr == nil && ok {
			return t, true
		}
		return meta.Topic{}, false
	}
	for p := range created.Partitions {
		if _, err := b.logs.Partition(created.Name, p); err != nil {
			return meta.Topic{}, false
		}
	}
	return created, true
}

// handleDeleteTopics removes each topic from the registry and deletes its partition
// data from disk. It accepts the v0-v5 name list and the v6+ (name-or-id) form.
func (b *Broker) handleDeleteTopics(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	dr := kreq.(*kmsg.DeleteTopicsRequest)

	resp := kmsg.NewPtrDeleteTopicsResponse()
	resp.SetVersion(req.Header.APIVersion)

	// Normalize both request forms into (name, id) targets.
	type target struct {
		name string
		id   [16]byte
	}
	var targets []target
	for _, n := range dr.TopicNames {
		targets = append(targets, target{name: n})
	}
	for _, t := range dr.Topics {
		tg := target{id: t.TopicID}
		if t.Topic != nil {
			tg.name = *t.Topic
		}
		targets = append(targets, tg)
	}

	for _, tg := range targets {
		rt := kmsg.NewDeleteTopicsResponseTopic()
		rt.TopicID = tg.id

		name, topic, ok := b.resolveTopic(tg.name, tg.id)
		if name != "" {
			n := name
			rt.Topic = &n
		}
		switch {
		case !ok:
			rt.ErrorCode = int16(protocol.ErrUnknownTopicOrPart)
		default:
			rt.TopicID = topic.ID
			if err := b.meta.DeleteTopic(name); err != nil {
				rt.ErrorCode = int16(protocol.ErrUnknownServer)
				break
			}
			// Registry gone; best-effort remove the on-disk partition data and forget
			// any idempotent-producer sequence state for the topic.
			if err := b.logs.RemoveTopic(name, topic.Partitions); err != nil {
				b.log.Warn("delete topic: partition cleanup failed", "topic", name, "err", err)
			}
			b.idem.dropPartitions(name, topic.Partitions)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}
