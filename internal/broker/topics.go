package broker

import (
	"context"
	"errors"
	"fmt"

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
	if err := meta.ValidateTopicName(t.Topic); err != nil {
		rt.ErrorCode = int16(protocol.ErrInvalidTopic)
		msg := err.Error()
		rt.ErrorMessage = &msg
		return rt
	}
	if int(partitions) > b.cfg.MaxPartitions {
		rt.ErrorCode = int16(protocol.ErrInvalidPartitions)
		msg := fmt.Sprintf("%d partitions is above this broker's limit of %d (--max-partitions)", partitions, b.cfg.MaxPartitions)
		rt.ErrorMessage = &msg
		return rt
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
	// if nothing is ever produced to them. Registering the topic and creating its
	// directories are two steps with no shared transaction, so a failure in the
	// second must undo the first: otherwise the name stays registered, every later
	// produce fails, and CreateTopics answers TOPIC_ALREADY_EXISTS.
	if err := b.materialize(created); err != nil {
		rt.ErrorCode = int16(protocol.ErrUnknownServer)
		msg := err.Error()
		rt.ErrorMessage = &msg
		return rt
	}

	b.cg848.topicChanged(created.Name)

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
	if err := b.materialize(created); err != nil {
		b.log.Warn("auto-create topic failed", "topic", name, "err", err)
		return meta.Topic{}, false
	}
	b.cg848.topicChanged(created.Name)
	return created, true
}

// materialize opens (creating) every partition of a freshly registered topic.
// On failure it rolls the registration back and removes whatever was created, so
// a topic is either fully there or not there at all.
func (b *Broker) materialize(t meta.Topic) error {
	for p := range t.Partitions {
		if _, err := b.logs.Partition(t.Name, p); err != nil {
			if derr := b.meta.DeleteTopic(t.Name); derr != nil {
				b.log.Warn("create topic: rollback of registration failed", "topic", t.Name, "err", derr)
			}
			if rerr := b.logs.RemoveTopic(t.Name, t.Partitions); rerr != nil {
				b.log.Warn("create topic: rollback of partition data failed", "topic", t.Name, "err", rerr)
			}
			return err
		}
	}
	return nil
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
			b.cg848.topicChanged(name)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}
