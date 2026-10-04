package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"

	"github.com/twmb/franz-go/pkg/kerr"
)

// handleCreateTopics registers each topic (assigning a UUID) and materializes its
// partition directories with LEO=0. Single-node: replication factor is always 1.
func (b *Broker) handleCreateTopics(ctx context.Context, req *protocol.Request, cr *kmsg.CreateTopicsRequest, resp *kmsg.CreateTopicsResponse) error {
	// A name listed twice is a malformed request: neither entry is acted on.
	count := map[string]int{}
	for _, t := range cr.Topics {
		count[t.Topic]++
	}
	for _, t := range cr.Topics {
		if count[t.Topic] > 1 {
			rt := kmsg.NewCreateTopicsResponseTopic()
			rt.Topic = t.Topic
			rt.ErrorCode = kerr.InvalidRequest.Code
			msg := "the topic is named more than once in this request"
			rt.ErrorMessage = &msg
			resp.Topics = append(resp.Topics, rt)
			continue
		}
		resp.Topics = append(resp.Topics, b.createTopic(t, cr.ValidateOnly))
	}
	return nil
}

// createTopic handles a single topic and returns its response entry.
func (b *Broker) createTopic(t kmsg.CreateTopicsRequestTopic, validateOnly bool) kmsg.CreateTopicsResponseTopic {
	rt := kmsg.NewCreateTopicsResponseTopic()
	rt.Topic = t.Topic

	// -1 asks for the broker's default. Zero or any other negative count is not a
	// request for the default; it used to be quietly turned into one.
	partitions := t.NumPartitions
	if partitions == -1 {
		partitions = int32(b.cfg.DefaultPartitions)
	}
	if err := meta.ValidateTopicName(t.Topic); err != nil {
		rt.ErrorCode = kerr.InvalidTopicException.Code
		msg := strings.TrimPrefix(err.Error(), "meta: ") // a client is shown this; the package is not its business.
		rt.ErrorMessage = &msg
		return rt
	}
	if partitions <= 0 {
		rt.ErrorCode = kerr.InvalidPartitions.Code
		msg := fmt.Sprintf("%d is not a partition count (use -1 for the broker default)", t.NumPartitions)
		rt.ErrorMessage = &msg
		return rt
	}
	if err := validateTopicConfig(configMap(t.Configs)); err != nil {
		rt.ErrorCode = kerr.InvalidConfig.Code
		msg := err.Error()
		rt.ErrorMessage = &msg
		return rt
	}
	if int(partitions) > b.cfg.MaxPartitions {
		rt.ErrorCode = kerr.InvalidPartitions.Code
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
			rt.ErrorCode = kerr.TopicAlreadyExists.Code
		} else if errors.Is(err, meta.ErrTopicCollides) {
			rt.ErrorCode = kerr.InvalidTopicException.Code
			msg := strings.TrimPrefix(err.Error(), "meta: ")
			rt.ErrorMessage = &msg
		} else {
			rt.ErrorCode = kerr.UnknownServerError.Code
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
		rt.ErrorCode = kerr.UnknownServerError.Code
		msg := err.Error()
		rt.ErrorMessage = &msg
		return rt
	}

	b.applyTopicLimits(created.Name, created.Config)
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
	// Create the directories only; partitions open on first use. Opening them all
	// here held two descriptors per partition for as long as the broker ran.
	// The registry has just said this name was free, so any partition directory
	// already on disk for it belongs to no topic (a lost or relocated metadata
	// database leaves them). Adopting them would hand the new topic old records.
	if n, err := b.logs.RemoveStrayTopicDirs(t.Name); err != nil {
		b.log.Warn("create topic: could not clear leftover partition directories", "topic", t.Name, "err", err)
	} else if n > 0 {
		b.log.Warn("create topic: removed leftover partition directories that belonged to no topic", "topic", t.Name, "directories", n)
	}
	if err := b.logs.EnsurePartitionDirs(t.Name, t.Partitions); err != nil {
		if derr := b.meta.DeleteTopic(t.Name); derr != nil {
			b.log.Warn("create topic: rollback of registration failed", "topic", t.Name, "err", derr)
		}
		if rerr := b.logs.RemoveTopic(t.Name, t.Partitions); rerr != nil {
			b.log.Warn("create topic: rollback of partition data failed", "topic", t.Name, "err", rerr)
		}
		return err
	}
	return nil
}

// handleDeleteTopics removes each topic from the registry and deletes its partition
// data from disk. It accepts the v0-v5 name list and the v6+ (name-or-id) form.
func (b *Broker) handleDeleteTopics(ctx context.Context, req *protocol.Request, dr *kmsg.DeleteTopicsRequest, resp *kmsg.DeleteTopicsResponse) error {
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
			rt.ErrorCode = kerr.UnknownTopicOrPartition.Code
		default:
			rt.TopicID = topic.ID
			if err := b.meta.DeleteTopic(name); err != nil {
				rt.ErrorCode = b.storageFault("delete topic", err, "topic", name)
				break
			}
			// Registry gone; best-effort remove the on-disk partition data and forget
			// any idempotent-producer sequence state for the topic.
			if err := b.logs.RemoveTopic(name, topic.Partitions); err != nil {
				b.log.Warn("delete topic: partition cleanup failed", "topic", name, "err", err)
			}
			b.idem.dropPartitions(name, topic.Partitions)
			b.cg848.topicChanged(name)
			b.produce.signal(name) // wake fetches parked on it: they have an answer now (the topic is gone).
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return nil
}
