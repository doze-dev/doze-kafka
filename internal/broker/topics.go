package broker

import (
	"cmp"
	"context"
	"crypto/rand"
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
	// A name listed twice is a malformed request: neither entry is acted on, and
	// Kafka answers once per name ("Duplicate topic name."; claim: api V10).
	count := map[string]int{}
	for _, t := range cr.Topics {
		count[t.Topic]++
	}
	answered := map[string]bool{}
	defer func() {
		// Kafka answers an error with an empty list of configs, never null (claim: api V10).
		for i := range resp.Topics {
			if resp.Topics[i].Configs == nil {
				resp.Topics[i].Configs = []kmsg.CreateTopicsResponseTopicConfig{}
			}
		}
	}()
	for _, t := range cr.Topics {
		if count[t.Topic] > 1 {
			if answered[t.Topic] {
				continue
			}
			answered[t.Topic] = true
			rt := kmsg.NewCreateTopicsResponseTopic()
			rt.Topic = t.Topic
			rt.ErrorCode = kerr.InvalidRequest.Code
			msg := "Duplicate topic name."
			rt.ErrorMessage = &msg
			resp.Topics = append(resp.Topics, rt)
			continue
		}
		if !b.canCreateTopic(req.Conn, t.Topic) {
			rt := kmsg.NewCreateTopicsResponseTopic()
			rt.Topic = t.Topic
			rt.ErrorCode = kerr.TopicAuthorizationFailed.Code
			msg := "not authorized to create topic " + t.Topic
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

	refuse := func(code int16, format string, args ...any) kmsg.CreateTopicsResponseTopic {
		rt.ErrorCode = code
		msg := fmt.Sprintf(format, args...)
		rt.ErrorMessage = &msg
		return rt
	}
	// (The wording of the name, partition-count and config messages is this
	// broker's own: the console shows it, and its tests read it. The codes are
	// Kafka's. conformance/scenarios.go lists the steps whose text differs.)
	if err := meta.ValidateTopicName(t.Topic); err != nil {
		return refuse(kerr.InvalidTopicException.Code, "%s", strings.TrimPrefix(err.Error(), "meta: "))
	}
	if _, ok, _ := b.meta.Topic(t.Topic); ok {
		return refuse(kerr.TopicAlreadyExists.Code, "Topic '%s' already exists.", t.Topic)
	}
	if all, err := b.meta.ListTopics(); err == nil {
		names := make([]string, len(all))
		for i, e := range all {
			names[i] = e.Name
		}
		if c := meta.FindCollision(t.Topic, names); c != nil {
			if c.CaseOnly {
				return refuse(kerr.InvalidTopicException.Code,
					"Topic '%s' collides with existing topic: %s (names that differ only in letter case would share a directory on a case-insensitive file system)", c.Name, c.Existing)
			}
			return refuse(kerr.InvalidTopicException.Code, "Topic '%s' collides with existing topic: %s", c.Name, c.Existing)
		}
	}

	// -1 asks for the broker's default. Zero or any other negative count is not a
	// request for the default; it used to be quietly turned into one.
	partitions := t.NumPartitions
	if len(t.ReplicaAssignment) > 0 {
		// A manual assignment says how many partitions there are; the request
		// must not say so as well, nor ask for a replication factor.
		if t.ReplicationFactor != -1 {
			return refuse(kerr.InvalidRequest.Code, "A manual partition assignment was specified, but replication factor was not set to -1.")
		}
		if t.NumPartitions != -1 {
			return refuse(kerr.InvalidRequest.Code, "A manual partition assignment was specified, but numPartitions was not set to -1.")
		}
		byPartition := map[int32][]int32{}
		for _, a := range t.ReplicaAssignment {
			if _, dup := byPartition[a.Partition]; dup {
				return refuse(kerr.InvalidReplicaAssignment.Code, "Partition %d is assigned more than once.", a.Partition)
			}
			byPartition[a.Partition] = a.Replicas
		}
		for p := int32(0); p < int32(len(t.ReplicaAssignment)); p++ {
			if _, ok := byPartition[p]; !ok {
				return refuse(kerr.InvalidReplicaAssignment.Code, "partitions should be a consecutive 0-based integer sequence")
			}
		}
		for p := int32(0); p < int32(len(t.ReplicaAssignment)); p++ {
			replicas := byPartition[p]
			if len(replicas) == 0 {
				return refuse(kerr.InvalidReplicaAssignment.Code, "The manual partition assignment includes an empty replica list for partition %d.", p)
			}
			seen := map[int32]bool{}
			for _, r := range replicas {
				if r != b.nodeID() {
					return refuse(kerr.InvalidReplicaAssignment.Code, "The manual partition assignment includes broker %d, but no such broker is registered.", r)
				}
				if seen[r] {
					return refuse(kerr.InvalidReplicaAssignment.Code, "The manual partition assignment includes the broker %d more than once.", r)
				}
				seen[r] = true
			}
		}
		partitions = int32(len(t.ReplicaAssignment))
	} else {
		if partitions == -1 {
			partitions = int32(b.cfg.DefaultPartitions)
		}
		if partitions <= 0 {
			return refuse(kerr.InvalidPartitions.Code, "%d is not a partition count (use -1 for the broker default)", t.NumPartitions)
		}
		if t.ReplicationFactor == 0 || t.ReplicationFactor < -1 {
			return refuse(kerr.InvalidReplicationFactor.Code, "Replication factor must be larger than 0, or -1 to use the default value.")
		}
	}
	if err := validateTopicConfig(configMap(t.Configs)); err != nil {
		return refuse(kerr.InvalidConfig.Code, "%s", err.Error())
	}
	if int(partitions) > b.cfg.MaxPartitions {
		return refuse(kerr.InvalidPartitions.Code, "%d partitions is above this broker's limit of %d (--max-partitions)", partitions, b.cfg.MaxPartitions)
	}
	if validateOnly {
		// Kafka answers a validation with an id it made up for the topic that
		// was not created (claim: api V10).
		var id meta.TopicID
		_, _ = rand.Read(id[:])
		rt.TopicID = id
		rt.NumPartitions = partitions
		rt.ReplicationFactor = 1
		rt.Configs = b.createdConfigs(meta.Topic{Name: t.Topic, Config: configMap(t.Configs)})
		return rt
	}

	created, err := b.meta.CreateTopic(t.Topic, partitions, configMap(t.Configs))
	if err != nil {
		if errors.Is(err, meta.ErrTopicExists) {
			rt.ErrorCode = kerr.TopicAlreadyExists.Code
			msg := fmt.Sprintf("Topic '%s' already exists.", t.Topic)
			rt.ErrorMessage = &msg
		} else if errors.Is(err, meta.ErrTopicCollides) {
			rt.ErrorCode = kerr.InvalidTopicException.Code
			msg := strings.TrimPrefix(err.Error(), "meta: ")
			rt.ErrorMessage = &msg
		} else {
			rt.ErrorCode = b.storageFault("create topic: register", err, "topic", t.Topic)
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
		rt.ErrorCode = b.storageFault("create topic: make the partition directories", err, "topic", t.Topic)
		msg := err.Error()
		rt.ErrorMessage = &msg
		return rt
	}

	b.applyTopicLimits(created.Name, created.Config)
	b.cg848.topicChanged(created.Name)

	rt.TopicID = created.ID
	rt.NumPartitions = created.Partitions
	rt.ReplicationFactor = 1
	rt.Configs = b.createdConfigs(created)
	return rt
}

// createdConfigs is the topic's effective configuration as CreateTopics (v5+) returns
// it. It is never null: the Java admin client reads it without a check, so a null here
// was a NullPointerException inside every createTopics call (Schema Registry, Connect
// and Streams all create their topics that way).
func (b *Broker) createdConfigs(t meta.Topic) []kmsg.CreateTopicsResponseTopicConfig {
	entries := b.topicConfigEntries(t, nil)
	out := make([]kmsg.CreateTopicsResponseTopicConfig, 0, len(entries))
	for _, e := range entries {
		c := kmsg.NewCreateTopicsResponseTopicConfig()
		c.Name = e.Name
		c.Value = e.Value
		c.ReadOnly = e.ReadOnly
		c.Source = int8(e.Source)
		out = append(out, c)
	}
	return out
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
		case !b.canTopic(req.Conn, cmp.Or(name, tg.name), opDelete):
			rt.ErrorCode = kerr.TopicAuthorizationFailed.Code
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
