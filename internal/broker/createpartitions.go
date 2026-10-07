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

// handleCreatePartitions grows topics. Count is the FINAL number of partitions, and
// it must be above the current one: Kafka never removes partitions, because that
// would orphan the records in them and move every key. Terraform, Spring's
// KafkaAdmin (a NewTopic bean whose partition count was raised) and kafka-topics
// --alter all arrive here.
//
// The new partitions' directories are made before the registry says they exist, so
// a request that sees the new count never finds a partition with nowhere to live.
// Groups learn of them as they would of a new topic: a KIP-848 group is reassigned
// here, and a classic one notices on its next metadata refresh.
func (b *Broker) handleCreatePartitions(ctx context.Context, req *protocol.Request, cr *kmsg.CreatePartitionsRequest, resp *kmsg.CreatePartitionsResponse) error {
	seen := map[string]int{}
	for _, t := range cr.Topics {
		seen[t.Topic]++
	}
	for _, t := range cr.Topics {
		rt := kmsg.NewCreatePartitionsResponseTopic()
		rt.Topic = t.Topic
		if seen[t.Topic] > 1 {
			setPartitionsError(&rt, kerr.InvalidRequest, "the topic is named more than once in this request")
		} else if !b.canTopic(req.Conn, t.Topic, opAlter) {
			setPartitionsError(&rt, kerr.TopicAuthorizationFailed, "not authorized to alter topic "+t.Topic)
		} else if ec, msg := b.growTopic(t, cr.ValidateOnly); ec != nil {
			setPartitionsError(&rt, ec, msg)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return nil
}

func setPartitionsError(rt *kmsg.CreatePartitionsResponseTopic, e *kerr.Error, msg string) {
	rt.ErrorCode = e.Code
	rt.ErrorMessage = &msg
}

// growTopic applies one topic of a request, or says why not.
func (b *Broker) growTopic(t kmsg.CreatePartitionsRequestTopic, validateOnly bool) (*kerr.Error, string) {
	// Serialised with config changes and with other growths: two requests that each
	// read the current count would otherwise both think theirs was the increase.
	b.alterMu.Lock()
	defer b.alterMu.Unlock()

	cur, ok, err := b.meta.Topic(t.Topic)
	if err != nil {
		return kerr.UnknownServerError, err.Error()
	}
	if !ok {
		return kerr.UnknownTopicOrPartition, fmt.Sprintf("there is no topic %q", t.Topic)
	}
	switch {
	case t.Count == cur.Partitions:
		return kerr.InvalidPartitions, fmt.Sprintf("the topic already has %d partitions", cur.Partitions)
	case t.Count < cur.Partitions:
		return kerr.InvalidPartitions, fmt.Sprintf("the topic has %d partitions and %d is fewer: partitions cannot be removed", cur.Partitions, t.Count)
	case int(t.Count) > b.cfg.MaxPartitions:
		return kerr.InvalidPartitions, fmt.Sprintf("%d partitions is above this broker's limit of %d (--max-partitions)", t.Count, b.cfg.MaxPartitions)
	}
	// An explicit assignment is only meaningful if it says what a single node
	// can do: one new entry per new partition, each on this broker alone.
	if t.Assignment != nil {
		if len(t.Assignment) != int(t.Count-cur.Partitions) {
			return kerr.InvalidReplicaAssignment, fmt.Sprintf("the assignment names %d new partitions, the request adds %d", len(t.Assignment), t.Count-cur.Partitions)
		}
		for _, a := range t.Assignment {
			if len(a.Replicas) != 1 || a.Replicas[0] != b.nodeID() {
				return kerr.InvalidReplicaAssignment, fmt.Sprintf("this broker is the only node (id %d) and every partition has one replica", b.nodeID())
			}
		}
	}
	if validateOnly {
		return nil, ""
	}

	if err := b.logs.EnsurePartitionDirs(t.Topic, t.Count); err != nil {
		b.log.Warn("create partitions: could not make the partition directories", "topic", t.Topic, "err", err)
		return kerr.UnknownServerError, err.Error()
	}
	updated, err := b.meta.SetPartitions(t.Topic, t.Count)
	switch {
	case errors.Is(err, meta.ErrNoSuchTopic): // deleted since the lookup above.
		return kerr.UnknownTopicOrPartition, fmt.Sprintf("there is no topic %q", t.Topic)
	case errors.Is(err, meta.ErrPartitionsNotIncreased):
		return kerr.InvalidPartitions, strings.TrimPrefix(err.Error(), "meta: ")
	case err != nil:
		b.log.Warn("create partitions: could not store the new count", "topic", t.Topic, "err", err)
		return kerr.UnknownServerError, err.Error()
	}
	b.cg848.topicChanged(updated.Name)
	b.produce.signal(updated.Name)
	return nil, ""
}
