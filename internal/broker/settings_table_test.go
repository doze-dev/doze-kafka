package broker_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/settings"
)

// kafkaTopicConfigs is Apache Kafka 4.3.1's topic-level config list: the 33 names a
// real broker reports for a topic, in the order it reports them. Obtained on 2026-10-12
// from the image the recordings use (apache/kafka:4.3.1, single-node KRaft, default
// config), with
//
//	kafka-topics.sh --bootstrap-server localhost:9092 --create --topic t
//	kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name t --describe --all
//
// The test is deliberately a list of its own, not read from the settings table: it
// asks the broker which keys it accepts, over the wire, and the table must have a
// record for every one.
var kafkaTopicConfigs = []string{
	"cleanup.policy", "compression.gzip.level", "compression.lz4.level", "compression.type",
	"compression.zstd.level", "delete.retention.ms", "file.delete.delay.ms", "flush.messages",
	"flush.ms", "follower.replication.throttled.replicas", "index.interval.bytes",
	"leader.replication.throttled.replicas", "local.retention.bytes", "local.retention.ms",
	"max.compaction.lag.ms", "max.message.bytes", "message.timestamp.after.max.ms",
	"message.timestamp.before.max.ms", "message.timestamp.type", "min.cleanable.dirty.ratio",
	"min.compaction.lag.ms", "min.insync.replicas", "preallocate", "remote.log.copy.disable",
	"remote.log.delete.on.disable", "remote.storage.enable", "retention.bytes", "retention.ms",
	"segment.bytes", "segment.index.bytes", "segment.jitter.ms", "segment.ms",
	"unclean.leader.election.enable",
}

// notKafka431TopicConfigs are names Apache Kafka 4.3.1 does not have: the same broker
// refused each with "Unknown topic config name" (kafka-configs.sh --alter --add-config,
// same day). They are probed like the rest, and the rule is the same: the broker
// accepts a name if and only if the table has a record for it.
//
// The first three are a difference from Kafka 4.3.1, not agreement with it: this
// broker accepts them and the settings table stores them, and Kafka 4.3.1 refuses
// them. This test does not judge that; it only keeps the broker and the table in step.
var notKafka431TopicConfigs = []string{
	"message.downconversion.enable", "message.format.version", "message.timestamp.difference.max.ms",
	// Never topic configs: the broker must refuse these, and the table must not know them.
	"no.such.config", "log.retention.ms", "retention",
}

// Every topic config the broker accepts (asked over the wire, by
// IncrementalAlterConfigs with validate_only) has a record in the settings table,
// and every key DescribeConfigs reports, for a topic and for the broker, has one.
func TestSettingsTable_EveryAcceptedTopicConfigHasARecord(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "settings-probe", 1)

	values := []string{"1", "true", "delete", "CreateTime", "0.5", "gzip", "1048576"}
	for _, key := range append(append([]string{}, kafkaTopicConfigs...), notKafka431TopicConfigs...) {
		accepted := false
		for _, v := range values {
			code, msg := incrementalAlter(t, ctx, cl, kmsg.ConfigResourceTypeTopic, "settings-probe", true, alterOp{name: key, op: opSet, value: v})
			if code == 0 || !strings.Contains(msg, "unknown topic config") {
				accepted = true
				break
			}
		}
		_, has := settings.TopicConfig(key)
		switch {
		case accepted && !has:
			t.Errorf("the broker accepts topic config %q and the settings table has no record of it", key)
		case !accepted && has:
			t.Errorf("the settings table has a record for %q, which the broker refuses", key)
		}
	}

	// What DescribeConfigs reports is a subset of the table.
	topic, _ := describe(t, ctx, cl, "settings-probe")
	for k := range topic {
		if _, ok := settings.TopicConfig(k); !ok {
			t.Errorf("DescribeConfigs reports topic config %q, which has no record", k)
		}
	}
	req := kmsg.NewPtrDescribeConfigsRequest()
	rr := kmsg.NewDescribeConfigsRequestResource()
	rr.ResourceType, rr.ResourceName = kmsg.ConfigResourceTypeBroker, ""
	req.Resources = append(req.Resources, rr)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Resources) != 1 {
		t.Fatalf("DescribeConfigs broker: %v %+v", err, resp)
	}
	if len(resp.Resources[0].Configs) == 0 {
		t.Fatal("DescribeConfigs reported no broker configs")
	}
	for _, c := range resp.Resources[0].Configs {
		if s, ok := settings.Lookup(settings.Broker, c.Name); !ok || !s.Wire {
			t.Errorf("DescribeConfigs reports broker config %q, which has no wire record", c.Name)
		}
	}
}
