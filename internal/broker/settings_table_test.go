package broker_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/settings"
)

// kafkaTopicConfigs is Apache Kafka 4.0's topic-level config list (`doc`:
// kafka.apache.org/documentation/#topicconfigs), plus keys that were never Kafka's,
// which must be refused. The test is deliberately a list of its own, not read from
// the settings table: it asks the broker which keys it accepts, over the wire, and
// the table must have a record for every one.
var kafkaTopicConfigs = []string{
	"cleanup.policy", "compression.type", "compression.gzip.level", "compression.lz4.level",
	"compression.zstd.level", "delete.retention.ms", "file.delete.delay.ms", "flush.messages",
	"flush.ms", "follower.replication.throttled.replicas", "index.interval.bytes",
	"leader.replication.throttled.replicas", "local.retention.bytes", "local.retention.ms",
	"max.compaction.lag.ms", "max.message.bytes", "message.downconversion.enable",
	"message.format.version", "message.timestamp.after.max.ms", "message.timestamp.before.max.ms",
	"message.timestamp.difference.max.ms", "message.timestamp.type", "min.cleanable.dirty.ratio",
	"min.compaction.lag.ms", "min.insync.replicas", "preallocate", "remote.log.copy.disable",
	"remote.log.delete.on.disable", "remote.storage.enable", "retention.bytes", "retention.ms",
	"segment.bytes", "segment.index.bytes", "segment.jitter.ms", "segment.ms",
	"unclean.leader.election.enable",
	// Not topic configs: the broker must refuse these, and the table must not know them.
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
	for _, key := range kafkaTopicConfigs {
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
