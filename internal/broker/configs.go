package broker

import (
	"context"
	"slices"
	"strconv"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// The admin config-read APIs — DescribeConfigs and DescribeCluster — back
// `kafka-configs --describe` and the cluster view that admin tools and UIs show.
// A topic's configuration is the broker's defaults overlaid with the topic's own
// values (set at CreateTopics or by AlterConfigs; see alterconfigs.go). The
// broker's own configuration is static and reported read-only.

// unbounded is how Kafka reports a disabled size/time bound; our config uses 0.
func retentionValue(v int64) string {
	if v <= 0 {
		return "-1"
	}
	return strconv.FormatInt(v, 10)
}

// handleDescribeConfigs returns the effective configuration of a topic or the
// broker. Topic configs are the broker defaults overlaid with the per-topic
// overrides stored at CreateTopics; broker configs are the process's static
// settings.
func (b *Broker) handleDescribeConfigs(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	dr := kreq.(*kmsg.DescribeConfigsRequest)

	resp := kmsg.NewPtrDescribeConfigsResponse()
	resp.SetVersion(req.Header.APIVersion)

	for _, res := range dr.Resources {
		rr := kmsg.NewDescribeConfigsResponseResource()
		rr.ResourceType = res.ResourceType
		rr.ResourceName = res.ResourceName

		switch res.ResourceType {
		case kmsg.ConfigResourceTypeTopic:
			t, ok, err := b.meta.Topic(res.ResourceName)
			if err != nil || !ok {
				rr.ErrorCode = int16(protocol.ErrUnknownTopicOrPart)
			} else {
				rr.Configs = b.topicConfigEntries(t, res.ConfigNames)
			}
		case kmsg.ConfigResourceTypeBroker:
			rr.Configs = b.brokerConfigEntries(res.ConfigNames)
		default:
			rr.ErrorCode = int16(protocol.ErrInvalidRequest)
			msg := "unsupported config resource type"
			rr.ErrorMessage = &msg
		}
		resp.Resources = append(resp.Resources, rr)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// configEntry is one effective config value plus where it came from.
type configEntry struct {
	value  string
	source kmsg.ConfigSource
}

func (b *Broker) topicConfigEntries(t meta.Topic, names []string) []kmsg.DescribeConfigsResponseResourceConfig {
	// Broker defaults for the standard topic knobs...
	eff := map[string]configEntry{
		"cleanup.policy":       {"delete", kmsg.ConfigSourceDefaultConfig},
		"retention.ms":         {retentionValue(b.cfg.RetentionMs), kmsg.ConfigSourceDefaultConfig},
		"retention.bytes":      {retentionValue(b.cfg.RetentionBytes), kmsg.ConfigSourceDefaultConfig},
		"segment.bytes":        {strconv.FormatInt(b.cfg.SegmentBytes, 10), kmsg.ConfigSourceDefaultConfig},
		"index.interval.bytes": {strconv.FormatInt(b.cfg.IndexIntervalBytes, 10), kmsg.ConfigSourceDefaultConfig},
	}
	// ...overlaid with per-topic overrides from CreateTopics (dynamic topic config).
	for k, v := range t.Config {
		eff[k] = configEntry{v, kmsg.ConfigSourceDynamicTopicConfig}
	}
	return configEntries(eff, names)
}

func (b *Broker) brokerConfigEntries(names []string) []kmsg.DescribeConfigsResponseResourceConfig {
	eff := map[string]configEntry{
		"log.retention.bytes":       {retentionValue(b.cfg.RetentionBytes), kmsg.ConfigSourceStaticBrokerConfig},
		"log.retention.ms":          {retentionValue(b.cfg.RetentionMs), kmsg.ConfigSourceStaticBrokerConfig},
		"log.segment.bytes":         {strconv.FormatInt(b.cfg.SegmentBytes, 10), kmsg.ConfigSourceStaticBrokerConfig},
		"log.index.interval.bytes":  {strconv.FormatInt(b.cfg.IndexIntervalBytes, 10), kmsg.ConfigSourceStaticBrokerConfig},
		"num.partitions":            {strconv.Itoa(b.cfg.DefaultPartitions), kmsg.ConfigSourceStaticBrokerConfig},
		"auto.create.topics.enable": {strconv.FormatBool(b.cfg.AutoCreateTopics), kmsg.ConfigSourceStaticBrokerConfig},
	}
	return configEntries(eff, names)
}

// configEntries renders an effective-config map into the response, filtered to
// names when the request asked for specific keys, sorted for determinism.
func configEntries(eff map[string]configEntry, names []string) []kmsg.DescribeConfigsResponseResourceConfig {
	want := func(name string) bool { return len(names) == 0 || slices.Contains(names, name) }

	keys := make([]string, 0, len(eff))
	for k := range eff {
		if want(k) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)

	out := make([]kmsg.DescribeConfigsResponseResourceConfig, 0, len(keys))
	for _, k := range keys {
		e := eff[k]
		c := kmsg.NewDescribeConfigsResponseResourceConfig()
		c.Name = k
		val := e.value
		c.Value = &val
		c.Source = e.source
		c.IsDefault = e.source == kmsg.ConfigSourceDefaultConfig
		c.ReadOnly = e.source == kmsg.ConfigSourceStaticBrokerConfig // broker config isn't alterable at runtime.
		out = append(out, c)
	}
	return out
}

// handleDescribeCluster returns the single-node cluster view: this broker is the
// only node and its own controller.
func (b *Broker) handleDescribeCluster(ctx context.Context, req *protocol.Request) ([]byte, error) {
	if _, err := protocol.DecodeRequest(req.Header, req.Body); err != nil {
		return nil, err
	}

	resp := kmsg.NewPtrDescribeClusterResponse()
	resp.SetVersion(req.Header.APIVersion)
	resp.ClusterID = b.clusterID()
	resp.ControllerID = b.nodeID()

	broker := kmsg.NewDescribeClusterResponseBroker()
	broker.NodeID = b.nodeID()
	broker.Host = b.cfg.AdvertisedHost
	broker.Port = b.advertisedPort()
	resp.Brokers = append(resp.Brokers, broker)

	// Sentinel meaning "authorized operations not included" (we have no ACLs).
	resp.ClusterAuthorizedOperations = -2147483648
	return protocol.EncodeResponse(req.Header, resp), nil
}
