package broker

import (
	"context"
	"slices"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/settings"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The admin config-read APIs — DescribeConfigs and DescribeCluster — back
// `kafka-configs --describe` and the cluster view that admin tools and UIs show.
// A topic's configuration is the broker's defaults overlaid with the topic's own
// values (set at CreateTopics or by AlterConfigs; see alterconfigs.go). The
// broker's own configuration is static and reported read-only.

// handleDescribeConfigs returns the effective configuration of a topic or the
// broker. Topic configs are the broker defaults overlaid with the per-topic
// overrides stored at CreateTopics; broker configs are the process's static
// settings.
func (b *Broker) handleDescribeConfigs(ctx context.Context, req *protocol.Request, dr *kmsg.DescribeConfigsRequest, resp *kmsg.DescribeConfigsResponse) error {
	for _, res := range dr.Resources {
		rr := kmsg.NewDescribeConfigsResponseResource()
		rr.ResourceType = res.ResourceType
		rr.ResourceName = res.ResourceName
		// An empty message, not a null one, where Kafka answers a resource
		// (claim: api V6).
		rr.ErrorMessage = new(string)

		switch res.ResourceType {
		case kmsg.ConfigResourceTypeTopic:
			t, ok, err := b.meta.Topic(res.ResourceName)
			if !b.canTopic(req.Conn, res.ResourceName, opDescribeConfig) {
				rr.ErrorCode, rr.ErrorMessage = kerr.TopicAuthorizationFailed.Code, nil
			} else if err != nil || !ok {
				rr.ErrorCode, rr.ErrorMessage = kerr.UnknownTopicOrPartition.Code, nil
			} else {
				rr.Configs = b.topicConfigEntries(t, res.ConfigNames)
			}
		case kmsg.ConfigResourceTypeBroker:
			if !b.canCluster(req.Conn, opDescribeConfig) {
				rr.ErrorCode, rr.ErrorMessage = kerr.ClusterAuthorizationFailed.Code, nil
				break
			}
			rr.Configs = b.brokerConfigEntries(res.ConfigNames)
		default:
			rr.ErrorCode = kerr.InvalidRequest.Code
			msg := "unsupported config resource type"
			rr.ErrorMessage = &msg
		}
		resp.Resources = append(resp.Resources, rr)
	}
	return nil
}

// configEntry is one effective config value plus where it came from.
type configEntry struct {
	value  string
	source kmsg.ConfigSource
}

func (b *Broker) topicConfigEntries(t meta.Topic, names []string) []kmsg.DescribeConfigsResponseResourceConfig {
	// The broker's defaults for the topic configs that have one (the settings
	// table says which)...
	eff := map[string]configEntry{}
	for _, s := range settings.All(settings.Topic) {
		if s.Default != nil {
			eff[s.Name] = configEntry{s.Default(b.cfg), kmsg.ConfigSourceDefaultConfig}
		}
	}
	// ...overlaid with per-topic overrides from CreateTopics (dynamic topic config).
	for k, v := range t.Config {
		eff[k] = configEntry{v, kmsg.ConfigSourceDynamicTopicConfig}
	}
	return configEntries(eff, names)
}

func (b *Broker) brokerConfigEntries(names []string) []kmsg.DescribeConfigsResponseResourceConfig {
	eff := map[string]configEntry{}
	for _, s := range settings.All(settings.Broker) {
		if s.Wire && s.Default != nil {
			eff[s.Name] = configEntry{s.Default(b.cfg), kmsg.ConfigSourceStaticBrokerConfig}
		}
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
func (b *Broker) handleDescribeCluster(ctx context.Context, req *protocol.Request, _ *kmsg.DescribeClusterRequest, resp *kmsg.DescribeClusterResponse) error {
	if !b.canCluster(req.Conn, opDescribe) {
		resp.ErrorCode = kerr.ClusterAuthorizationFailed.Code
		return nil
	}
	resp.ClusterID = b.clusterID()
	resp.ControllerID = b.nodeID()

	broker := kmsg.NewDescribeClusterResponseBroker()
	broker.NodeID = b.nodeID()
	broker.Host = b.cfg.AdvertisedHost
	broker.Port = b.advertisedPort()
	resp.Brokers = append(resp.Brokers, broker)

	// Sentinel meaning "authorized operations not included" (we have no ACLs).
	resp.ClusterAuthorizedOperations = -2147483648
	return nil
}
