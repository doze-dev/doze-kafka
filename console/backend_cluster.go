package console

import (
	"context"
	"sort"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ClusterInfo is a snapshot of the broker's own identity and capabilities —
// deliberately modest. doze-kafka is single-node, so there's genuinely
// little to show here; padding it out with a dashboard would be dishonest.
type ClusterInfo struct {
	NodeID        int32
	ClusterID     string
	Host          string
	Port          int32
	SupportedAPIs []string // human-readable names, sorted

	// How this console's own connection is secured — facts the console knows
	// first-hand from its dial options, not read off the wire.
	AuthMech string // SASL mechanism in use, "" when the broker runs open
	TLS      bool
}

// apiKeyNames is the small set of API keys doze-kafka implements — a public
// Kafka-protocol fact (see docs/api-support.md), not something read from
// doze-kafka's internal packages: the console is a pure external client and
// deliberately never imports internal/protocol.
var apiKeyNames = map[int16]string{
	0: "Produce", 1: "Fetch", 2: "ListOffsets", 3: "Metadata",
	8: "OffsetCommit", 9: "OffsetFetch", 10: "FindCoordinator",
	11: "JoinGroup", 12: "Heartbeat", 13: "LeaveGroup", 14: "SyncGroup",
	15: "DescribeGroups", 16: "ListGroups", 17: "SaslHandshake",
	18: "ApiVersions", 19: "CreateTopics", 20: "DeleteTopics",
	22: "InitProducerId", 32: "DescribeConfigs", 36: "SaslAuthenticate",
	60: "DescribeCluster", 68: "ConsumerGroupHeartbeat", 69: "ConsumerGroupDescribe",
}

// ClusterInfo describes the broker itself, via DescribeCluster + ApiVersions
// — the same two calls a real admin client (kadm, kafka-broker-api-versions)
// would make.
func (b *backend) ClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	dreq := kmsg.NewPtrDescribeClusterRequest()
	dresp, err := dreq.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if dresp.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(dresp.ErrorCode)
	}
	info := &ClusterInfo{ClusterID: dresp.ClusterID, TLS: b.opts.TLSConfig != nil}
	if b.opts.SASLUser != "" {
		if info.AuthMech = b.opts.SASLMechanism; info.AuthMech == "" {
			info.AuthMech = "PLAIN"
		}
	}
	for _, br := range dresp.Brokers {
		if br.NodeID == dresp.ControllerID {
			info.NodeID, info.Host, info.Port = br.NodeID, br.Host, br.Port
			break
		}
	}
	if info.Host == "" && len(dresp.Brokers) > 0 {
		info.NodeID, info.Host, info.Port = dresp.Brokers[0].NodeID, dresp.Brokers[0].Host, dresp.Brokers[0].Port
	}

	vreq := kmsg.NewPtrApiVersionsRequest()
	vreq.ClientSoftwareName = "doze-kafka-console"
	vreq.ClientSoftwareVersion = "0"
	vresp, err := vreq.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	for _, k := range vresp.ApiKeys {
		if name, ok := apiKeyNames[k.ApiKey]; ok {
			info.SupportedAPIs = append(info.SupportedAPIs, name)
		}
	}
	sort.Strings(info.SupportedAPIs)
	return info, nil
}
