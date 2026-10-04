package protocol

import "fmt"

// APIKey identifies a Kafka request type on the wire (the request header's
// api_key field). Only the keys this broker implements or plans to are named;
// see docs/api-support.md for the handler set.
type APIKey int16

const (
	APIProduce          APIKey = 0
	APIFetch            APIKey = 1
	APIListOffsets      APIKey = 2
	APIMetadata         APIKey = 3
	APIOffsetCommit     APIKey = 8
	APIOffsetFetch      APIKey = 9
	APIFindCoordinator  APIKey = 10
	APIJoinGroup        APIKey = 11
	APIHeartbeat        APIKey = 12
	APILeaveGroup       APIKey = 13
	APISyncGroup        APIKey = 14
	APIDescribeGroups   APIKey = 15
	APIListGroups       APIKey = 16
	APISASLHandshake    APIKey = 17
	APIApiVersions      APIKey = 18
	APICreateTopics     APIKey = 19
	APIDeleteTopics     APIKey = 20
	APIInitProducerID   APIKey = 22
	APIDescribeConfigs  APIKey = 32
	APIAlterConfigs     APIKey = 33
	APIIncrAlterConfigs APIKey = 44
	APIDeleteGroups     APIKey = 42
	APIOffsetDelete     APIKey = 47
	APISASLAuthenticate APIKey = 36
	APIDescribeCluster  APIKey = 60

	// KIP-848 broker-driven consumer group protocol.
	APIConsumerGroupHeartbeat APIKey = 68
	APIConsumerGroupDescribe  APIKey = 69
)

var apiKeyNames = map[APIKey]string{
	APIProduce:          "Produce",
	APIFetch:            "Fetch",
	APIListOffsets:      "ListOffsets",
	APIMetadata:         "Metadata",
	APIOffsetCommit:     "OffsetCommit",
	APIOffsetFetch:      "OffsetFetch",
	APIFindCoordinator:  "FindCoordinator",
	APIJoinGroup:        "JoinGroup",
	APIHeartbeat:        "Heartbeat",
	APILeaveGroup:       "LeaveGroup",
	APISyncGroup:        "SyncGroup",
	APIDescribeGroups:   "DescribeGroups",
	APIListGroups:       "ListGroups",
	APISASLHandshake:    "SaslHandshake",
	APIApiVersions:      "ApiVersions",
	APICreateTopics:     "CreateTopics",
	APIDeleteTopics:     "DeleteTopics",
	APIInitProducerID:   "InitProducerId",
	APIDescribeConfigs:  "DescribeConfigs",
	APIAlterConfigs:     "AlterConfigs",
	APIIncrAlterConfigs: "IncrementalAlterConfigs",
	APIDeleteGroups:     "DeleteGroups",
	APIOffsetDelete:     "OffsetDelete",
	APISASLAuthenticate: "SaslAuthenticate",
	APIDescribeCluster:  "DescribeCluster",

	APIConsumerGroupHeartbeat: "ConsumerGroupHeartbeat",
	APIConsumerGroupDescribe:  "ConsumerGroupDescribe",
}

// Name returns a human-readable name for logging, or "Unknown(N)".
func (k APIKey) Name() string {
	if n, ok := apiKeyNames[k]; ok {
		return n
	}
	return fmt.Sprintf("Unknown(%d)", int16(k))
}
