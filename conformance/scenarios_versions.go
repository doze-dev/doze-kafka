package conformance

import (
	"fmt"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Scenarios of the API table (conformance/claims/api.md): what a real Apache Kafka
// answers to a default request of every API doze-kafka offers, at every version
// it offers it, and what it does with a version it does not serve.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "versions-not-served", Run: versionsNotServed},
		Scenario{Name: "version-matrix", Run: versionMatrix, Known: knownVersionMatrix},
	)
}

// knownVersionMatrix is what doze-kafka answers differently to a default request,
// each with its reason (docs/limitations.md).
var knownVersionMatrix = func() []Known {
	var k []Known
	// Group APIs with an empty group id: Kafka answers INVALID_GROUP_ID (24) or
	// NOT_COORDINATOR (16). JoinGroup, Heartbeat, LeaveGroup and OffsetDelete now answer
	// as Kafka does (recorded: group-requests-refused, claims groups R21a); these two
	// still answer for the group, and the replay holds the rest.
	for _, s := range []string{"OffsetFetch v*", "SyncGroup v*"} {
		k = append(k, Known{Step: s, Path: "ErrorCode", Limitation: limEmptyGroupID})
	}
	return append(k,
		// The list of APIs is the table's (claims api V1); ApiVersions v3 with no
		// client software name (Kafka: INVALID_REQUEST) is open, owned by the
		// work on apiversions.go.
		Known{Step: "ApiVersions v*", Path: "ApiKeys", Limitation: "ApiVersions lists the APIs and versions this broker serves, not Kafka's."},
		Known{Step: "ConsumerGroupHeartbeat v*", Path: "ErrorMessage", Limitation: limMessages},
		Known{Step: "GetTelemetrySubscriptions v*", Path: "PushIntervalMillis", Limitation: limSubscription},
		Known{Step: "GetTelemetrySubscriptions v*", Path: "RequestedMetrics", Limitation: limSubscription},
		Known{Step: "GetTelemetrySubscriptions v*", Path: "DeltaTemporality", Limitation: limSubscription},
	)
}()

const (
	limEmptyGroupID = "A group request with an empty group id is answered for the group"
)

// MatrixRequest is a request of an API with every field at its default, at a
// version, with the few settings without which it would not be answered at once:
// the request the version matrix sends. internal/broker's own matrix test sends
// the same (it holds every answer to decode and encode back to the same bytes).
func MatrixRequest(key, version int16) kmsg.Request {
	req := kmsg.RequestForKey(key)
	switch r := req.(type) {
	case *kmsg.FetchRequest:
		r.MaxWaitMillis = 0 // no long poll.
	case *kmsg.ProduceRequest:
		r.Acks = 1 // the default, 0, is fire-and-forget: Kafka answers it with nothing.
	case *kmsg.ConsumerGroupHeartbeatRequest:
		r.Group = "matrix"
	}
	req.SetVersion(version)
	return req
}

// MatrixSteps is every (API, version) the Kafka 4 profile offers with nothing but
// client telemetry configured, in key and version order: the steps of the
// version-matrix scenario, which is recorded again whenever the table changes
// (the replay fails on a step the recording does not have).
func MatrixSteps() [][2]int16 {
	profile, _ := protocol.ProfileFor(protocol.DefaultKafkaMajor)
	var out [][2]int16
	for _, a := range profile.Advertised(false, true) {
		for v := a.Min; v <= a.Max; v++ {
			out = append(out, [2]int16{int16(a.Key), v})
		}
	}
	return out
}

// MatrixLabel is the label of a matrix step: "Fetch v13".
func MatrixLabel(key, version int16) string {
	return fmt.Sprintf("%s v%d", kmsg.NameForKey(key), version)
}

// versionMatrix sends the default request of every offered (API, version), each
// over the scenario's one connection (made again when the broker closes it).
func versionMatrix(e *Env) {
	for _, kv := range MatrixSteps() {
		e.Do(MatrixLabel(kv[0], kv[1]), MatrixRequest(kv[0], kv[1]), kv[1])
	}
}

// versionsNotServed sends a default request at a version below the lowest Kafka
// 4.3.1 lists for the API in ApiVersions (recordings/api-versions.json), for the
// APIs whose older versions Kafka 4 removed (KIP-896), and at a version above the
// highest it lists, and two APIs it does not list. Kafka answers none of them: it
// closes the connection.
func versionsNotServed(e *Env) {
	for _, kv := range [][2]int16{
		{int16(kmsg.ListOffsets), 0},
		{int16(kmsg.OffsetCommit), 0}, {int16(kmsg.OffsetCommit), 1},
		{int16(kmsg.OffsetFetch), 0},
		{int16(kmsg.CreateTopics), 0}, {int16(kmsg.CreateTopics), 1},
		{int16(kmsg.DeleteTopics), 0},
		{int16(kmsg.DescribeACLs), 0}, {int16(kmsg.CreateACLs), 0}, {int16(kmsg.DeleteACLs), 0},
		{int16(kmsg.DescribeConfigs), 0},
		{int16(kmsg.Fetch), 3},
		// One above the highest Kafka 4.3.1 lists.
		{int16(kmsg.Metadata), 14},
		{int16(kmsg.FindCoordinator), 7},
		{int16(kmsg.Heartbeat), 5},
		{int16(kmsg.DescribeConfigs), 5},
		// APIs Kafka 4.3.1 does not list at all (ZooKeeper-era broker-to-broker
		// traffic), at a version the protocol has.
		{int16(kmsg.LeaderAndISR), 5},
		{int16(kmsg.ControlledShutdown), 3},
	} {
		e.Do(MatrixLabel(kv[0], kv[1])+" below or above", MatrixRequest(kv[0], kv[1]), kv[1])
		// The connection made after it is served: the broker is still there.
		e.Do(MatrixLabel(kv[0], kv[1])+" then served", metadata(false), 9)
	}
}
