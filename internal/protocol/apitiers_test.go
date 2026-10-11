package protocol

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// Foundation B6: nothing absent. docs/api-support.md has one row for every API
// key the protocol library knows (the Kafka 4.x specification), with a tier:
//
//	F  functional: it works locally and its effect is observable.
//	C  cosmetic: accepted and round-tripped with no local effect.
//	S  stub: not served. Every S row says why.
//
// Whether a key is advertised is read from the API table, so a row cannot say
// F for an API the broker does not serve. The only hand-written parts are the
// reasons and the notes below.

// Reasons shared by many keys.
const (
	reasonPeer    = "Broker-to-broker traffic; there is one node and no peer sends it or waits for it."
	reasonQuorum  = "KRaft controller-quorum management; there is no controller quorum, the one node is its own metadata store."
	reasonShare   = "Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848."
	reasonStreams = "Streams groups (KIP-1071) are not implemented; Kafka Streams uses the classic group protocol."
	reasonReassgn = "Replica placement and reassignment; every partition has one replica on this node."
	reasonDelegat = "Delegation tokens are not implemented; authentication is SASL/PLAIN and SASL/SCRAM."
	reasonRegBrkr = "Broker registration with a controller; there is no controller to register with."
)

// apiNotes holds the exceptions to "advertised means F, and a note only where
// there is something to say". A C row is cosmetic in the whole; a F row with a
// note is functional with a caveat.
var apiNotes = map[kmsg.Key]struct{ tier, note string }{
	kmsg.Produce:                   {"F", "`acks=all` fsyncs before the reply."},
	kmsg.Fetch:                     {"F", "Long-poll capped at 30 s; no fetch sessions."},
	kmsg.DescribeConfigs:           {"F", "Topic configs; broker configs read-only."},
	kmsg.AlterConfigs:              {"F", "Five topic settings act, the rest are stored (see [Limitations](limitations.md))."},
	kmsg.IncrementalAlterConfigs:   {"F", "As AlterConfigs."},
	kmsg.CreateTopics:              {"F", "A replication factor above 1 is not honoured (one node)."},
	kmsg.FindCoordinator:           {"F", "Always this node."},
	kmsg.DescribeCluster:           {"F", "This single node."},
	kmsg.SASLHandshake:             {"F", "Only when SASL is configured."},
	kmsg.SASLAuthenticate:          {"F", "Only when SASL is configured."},
	kmsg.ConsumerGroupHeartbeat:    {"F", "Kafka 4 profile only."},
	kmsg.ConsumerGroupDescribe:     {"F", "Kafka 4 profile only."},
	kmsg.GetTelemetrySubscriptions: {"F", "Kafka 4 profile only; offered with the console on and not `--client-telemetry=false`. Answers a fixed subscription (every `org.apache.kafka.` metric, every 5 s, cumulative: C, there is no `CLIENT_METRICS` resource); `INVALID_REQUEST` when one connection already holds 8 instances."},
	kmsg.PushTelemetry:             {"F", "Kafka 4 profile only; same condition. Codes: `INVALID_REQUEST` (no instance id, an undecodable or cut-off payload, another principal's instance), `UNKNOWN_SUBSCRIPTION_ID`, `UNSUPPORTED_COMPRESSION_TYPE`, `TELEMETRY_TOO_LARGE`. A push faster than the interval allows is answered 0 and counted, not read."},

	kmsg.LeaderAndISR:                 {"S", reasonPeer},
	kmsg.StopReplica:                  {"S", reasonPeer},
	kmsg.UpdateMetadata:               {"S", reasonPeer},
	kmsg.ControlledShutdown:           {"S", "The controller's shutdown handshake; there is no controller."},
	kmsg.OffsetForLeaderEpoch:         {"S", "Replica truncation after a leader change; there are no leader changes. A client falls back to ListOffsets."},
	kmsg.WriteTxnMarkers:              {"S", "A transaction coordinator writing markers to other brokers' partitions; the coordinator here writes its own log directly."},
	kmsg.AlterReplicaLogDirs:          {"S", "Moves a replica between log directories; there is one data directory."},
	kmsg.DescribeLogDirs:              {"S", "Not implemented yet: per-partition sizes are planned (the topics stage). A UI that probes it gets the connection closed."},
	kmsg.CreateDelegationToken:        {"S", reasonDelegat},
	kmsg.RenewDelegationToken:         {"S", reasonDelegat},
	kmsg.ExpireDelegationToken:        {"S", reasonDelegat},
	kmsg.DescribeDelegationToken:      {"S", reasonDelegat},
	kmsg.ElectLeaders:                 {"S", "There is one replica and it is always the leader."},
	kmsg.AlterPartitionAssignments:    {"S", reasonReassgn},
	kmsg.ListPartitionReassignments:   {"S", reasonReassgn},
	kmsg.DescribeClientQuotas:         {"S", "Quotas are not implemented: nothing is throttled, so there is nothing to describe."},
	kmsg.AlterClientQuotas:            {"S", "Quotas are not implemented: nothing is throttled."},
	kmsg.DescribeUserSCRAMCredentials: {"S", "SCRAM users come from the configuration file and flags; they cannot be managed over the wire."},
	kmsg.AlterUserSCRAMCredentials:    {"S", "SCRAM users come from the configuration file and flags; they cannot be managed over the wire."},
	kmsg.Vote:                         {"S", reasonQuorum},
	kmsg.BeginQuorumEpoch:             {"S", reasonQuorum},
	kmsg.EndQuorumEpoch:               {"S", reasonQuorum},
	kmsg.DescribeQuorum:               {"S", reasonQuorum},
	kmsg.AlterPartition:               {"S", "ISR changes sent to the controller; there is no ISR to change."},
	kmsg.UpdateFeatures:               {"S", "Cluster feature levels are not tracked; the broker emulates one Kafka major (`--kafka-version`)."},
	kmsg.Envelope:                     {"S", "Forwarding a request from a broker to the controller; there is no controller to forward to."},
	kmsg.FetchSnapshot:                {"S", "KRaft metadata snapshots; there is no metadata log to snapshot."},
	kmsg.BrokerRegistration:           {"S", reasonRegBrkr},
	kmsg.BrokerHeartbeat:              {"S", reasonRegBrkr},
	kmsg.UnregisterBroker:             {"S", reasonRegBrkr},
	kmsg.ControllerRegistration:       {"S", reasonRegBrkr},
	kmsg.AllocateProducerIDs:          {"S", "A broker asking the controller for a block of producer ids; this node allocates its own."},
	kmsg.AssignReplicasToDirs:         {"S", "Replica-to-directory assignment between a broker and the controller; one directory."},
	kmsg.ListConfigResources:          {"S", "Not implemented; Metadata and DescribeConfigs cover what a client needs."},
	kmsg.DescribeTopicPartitions:      {"S", "Not implemented; Metadata serves the same answer, and clients fall back to it."},
	kmsg.ShareGroupHeartbeat:          {"S", reasonShare},
	kmsg.ShareGroupDescribe:           {"S", reasonShare},
	kmsg.ShareFetch:                   {"S", reasonShare},
	kmsg.ShareAcknowledge:             {"S", reasonShare},
	kmsg.InitializeShareGroupState:    {"S", reasonShare},
	kmsg.ReadShareGroupState:          {"S", reasonShare},
	kmsg.WriteShareGroupState:         {"S", reasonShare},
	kmsg.DeleteShareGroupState:        {"S", reasonShare},
	kmsg.ReadShareGroupStateSummary:   {"S", reasonShare},
	kmsg.DescribeShareGroupOffsets:    {"S", reasonShare},
	kmsg.AlterShareGroupOffsets:       {"S", reasonShare},
	kmsg.DeleteShareGroupOffsets:      {"S", reasonShare},
	kmsg.AddRaftVoter:                 {"S", reasonQuorum},
	kmsg.RemoveRaftVoter:              {"S", reasonQuorum},
	kmsg.UpdateRaftVoter:              {"S", reasonQuorum},
	kmsg.StreamsGroupHeartbeat:        {"S", reasonStreams},
	kmsg.StreamsGroupDescribe:         {"S", reasonStreams},
}

type tierRow struct {
	key        kmsg.Key
	name, tier string
	note       string
}

// tierRows is one row per key the protocol library knows, in key order.
func tierRows(t testing.TB) []tierRow {
	adv := map[APIKey]bool{}
	for _, a := range APIs {
		adv[a.Key] = true
	}
	var rows []tierRow
	for k := int16(0); k < 1000; k++ {
		if kmsg.RequestForKey(k) == nil {
			continue
		}
		key := kmsg.Key(k)
		row := tierRow{key: key, name: kmsg.NameForKey(k)}
		n, hasNote := apiNotes[key]
		switch {
		case adv[key]:
			row.tier = "F"
			if hasNote {
				row.tier, row.note = n.tier, n.note
			}
		default:
			if !hasNote {
				t.Errorf("%s (key %d) is a Kafka API the broker does not advertise and has no S entry with a reason in apiNotes", row.name, k)
				row.tier = "S"
			} else {
				row.tier, row.note = n.tier, n.note
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func tiersTable(t testing.TB) string {
	var b strings.Builder
	b.WriteString("| API | Key | Tier | Note |\n|---|---|---|---|\n")
	for _, r := range tierRows(t) {
		fmt.Fprintf(&b, "| %s | %d | %s | %s |\n", r.name, r.key, r.tier, r.note)
	}
	return b.String()
}

func TestEveryKafkaAPIHasATier(t *testing.T) {
	rows := tierRows(t)
	if len(rows) < 90 {
		t.Fatalf("the protocol library knows %d API keys; the Kafka 4 specification has about 90", len(rows))
	}
	known := map[kmsg.Key]bool{}
	adv := map[APIKey]bool{}
	for _, a := range APIs {
		adv[a.Key] = true
	}
	counts := map[string]int{}
	for _, r := range rows {
		known[r.key] = true
		counts[r.tier]++
		switch r.tier {
		case "F", "C", "S":
		default:
			t.Errorf("%s: tier %q is not F, C or S", r.name, r.tier)
		}
		if r.tier == "S" && strings.TrimSpace(r.note) == "" {
			t.Errorf("%s is S with no reason", r.name)
		}
		if r.tier == "S" && adv[r.key] {
			t.Errorf("%s is advertised and served, so it is not a stub", r.name)
		}
		if r.tier != "S" && !adv[r.key] {
			t.Errorf("%s is %s but the broker does not advertise it", r.name, r.tier)
		}
	}
	// An entry for a key kmsg does not know is a typo or a stale row.
	var stale []string
	for k := range apiNotes {
		if !known[k] {
			stale = append(stale, fmt.Sprint(int16(k)))
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("apiNotes has keys the protocol library does not know: %v", stale)
	}
	for _, a := range APIs {
		if !known[a.Key] {
			t.Errorf("served key %d is not in the protocol library", a.Key)
		}
	}
	t.Logf("%d keys: F %d, C %d, S %d", len(rows), counts["F"], counts["C"], counts["S"])
}

// The document's tier table is generated from kmsg and the API table. Regenerate
// it: UPDATE_API_DOCS=1 go test ./internal/protocol -run TestAPITiersDoc
func TestAPITiersDocMatches(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "api-support.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	const begin, end = "<!-- api-tiers:begin (generated from the protocol library's key table and internal/protocol/versions.go; do not edit by hand) -->\n", "<!-- api-tiers:end -->"
	i, j := strings.Index(doc, begin), strings.Index(doc, end)
	if i < 0 || j < i {
		t.Fatalf("%s has no generated table between the api-tiers markers", path)
	}
	want := tiersTable(t)
	if got := doc[i+len(begin) : j]; got != want {
		if os.Getenv("UPDATE_API_DOCS") != "" {
			if err := os.WriteFile(path, []byte(doc[:i+len(begin)]+want+doc[j:]), 0o644); err != nil {
				t.Fatal(err)
			}
			return
		}
		t.Errorf("docs/api-support.md tier table is out of date.\nRegenerate it: UPDATE_API_DOCS=1 go test ./internal/protocol -run TestAPITiersDoc\n\nwant:\n%s\ngot:\n%s", want, got)
	}
}
