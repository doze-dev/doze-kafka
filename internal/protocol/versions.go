package protocol

import (
	"fmt"
	"sort"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// APIKey identifies a Kafka request type on the wire (the request header's
// api_key field). It is kmsg's key type: the names and numbers are the protocol
// library's, not a second list kept here.
type APIKey = kmsg.Key

// APIName is a key's name for logs: "Fetch", or "Unknown(99)" for a key the
// protocol library does not know.
func APIName(k APIKey) string {
	if kmsg.RequestForKey(int16(k)) == nil {
		return fmt.Sprintf("Unknown(%d)", int16(k))
	}
	return k.Name()
}

// GroupProtocol selects which consumer-group protocol(s) a version profile
// advertises. See docs/api-support.md and docs/consumer-groups.md.
type GroupProtocol int

const (
	// GroupClassic advertises only the classic, client-driven protocol
	// (JoinGroup/SyncGroup/Heartbeat). Valid for every supported Kafka major.
	GroupClassic GroupProtocol = iota

	// GroupClassicAndConsumer additionally advertises the KIP-848 broker-driven
	// protocol (ConsumerGroupHeartbeat/Describe). Kafka 4+ only.
	GroupClassicAndConsumer
)

// VersionRange is the inclusive [Min,Max] span of an API supported on the wire.
type VersionRange struct {
	Min int16
	Max int16
}

// Offered is whether an API is offered when emulating one Kafka major, and at
// which versions.
type Offered struct {
	VersionRange
	OK bool
}

func v(min, max int16) Offered { return Offered{VersionRange{min, max}, true} }

var none = Offered{}

// Condition is something beyond the Kafka major that an API depends on.
type Condition int

const (
	Always   Condition = iota
	WhenSASL           // only when SASL authentication is configured.
)

// API is one row of the table below.
type API struct {
	Key APIKey
	// Versions is what is offered when emulating Kafka 1, 2, 3 and 4, in that order.
	Versions [4]Offered
	When     Condition
}

// APIs is every Kafka API this broker serves: the one table. ApiVersions is
// answered from it, the router refuses whatever is not in it, the broker must
// have a handler for every row (it will not start otherwise), and the table in
// docs/api-support.md is checked against it. Adding an API is a row here and a
// handler in internal/broker.
//
// The ranges are ours, not a copy of Apache Kafka's: each column is close to what
// a real broker of that major offers, capped where a newer version carries
// something not implemented here. Two floors are deliberate. Produce starts at v3
// and Fetch at v4, the first versions that carry record batch format v2, the only
// format the log stores; earlier versions use the legacy message-set layout, and
// advertising them invited clients to send data the log would keep as garbage.
// OffsetCommit and OffsetFetch stop below v10, where the wire form switches to
// topic ids. SaslHandshake is v1 only: v0 means "raw tokens follow", which is not
// served. The two KIP-848 APIs exist only in the Kafka 4 column, which is what
// makes a client at group.protocol=consumer negotiate that protocol there and
// nowhere else.
var APIs = []API{
	{Key: kmsg.Produce, Versions: [4]Offered{v(3, 5), v(3, 7), v(3, 9), v(3, 9)}},
	{Key: kmsg.Fetch, Versions: [4]Offered{v(4, 6), v(4, 10), v(4, 13), v(4, 13)}},
	{Key: kmsg.ListOffsets, Versions: [4]Offered{v(0, 2), v(0, 5), v(0, 7), v(0, 7)}},
	{Key: kmsg.Metadata, Versions: [4]Offered{v(0, 6), v(0, 8), v(0, 12), v(0, 12)}},
	{Key: kmsg.OffsetCommit, Versions: [4]Offered{v(0, 3), v(0, 6), v(0, 8), v(0, 9)}},
	{Key: kmsg.OffsetFetch, Versions: [4]Offered{v(0, 3), v(0, 5), v(0, 8), v(0, 9)}},
	{Key: kmsg.FindCoordinator, Versions: [4]Offered{v(0, 2), v(0, 2), v(0, 4), v(0, 4)}},
	{Key: kmsg.JoinGroup, Versions: [4]Offered{v(0, 2), v(0, 4), v(0, 7), v(0, 9)}},
	{Key: kmsg.Heartbeat, Versions: [4]Offered{v(0, 1), v(0, 2), v(0, 4), v(0, 4)}},
	{Key: kmsg.LeaveGroup, Versions: [4]Offered{v(0, 1), v(0, 2), v(0, 4), v(0, 5)}},
	{Key: kmsg.SyncGroup, Versions: [4]Offered{v(0, 1), v(0, 3), v(0, 4), v(0, 5)}},
	{Key: kmsg.DescribeGroups, Versions: [4]Offered{v(0, 1), v(0, 3), v(0, 5), v(0, 5)}},
	{Key: kmsg.ListGroups, Versions: [4]Offered{v(0, 2), v(0, 2), v(0, 4), v(0, 5)}},
	{Key: kmsg.SASLHandshake, Versions: [4]Offered{v(1, 1), v(1, 1), v(1, 1), v(1, 1)}, When: WhenSASL},
	{Key: kmsg.ApiVersions, Versions: [4]Offered{v(0, 1), v(0, 2), v(0, 3), v(0, 3)}},
	{Key: kmsg.CreateTopics, Versions: [4]Offered{v(0, 2), v(0, 3), v(0, 7), v(0, 7)}},
	{Key: kmsg.DeleteTopics, Versions: [4]Offered{v(0, 1), v(0, 3), v(0, 5), v(0, 6)}},
	{Key: kmsg.DeleteRecords, Versions: [4]Offered{v(0, 0), v(0, 1), v(0, 2), v(0, 2)}},
	{Key: kmsg.InitProducerID, Versions: [4]Offered{v(0, 1), v(0, 1), v(0, 4), v(0, 4)}},
	{Key: kmsg.AddPartitionsToTxn, Versions: [4]Offered{v(0, 0), v(0, 1), v(0, 3), v(0, 3)}},
	{Key: kmsg.AddOffsetsToTxn, Versions: [4]Offered{v(0, 0), v(0, 1), v(0, 3), v(0, 3)}},
	{Key: kmsg.EndTxn, Versions: [4]Offered{v(0, 0), v(0, 1), v(0, 3), v(0, 3)}},
	{Key: kmsg.TxnOffsetCommit, Versions: [4]Offered{v(0, 0), v(0, 2), v(0, 3), v(0, 3)}},
	{Key: kmsg.DescribeACLs, Versions: [4]Offered{v(0, 0), v(0, 1), v(0, 3), v(0, 3)}},
	{Key: kmsg.CreateACLs, Versions: [4]Offered{v(0, 0), v(0, 1), v(0, 3), v(0, 3)}},
	{Key: kmsg.DeleteACLs, Versions: [4]Offered{v(0, 0), v(0, 1), v(0, 3), v(0, 3)}},
	{Key: kmsg.DescribeConfigs, Versions: [4]Offered{v(0, 1), v(0, 2), v(0, 4), v(0, 4)}},
	{Key: kmsg.AlterConfigs, Versions: [4]Offered{v(0, 0), v(0, 2), v(0, 2), v(0, 2)}},
	{Key: kmsg.SASLAuthenticate, Versions: [4]Offered{v(0, 2), v(0, 2), v(0, 2), v(0, 2)}, When: WhenSASL},
	{Key: kmsg.CreatePartitions, Versions: [4]Offered{v(0, 0), v(0, 1), v(0, 3), v(0, 3)}},
	{Key: kmsg.DeleteGroups, Versions: [4]Offered{v(0, 1), v(0, 2), v(0, 2), v(0, 2)}},
	{Key: kmsg.IncrementalAlterConfigs, Versions: [4]Offered{none, v(0, 1), v(0, 1), v(0, 1)}},
	{Key: kmsg.OffsetDelete, Versions: [4]Offered{none, v(0, 0), v(0, 0), v(0, 0)}},
	{Key: kmsg.DescribeCluster, Versions: [4]Offered{none, none, v(0, 1), v(0, 2)}},
	{Key: kmsg.DescribeProducers, Versions: [4]Offered{none, none, v(0, 0), v(0, 0)}},
	{Key: kmsg.DescribeTransactions, Versions: [4]Offered{none, none, v(0, 0), v(0, 0)}},
	{Key: kmsg.ListTransactions, Versions: [4]Offered{none, none, v(0, 0), v(0, 0)}},
	{Key: kmsg.ConsumerGroupHeartbeat, Versions: [4]Offered{none, none, none, v(0, 1)}},
	{Key: kmsg.ConsumerGroupDescribe, Versions: [4]Offered{none, none, none, v(0, 1)}},
}

// Profile is the behavioral envelope for a declared Kafka major version: the
// per-API version ranges advertised in ApiVersions, plus the feature toggles
// that change observable behavior (currently the consumer-group protocol).
//
// Selecting a profile is how `--kafka-version N` (or `kafka-version` in the config
// file) maps to what clients see. The version does not fetch an upstream binary:
// our own code is the engine, so the version is a profile we implement.
// See docs/api-support.md.
type Profile struct {
	KafkaMajor    int           // the Kafka major this profile emulates (1..4).
	GroupProtocol GroupProtocol // which group protocol(s) to advertise.
}

// DefaultKafkaMajor is the version emulated when none is declared.
const DefaultKafkaMajor = 4

// firstMajor and lastMajor are the Kafka majors the table has a column for. The
// floor is record batch format v2 (Kafka 0.11), so major 1 is the minimum.
const firstMajor, lastMajor = 1, 4

// ProfileFor returns the profile for a declared Kafka major, or ok=false when the
// major is outside the supported range.
func ProfileFor(kafkaMajor int) (Profile, bool) {
	if kafkaMajor < firstMajor || kafkaMajor > lastMajor {
		return Profile{}, false
	}
	p := Profile{KafkaMajor: kafkaMajor, GroupProtocol: GroupClassic}
	if _, ok := p.RangeFor(kmsg.ConsumerGroupHeartbeat); ok {
		p.GroupProtocol = GroupClassicAndConsumer
	}
	return p, true
}

// RangeFor returns the version range of an API in this profile, or ok=false if
// the profile does not offer it at all. APIs that depend on a Condition are
// included: whether the condition holds is the caller's knowledge (see Advertised).
func (p Profile) RangeFor(key APIKey) (VersionRange, bool) {
	for _, a := range APIs {
		if a.Key == key {
			o := a.Versions[p.KafkaMajor-firstMajor]
			return o.VersionRange, o.OK
		}
	}
	return VersionRange{}, false
}

// SupportedKafkaMajors returns the accepted declared versions in ascending order.
func SupportedKafkaMajors() []int {
	var majors []int
	for m := firstMajor; m <= lastMajor; m++ {
		majors = append(majors, m)
	}
	return majors
}

// AdvertisedAPI is one line of an ApiVersions response: an API key and the
// version range we support for it.
type AdvertisedAPI struct {
	Key APIKey
	Min int16
	Max int16
}

// Advertised returns what this profile offers, sorted by API key: the set that
// ApiVersions reports and the router enforces. sasl says whether authentication
// is configured, which is what the WhenSASL rows wait on.
func (p Profile) Advertised(sasl bool) []AdvertisedAPI {
	var out []AdvertisedAPI
	for _, a := range APIs {
		o := a.Versions[p.KafkaMajor-firstMajor]
		if !o.OK || (a.When == WhenSASL && !sasl) {
			continue
		}
		out = append(out, AdvertisedAPI{Key: a.Key, Min: o.Min, Max: o.Max})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
