package conformance

import (
	"encoding/binary"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Scenarios are the scripts. Each starts a fresh broker, so one cannot leak
// into the next, and each is deterministic: the same script against the same
// behaviour gives the same normalised steps.
//
// The versions are the highest that both Apache Kafka and doze-kafka offer
// for the API, so one script runs against both (internal/protocol/versions.go).
var Scenarios = []Scenario{
	{Name: "createtopics-validation", Run: createTopicsValidation, Known: knownCreateTopics},
	{Name: "metadata-autocreate", Run: metadataAutoCreate, Known: knownMetadata},
	{Name: "listoffsets", Run: listOffsets, Known: knownListOffsets},
	{Name: "offsetcommit-generations", Run: offsetCommitGenerations, Known: knownOffsetCommit},
	{Name: "findcoordinator", Run: findCoordinator, Known: knownFindCoordinator},
}

// Differences each scenario found and did not fix. Every entry is explained by a
// sentence in docs/limitations.md (a test checks the sentence is there).
var (
	knownCreateTopics    = createTopicsKnown()
	knownMetadata        = []Known{{Step: "set-num-partitions", Path: "Resources[0].ErrorMessage", Limitation: limMessages}}
	knownListOffsets     = []Known{{Step: "repeated-partition", Path: "Topics[0].Partitions", Limitation: limDuplicatePartition}}
	knownFindCoordinator []Known
	knownOffsetCommit    = []Known{
		// Kafka reloads its groups; a restart here forgets the members (limMembership).
		{Step: "commit-old-generation-after-restart", Path: "Topics[0].Partitions[0].ErrorCode", Limitation: limMembership},
		{Step: "fetch-after-commit-after-restart", Path: "Groups[0].Topics[0].Partitions[0]", Limitation: limMembership},
		{Step: "join-after-restart-with-id", Path: "Generation", Limitation: limMembership},
	}
)

// The sentences of docs/limitations.md the known differences stand on.
const (
	limReplication        = "A replication factor above 1 is accepted."
	limCase               = "Topic names that differ only in letter case are refused."
	limMessages           = "Validation messages are doze-kafka's own words."
	limDuplicatePartition = "A partition named twice in one ListOffsets request is answered twice."
	limMembership         = "Group membership is not kept across a restart."
)

func createTopicsKnown() []Known {
	k := []Known{
		{Step: "case-differs-second", Path: "Topics[0]", Limitation: limCase},
		{Step: "what-exists", Path: "Topics", Limitation: limCase}, // the topic it would have listed.
		{Step: "replication-two", Path: "Topics[0]", Limitation: limReplication},
	}
	// The same refusal, the same code, in this broker's words.
	for _, step := range []string{
		"empty-name", "dot", "dot-dot", "name-250", "illegal-space", "illegal-slash", "illegal-unicode",
		"zero-partitions", "negative-partitions", "validate-only-invalid",
		"config-unknown-name", "config-bad-number", "config-below-minimum", "config-bad-enum",
		"config-bad-compression", "config-min-isr-zero", "config-max-message-negative", "config-timestamp-type",
	} {
		k = append(k, Known{Step: step, Path: "Topics[0].ErrorMessage", Limitation: limMessages})
	}
	k = append(k,
		Known{Step: "mixed-batch", Path: "Topics[1].ErrorMessage", Limitation: limMessages},
		Known{Step: "mixed-batch", Path: "Topics[2].ErrorMessage", Limitation: limMessages},
	)
	return k
}

// --- request builders ---

func str(s string) *string { return &s }

func newTopic(name string, partitions int32, rf int16, configs ...string) kmsg.CreateTopicsRequestTopic {
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic, t.NumPartitions, t.ReplicationFactor = name, partitions, rf
	for i := 0; i+1 < len(configs); i += 2 {
		c := kmsg.NewCreateTopicsRequestTopicConfig()
		c.Name, c.Value = configs[i], str(configs[i+1])
		t.Configs = append(t.Configs, c)
	}
	return t
}

func createTopics(validateOnly bool, topics ...kmsg.CreateTopicsRequestTopic) *kmsg.CreateTopicsRequest {
	r := kmsg.NewPtrCreateTopicsRequest()
	r.Topics = topics
	r.TimeoutMillis = 15000
	r.ValidateOnly = validateOnly
	return r
}

func metadata(allowAuto bool, topics ...string) *kmsg.MetadataRequest {
	r := kmsg.NewPtrMetadataRequest()
	r.AllowAutoTopicCreation = allowAuto
	r.Topics = []kmsg.MetadataRequestTopic{}
	for _, n := range topics {
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = str(n)
		r.Topics = append(r.Topics, t)
	}
	return r
}

func metadataAll() *kmsg.MetadataRequest {
	r := kmsg.NewPtrMetadataRequest()
	r.Topics = nil // null: every topic.
	return r
}

// consumerSubscription is the classic consumer protocol's subscription:
// version 0, the topics, no user data.
func consumerSubscription(topics ...string) []byte {
	b := binary.BigEndian.AppendUint16(nil, 0)
	b = binary.BigEndian.AppendUint32(b, uint32(len(topics)))
	for _, t := range topics {
		b = binary.BigEndian.AppendUint16(b, uint16(len(t)))
		b = append(b, t...)
	}
	return binary.BigEndian.AppendUint32(b, 0xffffffff)
}

// consumerAssignment is the classic consumer protocol's assignment.
func consumerAssignment(topic string, partitions ...int32) []byte {
	b := binary.BigEndian.AppendUint16(nil, 0)
	b = binary.BigEndian.AppendUint32(b, 1)
	b = binary.BigEndian.AppendUint16(b, uint16(len(topic)))
	b = append(b, topic...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(partitions)))
	for _, p := range partitions {
		b = binary.BigEndian.AppendUint32(b, uint32(p))
	}
	return binary.BigEndian.AppendUint32(b, 0xffffffff)
}

// --- 1. CreateTopics validation ---

func createTopicsValidation(e *Env) {
	const v = 7
	e.Do("valid", createTopics(false, newTopic("ct-valid", 3, 1)), v)
	e.Do("already-exists", createTopics(false, newTopic("ct-valid", 3, 1)), v)
	e.Do("empty-name", createTopics(false, newTopic("", 1, 1)), v)
	e.Do("dot", createTopics(false, newTopic(".", 1, 1)), v)
	e.Do("dot-dot", createTopics(false, newTopic("..", 1, 1)), v)
	e.Do("name-249", createTopics(false, newTopic(strings.Repeat("a", 249), 1, 1)), v)
	e.Do("name-250", createTopics(false, newTopic(strings.Repeat("b", 250), 1, 1)), v)
	e.Do("illegal-space", createTopics(false, newTopic("bad name", 1, 1)), v)
	e.Do("illegal-slash", createTopics(false, newTopic("bad/name", 1, 1)), v)
	e.Do("illegal-unicode", createTopics(false, newTopic("café", 1, 1)), v)
	e.Do("legal-punctuation", createTopics(false, newTopic("ok.name-with_all.3", 1, 1)), v)
	e.Do("collision-first", createTopics(false, newTopic("col.lide", 1, 1)), v)
	e.Do("collision-second", createTopics(false, newTopic("col_lide", 1, 1)), v)
	e.Do("case-differs-first", createTopics(false, newTopic("Case.Topic", 1, 1)), v)
	e.Do("case-differs-second", createTopics(false, newTopic("case.topic", 1, 1)), v)
	e.Do("zero-partitions", createTopics(false, newTopic("ct-zero", 0, 1)), v)
	e.Do("negative-partitions", createTopics(false, newTopic("ct-neg", -2, 1)), v)
	e.Do("replication-zero", createTopics(false, newTopic("ct-rf0", 1, 0)), v)
	e.Do("replication-two", createTopics(false, newTopic("ct-rf2", 1, 2)), v)
	e.Do("replication-negative", createTopics(false, newTopic("ct-rfneg", 1, -2)), v)
	e.Do("broker-defaults", createTopics(false, newTopic("ct-defaults", -1, -1)), v)

	both := newTopic("ct-both", 2, 1)
	a := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	a.Partition, a.Replicas = 0, []int32{1}
	both.ReplicaAssignment = append(both.ReplicaAssignment, a)
	e.Do("assignment-and-partitions", createTopics(false, both), v)

	only := newTopic("ct-assigned", -1, -1)
	for p := int32(0); p < 2; p++ {
		a := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
		a.Partition, a.Replicas = p, []int32{1}
		only.ReplicaAssignment = append(only.ReplicaAssignment, a)
	}
	e.Do("assignment-only", createTopics(false, only), v)

	gap := newTopic("ct-gap", -1, -1)
	g := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	g.Partition, g.Replicas = 1, []int32{1}
	gap.ReplicaAssignment = append(gap.ReplicaAssignment, g)
	e.Do("assignment-gap", createTopics(false, gap), v)

	unknownNode := newTopic("ct-badnode", -1, -1)
	u := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	u.Partition, u.Replicas = 0, []int32{42}
	unknownNode.ReplicaAssignment = append(unknownNode.ReplicaAssignment, u)
	e.Do("assignment-unknown-broker", createTopics(false, unknownNode), v)

	e.Do("duplicate-in-request", createTopics(false, newTopic("ct-dup", 1, 1), newTopic("ct-dup", 1, 1)), v)
	e.Do("mixed-batch", createTopics(false, newTopic("ct-mix-ok", 1, 1), newTopic("", 1, 1), newTopic("ct-mix-ok2", 0, 1)), v)

	e.Do("config-unknown-name", createTopics(false, newTopic("ct-cfg1", 1, 1, "no.such.config", "1")), v)
	e.Do("config-bad-number", createTopics(false, newTopic("ct-cfg2", 1, 1, "retention.ms", "abc")), v)
	e.Do("config-below-minimum", createTopics(false, newTopic("ct-cfg3", 1, 1, "segment.bytes", "1")), v)
	e.Do("config-bad-enum", createTopics(false, newTopic("ct-cfg4", 1, 1, "cleanup.policy", "bogus")), v)
	e.Do("config-bad-compression", createTopics(false, newTopic("ct-cfg5", 1, 1, "compression.type", "zip")), v)
	e.Do("config-min-isr-zero", createTopics(false, newTopic("ct-cfg6", 1, 1, "min.insync.replicas", "0")), v)
	e.Do("config-max-message-negative", createTopics(false, newTopic("ct-cfg7", 1, 1, "max.message.bytes", "-1")), v)
	e.Do("config-timestamp-type", createTopics(false, newTopic("ct-cfg8", 1, 1, "message.timestamp.type", "WallClock")), v)
	e.Do("config-valid-set", createTopics(false, newTopic("ct-cfg9", 1, 1, "retention.ms", "3600000", "cleanup.policy", "compact,delete")), v)
	e.Do("config-delete-compact-only-with-delete", createTopics(false, newTopic("ct-cfg10", 1, 1, "cleanup.policy", "delete,delete")), v)

	e.Do("validate-only-ok", createTopics(true, newTopic("ct-validate", 2, 1)), v)
	e.Do("validate-only-invalid", createTopics(true, newTopic("ct-validate2", 0, 1)), v)
	e.Do("validate-only-exists", createTopics(true, newTopic("ct-valid", 1, 1)), v)

	e.Do("what-exists", metadataAll(), 12)
}

// --- 2. Metadata with auto-create ---

func metadataAutoCreate(e *Env) {
	const v = 12
	e.Do("unknown-without-auto-create", metadata(false, "md-a"), v)
	// Kafka creates the topic after it has answered, so the request that asks for
	// it is told it is unknown, and the topic is there a moment later.
	e.Do("unknown-with-auto-create", metadata(true, "md-b"), v)
	e.DoUntil("auto-created-is-there", metadata(false, "md-b"), v, noTopicUnknown)
	e.Do("auto-create-two", metadata(true, "md-c", "md-d"), v)
	e.DoUntil("auto-created-two-are-there", metadata(false, "md-c", "md-d"), v, noTopicUnknown)
	e.Do("auto-create-existing-is-just-there", metadata(true, "md-b"), v)
	e.Do("auto-create-invalid-name", metadata(true, "bad name"), v)
	e.Do("auto-create-empty-name", metadata(true, ""), v)
	e.Do("auto-create-too-long", metadata(true, strings.Repeat("x", 250)), v)
	e.Do("auto-create-dot", metadata(true, "."), v)
	e.Do("known-and-unknown", metadata(false, "md-b", "md-nope"), v)
	e.Do("no-topics", metadata(true), v)
	e.Do("every-topic", metadataAll(), v)

	byID := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.TopicID = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	byID.Topics = append(byID.Topics, t)
	e.Do("unknown-topic-id", byID, v)

	// A step the oracle needs (foundation E17): change a broker config, then see
	// its effect on the next auto-create.
	e.SetBrokerConfig("set-num-partitions", "num.partitions", "3")
	e.Do("auto-create-after-config", metadata(true, "md-e"), v)
	e.DoUntil("auto-created-after-config-is-there", metadata(false, "md-e"), v, noTopicUnknown)
	e.Do("create-topics-after-config", createTopics(false, newTopic("md-f", -1, -1)), 7)
}

// noTopicUnknown is true once no topic in a Metadata response is
// UNKNOWN_TOPIC_OR_PARTITION: whatever was being created has been.
func noTopicUnknown(r kmsg.Response) bool {
	for _, t := range r.(*kmsg.MetadataResponse).Topics {
		if t.ErrorCode == 3 {
			return false
		}
	}
	return true
}

// --- 3. ListOffsets ---

func listOffsets(e *Env) {
	const v = 7
	e.Do("create", createTopics(false, newTopic("lo", 2, 1), newTopic("lo-empty", 1, 1)), 7)

	base := int64(1_600_000_000_000)
	e.Produce("produce-five", "lo", 0,
		ProduceRecord{TimestampMs: base + 1000, Value: "a"},
		ProduceRecord{TimestampMs: base + 2000, Value: "b"},
		ProduceRecord{TimestampMs: base + 4000, Value: "c"},
		ProduceRecord{TimestampMs: base + 3000, Value: "d"}, // out of order on purpose.
		ProduceRecord{TimestampMs: base + 5000, Value: "e"},
	)
	at := func(label, topic string, partition int32, ts int64, isolation int8) {
		r := kmsg.NewPtrListOffsetsRequest()
		r.ReplicaID = -1
		r.IsolationLevel = isolation
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition, rp.Timestamp, rp.CurrentLeaderEpoch = partition, ts, -1
		rt.Partitions = append(rt.Partitions, rp)
		r.Topics = append(r.Topics, rt)
		e.Do(label, r, v)
	}
	at("earliest", "lo", 0, -2, 0)
	at("latest", "lo", 0, -1, 0)
	at("max-timestamp", "lo", 0, -3, 0)
	at("earliest-local", "lo", 0, -4, 0)
	at("latest-tiered", "lo", 0, -5, 0)
	at("before-everything", "lo", 0, 1, 0)
	at("exact-first", "lo", 0, base+1000, 0)
	at("between", "lo", 0, base+2500, 0)
	at("exact-out-of-order", "lo", 0, base+3000, 0)
	at("after-the-out-of-order", "lo", 0, base+3500, 0)
	at("exact-last", "lo", 0, base+5000, 0)
	at("after-everything", "lo", 0, base+9000, 0)
	at("unsupported-negative", "lo", 0, -6, 0)
	at("zero-timestamp", "lo", 0, 0, 0)
	at("read-committed-latest", "lo", 0, -1, 1)
	at("empty-partition-earliest", "lo", 1, -2, 0)
	at("empty-partition-latest", "lo", 1, -1, 0)
	at("empty-partition-max-timestamp", "lo", 1, -3, 0)
	at("empty-partition-by-time", "lo", 1, base, 0)
	at("empty-topic-max-timestamp", "lo-empty", 0, -3, 0)
	at("unknown-partition", "lo", 9, -1, 0)
	at("negative-partition", "lo", -1, -1, 0)
	at("unknown-topic", "lo-nope", 0, -1, 0)

	// A stale or future leader epoch, and an unknown isolation level.
	epoch := kmsg.NewPtrListOffsetsRequest()
	epoch.ReplicaID = -1
	et := kmsg.NewListOffsetsRequestTopic()
	et.Topic = "lo"
	ep := kmsg.NewListOffsetsRequestTopicPartition()
	ep.Partition, ep.Timestamp, ep.CurrentLeaderEpoch = 0, -1, 100
	et.Partitions = append(et.Partitions, ep)
	epoch.Topics = append(epoch.Topics, et)
	e.Do("future-leader-epoch", epoch, v)

	bad := kmsg.NewPtrListOffsetsRequest()
	bad.ReplicaID = -1
	bad.IsolationLevel = 7
	bt := kmsg.NewListOffsetsRequestTopic()
	bt.Topic = "lo"
	bp := kmsg.NewListOffsetsRequestTopicPartition()
	bp.Partition, bp.Timestamp, bp.CurrentLeaderEpoch = 0, -1, -1
	bt.Partitions = append(bt.Partitions, bp)
	bad.Topics = append(bad.Topics, bt)
	e.Do("unknown-isolation-level", bad, v)

	// Both partitions and a repeated partition in one request.
	multi := kmsg.NewPtrListOffsetsRequest()
	multi.ReplicaID = -1
	mt := kmsg.NewListOffsetsRequestTopic()
	mt.Topic = "lo"
	for _, p := range []int32{0, 1, 0} {
		mp := kmsg.NewListOffsetsRequestTopicPartition()
		mp.Partition, mp.Timestamp, mp.CurrentLeaderEpoch = p, -1, -1
		mt.Partitions = append(mt.Partitions, mp)
	}
	multi.Topics = append(multi.Topics, mt)
	e.Do("repeated-partition", multi, v)

	// A consumer's replica id (not -1) is refused by a real broker.
	rep := kmsg.NewPtrListOffsetsRequest()
	rep.ReplicaID = 5
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = "lo"
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition, rp.Timestamp, rp.CurrentLeaderEpoch = 0, -1, -1
	rt.Partitions = append(rt.Partitions, rp)
	rep.Topics = append(rep.Topics, rt)
	e.Do("replica-id-set", rep, v)
}

// --- 4. OffsetCommit and OffsetFetch across generations ---

func offsetCommitGenerations(e *Env) {
	e.Do("create", createTopics(false, newTopic("oc", 2, 1)), 7)

	join := func(label, group, member string) *kmsg.JoinGroupResponse {
		r := kmsg.NewPtrJoinGroupRequest()
		r.Group, r.MemberID = group, member
		r.SessionTimeoutMillis, r.RebalanceTimeoutMillis = 30000, 30000
		r.ProtocolType = "consumer"
		p := kmsg.NewJoinGroupRequestProtocol()
		p.Name, p.Metadata = "range", consumerSubscription("oc")
		r.Protocols = append(r.Protocols, p)
		return e.Do(label, r, 7).(*kmsg.JoinGroupResponse)
	}
	sync := func(label, group, member string, gen int32, assign bool) {
		r := kmsg.NewPtrSyncGroupRequest()
		r.Group, r.MemberID, r.Generation = group, member, gen
		r.ProtocolType, r.Protocol = str("consumer"), str("range")
		if assign {
			a := kmsg.NewSyncGroupRequestGroupAssignment()
			a.MemberID, a.MemberAssignment = member, consumerAssignment("oc", 0, 1)
			r.GroupAssignment = append(r.GroupAssignment, a)
		}
		e.Do(label, r, 5)
	}
	commit := func(label, group, member string, gen int32, topic string, partition int32, offset int64, meta string) {
		r := kmsg.NewPtrOffsetCommitRequest()
		r.Group, r.MemberID, r.Generation = group, member, gen
		t := kmsg.NewOffsetCommitRequestTopic()
		t.Topic = topic
		p := kmsg.NewOffsetCommitRequestTopicPartition()
		p.Partition, p.Offset, p.LeaderEpoch = partition, offset, -1
		if meta != "" {
			p.Metadata = str(meta)
		}
		t.Partitions = append(t.Partitions, p)
		r.Topics = append(r.Topics, t)
		e.Do(label, r, 8)
	}
	fetch := func(label, group string, topics map[string][]int32, all bool) {
		r := kmsg.NewPtrOffsetFetchRequest()
		g := kmsg.NewOffsetFetchRequestGroup()
		g.Group = group
		if all {
			g.Topics = nil
		} else {
			g.Topics = []kmsg.OffsetFetchRequestGroupTopic{}
			for _, name := range sortedKeys(topics) {
				t := kmsg.NewOffsetFetchRequestGroupTopic()
				t.Topic, t.Partitions = name, topics[name]
				g.Topics = append(g.Topics, t)
			}
		}
		r.Groups = append(r.Groups, g)
		e.Do(label, r, 8)
	}

	// A client finds its coordinator first, which is also what makes a real
	// broker create its offsets topic; nothing else does.
	for _, g := range []string{"g1", "g-simple", "g-never"} {
		fc := kmsg.NewPtrFindCoordinatorRequest()
		fc.CoordinatorKeys = []string{g}
		e.Do("find-coordinator-"+g, fc, 4)
	}

	first := join("join-needs-member-id", "g1", "")
	member := first.MemberID
	second := join("join-gen-1", "g1", member)
	sync("sync-gen-1", "g1", member, second.Generation, true)

	fetch("fetch-nothing-committed", "g1", map[string][]int32{"oc": {0, 1}}, false)
	commit("commit-gen-1", "g1", member, second.Generation, "oc", 0, 3, "first")
	commit("commit-gen-1-p1", "g1", member, second.Generation, "oc", 1, 7, "")
	fetch("fetch-after-commit", "g1", map[string][]int32{"oc": {0, 1}}, false)

	third := join("rejoin-gen-2", "g1", member)
	sync("sync-gen-2", "g1", member, third.Generation, true)
	commit("commit-stale-generation", "g1", member, second.Generation, "oc", 0, 4, "stale")
	commit("commit-future-generation", "g1", member, third.Generation+5, "oc", 0, 4, "future")
	fetch("fetch-after-refused-commits", "g1", map[string][]int32{"oc": {0}}, false)
	commit("commit-gen-2", "g1", member, third.Generation, "oc", 0, 4, "second")
	commit("commit-unknown-member", "g1", "no-such-member", third.Generation, "oc", 0, 5, "")
	commit("commit-no-member-with-generation", "g1", "", third.Generation, "oc", 0, 5, "")
	commit("commit-standalone-into-live-group", "g1", "", -1, "oc", 0, 6, "")
	fetch("fetch-after-gen-2", "g1", map[string][]int32{"oc": {0, 1}}, false)

	commit("commit-unknown-topic", "g1", member, third.Generation, "oc-nope", 0, 1, "")
	commit("commit-unknown-partition", "g1", member, third.Generation, "oc", 9, 1, "")
	commit("commit-negative-offset", "g1", member, third.Generation, "oc", 1, -1, "")
	commit("commit-offset-past-end", "g1", member, third.Generation, "oc", 1, 1_000_000, "")
	commit("commit-metadata-4096", "g1", member, third.Generation, "oc", 1, 8, strings.Repeat("m", 4096))
	commit("commit-metadata-4097", "g1", member, third.Generation, "oc", 1, 9, strings.Repeat("m", 4097))

	commit("standalone-commit", "g-simple", "", -1, "oc", 0, 11, "solo")
	commit("standalone-commit-with-member", "g-simple", "ghost", -1, "oc", 1, 12, "")
	fetch("fetch-standalone", "g-simple", map[string][]int32{"oc": {0, 1}}, false)

	fetch("fetch-all-topics", "g1", nil, true)
	fetch("fetch-unknown-group", "g-never", map[string][]int32{"oc": {0}}, false)
	fetch("fetch-unknown-group-all", "g-never", nil, true)
	fetch("fetch-unknown-partition", "g1", map[string][]int32{"oc": {0, 9}}, false)
	fetch("fetch-unknown-topic", "g1", map[string][]int32{"oc-nope": {0}}, false)

	// Across a restart: the committed offsets are what a restart must keep.
	e.Restart("restart")
	fetch("fetch-after-restart", "g1", map[string][]int32{"oc": {0, 1}}, false)
	fetch("fetch-standalone-after-restart", "g-simple", map[string][]int32{"oc": {0, 1}}, false)
	commit("commit-old-generation-after-restart", "g1", member, third.Generation, "oc", 0, 20, "after")
	fetch("fetch-after-commit-after-restart", "g1", map[string][]int32{"oc": {0}}, false)

	// A fresh join after the restart, and the generation it gets.
	again := join("join-after-restart", "g1", "")
	if again.MemberID != "" {
		join("join-after-restart-with-id", "g1", again.MemberID)
	}
	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group = "g1"
	m := kmsg.NewLeaveGroupRequestMember()
	m.MemberID = again.MemberID
	leave.Members = append(leave.Members, m)
	e.Do("leave", leave, 4)
	commit("commit-after-leave", "g1", again.MemberID, 1, "oc", 0, 30, "")
}

// --- 5. FindCoordinator ---

func findCoordinator(e *Env) {
	find := func(label string, version int16, keyType int8, keys ...string) {
		r := kmsg.NewPtrFindCoordinatorRequest()
		r.CoordinatorType = keyType
		if version >= 4 {
			r.CoordinatorKeys = keys
		} else if len(keys) > 0 {
			r.CoordinatorKey = keys[0]
		}
		e.Do(label, r, version)
	}
	find("group", 4, 0, "fc-group")
	find("transaction", 4, 1, "fc-txn")
	find("share-group", 4, 2, "fc-share")
	find("group-and-another", 4, 0, "fc-a", "fc-b")
	find("empty-group-key", 4, 0, "")
	find("empty-transaction-key", 4, 1, "")
	find("no-keys", 4, 0)
	find("unknown-key-type", 4, 9, "fc-x")
	find("negative-key-type", 4, -1, "fc-x")
	find("repeated-key", 4, 0, "fc-r", "fc-r")
	find("long-key", 4, 0, strings.Repeat("g", 300))
	find("v3-group", 3, 0, "fc-group")
	find("v3-transaction", 3, 1, "fc-txn")
	find("v2-group", 2, 0, "fc-group")
	find("v2-transaction", 2, 1, "fc-txn")
	find("v1-empty-key", 1, 0, "")
	find("v0-group", 0, 0, "fc-group")
	find("v1-unknown-key-type", 1, 9, "fc-x")
}
