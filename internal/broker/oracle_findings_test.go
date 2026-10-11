package broker_test

import (
	"context"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Each test here pins one difference the real-broker oracle found
// (conformance/recordings, recorded against apache/kafka:4.3.1) and that was
// fixed. The replay in conformance/ is the broad net; these name the behaviour.

func oracleCreate(t *testing.T, c *rawConn, validateOnly bool, topics ...kmsg.CreateTopicsRequestTopic) []kmsg.CreateTopicsResponseTopic {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.SetVersion(7)
	req.TimeoutMillis = 5000
	req.ValidateOnly = validateOnly
	req.Topics = topics
	var resp kmsg.CreateTopicsResponse
	if !c.do(req, &resp) {
		t.Fatal("connection closed")
	}
	return resp.Topics
}

func oracleTopic(name string, partitions int32, rf int16) kmsg.CreateTopicsRequestTopic {
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, partitions, rf
	return rt
}

func TestOracle_CreateTopicsRefusesWhatKafkaRefuses(t *testing.T) {
	c := openRaw(t, startBroker(t, 4))

	// "a.b" and "a_b" share metric names: the second is refused.
	if got := oracleCreate(t, c, false, oracleTopic("col.lide", 1, 1)); got[0].ErrorCode != 0 {
		t.Fatalf("first: %+v", got)
	}
	if got := oracleCreate(t, c, false, oracleTopic("col_lide", 1, 1)); got[0].ErrorCode != 17 || !strings.Contains(deref(got[0].ErrorMessage), "collides with existing topic: col.lide") {
		t.Errorf("a name that collides: %d %q, want INVALID_TOPIC_EXCEPTION naming the topic", got[0].ErrorCode, deref(got[0].ErrorMessage))
	}

	// A replication factor of zero or below -1 is no factor at all.
	for _, rf := range []int16{0, -2} {
		if got := oracleCreate(t, c, false, oracleTopic("rf-bad", 1, rf)); got[0].ErrorCode != 38 {
			t.Errorf("replication factor %d: code %d, want INVALID_REPLICATION_FACTOR (38)", rf, got[0].ErrorCode)
		}
	}

	// A topic that exists is TOPIC_ALREADY_EXISTS under validate_only too.
	if got := oracleCreate(t, c, true, oracleTopic("col.lide", 1, 1)); got[0].ErrorCode != 36 {
		t.Errorf("validate_only of an existing topic: code %d, want TOPIC_ALREADY_EXISTS (36)", got[0].ErrorCode)
	}
	// ...and one that does not is answered with a made-up id, and created nowhere.
	got := oracleCreate(t, c, true, oracleTopic("only-validated", 2, 1))
	if got[0].ErrorCode != 0 || got[0].TopicID == ([16]byte{}) || got[0].NumPartitions != 2 {
		t.Errorf("validate_only of a new topic: %+v", got[0])
	}
	if again := oracleCreate(t, c, false, oracleTopic("only-validated", 1, 1)); again[0].ErrorCode != 0 || again[0].NumPartitions != 1 {
		t.Errorf("a validated topic should not exist yet: %+v", again[0])
	}

	// Errors carry an empty config list, never null.
	if got := oracleCreate(t, c, false, oracleTopic("rf-bad", 1, 0)); got[0].Configs == nil {
		t.Error("an error answer carries null configs; Kafka sends an empty list")
	}
}

func TestOracle_CreateTopicsManualAssignment(t *testing.T) {
	c := openRaw(t, startBroker(t, 4))
	assign := func(name string, partitions int32, rf int16, parts ...int32) kmsg.CreateTopicsRequestTopic {
		rt := oracleTopic(name, partitions, rf)
		for _, p := range parts {
			a := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
			a.Partition, a.Replicas = p, []int32{1}
			rt.ReplicaAssignment = append(rt.ReplicaAssignment, a)
		}
		return rt
	}

	// The assignment says how many partitions there are.
	got := oracleCreate(t, c, false, assign("assigned", -1, -1, 0, 1, 2))
	if got[0].ErrorCode != 0 || got[0].NumPartitions != 3 {
		t.Fatalf("an assignment of three partitions: %+v", got[0])
	}
	// Alongside a partition count or a replication factor it is a malformed request.
	if got := oracleCreate(t, c, false, assign("both-a", 2, -1, 0, 1)); got[0].ErrorCode != 42 {
		t.Errorf("assignment with a partition count: code %d, want INVALID_REQUEST (42)", got[0].ErrorCode)
	}
	if got := oracleCreate(t, c, false, assign("both-b", -1, 1, 0)); got[0].ErrorCode != 42 {
		t.Errorf("assignment with a replication factor: code %d, want INVALID_REQUEST (42)", got[0].ErrorCode)
	}
	// Partitions must be 0..n-1, replicas must be this broker, once.
	if got := oracleCreate(t, c, false, assign("gap", -1, -1, 1)); got[0].ErrorCode != 39 {
		t.Errorf("an assignment that skips partition 0: code %d, want INVALID_REPLICA_ASSIGNMENT (39)", got[0].ErrorCode)
	}
	if got := oracleCreate(t, c, false, assign("twice", -1, -1, 0, 0)); got[0].ErrorCode != 39 {
		t.Errorf("a partition assigned twice: code %d, want 39", got[0].ErrorCode)
	}
	other := assign("other-broker", -1, -1, 0)
	other.ReplicaAssignment[0].Replicas = []int32{42}
	if got := oracleCreate(t, c, false, other); got[0].ErrorCode != 39 || !strings.Contains(deref(got[0].ErrorMessage), "broker 42") {
		t.Errorf("a replica on an unknown broker: %d %q", got[0].ErrorCode, deref(got[0].ErrorMessage))
	}
	dup := assign("dup-replica", -1, -1, 0)
	dup.ReplicaAssignment[0].Replicas = []int32{1, 1}
	if got := oracleCreate(t, c, false, dup); got[0].ErrorCode != 39 {
		t.Errorf("a replica listed twice: code %d, want 39", got[0].ErrorCode)
	}
	none := assign("no-replicas", -1, -1, 0)
	none.ReplicaAssignment[0].Replicas = nil
	if got := oracleCreate(t, c, false, none); got[0].ErrorCode != 39 {
		t.Errorf("a partition with no replicas: code %d, want 39", got[0].ErrorCode)
	}
}

func TestOracle_ListOffsetsAnswersAsKafkaDoes(t *testing.T) {
	addr := startBroker(t, 4)
	ctx := context.Background()
	cl := newClient(t, addr)
	mustCreateTopic(t, ctx, cl, "lo", 1)
	produceN(t, ctx, cl, "lo", 3)
	c := openRaw(t, addr)

	ask := func(ts int64, epoch int32, isolation int8, version int16) kmsg.ListOffsetsResponseTopicPartition {
		t.Helper()
		req := kmsg.NewPtrListOffsetsRequest()
		req.SetVersion(version)
		req.ReplicaID, req.IsolationLevel = -1, isolation
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = "lo"
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition, rp.Timestamp, rp.CurrentLeaderEpoch = 0, ts, epoch
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		var resp kmsg.ListOffsetsResponse
		if !c.do(req, &resp) {
			t.Fatal("connection closed")
		}
		return resp.Topics[0].Partitions[0]
	}

	// An answer that found an offset carries leader epoch 0, one that found nothing -1.
	if p := ask(-1, -1, 0, 7); p.ErrorCode != 0 || p.Offset != 3 || p.LeaderEpoch != 0 {
		t.Errorf("latest: %+v, want offset 3 at epoch 0", p)
	}
	if p := ask(1<<50, -1, 0, 7); p.Offset != -1 || p.LeaderEpoch != -1 {
		t.Errorf("a timestamp after everything: %+v, want offset -1 at epoch -1", p)
	}
	// Timestamps -4 and -5 came with v8 and v9, which are not offered; -6 is none.
	for _, ts := range []int64{-4, -5, -6} {
		if p := ask(ts, -1, 0, 7); p.ErrorCode != 35 {
			t.Errorf("timestamp %d at v7: code %d, want UNSUPPORTED_VERSION (35)", ts, p.ErrorCode)
		}
	}
	// Max timestamp is v7 and later.
	if p := ask(-3, -1, 0, 6); p.ErrorCode != 35 {
		t.Errorf("max timestamp at v6: code %d, want UNSUPPORTED_VERSION (35)", p.ErrorCode)
	}
	// A leader epoch this broker has not had.
	if p := ask(-1, 100, 0, 7); p.ErrorCode != 75 {
		t.Errorf("a future leader epoch: code %d, want UNKNOWN_LEADER_EPOCH (75)", p.ErrorCode)
	}
	if p := ask(-1, 0, 0, 7); p.ErrorCode != 0 {
		t.Errorf("the current leader epoch: code %d, want 0", p.ErrorCode)
	}
	// An isolation level that is neither 0 nor 1 is Kafka's unexplained error.
	if p := ask(-1, -1, 7, 7); p.ErrorCode != -1 {
		t.Errorf("isolation level 7: code %d, want UNKNOWN_SERVER_ERROR (-1)", p.ErrorCode)
	}
}

func TestOracle_FindCoordinatorKeyTypesAndMessages(t *testing.T) {
	c := openRaw(t, startBroker(t, 4))
	find := func(version int16, keyType int8, key string) kmsg.FindCoordinatorResponse {
		t.Helper()
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.SetVersion(version)
		req.CoordinatorType = keyType
		if version >= 4 {
			req.CoordinatorKeys = []string{key}
		} else {
			req.CoordinatorKey = key
		}
		var resp kmsg.FindCoordinatorResponse
		if !c.do(req, &resp) {
			t.Fatal("connection closed")
		}
		return resp
	}

	// v4: a success says "" per coordinator; v1 to v3 say "NONE".
	if r := find(4, 0, "g"); len(r.Coordinators) != 1 || r.Coordinators[0].ErrorCode != 0 || deref(r.Coordinators[0].ErrorMessage) != "" || r.Coordinators[0].ErrorMessage == nil {
		t.Errorf("v4 group: %+v", r.Coordinators)
	}
	if r := find(3, 1, "txn"); r.ErrorCode != 0 || deref(r.ErrorMessage) != "NONE" {
		t.Errorf("v3 transaction: code %d message %q, want NONE", r.ErrorCode, deref(r.ErrorMessage))
	}
	// A key type that is not a group or a transaction is INVALID_REQUEST, with no node.
	for _, kt := range []int8{9, -1} {
		r := find(4, kt, "x")
		if r.Coordinators[0].ErrorCode != 42 || r.Coordinators[0].NodeID != -1 || !strings.HasPrefix(deref(r.Coordinators[0].ErrorMessage), "This most likely occurs") {
			t.Errorf("v4 key type %d: %+v", kt, r.Coordinators[0])
		}
	}
	// A share group (2) needs v6, which is not offered.
	if r := find(4, 2, "x"); r.Coordinators[0].ErrorCode != 42 || r.Coordinators[0].NodeID != -1 {
		t.Errorf("v4 share group: %+v", r.Coordinators[0])
	}
	if r := find(1, 9, "x"); r.ErrorCode != 42 || r.NodeID != -1 {
		t.Errorf("v1 key type 9: code %d node %d", r.ErrorCode, r.NodeID)
	}
}

func TestOracle_OffsetMetadataLimitAndFetchNeverNull(t *testing.T) {
	addr := startBroker(t, 4)
	ctx := context.Background()
	cl := newClient(t, addr)
	mustCreateTopic(t, ctx, cl, "om", 1)
	c := openRaw(t, addr)

	commit := func(meta string) int16 {
		t.Helper()
		req := kmsg.NewPtrOffsetCommitRequest()
		req.SetVersion(8)
		req.Group, req.Generation = "gm", -1
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = "om"
		rp := kmsg.NewOffsetCommitRequestTopicPartition()
		rp.Partition, rp.Offset, rp.LeaderEpoch = 0, 1, -1
		rp.Metadata = &meta
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		var resp kmsg.OffsetCommitResponse
		if !c.do(req, &resp) {
			t.Fatal("connection closed")
		}
		return resp.Topics[0].Partitions[0].ErrorCode
	}
	// offset.metadata.max.bytes is 4096: that is accepted, one more is not.
	if code := commit(strings.Repeat("m", 4096)); code != 0 {
		t.Errorf("4096 bytes of metadata: code %d, want 0", code)
	}
	if code := commit(strings.Repeat("m", 4097)); code != 12 {
		t.Errorf("4097 bytes of metadata: code %d, want OFFSET_METADATA_TOO_LARGE (12)", code)
	}

	// A fetch of a partition with no commit, or a commit with no metadata,
	// answers an empty string: Kafka never sends null there.
	fetch := func(group string) *string {
		t.Helper()
		req := kmsg.NewPtrOffsetFetchRequest()
		req.SetVersion(8)
		g := kmsg.NewOffsetFetchRequestGroup()
		g.Group = group
		ft := kmsg.NewOffsetFetchRequestGroupTopic()
		ft.Topic, ft.Partitions = "om", []int32{0}
		g.Topics = append(g.Topics, ft)
		req.Groups = append(req.Groups, g)
		var resp kmsg.OffsetFetchResponse
		if !c.do(req, &resp) {
			t.Fatal("connection closed")
		}
		return resp.Groups[0].Topics[0].Partitions[0].Metadata
	}
	if m := fetch("never-committed"); m == nil || *m != "" {
		t.Errorf("metadata of an uncommitted partition: %v, want an empty string", m)
	}
}
