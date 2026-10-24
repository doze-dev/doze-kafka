package broker

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// Topic administration: creating, growing, configuring, deleting and describing.

// adminRig is a broker over a store that can fail to delete a topic, with its data
// directory known.
type adminRig struct {
	meta.Store
	deleteErr error
	// legacy are topics the registry holds under names the store would refuse today.
	legacy map[string]meta.Topic
}

func (r *adminRig) Topic(name string) (meta.Topic, bool, error) {
	if t, ok := r.legacy[name]; ok {
		return t, true, nil
	}
	return r.Store.Topic(name)
}

func (r *adminRig) DeleteTopic(name string) error {
	if r.deleteErr != nil {
		return r.deleteErr
	}
	return r.Store.DeleteTopic(name)
}

func newAdminBroker(t *testing.T) (b *Broker, rig *adminRig, dataDir string) {
	t.Helper()
	dir := t.TempDir()
	store, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	dataDir = filepath.Join(dir, "data")
	logs, err := log.OpenManager(dataDir, log.ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logs.Close() })
	cfg := config.Default()
	cfg.FsyncInterval = 0
	rig = &adminRig{Store: store}
	b, err = New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), logs, rig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Stop)
	return b, rig, dataDir
}

func createTopicsReq(validateOnly bool, topics ...kmsg.CreateTopicsRequestTopic) *kmsg.CreateTopicsRequest {
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis, req.ValidateOnly = 5000, validateOnly
	req.Topics = topics
	return req
}

func newTopic(name string, partitions int32, rf int16) kmsg.CreateTopicsRequestTopic {
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic, t.NumPartitions, t.ReplicationFactor = name, partitions, rf
	return t
}

func assigned(name string, parts ...[]int32) kmsg.CreateTopicsRequestTopic {
	t := newTopic(name, -1, -1)
	for p, replicas := range parts {
		a := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
		a.Partition, a.Replicas = int32(p), replicas
		t.ReplicaAssignment = append(t.ReplicaAssignment, a)
	}
	return t
}

func createOne(t *testing.T, b *Broker, topic kmsg.CreateTopicsRequestTopic) kmsg.CreateTopicsResponseTopic {
	t.Helper()
	return callAPI(t, b.handleCreateTopics, createTopicsReq(false, topic), 7).Topics[0]
}

func TestCreateTopicsHoldsReplicationAndAssignmentToWhatOneNodeCanDo(t *testing.T) {
	b, _, _ := newAdminBroker(t)
	for i, tc := range []struct {
		name  string
		topic kmsg.CreateTopicsRequestTopic
		code  int16
		text  string // a piece of the message, "" for none.
	}{
		{"replication factor -1 is the default", newTopic("rf-default", 1, -1), 0, ""},
		{"replication factor 1", newTopic("rf-one", 1, 1), 0, ""},
		{"replication factor 0", newTopic("rf-zero", 1, 0), kerr.InvalidReplicationFactor.Code, "larger than 0"},
		{"replication factor -2", newTopic("rf-minus-two", 1, -2), kerr.InvalidReplicationFactor.Code, "larger than 0"},
		{"zero partitions", newTopic("p-zero", 0, 1), kerr.InvalidPartitions.Code, "0 is not a partition count"},
		{"a consecutive assignment", assigned("a-good", []int32{b.nodeID()}, []int32{b.nodeID()}), 0, ""},
		{"an assignment with a gap", func() kmsg.CreateTopicsRequestTopic {
			x := assigned("a-gap", []int32{b.nodeID()}, []int32{b.nodeID()})
			x.ReplicaAssignment[1].Partition = 2
			return x
		}(), kerr.InvalidReplicaAssignment.Code, "consecutive"},
		{"an assignment with a partition twice", func() kmsg.CreateTopicsRequestTopic {
			x := assigned("a-twice", []int32{b.nodeID()}, []int32{b.nodeID()})
			x.ReplicaAssignment[1].Partition = 0
			return x
		}(), kerr.InvalidReplicaAssignment.Code, "more than once"},
		{"an assignment with an empty replica list", assigned("a-empty", []int32{b.nodeID()}, nil), kerr.InvalidReplicaAssignment.Code, "empty replica list for partition 1"},
		{"an assignment naming another broker", assigned("a-other", []int32{b.nodeID() + 5}), kerr.InvalidReplicaAssignment.Code, "no such broker"},
		{"an assignment naming this broker twice", assigned("a-dup", []int32{b.nodeID(), b.nodeID()}), kerr.InvalidReplicaAssignment.Code, "more than once"},
		{"an assignment with a replication factor", func() kmsg.CreateTopicsRequestTopic {
			x := assigned("a-rf", []int32{b.nodeID()})
			x.ReplicationFactor = 1
			return x
		}(), kerr.InvalidRequest.Code, "replication factor"},
		{"an assignment with a partition count", func() kmsg.CreateTopicsRequestTopic {
			x := assigned("a-np", []int32{b.nodeID()})
			x.NumPartitions = 1
			return x
		}(), kerr.InvalidRequest.Code, "numPartitions"},
	} {
		got := createOne(t, b, tc.topic)
		if got.ErrorCode != tc.code {
			t.Errorf("%d %s: code %d, want %d (%v)", i, tc.name, got.ErrorCode, tc.code, got.ErrorMessage)
			continue
		}
		if tc.text != "" && (got.ErrorMessage == nil || !strings.Contains(*got.ErrorMessage, tc.text)) {
			t.Errorf("%s: message %v, want it to say %q", tc.name, got.ErrorMessage, tc.text)
		}
		if tc.code == 0 && got.NumPartitions < 1 {
			t.Errorf("%s: %d partitions made", tc.name, got.NumPartitions)
		}
	}
}

// A topic that cannot be given its directories is not left registered: the name is
// free again, and the answer says the disk is the trouble.
func TestCreateTopicRollsBackWhenItsDirectoriesCannotBeMade(t *testing.T) {
	b, rig, dataDir := newAdminBroker(t)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A file where the topic's first partition directory has to go.
	if err := os.WriteFile(filepath.Join(dataDir, "blocked-0"), []byte("in the way"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := createOne(t, b, newTopic("blocked", 2, 1))
	if got.ErrorCode != kerr.KafkaStorageError.Code || got.ErrorMessage == nil {
		t.Errorf("a topic whose directories cannot be made: code %d message %v, want KAFKA_STORAGE_ERROR with a message", got.ErrorCode, got.ErrorMessage)
	}
	if _, ok, _ := b.meta.Topic("blocked"); ok {
		t.Error("the topic stayed registered after its directories could not be made")
	}
	// The name is free: once the way is clear it can be made.
	_ = os.Remove(filepath.Join(dataDir, "blocked-0")) // the rollback may have cleared it.
	if got := createOne(t, b, newTopic("blocked", 2, 1)); got.ErrorCode != 0 {
		t.Errorf("creating it again: code %d %v", got.ErrorCode, got.ErrorMessage)
	}

	// The same through auto-creation: nothing is created and nothing is reported as created.
	b.cfg.AutoCreateTopics = true
	if err := os.WriteFile(filepath.Join(dataDir, "auto-0"), []byte("in the way"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, made := b.maybeAutoCreate("auto", true); made {
		t.Error("auto-creation reported a topic whose directories could not be made")
	}
	if _, ok, _ := b.meta.Topic("auto"); ok {
		t.Error("auto-creation left the topic registered")
	}

	// A rollback that itself fails is logged, not swallowed.
	logs := captureLogs(b)
	if err := os.WriteFile(filepath.Join(dataDir, "stuck-0"), []byte("in the way"), 0o644); err != nil {
		t.Fatal(err)
	}
	rig.deleteErr = errors.New("the registry is read-only")
	createOne(t, b, newTopic("stuck", 1, 1))
	if s := logs.String(); !strings.Contains(s, "rollback of registration failed") {
		t.Errorf("a rollback that failed was not logged:\n%s", s)
	}
}

// A name taken by directories on disk that belong to no topic is cleared, and said.
func TestCreateTopicClearsPartitionDirectoriesThatBelongToNoTopic(t *testing.T) {
	b, _, dataDir := newAdminBroker(t)
	logs := captureLogs(b)
	for _, d := range []string{"ghost-0", "ghost-1"} {
		if err := os.MkdirAll(filepath.Join(dataDir, d), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dataDir, d, "00000000000000000000.log"), []byte("old"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if got := createOne(t, b, newTopic("ghost", 1, 1)); got.ErrorCode != 0 {
		t.Fatalf("create: %d", got.ErrorCode)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "ghost-1")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a leftover partition directory survived: %v", err)
	}
	if s := logs.String(); !strings.Contains(s, "removed leftover partition directories") || !strings.Contains(s, "directories=2") {
		t.Errorf("the clearing was not logged with its count:\n%s", s)
	}
	// With nothing to clear nothing is said.
	logs2 := captureLogs(b)
	createOne(t, b, newTopic("clean", 1, 1))
	if s := logs2.String(); strings.Contains(s, "leftover") {
		t.Errorf("a topic with nothing leftover logged:\n%s", s)
	}
}

func TestAutoCreationOnlyWhenAskedAndAllowed(t *testing.T) {
	b, _, _ := newAdminBroker(t)
	b.cfg.AutoCreateTopics = false
	if _, made := b.maybeAutoCreate("a", true); made {
		t.Error("created with auto-creation off")
	}
	b.cfg.AutoCreateTopics = true
	if _, made := b.maybeAutoCreate("a", false); made {
		t.Error("created though the request did not ask")
	}
	created, made := b.maybeAutoCreate("a", true)
	if !made || created.Name != "a" {
		t.Fatalf("not created: %v %v", created, made)
	}
	// A lost race: the topic is already there, and that is what is returned.
	again, made := b.maybeAutoCreate("a", true)
	if !made || again.ID != created.ID {
		t.Errorf("an existing topic: %v %v, want the same topic", again, made)
	}
}

func deleteTopicsReq(topics ...kmsg.DeleteTopicsRequestTopic) *kmsg.DeleteTopicsRequest {
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TimeoutMillis = 5000
	req.Topics = topics
	return req
}

func TestDeleteTopicsNamesTheTopicItResolved(t *testing.T) {
	b, _, _ := newAdminBroker(t)
	made := createOne(t, b, newTopic("doomed", 1, 1))
	made2 := createOne(t, b, newTopic("doomed-too", 1, 1))
	name := "doomed"
	byName := kmsg.NewDeleteTopicsRequestTopic()
	byName.Topic = &name
	byID := kmsg.NewDeleteTopicsRequestTopic()
	byID.TopicID = made2.TopicID
	unknownID := kmsg.NewDeleteTopicsRequestTopic()
	unknownID.TopicID = [16]byte{9, 9, 9}
	missing := "missing"
	byMissing := kmsg.NewDeleteTopicsRequestTopic()
	byMissing.Topic = &missing

	resp := callAPI(t, b.handleDeleteTopics, deleteTopicsReq(byName, byID, unknownID, byMissing), 6)
	if len(resp.Topics) != 4 {
		t.Fatalf("%d answers", len(resp.Topics))
	}
	for i, want := range []struct {
		name string // "" = none.
		code int16
		id   [16]byte
	}{
		{"doomed", 0, made.TopicID},
		{"doomed-too", 0, made2.TopicID},
		{"", kerr.UnknownTopicOrPartition.Code, [16]byte{9, 9, 9}},
		{"missing", kerr.UnknownTopicOrPartition.Code, [16]byte{}},
	} {
		got := resp.Topics[i]
		gotName := ""
		if got.Topic != nil {
			gotName = *got.Topic
		}
		if gotName != want.name || got.ErrorCode != want.code || got.TopicID != want.id {
			t.Errorf("answer %d: topic %q code %d id %v, want %q %d %v", i, gotName, got.ErrorCode, got.TopicID, want.name, want.code, want.id)
		}
	}
	for _, n := range []string{"doomed", "doomed-too"} {
		if _, ok, _ := b.meta.Topic(n); ok {
			t.Errorf("%s is still registered", n)
		}
	}
}

func TestCreatePartitionsHoldsTheCountToTheTopicAndTheBroker(t *testing.T) {
	b, _, _ := newAdminBroker(t)
	b.cfg.MaxPartitions = 6
	if got := createOne(t, b, newTopic("grow", 3, 1)); got.ErrorCode != 0 {
		t.Fatal(got.ErrorCode)
	}
	grow := func(count int32, assignments ...[]int32) kmsg.CreatePartitionsResponseTopic {
		req := kmsg.NewPtrCreatePartitionsRequest()
		req.TimeoutMillis = 5000
		rt := kmsg.NewCreatePartitionsRequestTopic()
		rt.Topic, rt.Count = "grow", count
		if assignments != nil {
			rt.Assignment = []kmsg.CreatePartitionsRequestTopicAssignment{}
			for _, replicas := range assignments {
				a := kmsg.NewCreatePartitionsRequestTopicAssignment()
				a.Replicas = replicas
				rt.Assignment = append(rt.Assignment, a)
			}
		}
		req.Topics = append(req.Topics, rt)
		return callAPI(t, b.handleCreatePartitions, req, 3).Topics[0]
	}
	msg := func(r kmsg.CreatePartitionsResponseTopic) string {
		if r.ErrorMessage == nil {
			return ""
		}
		return *r.ErrorMessage
	}
	if r := grow(3); r.ErrorCode != kerr.InvalidPartitions.Code || !strings.Contains(msg(r), "already has 3 partitions") {
		t.Errorf("the same count: %d %q", r.ErrorCode, msg(r))
	}
	if r := grow(2); r.ErrorCode != kerr.InvalidPartitions.Code || !strings.Contains(msg(r), "3 partitions and 2 is fewer") {
		t.Errorf("a smaller count: %d %q", r.ErrorCode, msg(r))
	}
	if r := grow(7); r.ErrorCode != kerr.InvalidPartitions.Code || !strings.Contains(msg(r), "7 partitions is above this broker's limit of 6") {
		t.Errorf("above the limit: %d %q", r.ErrorCode, msg(r))
	}
	if r := grow(5, []int32{b.nodeID()}); r.ErrorCode != kerr.InvalidReplicaAssignment.Code || !strings.Contains(msg(r), "names 1 new partitions, the request adds 2") {
		t.Errorf("an assignment for too few: %d %q", r.ErrorCode, msg(r))
	}
	if r := grow(4, []int32{b.nodeID() + 1}); r.ErrorCode != kerr.InvalidReplicaAssignment.Code {
		t.Errorf("an assignment naming another broker: %d %q", r.ErrorCode, msg(r))
	}
	if r := grow(6); r.ErrorCode != 0 {
		t.Errorf("up to exactly the limit: %d %q", r.ErrorCode, msg(r))
	}
	if cur, _, _ := b.meta.Topic("grow"); cur.Partitions != 6 {
		t.Errorf("%d partitions after growing to 6", cur.Partitions)
	}
}

func TestMetadataCreatesTheTopicOnlyWhereTheRequestAllowsIt(t *testing.T) {
	b, _, _ := newAdminBroker(t)
	b.cfg.AutoCreateTopics = true
	ask := func(version int16, allow bool, name string) kmsg.MetadataResponseTopic {
		req := kmsg.NewPtrMetadataRequest()
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = &name
		req.Topics = append(req.Topics, rt)
		req.AllowAutoTopicCreation = allow
		return callAPI(t, b.handleMetadata, req, version).Topics[0]
	}
	exists := func(name string) bool { _, ok, _ := b.meta.Topic(name); return ok }
	for _, tc := range []struct {
		name    string
		version int16
		allow   bool
		made    bool
	}{
		{"v3 has no flag: always allowed", 3, false, true},
		{"v4 not allowed", 4, false, false},
		{"v4 allowed", 4, true, true},
		{"v9 not allowed", 9, false, false},
	} {
		name := fmt.Sprintf("v%d-allow-%v", tc.version, tc.allow)
		first := ask(tc.version, tc.allow, name)
		// The request that asked for it is told it is not there, as Kafka does.
		if first.ErrorCode != kerr.UnknownTopicOrPartition.Code {
			t.Errorf("%s: code %d, want UNKNOWN_TOPIC_OR_PARTITION", tc.name, first.ErrorCode)
		}
		if exists(name) != tc.made {
			t.Errorf("%s: created=%v, want %v", tc.name, exists(name), tc.made)
		}
	}
}

func TestMetadataReachesATopicRegisteredBeforeNamesWereChecked(t *testing.T) {
	b, rig, _ := newAdminBroker(t)
	const legacy = "legacy topic!"
	rig.legacy = map[string]meta.Topic{legacy: {Name: legacy, ID: meta.TopicID{7}, Partitions: 1}}
	ask := func(name string) kmsg.MetadataResponseTopic {
		req := kmsg.NewPtrMetadataRequest()
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = &name
		req.Topics = append(req.Topics, rt)
		return callAPI(t, b.handleMetadata, req, 9).Topics[0]
	}
	if got := ask(legacy); got.ErrorCode != 0 || len(got.Partitions) != 1 {
		t.Errorf("a registered topic whose name would be refused today: code %d with %d partitions, want it served", got.ErrorCode, len(got.Partitions))
	}
	if got := ask("never registered!"); got.ErrorCode != kerr.InvalidTopicException.Code {
		t.Errorf("a name that would be refused: code %d, want INVALID_TOPIC_EXCEPTION", got.ErrorCode)
	}
}
