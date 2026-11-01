package broker

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// The transaction APIs a producer drives, and the three that are for tooling.

// owner starts a transaction for id on partition 0 of "t" and returns who owns it.
func owner(t *testing.T, b *Broker, id string) (pid int64, epoch int16) {
	t.Helper()
	r := initTxn(t, b, id, 60000)
	if r.ErrorCode != 0 {
		t.Fatalf("init %s: %d", id, r.ErrorCode)
	}
	return r.ProducerID, r.ProducerEpoch
}

// topicWithPartitions adds a topic with n partitions to the broker's store and log.
func topicWithPartitions(t *testing.T, b *Broker, name string, n int32) {
	t.Helper()
	if _, err := b.meta.CreateTopic(name, n, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.logs.EnsurePartitionDirs(name, n); err != nil {
		t.Fatal(err)
	}
}

// denyUnlessAllowed replaces the broker's rules with default-deny and the entries
// given, for the anonymous principal a nil connection is.
func denyUnlessAllowed(b *Broker, entries ...meta.ACL) {
	cfg := config.Default()
	cfg.ACLDefault = config.ACLDeny
	b.authz = newAuthorizer(cfg, entries)
}

func allowAnon(rt int8, name string, op int8) meta.ACL {
	return acl(rt, name, aclLiteral, "User:ANONYMOUS", "*", op, permAllow)
}

func TestAddPartitionsToTxnAnswersEachPartitionAndStoresThemOnce(t *testing.T) {
	b, rig, _, _ := newTxnRig(t)
	topicWithPartitions(t, b, "u", 2)
	pid, epoch := owner(t, b, "tx")

	codes := func(ps []kmsg.AddPartitionsToTxnResponseTopicPartition) []int16 {
		var out []int16
		for _, p := range ps {
			out = append(out, p.ErrorCode)
		}
		return out
	}
	// A partition past the last is unknown, and the good ones are not attempted.
	got := codes(addPartitions(t, b, "tx", pid, epoch, "u", 0, 2))
	if want := []int16{kerr.OperationNotAttempted.Code, kerr.UnknownTopicOrPartition.Code}; !slices.Equal(got, want) {
		t.Errorf("partitions 0 and 2 of a 2-partition topic: %v, want %v", got, want)
	}
	got = codes(addPartitions(t, b, "tx", pid, epoch, "u", -1, 0))
	if want := []int16{kerr.UnknownTopicOrPartition.Code, kerr.OperationNotAttempted.Code}; !slices.Equal(got, want) {
		t.Errorf("partitions -1 and 0: %v, want %v", got, want)
	}
	if n := len(b.txns.get("tx").rec.Partitions); n != 0 {
		t.Errorf("%d partitions stored after refusals, want none", n)
	}
	// The same partition twice is stored once.
	for range 2 {
		if got := codes(addPartitions(t, b, "tx", pid, epoch, "u", 1, 0)); !slices.Equal(got, []int16{0, 0}) {
			t.Fatalf("adding 1 and 0: %v", got)
		}
	}
	if ps := b.txns.get("tx").rec.Partitions; fmt.Sprint(ps) != "[{u 1} {u 0}]" {
		t.Errorf("stored %v, want the two partitions once each, in the order added", ps)
	}
	// When the store cannot take them every partition says so.
	rig.putErr = errStore
	if got := codes(addPartitions(t, b, "tx", pid, epoch, "u", 0, 1)); !slices.Equal(got, []int16{txnStoreUnavailable, txnStoreUnavailable}) {
		t.Errorf("store down: %v, want NOT_COORDINATOR for both", got)
	}
}

func TestAddPartitionsToTxnRefusesWhoIsNotTheOwner(t *testing.T) {
	b, _, failSync, _ := newTxnRig(t)
	pid, epoch := owner(t, b, "tx")
	first := func(id string, p int64, e int16) int16 {
		return addPartitions(t, b, id, p, e, "t", 0)[0].ErrorCode
	}
	for _, tc := range []struct {
		name  string
		id    string
		pid   int64
		epoch int16
		code  int16
	}{
		{"an id nobody owns", "nobody", pid, epoch, kerr.InvalidProducerIDMapping.Code},
		{"another producer id", "tx", pid + 1, epoch, kerr.InvalidProducerIDMapping.Code},
		{"an older epoch", "tx", pid, epoch - 1, kerr.ProducerFenced.Code},
		{"a newer epoch", "tx", pid, epoch + 1, kerr.InvalidProducerEpoch.Code},
		{"no id", "", pid, epoch, kerr.InvalidRequest.Code},
		{"the owner", "tx", pid, epoch, 0},
	} {
		if got := first(tc.id, tc.pid, tc.epoch); got != tc.code {
			t.Errorf("%s: code %d, want %d", tc.name, got, tc.code)
		}
	}
	// Rules come first: the id needs Write, the topic needs Write.
	denyUnlessAllowed(b)
	if got := first("tx", pid, epoch); got != kerr.TransactionalIDAuthorizationFailed.Code {
		t.Errorf("no right on the id: code %d", got)
	}
	denyUnlessAllowed(b, allowAnon(resTxnID, "tx", opWrite))
	if got := first("tx", pid, epoch); got != kerr.TopicAuthorizationFailed.Code {
		t.Errorf("no right on the topic: code %d", got)
	}
	// While a transaction is being ended nothing may be added.
	denyUnlessAllowed(b, allowAnon(resTxnID, "tx", opWrite), allowAnon(resTopic, "t", opWrite))
	dirty(t, b)
	*failSync = true
	if code := endTxnCall(t, b, "tx", pid, epoch, true); code != kerr.ConcurrentTransactions.Code {
		t.Fatalf("EndTxn with the flush failing: %d", code)
	}
	if got := first("tx", pid, epoch); got != kerr.ConcurrentTransactions.Code {
		t.Errorf("adding to a transaction being ended: code %d, want CONCURRENT_TRANSACTIONS", got)
	}
}

func TestAddOffsetsToTxnNamesTheGroupOnce(t *testing.T) {
	b, rig, failSync, _ := newTxnRig(t)
	pid, epoch := owner(t, b, "tx")
	add := func(id, group string, p int64, e int16) int16 {
		req := kmsg.NewPtrAddOffsetsToTxnRequest()
		req.TransactionalID, req.ProducerID, req.ProducerEpoch, req.Group = id, p, e, group
		return callAPI(t, b.handleAddOffsetsToTxn, req, 3).ErrorCode
	}
	for range 2 {
		if code := add("tx", "g", pid, epoch); code != 0 {
			t.Fatalf("adding g: %d", code)
		}
	}
	if code := add("tx", "h", pid, epoch); code != 0 {
		t.Fatalf("adding h: %d", code)
	}
	if got := b.txns.get("tx").rec.Groups; !slices.Equal(got, []string{"g", "h"}) {
		t.Errorf("groups %v, want [g h]", got)
	}
	if e := b.txns.get("tx"); e.rec.State != txnOngoing {
		t.Errorf("state %s, want Ongoing", e.rec.State)
	}
	for _, tc := range []struct {
		name  string
		id    string
		pid   int64
		epoch int16
		code  int16
	}{
		{"an id nobody owns", "nobody", pid, epoch, kerr.InvalidProducerIDMapping.Code},
		{"another producer id", "tx", pid + 1, epoch, kerr.InvalidProducerIDMapping.Code},
		{"an older epoch", "tx", pid, epoch - 1, kerr.ProducerFenced.Code},
		{"no id", "", pid, epoch, kerr.InvalidRequest.Code},
	} {
		if got := add(tc.id, "g", tc.pid, tc.epoch); got != tc.code {
			t.Errorf("%s: code %d, want %d", tc.name, got, tc.code)
		}
	}
	// A group already named needs no write; one not yet named, with the store down, fails.
	rig.putErr = errStore
	if got := add("tx", "g", pid, epoch); got != 0 {
		t.Errorf("a group already named, store down: code %d, want 0 with nothing written", got)
	}
	if got := add("tx", "new", pid, epoch); got != txnStoreUnavailable {
		t.Errorf("store down: code %d, want NOT_COORDINATOR", got)
	}
	rig.putErr = nil
	// Nothing is added while the transaction is being ended.
	if ps := addPartitions(t, b, "tx", pid, epoch, "t", 0); ps[0].ErrorCode != 0 {
		t.Fatal(ps[0].ErrorCode)
	}
	dirty(t, b)
	*failSync = true
	endTxnCall(t, b, "tx", pid, epoch, true)
	if got := add("tx", "late", pid, epoch); got != kerr.ConcurrentTransactions.Code {
		t.Errorf("adding to a transaction being ended: code %d", got)
	}
	*failSync = false
	denyUnlessAllowed(b)
	if got := add("tx", "g", pid, epoch); got != kerr.TransactionalIDAuthorizationFailed.Code {
		t.Errorf("no right on the id: code %d", got)
	}
	denyUnlessAllowed(b, allowAnon(resTxnID, "tx", opWrite))
	if got := add("tx", "g", pid, epoch); got != kerr.GroupAuthorizationFailed.Code {
		t.Errorf("no right on the group: code %d", got)
	}
}

func txnOffsetCommit(t *testing.T, b *Broker, id, group string, pid int64, epoch int16, generation int32, member string, parts map[string][]int32, offset int64) *kmsg.TxnOffsetCommitResponse {
	t.Helper()
	req := kmsg.NewPtrTxnOffsetCommitRequest()
	req.TransactionalID, req.Group, req.ProducerID, req.ProducerEpoch = id, group, pid, epoch
	req.Generation, req.MemberID = generation, member
	for topic, ps := range parts {
		rt := kmsg.NewTxnOffsetCommitRequestTopic()
		rt.Topic = topic
		for _, p := range ps {
			rp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
			rp.Partition, rp.Offset = p, offset
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
	}
	return callAPI(t, b.handleTxnOffsetCommit, req, 3)
}

func TestTxnOffsetCommitStagesOffsetsAndAppliesThemOnCommit(t *testing.T) {
	b, rig, _, _ := newTxnRig(t)
	topicWithPartitions(t, b, "u", 3)
	pid, epoch := owner(t, b, "tx")
	addOffsets := func(group string) {
		req := kmsg.NewPtrAddOffsetsToTxnRequest()
		req.TransactionalID, req.ProducerID, req.ProducerEpoch, req.Group = "tx", pid, epoch, group
		if code := callAPI(t, b.handleAddOffsetsToTxn, req, 3).ErrorCode; code != 0 {
			t.Fatalf("AddOffsetsToTxn %s: %d", group, code)
		}
	}
	code0 := func(r *kmsg.TxnOffsetCommitResponse) []int16 {
		var out []int16
		for _, tp := range r.Topics {
			for _, p := range tp.Partitions {
				out = append(out, p.ErrorCode)
			}
		}
		return out
	}

	// Offsets for a group the transaction has not named are refused.
	if got := code0(txnOffsetCommit(t, b, "tx", "g", pid, epoch, -1, "", map[string][]int32{"u": {0}}, 1)); !slices.Equal(got, []int16{kerr.InvalidTxnState.Code}) {
		t.Errorf("a group not named: %v, want INVALID_TXN_STATE", got)
	}
	addOffsets("g")
	addOffsets("h")

	// The same partition committed again replaces its offset; a partition of another
	// topic, another partition or another group is another entry.
	for _, step := range []struct {
		group string
		topic string
		part  int32
		off   int64
	}{{"g", "u", 0, 5}, {"g", "u", 0, 6}, {"g", "u", 1, 7}, {"g", "t", 0, 8}, {"h", "u", 0, 9}} {
		got := code0(txnOffsetCommit(t, b, "tx", step.group, pid, epoch, -1, "", map[string][]int32{step.topic: {step.part}}, step.off))
		if !slices.Equal(got, []int16{0}) {
			t.Fatalf("committing %+v: %v", step, got)
		}
	}
	staged := map[string]int64{}
	for _, o := range b.txns.get("tx").rec.Offsets {
		staged[fmt.Sprintf("%s/%s/%d", o.Group, o.Topic, o.Partition)] = o.Offset
	}
	want := map[string]int64{"g/u/0": 6, "g/u/1": 7, "g/t/0": 8, "h/u/0": 9}
	if fmt.Sprint(staged) != fmt.Sprint(want) || len(b.txns.get("tx").rec.Offsets) != 4 {
		t.Errorf("staged %v (%d entries), want %v", staged, len(b.txns.get("tx").rec.Offsets), want)
	}
	// Nothing is visible to the group until the transaction commits.
	if _, ok, _ := b.meta.FetchOffset("g", "u", 0); ok {
		t.Error("a staged offset was committed before the transaction ended")
	}
	if code := endTxnCall(t, b, "tx", pid, epoch, true); code != 0 {
		t.Fatalf("EndTxn: %d", code)
	}
	if off, ok, _ := b.meta.FetchOffset("g", "u", 0); !ok || off != 6 {
		t.Errorf("g/u/0 after the commit: %d %v, want 6", off, ok)
	}

	// Per-partition answers: past the last, negative, and a member that does not belong.
	pid, epoch = owner(t, b, "tx2")
	req := kmsg.NewPtrAddOffsetsToTxnRequest()
	req.TransactionalID, req.ProducerID, req.ProducerEpoch, req.Group = "tx2", pid, epoch, "g"
	callAPI(t, b.handleAddOffsetsToTxn, req, 3)
	if got := code0(txnOffsetCommit(t, b, "tx2", "g", pid, epoch, -1, "", map[string][]int32{"u": {2, 3, -1}}, 1)); !slices.Equal(got, []int16{0, kerr.UnknownTopicOrPartition.Code, kerr.UnknownTopicOrPartition.Code}) {
		t.Errorf("partitions 2, 3, -1 of a 3-partition topic: %v", got)
	}
	// The store failing marks only the partitions that were to be stored.
	rig.putErr = errStore
	if got := code0(txnOffsetCommit(t, b, "tx2", "g", pid, epoch, -1, "", map[string][]int32{"u": {1, 7}}, 1)); !slices.Equal(got, []int16{txnStoreUnavailable, kerr.UnknownTopicOrPartition.Code}) {
		t.Errorf("store down, partitions 1 and 7: %v, want NOT_COORDINATOR and UNKNOWN_TOPIC_OR_PARTITION", got)
	}
	rig.putErr = nil

	// A commit in which nothing could be staged does not touch the transaction, whose
	// timeout runs from its last change.
	e := b.txns.get("tx2")
	e.mu.Lock()
	e.rec.UpdatedMs = 1
	e.mu.Unlock()
	txnOffsetCommit(t, b, "tx2", "g", pid, epoch, -1, "", map[string][]int32{"u": {9}}, 1)
	if got := e.rec.UpdatedMs; got != 1 {
		t.Errorf("a commit that staged nothing set the transaction's clock to %d", got)
	}

	// A member that names itself must belong to the group at that generation; one that
	// does not, or whose generation is negative, is a simple commit.
	fence := b.groupFence("g", "m", nil, 3)
	if fence == 0 {
		t.Fatal("a member of a group that does not exist passes the fence")
	}
	for _, tc := range []struct {
		name   string
		gen    int32
		member string
		want   int16
	}{
		{"a member at generation 3", 3, "m", fence},
		{"a member at generation 0", 0, "m", b.groupFence("g", "m", nil, 0)},
		{"no member at generation 3", 3, "", 0},
		{"a member at generation -1", -1, "m", 0},
	} {
		got := code0(txnOffsetCommit(t, b, "tx2", "g", pid, epoch, tc.gen, tc.member, map[string][]int32{"u": {0}}, 1))
		if !slices.Equal(got, []int16{tc.want}) {
			t.Errorf("%s: %v, want %d", tc.name, got, tc.want)
		}
	}
	if b.groupFence("g", "m", nil, 0) == 0 {
		t.Error("generation 0 is a generation: a member of a group that does not exist must not pass at it")
	}

	// Who may commit.
	for _, tc := range []struct {
		name  string
		id    string
		pid   int64
		epoch int16
		code  int16
	}{
		{"an id nobody owns", "nobody", pid, epoch, kerr.InvalidProducerIDMapping.Code},
		{"another producer id", "tx2", pid + 1, epoch, kerr.InvalidProducerIDMapping.Code},
		{"an older epoch", "tx2", pid, epoch - 1, kerr.ProducerFenced.Code},
	} {
		got := code0(txnOffsetCommit(t, b, tc.id, "g", tc.pid, tc.epoch, -1, "", map[string][]int32{"u": {0}}, 1))
		if !slices.Equal(got, []int16{tc.code}) {
			t.Errorf("%s: %v, want %d", tc.name, got, tc.code)
		}
	}
	denyUnlessAllowed(b)
	if got := code0(txnOffsetCommit(t, b, "tx2", "g", pid, epoch, -1, "", map[string][]int32{"u": {0}}, 1)); !slices.Equal(got, []int16{kerr.TransactionalIDAuthorizationFailed.Code}) {
		t.Errorf("no right on the id: %v", got)
	}
	denyUnlessAllowed(b, allowAnon(resTxnID, "tx2", opWrite))
	if got := code0(txnOffsetCommit(t, b, "tx2", "g", pid, epoch, -1, "", map[string][]int32{"u": {0}}, 1)); !slices.Equal(got, []int16{kerr.GroupAuthorizationFailed.Code}) {
		t.Errorf("no right on the group: %v", got)
	}
	denyUnlessAllowed(b, allowAnon(resTxnID, "tx2", opWrite), allowAnon(resGroup, "g", opRead))
	if got := code0(txnOffsetCommit(t, b, "tx2", "g", pid, epoch, -1, "", map[string][]int32{"u": {0}}, 1)); !slices.Equal(got, []int16{kerr.TopicAuthorizationFailed.Code}) {
		t.Errorf("no right on the topic: %v", got)
	}
}

// The group fence of a transactional offset commit is the group's own: the id must be
// a group id, and whichever protocol holds the group answers for it.
func TestGroupFenceAsksTheProtocolThatHoldsTheGroup(t *testing.T) {
	b, _, _, _ := newTxnRig(t)
	for _, tc := range []struct {
		name   string
		group  string
		member string
		gen    int32
		want   int16
	}{
		{"no group id", "", "m", 3, kerr.InvalidGroupID.Code},
		{"a NUL in the id", "g\x00", "m", 3, kerr.InvalidGroupID.Code},
		{"a classic group that does not exist, a generation", "gc", "m", 3, kerr.IllegalGeneration.Code},
		{"a classic group, a simple commit", "gc", "", -1, 0},
	} {
		if got := b.groupFence(tc.group, tc.member, nil, tc.gen); got != tc.want {
			t.Errorf("%s: %d, want %d", tc.name, got, tc.want)
		}
	}
	if !b.claims.claim("gk", protoConsumer) {
		t.Fatal("claiming a group id for the consumer protocol")
	}
	if got := b.groupFence("gk", "m", nil, 3); got != kerr.UnknownMemberID.Code {
		t.Errorf("a consumer-protocol group with no such member: %d, want UNKNOWN_MEMBER_ID", got)
	}
}

func TestDescribeAndListTransactions(t *testing.T) {
	b, _, _, _ := newTxnRig(t)
	pids := map[string]int64{}
	for _, id := range []string{"c", "a", "b"} {
		pids[id], _ = owner(t, b, id)
	}
	b.txns.getOrCreate("blank") // an entry that was never initialised is not a transaction.
	set := func(id, state string, started int64, parts ...meta.TxnPartition) {
		e := b.txns.get(id)
		e.mu.Lock()
		e.rec.State, e.rec.StartedMs, e.rec.Partitions = state, started, parts
		e.mu.Unlock()
	}
	set("a", txnOngoing, 111, meta.TxnPartition{Topic: "t", Partition: 0})
	set("b", txnPrepareCommit, 222)
	set("c", txnCompleteAbort, 333)

	describe := func(ids ...string) []kmsg.DescribeTransactionsResponseTransactionState {
		req := kmsg.NewPtrDescribeTransactionsRequest()
		req.TransactionalIDs = ids
		return callAPI(t, b.handleDescribeTransactions, req, 0).TransactionStates
	}
	got := describe("a", "b", "c", "missing")
	if len(got) != 4 {
		t.Fatalf("%d answers", len(got))
	}
	for i, want := range []struct {
		state   string
		started int64
		code    int16
	}{{txnOngoing, 111, 0}, {txnPrepareCommit, 222, 0}, {txnCompleteAbort, -1, 0}, {"", 0, kerr.TransactionalIDNotFound.Code}} {
		if got[i].ErrorCode != want.code || got[i].State != want.state || (want.code == 0 && got[i].StartTimestamp != want.started) {
			t.Errorf("%s: code %d state %q started %d, want code %d state %q started %d", got[i].TransactionalID, got[i].ErrorCode, got[i].State, got[i].StartTimestamp, want.code, want.state, want.started)
		}
	}
	if len(got[0].Topics) != 1 || got[0].Topics[0].Topic != "t" || !slices.Equal(got[0].Topics[0].Partitions, []int32{0}) {
		t.Errorf("a's partitions: %+v", got[0].Topics)
	}
	for _, state := range []string{txnPrepareAbort, txnEmpty, txnCompleteCommit} {
		set("a", state, 444)
		started := describe("a")[0].StartTimestamp
		want := int64(-1)
		if state == txnPrepareAbort {
			want = 444
		}
		if started != want {
			t.Errorf("a in %s: started %d, want %d", state, started, want)
		}
	}
	if p, e := describe("a")[0], b.txns.get("a").rec; p.ProducerID != e.ProducerID || int32(p.ProducerEpoch) != int32(e.Epoch) || p.TimeoutMillis != e.TimeoutMs {
		t.Errorf("a's owner: %+v", p)
	}

	set("a", txnOngoing, 111)
	list := func(states []string, pids []int64) []string {
		req := kmsg.NewPtrListTransactionsRequest()
		req.StateFilters, req.ProducerIDFilters = states, pids
		var out []string
		for _, r := range callAPI(t, b.handleListTransactions, req, 0).TransactionStates {
			out = append(out, r.TransactionalID+":"+r.TransactionState)
		}
		return out
	}
	if got := list(nil, nil); !slices.Equal(got, []string{"a:Ongoing", "b:PrepareCommit", "c:CompleteAbort"}) {
		t.Errorf("all: %v, want the three in order and not the blank one", got)
	}
	if got := list([]string{txnOngoing, txnCompleteAbort}, nil); !slices.Equal(got, []string{"a:Ongoing", "c:CompleteAbort"}) {
		t.Errorf("by state: %v", got)
	}
	if got := list(nil, []int64{pids["b"]}); !slices.Equal(got, []string{"b:PrepareCommit"}) {
		t.Errorf("by producer id: %v", got)
	}
	if got := list([]string{txnOngoing}, []int64{pids["b"]}); len(got) != 0 {
		t.Errorf("by state and producer id that disagree: %v", got)
	}
	// Only what the caller may describe.
	denyUnlessAllowed(b, allowAnon(resTxnID, "b", opDescribe))
	if got := list(nil, nil); !slices.Equal(got, []string{"b:PrepareCommit"}) {
		t.Errorf("with a right on b alone: %v", got)
	}
	if got := describe("a", "b"); got[0].ErrorCode != kerr.TransactionalIDAuthorizationFailed.Code || got[1].ErrorCode != 0 {
		t.Errorf("describe with a right on b alone: %d and %d", got[0].ErrorCode, got[1].ErrorCode)
	}
}

func TestDescribeProducersListsThemInOrderWithTheirOpenTransaction(t *testing.T) {
	b, _, _, _ := newTxnRig(t)
	topicWithPartitions(t, b, "u", 1)
	for _, pid := range []int64{30, 10, 20} {
		if p := produceTo(t, b, "t", 0, 1, idemBatch(pid, 2, 0, 3, 0)); p.ErrorCode != 0 {
			t.Fatalf("produce %d: %d", pid, p.ErrorCode)
		}
	}
	// A transaction open on the partition: its producer's first offset is named.
	tpid, tepoch := owner(t, b, "tx")
	if ps := addPartitions(t, b, "tx", tpid, tepoch, "t", 0); ps[0].ErrorCode != 0 {
		t.Fatal(ps[0].ErrorCode)
	}
	id := "tx"
	if p := produceTxn(t, b, &id, "t", 0, idemBatch(tpid, tepoch, 0, 1, 0x10)); p.ErrorCode != 0 || p.BaseOffset != 9 {
		t.Fatalf("transactional produce: %+v", p)
	}

	describe := func(topic string, parts ...int32) []kmsg.DescribeProducersResponseTopicPartition {
		req := kmsg.NewPtrDescribeProducersRequest()
		rt := kmsg.NewDescribeProducersRequestTopic()
		rt.Topic, rt.Partitions = topic, parts
		req.Topics = append(req.Topics, rt)
		return callAPI(t, b.handleDescribeProducers, req, 0).Topics[0].Partitions
	}
	got := describe("t", 0, 1, -1)
	if got[0].ErrorCode != 0 || got[1].ErrorCode != kerr.UnknownTopicOrPartition.Code || got[2].ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Fatalf("partitions 0, 1, -1: %d %d %d", got[0].ErrorCode, got[1].ErrorCode, got[2].ErrorCode)
	}
	var ids []int64
	var summary []string
	for _, a := range got[0].ActiveProducers {
		ids = append(ids, a.ProducerID)
		summary = append(summary, fmt.Sprintf("%d/%d/%d/%d", a.ProducerID, a.ProducerEpoch, a.LastSequence, a.CurrentTxnStartOffset))
	}
	if !slices.IsSorted(ids) || len(ids) != 4 {
		t.Errorf("producers %v, want four in ascending order", ids)
	}
	if want := []string{"10/2/2/-1", "20/2/2/-1", "30/2/2/-1"}; !slices.Equal(summary[:3], want) && !strings.Contains(strings.Join(summary, " "), "10/2/2/-1 20/2/2/-1 30/2/2/-1") {
		t.Errorf("producers %v, want 10, 20, 30 at epoch 2, last sequence 2, no open transaction", summary)
	}
	for _, a := range got[0].ActiveProducers {
		if a.ProducerID == tpid && (a.CurrentTxnStartOffset != 9 || a.LastSequence != 0) {
			t.Errorf("the transactional producer: %+v, want its transaction opened at offset 9 and last sequence 0", a)
		}
	}
	// After a restart the tracker is empty: what the log holds is read back.
	b.idem = newIdempotency()
	if got := describe("t", 0)[0].ActiveProducers; len(got) != 4 {
		t.Errorf("after a restart: %d producers, want the 4 the log holds", len(got))
	}
	denyUnlessAllowed(b)
	if got := describe("t", 0); got[0].ErrorCode != kerr.TopicAuthorizationFailed.Code {
		t.Errorf("no right on the topic: %d", got[0].ErrorCode)
	}
}

// A produce of a transactional batch is held to the transaction: who owns the id,
// that one is open, and that the partition is in it.
func TestTransactionalProduceIsHeldToTheTransaction(t *testing.T) {
	b, _, _, _ := newTxnRig(t)
	topicWithPartitions(t, b, "u", 1)
	pid, epoch := owner(t, b, "tx")
	if ps := addPartitions(t, b, "tx", pid, epoch, "t", 0); ps[0].ErrorCode != 0 {
		t.Fatal(ps[0].ErrorCode)
	}
	id, other, empty := "tx", "nobody", ""
	for _, tc := range []struct {
		name    string
		txn     *string
		topic   string
		pid     int64
		epoch   int16
		code    int16
		message bool
	}{
		{"no transactional id", nil, "t", pid, epoch, kerr.TransactionalIDAuthorizationFailed.Code, false},
		{"an empty transactional id", &empty, "t", pid, epoch, kerr.InvalidTxnState.Code, true},
		{"an id nobody owns", &other, "t", pid, epoch, kerr.InvalidProducerIDMapping.Code, true},
		{"another producer id", &id, "t", pid + 1, epoch, kerr.InvalidProducerIDMapping.Code, true},
		{"an older epoch", &id, "t", pid, epoch - 1, kerr.InvalidProducerEpoch.Code, true},
		{"a newer epoch", &id, "t", pid, epoch + 1, kerr.InvalidProducerEpoch.Code, true},
		{"a partition not in the transaction", &id, "u", pid, epoch, kerr.InvalidTxnState.Code, true},
	} {
		p := produceTxn(t, b, tc.txn, tc.topic, 0, idemBatch(tc.pid, tc.epoch, 0, 1, 0x10))
		if p.ErrorCode != tc.code || p.BaseOffset != -1 || (p.ErrorMessage != nil) != tc.message {
			t.Errorf("%s: code %d base offset %d message %v, want code %d, -1 and message=%v", tc.name, p.ErrorCode, p.BaseOffset, p.ErrorMessage, tc.code, tc.message)
		}
	}
	if p := produceTxn(t, b, &id, "t", 0, idemBatch(pid, epoch, 0, 1, 0x10)); p.ErrorCode != 0 || p.BaseOffset != 0 {
		t.Errorf("the owner, on a partition in the transaction: %+v", p)
	}
	// Once it is ended nothing more may be written to it.
	if code := endTxnCall(t, b, "tx", pid, epoch, true); code != 0 {
		t.Fatalf("EndTxn: %d", code)
	}
	if p := produceTxn(t, b, &id, "t", 0, idemBatch(pid, epoch, 1, 1, 0x10)); p.ErrorCode != kerr.InvalidTxnState.Code || p.BaseOffset != -1 {
		t.Errorf("after the end: %+v, want INVALID_TXN_STATE", p)
	}
	// The id needs Write, apart from the topic.
	denyUnlessAllowed(b, allowAnon(resTopic, "t", opWrite), allowAnon(resCluster, clusterResourceName, opIdempotent))
	p := produceTxn(t, b, &id, "t", 0, idemBatch(pid, epoch, 1, 1, 0x10))
	if p.ErrorCode != kerr.TransactionalIDAuthorizationFailed.Code || p.BaseOffset != -1 {
		t.Errorf("no right on the id: %+v, want TRANSACTIONAL_ID_AUTHORIZATION_FAILED and -1", p)
	}
}

// A restart finishes what was decided and leaves open what was open, with its timeout
// counted from now.
func TestRecoverTransactionsFinishesWhatWasDecidedAndKeepsWhatIsOpen(t *testing.T) {
	b, _, failSync, _ := newTxnRig(t)
	for _, id := range []string{"commit", "abort", "open", "done"} {
		owner(t, b, id)
	}
	setState := func(id, state string, updated int64, offsets ...meta.TxnOffset) {
		e := b.txns.get(id)
		e.mu.Lock()
		e.rec.State, e.rec.UpdatedMs, e.rec.Offsets = state, updated, offsets
		e.rec.Partitions = []meta.TxnPartition{{Topic: "t", Partition: 0}}
		e.mu.Unlock()
	}
	setState("commit", txnPrepareCommit, 1, meta.TxnOffset{Group: "rg", Topic: "t", Partition: 0, Offset: 4})
	setState("abort", txnPrepareAbort, 1, meta.TxnOffset{Group: "ra", Topic: "t", Partition: 0, Offset: 5})
	setState("open", txnOngoing, 1)
	setState("done", txnCompleteCommit, 1)

	// With the log unable to flush a decision stands.
	dirty(t, b)
	*failSync = true
	b.RecoverTransactions()
	for id, want := range map[string]string{"commit": txnPrepareCommit, "abort": txnPrepareAbort} {
		if got := b.txns.get(id).rec.State; got != want {
			t.Errorf("%s with the flush failing: %s, want %s to stand", id, got, want)
		}
	}
	*failSync = false

	before := nowMs()
	b.RecoverTransactions()
	for id, want := range map[string]string{"commit": txnCompleteCommit, "abort": txnCompleteAbort, "open": txnOngoing, "done": txnCompleteCommit} {
		if got := b.txns.get(id).rec.State; got != want {
			t.Errorf("%s after recovery: %s, want %s", id, got, want)
		}
	}
	if off, ok, _ := b.meta.FetchOffset("rg", "t", 0); !ok || off != 4 {
		t.Errorf("the decided commit's offset: %d %v, want 4", off, ok)
	}
	if _, ok, _ := b.meta.FetchOffset("ra", "t", 0); ok {
		t.Error("the decided abort's offset was committed")
	}
	if got := b.txns.get("open").rec.UpdatedMs; got < before {
		t.Errorf("an open transaction's clock %d was not restarted (now %d)", got, before)
	}
	if got := b.txns.get("done").rec.UpdatedMs; got != 1 {
		t.Errorf("a finished transaction was touched: updated %d, want 1", got)
	}
}

// The coordinator aborts a transaction that outlives its timeout, fences its producer,
// carries out a decision that could not be carried out, and leaves alone one that is
// within its time.
func TestRunTransactionsAbortsTheExpiredAndFinishesTheDecided(t *testing.T) {
	b, _, _, _ := newTxnRig(t)
	pids := map[string]int64{}
	epochs := map[string]int16{}
	for _, id := range []string{"expired", "fresh", "decided-commit", "decided-abort"} {
		pids[id], epochs[id] = owner(t, b, id)
	}
	setState := func(id, state string, updated int64, timeoutMs int32) {
		e := b.txns.get(id)
		e.mu.Lock()
		e.rec.State, e.rec.UpdatedMs, e.rec.TimeoutMs = state, updated, timeoutMs
		e.rec.Partitions = []meta.TxnPartition{{Topic: "t", Partition: 0}}
		e.mu.Unlock()
	}
	now := nowMs()
	setState("expired", txnOngoing, now-10_000, 1000)
	setState("fresh", txnOngoing, now, 600_000)
	setState("decided-commit", txnPrepareCommit, now, 600_000)
	setState("decided-abort", txnPrepareAbort, now, 600_000)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.RunTransactions(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	state := func(id string) (string, int16) {
		e := b.txns.get(id)
		e.mu.RLock()
		defer e.mu.RUnlock()
		return e.rec.State, e.rec.Epoch
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s1, _ := state("expired")
		s2, _ := state("decided-commit")
		s3, _ := state("decided-abort")
		if s1 == txnCompleteAbort && s2 == txnCompleteCommit && s3 == txnCompleteAbort {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if s, e := state("expired"); s != txnCompleteAbort || e != epochs["expired"]+1 {
		t.Errorf("the expired transaction: %s at epoch %d, want CompleteAbort at epoch %d", s, e, epochs["expired"]+1)
	}
	if s, _ := state("decided-commit"); s != txnCompleteCommit {
		t.Errorf("the decided commit: %s", s)
	}
	if s, _ := state("decided-abort"); s != txnCompleteAbort {
		t.Errorf("the decided abort: %s", s)
	}
	// By now the loop has run at least twice more.
	time.Sleep(2 * txnCheckEvery)
	if s, e := state("fresh"); s != txnOngoing || e != epochs["fresh"] {
		t.Errorf("a transaction within its timeout: %s at epoch %d, want Ongoing at epoch %d", s, e, epochs["fresh"])
	}
}

// logSink collects what a broker logs.
type logSink struct {
	mu sync.Mutex
	sb strings.Builder
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sb.Write(p)
}

func (s *logSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sb.String()
}

// captureLogs makes the broker log, at every level, to the sink returned.
func captureLogs(b *Broker) *logSink {
	s := &logSink{}
	b.log = slog.New(slog.NewTextHandler(s, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return s
}

// A decision that cannot be carried out is logged when it first fails and again only
// when the reason changes: the coordinator retries twice a second.
func TestAFailedFinishIsLoggedOnceNotOnEveryAttempt(t *testing.T) {
	b, _, failSync, _ := newTxnRig(t)
	logs := captureLogs(b)
	pid, epoch := owner(t, b, "tx")
	e := b.txns.get("tx")
	e.mu.Lock()
	e.rec.State = txnOngoing
	e.rec.Partitions = []meta.TxnPartition{{Topic: "t", Partition: 0}}
	e.mu.Unlock()
	dirty(t, b)
	*failSync = true
	for range 3 {
		if code := endTxnCall(t, b, "tx", pid, epoch, true); code != kerr.ConcurrentTransactions.Code {
			t.Fatalf("EndTxn: %d", code)
		}
	}
	const line = "could not flush a transaction marker"
	if n := strings.Count(logs.String(), line); n != 1 {
		t.Errorf("the failure was logged %d times in 3 attempts, want once:\n%s", n, logs.String())
	}
	*failSync = false
	if code := endTxnCall(t, b, "tx", pid, epoch, true); code != 0 {
		t.Fatalf("EndTxn once the flush works: %d", code)
	}
	// Once it has worked a new failure is news again.
	e.mu.Lock()
	e.rec.State = txnOngoing
	e.rec.Partitions = []meta.TxnPartition{{Topic: "t", Partition: 0}}
	e.mu.Unlock()
	dirty(t, b)
	*failSync = true
	endTxnCall(t, b, "tx", pid, epoch, true)
	if n := strings.Count(logs.String(), line); n != 2 {
		t.Errorf("a failure after a success was logged %d times in all, want 2", n)
	}
}

func TestRecoveryLogsWhatItCouldNotFinishAndWhatItDid(t *testing.T) {
	b, _, failSync, _ := newTxnRig(t)
	logs := captureLogs(b)
	owner(t, b, "tx")
	e := b.txns.get("tx")
	set := func(state string) {
		e.mu.Lock()
		e.rec.State = state
		e.rec.Partitions = []meta.TxnPartition{{Topic: "t", Partition: 0}}
		e.mu.Unlock()
	}
	const failed, finished = "could not finish a transaction left from before the restart", "finished after a restart"

	set(txnPrepareCommit)
	dirty(t, b)
	*failSync = true
	b.RecoverTransactions()
	if s := logs.String(); !strings.Contains(s, failed) || strings.Contains(s, finished) {
		t.Errorf("a decision that could not be finished:\n%s", s)
	}
	*failSync = false
	b.RecoverTransactions()
	if s := logs.String(); !strings.Contains(s, finished) {
		t.Errorf("a decision that was finished:\n%s", s)
	}
	for _, state := range []string{txnOngoing, txnEmpty} {
		set(state)
		before := logs.String()
		b.RecoverTransactions()
		if got := strings.TrimPrefix(logs.String(), before); strings.Contains(got, failed) || strings.Contains(got, finished) {
			t.Errorf("a transaction left %s was logged as finished or failed:\n%s", state, got)
		}
	}
	// A decided abort is finished and logged as the commit was.
	set(txnPrepareAbort)
	before := logs.String()
	b.RecoverTransactions()
	if got := strings.TrimPrefix(logs.String(), before); !strings.Contains(got, finished) || !strings.Contains(got, "CompleteAbort") {
		t.Errorf("a decided abort that was finished:\n%s", got)
	}
}

func TestATimedOutTransactionThatCannotBeAbortedIsLogged(t *testing.T) {
	b, rig, _, _ := newTxnRig(t)
	logs := captureLogs(b)
	owner(t, b, "tx")
	e := b.txns.get("tx")
	e.mu.Lock()
	e.rec.State, e.rec.UpdatedMs, e.rec.TimeoutMs = txnOngoing, nowMs()-10_000, 1000
	e.mu.Unlock()
	rig.putErr = errStore

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.RunTransactions(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(logs.String(), "could not abort a timed-out transaction") {
		time.Sleep(50 * time.Millisecond)
	}
	if s := logs.String(); !strings.Contains(s, "aborting a transaction that timed out") || !strings.Contains(s, "could not abort a timed-out transaction") {
		t.Errorf("the timed-out transaction's abort failed without a log:\n%s", s)
	}
	e.mu.RLock()
	state := e.rec.State
	e.mu.RUnlock()
	if state != txnOngoing {
		t.Errorf("state %s after an abort that could not be stored, want Ongoing", state)
	}
}
