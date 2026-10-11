package protocol

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// batchOf is a record batch's header with a record count and padding: all the
// summariser reads of it.
func batchOf(records int) []byte {
	b := make([]byte, 61+20)
	binary.BigEndian.PutUint32(b[8:], uint32(len(b)-12))
	binary.BigEndian.PutUint32(b[57:], uint32(records))
	return b
}

func str(s string) *string { return &s }

// The summariser is what every column of the Traffic page is built from. Each row says
// what a request names, so a change to one cannot silently turn a transactional id back
// into a group (TRF-10, TRF-17).
func TestSummarisersReportWhatTheRequestNames(t *testing.T) {
	produce := func(txn *string, topic string, parts ...int32) *kmsg.ProduceRequest {
		q := kmsg.NewPtrProduceRequest()
		q.Acks, q.TransactionID = -1, txn
		tp := kmsg.NewProduceRequestTopic()
		tp.Topic = topic
		for _, p := range parts {
			pp := kmsg.NewProduceRequestTopicPartition()
			pp.Partition, pp.Records = p, batchOf(1)
			tp.Partitions = append(tp.Partitions, pp)
		}
		q.Topics = append(q.Topics, tp)
		return q
	}
	fetch := func(topic string, part int32, from int64) (*kmsg.FetchRequest, *kmsg.FetchResponse) {
		q := kmsg.NewPtrFetchRequest()
		q.SetVersion(12)
		q.MaxWaitMillis = 500
		tp := kmsg.NewFetchRequestTopic()
		tp.Topic = topic
		pp := kmsg.NewFetchRequestTopicPartition()
		pp.Partition, pp.FetchOffset = part, from
		tp.Partitions = append(tp.Partitions, pp)
		q.Topics = append(q.Topics, tp)
		return q, kmsg.NewPtrFetchResponse()
	}
	fq, fr := fetch("orders", 0, 4812)
	rt := kmsg.NewFetchResponseTopic()
	rp := kmsg.NewFetchResponseTopicPartition()
	rp.RecordBatches = append(batchOf(2), batchOf(1)...)
	rt.Partitions = append(rt.Partitions, rp)
	fr.Topics = append(fr.Topics, rt)
	eq, _ := fetch("orders", 0, 7)

	jr := kmsg.NewPtrJoinGroupResponse()
	jr.Generation = 7
	jr.Members = []kmsg.JoinGroupResponseMember{kmsg.NewJoinGroupResponseMember(), kmsg.NewJoinGroupResponseMember(), kmsg.NewJoinGroupResponseMember()}
	jmr := kmsg.NewPtrJoinGroupResponse()
	jmr.ErrorCode = kerr.MemberIDRequired.Code
	reason := "rebalance triggered"
	jq := kmsg.NewPtrJoinGroupRequest()
	jq.Group = "g-depot"
	jqr := kmsg.NewPtrJoinGroupRequest()
	jqr.Group, jqr.Reason = "g-depot", &reason

	oc := kmsg.NewPtrOffsetCommitRequest()
	oc.Group, oc.Generation = "g-depot", 7
	for _, p := range []int32{0, 1, 2, 5} {
		ot := kmsg.NewOffsetCommitRequestTopic()
		ot.Topic = "dispatch"
		op := kmsg.NewOffsetCommitRequestTopicPartition()
		op.Partition = p
		ot.Partitions = append(ot.Partitions, op)
		if len(oc.Topics) == 0 {
			oc.Topics = append(oc.Topics, ot)
		} else {
			oc.Topics[0].Partitions = append(oc.Topics[0].Partitions, op)
		}
	}

	hb := kmsg.NewPtrHeartbeatRequest()
	hb.Group, hb.Generation = "g-depot", 7
	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.Generation = "g-depot", 7
	sync.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{kmsg.NewSyncGroupRequestGroupAssignment(), kmsg.NewSyncGroupRequestGroupAssignment()}

	fcGroup := kmsg.NewPtrFindCoordinatorRequest()
	fcGroup.CoordinatorKey, fcGroup.CoordinatorType = "g-depot", 0
	fcTxn := kmsg.NewPtrFindCoordinatorRequest()
	fcTxn.CoordinatorKey, fcTxn.CoordinatorType = "checkout-1", 1

	initTxn := kmsg.NewPtrInitProducerIDRequest()
	initTxn.TransactionalID, initTxn.ProducerID, initTxn.ProducerEpoch = str("checkout-1"), 1000, 4
	initResp := kmsg.NewPtrInitProducerIDResponse()
	initResp.ProducerID, initResp.ProducerEpoch = 1000, 5
	initPlain := kmsg.NewPtrInitProducerIDRequest()

	addParts := kmsg.NewPtrAddPartitionsToTxnRequest()
	addParts.SetVersion(3)
	addParts.TransactionalID = "checkout-1"
	addOffsets := kmsg.NewPtrAddOffsetsToTxnRequest()
	addOffsets.TransactionalID, addOffsets.Group = "checkout-1", "g-depot"
	end := kmsg.NewPtrEndTxnRequest()
	end.TransactionalID, end.Commit = "checkout-1", true
	txnCommit := kmsg.NewPtrTxnOffsetCommitRequest()
	txnCommit.TransactionalID, txnCommit.Group = "checkout-1", "g-depot"
	descTxn := kmsg.NewPtrDescribeTransactionsRequest()
	descTxn.TransactionalIDs = []string{"checkout-1"}

	meta := kmsg.NewPtrMetadataRequest()
	meta.Topics = []kmsg.MetadataRequestTopic{{Topic: str("a")}, {Topic: str("b")}}
	meta.AllowAutoTopicCreation = true
	metaAll := kmsg.NewPtrMetadataRequest()
	metaAll.Topics = nil

	av := kmsg.NewPtrApiVersionsRequest()
	av.ClientSoftwareName, av.ClientSoftwareVersion = "librdkafka", "2.5.3"

	for _, c := range []struct {
		name string
		q    kmsg.Request
		r    kmsg.Response
		want Summary
	}{
		{"produce names the topic and the partition", produce(nil, "orders", 1), nil,
			Summary{Topics: []string{"orders"}, Detail: "p1 · 1 batch · 81 B"}},
		{"a transactional produce names the transaction, not a group", produce(str("checkout-1"), "orders", 0, 1, 2), nil,
			Summary{Topics: []string{"orders"}, TxnID: "checkout-1", Detail: "p0–p2 · 3 batches · 243 B"}},
		{"a fetch with data says where it began and how many records", fq, fr,
			Summary{Topics: []string{"orders"}, Detail: "p0 from 4,812 · 3 records"}},
		{"a fetch with nothing is idle and says how long it waited", eq, kmsg.NewPtrFetchResponse(),
			Summary{Topics: []string{"orders"}, Idle: true, Detail: "empty, waiting up to 500 ms"}},
		{"an offset commit names the group, the topic and the partitions", oc, nil,
			Summary{Group: "g-depot", Topics: []string{"dispatch"}, Detail: "p0–p2, p5 · generation 7"}},
		{"a first join is given an id to join with", jq, jmr,
			Summary{Group: "g-depot", Detail: "a new member asks in; it is given an id to join with"}},
		{"a join answered to the leader counts the members", jq, jr,
			Summary{Group: "g-depot", Detail: "generation 7 · 3 members, leader"}},
		{"a join says its reason", jqr, kmsg.NewPtrJoinGroupResponse(),
			Summary{Group: "g-depot", Detail: "rebalance triggered"}},
		{"a heartbeat is idle and names its generation", hb, nil,
			Summary{Group: "g-depot", Idle: true, Detail: "generation 7"}},
		{"a sync from the leader says whom it hands partitions to", sync, nil,
			Summary{Group: "g-depot", Detail: "generation 7 · hands out partitions to 2 members"}},
		{"a coordinator lookup for a group", fcGroup, nil, Summary{Group: "g-depot", Detail: "group coordinator"}},
		{"a coordinator lookup for a transaction is not a group", fcTxn, nil, Summary{TxnID: "checkout-1", Detail: "transaction coordinator"}},
		{"a transactional producer id is a transaction, with its epoch move", initTxn, initResp,
			Summary{TxnID: "checkout-1", Detail: "transactional · epoch 4 → 5"}},
		{"an idempotent producer has no transaction", initPlain, nil, Summary{Detail: "idempotent producer"}},
		{"adding partitions to a transaction", addParts, nil, Summary{TxnID: "checkout-1"}},
		{"adding offsets names the transaction and the group apart", addOffsets, nil,
			Summary{Group: "g-depot", TxnID: "checkout-1", Detail: "adds the group's offsets to the transaction"}},
		{"ending a transaction", end, nil, Summary{TxnID: "checkout-1", Detail: "commit"}},
		{"committing offsets in a transaction", txnCommit, nil,
			Summary{Group: "g-depot", TxnID: "checkout-1", Detail: "offsets inside the transaction"}},
		{"describing a transaction", descTxn, nil, Summary{TxnID: "checkout-1"}},
		{"metadata that may create", meta, nil, Summary{Topics: []string{"a", "b"}, Detail: "auto-create on"}},
		{"metadata for everything", metaAll, nil, Summary{Detail: "all topics"}},
		{"version negotiation says the software", av, nil, Summary{Software: "librdkafka 2.5.3", Detail: "librdkafka 2.5.3"}},
	} {
		key := kmsg.Key(c.q.Key())
		var got Summary
		for _, a := range APIs {
			if a.Key == key {
				got = a.Summary(c.q, c.r)
			}
		}
		got.TopicIDs = nil
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n got  %+v\n want %+v", c.name, got, c.want)
		}
	}
}

func TestKindOfEveryAPIIsOneOfTheBoardsChips(t *testing.T) {
	for _, a := range APIs {
		switch k := Kind(a.Key); k {
		case "", "produce", "fetch", "groups", "admin":
		default:
			t.Errorf("%s is kind %q", APIName(a.Key), k)
		}
	}
	for api, want := range map[APIKey]string{kmsg.Produce: "produce", kmsg.Fetch: "fetch", kmsg.Heartbeat: "groups",
		kmsg.OffsetCommit: "groups", kmsg.CreateTopics: "admin", kmsg.Metadata: "", kmsg.ApiVersions: ""} {
		if got := Kind(api); got != want {
			t.Errorf("%s is kind %q, want %q", APIName(api), got, want)
		}
	}
}
