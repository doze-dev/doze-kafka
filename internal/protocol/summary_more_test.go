package protocol

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The summarisers the first table did not reach: the admin and transaction requests
// that name topics, the requests whose shape changes with the version (DeleteTopics,
// AddPartitionsToTxn, FindCoordinator), and what a request across several topics
// says where one topic's partition numbers would mislead. Every column of the Traffic
// page is one of these fields.
func TestSummarisersNameWhatAdminAndTransactionRequestsAreAbout(t *testing.T) {
	deleteRecords := kmsg.NewPtrDeleteRecordsRequest()
	for _, name := range []string{"orders", "payments", "orders"} { // named twice: listed once.
		tp := kmsg.NewDeleteRecordsRequestTopic()
		tp.Topic = name
		deleteRecords.Topics = append(deleteRecords.Topics, tp)
	}

	createPartitions := kmsg.NewPtrCreatePartitionsRequest()
	cp := kmsg.NewCreatePartitionsRequestTopic()
	cp.Topic, cp.Count = "orders", 6
	createPartitions.Topics = append(createPartitions.Topics, cp)

	offsetDelete := kmsg.NewPtrOffsetDeleteRequest()
	offsetDelete.Group = "g-depot"
	od := kmsg.NewOffsetDeleteRequestTopic()
	od.Topic = "dispatch"
	offsetDelete.Topics = append(offsetDelete.Topics, od)

	describeProducers := kmsg.NewPtrDescribeProducersRequest()
	dp := kmsg.NewDescribeProducersRequestTopic()
	dp.Topic = "orders"
	describeProducers.Topics = append(describeProducers.Topics, dp)

	txnCommit := kmsg.NewPtrTxnOffsetCommitRequest()
	txnCommit.TransactionalID, txnCommit.Group = "checkout-1", "g-depot"
	tc := kmsg.NewTxnOffsetCommitRequestTopic()
	tc.Topic = "dispatch"
	txnCommit.Topics = append(txnCommit.Topics, tc)

	// To v3 one transaction's topics are at the top; from v4 a request is a list of
	// transactions, and the row is about the first.
	addV3 := kmsg.NewPtrAddPartitionsToTxnRequest()
	addV3.SetVersion(3)
	addV3.TransactionalID = "checkout-1"
	at := kmsg.NewAddPartitionsToTxnRequestTopic()
	at.Topic = "orders"
	addV3.Topics = append(addV3.Topics, at)
	addV4 := kmsg.NewPtrAddPartitionsToTxnRequest()
	addV4.SetVersion(4)
	for _, tx := range []struct {
		id     string
		topics []string
	}{{"checkout-1", []string{"orders", "payments"}}, {"checkout-2", []string{"payments", "ledger"}}} {
		x := kmsg.NewAddPartitionsToTxnRequestTransaction()
		x.TransactionalID = tx.id
		for _, name := range tx.topics {
			xt := kmsg.NewAddPartitionsToTxnRequestTransactionTopic()
			xt.Topic = name
			x.Topics = append(x.Topics, xt)
		}
		addV4.Transactions = append(addV4.Transactions, x)
	}

	// DeleteTopics names its topics three ways: a list of names (to v5), and from v6 a
	// list of entries that hold a name or an id.
	deleteByName := kmsg.NewPtrDeleteTopicsRequest()
	deleteByName.TopicNames = []string{"old-orders", "old-payments"}
	deleteV6 := kmsg.NewPtrDeleteTopicsRequest()
	deleteV6.SetVersion(6)
	byName := kmsg.NewDeleteTopicsRequestTopic()
	byName.Topic = str("old-orders")
	byID := kmsg.NewDeleteTopicsRequestTopic()
	byID.TopicID = [16]byte{7, 7}
	neither := kmsg.NewDeleteTopicsRequestTopic() // no name and a zero id: nothing to name.
	deleteV6.Topics = append(deleteV6.Topics, byName, byID, neither)

	// A commit and a fetch across two topics: "p0, p0" would say nothing true.
	commitTwo := kmsg.NewPtrOffsetCommitRequest()
	commitTwo.Group = "g-depot"
	fetchTwo, fetchedTwo := kmsg.NewPtrFetchRequest(), kmsg.NewPtrFetchResponse()
	fetchTwo.SetVersion(12)
	for _, name := range []string{"orders", "payments"} {
		ct := kmsg.NewOffsetCommitRequestTopic()
		ct.Topic = name
		ct.Partitions = append(ct.Partitions, kmsg.NewOffsetCommitRequestTopicPartition())
		commitTwo.Topics = append(commitTwo.Topics, ct)

		ft, rt := kmsg.NewFetchRequestTopic(), kmsg.NewFetchResponseTopic()
		ft.Topic, rt.Topic = name, name
		ft.Partitions = append(ft.Partitions, kmsg.NewFetchRequestTopicPartition())
		rp := kmsg.NewFetchResponseTopicPartition()
		rp.RecordBatches = batchOf(2)
		rt.Partitions = append(rt.Partitions, rp)
		fetchTwo.Topics, fetchedTwo.Topics = append(fetchTwo.Topics, ft), append(fetchedTwo.Topics, rt)
	}

	// FindCoordinator v4 asks for several keys at once; the row names the first.
	findMany := kmsg.NewPtrFindCoordinatorRequest()
	findMany.SetVersion(4)
	findMany.CoordinatorKeys = []string{"g-depot", "g-billing"}

	subscribeNew := kmsg.NewPtrGetTelemetrySubscriptionsRequest()
	subscribeAgain := kmsg.NewPtrGetTelemetrySubscriptionsRequest()
	subscribeAgain.ClientInstanceID = [16]byte{1}
	push := kmsg.NewPtrPushTelemetryRequest()
	push.Metrics = make([]byte, 2048)
	lastPush := kmsg.NewPtrPushTelemetryRequest()
	lastPush.Metrics, lastPush.Terminating = make([]byte, 100), true

	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group = "g-depot"
	abort := kmsg.NewPtrEndTxnRequest()
	abort.TransactionalID = "checkout-1"
	addOffsets := kmsg.NewPtrAddOffsetsToTxnRequest()
	addOffsets.TransactionalID, addOffsets.Group = "checkout-1", "g-depot"
	syncFollower := kmsg.NewPtrSyncGroupRequest() // a follower hands out nothing.
	syncFollower.Group, syncFollower.Generation = "g-depot", 7
	leaveEpoch := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	leaveEpoch.Group, leaveEpoch.MemberEpoch = "g-depot", -1

	for _, c := range []struct {
		name string
		key  APIKey
		q    kmsg.Request
		r    kmsg.Response
		want Summary
	}{
		{"DeleteRecords", kmsg.DeleteRecords, deleteRecords, nil, Summary{Topics: []string{"orders", "payments"}}},
		{"CreatePartitions", kmsg.CreatePartitions, createPartitions, nil, Summary{Topics: []string{"orders"}}},
		{"OffsetDelete", kmsg.OffsetDelete, offsetDelete, nil, Summary{Group: "g-depot", Topics: []string{"dispatch"}}},
		{"DescribeProducers", kmsg.DescribeProducers, describeProducers, nil, Summary{Topics: []string{"orders"}}},
		{"TxnOffsetCommit", kmsg.TxnOffsetCommit, txnCommit, nil,
			Summary{Group: "g-depot", TxnID: "checkout-1", Topics: []string{"dispatch"}, Detail: "offsets inside the transaction"}},
		{"AddPartitionsToTxn v3", kmsg.AddPartitionsToTxn, addV3, nil, Summary{TxnID: "checkout-1", Topics: []string{"orders"}}},
		{"AddPartitionsToTxn v4", kmsg.AddPartitionsToTxn, addV4, nil, Summary{TxnID: "checkout-1", Topics: []string{"orders", "payments", "ledger"}}},
		{"DeleteTopics by name", kmsg.DeleteTopics, deleteByName, nil, Summary{Topics: []string{"old-orders", "old-payments"}}},
		{"DeleteTopics v6", kmsg.DeleteTopics, deleteV6, nil, Summary{Topics: []string{"old-orders"}, TopicIDs: [][16]byte{{7, 7}}}},
		{"OffsetCommit of two topics", kmsg.OffsetCommit, commitTwo, nil,
			Summary{Group: "g-depot", Topics: []string{"orders", "payments"}, Detail: "2 partitions"}},
		{"Fetch of two topics", kmsg.Fetch, fetchTwo, fetchedTwo,
			Summary{Topics: []string{"orders", "payments"}, Detail: "2 partitions · 4 records · 162 B"}},
		{"FindCoordinator of several keys", kmsg.FindCoordinator, findMany, nil, Summary{Group: "g-depot", Detail: "group coordinator"}},
		{"GetTelemetrySubscriptions, new", kmsg.GetTelemetrySubscriptions, subscribeNew, nil, Summary{Detail: "subscribes (a new client instance)"}},
		{"GetTelemetrySubscriptions, again", kmsg.GetTelemetrySubscriptions, subscribeAgain, nil, Summary{Detail: "asks what to report"}},
		{"PushTelemetry", kmsg.PushTelemetry, push, nil, Summary{Detail: "2.0 KiB of metrics", Idle: true}},
		{"PushTelemetry, the last", kmsg.PushTelemetry, lastPush, nil, Summary{Detail: "100 B of metrics, client is closing"}},
		{"LeaveGroup", kmsg.LeaveGroup, leave, nil, Summary{Group: "g-depot"}},
		{"EndTxn, abort", kmsg.EndTxn, abort, nil, Summary{TxnID: "checkout-1", Detail: "abort"}},
		{"AddOffsetsToTxn", kmsg.AddOffsetsToTxn, addOffsets, nil,
			Summary{Group: "g-depot", TxnID: "checkout-1", Detail: "adds the group's offsets to the transaction"}},
		{"SyncGroup of a follower", kmsg.SyncGroup, syncFollower, nil, Summary{Group: "g-depot", Detail: "generation 7"}},
		{"ConsumerGroupHeartbeat, leaving", kmsg.ConsumerGroupHeartbeat, leaveEpoch, nil, Summary{Group: "g-depot", Detail: "leave"}},
	} {
		got := summariserFor(t, c.key)(c.q, c.r)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n got %+v\nwant %+v", c.name, got, c.want)
		}
	}
}

func summariserFor(t *testing.T, key APIKey) Summariser {
	t.Helper()
	for _, a := range APIs {
		if a.Key == key {
			return a.Summary
		}
	}
	t.Fatalf("API %d is not in the table", key)
	return nil
}

// The table pairs each API with its summariser by hand. A row that named the wrong
// one would hand a summariser another API's request: it must then say nothing, not
// panic and not describe a request as something it is not.
func TestASummariserHandedAnotherAPIsRequestSaysNothing(t *testing.T) {
	stranger := kmsg.NewPtrSASLHandshakeRequest() // no summariser is for this one.
	other := kmsg.NewPtrDescribeClusterRequest()
	for _, a := range APIs {
		for _, q := range []kmsg.Request{stranger, other} {
			if a.Key == APIKey(q.Key()) {
				continue
			}
			if got := a.Summary(q, q.ResponseKind()); !reflect.DeepEqual(got, Summary{}) {
				t.Errorf("%s, handed a %T, says %+v", APIName(a.Key), q, got)
			}
		}
	}
}

// firstError reads a response it may have been handed nothing of.
func TestFirstErrorOfNoResponse(t *testing.T) {
	if got := firstError(nil); got != 0 {
		t.Errorf("no response: %d", got)
	}
	var none *kmsg.ProduceResponse // a handler that failed before it made one.
	if got := firstError(none); got != 0 {
		t.Errorf("a nil response of a type: %d", got)
	}
}

// The search for an error is bounded by how much of a response it looks at, so that a
// response as large as a frame cannot keep a connection's goroutine walking it: within
// the bound an error is found wherever it is, and past it the answer is "none found",
// promptly.
func TestFirstErrorLooksAtABoundedAmountOfAResponse(t *testing.T) {
	big := func(partitions int, bad int) *kmsg.OffsetCommitResponse {
		resp := kmsg.NewPtrOffsetCommitResponse()
		tp := kmsg.NewOffsetCommitResponseTopic()
		tp.Topic = "orders"
		tp.Partitions = make([]kmsg.OffsetCommitResponseTopicPartition, partitions)
		tp.Partitions[bad].ErrorCode = kerr.UnknownTopicOrPartition.Code
		resp.Topics = append(resp.Topics, tp)
		return resp
	}
	if got := firstError(big(1000, 999)); got != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("the last of 1,000 partitions: %d, want %d", got, kerr.UnknownTopicOrPartition.Code)
	}
	// The sizes are written out, not taken from the constant: a bound that grew would
	// otherwise grow this response with it and still pass.
	const partitions, within = 300_000, 30_000 // a partition is a few values: 30,000 are well inside 262,144.
	if maxErrorWalk != 1<<18 {
		t.Fatalf("the walk looks at %d values; this test is written for 262,144", maxErrorWalk)
	}
	if got := firstError(big(partitions, within)); got != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("an error %d partitions in, inside the bound: %d", within, got)
	}
	if got := firstError(big(partitions, partitions-1)); got != 0 {
		t.Errorf("an error past the %d values looked at was found (%d): the walk is not bounded", maxErrorWalk, got)
	}
}

// A fetch answers through ReplySized, which sizes the buffer for the records it
// carries. It must frame exactly what Reply frames, and keep the answer as Reply does:
// the Traffic row of a fetch reads its records and its errors from it.
func TestReplySizedFramesAndKeepsTheAnswerAsReplyDoes(t *testing.T) {
	resp := kmsg.NewPtrFetchResponse()
	resp.SetVersion(12)
	rt := kmsg.NewFetchResponseTopic()
	rt.Topic = "orders"
	rp := kmsg.NewFetchResponseTopicPartition()
	rp.ErrorCode, rp.RecordBatches = kerr.OffsetOutOfRange.Code, batchOf(3)
	rt.Partitions = append(rt.Partitions, rp)
	resp.Topics = append(resp.Topics, rt)

	q := kmsg.NewPtrFetchRequest()
	q.SetVersion(12)
	ft := kmsg.NewFetchRequestTopic()
	ft.Topic = "orders"
	ft.Partitions = append(ft.Partitions, kmsg.NewFetchRequestTopicPartition())
	q.Topics = append(q.Topics, ft)

	header := RequestHeader{APIKey: kmsg.Fetch, APIVersion: 12, CorrelationID: 41}
	plain, sized := &Request{Header: header}, &Request{Header: header}
	plain.Noted(q, nil)
	sized.Noted(q, nil)
	want := plain.Reply(resp)
	for _, hint := range []int{0, 1, len(want), 10 * len(want)} {
		if got := sized.ReplySized(resp, hint); !bytes.Equal(got, want) {
			t.Errorf("with a size hint of %d the frame is %d bytes, Reply's is %d", hint, len(got), len(want))
		}
	}
	s := sized.Describe()
	if s.ErrorCode != kerr.OffsetOutOfRange.Code || s.Detail != "p0 from 0 · 3 records" || !reflect.DeepEqual(s, plain.Describe()) {
		t.Errorf("described after ReplySized: %+v\n          after Reply: %+v", s, plain.Describe())
	}
	// Before any answer there is nothing to read an error from.
	if s := (&Request{Header: header}).Describe(); s.ErrorCode != 0 {
		t.Errorf("a request with no answer: %+v", s)
	}
}
