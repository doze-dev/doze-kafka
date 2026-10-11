package protocol

import (
	"reflect"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The words and numbers the summariser's helpers produce are read off the Traffic page
// as they are: a size is in the unit a person reads, partitions are ranges, a wait is
// seconds only when it is whole seconds.
func TestSummaryHelpersWriteWhatThePageShows(t *testing.T) {
	for n, want := range map[int]string{0: "0 B", 1023: "1023 B", 1024: "1.0 KiB", 1536: "1.5 KiB", 1<<20 - 1: "1024.0 KiB", 1 << 20: "1.0 MiB", 5 << 20: "5.0 MiB"} {
		if got := bytesText(n); got != want {
			t.Errorf("bytesText(%d) = %q, want %q", n, got, want)
		}
	}
	for _, c := range []struct {
		in   []int32
		want string
	}{
		{nil, ""},
		{[]int32{3}, "p3"},
		{[]int32{1, 0}, "p0, p1"},
		{[]int32{2, 0, 1}, "p0–p2"},
		{[]int32{5, 0, 1, 2, 2}, "p0–p2, p5"},
	} {
		if got := partitionRanges(c.in); got != c.want {
			t.Errorf("partitionRanges(%v) = %q, want %q", c.in, got, c.want)
		}
	}
	for ms, want := range map[int32]string{500: "500 ms", 999: "999 ms", 1000: "1 s", 1500: "1500 ms", 30000: "30 s"} {
		if got := shortMillis(ms); got != want {
			t.Errorf("shortMillis(%d) = %q, want %q", ms, got, want)
		}
	}
	for n, want := range map[int64]string{0: "0", 999: "999", 1000: "1,000", 4812: "4,812", 1234567: "1,234,567", -4812: "-4,812"} {
		if got := groupInt(n); got != want {
			t.Errorf("groupInt(%d) = %q, want %q", n, got, want)
		}
	}
}

// Each of these is a request a client sends and a sentence the page prints for it.
func TestSummarisersSayWhatEachRequestAsks(t *testing.T) {
	listOffsets := func(stamps ...int64) *kmsg.ListOffsetsRequest {
		q := kmsg.NewPtrListOffsetsRequest()
		tp := kmsg.NewListOffsetsRequestTopic()
		tp.Topic = "orders"
		for i, ts := range stamps {
			pp := kmsg.NewListOffsetsRequestTopicPartition()
			pp.Partition, pp.Timestamp = int32(i), ts
			tp.Partitions = append(tp.Partitions, pp)
		}
		q.Topics = append(q.Topics, tp)
		return q
	}
	for _, c := range []struct {
		stamps []int64
		want   string
	}{
		{[]int64{-1}, "p0 · latest offset"},
		{[]int64{-2}, "p0 · earliest offset"},
		{[]int64{-3}, "p0 · max timestamp"},
		{[]int64{1700000000000}, "p0 · offset at a time"},
		{[]int64{-1, -2}, "p0, p1 · offsets"},
	} {
		if got := sumListOffsets(listOffsets(c.stamps...), nil).Detail; got != c.want {
			t.Errorf("ListOffsets %v: %q, want %q", c.stamps, got, c.want)
		}
	}
	two := listOffsets(-1)
	second := kmsg.NewListOffsetsRequestTopic()
	second.Topic = "payments"
	sp := kmsg.NewListOffsetsRequestTopicPartition()
	sp.Timestamp = -1
	second.Partitions = append(second.Partitions, sp)
	two.Topics = append(two.Topics, second)
	if got := sumListOffsets(two, nil).Detail; got != "2 partitions · latest offset" {
		t.Errorf("ListOffsets over two topics: %q", got)
	}

	// Metadata: what came back, or why a named topic did not.
	named := func(auto bool, names ...string) *kmsg.MetadataRequest {
		q := kmsg.NewPtrMetadataRequest()
		q.AllowAutoTopicCreation = auto
		for _, n := range names {
			mt := kmsg.NewMetadataRequestTopic()
			mt.Topic = str(n)
			q.Topics = append(q.Topics, mt)
		}
		return q
	}
	resp := func(codes ...int16) *kmsg.MetadataResponse {
		r := kmsg.NewPtrMetadataResponse()
		for _, c := range codes {
			mt := kmsg.NewMetadataResponseTopic()
			mt.ErrorCode = c
			r.Topics = append(r.Topics, mt)
		}
		return r
	}
	if got := sumMetadata(named(false), nil).Detail; got != "all topics" {
		t.Errorf("Metadata for nothing: %q", got)
	}
	if got := sumMetadata(named(true, "orders"), nil).Detail; got != "auto-create on" {
		t.Errorf("Metadata that may create: %q", got)
	}
	if got := sumMetadata(named(false), resp(0, 0, 0)).Detail; got != "3 topics" {
		t.Errorf("Metadata for everything, answered: %q", got)
	}
	if got := sumMetadata(named(false, "orders"), resp(0)).Detail; got != "1 topic" {
		t.Errorf("Metadata for one, answered: %q", got)
	}
	if got := sumMetadata(named(false, "nope"), resp(kerr.UnknownTopicOrPartition.Code)).Detail; got != ErrorPhrase(kerr.UnknownTopicOrPartition.Code) {
		t.Errorf("Metadata for a missing topic: %q", got)
	}
	idq := kmsg.NewPtrMetadataRequest()
	byID := kmsg.NewMetadataRequestTopic()
	byID.TopicID = [16]byte{9}
	idq.Topics = append(idq.Topics, byID)
	if s := sumMetadata(idq, nil); !reflect.DeepEqual(s.TopicIDs, [][16]byte{{9}}) || len(s.Topics) != 0 {
		t.Errorf("Metadata by id: %+v", s)
	}

	// OffsetFetch: the group's whole commit set, or the partitions named.
	of := kmsg.NewPtrOffsetFetchRequest()
	of.Group = "g"
	if got := sumOffsetFetch(of, nil).Detail; got != "all committed offsets" {
		t.Errorf("OffsetFetch of nothing named: %q", got)
	}
	for _, topic := range []string{"a", "b"} {
		ot := kmsg.NewOffsetFetchRequestTopic()
		ot.Topic, ot.Partitions = topic, []int32{0, 1}
		of.Topics = append(of.Topics, ot)
	}
	if got := sumOffsetFetch(of, nil).Detail; got != "4 partitions" {
		t.Errorf("OffsetFetch over two topics: %q", got)
	}
	v8 := kmsg.NewPtrOffsetFetchRequest()
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = "g8"
	gt := kmsg.NewOffsetFetchRequestGroupTopic()
	gt.Topic, gt.Partitions = "orders", []int32{0, 1, 2}
	g.Topics = append(g.Topics, gt)
	v8.Groups = append(v8.Groups, g)
	if s := sumOffsetFetch(v8, nil); s.Group != "g8" || s.Detail != "p0–p2" || !reflect.DeepEqual(s.Topics, []string{"orders"}) {
		t.Errorf("OffsetFetch for a v8 group: %+v", s)
	}

	// A group's join, in the words of the answer.
	jq := kmsg.NewPtrJoinGroupRequest()
	jq.Group = "g"
	jr := kmsg.NewPtrJoinGroupResponse()
	jr.ErrorCode = kerr.MemberIDRequired.Code
	if got := sumJoin(jq, jr).Detail; got != "a new member asks in; it is given an id to join with" {
		t.Errorf("JoinGroup that is asked for a member id: %q", got)
	}
	jr = kmsg.NewPtrJoinGroupResponse()
	jr.Generation = 4
	if got := sumJoin(jq, jr).Detail; got != "generation 4" {
		t.Errorf("JoinGroup answered: %q", got)
	}

	// Several groups in one request are counted; one is named.
	dg := kmsg.NewPtrDescribeGroupsRequest()
	dg.Groups = []string{"a", "b", "c"}
	if s := sumGroups(dg, nil); s.Group != "a" || s.Detail != "3 groups" {
		t.Errorf("DescribeGroups of three: %+v", s)
	}

	// CreateTopics: a refusal says why, a creation says how big.
	ct := func(parts int32, names ...string) *kmsg.CreateTopicsRequest {
		q := kmsg.NewPtrCreateTopicsRequest()
		for _, n := range names {
			t := kmsg.NewCreateTopicsRequestTopic()
			t.Topic, t.NumPartitions = n, parts
			q.Topics = append(q.Topics, t)
		}
		return q
	}
	if got := sumCreateTopics(ct(3, "a"), nil).Detail; got != "3 partitions" {
		t.Errorf("CreateTopics of one: %q", got)
	}
	if got := sumCreateTopics(ct(1, "a", "b"), nil).Detail; got != "2 topics" {
		t.Errorf("CreateTopics of two: %q", got)
	}
	vo := ct(1, "a")
	vo.ValidateOnly = true
	if got := sumCreateTopics(vo, nil).Detail; got != "1 partition · validate only" {
		t.Errorf("CreateTopics that only validates: %q", got)
	}
	cr := kmsg.NewPtrCreateTopicsResponse()
	rt := kmsg.NewCreateTopicsResponseTopic()
	rt.ErrorCode = kerr.TopicAlreadyExists.Code
	cr.Topics = append(cr.Topics, rt)
	if got := sumCreateTopics(ct(1, "a"), cr).Detail; got != ErrorPhrase(kerr.TopicAlreadyExists.Code) {
		t.Errorf("CreateTopics that is refused: %q", got)
	}

	// A KIP-848 heartbeat: epoch 0 joins, -1 leaves, the rest is the steady beat.
	hb := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hb.Group = "g"
	for epoch, want := range map[int32]string{0: "join", -1: "leave", 5: ""} {
		hb.MemberEpoch = epoch
		s := sumGroupHeartbeat(hb, nil)
		if s.Detail != want || s.Idle != (epoch > 0) {
			t.Errorf("ConsumerGroupHeartbeat epoch %d: %+v", epoch, s)
		}
	}

	// A transaction names the topics it adds, by v4's transactions list too.
	ap := kmsg.NewPtrAddPartitionsToTxnRequest()
	tx := kmsg.NewAddPartitionsToTxnRequestTransaction()
	tx.TransactionalID = "tx-9"
	tt := kmsg.NewAddPartitionsToTxnRequestTransactionTopic()
	tt.Topic = "orders"
	tx.Topics = append(tx.Topics, tt, tt)
	ap.Transactions = append(ap.Transactions, tx)
	if s := sumAddPartitionsToTxn(ap, nil); s.TxnID != "tx-9" || !reflect.DeepEqual(s.Topics, []string{"orders"}) {
		t.Errorf("AddPartitionsToTxn v4: %+v", s)
	}

	// Config requests name the topics among their resources, once each.
	res := func(kind kmsg.ConfigResourceType, name string) (r kmsg.DescribeConfigsRequestResource) {
		r = kmsg.NewDescribeConfigsRequestResource()
		r.ResourceType, r.ResourceName = kind, name
		return r
	}
	dc := kmsg.NewPtrDescribeConfigsRequest()
	dc.Resources = append(dc.Resources, res(kmsg.ConfigResourceTypeTopic, "orders"), res(kmsg.ConfigResourceTypeTopic, "orders"), res(kmsg.ConfigResourceTypeBroker, "0"))
	if s := sumConfigs(dc, nil); !reflect.DeepEqual(s.Topics, []string{"orders"}) {
		t.Errorf("DescribeConfigs: %+v", s)
	}
	ac := kmsg.NewPtrAlterConfigsRequest()
	ar := kmsg.NewAlterConfigsRequestResource()
	ar.ResourceType, ar.ResourceName = kmsg.ConfigResourceTypeTopic, "a"
	ac.Resources = append(ac.Resources, ar, ar)
	if s := sumConfigs(ac, nil); !reflect.DeepEqual(s.Topics, []string{"a"}) {
		t.Errorf("AlterConfigs: %+v", s)
	}
	ic := kmsg.NewPtrIncrementalAlterConfigsRequest()
	ir := kmsg.NewIncrementalAlterConfigsRequestResource()
	ir.ResourceType, ir.ResourceName = kmsg.ConfigResourceTypeTopic, "b"
	ic.Resources = append(ic.Resources, ir, ir)
	if s := sumConfigs(ic, nil); !reflect.DeepEqual(s.Topics, []string{"b"}) {
		t.Errorf("IncrementalAlterConfigs: %+v", s)
	}
}
