package protocol

import (
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

func summaryOf(q kmsg.Request, r kmsg.Response) Summary {
	for _, a := range APIs {
		if a.Key == kmsg.Key(q.Key()) {
			return a.Summary(q, r)
		}
	}
	return Summary{}
}

// The Detail column is never left empty where the request has something to say
// (Main, Traf-03): how many topics Metadata found, why a topic was not created, what
// ListOffsets asked for, which offsets OffsetFetch wanted.
func TestDetailOfTheRequestsThatLeftItEmpty(t *testing.T) {
	// Metadata: the answer's topic count.
	mq := kmsg.NewPtrMetadataRequest()
	mq.Topics = nil
	mr := kmsg.NewPtrMetadataResponse()
	for range 3 {
		mr.Topics = append(mr.Topics, kmsg.NewMetadataResponseTopic())
	}
	if got := summaryOf(mq, mr).Detail; got != "3 topics" {
		t.Errorf("metadata for everything answered with 3 topics: %q", got)
	}
	mq.Topics = []kmsg.MetadataRequestTopic{{Topic: str("a")}}
	mq.AllowAutoTopicCreation = true
	if got := summaryOf(mq, mr); got.Detail != "3 topics" || !got.AutoCreate { // the broker says whether it creates (A8)
		t.Errorf("metadata that may create: %+v", got)
	}
	missing := kmsg.NewPtrMetadataResponse()
	mt := kmsg.NewMetadataResponseTopic()
	mt.ErrorCode = kerr.UnknownTopicOrPartition.Code
	missing.Topics = append(missing.Topics, mt)
	if got := summaryOf(mq, missing).Detail; !strings.HasPrefix(got, "no such topic") {
		t.Errorf("metadata for a topic that is not there: %q", got)
	}

	// ListOffsets: where, and what it asked for.
	lq := kmsg.NewPtrListOffsetsRequest()
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = "orders"
	for _, p := range []int32{0, 1, 2} {
		lp := kmsg.NewListOffsetsRequestTopicPartition()
		lp.Partition, lp.Timestamp = p, -2
		lt.Partitions = append(lt.Partitions, lp)
	}
	lq.Topics = append(lq.Topics, lt)
	if got := summaryOf(lq, nil).Detail; got != "p0–p2 · earliest offset" {
		t.Errorf("list offsets: %q", got)
	}

	// OffsetFetch: the partitions asked about, or all of them.
	oq := kmsg.NewPtrOffsetFetchRequest()
	oq.SetVersion(5)
	oq.Group = "g"
	if got := summaryOf(oq, nil).Detail; got != "all committed offsets" {
		t.Errorf("offset fetch with no topics: %q", got)
	}
	ot := kmsg.NewOffsetFetchRequestTopic()
	ot.Topic, ot.Partitions = "orders", []int32{0, 1}
	oq.Topics = append(oq.Topics, ot)
	if got := summaryOf(oq, nil).Detail; got != "p0, p1" {
		t.Errorf("offset fetch of two partitions: %q", got)
	}

	// CreateTopics: a refusal says why; a creation says what it made.
	cq := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions = "orders", 3
	cq.Topics = append(cq.Topics, ct)
	if got := summaryOf(cq, nil).Detail; got != "3 partitions" {
		t.Errorf("create topic: %q", got)
	}
	cr := kmsg.NewPtrCreateTopicsResponse()
	crt := kmsg.NewCreateTopicsResponseTopic()
	crt.Topic, crt.ErrorCode = "orders", kerr.TopicAlreadyExists.Code
	cr.Topics = append(cr.Topics, crt)
	if got := summaryOf(cq, cr).Detail; got != "the topic already exists" {
		t.Errorf("create topic refused: %q", got)
	}
}

// A refused request with nothing else to say says why, from the table.
func TestARefusalWithNoDetailSaysWhy(t *testing.T) {
	r := &Request{Header: RequestHeader{APIKey: kmsg.DeleteTopics}}
	q := kmsg.NewPtrDeleteTopicsRequest()
	resp := kmsg.NewPtrDeleteTopicsResponse()
	rt := kmsg.NewDeleteTopicsResponseTopic()
	rt.ErrorCode = kerr.UnknownTopicOrPartition.Code
	resp.Topics = append(resp.Topics, rt)
	r.Noted(q, resp)
	if got := r.Describe().Detail; got != "no such topic" {
		t.Errorf("the refusal's detail = %q, want %q", got, "no such topic")
	}
}

// A request for a version the broker does not serve says which API and which version
// (TRF-11): "Produce v99", not only a key number.
func TestNotServedNamesTheAPIAndVersionWhenTheKeyIsKnown(t *testing.T) {
	if got := notServed(RequestHeader{APIKey: kmsg.Produce, APIVersion: 99}); got != "Produce v99 is not served by this broker" {
		t.Errorf("known key: %q", got)
	}
	if got := notServed(RequestHeader{APIKey: 999, APIVersion: 0}); got != "API key 999 is not served by this broker" {
		t.Errorf("unknown key: %q", got)
	}
}

// A topic named by name carries a zero topic id on the wire; the decoded fields leave it
// out, and one that is set is written as a UUID.
func TestDecodedFieldsOmitAZeroTopicIDAndDashANonZeroOne(t *testing.T) {
	q := kmsg.NewPtrFetchRequest()
	q.SetVersion(13)
	tp := kmsg.NewFetchRequestTopic()
	tp.Topic = "orders"
	q.Topics = append(q.Topics, tp)
	if got := decodedFields(q); strings.Contains(got, "topic_id") || strings.Contains(got, "0000000000000000") {
		t.Errorf("a zero topic id is in the decoded fields: %s", got)
	}
	tp.TopicID = [16]byte{0xab, 0xcd, 0xef, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
	q.Topics[0] = tp
	if got := decodedFields(q); !strings.Contains(got, `"topic_id": "abcdef01-0203-0405-0607-08090a0b0c0d"`) {
		t.Errorf("a topic id is not written as a UUID: %s", got)
	}
}
