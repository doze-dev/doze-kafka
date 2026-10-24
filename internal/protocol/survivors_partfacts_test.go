package protocol

import (
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

var topicID1 = [16]byte{1}

// The answer is paired with the partition asked by its number, not by its position.
func TestAnswersAreFoundByPartitionNumberWhenOutOfOrder(t *testing.T) {
	q := kmsg.NewPtrProduceRequest()
	tp := kmsg.NewProduceRequestTopic()
	tp.Topic = "orders"
	for _, n := range []int32{0, 1} {
		pp := kmsg.NewProduceRequestTopicPartition()
		pp.Partition = n
		tp.Partitions = append(tp.Partitions, pp)
	}
	q.Topics = append(q.Topics, tp)
	resp := kmsg.NewPtrProduceResponse()
	rt := kmsg.NewProduceResponseTopic()
	rt.Topic = "orders"
	for _, c := range []struct {
		n    int32
		base int64
	}{{1, 100}, {0, 50}} {
		rp := kmsg.NewProduceResponseTopicPartition()
		rp.Partition, rp.BaseOffset = c.n, c.base
		rt.Partitions = append(rt.Partitions, rp)
	}
	resp.Topics = append(resp.Topics, rt)
	f := DescribeFacts(q, resp)
	if len(f.Partitions) != 2 || f.Partitions[0].Offset != 50 || f.Partitions[1].Offset != 100 {
		t.Errorf("partitions = %+v, want offsets 50 and 100", f.Partitions)
	}
}

// A topic asked for by id is named by the request, else by the answer; the request's
// own name is not replaced by the answer's.
func TestATopicAskedByIDIsNamedByTheRequestElseTheAnswer(t *testing.T) {
	type named struct{ req, ans, want string }
	cases := []named{{"", "from-answer", "from-answer"}, {"from-request", "", "from-request"}, {"from-request", "other", "from-request"}}

	for _, c := range cases {
		// Produce
		q := kmsg.NewPtrProduceRequest()
		tp := kmsg.NewProduceRequestTopic()
		tp.Topic, tp.TopicID = c.req, topicID1
		pp := kmsg.NewProduceRequestTopicPartition()
		tp.Partitions = append(tp.Partitions, pp)
		q.Topics = append(q.Topics, tp)
		resp := kmsg.NewPtrProduceResponse()
		rt := kmsg.NewProduceResponseTopic()
		rt.Topic, rt.TopicID = c.ans, topicID1
		rt.Partitions = append(rt.Partitions, kmsg.NewProduceResponseTopicPartition())
		resp.Topics = append(resp.Topics, rt)
		if got := DescribeFacts(q, resp).Partitions[0].Topic; got != c.want {
			t.Errorf("produce %+v: topic %q", c, got)
		}

		// Fetch
		fq := kmsg.NewPtrFetchRequest()
		ft := kmsg.NewFetchRequestTopic()
		ft.Topic, ft.TopicID = c.req, topicID1
		ft.Partitions = append(ft.Partitions, kmsg.NewFetchRequestTopicPartition())
		fq.Topics = append(fq.Topics, ft)
		fresp := kmsg.NewPtrFetchResponse()
		frt := kmsg.NewFetchResponseTopic()
		frt.Topic, frt.TopicID = c.ans, topicID1
		frt.Partitions = append(frt.Partitions, kmsg.NewFetchResponseTopicPartition())
		fresp.Topics = append(fresp.Topics, frt)
		if got := DescribeFacts(fq, fresp).Partitions[0].Topic; got != c.want {
			t.Errorf("fetch %+v: topic %q", c, got)
		}

		// OffsetCommit
		oq := kmsg.NewPtrOffsetCommitRequest()
		ot := kmsg.NewOffsetCommitRequestTopic()
		ot.Topic, ot.TopicID = c.req, topicID1
		ot.Partitions = append(ot.Partitions, kmsg.NewOffsetCommitRequestTopicPartition())
		oq.Topics = append(oq.Topics, ot)
		oresp := kmsg.NewPtrOffsetCommitResponse()
		ort := kmsg.NewOffsetCommitResponseTopic()
		ort.Topic, ort.TopicID = c.ans, topicID1
		ort.Partitions = append(ort.Partitions, kmsg.NewOffsetCommitResponseTopicPartition())
		oresp.Topics = append(oresp.Topics, ort)
		if got := DescribeFacts(oq, oresp).Partitions[0].Topic; got != c.want {
			t.Errorf("offset commit %+v: topic %q", c, got)
		}

		// OffsetFetch of one group, v8 and later.
		gq := kmsg.NewPtrOffsetFetchRequest()
		g := kmsg.NewOffsetFetchRequestGroup()
		g.Group = "g"
		gt := kmsg.NewOffsetFetchRequestGroupTopic()
		gt.Topic, gt.TopicID = c.req, topicID1
		gt.Partitions = []int32{0}
		g.Topics = append(g.Topics, gt)
		gq.Groups = append(gq.Groups, g)
		gresp := kmsg.NewPtrOffsetFetchResponse()
		rg := kmsg.NewOffsetFetchResponseGroup()
		rg.Group = "g"
		rgt := kmsg.NewOffsetFetchResponseGroupTopic()
		rgt.Topic, rgt.TopicID = c.ans, topicID1
		rgt.Partitions = append(rgt.Partitions, kmsg.NewOffsetFetchResponseGroupTopicPartition())
		rg.Topics = append(rg.Topics, rgt)
		gresp.Groups = append(gresp.Groups, rg)
		if got := DescribeFacts(gq, gresp).Partitions[0].Topic; got != c.want {
			t.Errorf("offset fetch %+v: topic %q", c, got)
		}
	}
}

// Offset 0 is an offset: committed, listed. -1 is none.
func TestAnOffsetOfZeroIsAnOffset(t *testing.T) {
	gq := kmsg.NewPtrOffsetFetchRequest()
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = "g"
	gt := kmsg.NewOffsetFetchRequestGroupTopic()
	gt.Topic = "t"
	gt.Partitions = []int32{0, 1}
	g.Topics = append(g.Topics, gt)
	gq.Groups = append(gq.Groups, g)
	gresp := kmsg.NewPtrOffsetFetchResponse()
	rg := kmsg.NewOffsetFetchResponseGroup()
	rg.Group = "g"
	rgt := kmsg.NewOffsetFetchResponseGroupTopic()
	rgt.Topic = "t"
	for _, c := range []struct {
		n   int32
		off int64
	}{{0, 0}, {1, -1}} {
		rp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
		rp.Partition, rp.Offset = c.n, c.off
		rgt.Partitions = append(rgt.Partitions, rp)
	}
	rg.Topics = append(rg.Topics, rgt)
	gresp.Groups = append(gresp.Groups, rg)
	f := DescribeFacts(gq, gresp)
	if !f.Partitions[0].HasOffset || f.Partitions[0].Offset != 0 || f.Partitions[1].HasOffset {
		t.Errorf("partitions = %+v", f.Partitions)
	}

	lq := kmsg.NewPtrListOffsetsRequest()
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = "t"
	for _, n := range []int32{0, 1} {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition = n
		lt.Partitions = append(lt.Partitions, p)
	}
	lq.Topics = append(lq.Topics, lt)
	lresp := kmsg.NewPtrListOffsetsResponse()
	lresp.SetVersion(1)
	lrt := kmsg.NewListOffsetsResponseTopic()
	lrt.Topic = "t"
	for _, c := range []struct {
		n   int32
		off int64
	}{{0, 0}, {1, -1}} {
		rp := kmsg.NewListOffsetsResponseTopicPartition()
		rp.Partition, rp.Offset = c.n, c.off
		lrt.Partitions = append(lrt.Partitions, rp)
	}
	lresp.Topics = append(lresp.Topics, lrt)
	lf := DescribeFacts(lq, lresp)
	if !lf.Partitions[0].HasOffset || lf.Partitions[0].Offset != 0 || lf.Partitions[1].HasOffset {
		t.Errorf("list offsets partitions = %+v", lf.Partitions)
	}
}
