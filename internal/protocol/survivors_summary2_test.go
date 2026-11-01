package protocol

import (
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// A commit says its generation only when it has one (0 is none, as the -1 default is).
func TestOffsetCommitSummaryNamesAGenerationOnlyWhenThereIsOne(t *testing.T) {
	sum := summariserFor(t, kmsg.OffsetCommit)
	for _, c := range []struct {
		gen    int32
		has    bool
		detail string
	}{{-1, false, "p0"}, {0, false, "p0"}, {1, true, "p0 · generation 1"}} {
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group, req.Generation = "g", c.gen
		tp := kmsg.NewOffsetCommitRequestTopic()
		tp.Topic = "t"
		tp.Partitions = append(tp.Partitions, kmsg.NewOffsetCommitRequestTopicPartition())
		req.Topics = append(req.Topics, tp)
		got := sum(req, nil)
		if got.HasGen != c.has || got.Detail != c.detail {
			t.Errorf("generation %d: %+v, want has %v detail %q", c.gen, got, c.has, c.detail)
		}
	}
}

// An OffsetFetch of one group (v8 and later) that names no topics asks for every
// committed offset.
func TestOffsetFetchSummaryOfAGroupWithNoTopics(t *testing.T) {
	sum := summariserFor(t, kmsg.OffsetFetch)
	req := kmsg.NewPtrOffsetFetchRequest()
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = "g"
	req.Groups = append(req.Groups, g)
	if got := sum(req, nil); got.Group != "g" || got.Detail != "all committed offsets" {
		t.Errorf("no topics: %+v", got)
	}
	gt := kmsg.NewOffsetFetchRequestGroupTopic()
	gt.Topic, gt.Partitions = "t", []int32{0, 1}
	req.Groups[0].Topics = append(req.Groups[0].Topics, gt)
	if got := sum(req, nil); got.Detail != "p0, p1" {
		t.Errorf("a topic: %+v", got)
	}
}
