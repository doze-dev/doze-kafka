package protocol

import (
	"reflect"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestMetadataSummaryAutoCreateNeedsANamedTopic(t *testing.T) {
	sum := summariserFor(t, kmsg.Metadata)
	none := kmsg.NewPtrMetadataRequest()
	none.AllowAutoTopicCreation = true
	if got := sum(none, nil); got.AutoCreate || got.Detail != "all topics" {
		t.Errorf("no topics named: %+v", got)
	}
	one := kmsg.NewPtrMetadataRequest()
	one.AllowAutoTopicCreation = true
	tp := kmsg.NewMetadataRequestTopic()
	tp.Topic = str("orders")
	one.Topics = append(one.Topics, tp)
	if got := sum(one, nil); !got.AutoCreate {
		t.Errorf("a topic named: %+v", got)
	}
}

// A topic's error is the row's detail when topics were named, not when all were asked
// for (then the count says what came back).
func TestMetadataSummaryNamesAnErrorOnlyForNamedTopics(t *testing.T) {
	sum := summariserFor(t, kmsg.Metadata)
	resp := kmsg.NewPtrMetadataResponse()
	rt := kmsg.NewMetadataResponseTopic()
	rt.ErrorCode = kerr.UnknownTopicOrPartition.Code
	resp.Topics = append(resp.Topics, rt)

	all := kmsg.NewPtrMetadataRequest()
	if got := sum(all, resp); got.Detail != "1 topic" {
		t.Errorf("all topics: %q", got.Detail)
	}
	named := kmsg.NewPtrMetadataRequest()
	tp := kmsg.NewMetadataRequestTopic()
	tp.Topic = str("nope")
	named.Topics = append(named.Topics, tp)
	if got := sum(named, resp); got.Detail != ErrorPhrase(kerr.UnknownTopicOrPartition.Code) {
		t.Errorf("a named topic: %q", got.Detail)
	}
}

func TestJoinGroupSummaryOfGenerationZero(t *testing.T) {
	sum := summariserFor(t, kmsg.JoinGroup)
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group = "g"
	resp := kmsg.NewPtrJoinGroupResponse()
	resp.Generation = 0
	if got := sum(req, resp).Detail; got != "generation 0" {
		t.Errorf("generation 0: %q", got)
	}
	resp.Generation = -1
	if got := sum(req, resp).Detail; strings.HasPrefix(got, "generation") {
		t.Errorf("generation -1: %q", got)
	}
}

func TestCreateTopicsSummaryOfOneTopicWithNoPartitionCount(t *testing.T) {
	sum := summariserFor(t, kmsg.CreateTopics)
	req := kmsg.NewPtrCreateTopicsRequest()
	tp := kmsg.NewCreateTopicsRequestTopic()
	tp.Topic, tp.NumPartitions = "t", -1
	req.Topics = append(req.Topics, tp)
	if got := sum(req, nil).Detail; got != "" {
		t.Errorf("no count: %q", got)
	}
	req.Topics[0].NumPartitions = 0
	if got := sum(req, nil).Detail; got != "" {
		t.Errorf("a count of 0: %q", got)
	}
	req.Topics[0].NumPartitions = 1
	if got := sum(req, nil).Detail; got != "1 partition" {
		t.Errorf("a count of 1: %q", got)
	}
}

func TestInitProducerIDSummaryBumpsOnlyWithBothIDsPresent(t *testing.T) {
	sum := summariserFor(t, kmsg.InitProducerID)
	for _, c := range []struct {
		epoch int16
		id    int64
		want  string
	}{
		{0, 0, "epoch 0 → 3"},
		{2, 9, "epoch 2 → 3"},
		{-1, 0, "producer id 77, epoch 3"},
		{0, -1, "producer id 77, epoch 3"},
		{-1, -1, "producer id 77, epoch 3"},
	} {
		req := kmsg.NewPtrInitProducerIDRequest()
		req.ProducerEpoch, req.ProducerID = c.epoch, c.id
		resp := kmsg.NewPtrInitProducerIDResponse()
		resp.ProducerID, resp.ProducerEpoch = 77, 3
		if got := sum(req, resp).Detail; got != c.want {
			t.Errorf("epoch %d id %d: %q, want %q", c.epoch, c.id, got, c.want)
		}
	}
}

func TestOneGroupIsNotCountedAsGroups(t *testing.T) {
	// Whichever API summarises a list of groups with a count when there are several.
	for _, a := range APIs {
		var q kmsg.Request
		switch a.Key {
		case kmsg.DescribeGroups:
			r := kmsg.NewPtrDescribeGroupsRequest()
			r.Groups = []string{"a"}
			q = r
		case kmsg.DeleteGroups:
			r := kmsg.NewPtrDeleteGroupsRequest()
			r.Groups = []string{"a"}
			q = r
		default:
			continue
		}
		if got := a.Summary(q, nil); got.Group != "a" || got.Detail != "" {
			t.Errorf("%s: %+v", APIName(a.Key), got)
		}
		switch r := q.(type) {
		case *kmsg.DescribeGroupsRequest:
			r.Groups = []string{"a", "b"}
		case *kmsg.DeleteGroupsRequest:
			r.Groups = []string{"a", "b"}
		}
		if got := a.Summary(q, nil); got.Detail != "2 groups" {
			t.Errorf("%s of two: %+v", APIName(a.Key), got)
		}
	}
}

// A software name or version that is refused is shown up to 48 bytes of it.
func TestRefusedClientSoftwareIsShownUpTo48Bytes(t *testing.T) {
	sum := summariserFor(t, kmsg.ApiVersions)
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.ErrorCode = kerr.InvalidRequest.Code
	for _, c := range []struct {
		n    int
		want string
	}{
		{3, `client software "aaa" version "1"`},
		{48, `client software "` + strings.Repeat("a", 48) + `" version "1"`},
		{49, `client software "` + strings.Repeat("a", 48) + `…" version "1"`},
	} {
		req := kmsg.NewPtrApiVersionsRequest()
		req.ClientSoftwareName, req.ClientSoftwareVersion = strings.Repeat("a", c.n), "1"
		if got := sum(req, resp).Detail; !strings.HasPrefix(got, c.want) {
			t.Errorf("%d bytes: %q, want it to begin %q", c.n, got, c.want)
		}
	}
}

type errLeaf struct{ ErrorCode int16 }
type errTree struct {
	Items []errLeaf
}
type errNested struct {
	Inner  errLeaf
	Ptr    *errLeaf
	Ints   []int32
	Bytes  []byte
	hidden errLeaf
}

// The error search finds a code in a nested struct, a pointer to one, and a list of them,
// and not in a list of numbers.
func TestFirstErrorWalksStructsPointersAndLists(t *testing.T) {
	walk := func(v any) int16 {
		b := 100
		return walkErrors(reflect.ValueOf(v), &b)
	}
	if got := walk(&errNested{Inner: errLeaf{4}}); got != 4 {
		t.Errorf("a struct field: %d", got)
	}
	if got := walk(&errNested{Ptr: &errLeaf{5}}); got != 5 {
		t.Errorf("a pointer field: %d", got)
	}
	if got := walk(&errTree{Items: []errLeaf{{0}, {6}, {7}}}); got != 6 {
		t.Errorf("a list: %d", got)
	}
	if got := walk(&errNested{Ints: []int32{3}, Bytes: []byte{3}, hidden: errLeaf{9}}); got != 0 {
		t.Errorf("no errors: %d", got)
	}
	if got := walk(&errNested{Inner: errLeaf{4}, Ptr: &errLeaf{5}}); got != 4 {
		t.Errorf("first of two: %d", got)
	}
}

// The search looks at maxErrorWalk values and no more: an error in the last one it looks
// at is found, in the next is not.
func TestFirstErrorSearchStopsAfterItsBudget(t *testing.T) {
	// walkErrors counts the pointer, the struct, the list, then each element once.
	build := func(n int) *errTree {
		items := make([]errLeaf, n)
		items[n-1].ErrorCode = 7
		return &errTree{Items: items}
	}
	for _, c := range []struct {
		elements int
		want     int16
	}{{maxErrorWalk - 3, 7}, {maxErrorWalk - 2, 0}} {
		budget := maxErrorWalk
		if got := walkErrors(reflect.ValueOf(build(c.elements)), &budget); got != c.want {
			t.Errorf("%d elements: %d, want %d", c.elements, got, c.want)
		}
	}
}
