package broker_test

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// OffsetCommit: the partition just past the last one is unknown, as is a negative one.
func TestWire_OffsetCommitRefusesPartitionsOutsideTheTopic(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx := context.Background()
	mustCreateTopic(t, ctx, newClient(t, addr), "two", 2)
	c := openRaw(t, addr)
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group, req.Generation = "g-range", -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = "two"
	for _, p := range []int32{-1, 0, 1, 2, 3} {
		rp := kmsg.NewOffsetCommitRequestTopicPartition()
		rp.Partition, rp.Offset, rp.LeaderEpoch = p, 5, -1
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)
	got := map[int32]int16{}
	for _, p := range ask(t, c, req, 8, kmsg.NewPtrOffsetCommitResponse()).Topics[0].Partitions {
		got[p.Partition] = p.ErrorCode
	}
	want := map[int32]int16{-1: kerr.UnknownTopicOrPartition.Code, 0: 0, 1: 0, 2: kerr.UnknownTopicOrPartition.Code, 3: kerr.UnknownTopicOrPartition.Code}
	for p, code := range want {
		if got[p] != code {
			t.Errorf("partition %d: code %d, want %d", p, got[p], code)
		}
	}
}

// OffsetFetch for a principal that may describe the group but only some of its
// topics: "all offsets" leaves the others out, a named one is refused with offset
// -1 and empty metadata. v7 and v8 differ in shape, so both are read; a topic with
// two committed partitions stays one entry in v7.
func TestWire_OffsetFetchHidesTopicsThePrincipalCannotDescribe(t *testing.T) {
	skipShort(t)
	addr := startAuthzBroker(t, "deny")
	ctx := context.Background()
	adminCl := as(t, addr, "admin")
	mustCreateTopic(t, ctx, adminCl, "open", 2)
	mustCreateTopic(t, ctx, adminCl, "secret", 1)
	createACLs(t, ctx, adminCl,
		allow(addr, groupRes, "g", literal, "alice", kmsg.ACLOperationDescribe, permit),
		allow(addr, topicRes, "open", literal, "alice", kmsg.ACLOperationDescribe, permit),
	)
	admin := rawAs(t, addr, "admin")
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group, commit.Generation = "g", -1
	for _, tp := range []struct {
		topic string
		parts []int32
	}{{"open", []int32{0, 1}}, {"secret", []int32{0}}} {
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = tp.topic
		for _, p := range tp.parts {
			rp := kmsg.NewOffsetCommitRequestTopicPartition()
			rp.Partition, rp.Offset, rp.LeaderEpoch = p, 40+int64(p), -1
			rt.Partitions = append(rt.Partitions, rp)
		}
		commit.Topics = append(commit.Topics, rt)
	}
	for _, tp := range ask(t, admin, commit, 8, kmsg.NewPtrOffsetCommitResponse()).Topics {
		for _, p := range tp.Partitions {
			if p.ErrorCode != 0 {
				t.Fatalf("setup commit %s/%d: code %d", tp.Topic, p.Partition, p.ErrorCode)
			}
		}
	}

	alice := rawAs(t, addr, "alice")
	type pr struct {
		offset int64
		code   int16
		meta   *string
	}
	// seen7 flattens a v7 answer into topic/partition -> what it says, and counts topic entries.
	seen7 := func(topics []kmsg.OffsetFetchRequestTopic) (map[string]pr, int) {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group, req.Topics = "g", topics
		resp := ask(t, alice, req, 7, kmsg.NewPtrOffsetFetchResponse())
		out := map[string]pr{}
		for _, tp := range resp.Topics {
			for _, p := range tp.Partitions {
				out[tp.Topic+"/"+string(rune('0'+p.Partition))] = pr{p.Offset, p.ErrorCode, p.Metadata}
			}
		}
		return out, len(resp.Topics)
	}
	named := func(topic string, parts ...int32) kmsg.OffsetFetchRequestTopic {
		return kmsg.OffsetFetchRequestTopic{Topic: topic, Partitions: parts}
	}

	all, entries := seen7(nil)
	if len(all) != 2 || entries != 1 || all["open/0"].offset != 40 || all["open/1"].offset != 41 {
		t.Errorf("v7 all offsets: %+v in %d topic entries, want open/0=40 and open/1=41 in one", all, entries)
	}
	explicit, _ := seen7([]kmsg.OffsetFetchRequestTopic{named("open", 0), named("secret", 0)})
	s := explicit["secret/0"]
	if s.offset != -1 || s.code != kerr.TopicAuthorizationFailed.Code || s.meta == nil || *s.meta != "" {
		t.Errorf("v7 named secret/0: %+v, want offset -1, code 29 and empty metadata", s)
	}
	if o := explicit["open/0"]; o.offset != 40 || o.code != 0 {
		t.Errorf("v7 named open/0: %+v", o)
	}

	// v8: the batched form.
	fetch8 := func(topics []kmsg.OffsetFetchRequestGroupTopic) kmsg.OffsetFetchResponseGroup {
		req := kmsg.NewPtrOffsetFetchRequest()
		g := kmsg.NewOffsetFetchRequestGroup()
		g.Group, g.Topics = "g", topics
		req.Groups = append(req.Groups, g)
		return ask(t, alice, req, 8, kmsg.NewPtrOffsetFetchResponse()).Groups[0]
	}
	g8 := fetch8(nil)
	if len(g8.Topics) != 1 || g8.Topics[0].Topic != "open" || len(g8.Topics[0].Partitions) != 2 {
		t.Errorf("v8 all offsets: %+v, want only open with two partitions", g8.Topics)
	}
	g8 = fetch8([]kmsg.OffsetFetchRequestGroupTopic{{Topic: "secret", Partitions: []int32{0}}})
	if len(g8.Topics) != 1 || len(g8.Topics[0].Partitions) != 1 {
		t.Fatalf("v8 named secret: %+v", g8.Topics)
	}
	p := g8.Topics[0].Partitions[0]
	if p.Offset != -1 || p.ErrorCode != kerr.TopicAuthorizationFailed.Code || p.Metadata == nil || *p.Metadata != "" {
		t.Errorf("v8 named secret/0: %+v, want offset -1, code 29 and empty metadata", p)
	}
}
