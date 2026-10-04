package broker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The persistence rule: everything the broker keeps is kept across a restart, and
// this is the list. Each thing is set to a distinctive, non-default value before
// the restart and read back after it, because a default that happens to match is
// how "it was never saved" goes unnoticed. A new persisted field belongs here, in
// the same change that adds it.
//
//	topic registry     name, partition count, topic id, every config override
//	log                key, value, headers, timestamp, offsets
//	committed offsets  offset and the metadata committed with it
//	producer ids       never issued twice
//	deletions          a deleted topic stays deleted, and its offsets with it
//	group list         a group known only by its committed offsets
//
// (What is deliberately not kept: group membership and idempotent-producer
// sequence state, both documented in docs/limitations.md.)
func TestPersistence_EverythingTheBrokerKeepsSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stop := runBrokerAt(t, addr, dir, 4)
	cl := newClient(t, addr)

	// ---- topic registry: a topic with a count and overrides unlike any default.
	overrides := map[string]string{
		"retention.ms": "123456789", "retention.bytes": "987654321", "segment.bytes": "7654321", "max.message.bytes": "777777",
		"message.timestamp.type": "CreateTime", "cleanup.policy": "compact,delete", "min.insync.replicas": "1", "flush.ms": "4242",
	}
	if code := createTopicWith(t, ctx, cl, "kept", overrides); code != 0 {
		t.Fatalf("create: %d", code)
	}
	// createTopicWith asks for one partition; the count worth keeping is another one.
	mustCreateTopic(t, ctx, cl, "wide", 7)
	topicID := func(c *kgo.Client, name string) [16]byte {
		req := kmsg.NewPtrMetadataRequest()
		mt := kmsg.NewMetadataRequestTopic()
		mt.Topic = &name
		req.Topics = append(req.Topics, mt)
		resp, err := req.RequestWith(ctx, c)
		if err != nil || len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
			t.Fatalf("Metadata(%s): %v %+v", name, err, resp)
		}
		return resp.Topics[0].TopicID
	}
	idKept, idWide := topicID(cl, "kept"), topicID(cl, "wide")

	// ---- log: a record with every field set to something recognisable.
	stamp := time.Date(2009, 2, 13, 23, 31, 30, 0, time.UTC) // 1234567890.
	rec := &kgo.Record{Topic: "kept", Key: []byte("a-key"), Value: []byte("a-value"), Timestamp: stamp,
		Headers: []kgo.RecordHeader{{Key: "h1", Value: []byte("v1")}, {Key: "h2", Value: []byte{0, 255}}}}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatal(err)
	}

	// ---- committed offsets, with metadata.
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group, commit.Generation = "persisted-group", -1
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic = "kept"
	cp := kmsg.NewOffsetCommitRequestTopicPartition()
	cp.Partition, cp.Offset, cp.Metadata = 0, 1, kmsg.StringPtr("remember-me")
	ct.Partitions = append(ct.Partitions, cp)
	commit.Topics = append(commit.Topics, ct)
	if resp, err := commit.RequestWith(ctx, cl); err != nil || resp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("commit: %v %+v", err, resp)
	}

	// ---- producer ids.
	initPID := func(c *kgo.Client) int64 {
		resp, err := kmsg.NewPtrInitProducerIDRequest().RequestWith(ctx, c)
		if err != nil || resp.ErrorCode != 0 {
			t.Fatalf("InitProducerID: %v %+v", err, resp)
		}
		return resp.ProducerID
	}
	var pids []int64
	for range 3 {
		pids = append(pids, initPID(cl))
	}

	// ---- deletions: a topic with an offset, deleted.
	mustCreateTopic(t, ctx, cl, "doomed", 1)
	commitOffset(t, ctx, cl, "persisted-group", "doomed", 5)
	del := kmsg.NewPtrDeleteTopicsRequest()
	dt := kmsg.NewDeleteTopicsRequestTopic()
	dt.Topic = kmsg.StringPtr("doomed")
	del.Topics = append(del.Topics, dt)
	del.TopicNames = []string{"doomed"}
	if resp, err := del.RequestWith(ctx, cl); err != nil || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("delete: %v %+v", err, resp)
	}

	cl.Close()
	stop()

	// ================= restart =================
	stop = runBrokerAt(t, addr, dir, 4)
	defer stop()
	cl = newClient(t, addr)
	defer cl.Close()

	values, own := describe(t, ctx, cl, "kept")
	for k, v := range overrides {
		if values[k] != v || !own[k] {
			t.Errorf("topic config %s = %q (own=%v) after the restart, want %q", k, values[k], own[k], v)
		}
	}
	if got := topicID(cl, "kept"); got != idKept {
		t.Errorf("topic id of kept changed across the restart: %x -> %x", idKept, got)
	}
	if got := topicID(cl, "wide"); got != idWide {
		t.Errorf("topic id of wide changed across the restart")
	}
	meta := kmsg.NewPtrMetadataRequest()
	wide := "wide"
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = &wide
	meta.Topics = append(meta.Topics, mt)
	if resp, err := meta.RequestWith(ctx, cl); err != nil || len(resp.Topics[0].Partitions) != 7 {
		t.Errorf("wide has %d partitions after the restart, want 7 (%v)", len(resp.Topics[0].Partitions), err)
	}

	got := consumeRecords(t, ctx, addr, "kept", 1)
	if len(got) != 1 || string(got[0].Key) != "a-key" || string(got[0].Value) != "a-value" || !got[0].Timestamp.Equal(stamp) ||
		len(got[0].Headers) != 2 || got[0].Headers[0].Key != "h1" || string(got[0].Headers[0].Value) != "v1" || string(got[0].Headers[1].Value) != string([]byte{0, 255}) {
		t.Errorf("the record after the restart: %+v", got)
	}

	of := kmsg.NewPtrOffsetFetchRequest()
	of.Version = 7
	of.Group = "persisted-group"
	ofResp, err := of.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, rt := range ofResp.Topics {
		for _, rp := range rt.Partitions {
			if rp.Offset < 0 {
				continue
			}
			if rt.Topic == "doomed" {
				t.Error("an offset of a deleted topic came back after the restart")
			}
			if rt.Topic == "kept" && rp.Offset == 1 && rp.Metadata != nil && *rp.Metadata == "remember-me" {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("the committed offset (1, with metadata) is not there after the restart: %+v", ofResp.Topics)
	}
	lg, err := kmsg.NewPtrListGroupsRequest().RequestWith(ctx, cl)
	if err != nil || len(lg.Groups) != 1 || lg.Groups[0].Group != "persisted-group" {
		t.Errorf("groups after the restart: %v %+v", err, lg)
	}

	// A producer id is never issued twice, across the restart too.
	top := pids[len(pids)-1]
	if next := initPID(cl); next <= top {
		t.Errorf("producer id %d issued after the restart, but %d had been issued before it", next, top)
	}

	// Deleted stays deleted.
	md := kmsg.NewPtrMetadataRequest()
	doomed := "doomed"
	dm := kmsg.NewMetadataRequestTopic()
	dm.Topic = &doomed
	md.Topics = append(md.Topics, dm)
	if resp, err := md.RequestWith(ctx, cl); err != nil || resp.Topics[0].ErrorCode == 0 {
		t.Errorf("the deleted topic exists again after the restart: %v %+v", err, resp)
	}
	// And re-creating it starts from nothing: no offset inherited.
	mustCreateTopic(t, ctx, cl, "doomed", 1)
	if off := fetchCommitted(t, ctx, cl, "persisted-group", "doomed"); off != -1 {
		t.Errorf("a re-created topic inherited committed offset %d", off)
	}
}

// consumeRecords reads n records of a topic from the start, by a client that is
// not in a group.
func consumeRecords(t *testing.T, ctx context.Context, addr, topic string, n int) []*kgo.Record {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var out []*kgo.Record
	for len(out) < n && ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		c.PollFetches(pctx).EachRecord(func(r *kgo.Record) { out = append(out, r) })
		cancel()
	}
	return out
}
