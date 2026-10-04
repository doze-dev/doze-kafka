package broker_test

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// AllowAutoTopicCreation only exists from Metadata v4. Before that, asking about
// an unknown topic always meant "create it if the broker allows"; a v1 client got
// UNKNOWN_TOPIC_OR_PARTITION forever.
func TestMetadataBelowV4AutoCreates(t *testing.T) {
	c := openRaw(t, startBroker(t, 4))
	for _, v := range []int16{1, 3} {
		md := kmsg.NewPtrMetadataRequest()
		md.SetVersion(v)
		name := "auto-v" + string(rune('0'+v))
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(name)
		md.Topics = append(md.Topics, rt)
		var resp kmsg.MetadataResponse
		if !c.do(md, &resp) {
			t.Fatal("connection closed")
		}
		if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 || len(resp.Topics[0].Partitions) != 1 {
			t.Errorf("Metadata v%d for an unknown topic: %+v; want it created with 1 partition", v, resp.Topics)
		}
	}
	// v4 with the flag off still does not create.
	md := kmsg.NewPtrMetadataRequest()
	md.SetVersion(4)
	md.AllowAutoTopicCreation = false
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr("not-created")
	md.Topics = append(md.Topics, rt)
	var resp kmsg.MetadataResponse
	c.do(md, &resp)
	if resp.Topics[0].ErrorCode != 3 {
		t.Errorf("Metadata v4 with auto-creation off: code %d, want 3", resp.Topics[0].ErrorCode)
	}
}

// ListOffsets v0 answers with a list, not a single field. It was left empty.
func TestListOffsetsV0ReturnsAnOffset(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	mustCreateTopic(t, context.Background(), cl, "t", 1)
	produceN(t, context.Background(), cl, "t", 5)
	c := openRaw(t, addr)
	for ts, want := range map[int64]int64{-1: 5, -2: 0} {
		lo := kmsg.NewPtrListOffsetsRequest()
		lo.SetVersion(0)
		lo.ReplicaID = -1
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = "t"
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Timestamp, rp.MaxNumOffsets = ts, 1
		rt.Partitions = append(rt.Partitions, rp)
		lo.Topics = append(lo.Topics, rt)
		var resp kmsg.ListOffsetsResponse
		if !c.do(lo, &resp) {
			t.Fatal("connection closed")
		}
		got := resp.Topics[0].Partitions[0].OldStyleOffsets
		if len(got) != 1 || got[0] != want {
			t.Errorf("ListOffsets v0 timestamp %d: offsets %v, want [%d]", ts, got, want)
		}
	}
}

func TestCreateTopicsValidatesWhatItStores(t *testing.T) {
	c := openRaw(t, startBroker(t, 4))
	create := func(topics ...kmsg.CreateTopicsRequestTopic) []kmsg.CreateTopicsResponseTopic {
		t.Helper()
		ct := kmsg.NewPtrCreateTopicsRequest()
		ct.SetVersion(4)
		ct.TimeoutMillis = 5000
		ct.Topics = topics
		var resp kmsg.CreateTopicsResponse
		if !c.do(ct, &resp) {
			t.Fatal("connection closed")
		}
		return resp.Topics
	}
	topic := func(name string, partitions int32, cfg ...string) kmsg.CreateTopicsRequestTopic {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, partitions, 3 // any factor is accepted: single node.
		for i := 0; i+1 < len(cfg); i += 2 {
			rc := kmsg.NewCreateTopicsRequestTopicConfig()
			rc.Name, rc.Value = cfg[i], kmsg.StringPtr(cfg[i+1])
			rt.Configs = append(rt.Configs, rc)
		}
		return rt
	}
	for _, tc := range []struct {
		name string
		req  kmsg.CreateTopicsRequestTopic
		want int16
	}{
		{"plain", topic("ok", 2), 0},
		{"default partitions", topic("dflt", -1), 0},
		{"real configs", topic("cfg", 1, "retention.ms", "1000", "cleanup.policy", "compact,delete", "min.insync.replicas", "2"), 0},
		{"zero partitions", topic("p0", 0), 37},
		{"negative partitions", topic("pneg", -5), 37},
		{"unknown config key", topic("badkey", 1, "no.such.config", "1"), 40},
		{"non-numeric value", topic("badval", 1, "segment.bytes", "banana"), 40},
		{"bad policy", topic("badpol", 1, "cleanup.policy", "shred"), 40},
		{"bad bool", topic("badbool", 1, "preallocate", "yes"), 40},
	} {
		if got := create(tc.req); got[0].ErrorCode != tc.want {
			t.Errorf("%s: code %d (%v), want %d", tc.name, got[0].ErrorCode, deref(got[0].ErrorMessage), tc.want)
		}
	}
	// The same name twice in one request: neither is created.
	got := create(topic("twice", 1), topic("twice", 1))
	if len(got) != 2 || got[0].ErrorCode != 42 || got[1].ErrorCode != 42 {
		t.Errorf("a duplicated name: codes %d and %d, want INVALID_REQUEST (42) for both", got[0].ErrorCode, got[1].ErrorCode)
	}
	if again := create(topic("twice", 1)); again[0].ErrorCode != 0 {
		t.Errorf("after the refused duplicate the name should be free, got code %d", again[0].ErrorCode)
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
