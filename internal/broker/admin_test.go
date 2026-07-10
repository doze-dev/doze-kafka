package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// metadataTopics returns the set of topic names the broker currently reports (an
// all-topics Metadata request — which never auto-creates).
func metadataTopics(t *testing.T, ctx context.Context, cl *kgo.Client) map[string]bool {
	t.Helper()
	resp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	out := make(map[string]bool)
	for _, tpc := range resp.Topics {
		if tpc.Topic != nil {
			out[*tpc.Topic] = true
		}
	}
	return out
}

// startGroupMember joins a group with the given protocol ("classic" or "consumer")
// and consumes one record — guaranteeing it has joined and been assigned — then
// stays alive (heartbeating) until the returned stop is called.
func startGroupMember(t *testing.T, ctx context.Context, addr, group, topic, proto string) func() {
	t.Helper()
	opts := []kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DialTimeout(2 * time.Second),
	}
	if proto == "consumer" {
		opts = append(opts, kip848Opts()...) // the real KIP-848 opt-in (see phase6_test.go).
	} else {
		opts = append(opts, kgo.GroupProtocol("classic"))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("group member %s: %v", group, err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(deadline) {
			cl.Close()
			t.Fatalf("group member %s never consumed (did not join)", group)
		}
		fs := cl.PollFetches(ctx)
		if fs.NumRecords() > 0 {
			return cl.Close
		}
	}
}

// TestAdmin_DeleteTopics covers DeleteTopics: a created, produced-to topic is
// deleted (registry + on-disk data), disappears from metadata, and an unknown topic
// reports UNKNOWN_TOPIC_OR_PARTITION.
func TestAdmin_DeleteTopics(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	mustCreateTopic(t, ctx, cl, "victim", 2)
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "victim", Value: []byte("x")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	if !metadataTopics(t, ctx, cl)["victim"] {
		t.Fatal("topic 'victim' missing after create")
	}

	// franz negotiates DeleteTopics v6, which carries topics by (name-or-id) in
	// Topics rather than the v0-5 TopicNames list.
	deleteReq := func(name string) *kmsg.DeleteTopicsRequest {
		r := kmsg.NewPtrDeleteTopicsRequest()
		rt := kmsg.NewDeleteTopicsRequestTopic()
		n := name
		rt.Topic = &n
		r.Topics = append(r.Topics, rt)
		r.TimeoutMillis = 5000
		return r
	}
	resp, err := deleteReq("victim").RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("delete topics: %v", err)
	}
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("delete victim: %+v", resp.Topics)
	}
	if metadataTopics(t, ctx, cl)["victim"] {
		t.Fatal("topic 'victim' still present after delete")
	}

	// Deleting an unknown topic is reported, not fatal.
	resp2, err := deleteReq("ghost").RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("delete ghost: %v", err)
	}
	if resp2.Topics[0].ErrorCode != 3 { // UNKNOWN_TOPIC_OR_PARTITION
		t.Fatalf("delete unknown topic error = %d, want 3", resp2.Topics[0].ErrorCode)
	}
}

// TestAdmin_ListGroups covers ListGroups across both protocols: a classic and a
// KIP-848 group are both listed with the correct group type.
func TestAdmin_ListGroups(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	producer := newClient(t, addr)
	for _, topic := range []string{"lg-classic", "lg-new"} {
		mustCreateTopic(t, ctx, producer, topic, 1)
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr(); err != nil {
			t.Fatalf("produce %s: %v", topic, err)
		}
	}
	producer.Close()

	defer startGroupMember(t, ctx, addr, "grp-classic", "lg-classic", "classic")()
	defer startGroupMember(t, ctx, addr, "grp-new", "lg-new", "consumer")()

	admin := newClient(t, addr)
	defer admin.Close()

	want := map[string]string{"grp-classic": "classic", "grp-new": "consumer"}
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := kmsg.NewPtrListGroupsRequest().RequestWith(ctx, admin)
		if err != nil {
			t.Fatalf("list groups: %v", err)
		}
		got := make(map[string]string)
		for _, g := range resp.Groups {
			got[g.Group] = g.GroupType
		}
		ok := true
		for id, typ := range want {
			if got[id] != typ {
				ok = false
			}
		}
		if ok {
			return // both groups listed with the right types.
		}
		if time.Now().After(deadline) {
			t.Fatalf("ListGroups never showed both groups with correct types; got %v", got)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// TestAdmin_DescribeGroups covers DescribeGroups for a classic group (a live member
// appears) and reports an unknown group as Dead rather than erroring.
func TestAdmin_DescribeGroups(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	producer := newClient(t, addr)
	mustCreateTopic(t, ctx, producer, "dg-topic", 1)
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: "dg-topic", Value: []byte("x")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	producer.Close()

	defer startGroupMember(t, ctx, addr, "dg-group", "dg-topic", "classic")()

	admin := newClient(t, addr)
	defer admin.Close()

	deadline := time.Now().Add(15 * time.Second)
	for {
		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{"dg-group"}
		resp, err := req.RequestWith(ctx, admin)
		if err != nil {
			t.Fatalf("describe groups: %v", err)
		}
		g := resp.Groups[0]
		if g.ErrorCode == 0 && len(g.Members) >= 1 && g.Members[0].MemberID != "" {
			break // a live member is described.
		}
		if time.Now().After(deadline) {
			t.Fatalf("DescribeGroups never showed a member for dg-group: %+v", g)
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Unknown group -> Dead, no error.
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{"nope"}
	resp, err := req.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("describe unknown: %v", err)
	}
	if resp.Groups[0].ErrorCode != 0 || resp.Groups[0].State != "Dead" {
		t.Fatalf("unknown group = code %d state %q, want 0/Dead", resp.Groups[0].ErrorCode, resp.Groups[0].State)
	}
}

// TestAdmin_ConsumerGroupDescribe covers the KIP-848 describe: a live new-protocol
// member is described with its assignment, and an unknown group reports
// GROUP_ID_NOT_FOUND.
func TestAdmin_ConsumerGroupDescribe(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	producer := newClient(t, addr)
	mustCreateTopic(t, ctx, producer, "cgd-topic", 1)
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: "cgd-topic", Value: []byte("x")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	producer.Close()

	defer startGroupMember(t, ctx, addr, "cgd-group", "cgd-topic", "consumer")()

	admin := newClient(t, addr)
	defer admin.Close()

	deadline := time.Now().Add(15 * time.Second)
	for {
		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{"cgd-group"}
		resp, err := req.RequestWith(ctx, admin)
		if err != nil {
			t.Fatalf("consumer group describe: %v", err)
		}
		g := resp.Groups[0]
		if g.ErrorCode == 0 && g.Epoch > 0 && len(g.Members) >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ConsumerGroupDescribe never showed a member for cgd-group: %+v", g)
		}
		time.Sleep(300 * time.Millisecond)
	}

	req := kmsg.NewPtrConsumerGroupDescribeRequest()
	req.Groups = []string{"nope848"}
	resp, err := req.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("describe unknown 848: %v", err)
	}
	if resp.Groups[0].ErrorCode != 69 { // GROUP_ID_NOT_FOUND
		t.Fatalf("unknown 848 group error = %d, want 69", resp.Groups[0].ErrorCode)
	}
}

// TestAdmin_DescribeConfigs covers DescribeConfigs for a topic (defaults overlaid
// with a per-topic override) and for the broker, and errors on an unknown topic.
func TestAdmin_DescribeConfigs(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create a topic with an explicit retention.ms override.
	creq := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = "cfg-topic"
	ct.NumPartitions = 1
	ct.ReplicationFactor = 1
	cc := kmsg.NewCreateTopicsRequestTopicConfig()
	cc.Name = "retention.ms"
	rms := "123456"
	cc.Value = &rms
	ct.Configs = append(ct.Configs, cc)
	creq.Topics = append(creq.Topics, ct)
	if cresp, err := creq.RequestWith(ctx, cl); err != nil || cresp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create cfg-topic: err=%v code=%d", err, cresp.Topics[0].ErrorCode)
	}

	describe := func(rtype kmsg.ConfigResourceType, name string) map[string]string {
		req := kmsg.NewPtrDescribeConfigsRequest()
		rr := kmsg.NewDescribeConfigsRequestResource()
		rr.ResourceType = rtype
		rr.ResourceName = name
		req.Resources = append(req.Resources, rr)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("describe configs: %v", err)
		}
		res := resp.Resources[0]
		if res.ErrorCode != 0 {
			t.Fatalf("describe %s %q: error code %d", rtype, name, res.ErrorCode)
		}
		out := map[string]string{}
		for _, c := range res.Configs {
			if c.Value != nil {
				out[c.Name] = *c.Value
			}
		}
		return out
	}

	topicCfg := describe(kmsg.ConfigResourceTypeTopic, "cfg-topic")
	if topicCfg["retention.ms"] != "123456" {
		t.Fatalf("topic retention.ms = %q, want 123456 (the override)", topicCfg["retention.ms"])
	}
	if _, ok := topicCfg["segment.bytes"]; !ok {
		t.Fatal("topic configs missing the default segment.bytes")
	}

	brokerCfg := describe(kmsg.ConfigResourceTypeBroker, "1")
	if _, ok := brokerCfg["log.segment.bytes"]; !ok {
		t.Fatalf("broker configs missing log.segment.bytes; got %v", brokerCfg)
	}

	// Unknown topic -> error.
	req := kmsg.NewPtrDescribeConfigsRequest()
	rr := kmsg.NewDescribeConfigsRequestResource()
	rr.ResourceType = kmsg.ConfigResourceTypeTopic
	rr.ResourceName = "ghost-topic"
	req.Resources = append(req.Resources, rr)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("describe unknown: %v", err)
	}
	if resp.Resources[0].ErrorCode != 3 { // UNKNOWN_TOPIC_OR_PARTITION
		t.Fatalf("unknown topic config error = %d, want 3", resp.Resources[0].ErrorCode)
	}
}

// TestAdmin_DescribeCluster covers DescribeCluster: the single node is the only
// broker and its own controller.
func TestAdmin_DescribeCluster(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := kmsg.NewPtrDescribeClusterRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("describe cluster: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("describe cluster error: %d", resp.ErrorCode)
	}
	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(resp.Brokers))
	}
	if resp.ControllerID != resp.Brokers[0].NodeID {
		t.Fatalf("controller %d != broker node %d", resp.ControllerID, resp.Brokers[0].NodeID)
	}
	if resp.ClusterID == "" {
		t.Fatal("empty cluster id")
	}
}

// TestApiVersions_AdvertisedSurface guards the client-visible API surface: a Kafka-4
// broker must advertise every API it serves. This catches an implemented handler
// that was never advertised (and so is invisible to clients).
func TestApiVersions_AdvertisedSurface(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := kmsg.NewPtrApiVersionsRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("api versions: %v", err)
	}
	advertised := make(map[int16]bool)
	for _, k := range resp.ApiKeys {
		advertised[k.ApiKey] = true
	}

	// Every API the version-4 broker serves (SASL excluded: auth is disabled here).
	want := map[int16]string{
		0: "Produce", 1: "Fetch", 2: "ListOffsets", 3: "Metadata",
		8: "OffsetCommit", 9: "OffsetFetch", 10: "FindCoordinator", 11: "JoinGroup",
		12: "Heartbeat", 13: "LeaveGroup", 14: "SyncGroup", 15: "DescribeGroups",
		16: "ListGroups", 18: "ApiVersions", 19: "CreateTopics", 20: "DeleteTopics",
		22: "InitProducerId", 32: "DescribeConfigs", 60: "DescribeCluster",
		68: "ConsumerGroupHeartbeat", 69: "ConsumerGroupDescribe",
	}
	for key, name := range want {
		if !advertised[key] {
			t.Errorf("API %s (%d) is served but not advertised in ApiVersions", name, key)
		}
	}
}
