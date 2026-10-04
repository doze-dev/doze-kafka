package broker_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Per-topic configuration: retention.ms, retention.bytes and segment.bytes set on
// a topic are in force for that topic, at creation and when altered later.

func createTopicWith(t *testing.T, ctx context.Context, cl *kgo.Client, name string, cfg map[string]string) int16 {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, 1, 1
	for k, v := range cfg {
		rc := kmsg.NewCreateTopicsRequestTopicConfig()
		rc.Name, rc.Value = k, kmsg.StringPtr(v)
		rt.Configs = append(rt.Configs, rc)
	}
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) != 1 {
		t.Fatalf("CreateTopics: %v %+v", err, resp)
	}
	return resp.Topics[0].ErrorCode
}

// fill produces n records of about 100 bytes each, one batch per record.
func fill(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, n int) {
	t.Helper()
	pad := strings.Repeat("x", 90)
	for i := range n {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "%05d %s", i, pad)}).FirstErr(); err != nil {
			t.Fatalf("produce to %s: %v", topic, err)
		}
	}
}

// earliestSoon waits up to wait for the topic's first offset to pass 0.
func earliestSoon(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, wait time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		if e := listOffset(t, ctx, cl, topic, 0, -2); e > 0 || time.Now().After(deadline) {
			return e
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type alterOp struct {
	name  string
	op    kmsg.IncrementalAlterConfigOp
	value string
}

func incrementalAlter(t *testing.T, ctx context.Context, cl *kgo.Client, resource kmsg.ConfigResourceType, name string, validateOnly bool, ops ...alterOp) (int16, string) {
	t.Helper()
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	req.ValidateOnly = validateOnly
	res := kmsg.NewIncrementalAlterConfigsRequestResource()
	res.ResourceType, res.ResourceName = resource, name
	for _, o := range ops {
		c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		c.Name, c.Op = o.name, o.op
		if o.op != kmsg.IncrementalAlterConfigOpDelete {
			c.Value = kmsg.StringPtr(o.value)
		}
		res.Configs = append(res.Configs, c)
	}
	req.Resources = append(req.Resources, res)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Resources) != 1 {
		t.Fatalf("IncrementalAlterConfigs: %v %+v", err, resp)
	}
	msg := ""
	if resp.Resources[0].ErrorMessage != nil {
		msg = *resp.Resources[0].ErrorMessage
	}
	return resp.Resources[0].ErrorCode, msg
}

// describe returns a topic's configuration as DescribeConfigs reports it:
// value, and whether it is the topic's own.
func describe(t *testing.T, ctx context.Context, cl *kgo.Client, topic string) (values map[string]string, own map[string]bool) {
	t.Helper()
	req := kmsg.NewPtrDescribeConfigsRequest()
	res := kmsg.NewDescribeConfigsRequestResource()
	res.ResourceType, res.ResourceName = kmsg.ConfigResourceTypeTopic, topic
	req.Resources = append(req.Resources, res)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Resources) != 1 || resp.Resources[0].ErrorCode != 0 {
		t.Fatalf("DescribeConfigs: %v %+v", err, resp)
	}
	values, own = map[string]string{}, map[string]bool{}
	for _, c := range resp.Resources[0].Configs {
		if c.Value != nil {
			values[c.Name] = *c.Value
		}
		own[c.Name] = c.Source == kmsg.ConfigSourceDynamicTopicConfig
	}
	return values, own
}

const (
	opSet      = kmsg.IncrementalAlterConfigOpSet
	opDelete   = kmsg.IncrementalAlterConfigOpDelete
	opAppend   = kmsg.IncrementalAlterConfigOpAppend
	opSubtract = kmsg.IncrementalAlterConfigOpSubtract
)

// A topic created with its own retention and segment size is trimmed to them,
// while a topic beside it on the same broker (which keeps everything) is not.
// These values used to be stored and reported and to change nothing.
func TestTopicConfig_RetentionAndSegmentSizeSetAtCreationAreInForce(t *testing.T) {
	addr := startBroker(t, 4) // the broker itself: no retention, 128 MiB segments.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	defer cl.Close()

	if code := createTopicWith(t, ctx, cl, "small", map[string]string{"segment.bytes": "2048", "retention.bytes": "8192"}); code != 0 {
		t.Fatalf("create: code %d", code)
	}
	mustCreateTopic(t, ctx, cl, "plain", 1)
	fill(t, ctx, cl, "small", 400) // ~60 KiB against an 8 KiB bound.
	fill(t, ctx, cl, "plain", 400)

	if e := earliestSoon(t, ctx, cl, "small", 5*time.Second); e == 0 {
		t.Error("a topic with retention.bytes=8192 holding ~60 KiB was not trimmed")
	}
	if e := listOffset(t, ctx, cl, "plain", 0, -2); e != 0 {
		t.Errorf("a topic with no retention of its own lost records: earliest is %d", e)
	}
	// What is left is about the bound (it is enforced in whole segments).
	earliest, latest := listOffset(t, ctx, cl, "small", 0, -2), listOffset(t, ctx, cl, "small", 0, -1)
	if kept := latest - earliest; latest != 400 || kept < 20 || kept > 120 {
		t.Errorf("retention.bytes=8192 over ~150-byte records kept %d of %d", kept, latest)
	}
}

// retention.ms by age, and -1 as "keep everything" on a broker that otherwise
// trims: a topic's own setting wins in both directions.
func TestTopicConfig_ATopicsOwnRetentionBeatsTheBrokers(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Version = 4
	cfg.ListenAddr = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg.DataDir, cfg.MetaDB = filepath.Join(dir, "data"), filepath.Join(dir, "meta.db")
	cfg.SegmentBytes = 2048
	cfg.RetentionBytes = 8192 // the broker trims everything to ~8 KiB…
	stop := runBroker(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl := newClient(t, cfg.ListenAddr)
	// …except a topic that says to keep everything.
	if code := createTopicWith(t, ctx, cl, "forever", map[string]string{"retention.bytes": "-1"}); code != 0 {
		t.Fatalf("create: %d", code)
	}
	mustCreateTopic(t, ctx, cl, "default", 1)
	if code := createTopicWith(t, ctx, cl, "brief", map[string]string{"retention.bytes": "-1", "retention.ms": "300"}); code != 0 {
		t.Fatalf("create: %d", code)
	}
	for _, topic := range []string{"forever", "default", "brief"} {
		fill(t, ctx, cl, topic, 300)
	}
	if e := earliestSoon(t, ctx, cl, "default", 5*time.Second); e == 0 {
		t.Fatal("the broker's own retention is not being applied at all")
	}
	if e := earliestSoon(t, ctx, cl, "brief", 5*time.Second); e == 0 {
		t.Error("retention.ms=300 did not remove records several hundred milliseconds old")
	}
	if e := listOffset(t, ctx, cl, "forever", 0, -2); e != 0 {
		t.Errorf("a topic with retention.bytes=-1 was trimmed by the broker's default: earliest %d", e)
	}
	cl.Close()
	stop()

	// After a restart nothing has been opened yet, and the first sweep goes over
	// the directories as they lie. The topic that keeps everything must still do so.
	stop = runBroker(t, cfg)
	defer stop()
	time.Sleep(1500 * time.Millisecond) // the start-up sweep, and one more.
	cl = newClient(t, cfg.ListenAddr)
	defer cl.Close()
	if e, l := listOffset(t, ctx, cl, "forever", 0, -2), listOffset(t, ctx, cl, "forever", 0, -1); e != 0 || l != 300 {
		t.Errorf("after a restart the keep-everything topic holds %d–%d, want 0–300", e, l)
	}
}

// A topic's retention can be given, changed and taken away after it exists.
func TestTopicConfig_AlterTakesEffectAndSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	stop := runBrokerAt(t, addr, dir, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl := newClient(t, addr)

	topic := kmsg.ConfigResourceTypeTopic
	mustCreateTopic(t, ctx, cl, "t", 1)
	fill(t, ctx, cl, "t", 50) // opens the partition with the broker's 128 MiB segments.
	if code, msg := incrementalAlter(t, ctx, cl, topic, "t", false, alterOp{"segment.bytes", opSet, "2048"}, alterOp{"retention.bytes", opSet, "8192"}); code != 0 {
		t.Fatalf("alter: %d %s", code, msg)
	}
	values, own := describe(t, ctx, cl, "t")
	if values["segment.bytes"] != "2048" || values["retention.bytes"] != "8192" || !own["segment.bytes"] || !own["retention.bytes"] || own["retention.ms"] {
		t.Errorf("DescribeConfigs after the alter: %v (own: %v)", values, own)
	}
	// The partition was already open: the new segment size applies from the next
	// append, so the log starts rolling and retention has segments to remove.
	fill(t, ctx, cl, "t", 400)
	if e := earliestSoon(t, ctx, cl, "t", 5*time.Second); e == 0 {
		t.Error("retention set by AlterConfigs on an open partition was not applied")
	}

	// A validate-only request changes nothing.
	if code, _ := incrementalAlter(t, ctx, cl, topic, "t", true, alterOp{"retention.bytes", opSet, "1"}); code != 0 {
		t.Errorf("validate-only: %d", code)
	}
	if values, _ := describe(t, ctx, cl, "t"); values["retention.bytes"] != "8192" {
		t.Errorf("a validate-only alter changed the topic: retention.bytes=%s", values["retention.bytes"])
	}
	cl.Close()
	stop()

	stop = runBrokerAt(t, addr, dir, 4)
	defer stop()
	cl = newClient(t, addr)
	defer cl.Close()
	if values, own := describe(t, ctx, cl, "t"); values["retention.bytes"] != "8192" || !own["retention.bytes"] {
		t.Errorf("after a restart the topic's retention.bytes is %q (own=%v)", values["retention.bytes"], own["retention.bytes"])
	}
	// Taking it away returns the topic to the broker's default: keep everything.
	if code, msg := incrementalAlter(t, ctx, cl, topic, "t", false, alterOp{"retention.bytes", opDelete, ""}); code != 0 {
		t.Fatalf("delete: %d %s", code, msg)
	}
	if values, own := describe(t, ctx, cl, "t"); values["retention.bytes"] != "-1" || own["retention.bytes"] || !own["segment.bytes"] {
		t.Errorf("after deleting the override: retention.bytes=%s own=%v; segment.bytes own=%v", values["retention.bytes"], own["retention.bytes"], own["segment.bytes"])
	}
	before := listOffset(t, ctx, cl, "t", 0, -2)
	fill(t, ctx, cl, "t", 300)
	time.Sleep(2500 * time.Millisecond)
	if after := listOffset(t, ctx, cl, "t", 0, -2); after != before {
		t.Errorf("with its retention removed the topic was still trimmed: earliest %d -> %d", before, after)
	}
}

func TestTopicConfig_AlterIsValidatedAndRefusesWhatItCannotDo(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "t", 1)
	topic := kmsg.ConfigResourceTypeTopic
	alter := func(ops ...alterOp) (int16, string) { return incrementalAlter(t, ctx, cl, topic, "t", false, ops...) }
	if code, _ := alter(alterOp{"retention.ms", opSet, "60000"}); code != 0 {
		t.Fatalf("set: %d", code)
	}

	for _, tc := range []struct {
		what string
		op   alterOp
		want int16
	}{
		{"a value that is not a number", alterOp{"retention.ms", opSet, "soon"}, kerr.InvalidConfig.Code},
		{"a key Kafka does not have", alterOp{"no.such.config", opSet, "1"}, kerr.InvalidConfig.Code},
		{"a segment too small to be sane", alterOp{"segment.bytes", opSet, "100"}, kerr.InvalidConfig.Code},
		{"a retention that overflows", alterOp{"retention.ms", opSet, "9223372036854775807"}, kerr.InvalidConfig.Code},
		{"appending to something that is not a list", alterOp{"retention.ms", opAppend, "5"}, kerr.InvalidRequest.Code},
	} {
		code, msg := alter(tc.op)
		if code != int16(tc.want) || msg == "" {
			t.Errorf("%s: code %d %q, want %d and a message", tc.what, code, msg, tc.want)
		}
	}
	// One bad config refuses the whole change, good ones included.
	if code, _ := alter(alterOp{"retention.bytes", opSet, "4096"}, alterOp{"segment.bytes", opSet, "banana"}); code != kerr.InvalidConfig.Code {
		t.Errorf("a change with one bad value: code %d", code)
	}
	if values, own := describe(t, ctx, cl, "t"); values["retention.ms"] != "60000" || own["retention.bytes"] {
		t.Errorf("a refused change altered the topic: %v", values)
	}

	if code, _ := incrementalAlter(t, ctx, cl, topic, "nope", false, alterOp{"retention.ms", opSet, "1"}); code != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("a topic that does not exist: code %d", code)
	}
	if code, msg := incrementalAlter(t, ctx, cl, kmsg.ConfigResourceTypeBroker, "1", false, alterOp{"log.retention.ms", opSet, "1"}); code != kerr.InvalidRequest.Code || !strings.Contains(msg, "read-only") {
		t.Errorf("altering the broker's own config: code %d %q, want INVALID_REQUEST saying it is read-only", code, msg)
	}

	// Lists: cleanup.policy starts from its default.
	if code, msg := alter(alterOp{"cleanup.policy", opAppend, "compact"}); code != 0 {
		t.Fatalf("append: %d %s", code, msg)
	}
	if values, _ := describe(t, ctx, cl, "t"); values["cleanup.policy"] != "delete,compact" {
		t.Errorf("cleanup.policy after appending compact: %q", values["cleanup.policy"])
	}
	alter(alterOp{"cleanup.policy", opSubtract, "delete"})
	if values, _ := describe(t, ctx, cl, "t"); values["cleanup.policy"] != "compact" {
		t.Errorf("cleanup.policy after subtracting delete: %q", values["cleanup.policy"])
	}

	// The original AlterConfigs replaces the topic's whole configuration.
	legacy := kmsg.NewPtrAlterConfigsRequest()
	res := kmsg.NewAlterConfigsRequestResource()
	res.ResourceType, res.ResourceName = topic, "t"
	c := kmsg.NewAlterConfigsRequestResourceConfig()
	c.Name, c.Value = "retention.bytes", kmsg.StringPtr("1048576")
	res.Configs = append(res.Configs, c)
	legacy.Resources = append(legacy.Resources, res)
	resp, err := legacy.RequestWith(ctx, cl)
	if err != nil || resp.Resources[0].ErrorCode != 0 {
		t.Fatalf("AlterConfigs: %v %+v", err, resp)
	}
	values, own := describe(t, ctx, cl, "t")
	if values["retention.bytes"] != "1048576" || own["retention.ms"] || own["cleanup.policy"] || values["cleanup.policy"] != "delete" {
		t.Errorf("after AlterConfigs with only retention.bytes: %v (own %v); everything else should be back to the default", values, own)
	}
}

// Older profiles offer what Kafka of that age had: no IncrementalAlterConfigs on 1.x.
func TestTopicConfig_AlterAPIsAreAdvertisedByProfile(t *testing.T) {
	for major, wantIncremental := range map[int]bool{1: false, 2: true, 4: true} {
		p, _ := protocol.ProfileFor(major)
		if _, ok := p.RangeFor(protocol.APIKey(kmsg.AlterConfigs)); !ok {
			t.Errorf("Kafka %d profile does not offer AlterConfigs", major)
		}
		if _, ok := p.RangeFor(protocol.APIKey(kmsg.IncrementalAlterConfigs)); ok != wantIncremental {
			t.Errorf("Kafka %d profile: IncrementalAlterConfigs offered=%v, want %v", major, ok, wantIncremental)
		}
	}
}

// max.message.bytes on a topic refuses a larger record batch, by name, and leaves
// other topics alone. It used to be stored and to refuse nothing.
func TestTopicConfig_MaxMessageBytesRefusesLargerBatches(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Uncompressed: the limit is on the batch as it arrives, and 5000 repeated
	// letters compress to almost nothing.
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ProducerBatchMaxBytes(1<<20), kgo.RecordRetries(1), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if code := createTopicWith(t, ctx, cl, "capped", map[string]string{"max.message.bytes": "2000"}); code != 0 {
		t.Fatalf("create: %d", code)
	}
	mustCreateTopic(t, ctx, cl, "open", 1)

	small, big := []byte(strings.Repeat("s", 500)), []byte(strings.Repeat("b", 5000))
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "capped", Value: small}).FirstErr(); err != nil {
		t.Fatalf("a batch under the limit: %v", err)
	}
	err = cl.ProduceSync(ctx, &kgo.Record{Topic: "capped", Value: big}).FirstErr()
	if err == nil || !strings.Contains(err.Error(), "MESSAGE_TOO_LARGE") {
		t.Errorf("a 5000-byte record on a topic with max.message.bytes=2000: %v, want MESSAGE_TOO_LARGE", err)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "open", Value: big}).FirstErr(); err != nil {
		t.Errorf("a topic with no limit refused the same record: %v", err)
	}
	if latest := listOffset(t, ctx, cl, "capped", 0, -1); latest != 1 {
		t.Errorf("the refused batch was stored anyway: log end %d, want 1", latest)
	}
	// Raising the limit lets it through.
	if code, msg := incrementalAlter(t, ctx, cl, kmsg.ConfigResourceTypeTopic, "capped", false, alterOp{"max.message.bytes", opSet, "100000"}); code != 0 {
		t.Fatalf("alter: %d %s", code, msg)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "capped", Value: big}).FirstErr(); err != nil {
		t.Errorf("after raising the limit: %v", err)
	}
}

// message.timestamp.type=LogAppendTime: a consumer sees the broker's clock, not
// whatever the producer wrote, and the batch is still one a client accepts (its
// checksum covers the fields that were changed).
func TestTopicConfig_LogAppendTimeReplacesTheProducersTimestamp(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	defer cl.Close()
	if code := createTopicWith(t, ctx, cl, "stamped", map[string]string{"message.timestamp.type": "LogAppendTime"}); code != 0 {
		t.Fatalf("create: %d", code)
	}
	mustCreateTopic(t, ctx, cl, "asis", 1)

	old := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	before := time.Now().Add(-time.Second)
	for _, topic := range []string{"stamped", "asis"} {
		// Idempotent (the client's default) and compressed: the paths with most to get wrong.
		res := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Timestamp: old, Value: []byte("v1")}, &kgo.Record{Topic: topic, Timestamp: old, Value: []byte("v2")})
		if err := res.FirstErr(); err != nil {
			t.Fatalf("produce to %s: %v", topic, err)
		}
	}
	read := func(topic string) []*kgo.Record {
		c, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		var out []*kgo.Record
		for len(out) < 2 && ctx.Err() == nil {
			fs := c.PollFetches(ctx)
			if errs := fs.Errors(); len(errs) > 0 {
				t.Fatalf("consume %s: %v", topic, errs)
			}
			out = append(out, fs.Records()...)
		}
		return out
	}
	for _, r := range read("stamped") {
		if r.Timestamp.Before(before) || r.Timestamp.After(time.Now().Add(time.Second)) {
			t.Errorf("LogAppendTime topic: record %q has timestamp %v, want the broker's clock (about now)", r.Value, r.Timestamp)
		}
		if r.Attrs.TimestampType() != 1 {
			t.Errorf("record %q is not marked as LogAppendTime", r.Value)
		}
	}
	for _, r := range read("asis") {
		if !r.Timestamp.Equal(old) {
			t.Errorf("CreateTime topic: record %q has timestamp %v, want the producer's %v", r.Value, r.Timestamp, old)
		}
	}
}
