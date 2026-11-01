package broker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// Describing and altering configuration.

func describeConfigs(t *testing.T, b *Broker, version int16, rt kmsg.ConfigResourceType, name string) kmsg.DescribeConfigsResponseResource {
	t.Helper()
	req := kmsg.NewPtrDescribeConfigsRequest()
	res := kmsg.NewDescribeConfigsRequestResource()
	res.ResourceType, res.ResourceName = rt, name
	req.Resources = append(req.Resources, res)
	return callAPI(t, b.handleDescribeConfigs, req, version).Resources[0]
}

// A topic's configuration is the broker's defaults overlaid with what the topic set,
// and the broker's own is static and read-only. Version 0 says whether a value is a
// default; version 1 on says where it came from.
func TestDescribeConfigsSaysWhereEachValueCameFrom(t *testing.T) {
	b, _, _ := newAdminBroker(t)
	topic := newTopic("c", 1, 1)
	ret := "1234"
	cfg := kmsg.NewCreateTopicsRequestTopicConfig()
	cfg.Name, cfg.Value = "retention.ms", &ret
	topic.Configs = append(topic.Configs, cfg)
	if got := createOne(t, b, topic); got.ErrorCode != 0 {
		t.Fatalf("create: %d %v", got.ErrorCode, got.ErrorMessage)
	}

	for _, version := range []int16{0, 4} {
		res := describeConfigs(t, b, version, kmsg.ConfigResourceTypeTopic, "c")
		var own, deflt *kmsg.DescribeConfigsResponseResourceConfig
		for _, c := range res.Configs {
			c := c
			if c.Name == "retention.ms" {
				own = &c
			} else if deflt == nil {
				deflt = &c
			}
		}
		if own == nil || deflt == nil {
			t.Fatalf("v%d: a topic's configs are %d entries, want its own and the defaults", version, len(res.Configs))
		}
		if own.Value == nil || *own.Value != "1234" || own.IsDefault || own.ReadOnly {
			t.Errorf("v%d, a value the topic set: %+v, want 1234, not default, not read-only", version, *own)
		}
		if deflt.IsDefault != (version == 0) || deflt.ReadOnly {
			t.Errorf("v%d, %s, the broker's default: %+v", version, deflt.Name, *deflt)
		}
		if version >= 1 && (own.Source != kmsg.ConfigSourceDynamicTopicConfig || deflt.Source != kmsg.ConfigSourceDefaultConfig) {
			t.Errorf("v%d sources: %v for the topic's own, %v for the default", version, own.Source, deflt.Source)
		}

		broker := describeConfigs(t, b, version, kmsg.ConfigResourceTypeBroker, "0")
		if broker.ErrorCode != 0 || len(broker.Configs) == 0 {
			t.Fatalf("v%d, the broker's configs: code %d, %d entries", version, broker.ErrorCode, len(broker.Configs))
		}
		for _, c := range broker.Configs {
			if c.IsDefault || !c.ReadOnly || (version >= 1 && c.Source != kmsg.ConfigSourceStaticBrokerConfig) {
				t.Errorf("v%d, %s: %+v, want a static, read-only value that is not a default", version, c.Name, c)
			}
		}
	}
}

func alterConfigsAt(t *testing.T, b *Broker, rt kmsg.ConfigResourceType, name string, configs map[string]string) kmsg.AlterConfigsResponseResource {
	t.Helper()
	req := kmsg.NewPtrAlterConfigsRequest()
	res := kmsg.NewAlterConfigsRequestResource()
	res.ResourceType, res.ResourceName = rt, name
	for k, v := range configs {
		c := kmsg.NewAlterConfigsRequestResourceConfig()
		v := v
		c.Name, c.Value = k, &v
		res.Configs = append(res.Configs, c)
	}
	req.Resources = append(req.Resources, res)
	return callAPI(t, b.handleAlterConfigs, req, 2).Resources[0]
}

func incrementalAlter(t *testing.T, b *Broker, rt kmsg.ConfigResourceType, name string, op kmsg.IncrementalAlterConfigOp, key, value string) kmsg.IncrementalAlterConfigsResponseResource {
	t.Helper()
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	res := kmsg.NewIncrementalAlterConfigsRequestResource()
	res.ResourceType, res.ResourceName = rt, name
	c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	c.Name, c.Op, c.Value = key, op, &value
	res.Configs = append(res.Configs, c)
	req.Resources = append(req.Resources, res)
	return callAPI(t, b.handleIncrementalAlterConfigs, req, 1).Resources[0]
}

// Only topics can be altered: a broker is read-only here, a type that is neither is
// refused, and the right needed is on the resource that is altered.
func TestAlterConfigsOnlyAltersTopicsAndHoldsEachResourceToItsRight(t *testing.T) {
	b, _, _ := newAdminBroker(t)
	if got := createOne(t, b, newTopic("c", 1, 1)); got.ErrorCode != 0 {
		t.Fatal(got.ErrorCode)
	}
	type outcome struct {
		code int16
		msg  string
	}
	both := func(rt kmsg.ConfigResourceType, name string) (full, incremental outcome) {
		f := alterConfigsAt(t, b, rt, name, map[string]string{"retention.ms": "5000"})
		i := incrementalAlter(t, b, rt, name, kmsg.IncrementalAlterConfigOpSet, "retention.ms", "5000")
		if f.ErrorMessage != nil {
			full.msg = *f.ErrorMessage
		}
		if i.ErrorMessage != nil {
			incremental.msg = *i.ErrorMessage
		}
		full.code, incremental.code = f.ErrorCode, i.ErrorCode
		return
	}
	for _, o := range func() []outcome { f, i := both(kmsg.ConfigResourceTypeBroker, "0"); return []outcome{f, i} }() {
		if o.code != kerr.InvalidRequest.Code || o.msg != errBrokerConfigReadOnly {
			t.Errorf("altering the broker: %+v, want INVALID_REQUEST saying it is read-only", o)
		}
	}
	for _, o := range func() []outcome { f, i := both(kmsg.ConfigResourceTypeBrokerLogger, "0"); return []outcome{f, i} }() {
		if o.code != kerr.InvalidRequest.Code || o.msg != "unsupported config resource type" {
			t.Errorf("altering a broker logger: %+v, want INVALID_REQUEST unsupported", o)
		}
	}
	for _, o := range func() []outcome { f, i := both(kmsg.ConfigResourceTypeTopic, "c"); return []outcome{f, i} }() {
		if o.code != 0 {
			t.Errorf("altering a topic: %+v", o)
		}
	}

	// A right on the cluster is not a right on a topic, nor the other way round.
	denyUnlessAllowed(b, allowAnon(resTopic, "c", opAlterConfigs))
	for _, o := range func() []outcome { f, i := both(kmsg.ConfigResourceTypeTopic, "c"); return []outcome{f, i} }() {
		if o.code != 0 {
			t.Errorf("a topic, with the right on it and none on the cluster: %+v", o)
		}
	}
	for _, o := range func() []outcome { f, i := both(kmsg.ConfigResourceTypeBroker, "0"); return []outcome{f, i} }() {
		if o.code != kerr.ClusterAuthorizationFailed.Code {
			t.Errorf("the broker, with no right on the cluster: %+v, want CLUSTER_AUTHORIZATION_FAILED", o)
		}
	}
	denyUnlessAllowed(b, allowAnon(resCluster, clusterResourceName, opAlterConfigs))
	for _, o := range func() []outcome { f, i := both(kmsg.ConfigResourceTypeTopic, "c"); return []outcome{f, i} }() {
		if o.code != kerr.TopicAuthorizationFailed.Code {
			t.Errorf("a topic, with the right on the cluster and none on it: %+v, want TOPIC_AUTHORIZATION_FAILED", o)
		}
	}
	for _, o := range func() []outcome { f, i := both(kmsg.ConfigResourceTypeBroker, "0"); return []outcome{f, i} }() {
		if o.code != kerr.InvalidRequest.Code || o.msg != errBrokerConfigReadOnly {
			t.Errorf("the broker, with the right on the cluster: %+v, want it read-only", o)
		}
	}
}

// A list config is added to once and subtracted from where it is; an unset
// cleanup.policy starts from delete.
func TestIncrementalAlterAppendsAndSubtractsListItems(t *testing.T) {
	b, _, _ := newAdminBroker(t)
	if got := createOne(t, b, newTopic("c", 1, 1)); got.ErrorCode != 0 {
		t.Fatal(got.ErrorCode)
	}
	policy := func() string {
		tp, _, _ := b.meta.Topic("c")
		return tp.Config["cleanup.policy"]
	}
	step := func(op kmsg.IncrementalAlterConfigOp, value, want string) {
		t.Helper()
		if r := incrementalAlter(t, b, kmsg.ConfigResourceTypeTopic, "c", op, "cleanup.policy", value); r.ErrorCode != 0 {
			t.Fatalf("op %d %q: code %d %v", op, value, r.ErrorCode, r.ErrorMessage)
		}
		if got := policy(); got != want {
			t.Errorf("op %d %q: cleanup.policy %q, want %q", op, value, got, want)
		}
	}
	step(kmsg.IncrementalAlterConfigOpAppend, "compact", "delete,compact")
	step(kmsg.IncrementalAlterConfigOpAppend, "compact", "delete,compact")
	step(kmsg.IncrementalAlterConfigOpAppend, "delete", "delete,compact")
	step(kmsg.IncrementalAlterConfigOpSubtract, "delete", "compact")
	step(kmsg.IncrementalAlterConfigOpAppend, "compact", "compact")
	step(kmsg.IncrementalAlterConfigOpSubtract, "nothing", "compact")
	step(kmsg.IncrementalAlterConfigOpAppend, "delete", "compact,delete")
	step(kmsg.IncrementalAlterConfigOpSubtract, "compact", "delete")
	if r := incrementalAlter(t, b, kmsg.ConfigResourceTypeTopic, "c", kmsg.IncrementalAlterConfigOpAppend, "retention.ms", "1"); r.ErrorCode != kerr.InvalidRequest.Code {
		t.Errorf("appending to a config that is not a list: code %d", r.ErrorCode)
	}
}
