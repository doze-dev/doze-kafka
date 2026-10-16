package conformance

import (
	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Scenarios for what code comments say Kafka does (conformance/claims/api.md, rows
// V6 to V8): each comment names its row, and TestKafkaBehaviourCommentsCiteAClaim
// holds every such comment to one.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "list-config-append", Run: listConfigAppend, Known: []Known{
			{Step: "*", Path: "Resources[0].Configs[0].ConfigType", Limitation: "DescribeConfigs does not say a config's type"},
			{Step: "subtract-delete-from-unset*", Limitation: "An empty cleanup.policy is refused"},
		}},
		Scenario{Name: "fetch-above-the-end", Run: fetchAboveTheEnd},
		Scenario{Name: "idempotent-write-from-topic-write", Run: idempotentWriteFromTopicWrite, Users: saslUsers, Admin: "admin",
			KafkaEnv: []string{
				"KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
				"KAFKA_SUPER_USERS=User:admin;User:ANONYMOUS",
			},
			Doze: func(c *config.Config) { c.ACLDefault, c.ACLSuperUsers = "deny", []string{"User:admin"} }},
	)
}

// describeTopicConfig asks for one config of a topic.
func describeTopicConfig(topic, name string) *kmsg.DescribeConfigsRequest {
	d := kmsg.NewPtrDescribeConfigsRequest()
	r := kmsg.NewDescribeConfigsRequestResource()
	r.ResourceType, r.ResourceName, r.ConfigNames = kmsg.ConfigResourceTypeTopic, topic, []string{name}
	d.Resources = append(d.Resources, r)
	return d
}

// listConfigAppend is V6: APPEND to a list config the topic does not set starts
// from the default (cleanup.policy: delete), and SUBTRACT from one likewise.
func listConfigAppend(e *Env) {
	e.Do("create", createTopics(false, newTopic("la", 1, 1), newTopic("ls", 1, 1)), 7)
	alter := func(label, topic string, op kmsg.IncrementalAlterConfigOp, value string) {
		a := kmsg.NewPtrIncrementalAlterConfigsRequest()
		r := kmsg.NewIncrementalAlterConfigsRequestResource()
		r.ResourceType, r.ResourceName = kmsg.ConfigResourceTypeTopic, topic
		c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		c.Name, c.Op, c.Value = "cleanup.policy", op, kmsg.StringPtr(value)
		r.Configs = append(r.Configs, c)
		a.Resources = append(a.Resources, r)
		got := e.Do(label, a, 1).(*kmsg.IncrementalAlterConfigsResponse)
		if len(got.Resources) != 1 || got.Resources[0].ErrorCode != 0 {
			e.Do(label+" then", describeTopicConfig(topic, "cleanup.policy"), 4)
			return
		}
		// The change reaches DescribeConfigs a moment after the answer (one
		// recording described the default): wait until the topic sets it.
		e.Await("cleanup.policy of "+topic+" is the topic's own", describeTopicConfig(topic, "cleanup.policy"), 4, func(r kmsg.Response) bool {
			rs := r.(*kmsg.DescribeConfigsResponse).Resources
			return len(rs) == 1 && len(rs[0].Configs) == 1 && rs[0].Configs[0].Source == kmsg.ConfigSourceDynamicTopicConfig
		})
		e.Do(label+" then", describeTopicConfig(topic, "cleanup.policy"), 4)
	}
	alter("append-compact-to-unset", "la", kmsg.IncrementalAlterConfigOpAppend, "compact")
	alter("subtract-delete-from-unset", "ls", kmsg.IncrementalAlterConfigOpSubtract, "delete")
}

// fetchAboveTheEnd is V7: a Fetch at an offset the log never reached is
// OFFSET_OUT_OF_RANGE, as is one below the log start; at the end it is an empty
// answer.
func fetchAboveTheEnd(e *Env) {
	e.Do("create", createTopics(false, newTopic("fa", 1, 1)), 7)
	e.Produce("produce-one", "fa", 0, ProduceRecord{TimestampMs: 1000, Key: "k", Value: "v"})
	at := func(label string, offset int64) {
		f := kmsg.NewPtrFetchRequest()
		f.ReplicaID, f.MaxWaitMillis, f.MinBytes, f.MaxBytes = -1, 0, 1, 1<<20
		ft := kmsg.NewFetchRequestTopic()
		ft.Topic = "fa"
		fp := kmsg.NewFetchRequestTopicPartition()
		fp.Partition, fp.FetchOffset, fp.PartitionMaxBytes, fp.CurrentLeaderEpoch, fp.LogStartOffset = 0, offset, 1<<20, -1, -1
		ft.Partitions = append(ft.Partitions, fp)
		f.Topics = append(f.Topics, ft)
		e.Do(label, f, 11)
	}
	at("fetch-at-the-end", 1)
	at("fetch-above-the-end", 100)
	at("fetch-below-the-start", -5)
}

// idempotentWriteFromTopicWrite is V8: with an authorizer, a user granted Write on
// one topic and nothing on the cluster may still InitProducerId without a
// transactional id (KIP-679); a user with nothing may not.
func idempotentWriteFromTopicWrite(e *Env) {
	e.Open("admin", "admin", saslUsers["admin"])
	e.Open("alice", "alice", saslUsers["alice"])
	e.Open("bob", "bob", saslUsers["bob"])
	e.Use("admin")
	e.Do("create", createTopics(false, newTopic("iw", 1, 1)), 7)
	cr := kmsg.NewPtrCreateACLsRequest()
	a := kmsg.NewCreateACLsRequestCreation()
	a.ResourceType, a.ResourceName, a.ResourcePatternType = kmsg.ACLResourceTypeTopic, "iw", kmsg.ACLResourcePatternTypeLiteral
	a.Principal, a.Host, a.Operation, a.PermissionType = "User:alice", "*", kmsg.ACLOperationWrite, kmsg.ACLPermissionTypeAllow
	cr.Creations = append(cr.Creations, a)
	e.Do("allow-alice-write-iw", cr, 3)

	init := func() *kmsg.InitProducerIDRequest {
		r := kmsg.NewPtrInitProducerIDRequest()
		r.TransactionTimeoutMillis, r.ProducerID, r.ProducerEpoch = 60000, -1, -1
		return r
	}
	// The ACL reaches the authorizer a moment after CreateAcls answered.
	e.Use("alice")
	e.Await("alice may write iw", init(), 4, func(r kmsg.Response) bool {
		return r.(*kmsg.InitProducerIDResponse).ErrorCode == 0
	})
	e.Do("alice-init-producer-id", init(), 4)
	e.Use("bob")
	e.Do("bob-init-producer-id", init(), 4)
}
