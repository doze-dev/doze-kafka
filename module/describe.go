package kafka

import "github.com/doze-dev/doze-sdk/engine"

// Describe implements engine.Describer: the catalog metadata the module
// registry publishes for kafka. Versions doubles as the engine-support list
// stamped into the signed index — but here the "versions" are advertised Kafka
// protocol profiles (doze-kafka is the engine; no artifact is fetched).
func (Driver) Describe() engine.Description {
	return engine.Description{
		Title:        "Kafka",
		Tagline:      "Single-node Kafka-protocol broker, no JVM.",
		Category:     "queue",
		Description:  "A local, disk-backed broker that speaks enough of the Kafka wire protocol that unmodified clients (franz-go, kcat, Sarama, the Java client) connect and work. No JVM, no ZooKeeper, no KRaft — the engine is doze-kafka, embedded in the module, and `version` selects the Kafka protocol profile it advertises (1–4). Declare topics and doze converges them on boot; auto-create handles the rest.",
		Port:         9092,
		Versions:     []string{"1", "2", "3", "4"},
		Source:       "doze/kafka",
		Homepage:     "https://github.com/doze-dev/doze-kafka/tree/main/module",
		ExampleLabel: "events",
		Example: `kafka "events" {
  version = 4
  port    = 9092

  auto_create_topics = true
  default_partitions = 1
  retention          = "168h"

  topic "orders" {
    partitions = 3
  }
  topic "payments" {
    partitions = 1
    config = { "cleanup.policy" = "compact" }
  }
}`,
		Config: []engine.ConfigArg{
			{Name: "version", Type: "number", Required: true, Desc: "Advertised Kafka protocol profile — 1, 2, 3, or 4."},
			{Name: "auto_create_topics", Type: "bool", Default: "true", Desc: "Create unknown topics on first reference."},
			{Name: "default_partitions", Type: "number", Default: "1", Desc: "Partition count for auto-created topics."},
			{Name: "retention", Type: "string", Desc: "Delete segments older than this (Go duration, e.g. \"168h\"); empty = unbounded."},
			{Name: "retention_bytes", Type: "number", Desc: "Delete old segments once a partition exceeds this size (0 = unbounded)."},
		},
		Blocks: []engine.ConfigBlock{{
			Name:  "topic",
			Label: "name",
			Desc:  "A topic to create on boot.",
			Args: []engine.ConfigArg{
				{Name: "partitions", Type: "number", Default: "1", Desc: "Partition count."},
				{Name: "config", Type: "map(string)", Desc: "Per-topic config (e.g. cleanup.policy = compact)."},
			},
		}},
	}
}
