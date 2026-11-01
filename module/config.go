package kafka

import (
	"fmt"
	"time"

	"github.com/hashicorp/hcl/v2"

	"github.com/doze-dev/doze-sdk/engine"
)

// Config is the kafka-specific configuration decoded from a `kafka` block.
type Config struct {
	AutoCreateTopics  *bool
	DefaultPartitions int
	RetentionMs       int64
	RetentionBytes    int64
	Topics            []TopicConfig
}

// TopicConfig is one declared topic (converged on boot).
type TopicConfig struct {
	Name       string
	Partitions int
	Settings   map[string]string
}

type kafkaBody struct {
	AutoCreateTopics  *bool        `hcl:"auto_create_topics,optional"`
	DefaultPartitions int          `hcl:"default_partitions,optional"`
	Retention         string       `hcl:"retention,optional"`
	RetentionBytes    int64        `hcl:"retention_bytes,optional"`
	Topics            []topicBlock `hcl:"topic,block"`
}

type topicBlock struct {
	Name       string            `hcl:"name,label"`
	Partitions int               `hcl:"partitions,optional"`
	Settings   map[string]string `hcl:"config,optional"`
}

// DecodeConfig implements engine.ConfigDecoder for the kafka block.
func (Driver) DecodeConfig(body hcl.Body, ctx *hcl.EvalContext, _ string, _ engine.VersionSpec) (engine.EngineConfig, error) {
	var raw kafkaBody
	if err := engine.DecodeStrict(body, ctx, &raw); err != nil {
		return nil, err
	}
	c := &Config{
		AutoCreateTopics:  raw.AutoCreateTopics,
		DefaultPartitions: raw.DefaultPartitions,
		RetentionBytes:    raw.RetentionBytes,
	}
	if raw.Retention != "" {
		d, err := parseDuration(raw.Retention)
		if err != nil {
			return nil, fmt.Errorf("kafka: retention: %w", err)
		}
		c.RetentionMs = d
	}
	for _, tb := range raw.Topics {
		parts := tb.Partitions
		if parts == 0 {
			parts = 1
		}
		c.Topics = append(c.Topics, TopicConfig{Name: tb.Name, Partitions: parts, Settings: tb.Settings})
	}
	return c, nil
}

// parseDuration accepts a Go duration string and returns milliseconds.
func parseDuration(s string) (int64, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return d.Milliseconds(), nil
}
