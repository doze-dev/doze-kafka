package broker

import "github.com/doze-dev/doze-kafka/internal/settings"

// Topic configuration accepted at CreateTopics and by the alter calls.
//
// A key is accepted only if it is a real Kafka topic config, and its value only if
// it is one Kafka would accept. The table of keys, ranges and defaults is
// internal/settings: DescribeConfigs, the alter calls, CreateTopics and the console
// all read it, so a config the broker accepts always has a record there.
//
// Accepting a key is not the same as acting on it: each record says whether the
// broker acts on it (Effect), and docs/limitations.md matches.

// validateTopicConfig reports the first key that is not a Kafka topic config, or
// whose value Kafka would refuse.
//
//fuzz:parser class=config-values FuzzTopicConfigValues
func validateTopicConfig(config map[string]string) error { return settings.ValidateTopic(config) }
