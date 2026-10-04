package broker

import (
	"fmt"
	"strconv"
	"strings"
)

// Topic configuration accepted at CreateTopics.
//
// A key is accepted only if it is a real Kafka topic config, and its value only if
// it is one Kafka would accept. Storing whatever arrived meant `segment.bytes =
// banana` and `no.such.config = 1` were answered with success and then echoed by
// DescribeConfigs as if they were in force.
//
// Accepting a key is not the same as acting on it: see docs/limitations.md for
// which ones change behaviour.

type configCheck func(value string) error

func isInt(min int64) configCheck {
	return func(v string) error {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("%q is not a number", v)
		}
		if n < min {
			return fmt.Errorf("%d is below the minimum of %d", n, min)
		}
		return nil
	}
}

func isFloat(min, max float64) configCheck {
	return func(v string) error {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < min || f > max {
			return fmt.Errorf("%q is not a number between %v and %v", v, min, max)
		}
		return nil
	}
}

func isBool(v string) error {
	if v != "true" && v != "false" {
		return fmt.Errorf("%q is not true or false", v)
	}
	return nil
}

func oneOf(allowed ...string) configCheck {
	return func(v string) error {
		for _, a := range allowed {
			if v == a {
				return nil
			}
		}
		return fmt.Errorf("%q is not one of %s", v, strings.Join(allowed, ", "))
	}
}

func cleanupPolicy(v string) error {
	seen := map[string]bool{}
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		if p != "delete" && p != "compact" || seen[p] {
			return fmt.Errorf("%q is not delete, compact, or both", v)
		}
		seen[p] = true
	}
	return nil
}

func anyValue(string) error { return nil }

// topicConfigs is every topic-level config Kafka defines, with its check.
var topicConfigs = map[string]configCheck{
	"cleanup.policy":                          cleanupPolicy,
	"compression.type":                        oneOf("uncompressed", "zstd", "lz4", "snappy", "gzip", "producer"),
	"compression.gzip.level":                  isInt(-1),
	"compression.lz4.level":                   isInt(1),
	"compression.zstd.level":                  isInt(-131072),
	"delete.retention.ms":                     isInt(0),
	"file.delete.delay.ms":                    isInt(0),
	"flush.messages":                          isInt(1),
	"flush.ms":                                isInt(0),
	"follower.replication.throttled.replicas": anyValue,
	"index.interval.bytes":                    isInt(0),
	"leader.replication.throttled.replicas":   anyValue,
	"local.retention.bytes":                   isInt(-2),
	"local.retention.ms":                      isInt(-2),
	"max.compaction.lag.ms":                   isInt(1),
	"max.message.bytes":                       isInt(0),
	"message.downconversion.enable":           isBool,
	"message.format.version":                  anyValue,
	"message.timestamp.after.max.ms":          isInt(0),
	"message.timestamp.before.max.ms":         isInt(0),
	"message.timestamp.difference.max.ms":     isInt(0),
	"message.timestamp.type":                  oneOf("CreateTime", "LogAppendTime"),
	"min.cleanable.dirty.ratio":               isFloat(0, 1),
	"min.compaction.lag.ms":                   isInt(0),
	"min.insync.replicas":                     isInt(1),
	"preallocate":                             isBool,
	"remote.log.copy.disable":                 isBool,
	"remote.log.delete.on.disable":            isBool,
	"remote.storage.enable":                   isBool,
	"retention.bytes":                         isInt(-1),
	"retention.ms":                            isInt(-1),
	"segment.bytes":                           isInt(14),
	"segment.index.bytes":                     isInt(4),
	"segment.jitter.ms":                       isInt(0),
	"segment.ms":                              isInt(1),
	"unclean.leader.election.enable":          isBool,
}

// validateTopicConfig reports the first key that is not a Kafka topic config, or
// whose value Kafka would refuse.
func validateTopicConfig(config map[string]string) error {
	for key, value := range config {
		check, known := topicConfigs[key]
		if !known {
			return fmt.Errorf("unknown topic config %q", key)
		}
		if err := check(value); err != nil {
			return fmt.Errorf("invalid value for %s: %v", key, err)
		}
	}
	return nil
}
