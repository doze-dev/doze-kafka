package broker

import (
	"strconv"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
)

// The topic configs this broker acts on. Every other key in topicConfigs is
// accepted, stored and reported, and changes nothing; docs/limitations.md says
// which is which.
//
//	retention.ms            delete segments older than this (-1: keep for ever)
//	retention.bytes         delete the oldest segments once the partition exceeds this (-1: no limit)
//	segment.bytes           roll to a new segment at this size
//	max.message.bytes       refuse a record batch larger than this (produce.go)
//	message.timestamp.type  LogAppendTime: records take the broker's clock (produce.go)
//
// A topic that sets none of the first three follows the broker's own settings;
// the last two have no broker-wide counterpart and apply only where a topic sets
// them.

// RetentionPolicy is the broker-wide retention from the configuration, as the log
// applies it. Zero in the configuration means no limit.
func RetentionPolicy(cfg config.Config) log.RetentionPolicy {
	return log.RetentionPolicy{MaxBytes: cfg.RetentionBytes, MaxAge: time.Duration(cfg.RetentionMs) * time.Millisecond}
}

// TopicLimits resolves a topic's own configuration against the broker's into the
// limits the log should apply. own is false when the topic overrides nothing that
// is enforced, and the broker's limits apply as they are.
func TopicLimits(cfg config.Config, topicConfig map[string]string) (limits log.TopicLimits, own bool) {
	limits.Retention = RetentionPolicy(cfg)
	num := func(key string) (int64, bool) {
		v, ok := topicConfig[key]
		if !ok {
			return 0, false
		}
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	}
	if n, ok := num("retention.ms"); ok {
		own = true
		switch {
		case n < 0:
			limits.Retention.MaxAge = 0 // -1: keep for ever, whatever the broker's default.
		case n == 0:
			// Kafka's 0 is "delete as soon as possible". The log's zero means no
			// limit, so this is the smallest age instead.
			limits.Retention.MaxAge = time.Millisecond
		default:
			limits.Retention.MaxAge = time.Duration(n) * time.Millisecond
		}
	}
	if n, ok := num("retention.bytes"); ok {
		own = true
		switch {
		case n < 0:
			limits.Retention.MaxBytes = 0
		case n == 0:
			limits.Retention.MaxBytes = 1
		default:
			limits.Retention.MaxBytes = n
		}
	}
	if n, ok := num("segment.bytes"); ok && n > 0 {
		own = true
		limits.SegmentBytes = n
	}
	return limits, own
}

// InitialTopicLimits is the limits of every topic that has its own, for the log
// manager to open with.
func InitialTopicLimits(cfg config.Config, topics map[string]map[string]string) map[string]log.TopicLimits {
	out := map[string]log.TopicLimits{}
	for name, tc := range topics {
		if l, own := TopicLimits(cfg, tc); own {
			out[name] = l
		}
	}
	return out
}

// applyTopicLimits puts a topic's configuration into force in the log.
func (b *Broker) applyTopicLimits(name string, topicConfig map[string]string) {
	if l, own := TopicLimits(b.cfg, topicConfig); own {
		b.logs.SetTopicLimits(name, l)
	} else {
		b.logs.ClearTopicLimits(name)
	}
}
