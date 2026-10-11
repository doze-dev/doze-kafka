package broker

import (
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// Foundation B12: topic config values arrive from CreateTopics, AlterConfigs and
// IncrementalAlterConfigs, and from the console's forms. Whatever value one of the
// keys the broker acts on is given, if validateTopicConfig accepts it, the limits
// the log is then given are usable: no duration or size is negative.
func FuzzTopicConfigValues(f *testing.F) {
	keys := []string{"retention.ms", "retention.bytes", "segment.bytes", "cleanup.policy",
		"min.cleanable.dirty.ratio", "min.compaction.lag.ms", "max.compaction.lag.ms",
		"delete.retention.ms", "compression.type", "max.message.bytes", "message.timestamp.type"}
	f.Add(uint8(0), "-1", uint8(3), "9223372036854775807")
	f.Add(uint8(5), "9223372036854775807", uint8(6), "9223372036854775807")
	f.Add(uint8(7), "9223372036854775807", uint8(3), "compact")
	f.Add(uint8(4), "NaN", uint8(3), "compact,delete")
	f.Add(uint8(2), "1024", uint8(1), "0")
	f.Fuzz(func(t *testing.T, k1 uint8, v1 string, k2 uint8, v2 string) {
		cfg := map[string]string{keys[int(k1)%len(keys)]: v1, keys[int(k2)%len(keys)]: v2}
		if validateTopicConfig(cfg) != nil {
			return
		}
		if cfg["cleanup.policy"] == "" {
			// A compaction setting acts only on a compacted topic.
			cfg["cleanup.policy"] = "compact"
		}
		l, _ := TopicLimits(config.Default(), cfg)
		neg := l.Retention.MaxAge < 0 || l.Retention.MaxBytes < 0 || l.SegmentBytes < 0 ||
			l.Compaction.MinLag < 0 || l.Compaction.MaxLag < 0 || l.Compaction.DeleteRetention < 0
		if neg {
			t.Fatalf("%v is accepted and gives negative limits: %+v", cfg, l)
		}
	})
}
