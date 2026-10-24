package broker_test

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/broker"
	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/settings"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
)

// How a topic's configuration becomes the limits the log applies (docs/limitations.md
// says which keys are acted on).

func TestRetentionPolicyIsTheConfigurationsMillisecondsAsADuration(t *testing.T) {
	cfg := config.Default()
	cfg.RetentionMs, cfg.RetentionBytes = 3000, 12345
	if got := broker.RetentionPolicy(cfg); got.MaxAge != 3*time.Second || got.MaxBytes != 12345 {
		t.Errorf("retention.ms 3000 and 12345 bytes: %+v, want 3s and 12345", got)
	}
}

func TestTopicLimitsResolveATopicsRetentionAgainstTheBrokers(t *testing.T) {
	cfg := config.Default()
	cfg.RetentionMs, cfg.RetentionBytes = 60_000, 1000
	for _, tc := range []struct {
		name  string
		conf  map[string]string
		own   bool
		age   time.Duration
		bytes int64
		seg   int64
	}{
		{"nothing set follows the broker", nil, false, time.Minute, 1000, 0},
		{"keys nobody acts on", map[string]string{"flush.ms": "5"}, false, time.Minute, 1000, 0},
		{"retention.ms -1 keeps for ever", map[string]string{"retention.ms": "-1"}, true, 0, 1000, 0},
		{"retention.ms below -1 too", map[string]string{"retention.ms": "-7"}, true, 0, 1000, 0},
		{"retention.ms 0 is the smallest age", map[string]string{"retention.ms": "0"}, true, time.Millisecond, 1000, 0},
		{"retention.ms 1", map[string]string{"retention.ms": "1"}, true, time.Millisecond, 1000, 0},
		{"retention.ms 5000", map[string]string{"retention.ms": "5000"}, true, 5 * time.Second, 1000, 0},
		{"retention.ms that is not a number", map[string]string{"retention.ms": "soon"}, false, time.Minute, 1000, 0},
		{"retention.bytes -1 is no limit", map[string]string{"retention.bytes": "-1"}, true, time.Minute, 0, 0},
		{"retention.bytes 0 is one byte", map[string]string{"retention.bytes": "0"}, true, time.Minute, 1, 0},
		{"retention.bytes 1", map[string]string{"retention.bytes": "1"}, true, time.Minute, 1, 0},
		{"retention.bytes 777", map[string]string{"retention.bytes": "777"}, true, time.Minute, 777, 0},
		{"segment.bytes 0 is not a setting", map[string]string{"segment.bytes": "0"}, false, time.Minute, 1000, 0},
		{"segment.bytes negative is not a setting", map[string]string{"segment.bytes": "-4"}, false, time.Minute, 1000, 0},
		{"segment.bytes 1", map[string]string{"segment.bytes": "1"}, true, time.Minute, 1000, 1},
		{"segment.bytes 4096", map[string]string{"segment.bytes": "4096"}, true, time.Minute, 1000, 4096},
	} {
		got, own := broker.TopicLimits(cfg, tc.conf)
		if own != tc.own || got.Retention.MaxAge != tc.age || got.Retention.MaxBytes != tc.bytes || got.SegmentBytes != tc.seg {
			t.Errorf("%s: own=%v age=%v bytes=%d segment=%d, want own=%v age=%v bytes=%d segment=%d",
				tc.name, own, got.Retention.MaxAge, got.Retention.MaxBytes, got.SegmentBytes, tc.own, tc.age, tc.bytes, tc.seg)
		}
	}
}

func TestTopicLimitsResolveACompactedTopic(t *testing.T) {
	cfg := config.Default()
	comp := func(kv ...string) map[string]string {
		m := map[string]string{"cleanup.policy": "compact"}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}
	policy := func(conf map[string]string) log.CompactionPolicy {
		l, own := broker.TopicLimits(cfg, conf)
		if !own {
			t.Fatalf("%v is not the topic's own", conf)
		}
		return l.Compaction
	}
	if p := policy(comp()); !p.Enabled || p.DeleteRetention != 24*time.Hour || p.MinDirtyRatio != 0 || p.MinLag != 0 || p.MaxLag != 0 || p.Codec != 0 {
		t.Errorf("a compacted topic with nothing else set: %+v, want enabled, tombstones kept 24h, nothing else", p)
	}
	if _, own := broker.TopicLimits(cfg, map[string]string{"cleanup.policy": "delete"}); own {
		t.Error("cleanup.policy=delete is the topic's own")
	}
	if p := policy(map[string]string{"cleanup.policy": "delete, compact"}); !p.Enabled {
		t.Error("compact,delete with a space does not compact")
	}
	for _, tc := range []struct {
		name string
		conf map[string]string
		want log.CompactionPolicy
	}{
		{"a dirty ratio", comp("min.cleanable.dirty.ratio", "0.5"), log.CompactionPolicy{Enabled: true, DeleteRetention: 24 * time.Hour, MinDirtyRatio: 0.5}},
		{"a dirty ratio that is not a number", comp("min.cleanable.dirty.ratio", "half"), log.CompactionPolicy{Enabled: true, DeleteRetention: 24 * time.Hour}},
		{"a minimum lag", comp("min.compaction.lag.ms", "250"), log.CompactionPolicy{Enabled: true, DeleteRetention: 24 * time.Hour, MinLag: 250 * time.Millisecond}},
		{"a minimum lag of 0 or less is none", comp("min.compaction.lag.ms", "-9"), log.CompactionPolicy{Enabled: true, DeleteRetention: 24 * time.Hour}},
		{"a minimum lag that is not a number", comp("min.compaction.lag.ms", "x"), log.CompactionPolicy{Enabled: true, DeleteRetention: 24 * time.Hour}},
		{"a maximum lag", comp("max.compaction.lag.ms", "900"), log.CompactionPolicy{Enabled: true, DeleteRetention: 24 * time.Hour, MaxLag: 900 * time.Millisecond}},
		{"a maximum lag of 0 or less is none", comp("max.compaction.lag.ms", "-9"), log.CompactionPolicy{Enabled: true, DeleteRetention: 24 * time.Hour}},
		{"a maximum lag that is not a number", comp("max.compaction.lag.ms", "x"), log.CompactionPolicy{Enabled: true, DeleteRetention: 24 * time.Hour}},
		{"tombstones kept for no time", comp("delete.retention.ms", "0"), log.CompactionPolicy{Enabled: true}},
		{"tombstones kept a while", comp("delete.retention.ms", "1500"), log.CompactionPolicy{Enabled: true, DeleteRetention: 1500 * time.Millisecond}},
		{"a negative retention of tombstones is the default", comp("delete.retention.ms", "-1"), log.CompactionPolicy{Enabled: true, DeleteRetention: 24 * time.Hour}},
		{"a retention of tombstones that is not a number", comp("delete.retention.ms", "x"), log.CompactionPolicy{Enabled: true, DeleteRetention: 24 * time.Hour}},
		{"a codec", comp("compression.type", "zstd"), log.CompactionPolicy{Enabled: true, DeleteRetention: 24 * time.Hour, Codec: log.CodecByName("zstd")}},
	} {
		if got := policy(tc.conf); got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
	}
	// A time that overflows a duration saturates; the largest one that fits does not.
	big := settings.MaxRetentionMs
	if p := policy(comp("delete.retention.ms", limitsItoa(big))); p.DeleteRetention != time.Duration(big)*time.Millisecond {
		t.Errorf("delete.retention.ms at the limit: %v, want %v", p.DeleteRetention, time.Duration(big)*time.Millisecond)
	}
	if p := policy(comp("delete.retention.ms", limitsItoa(big+1))); p.DeleteRetention != time.Duration(math.MaxInt64) {
		t.Errorf("delete.retention.ms past the limit: %v, want the longest duration", p.DeleteRetention)
	}
	if p := policy(comp("min.compaction.lag.ms", limitsItoa(math.MaxInt64))); p.MinLag != time.Duration(math.MaxInt64) {
		t.Errorf("min.compaction.lag.ms of the longest long: %v, want the longest duration", p.MinLag)
	}
	if l, _ := broker.TopicLimits(cfg, map[string]string{"retention.ms": limitsItoa(big + 1)}); l.Retention.MaxAge != time.Duration(math.MaxInt64) {
		t.Errorf("retention.ms past the limit: %v, want the longest duration", l.Retention.MaxAge)
	}
}

func limitsItoa(n int64) string { return strconv.FormatInt(n, 10) }
