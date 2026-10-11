package settings

import (
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// The table is built on first use, not at start-up (the idle heap is held by B9).
// This test must run before anything else here asks for a record.
func TestBuiltLazily(t *testing.T) {
	if byKey != nil || order != nil {
		t.Fatal("the table was built before anything asked for it")
	}
	if _, ok := TopicConfig("retention.ms"); !ok {
		t.Fatal("no record for retention.ms")
	}
	if byKey == nil {
		t.Fatal("the first lookup did not build the table")
	}
}

func TestTeachLineComesFromTheRecord(t *testing.T) {
	cfg := config.Default()
	s, _ := TopicConfig("retention.ms")
	line := s.Teach(cfg)
	for _, want := range []string{"unit: ms", "default: -1", "Kafka's default: 604800000", "applied"} {
		if !strings.Contains(line, want) {
			t.Errorf("retention.ms teach line %q lacks %q", line, want)
		}
	}
	cfg.RetentionMs = 3_600_000
	if line := s.Teach(cfg); !strings.Contains(line, "default: 3600000") {
		t.Errorf("the default must follow the broker's flag: %q", line)
	}
	st, _ := TopicConfig("min.insync.replicas")
	if line := st.Teach(cfg); !strings.Contains(line, "stored only") {
		t.Errorf("a stored setting must say so: %q", line)
	}
}

func TestEveryRecordIsComplete(t *testing.T) {
	for _, kind := range []Kind{Topic, Broker, Registry, Console, Group} {
		all := All(kind)
		if len(all) == 0 {
			t.Errorf("no %s records", kind)
		}
		for _, s := range all {
			if s.Name == "" || s.Effect == "" || s.Source == "" || s.Meaning == "" {
				t.Errorf("%s %q is incomplete: %+v", kind, s.Name, s)
			}
		}
	}
}

func TestRegistryLevelsAndModes(t *testing.T) {
	if got := len(CompatLevels()); got != 7 {
		t.Errorf("levels = %d, want 7", got)
	}
	imp, ok := Lookup(Registry, "mode:IMPORT")
	if !ok || imp.Effect != NotImplemented {
		t.Errorf("IMPORT must be present and marked not implemented: %+v", imp)
	}
}

func TestValidateTopic(t *testing.T) {
	if err := ValidateTopic(map[string]string{"segment.bytes": "banana"}); err == nil {
		t.Error("banana accepted")
	}
	if err := ValidateTopic(map[string]string{"no.such": "1"}); err == nil || !strings.Contains(err.Error(), "unknown topic config") {
		t.Errorf("unknown key: %v", err)
	}
	if err := ValidateTopic(map[string]string{"cleanup.policy": "compact,delete"}); err != nil {
		t.Error(err)
	}
	if !IsListTopicConfig("cleanup.policy") || IsListTopicConfig("retention.ms") {
		t.Error("list configs wrong")
	}
}

// Every flag the console shows has a default it can say for a configuration, and the
// offsets retention says the one the broker runs with (7 days, Kafka's
// offsets.retention.minutes), and zero for "for ever" is told in its range.
func TestBrokerSettingsStateTheirDefaults(t *testing.T) {
	cfg := config.Default()
	seen := map[string]string{}
	for _, s := range All(Broker) {
		if v, ok := s.DefaultValue(cfg); ok {
			seen[s.Name] = v
		}
	}
	if got := seen["offsets-retention"]; got != "168h0m0s" {
		t.Errorf("offsets-retention default %q, want 168h0m0s", got)
	}
	s, ok := Lookup(Broker, "offsets-retention")
	if !ok || !strings.Contains(s.Range, "for ever") {
		t.Errorf("offsets-retention: %+v %v", s, ok)
	}
	for _, name := range []string{"fsync-interval", "consumer-session-timeout", "kafka-version", "log-level", "client-telemetry", "schema-registry", "console", "max-partitions"} {
		if seen[name] == "" {
			t.Errorf("%s states no default", name)
		}
	}
}
