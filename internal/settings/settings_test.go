package settings

import (
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// lazilyChecked says TestBuiltLazily has run in this process.
var lazilyChecked bool

// The table is built on first use, not at start-up (the idle heap is held by B9).
// This test must run before anything else here asks for a record.
func TestBuiltLazily(t *testing.T) {
	// The table is unbuilt once in a process. Under -count=2 the second run finds what
	// the first one built, which is not the table being built at start-up.
	if lazilyChecked {
		t.Skip("checked on the first run of this process; the table stays built after it")
	}
	lazilyChecked = true
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

// Every key that has a default holds a default its own rule accepts: a default the broker
// would refuse is a key nobody can leave alone.
func TestEveryTopicDefaultIsAValueItsOwnRuleAccepts(t *testing.T) {
	cfg := config.Default()
	n := 0
	for _, s := range All(Topic) {
		v, ok := s.DefaultValue(cfg)
		if !ok {
			continue
		}
		n++
		if err := ValidateTopic(map[string]string{s.Name: v}); err != nil {
			t.Errorf("%s: its default %q is refused: %v", s.Name, v, err)
		}
	}
	if n < 20 {
		t.Errorf("only %d topic keys have a default", n)
	}
}

func TestNamesAreSortedAndComeFromTheTable(t *testing.T) {
	names := Names(Topic)
	if len(names) != len(All(Topic)) || len(names) < 30 {
		t.Fatalf("%d names for %d settings", len(names), len(All(Topic)))
	}
	for i := 1; i < len(names); i++ {
		if names[i-1] >= names[i] {
			t.Errorf("%q is before %q", names[i-1], names[i])
		}
	}
	found := false
	for _, n := range names {
		found = found || n == "retention.ms"
	}
	if !found {
		t.Error("retention.ms is not named")
	}
}

func TestEveryEffectHasItsOwnPhrase(t *testing.T) {
	seen := map[string]Effect{}
	for _, e := range []Effect{Applied, AppliedCompacted, Stored, StartupOnly, NotImplemented, Informational} {
		p := e.Phrase()
		if p == "" {
			t.Errorf("%q has no phrase", e)
		}
		if other, dup := seen[p]; dup {
			t.Errorf("%q and %q both say %q", e, other, p)
		}
		seen[p] = e
	}
}

func TestCleanupPolicyTakesDeleteCompactOrBoth(t *testing.T) {
	for _, ok := range []string{"delete", "compact", "compact,delete", "delete, compact"} {
		if err := ValidateTopic(map[string]string{"cleanup.policy": ok}); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "tiered", "delete,tiered", "compact;delete"} {
		if err := ValidateTopic(map[string]string{"cleanup.policy": bad}); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
