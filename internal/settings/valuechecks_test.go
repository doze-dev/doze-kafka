package settings

// (Named to run after settings_test.go: TestBuiltLazily needs the table unbuilt.)
//
// These pin what the mutation run found nothing pinned (plan/evidence/mutation-survivors.md,
// internal/settings): the exact words Teach, Rule and describe build, which effects
// act, and each value check at its boundary.

import (
	"strconv"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

func TestTeachAndRuleSayEachPartInOrder(t *testing.T) {
	full := Setting{Unit: "ms", Choices: []string{"a", "b"}, Range: "ignored when there are choices", DefaultNote: "7", KafkaDefault: "9", Effect: Applied}
	if got, want := full.Rule(), "unit: ms; one of a, b; default: 7; Kafka's default: 9; applied"; got != want {
		t.Errorf("Rule = %q, want %q", got, want)
	}
	if got, want := full.Teach(config.Config{}), "unit: ms; one of a, b; default: 7; Kafka's default: 9; applied"; got != want {
		t.Errorf("Teach = %q, want %q", got, want)
	}
	ranged := Setting{Range: "0 or more", Effect: Stored}
	if got, want := ranged.Rule(), "range: 0 or more; stored only: it changes nothing here"; got != want {
		t.Errorf("Rule = %q, want %q", got, want)
	}
	if got, want := ranged.Teach(config.Config{}), "range: 0 or more; stored only: it changes nothing here"; got != want {
		t.Errorf("Teach = %q, want %q", got, want)
	}
	bare := Setting{Effect: NotImplemented}
	if got := bare.Rule(); got != "not implemented" {
		t.Errorf("Rule of a bare record = %q", got)
	}
	if got := bare.Teach(config.Config{}); got != "not implemented" {
		t.Errorf("Teach of a bare record = %q", got)
	}
}

func TestDescribeBuildsEachSentence(t *testing.T) {
	cases := []struct {
		s    Setting
		want string
	}{
		{Setting{Meaning: "how long.", Choices: []string{"x", "y"}}, "How long. One of x, y."},
		{Setting{Meaning: "how long", Range: "1 or more", Unit: "ms"}, "How long. Accepts 1 or more, in ms."},
		{Setting{Meaning: "how long", Range: "1 or more"}, "How long. Accepts 1 or more."},
		{Setting{Meaning: "how long", Unit: "ms"}, "How long. In ms."},
		{Setting{Meaning: "how long", DefaultNote: "3", KafkaDefault: "4"}, "How long. Default: 3. Kafka's own default is 4."},
	}
	for _, c := range cases {
		if got := c.s.describe(); !strings.HasPrefix(got, c.want) {
			t.Errorf("describe(%+v) = %q, want it to begin %q", c.s, got, c.want)
		}
	}
}

func TestOnlyTheEffectsThatDoSomethingAct(t *testing.T) {
	for e, want := range map[Effect]bool{Applied: true, AppliedCompacted: true, StartupOnly: true, Stored: false, NotImplemented: false, Informational: false} {
		if e.Acts() != want {
			t.Errorf("%s.Acts() = %v, want %v", e, e.Acts(), want)
		}
	}
}

func TestValueChecksAtTheirBoundaries(t *testing.T) {
	ok := func(name string, err error) {
		t.Helper()
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	bad := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	ok("isInt(-1) -1", isInt(-1)("-1"))
	bad("isInt(-1) -2", isInt(-1)("-2"))
	bad("isInt(0) x", isInt(0)("x"))
	ok("isFloat(0,1) 0", isFloat(0, 1)("0"))
	ok("isFloat(0,1) 1", isFloat(0, 1)("1"))
	bad("isFloat(0,1) -0.1", isFloat(0, 1)("-0.1"))
	bad("isFloat(0,1) 1.1", isFloat(0, 1)("1.1"))
	bad("isFloat x", isFloat(0, 1)("x"))
	ok("true", isBool("true"))
	ok("false", isBool("false"))
	bad("yes", isBool("yes"))
	ok("oneOf a", oneOf("a", "b")("b"))
	bad("oneOf c", oneOf("a", "b")("c"))
	ok("retention -1", retentionMs("-1"))
	bad("retention -2", retentionMs("-2"))
	ok("retention max", retentionMs(strconv.FormatInt(MaxRetentionMs, 10)))
	bad("retention max+1", retentionMs(strconv.FormatInt(MaxRetentionMs+1, 10)))
}

// The minimum each topic config states is the one its check enforces.
func TestEachTopicConfigsMinimumIsEnforced(t *testing.T) {
	for name, min := range map[string]int64{
		"compression.gzip.level": -1, "compression.zstd.level": -131072, "local.retention.bytes": -2,
		"local.retention.ms": -2, "retention.bytes": -1,
	} {
		if err := ValidateTopic(map[string]string{name: strconv.FormatInt(min, 10)}); err != nil {
			t.Errorf("%s=%d refused: %v", name, min, err)
		}
		if err := ValidateTopic(map[string]string{name: strconv.FormatInt(min-1, 10)}); err == nil {
			t.Errorf("%s=%d accepted, below its minimum %d", name, min-1, min)
		}
	}
}

// A deny-by-default broker says so.
func TestACLDefaultIsSaidForEachSetting(t *testing.T) {
	s, ok := Lookup(Broker, "allow.everyone.if.no.acl.found")
	if !ok {
		t.Fatal("no allow.everyone.if.no.acl.found record")
	}
	cfg := config.Default()
	cfg.ACLDefault = config.ACLDeny
	if v, _ := s.DefaultValue(cfg); v != "false" {
		t.Errorf("deny: %q, want false", v)
	}
	cfg.ACLDefault = config.ACLAllow
	if v, _ := s.DefaultValue(cfg); v != "true" {
		t.Errorf("allow: %q, want true", v)
	}
}
