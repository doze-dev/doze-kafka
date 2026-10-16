package main

// What the mutation run found nothing pinned in cmd/doze-kafka
// (plan/evidence/mutation-survivors.md): the help text's flag lines, the "did you
// mean" for a near flag or a bare word, shell quoting, and the env command the
// summary prints in name mode.

import (
	"bytes"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

func TestHelpGivesTypesAndOnlyRealDefaults(t *testing.T) {
	cfg := config.Default()
	fs, _ := newFlagSet(&cfg)
	var b bytes.Buffer
	printHelp(&b, fs)
	h := b.String()
	// A typed flag says its type; a bool says none; a string default is said.
	for _, want := range []string{"  --data-dir string\n", "(default ./data)", "  --console\n", "  --listen string\n"} {
		if !strings.Contains(h, want) {
			t.Errorf("help lacks %q", want)
		}
	}
	// Zero values are not defaults worth saying.
	for _, never := range []string{"(default false)", "(default 0)", "(default [])", "(default 0s)", "(default )"} {
		if strings.Contains(h, never) {
			t.Errorf("help says %q", never)
		}
	}
	if strings.Contains(h, " value\n") {
		t.Error("a flag's type is printed as \"value\"")
	}
}

func TestNearFlagsAndBareWords(t *testing.T) {
	cfg := config.Default()
	fs, _ := newFlagSet(&cfg)
	for name, want := range map[string]string{
		"lis":          "listen",   // a prefix of three letters
		"listne":       "listen",   // two letters swapped: distance 2
		"listenn":      "listen",   // one extra letter
		"li":           "",         // a prefix of two is not enough, and too far
		"zzzzzzzz":     "",         // nothing near
		"data-dr":      "data-dir", // one missing letter
		"consol":       "console",  // a prefix
		"kafka-vrsion": "kafka-version",
	} {
		if got := nearestFlag(fs, name); got != want {
			t.Errorf("nearestFlag(%q) = %q, want %q", name, got, want)
		}
	}
	for a, b := range map[[2]string]int{{"", ""}: 0, {"a", ""}: 1, {"", "ab"}: 2, {"kitten", "sitting"}: 3, {"abc", "abc"}: 0, {"abc", "abd"}: 1} {
		if got := editDistance(a[0], a[1]); got != b {
			t.Errorf("editDistance(%q, %q) = %d, want %d", a[0], a[1], got, b)
		}
	}
	for arg, want := range map[string]string{
		"listen":     "did you mean --listen?",
		"lis":        "did you mean --listen?",
		"li":         "doze-kafka takes flags",
		"data-dir=x": "did you mean --data-dir=x?",
		"nonsense":   "doze-kafka takes flags",
	} {
		if err := unexpectedArgument(fs, arg); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("unexpectedArgument(%q) = %v, want it to say %q", arg, err, want)
		}
	}
}

func TestShellQuoteQuotesOnlyWhatTheShellWouldRead(t *testing.T) {
	for s, want := range map[string]string{
		"":          "''",
		"azAZ09":    "azAZ09",
		"-_./:=,@+": "-_./:=,@+",
		"a b":       "'a b'",
		"it's":      `'it'\''s'`,
		"[::]:9092": "'[::]:9092'",
		"`":         "'`'",
		"{":         "'{'",
		"@":         "@",
		"[":         "'['",
		"/":         "/",
		":":         ":",
	} {
		if got := shellQuote(s); got != want {
			t.Errorf("shellQuote(%q) = %s, want %s", s, got, want)
		}
	}
}

// By name, the env command names the instance and only the addresses the
// configuration fixed.
func TestTheEnvHintByName(t *testing.T) {
	cfg := config.Default()
	cfg.Name = "shop"
	got := envHint(startup{cfg: cfg}, envBound{console: "127.0.0.9:9093", registry: "127.0.0.9:8081"})
	if !strings.Contains(got, "--name shop") || strings.Contains(got, "--console-addr") || strings.Contains(got, "--schema-registry-addr") || strings.Contains(got, "--advertised-host") {
		t.Errorf("bare name mode: %s", got)
	}
	cfg.AdvertisedHost = "box.lan"
	cfg.ConsoleAddr = "127.0.0.1:9993"
	cfg.SchemaRegistryAddr = "127.0.0.1:9981"
	got = envHint(startup{cfg: cfg}, envBound{console: "127.0.0.1:9993", registry: "127.0.0.1:9981"})
	for _, want := range []string{"--advertised-host box.lan", "--console-addr 127.0.0.1:9993", "--schema-registry-addr 127.0.0.1:9981"} {
		if !strings.Contains(got, want) {
			t.Errorf("name mode with fixed addresses: %s lacks %s", got, want)
		}
	}
	got = envHint(startup{cfg: cfg}, envBound{})
	if strings.Contains(got, "--console-addr") || strings.Contains(got, "--schema-registry-addr") {
		t.Errorf("name mode with nothing bound: %s", got)
	}
}
