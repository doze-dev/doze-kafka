package main

import (
	"flag"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

func TestAStringSliceFlagThatHoldsNothingPrintsNothing(t *testing.T) {
	var none *stringSliceFlag
	if got := none.String(); got != "" {
		t.Errorf("nil flag: %q", got)
	}
	if got := (&stringSliceFlag{}).String(); got != "" {
		t.Errorf("flag with no slice: %q", got)
	}
	vals := []string{"PLAIN", "SCRAM-SHA-256"}
	if got := (&stringSliceFlag{vals: &vals}).String(); got != "PLAIN,SCRAM-SHA-256" {
		t.Errorf("flag with a slice: %q", got)
	}
}

// Of two flags equally near, the first by name is offered.
func TestTheNearestFlagOfTwoEquallyNearIsTheFirst(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.Bool("abcd", false, "")
	fs.Bool("abce", false, "")
	if got := nearestFlag(fs, "abcx"); got != "abcd" {
		t.Errorf("nearest of abcd and abce to abcx: %q, want abcd", got)
	}
}

// The console path comes from the flag, else from the environment, else the default.
func TestConsolePrefixComesFromTheFlagThenTheEnvironmentThenTheDefault(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("DOZE_KAFKA_CONSOLE_PREFIX", "")
	if st, err := loadConfig(nil); err != nil || st.cfg.ConsolePrefix != config.DefaultConsolePrefix {
		t.Errorf("neither: %q %v", st.cfg.ConsolePrefix, err)
	}
	t.Setenv("DOZE_KAFKA_CONSOLE_PREFIX", "/from-env")
	if st, err := loadConfig(nil); err != nil || st.cfg.ConsolePrefix != "/from-env" {
		t.Errorf("environment: %q %v", st.cfg.ConsolePrefix, err)
	}
	if st, err := loadConfig([]string{"--console-prefix", "/from-flag"}); err != nil || st.cfg.ConsolePrefix != "/from-flag" {
		t.Errorf("flag and environment: %q %v", st.cfg.ConsolePrefix, err)
	}
}

// `env` accepts SASL mechanisms without users, as it prints where to connect and never a
// password; starting the broker that way is refused.
func TestOnlyEnvAcceptsSASLMechanismsWithoutUsers(t *testing.T) {
	t.Chdir(t.TempDir())
	args := []string{"--sasl-mechanisms", "PLAIN"}
	if _, err := loadEnvConfig(args); err != nil {
		t.Errorf("env: %v", err)
	}
	if _, err := loadConfig(args); err == nil {
		t.Error("starting with mechanisms and no users was accepted")
	}
	if _, err := loadEnvConfig(nil); err != nil {
		t.Errorf("env with no authentication: %v", err)
	}
}
