package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// The console logs in with PLAIN when the broker has it, else the first mechanism it has,
// and with none when the broker asks for none.
func TestTheConsoleChoosesPLAINElseTheFirstMechanism(t *testing.T) {
	old := credentialRand
	defer func() { credentialRand = old }()
	for _, c := range []struct {
		mechs []string
		want  string
	}{
		{nil, ""},
		{[]string{"PLAIN"}, "PLAIN"},
		{[]string{"SCRAM-SHA-512"}, "SCRAM-SHA-512"},
		{[]string{"SCRAM-SHA-256", "PLAIN"}, "PLAIN"},
		{[]string{"PLAIN", "SCRAM-SHA-256"}, "PLAIN"},
		{[]string{"SCRAM-SHA-512", "SCRAM-SHA-256"}, "SCRAM-SHA-512"},
	} {
		credentialRand = bytes.NewReader(bytes.Repeat([]byte{0xab}, 18))
		cfg := config.Default()
		cfg.SASLMechanisms = c.mechs
		cfg.ACLSuperUsers = []string{"User:ops"}
		user, pass, mech, err := provisionConsoleCredential(&cfg)
		if err != nil {
			t.Fatalf("%v: %v", c.mechs, err)
		}
		if mech != c.want || user != consoleUserName || pass != strings.Repeat("ab", 18) {
			t.Errorf("%v: user %q mechanism %q password %q", c.mechs, user, mech, pass)
		}
		if cfg.Credentials[consoleUserName] != pass {
			t.Errorf("%v: the credential was not added", c.mechs)
		}
		if len(cfg.ACLSuperUsers) != 2 || cfg.ACLSuperUsers[0] != "User:ops" || cfg.ACLSuperUsers[1] != "User:"+consoleUserName {
			t.Errorf("%v: super users %v", c.mechs, cfg.ACLSuperUsers)
		}
	}

	credentialRand = bytes.NewReader(bytes.Repeat([]byte{1}, 18))
	cfg := config.Default()
	cfg.Credentials = map[string]string{consoleUserName: "theirs"}
	if _, _, _, err := provisionConsoleCredential(&cfg); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Errorf("a configured user of the reserved name: %v", err)
	}
	if cfg.Credentials[consoleUserName] != "theirs" {
		t.Error("the configured password was replaced")
	}
	credentialRand = bytes.NewReader([]byte{1, 2, 3})
	cfg = config.Default()
	if _, _, _, err := provisionConsoleCredential(&cfg); err == nil {
		t.Error("a short read of random bytes was accepted")
	}
}

func TestConsoleHostsAndDialAddress(t *testing.T) {
	cfg := config.Default()
	cfg.ConsoleAllowHosts = []string{"a.example"}
	for _, c := range []struct {
		addr string
		want []string
	}{
		{"127.0.0.1:8080", []string{"a.example"}},
		{"devbox.local:8080", []string{"a.example", "devbox.local"}},
		{":8080", []string{"a.example"}},
		{"no-port", []string{"a.example"}},
	} {
		cfg.ConsoleAddr = c.addr
		if got := consoleHosts(cfg); strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("consoleHosts(%q) = %v, want %v", c.addr, got, c.want)
		}
	}

	if got, err := consoleDialAddr(cfg, fakeAddr("0.0.0.0:9092")); err != nil || got != "127.0.0.1:9092" {
		t.Errorf("no advertised host: %q %v", got, err)
	}
	cfg.AdvertisedHost = "kafka.x.doze"
	if got, err := consoleDialAddr(cfg, fakeAddr("0.0.0.0:9092")); err != nil || got != "kafka.x.doze:9092" {
		t.Errorf("an advertised host: %q %v", got, err)
	}
	if _, err := consoleDialAddr(cfg, fakeAddr("nonsense")); err == nil {
		t.Error("an address without a port was accepted")
	}
}

func TestWorkspaceLabelIsEmptyForTheDefaultName(t *testing.T) {
	if workspaceLabel(config.DefaultName) != "" || workspaceLabel("doze-kafka") != "" || workspaceLabel("harbour") != "harbour" {
		t.Error("the workspace label is wrong")
	}
}
