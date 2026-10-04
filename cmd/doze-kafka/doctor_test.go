package main

import (
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	names "github.com/doze-dev/doze-names"
)

type fakeRegistry map[string]names.Entry

func (f fakeRegistry) Snapshot() map[string]names.Entry { return f }

// Something is missing and the operator needs to know WHICH thing and what to run.
func TestDoctorNamesWhatIsMissingAndHowToFixIt(t *testing.T) {
	var b strings.Builder
	status := names.Status{Platform: "darwin", Steps: []names.Step{
		{Name: "loopback pool", Done: true, Detail: "127.0.0.2-65 aliased on lo0"},
		{Name: "resolver route", Done: false, Detail: "/etc/resolver/doze missing"},
	}}
	code := doctorTo(&b, config.Default(), status, fakeRegistry{})
	out := b.String()
	if code != 0 {
		t.Errorf("exit = %d; being told what is missing is the command WORKING", code)
	}
	for _, want := range []string{"✗ resolver route", "✓ loopback pool", "dns-setup", "--print", "--listen"} {
		if !strings.Contains(out, want) {
			t.Errorf("the report is missing %q:\n%s", want, out)
		}
	}
}

// "My name stopped working" is almost always another process holding it.
func TestDoctorNamesTheHolder(t *testing.T) {
	var b strings.Builder
	reg := fakeRegistry{"kafka.harbour.doze": {IP: "127.0.0.2", PID: 4242, Owner: "doze-aws", Target: "127.0.0.2:9093"}}
	doctorTo(&b, config.Default(), names.Status{Platform: "darwin"}, reg)
	for _, want := range []string{"kafka.harbour.doze", "doze-aws", "4242", "127.0.0.2:9093"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("holder report is missing %q:\n%s", want, b.String())
		}
	}
}

// doctor reports the mode in use: under --listen no name is claimed, and printing
// "answers on kafka.<name>.doze" would describe one nothing holds.
func TestDoctorReportsWhicheverModeIsInUse(t *testing.T) {
	cfg := config.Default()
	cfg.Name = "harbour"
	var b strings.Builder
	doctorTo(&b, cfg, names.Status{Platform: "darwin"}, fakeRegistry{})
	if !strings.Contains(b.String(), "kafka.harbour.doze:9092") {
		t.Errorf("name mode should say where the name answers:\n%s", b.String())
	}

	cfg.ListenAddr = "127.0.0.1:9092"
	b.Reset()
	doctorTo(&b, cfg, names.Status{Platform: "darwin"}, fakeRegistry{})
	out := b.String()
	if !strings.Contains(out, "127.0.0.1:9092 (--listen; no .doze name is claimed)") || strings.Contains(out, "kafka.harbour.doze") {
		t.Errorf("--listen should be reported as an address with no name:\n%s", out)
	}
}
