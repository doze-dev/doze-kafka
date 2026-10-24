package main

import (
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	names "github.com/doze-dev/doze-names"
)

func TestDoctorSaysWhenThereIsNothingToReport(t *testing.T) {
	cfg := config.Default()
	var b strings.Builder
	doctorTo(&b, cfg, names.Status{Platform: "linux"}, fakeRegistry{})
	out := b.String()
	for _, want := range []string{"(nothing reported — this platform has no setup to do)", "names registered (0)", "(none — no doze binary is running)"} {
		if !strings.Contains(out, want) {
			t.Errorf("the report lacks %q:\n%s", want, out)
		}
	}

	b.Reset()
	status := names.Status{Platform: "linux", Steps: []names.Step{{Name: "resolver", Done: true, Detail: "ok"}}}
	reg := fakeRegistry{"kafka.a.doze": {IP: "127.0.0.2", PID: 7, Owner: "doze-kafka"}}
	doctorTo(&b, cfg, status, reg)
	out = b.String()
	for _, never := range []string{"nothing reported", "(none — no doze binary is running)"} {
		if strings.Contains(out, never) {
			t.Errorf("the report says %q although there is something:\n%s", never, out)
		}
	}
	if !strings.Contains(out, "names registered (1)") || strings.Contains(out, "  ->") {
		t.Errorf("registry section:\n%s", out)
	}
}

func TestRunDoctorRefusesBadFlagsAndReadsGoodOnes(t *testing.T) {
	t.Chdir(t.TempDir())
	var out, errs strings.Builder
	if code := runDoctor([]string{"--no-such-flag"}, &out, &errs); code != exitUsage || errs.Len() == 0 || out.Len() != 0 {
		t.Errorf("a bad flag: exit %d, stdout %q, stderr %q", code, out.String(), errs.String())
	}
	out.Reset()
	errs.Reset()
	runDoctor([]string{"--listen", "127.0.0.1:9092"}, &out, &errs)
	if errs.Len() != 0 || !strings.Contains(out.String(), "127.0.0.1:9092 (--listen; no .doze name is claimed)") {
		t.Errorf("good flags: stdout %q, stderr %q", out.String(), errs.String())
	}
	out.Reset()
	if code := runDoctor([]string{"--help"}, &out, &errs); code != 0 || !strings.Contains(out.String(), "--listen") {
		t.Errorf("--help: exit %d, stdout %q", code, out.String())
	}
}

// The env command under --listen carries the host clients are told to use only when the
// listener alone would not give it, and the console and registry addresses only when
// they are served.
func TestEnvHintUnderListenCarriesOnlyWhatTheListenerDoesNotGive(t *testing.T) {
	t.Chdir(t.TempDir())
	load := func(args ...string) startup {
		t.Helper()
		st, err := loadEnvConfig(append([]string{"--config", ""}, args...))
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	plain := load("--listen", "127.0.0.1:9092")
	got := envHint(plain, envBound{listen: "127.0.0.1:9092"})
	if strings.Contains(got, "--advertised-host") || strings.Contains(got, "--console-addr") || strings.Contains(got, "--schema-registry-addr") {
		t.Errorf("nothing beyond the listener: %s", got)
	}
	if !strings.Contains(got, "--listen 127.0.0.1:9092") || !strings.HasSuffix(got, "--config ''") {
		t.Errorf("hint = %s", got)
	}

	advertised := load("--listen", "127.0.0.1:9092", "--advertised-host", "box.local")
	if got := envHint(advertised, envBound{listen: "127.0.0.1:9092"}); !strings.Contains(got, "--advertised-host box.local") {
		t.Errorf("an advertised host the listener would not give: %s", got)
	}

	got = envHint(plain, envBound{listen: "127.0.0.1:9092", console: "127.0.0.1:8080", registry: "127.0.0.1:8081"})
	if !strings.Contains(got, "--console-addr 127.0.0.1:8080") || !strings.Contains(got, "--schema-registry-addr 127.0.0.1:8081") {
		t.Errorf("served console and registry: %s", got)
	}
}
