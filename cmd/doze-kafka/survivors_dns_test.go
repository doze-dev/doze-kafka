//go:build unix

package main

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// dnsSetup runs `doze-kafka dns-setup args` as main does, and returns what it wrote to the
// process's stdout and stderr (it writes there, not to the writers realMain is given) and
// its exit code.
func dnsSetup(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldOut, oldErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = outW, errW
	outDone, errDone := make(chan string, 1), make(chan string, 1)
	go func() { b, _ := io.ReadAll(outR); outDone <- string(b) }()
	go func() { b, _ := io.ReadAll(errR); errDone <- string(b) }()
	code = realMain(append([]string{"dns-setup"}, args...), io.Discard, io.Discard)
	os.Stdout, os.Stderr = oldOut, oldErr
	outW.Close()
	errW.Close()
	return <-outDone, <-errDone, code
}

// dns-setup: what it does not understand is a usage error, --print shows the script and
// changes nothing, and --check says what the machine has and, when it is not ready, what
// to do.
func TestDNSSetupAsCommanded(t *testing.T) {
	_, stderr, code := dnsSetup(t, "--no-such-flag")
	if code != exitUsage || !strings.Contains(stderr, "usage: doze-kafka dns-setup") || !strings.Contains(stderr, "doze-kafka dns-setup:") {
		t.Errorf("a bad flag: exit %d\n%s", code, stderr)
	}

	stdout, _, code := dnsSetup(t, "--help")
	if code != 0 || !strings.Contains(stdout, "usage: doze-kafka dns-setup") {
		t.Errorf("--help: exit %d\n%s", code, stdout)
	}

	for _, args := range [][]string{{"--print"}, {"--uninstall", "--print"}} {
		stdout, stderr, code := dnsSetup(t, args...)
		if code != 0 || strings.TrimSpace(stdout) == "" {
			t.Errorf("%v: exit %d\nstdout: %s\nstderr: %s", args, code, stdout, stderr)
		}
	}

	stdout, stderr, code = dnsSetup(t, "--check")
	hint := "\nrun `doze-kafka dns-setup` to finish, or serve on an address instead:\n  doze-kafka --listen 127.0.0.1:9092\n"
	switch code {
	case 0:
		if strings.Contains(stdout, hint) {
			t.Errorf("a machine that is ready is told to set up:\n%s", stdout)
		}
	case 1:
		if !strings.HasSuffix(stdout, hint) {
			t.Errorf("a machine that is not ready is not told what to do:\n%s", stdout)
		}
	default:
		t.Errorf("--check: exit %d\n%s\n%s", code, stdout, stderr)
	}
}

// Whether a name can be used is whether the machine says .doze resolves; a configuration
// with an address needs no zone at all.
func TestPrepareZoneFollowsTheMachine(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("as root the machine would be set up, which is not a test's business")
	}
	cfg := config.Default()
	cfg.Name = "preparezone"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	withAddress := cfg
	withAddress.ListenAddr = "127.0.0.1:0"
	if z, err := prepareZone(ctx, withAddress, quietLogger()); z != nil || err != nil {
		t.Errorf("with an address: zone %v, error %v", z, err)
	}

	_, want := ensureNames(liveNameEnv())
	z, err := prepareZone(ctx, cfg, quietLogger())
	if z != nil {
		defer z.close()
	}
	if (err != nil) != (want != nil) || (err == nil) != (z != nil) {
		t.Errorf("prepareZone: zone %v, error %v; the machine says %v", z, err, want)
	}
}

// Closing a zone releases what it holds and is safe on one that holds nothing.
func TestClosingAZone(t *testing.T) {
	var none *zone
	none.close()
	(&zone{}).close()

	z := claimed(t, "closing", &probe{})
	if _, ok := z.reg.Snapshot()["kafka.closing.doze"]; !ok {
		t.Fatal("the name was not claimed")
	}
	z.close()
	if _, ok := z.reg.Snapshot()["kafka.closing.doze"]; ok {
		t.Error("the name outlived the zone")
	}
}
