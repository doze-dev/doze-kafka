package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// docs/configuration.md: a setting that cannot work is one line on stderr and exit
// code 2, before anything starts. Each of these is a configuration the binary
// cannot run, given to the real binary in an empty directory: it must say so in one
// line, exit 2, log nothing, listen on nothing and leave the directory as it was
// (no ./data, no meta.db). --zone=false without --listen used to get past every
// check and fail after the start-up log line, exit 1, with "\n" inside the logfmt.
func TestEveryConfigurationErrorIsOneLineAndExitTwo(t *testing.T) {
	if testing.Short() {
		t.Skip("links the binary")
	}
	cert, key := selfSigned(t)
	scratch := t.TempDir()
	badTOML := filepath.Join(scratch, "bad.toml")
	os.WriteFile(badTOML, []byte("listen = \n"), 0o644)
	unknownKey := filepath.Join(scratch, "unknown.toml")
	os.WriteFile(unknownKey, []byte("listne = \"127.0.0.1:0\"\n"), 0o644)
	zoneOff := filepath.Join(scratch, "zone.toml")
	os.WriteFile(zoneOff, []byte("zone = false\n"), 0o644)

	l := []string{"--listen", "127.0.0.1:0"}
	cases := []struct {
		name string
		args []string
		env  []string
	}{
		{"zone off, no listen", []string{"--zone=false"}, nil},
		{"zone off in the file, no listen", []string{"--config", zoneOff}, nil},
		{"DOZE_ZONE=off, no listen", nil, []string{"DOZE_ZONE=off"}},
		{"unknown flag", append(l, "--no-such-flag"), nil},
		{"stray argument", append(l, "listen"), nil},
		{"listen not host:port", []string{"--listen", "9092"}, nil},
		{"advertised host with a port", append(l, "--advertised-host", "a:1"), nil},
		{"advertised port out of range", append(l, "--advertised-port", "70000"), nil},
		{"console addr not host:port", append(l, "--console-addr", "9093"), nil},
		{"registry addr not host:port", append(l, "--schema-registry-addr", "x"), nil},
		{"tls cert without key", append(l, "--tls-cert", cert), nil},
		{"tls files missing", append(l, "--tls-cert", filepath.Join(scratch, "no.pem"), "--tls-key", filepath.Join(scratch, "no.key")), nil},
		{"tls cert and key swapped", append(l, "--tls-cert", key, "--tls-key", cert), nil},
		{"sasl users without a mechanism", append(l, "--sasl-user", "a:b"), nil},
		{"sasl mechanism unknown", append(l, "--sasl-mechanisms", "GSSAPI", "--sasl-user", "a:b"), nil},
		{"sasl without users", append(l, "--sasl-mechanisms", "PLAIN"), nil},
		{"sasl user without a password", append(l, "--sasl-mechanisms", "PLAIN", "--sasl-user", "a:"), nil},
		{"acl default unknown", append(l, "--acl-default", "maybe"), nil},
		{"acl super user without a type", append(l, "--acl-super-user", "admin"), nil},
		{"kafka version unknown", append(l, "--kafka-version", "9"), nil},
		{"zero default partitions", append(l, "--default-partitions", "0"), nil},
		{"default above max partitions", append(l, "--default-partitions", "10", "--max-partitions", "5"), nil},
		{"segment bytes zero", append(l, "--segment-bytes", "0"), nil},
		{"retention ms not a number", append(l, "--retention-ms", "soon"), nil},
		{"fsync interval not a duration", append(l, "--fsync-interval", "often"), nil},
		{"session timeout not a duration", append(l, "--consumer-session-timeout", "x"), nil},
		{"offsets retention negative", append(l, "--offsets-retention", "-1h"), nil},
		{"log level unknown", append(l, "--log-level", "loud"), nil},
		{"log format unknown", append(l, "--log-format", "xml"), nil},
		{"console prefix not a path", append(l, "--console-prefix", "no-slash"), nil},
		{"config file missing", append(l, "--config", filepath.Join(scratch, "none.toml")), nil},
		{"config file not TOML", append(l, "--config", badTOML), nil},
		{"config file unknown key", append(l, "--config", unknownKey), nil},
		{"console beyond loopback with SASL", append(l, "--console-addr", "0.0.0.0:0", "--sasl-mechanisms", "PLAIN", "--sasl-user", "a:b"), nil},
		{"console beyond loopback with TLS", append(l, "--console-addr", "0.0.0.0:0", "--tls-cert", cert, "--tls-key", key), nil},
	}
	bin := binary(t)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, c.args...)
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), c.env...)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 2 {
				t.Errorf("%v: %v, want exit code 2\nstderr: %s", c.args, err, stderr.String())
			}
			lines := strings.Split(strings.TrimRight(stderr.String(), "\n"), "\n")
			if len(lines) != 1 || lines[0] == "" || strings.Contains(stderr.String(), "level=") {
				t.Errorf("%v: stderr is not one plain line:\n%s", c.args, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Errorf("%v: printed on stdout:\n%s", c.args, stdout.String())
			}
			if entries, _ := os.ReadDir(dir); len(entries) != 0 {
				var names []string
				for _, e := range entries {
					names = append(names, e.Name())
				}
				t.Errorf("%v: left %v in the directory", c.args, names)
			}
		})
	}
}
