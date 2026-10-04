package main

import (
	"net"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// Tests added by the October audit for the command line.

// The word after a flag that takes a value is that value. `--data-dir -v` used to
// print the version.
func TestAFlagsValueIsNeverReadAsTheVersionFlag(t *testing.T) {
	for _, tc := range []struct {
		in   []string
		show bool
	}{
		{[]string{"--data-dir", "-v"}, false},
		{[]string{"--data-dir", "--version"}, false},
		{[]string{"--sasl-user", "-version"}, false},
		{[]string{"--config", "-v", "--listen", ":1"}, false},
		{[]string{"--console=false", "--version"}, true},
		{[]string{"--console", "-v"}, true}, // --console is a switch: -v is its own flag.
		{[]string{"--listen", ":1", "-v"}, true},
		{[]string{"--", "--version"}, false},
	} {
		out, show, notices := normalizeArgs(tc.in)
		if show != tc.show || len(notices) != 0 {
			t.Errorf("normalizeArgs(%v): show=%v notices=%v, want show=%v", tc.in, show, notices, tc.show)
		}
		if !tc.show && !reflect.DeepEqual(out, tc.in) {
			t.Errorf("normalizeArgs(%v) rewrote the arguments to %v", tc.in, out)
		}
	}
	t.Chdir(t.TempDir())
	if code, out, errOut := run3("config", "print", "--version"); code != 0 || !strings.HasPrefix(out, "doze-kafka ") || errOut != "" {
		t.Errorf("config print --version: code %d, stdout %q, stderr %q; want the version", code, out, errOut)
	}
}

// One line, in the spelling --help uses, with a suggestion for a near miss.
func TestFlagErrorsAreOneLineInTheSpellingOfHelp(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--listn", ":1"}, "doze-kafka: unknown flag --listn — did you mean --listen?\n"},
		{[]string{"--retention", "5"}, "doze-kafka: unknown flag --retention — did you mean --retention-bytes?\n"},
		{[]string{"--zzzzzz"}, "doze-kafka: unknown flag --zzzzzz\n"},
		{[]string{"--listen"}, "doze-kafka: --listen needs a value\n"},
		{[]string{"--advertised-port", "abc"}, `doze-kafka: --advertised-port: "abc" is not a valid value (parse error)` + "\n"},
		{[]string{"--console=maybe"}, `doze-kafka: --console: "maybe" is not true or false` + "\n"},
	} {
		code, out, errOut := run3(tc.args...)
		if code != exitUsage || out != "" || errOut != tc.want {
			t.Errorf("%v: code %d, stderr %q; want exit 2 and %q", tc.args, code, errOut, tc.want)
		}
	}
}

// Settings that used to be taken as given and fail later, or never.
func TestSettingsAreValidatedBeforeAnythingStarts(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--advertised-port", "70000"}, "advertised-port"},
		{[]string{"--advertised-port", "-1"}, "advertised-port"},
		{[]string{"--advertised-host", "localhost:9092"}, "advertised-host"},
		{[]string{"--advertised-host", "http://broker"}, "advertised-host"},
		{[]string{"--consumer-session-timeout", "0"}, "consumer-session-timeout"},
		{[]string{"--consumer-session-timeout", "-5s"}, "consumer-session-timeout"},
		{[]string{"--retention-ms", "9223372036854775807"}, "retention-ms"},
		{[]string{"--fsync-interval", "500ns"}, "fsync-interval"},
		{[]string{"--pprof-addr", "nope"}, "pprof address"},
		{[]string{"--tls-cert", "missing.pem", "--tls-key", "missing.key"}, "cannot load the TLS certificate"},
	} {
		for _, prefix := range [][]string{nil, {"config", "print"}} {
			args := append(append([]string{}, prefix...), tc.args...)
			code, out, errOut := run3(args...)
			if code != exitUsage || out != "" || !strings.Contains(errOut, tc.want) || strings.Count(errOut, "\n") != 1 {
				t.Errorf("%v: code %d, stdout %q, stderr %q; want exit 2 and one line naming %q", args, code, out, errOut, tc.want)
			}
		}
	}
	for _, ok := range [][]string{
		{"--advertised-host", "::1"}, {"--advertised-host", "kafka.internal"}, {"--advertised-port", "65535"},
		{"--retention-ms", "9223372036854"}, {"--fsync-interval", "0"}, {"--fsync-interval", "1ms"}, {"--pprof-addr", "127.0.0.1:6060"},
	} {
		if code, _, errOut := run3(append([]string{"config", "print"}, ok...)...); code != 0 {
			t.Errorf("%v was refused: %s", ok, errOut)
		}
	}
}

// --config "" means no file, which is the way to ignore a doze-kafka.toml in the
// working directory. It used to load that file anyway.
func TestAnEmptyConfigFlagMeansNoFile(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeTOML(t, dir, `listen = ":19092"`)
	if st, err := loadConfig(nil); err != nil || st.cfg.ListenAddr != ":19092" || st.configFile == "" {
		t.Fatalf("the file in the working directory should be used by default: %+v %v", st.cfg.ListenAddr, err)
	}
	st, err := loadConfig([]string{"--config", ""})
	if err != nil || st.cfg.ListenAddr != config.Default().ListenAddr || st.configFile != "" {
		t.Errorf(`--config "": listen %q, file %q, err %v; want the defaults and no file`, st.cfg.ListenAddr, st.configFile, err)
	}
}

// The summary gives an address to connect to. A wildcard listener's own address
// is not one.
func TestSummaryAddressIsDialable(t *testing.T) {
	for _, tc := range []struct{ bound, host, want string }{
		{"[::]:9092", "localhost", "localhost:9092   (listening on every interface)"},
		{"0.0.0.0:19092", "host.docker.internal", "host.docker.internal:19092   (listening on every interface)"},
		{"127.0.0.1:9092", "localhost", "127.0.0.1:9092"},
		{"192.168.1.5:9092", "localhost", "192.168.1.5:9092"},
	} {
		addr, err := net.ResolveTCPAddr("tcp", tc.bound)
		if err != nil {
			t.Fatal(err)
		}
		if got := dialable(addr, tc.host); got != tc.want {
			t.Errorf("dialable(%s, %s) = %q, want %q", tc.bound, tc.host, got, tc.want)
		}
	}
}

// Closing the terminal sends SIGHUP. It must be a clean shutdown like Ctrl-C.
func TestHangupIsAShutdownSignal(t *testing.T) {
	var found bool
	for _, s := range shutdownSignals {
		if s == os.Signal(syscall.SIGHUP) {
			found = true
		}
	}
	if !found {
		t.Errorf("shutdownSignals = %v; SIGHUP is missing", shutdownSignals)
	}
}
