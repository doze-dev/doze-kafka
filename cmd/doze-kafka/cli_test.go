package main

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

func run3(args ...string) (code int, stdout, stderr string) {
	var o, e bytes.Buffer
	code = realMain(args, &o, &e)
	return code, o.String(), e.String()
}

// --version used to select the emulated Kafka major. It now prints the program's
// version, and the old use is --kafka-version; a numeric --version still works,
// with a notice, so existing scripts keep running while they are updated.
func TestVersionFlagMeansTheBinaryVersionAndOldUseIsRewritten(t *testing.T) {
	for _, tc := range []struct {
		in      []string
		out     []string
		show    bool
		deprecn bool
	}{
		{[]string{"--version"}, nil, true, false},
		{[]string{"-v"}, nil, true, false},
		{[]string{"-version"}, nil, true, false},
		{[]string{"--version", "3"}, []string{"--kafka-version", "3"}, false, true},
		{[]string{"--version=2", "--listen", ":1"}, []string{"--kafka-version=2", "--listen", ":1"}, false, true},
		{[]string{"--listen", ":1"}, []string{"--listen", ":1"}, false, false},
	} {
		out, show, notices := normalizeArgs(tc.in)
		if show != tc.show || !reflect.DeepEqual(out, tc.out) && !(len(out) == 0 && len(tc.out) == 0) || (len(notices) > 0) != tc.deprecn {
			t.Errorf("normalizeArgs(%v) = %v, show=%v, notices=%v; want %v, show=%v, deprecation=%v", tc.in, out, show, notices, tc.out, tc.show, tc.deprecn)
		}
	}

	t.Chdir(t.TempDir())
	st, err := loadConfig([]string{"--version", "3"})
	if err != nil || st.cfg.Version != 3 || len(st.notices) != 1 || !strings.Contains(st.notices[0], "--kafka-version 3") {
		t.Fatalf("--version 3: %+v, %v", st, err)
	}
	if st, err = loadConfig([]string{"--kafka-version", "2"}); err != nil || st.cfg.Version != 2 {
		t.Fatalf("--kafka-version 2: %+v, %v", st, err)
	}
	code, out, _ := run3("--version")
	if code != 0 || !strings.HasPrefix(out, "doze-kafka ") {
		t.Errorf("--version: code %d, out %q", code, out)
	}
	if code, out, _ = run3("version"); code != 0 || !strings.Contains(out, "--kafka-version") {
		t.Errorf("version: code %d, out %q", code, out)
	}
}

// A stray word used to be ignored by the flag package, so `doze-kafka listen :9092`
// started the broker on the default port with no complaint.
func TestStrayArgumentsAreRefused(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"listen", ":9092"}, "did you mean --listen"},
		{[]string{"--listen", ":9092", "extra"}, "unexpected argument"},
		{[]string{"data-dir=/tmp/x"}, "did you mean --data-dir=/tmp/x"},
	} {
		_, err := loadConfig(tc.args)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("loadConfig(%v) = %v, want an error containing %q", tc.args, err, tc.want)
		}
	}
	// Commands are recognised only first; anything else there is an unknown command.
	if code, _, stderr := run3("listen", ":9092"); code != exitUsage || !strings.Contains(stderr, "did you mean --listen") {
		t.Errorf("a flag without dashes: code %d, stderr %q", code, stderr)
	}
	code, _, stderr := run3("frobnicate")
	if code != exitUsage || !strings.Contains(stderr, `unknown command "frobnicate"`) {
		t.Errorf("unknown command: code %d, stderr %q", code, stderr)
	}
}

// --help goes to stdout and succeeds; every flag is documented in a group.
func TestHelpIsGroupedAndComplete(t *testing.T) {
	code, out, errOut := run3("--help")
	if code != 0 || errOut != "" {
		t.Fatalf("--help: code %d, stderr %q", code, errOut)
	}
	for _, want := range []string{"Usage:", "Commands:", "Network:", "Storage:", "Security:", "Console:", "--kafka-version", "--log-level", "--zone", "dns-setup"} {
		if !strings.Contains(out, want) {
			t.Errorf("--help is missing %q", want)
		}
	}
	d := config.Default()
	fs, _ := newFlagSet(&d)
	grouped := map[string]int{}
	for _, g := range helpGroups {
		for _, n := range g.flags {
			grouped[n]++
		}
	}
	fs.VisitAll(func(f *flag.Flag) {
		if grouped[f.Name] != 1 {
			t.Errorf("flag --%s is in %d help groups, want exactly 1 (add it to helpGroups)", f.Name, grouped[f.Name])
		}
	})
	for n := range grouped {
		if fs.Lookup(n) == nil {
			t.Errorf("helpGroups lists --%s, which is not a flag", n)
		}
	}
	if c, o, _ := run3("help"); c != 0 || !strings.Contains(o, "Usage:") {
		t.Errorf("help command: code %d", c)
	}
}

// Every configuration mistake is reported the same way, before anything starts:
// one line, the same exit code.
func TestConfigErrorsShareOneShapeAndExitCode(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--listen", "nonsense"}, "listen address"},
		{[]string{"--log-level", "loud"}, "log level"},
		{[]string{"--log-format", "xml"}, "log format"},
		{[]string{"--kafka-version", "9"}, "unsupported Kafka version"},
		{[]string{"--sasl-user", "alice:pw"}, "no mechanism is enabled"},
		{[]string{"--sasl-mechanisms", "PLAIN"}, "no credentials"},
		{[]string{"--no-such-flag"}, "not defined"},
		{[]string{"--tls-cert", "x.pem"}, "TLS needs both"},
		{[]string{"--console-addr", "nope"}, "console address"},
	} {
		code, out, errOut := run3(tc.args...)
		if code != exitUsage || out != "" || !strings.Contains(errOut, tc.want) || !strings.HasPrefix(errOut, "doze-kafka: ") {
			t.Errorf("%v: code %d, stdout %q, stderr %q; want exit 2 and %q", tc.args, code, out, errOut, tc.want)
		}
	}
	// `config print` validates too, so what it prints is something the broker accepts.
	if code, out, errOut := run3("config", "print", "--sasl-user", "alice:pw"); code != exitUsage || out != "" || !strings.Contains(errOut, "no mechanism") {
		t.Errorf("config print of an invalid config: code %d, stdout %q, stderr %q", code, out, errOut)
	}
	if code, out, _ := run3("config", "print", "--listen", ":1234"); code != 0 || !strings.Contains(out, `listen = ":1234"`) {
		t.Errorf("config print: code %d, out %q", code, out)
	}
}

// The default listener is loopback; reaching the broker from elsewhere is a choice.
func TestDefaultsAreLoopbackAndSelfContained(t *testing.T) {
	t.Chdir(t.TempDir())
	st, err := loadConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.cfg.ListenAddr != "127.0.0.1:9092" || st.cfg.ConsoleAddr != "127.0.0.1:9093" {
		t.Errorf("default listeners = %q and %q, want loopback", st.cfg.ListenAddr, st.cfg.ConsoleAddr)
	}
	if want := filepath.Join("data", "meta.db"); filepath.Clean(st.cfg.MetaDB) != want {
		t.Errorf("default meta db = %q, want %s", st.cfg.MetaDB, want)
	}
	if !st.cfg.Zone || !st.cfg.Console {
		t.Errorf("zone and console should default on: %+v", st.cfg)
	}

	// An earlier version's ./meta.db keeps being used, with a notice.
	if err := os.WriteFile("meta.db", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err = loadConfig(nil)
	if err != nil || st.cfg.MetaDB != "meta.db" || len(st.notices) != 1 || !strings.Contains(st.notices[0], "earlier version") {
		t.Fatalf("legacy ./meta.db: %q notices %v err %v", st.cfg.MetaDB, st.notices, err)
	}
	// ...but not once the new location exists, and never for a custom data dir.
	if err := os.MkdirAll("data", 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join("data", "meta.db"), []byte("y"), 0o644)
	if st, _ = loadConfig(nil); filepath.Clean(st.cfg.MetaDB) != filepath.Join("data", "meta.db") {
		t.Errorf("with data/meta.db present the legacy file was still chosen: %q", st.cfg.MetaDB)
	}
	os.Remove(filepath.Join("data", "meta.db"))
	if st, _ = loadConfig([]string{"--data-dir", "elsewhere"}); filepath.Clean(st.cfg.MetaDB) != filepath.Join("elsewhere", "meta.db") {
		t.Errorf("a custom data dir picked up ./meta.db: %q", st.cfg.MetaDB)
	}
}

// The settings that used to be flag-only are file keys too, so "every setting is
// available either way" is true, and `config print` round-trips them.
func TestFileKeysCoverTheProcessSettings(t *testing.T) {
	path := writeTOML(t, t.TempDir(), `
console = false
console-addr = "127.0.0.1:19999"
console-allow-host = ["devbox.lan"]
zone = false
log-level = "debug"
log-format = "json"
pprof-addr = "127.0.0.1:6060"
kafka-version = 3
`)
	st, err := loadConfig([]string{"--config", path})
	if err != nil {
		t.Fatal(err)
	}
	c := st.cfg
	if c.Console || c.ConsoleAddr != "127.0.0.1:19999" || !reflect.DeepEqual(c.ConsoleAllowHosts, []string{"devbox.lan"}) ||
		c.Zone || c.LogLevel != "debug" || c.LogFormat != "json" || c.PprofAddr != "127.0.0.1:6060" || c.Version != 3 {
		t.Errorf("file did not apply: %+v", c)
	}
	// The older `version` key keeps working, and a flag still wins over the file.
	old := writeTOML(t, t.TempDir(), "version = 2\n")
	if st, err = loadConfig([]string{"--config", old}); err != nil || st.cfg.Version != 2 {
		t.Errorf("legacy version key: %+v %v", st.cfg.Version, err)
	}
	if st, _ = loadConfig([]string{"--config", path, "--zone"}); !st.cfg.Zone {
		t.Error("--zone did not override zone = false in the file")
	}
	if !errors.Is(errHelp, flag.ErrHelp) {
		t.Error("errHelp should be flag.ErrHelp")
	}
}
