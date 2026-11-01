package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// The summary ends with the command that points a shell at this broker. That
// command is run here, as printed, in the broker's directory, and what it exports
// has to be what the summary says: under --listen the bare `doze-kafka env` used
// to print the .doze name, an address this broker was not on.
func TestTheEnvCommandInTheSummaryGivesThisBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("links the binary and boots it")
	}
	if runtime.GOOS == "windows" {
		t.Skip("runs the printed command with sh")
	}
	cert, key := selfSigned(t)
	cases := map[string][]string{
		"free ports":      {"--listen", "127.0.0.1:0"},
		"all given":       {"--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--schema-registry-addr", "127.0.0.1:0"},
		"no registry":     {"--listen", "127.0.0.1:0", "--schema-registry=false", "--console=false"},
		"sasl":            {"--listen", "127.0.0.1:0", "--sasl-mechanisms", "SCRAM-SHA-512,PLAIN", "--sasl-user", "alice:s3cret"},
		"tls":             {"--listen", "127.0.0.1:0", "--tls-cert", cert, "--tls-key", key},
		"tls, plain reg.": {"--listen", "127.0.0.1:0", "--tls-cert", cert, "--tls-key", key, "--schema-registry-tls=false"},
		"advertised host": {"--listen", "127.0.0.1:0", "--advertised-host", "localhost"},
		"every interface": {"--listen", ":0"},
	}
	for name, flags := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			// A doze-kafka.toml in the directory that says something else: the bare
			// command would read it, so the printed one must not rely on it.
			os.WriteFile(filepath.Join(dir, "doze-kafka.toml"), []byte("listen = \"127.0.0.1:19999\"\n"), 0o644)
			args := append(append([]string{}, flags...), "--config", "", "--data-dir", filepath.Join(dir, "data"))
			out, _ := startBinary(t, dir, args...)
			summary := out.String()

			hint := regexp.MustCompile(`eval "\$\((doze-kafka env[^"]*)\)"`).FindStringSubmatch(summary)
			if hint == nil {
				t.Fatalf("no env command in the summary:\n%s", summary)
			}
			sh := exec.Command("sh", "-c", hint[1])
			sh.Dir = dir
			sh.Env = append(os.Environ(), "PATH="+filepath.Dir(binary(t))+string(os.PathListSeparator)+os.Getenv("PATH"))
			env, err := sh.CombinedOutput()
			if err != nil {
				t.Fatalf("%s: %v\n%s", hint[1], err, env)
			}
			exports := map[string]string{}
			for _, m := range regexp.MustCompile(`(?m)^export (\w+)=(\S+)$`).FindAllStringSubmatch(string(env), -1) {
				exports[m[1]] = m[2]
			}
			line := func(label string) string {
				m := regexp.MustCompile(`(?m)^\s+` + label + `\s+(\S+)`).FindStringSubmatch(summary)
				if m == nil {
					return ""
				}
				return m[1]
			}
			want := line("kafka")
			if strings.Contains(name, "advertised") {
				// env gives the host clients are told to use (env_test.go), on this port.
				want = "localhost:" + want[strings.LastIndexByte(want, ':')+1:]
			}
			if got := exports["KAFKA_BOOTSTRAP_SERVERS"]; got != want {
				t.Errorf("%s exports KAFKA_BOOTSTRAP_SERVERS=%s; the summary says kafka %s", hint[1], got, want)
			}
			if got, want := exports["SCHEMA_REGISTRY_URL"], line("registry"); got != want {
				t.Errorf("%s exports SCHEMA_REGISTRY_URL=%q; the summary says registry %q", hint[1], got, want)
			}
			if c := line("console"); c != "" && !strings.Contains(string(env), "# Console: "+c) {
				t.Errorf("%s does not name the console at %s:\n%s", hint[1], c, env)
			}
			if strings.Contains(strings.Join(flags, " "), "sasl") {
				if exports["KAFKA_SASL_MECHANISM"] != "SCRAM-SHA-512" {
					t.Errorf("%s exports KAFKA_SASL_MECHANISM=%q, want SCRAM-SHA-512", hint[1], exports["KAFKA_SASL_MECHANISM"])
				}
				if strings.Contains(hint[1], "s3cret") {
					t.Errorf("the summary prints a password: %s", hint[1])
				}
			}
		})
	}
}

// With nothing that a bare `doze-kafka env` would not also see, the hint stays
// the short command.
func TestTheEnvHintIsBareWhenBareIsRight(t *testing.T) {
	t.Chdir(t.TempDir())
	st, err := loadConfig([]string{"--config", ""})
	if err != nil {
		t.Fatal(err)
	}
	if got := envHint(st, envBound{}); got != "doze-kafka env" {
		t.Errorf("hint = %q, want the bare command", got)
	}
}
