// Package compat holds the client-compatibility matrix (shell and the clients'
// own projects). Its one Go file tests the runner itself: compat/run.sh was
// green when cells skipped, even all of them (only a FAIL row failed it), so a
// CI job with no kcat, no uv and no Docker would have passed having run nothing.
package compat

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The runner against fake clients (a directory each, as the real ones): one
// that passes, one that skips (exit 77), one that fails, one that needs Docker.
// It boots the real binary for them, as it does for the real clients.
func TestRunnerIsGreenOnlyWhenEveryCellPassed(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary and boots it")
	}
	if runtime.GOOS == "windows" {
		t.Skip("the runner is a bash script; the matrix runs on Linux and macOS")
	}
	for _, tool := range []string{"bash", "openssl", "python3"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("the runner needs %s, which is not installed", tool)
		}
	}
	bin := filepath.Join(t.TempDir(), "doze-kafka")
	build := exec.Command("go", "build", "-o", bin, "./cmd/doze-kafka")
	build.Dir = ".."
	build.Env = append(os.Environ(), "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	runner, err := filepath.Abs("run.sh")
	if err != nil {
		t.Fatal(err)
	}

	clients := t.TempDir()
	client := func(name, script string, docker bool) {
		t.Helper()
		dir := filepath.Join(clients, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "run.sh"), []byte("#!/usr/bin/env bash\n"+script+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		if docker {
			if err := os.WriteFile(filepath.Join(dir, "docker"), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Each fake proves it was really handed a broker before it answers.
	reach := `(echo >/dev/tcp/${BOOTSTRAP%:*}/${BOOTSTRAP##*:}) 2>/dev/null || { echo "no broker at $BOOTSTRAP"; exit 1; }`
	client("passes", reach+"\necho \"ok   reached $BOOTSTRAP ($SECURITY)\"", false)
	client("skips", "echo 'the toolchain is not installed'; exit 77", false)
	client("fails", reach+"\necho 'FAIL on purpose'; exit 1", false)
	client("secure-only", `[ "$SECURITY" = scram-tls ] || { echo 'plain is not for this client'; exit 77; }; echo ok`, false)

	run := func(env []string, args ...string) (int, string) {
		t.Helper()
		cmd := exec.Command("bash", append([]string{runner, "-b", bin}, args...)...)
		cmd.Env = append(os.Environ(), "COMPAT_CLIENTS_DIR="+clients)
		// The developer's own setting must not reach the fakes; CI is set per case.
		cmd.Env = append(cmd.Env, "ALLOW_SKIP=", "CI=")
		cmd.Env = append(cmd.Env, env...)
		out, err := cmd.CombinedOutput()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatalf("running the runner: %v\n%s", err, out)
		}
		return code, string(out)
	}
	cases := []struct {
		name string
		env  []string
		args []string
		code int      // the exit status wanted.
		say  []string // what the output must contain.
	}{
		{"every cell passes", nil, []string{"passes"}, 0,
			[]string{"PASS  passes (plain)", "PASS  passes (scram-tls)", "2 of 2 cells passed, 0 skipped"}},
		{"a skipped client fails the run", nil, []string{"passes", "skips"}, 1,
			[]string{"SKIP  skips (plain): the toolchain is not installed", "SKIPPED, which is not a pass: skips (scram-tls)", "2 of 4 cells passed, 2 skipped", "the matrix is NOT green"}},
		{"every client skipping fails the run", nil, []string{"skips"}, 1,
			[]string{"0 of 2 cells passed, 2 skipped", "the matrix is NOT green"}},
		{"one skipped cell of a client fails the run", nil, []string{"secure-only"}, 1,
			[]string{"SKIP  secure-only (plain)", "PASS  secure-only (scram-tls)", "1 of 2 cells passed, 1 skipped"}},
		{"a skip that was allowed by name passes, and is counted", []string{"ALLOW_SKIP=skips"}, []string{"passes", "skips"}, 0,
			[]string{"allowed to skip (ALLOW_SKIP): skips (plain)", "2 of 4 cells passed, 2 skipped"}},
		{"allowing one client does not allow another", []string{"ALLOW_SKIP=skips"}, []string{"skips", "secure-only"}, 1,
			[]string{"SKIPPED, which is not a pass: secure-only (plain)"}},
		{"in CI nothing may be allowed to skip", []string{"ALLOW_SKIP=skips", "CI=true"}, []string{"passes", "skips"}, 2,
			[]string{"in CI every cell runs"}},
		{"in CI a skip fails", []string{"CI=true"}, []string{"passes", "skips"}, 1,
			[]string{"SKIPPED, which is not a pass"}},
		{"a failing client fails the run", nil, []string{"passes", "fails"}, 1,
			[]string{"FAIL  fails (plain)", "2 of 4 cells passed, 0 skipped", "the matrix is NOT green"}},
		{"ALLOW_SKIP must name a client", []string{"ALLOW_SKIP=nosuch"}, []string{"passes"}, 2,
			[]string{"ALLOW_SKIP names nosuch, which is not a client"}},
		{"an unknown client is an error, not an empty matrix", nil, []string{"nosuch"}, 2,
			[]string{"no such client: nosuch"}},
		{"with no client named, every directory is a client", nil, nil, 1,
			[]string{"PASS  passes (plain)", "FAIL  fails (scram-tls)", "3 of 8 cells passed, 3 skipped (4 clients, plain and scram-tls)"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			code, out := run(c.env, c.args...)
			if code != c.code {
				t.Errorf("exit %d, want %d\n%s", code, c.code, out)
			}
			for _, s := range c.say {
				if !strings.Contains(out, s) {
					t.Errorf("the output does not say %q:\n%s", s, out)
				}
			}
		})
	}

	// A client that needs Docker, on a machine where docker is not on PATH: both
	// its cells are skipped by name (they used to be one row with no security,
	// which the verdict could not match to a cell).
	t.Run("docker missing skips both cells of a docker client, and that fails", func(t *testing.T) {
		client("in-docker", "echo ok", true)
		defer os.RemoveAll(filepath.Join(clients, "in-docker"))
		bare := t.TempDir()
		for _, tool := range []string{"bash", "openssl", "python3", "env", "mktemp", "dirname", "basename", "tee", "sed", "tail", "seq", "sleep", "rm", "tr", "cat"} {
			if p, err := exec.LookPath(tool); err == nil {
				os.Symlink(p, filepath.Join(bare, tool))
			}
		}
		code, out := run([]string{"PATH=" + bare}, "in-docker")
		if code != 1 || !strings.Contains(out, "SKIP  in-docker (plain): docker is not available") || !strings.Contains(out, "0 of 2 cells passed, 2 skipped") {
			t.Errorf("exit %d\n%s", code, out)
		}
	})
}

// The matrix the documents count is the directories: a client added without the
// plan and the README being told shows up here.
func TestTheMatrixIsTwelveClients(t *testing.T) {
	dirs, err := filepath.Glob(filepath.Join("*", "run.sh"))
	if err != nil {
		t.Fatal(err)
	}
	const clients = 12
	if len(dirs) != clients {
		t.Errorf("compat/ has %d client directories; the documents say %d clients, %d cells (compat/README.md, plan/): correct them and this number", len(dirs), clients, 2*clients)
	}
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range dirs {
		if name := filepath.Dir(d); !strings.Contains(string(readme), "`"+name+"`") {
			t.Errorf("compat/README.md's table has no row for %s", name)
		}
	}
}
