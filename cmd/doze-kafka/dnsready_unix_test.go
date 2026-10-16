//go:build unix

package main

// The real binary, started the ways a broker really is started, with a stand-in
// `sudo` first in PATH that records being run. The decision under test is "may
// this process ask for a password", and the only observation that settles it is
// whether sudo was executed.

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
	names "github.com/doze-dev/doze-names"
)

var (
	builtOnce sync.Once
	builtBin  string
	builtErr  error
)

// builtBinary links the command once for the package's process tests.
func builtBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("links the binary and runs it")
	}
	builtOnce.Do(func() {
		dir, err := os.MkdirTemp("", "doze-kafka-bin")
		if err != nil {
			builtErr = err
			return
		}
		builtBin = filepath.Join(dir, "doze-kafka")
		if out, err := exec.Command("go", "build", "-o", builtBin, ".").CombinedOutput(); err != nil {
			builtErr = errors.New("build: " + err.Error() + "\n" + string(out))
		}
	})
	if builtErr != nil {
		t.Fatal(builtErr)
	}
	return builtBin
}

// sudoTrap is a directory holding a `sudo` that writes a line to a file and
// fails, and the environment that puts it first in PATH.
type sudoTrap struct {
	log string
	env []string
}

func newSudoTrap(t *testing.T) sudoTrap {
	t.Helper()
	if os.Geteuid() == 0 {
		// Root installs without sudo and without asking (the container case), so
		// starting the binary by name here would really change the machine.
		t.Skip("running as root: the binary would set .doze up directly")
	}
	dir := t.TempDir()
	log := filepath.Join(dir, "sudo-was-run")
	fake := filepath.Join(dir, "sudo")
	script := "#!/bin/sh\necho \"sudo $*\" >> '" + log + "'\nexit 1\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	env := []string{"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH")}
	for _, kv := range os.Environ() {
		// DOZE_ZONE=off in the developer's shell would skip the path under test.
		if !strings.HasPrefix(kv, "PATH=") && !strings.HasPrefix(kv, names.EnvZone+"=") {
			env = append(env, kv)
		}
	}
	// The trap is only a trap if it is the sudo that would be found.
	which := exec.Command("sh", "-c", "command -v sudo")
	which.Env = env
	if out, err := which.Output(); err != nil || strings.TrimSpace(string(out)) != fake {
		t.Fatalf("the stand-in sudo is not first in PATH: %q, %v", out, err)
	}
	return sudoTrap{log: log, env: env}
}

func (s sudoTrap) ran() bool {
	_, err := os.Stat(s.log)
	return err == nil
}

// runByName starts the binary with no flags (so it is its .doze name) in an
// empty directory and waits for it to stop, or for it to come up where this
// machine already resolves .doze, in which case it is stopped. It returns what
// was written to stderr and stdout, the exit code (-1 when it had to be
// stopped) and the directory it ran in.
func runByName(t *testing.T, trap sudoTrap, wire func(cmd *exec.Cmd, out *lockedBuf)) (output string, code int, dir string) {
	t.Helper()
	bin := builtBinary(t)
	dir = t.TempDir()
	cmd := exec.Command(bin)
	cmd.Dir, cmd.Env = dir, trap.env
	out := &lockedBuf{}
	cmd.Stdout, cmd.Stderr = out, out
	wire(cmd, out)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	deadline := time.After(30 * time.Second)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				return out.String(), ee.ExitCode(), dir
			}
			if err != nil {
				t.Fatal(err)
			}
			return out.String(), 0, dir
		case <-tick.C:
			if strings.Contains(out.String(), "doze-kafka is up") {
				_ = cmd.Process.Signal(syscall.SIGTERM)
				<-done
				return out.String(), -1, dir
			}
		case <-deadline:
			_ = cmd.Process.Kill()
			<-done
			t.Fatalf("the binary neither stopped nor came up in 30s (a hang at start-up is the failure this guards against):\n%s", out.String())
		}
	}
}

// Every stdin a supervisor hands a daemon. None of them is a person, so none of
// them may lead to sudo, and none may be read as "yes".
func TestNoSudoWithoutAPersonAtATerminal(t *testing.T) {
	zoneReady := names.Check().OK()
	cases := []struct {
		name string
		wire func(t *testing.T, cmd *exec.Cmd) *lockedBuf // returns what a pty showed, if it wired one
	}{
		{"stdin is /dev/null", func(t *testing.T, cmd *exec.Cmd) *lockedBuf {
			f, err := os.Open(os.DevNull)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { f.Close() })
			cmd.Stdin = f
			return nil
		}},
		{"stdin is closed", func(t *testing.T, cmd *exec.Cmd) *lockedBuf {
			// os/exec cannot start a child with descriptor 0 closed, so a shell does.
			cmd.Args = []string{"sh", "-c", `exec "$0" <&-`, cmd.Path}
			cmd.Path = "/bin/sh"
			return nil
		}},
		{"stdin is a pipe that ends", func(t *testing.T, cmd *exec.Cmd) *lockedBuf {
			cmd.Stdin = strings.NewReader("")
			return nil
		}},
		{"stdin is a pipe that says yes", func(t *testing.T, cmd *exec.Cmd) *lockedBuf {
			// `yes | doze-kafka` is not a person either.
			cmd.Stdin = strings.NewReader("y\n")
			return nil
		}},
		{"stdin is an empty file", func(t *testing.T, cmd *exec.Cmd) *lockedBuf {
			f, err := os.Create(filepath.Join(t.TempDir(), "empty"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { f.Close() })
			cmd.Stdin = f
			return nil
		}},
		{"stdin is a terminal and stderr is not", func(t *testing.T, cmd *exec.Cmd) *lockedBuf {
			// The question would go where nobody reads it.
			ptmx, tty, err := pty.Open()
			if err != nil {
				t.Skipf("no pty here: %v", err)
			}
			t.Cleanup(func() { tty.Close(); ptmx.Close() })
			cmd.Stdin = tty
			return nil
		}},
		{"stderr is a terminal and stdin is /dev/null", func(t *testing.T, cmd *exec.Cmd) *lockedBuf {
			// The case that ran sudo: /dev/null is a character device.
			ptmx, tty, err := pty.Open()
			if err != nil {
				t.Skipf("no pty here: %v", err)
			}
			t.Cleanup(func() { tty.Close(); ptmx.Close() })
			shown := &lockedBuf{}
			go drain(ptmx, shown)
			f, err := os.Open(os.DevNull)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { f.Close() })
			cmd.Stdin, cmd.Stderr = f, tty
			return shown
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trap := newSudoTrap(t)
			var shown *lockedBuf
			out, code, dir := runByName(t, trap, func(cmd *exec.Cmd, _ *lockedBuf) { shown = tc.wire(t, cmd) })
			if shown != nil {
				// The pty's last bytes arrive a moment after the process is gone.
				deadline := time.Now().Add(5 * time.Second)
				for !strings.Contains(shown.String(), "nothing has been changed") && !zoneReady && time.Now().Before(deadline) {
					time.Sleep(20 * time.Millisecond)
				}
				out += shown.String()
			}
			if trap.ran() {
				log, _ := os.ReadFile(trap.log)
				t.Fatalf("sudo was run with nobody at a terminal:\n%s\noutput:\n%s", log, out)
			}
			if strings.Contains(out, "[Y/n]") {
				t.Errorf("a question was asked with nobody to answer it:\n%s", out)
			}
			if zoneReady {
				return // this machine resolves .doze: it started, and that is all there is to see.
			}
			if code != exitRuntime {
				t.Errorf("exit code %d, want %d\n%s", code, exitRuntime, out)
			}
			if !strings.Contains(out, "no terminal to ask on, so nothing has been changed") {
				t.Errorf("it does not say why it stopped:\n%s", out)
			}
			if entries, _ := os.ReadDir(dir); len(entries) != 0 {
				t.Errorf("a start that was refused left %d entries in its directory (%s)", len(entries), entries[0].Name())
			}
		})
	}
}

// drain copies a pty's output until the pty closes.
func drain(ptmx *os.File, into *lockedBuf) {
	buf := make([]byte, 4096)
	for {
		n, err := ptmx.Read(buf)
		into.Write(buf[:n]) //nolint:errcheck
		if err != nil {
			return
		}
	}
}

// A person at a real terminal: the question is asked, Enter and "y" run the one
// sudo, "n" does not, and a terminal that ends its input (Ctrl-D, a hang-up)
// has not said yes.
func TestAtATerminalOnlyATypedAnswerRunsSudo(t *testing.T) {
	if names.Check().OK() {
		t.Skip("this machine already resolves .doze, so nothing is asked")
	}
	cases := []struct {
		name     string
		typed    string
		wantSudo bool
		wantText string
	}{
		{"Enter", "\n", true, "doze needs sudo once"},
		{"y", "y\n", true, "doze needs sudo once"},
		{"n", "n\n", false, "setup declined"},
		{"end of input", "\x04", false, "No answer was given, so nothing has been changed."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trap := newSudoTrap(t)
			ptmx, tty, err := pty.Open()
			if err != nil {
				t.Skipf("no pty here: %v", err)
			}
			defer ptmx.Close()
			seen := &lockedBuf{}
			go drain(ptmx, seen)
			typed := make(chan struct{})
			go func() {
				defer close(typed)
				deadline := time.Now().Add(20 * time.Second)
				for !strings.Contains(seen.String(), "[Y/n]") && time.Now().Before(deadline) {
					time.Sleep(20 * time.Millisecond)
				}
				ptmx.WriteString(tc.typed) //nolint:errcheck
			}()
			_, code, dir := runByName(t, trap, func(cmd *exec.Cmd, _ *lockedBuf) {
				cmd.Stdin, cmd.Stdout, cmd.Stderr = tty, tty, tty
			})
			tty.Close()
			<-typed
			// The pty's last bytes arrive a moment after the process is gone.
			deadline := time.Now().Add(5 * time.Second)
			for !strings.Contains(seen.String(), tc.wantText) && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			out := seen.String()
			if !strings.Contains(out, "Set it up now? [Y/n]") {
				t.Fatalf("at a terminal the question was not asked:\n%s", out)
			}
			if trap.ran() != tc.wantSudo {
				t.Errorf("sudo run = %v, want %v\n%s", trap.ran(), tc.wantSudo, out)
			}
			if !strings.Contains(out, tc.wantText) {
				t.Errorf("output does not say %q:\n%s", tc.wantText, out)
			}
			if code != exitRuntime {
				t.Errorf("exit code %d, want %d (the stand-in sudo fails, and so does a refusal)", code, exitRuntime)
			}
			if entries, _ := os.ReadDir(dir); len(entries) != 0 {
				t.Errorf("a start that did not happen left %s in its directory", entries[0].Name())
			}
		})
	}
}
