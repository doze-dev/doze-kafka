package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// The summary a person reads goes to stdout and the logs to stderr, so
// `doze-kafka 2>/dev/null` shows just the summary and a wrapper parsing the log lines
// never meets it. That split is the real binary's behaviour, so it is tested against
// the real binary: run() is handed one writer in the other tests.
func TestTheSummaryIsOnStdoutAndTheLogsOnStderr(t *testing.T) {
	if testing.Short() {
		t.Skip("links the binary and boots it")
	}
	bin := binary(t)

	dir := t.TempDir()
	cmd := exec.Command(bin, "--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--schema-registry-addr", "127.0.0.1:0", "--data-dir", filepath.Join(dir, "data"))
	cmd.Dir = dir
	var stdout, stderr lockedBuf
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })

	deadline := time.Now().Add(20 * time.Second)
	for !strings.Contains(stdout.String(), "doze-kafka is up") {
		if time.Now().After(deadline) {
			t.Fatalf("no summary on stdout.\nstdout: %s\nstderr: %s", stdout.String(), stderr.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
	if strings.Contains(stdout.String(), "level=") {
		t.Errorf("log lines on stdout:\n%s", stdout.String())
	}
	if strings.Contains(stderr.String(), "doze-kafka is up") {
		t.Errorf("the summary is on stderr:\n%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "level=INFO") {
		t.Errorf("the logs are not on stderr:\n%s", stderr.String())
	}
}

var (
	binOnce sync.Once
	binPath string
	binErr  error
	binDir  string
)

// binary is this package built once per test run, for the tests that have to run
// the real process (its streams, its exit code, two of them at once).
func binary(t *testing.T) string {
	t.Helper()
	binOnce.Do(func() {
		binDir, binErr = os.MkdirTemp("", "doze-kafka-bin-")
		if binErr != nil {
			return
		}
		binPath = filepath.Join(binDir, "doze-kafka")
		if runtime.GOOS == "windows" {
			binPath += ".exe"
		}
		if out, err := exec.Command("go", "build", "-o", binPath, ".").CombinedOutput(); err != nil {
			binErr = fmt.Errorf("build: %v\n%s", err, out)
		}
	})
	if binErr != nil {
		t.Fatal(binErr)
	}
	return binPath
}

// startBinary runs the binary with args in dir and waits for its summary.
func startBinary(t *testing.T, dir string, args ...string) (stdout, stderr *lockedBuf) {
	t.Helper()
	cmd := exec.Command(binary(t), args...)
	cmd.Dir = dir
	stdout, stderr = &lockedBuf{}, &lockedBuf{}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() { cmd.Wait(); close(exited) }()
	t.Cleanup(func() { cmd.Process.Kill(); <-exited })
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(stdout.String(), "doze-kafka is up") {
		select {
		case <-exited:
			t.Fatalf("%v exited before its summary.\nstdout: %s\nstderr: %s", args, stdout.String(), stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no summary.\nstdout: %s\nstderr: %s", stdout.String(), stderr.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
	return stdout, stderr
}

// docs/configuration.md's throwaway broker, run twice at once: a broker on a port
// the system chose takes chosen ports for the console and the registry too, so a
// second copy does not collide with the first on 8081.
func TestTwoThrowawayBrokersRunAtOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("links the binary and boots it")
	}
	var summaries []string
	for i := 0; i < 2; i++ {
		dir := t.TempDir()
		out, _ := startBinary(t, dir, "--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--data-dir", filepath.Join(dir, "data"))
		summaries = append(summaries, out.String())
	}
	reg := regexp.MustCompile(`registry\s+(http://\S+)`)
	a, b := reg.FindStringSubmatch(summaries[0]), reg.FindStringSubmatch(summaries[1])
	if a == nil || b == nil || a[1] == b[1] {
		t.Fatalf("want two registries on different ports:\n%s\n%s", summaries[0], summaries[1])
	}
	for _, m := range [][]string{a, b} {
		if strings.HasSuffix(m[1], ":8081") {
			t.Errorf("registry on the fixed port under --listen :0: %s", m[1])
		}
	}
}

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }
