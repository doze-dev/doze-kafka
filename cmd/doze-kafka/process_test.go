package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
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
	bin := filepath.Join(t.TempDir(), "doze-kafka")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	dir := t.TempDir()
	cmd := exec.Command(bin, "--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--data-dir", filepath.Join(dir, "data"))
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
	_ = os.Remove(bin)
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
