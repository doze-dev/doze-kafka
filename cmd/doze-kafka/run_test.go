package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// safeBuf is a bytes.Buffer safe for a goroutine to write while a test reads.
type safeBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *safeBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *safeBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// start runs the broker with the given flags (on an address, so no name; free ports) until the test
// ends, and returns its output once the summary has been printed.
func start(t *testing.T, flags ...string) (out *safeBuf, stop func() error) {
	t.Helper()
	t.Chdir(t.TempDir())
	args := append([]string{"--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--data-dir", t.TempDir()}, flags...)
	st, err := loadConfig(args)
	if err != nil {
		t.Fatal(err)
	}
	out = &safeBuf{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, st, newLogger(st.cfg, out), out) }()
	var once sync.Once
	var result error
	stop = func() error {
		once.Do(func() { cancel(); result = <-done })
		return result
	}
	t.Cleanup(func() { stop() })
	return out, stop
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// The summary is the block a developer reads to learn how to connect, and it is
// printed only once everything it names is bound.
func TestStartupSummaryNamesWhatIsListening(t *testing.T) {
	out, stop := start(t)
	waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "doze-kafka is up") })
	text := out.String()
	for _, want := range []string{"doze-kafka is up", "kafka     127.0.0.1:", "console   http://127.0.0.1:", "data      ", "Kafka 4 protocol · no auth · plaintext", `eval "$(doze-kafka env)"`, "doze-kafka doctor"} {
		if !strings.Contains(text, want) {
			t.Errorf("the summary is missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, ".doze") {
		t.Errorf("under --listen no name is claimed, but the summary names one:\n%s", text)
	}

	// The console answers where the summary says, /healthz included.
	m := regexp.MustCompile(`http://127\.0\.0\.1:\d+`).FindString(text)
	for path, want := range map[string]string{"/healthz": "ok", "/_console/topics": "Topics"} {
		resp, err := http.Get(m + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(string(body), want) {
			t.Errorf("GET %s = %d %.80q, want 200 containing %q", path, resp.StatusCode, body, want)
		}
	}
	if err := stop(); err != nil {
		t.Errorf("a clean shutdown returned %v", err)
	}
}

func TestStartupSummaryAsJSONAndQuiet(t *testing.T) {
	out, _ := start(t, "--log-format", "json")
	waitFor(t, "the ready record", func() bool { return strings.Contains(out.String(), `"msg":"ready"`) })
	if strings.Contains(out.String(), "emulating Kafka") {
		t.Error("JSON mode should not print the text block")
	}

	quiet, stop := start(t, "--log-level", "warn")
	time.Sleep(300 * time.Millisecond)
	stop()
	if strings.Contains(quiet.String(), "ready") {
		t.Errorf("--log-level warn should print no summary:\n%s", quiet.String())
	}
}

// A taken port is an error, not a "listening" line that was a lie, and the
// failed start releases the data directory so the next one can use it.
func TestPortClashesAreErrorsAndReleaseTheDataDir(t *testing.T) {
	t.Chdir(t.TempDir())
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	dir := t.TempDir()

	tryRun := func(listen, console string) error {
		st, err := loadConfig([]string{"--listen", listen, "--console-addr", console, "--data-dir", dir})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return run(ctx, st, newLogger(st.cfg, io.Discard), io.Discard)
	}

	err = tryRun("127.0.0.1:0", taken.Addr().String())
	if err == nil || !strings.Contains(err.Error(), "console: listen on") || !strings.Contains(err.Error(), "--console-addr") {
		t.Errorf("console port clash: %v", err)
	}
	err = tryRun(taken.Addr().String(), "127.0.0.1:0")
	if err == nil || !strings.Contains(err.Error(), "listen on") || !strings.Contains(err.Error(), "--listen") {
		t.Errorf("broker port clash: %v", err)
	}
	// Both failures released the metadata lock: this one starts (and is stopped by its timeout).
	if err := tryRun("127.0.0.1:0", "127.0.0.1:0"); err != nil {
		t.Errorf("starting after a failed start: %v", err)
	}
}
