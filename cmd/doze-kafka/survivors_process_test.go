//go:build unix

package main

import (
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"
)

// runBinary runs the command line as main does, in this process (so that the code it runs
// is the code these tests are measured on), and returns what it printed and the code it
// ended with. It ends the run with SIGTERM, which realMain stops on, once stop returns true
// for what has been printed; with a nil stop it waits for the run to end by itself. dir is
// the working directory, and env is KEY=value pairs set for the run.
func runBinary(t *testing.T, dir string, env []string, stop func(stdout, stderr string) bool, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	t.Chdir(dir)
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		t.Setenv(k, v)
	}
	var out, errs lockedBuf
	exited := make(chan int, 1)
	go func() { exited <- realMain(args, &out, &errs) }()
	deadline := time.After(30 * time.Second)
	for {
		select {
		case code := <-exited:
			return out.String(), errs.String(), code
		case <-deadline:
			syscall.Kill(os.Getpid(), syscall.SIGTERM)
			t.Fatalf("did not end.\nstdout: %s\nstderr: %s", out.String(), errs.String())
		case <-time.After(25 * time.Millisecond):
			if stop != nil && stop(out.String(), errs.String()) {
				stop = nil
				syscall.Kill(os.Getpid(), syscall.SIGTERM)
			}
		}
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func upSummary(stdout, _ string) bool { return strings.Contains(stdout, "doze-kafka is up") }

// A broker told to stop ends with 0 and no complaint; one that cannot run says why and ends
// with 1; one given no address and no names says so and ends with 2, as it does when given
// no arguments at all (not by running off the end of them).
func TestHowTheProcessEnds(t *testing.T) {
	if testing.Short() {
		t.Skip("links the binary and runs it")
	}
	dir := t.TempDir()
	addr := freePort(t)

	_, stderr, code := runBinary(t, dir, nil, upSummary, "--listen", addr, "--console=false", "--schema-registry=false", "--data-dir", filepath.Join(dir, "data"))
	if code != 0 || strings.Contains(stderr, "fatal") || !strings.Contains(stderr, "shutting down") {
		t.Errorf("a broker stopped with SIGTERM: exit %d\n%s", code, stderr)
	}

	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	_, stderr, code = runBinary(t, dir, nil, nil, "--listen", taken.Addr().String(), "--console=false", "--schema-registry=false", "--data-dir", filepath.Join(dir, "data2"))
	if code != exitRuntime || !strings.Contains(stderr, "fatal") {
		t.Errorf("a broker that cannot bind: exit %d\n%s", code, stderr)
	}

	_, stderr, code = runBinary(t, t.TempDir(), []string{"DOZE_ZONE=off"}, nil)
	if code != exitUsage || strings.Contains(stderr, "panic") || strings.Contains(stderr, "goroutine") || !strings.Contains(stderr, "nothing to listen on") {
		t.Errorf("no arguments and no names: exit %d\n%s", code, stderr)
	}
}

// The config file that was read is said, and pprof listens only when asked.
func TestTheLogSaysWhichConfigFileWasReadAndPprofListensWhenAsked(t *testing.T) {
	if testing.Short() {
		t.Skip("links the binary and runs it")
	}
	dir := t.TempDir()
	cfg := filepath.Join(dir, "my.toml")
	if err := os.WriteFile(cfg, []byte("console = false\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var base string
	_, stderr, code := runBinary(t, dir, nil, func(stdout, stderr string) bool {
		if !upSummary(stdout, stderr) {
			return false
		}
		if i := strings.Index(stderr, `pprof listening" addr=`); i >= 0 {
			base = strings.Fields(stderr[i+len(`pprof listening" addr=`):])[0]
		}
		if base == "" {
			return false
		}
		resp, err := http.Get("http://" + base + "/debug/pprof/")
		if err != nil {
			return false
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("/debug/pprof/ answered %d", resp.StatusCode)
		}
		resp, err = http.Get("http://" + base + "/debug/pprof/cmdline")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != 404 {
				t.Errorf("/debug/pprof/cmdline answered %d, want 404", resp.StatusCode)
			}
		}
		return true
	}, "--listen", "127.0.0.1:0", "--schema-registry=false", "--config", cfg, "--pprof-addr", "127.0.0.1:0", "--data-dir", filepath.Join(dir, "data"))
	if code != 0 || !strings.Contains(stderr, "loaded config file") || !strings.Contains(stderr, cfg) {
		t.Errorf("exit %d; the config file is not named:\n%s", code, stderr)
	}
	if base == "" {
		t.Errorf("pprof did not say where it listens:\n%s", stderr)
	}

	_, stderr, code = runBinary(t, dir, nil, upSummary, "--listen", "127.0.0.1:0", "--schema-registry=false", "--console=false", "--config", "", "--data-dir", filepath.Join(dir, "data"))
	if code != 0 || strings.Contains(stderr, "loaded config file") || strings.Contains(stderr, "pprof") {
		t.Errorf("with no file and no pprof: exit %d\n%s", code, stderr)
	}
}

// A client that sends half a request and stops is cut off after ten seconds, on every
// HTTP port the broker opens: the console, the schema registry and pprof.
func TestAStalledHTTPRequestIsCutOffAfterTenSeconds(t *testing.T) {
	if testing.Short() {
		t.Skip("links the binary and runs it")
	}
	dir := t.TempDir()
	var addrs map[string]string
	_, stderr, code := runBinary(t, dir, nil, func(stdout, stderr string) bool {
		if !upSummary(stdout, stderr) {
			return false
		}
		addrs = map[string]string{}
		for name, re := range map[string]*regexp.Regexp{
			"console":  regexp.MustCompile(`console\s+http://(\S+)`),
			"registry": regexp.MustCompile(`registry\s+http://(\S+)`),
			"pprof":    regexp.MustCompile(`pprof listening" addr=(\S+)`),
		} {
			text := stdout
			if name == "pprof" {
				text = stderr
			}
			if m := re.FindStringSubmatch(text); m != nil {
				addrs[name] = m[1]
			}
		}
		results := make(chan string, len(addrs))
		for name, addr := range addrs {
			go func() {
				c, err := net.Dial("tcp", addr)
				if err != nil {
					results <- name + ": " + err.Error()
					return
				}
				defer c.Close()
				start := time.Now()
				c.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n"))
				c.SetReadDeadline(time.Now().Add(16 * time.Second))
				io.Copy(io.Discard, c)
				if took := time.Since(start); took < 8*time.Second || took > 14*time.Second {
					results <- name + ": cut off after " + took.String()
					return
				}
				results <- ""
			}()
		}
		for range addrs {
			if r := <-results; r != "" {
				t.Error(r)
			}
		}
		return true
	}, "--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--schema-registry-addr", "127.0.0.1:0", "--pprof-addr", "127.0.0.1:0", "--data-dir", filepath.Join(dir, "data"))
	if len(addrs) != 3 {
		t.Fatalf("the addresses found: %v (exit %d)\n%s", addrs, code, stderr)
	}
	for _, bad := range []string{"console server stopped", "schema registry stopped", "pprof server stopped"} {
		if strings.Contains(stderr, bad) {
			t.Errorf("a clean stop logged %q:\n%s", bad, stderr)
		}
	}
}
