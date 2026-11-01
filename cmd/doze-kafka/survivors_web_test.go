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
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// The console is told that the registry beside it is the broker's own, which keeps its
// schemas in meta.db, and says so on the schemas page.
func TestTheConsoleSaysTheBuiltInRegistryKeepsItsSchemasInMetaDB(t *testing.T) {
	if testing.Short() {
		t.Skip("links the binary and runs it")
	}
	dir := t.TempDir()
	var page string
	runBinary(t, dir, nil, func(stdout, _ string) bool {
		m := regexp.MustCompile(`console\s+(http://\S+)`).FindStringSubmatch(stdout)
		if m == nil {
			return false
		}
		// The line is on the page once there is a subject to list.
		reg := regexp.MustCompile(`registry\s+(http://\S+)`).FindStringSubmatch(stdout)
		if reg == nil {
			return false
		}
		post, err := http.Post(reg[1]+"/subjects/orders-value/versions", "application/vnd.schemaregistry.v1+json", strings.NewReader(`{"schema": "\"string\""}`))
		if err != nil {
			return false
		}
		post.Body.Close()
		resp, err := http.Get(strings.TrimRight(m[1], "/") + config.DefaultConsolePrefix + "/schemas")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		page = string(b)
		return true
	}, "--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--schema-registry-addr", "127.0.0.1:0", "--data-dir", filepath.Join(dir, "data"))
	if !strings.Contains(page, "stored in meta.db, not a _schemas topic") {
		t.Errorf("the schemas page does not say where the built-in registry keeps its schemas:\n%.2000s", page)
	}
}

// pprof on an address that is taken is said and does not stop the broker.
func TestAPprofAddressThatIsTakenDoesNotStopTheBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("links the binary and runs it")
	}
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	dir := t.TempDir()
	_, stderr, code := runBinary(t, dir, nil, upSummary, "--listen", "127.0.0.1:0", "--console=false", "--schema-registry=false", "--pprof-addr", taken.Addr().String(), "--data-dir", filepath.Join(dir, "data"))
	if code != 0 || !strings.Contains(stderr, "pprof server stopped") {
		t.Errorf("exit %d\n%s", code, stderr)
	}
}

// Whether the process is root is what the operating system says it is.
func TestTheLiveNameEnvironmentAsksTheOperatingSystemWhoItIs(t *testing.T) {
	e := liveNameEnv()
	if got, want := e.isRoot(), os.Geteuid() == 0; got != want {
		t.Errorf("isRoot = %v, want %v", got, want)
	}
	if e.out != os.Stderr || e.in != os.Stdin {
		t.Error("the prompt is not on stdin and stderr")
	}
}
