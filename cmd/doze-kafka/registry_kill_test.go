package main

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"
)

// runRegistry starts the real binary on data with the broker's credentials and returns
// the registry's address and a way to kill it with SIGKILL (no shutdown, no flush the
// process chose to do).
func runRegistry(t *testing.T, data string, extra ...string) (url string, out *lockedBuf, kill func()) {
	t.Helper()
	args := append([]string{"--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--schema-registry-addr", "127.0.0.1:0",
		"--data-dir", data, "--sasl-mechanisms", "PLAIN", "--sasl-user", "alice:secret"}, extra...)
	cmd := exec.Command(binary(t), args...)
	cmd.Dir = t.TempDir()
	stdout, stderr := &lockedBuf{}, &lockedBuf{}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() { cmd.Wait(); close(exited) }()
	kill = func() {
		cmd.Process.Signal(syscall.SIGKILL)
		<-exited
	}
	t.Cleanup(kill)
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(stdout.String(), "doze-kafka is up") {
		select {
		case <-exited:
			t.Fatalf("exited before its summary.\nstdout: %s\nstderr: %s", stdout.String(), stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no summary.\nstdout: %s\nstderr: %s", stdout.String(), stderr.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
	if m := regexp.MustCompile(`registry\s+(http://\S+)`).FindStringSubmatch(stdout.String()); m != nil {
		url = m[1]
	}
	return url, stdout, kill
}

const (
	killV1 = `{"schema":"{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}"}`
	killV2 = `{"schema":"{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":\"int\",\"default\":0}]}"}`
	killV3 = `{"schema":"{\"type\":\"record\",\"name\":\"Gone\",\"fields\":[{\"name\":\"z\",\"type\":\"long\"}]}"}`
	killV4 = `{"schema":"{\"type\":\"record\",\"name\":\"Fresh\",\"fields\":[{\"name\":\"y\",\"type\":\"double\"}]}"}`
)

// A registry killed with SIGKILL comes back with everything it had answered for: ids,
// versions, levels, modes, when a version was registered and by whom, and the highest
// id stays spent after a permanent delete (Stage 5 acceptance: the SIGKILL variant).
func TestTheRegistrySurvivesASIGKILL(t *testing.T) {
	if testing.Short() {
		t.Skip("links the binary and kills it")
	}
	data := filepath.Join(t.TempDir(), "data dir") // a path with a space in it
	url, _, kill := runRegistry(t, data)
	must := func(method, path, body string, want int) string {
		t.Helper()
		code, got := call(t, method, url+path, body, "alice", "secret")
		if code != want {
			t.Fatalf("%s %s = %d %s, want %d", method, path, code, got, want)
		}
		return got
	}
	must("POST", "/subjects/orders-value/versions", killV1, 200)
	must("POST", "/subjects/orders-value/versions", killV2, 200)
	must("PUT", "/config/orders-value", `{"compatibility":"FULL"}`, 200)
	must("PUT", "/config", `{"compatibility":"NONE"}`, 200)
	must("POST", "/subjects/frozen-value/versions", killV1, 200)
	must("PUT", "/mode/frozen-value", `{"mode":"READONLY"}`, 200)
	if got := must("POST", "/subjects/gone-value/versions", killV3, 200); got != `{"id":3}` {
		t.Fatalf("third schema = %s, want id 3", got)
	}
	must("DELETE", "/subjects/gone-value", "", 200)
	must("DELETE", "/subjects/gone-value?permanent=true", "", 200)
	before := must("GET", "/_doze/subjects/orders-value/versions/2", "", 200)
	kill()

	url, _, _ = runRegistry(t, data)
	if got := must("GET", "/subjects", "", 200); got != `["frozen-value","orders-value"]` {
		t.Errorf("subjects after SIGKILL = %s", got)
	}
	if got := must("GET", "/subjects/orders-value/versions", "", 200); got != "[1,2]" {
		t.Errorf("versions after SIGKILL = %s, want [1,2]", got)
	}
	if got := must("GET", "/config/orders-value", "", 200); !strings.Contains(got, `"FULL"`) {
		t.Errorf("subject level after SIGKILL = %s, want FULL", got)
	}
	if got := must("GET", "/config", "", 200); !strings.Contains(got, `"NONE"`) {
		t.Errorf("global level after SIGKILL = %s, want NONE", got)
	}
	if got := must("GET", "/mode/frozen-value", "", 200); !strings.Contains(got, "READONLY") {
		t.Errorf("subject mode after SIGKILL = %s, want READONLY", got)
	}
	after := must("GET", "/_doze/subjects/orders-value/versions/2", "", 200)
	var v struct {
		ID           int        `json:"id"`
		RegisteredAt *time.Time `json:"registeredAt"`
		RegisteredBy string     `json:"registeredBy"`
	}
	if err := json.Unmarshal([]byte(after), &v); err != nil {
		t.Fatal(err)
	}
	if after != before || v.ID != 2 || v.RegisteredAt == nil || v.RegisteredBy != "alice" {
		t.Errorf("version 2 after SIGKILL says %s, before it said %s (want id 2, a time, by alice)", after, before)
	}
	if got := must("POST", "/subjects/fresh-value/versions", killV4, 200); got != `{"id":4}` {
		t.Errorf("a new schema after SIGKILL = %s; id 3 was spent by the permanent delete (want 4)", got)
	}
}

// --schema-registry=false serves no registry and leaves what it held alone: the
// Schemas page's "Nothing was lost: schemas stay in meta.db" is true.
func TestTheRegistryBucketIsIntactAfterAStartWithTheRegistryOff(t *testing.T) {
	if testing.Short() {
		t.Skip("links the binary and boots it three times")
	}
	data := filepath.Join(t.TempDir(), "data")
	url, _, kill := runRegistry(t, data)
	if code, got := call(t, "POST", url+"/subjects/orders-value/versions", killV1, "alice", "secret"); code != 200 {
		t.Fatalf("register = %d %s", code, got)
	}
	if code, got := call(t, "PUT", url+"/config/orders-value", `{"compatibility":"FULL"}`, "alice", "secret"); code != 200 {
		t.Fatalf("set level = %d %s", code, got)
	}
	_, before := call(t, "GET", url+"/_doze/overview", "", "alice", "secret")
	kill()

	off, out, kill := runRegistry(t, data, "--schema-registry=false")
	if off != "" {
		t.Fatalf("--schema-registry=false but the summary names a registry:\n%s", out.String())
	}
	kill()

	url, _, _ = runRegistry(t, data)
	code, after := call(t, "GET", url+"/_doze/overview", "", "alice", "secret")
	if code != 200 || after != before {
		t.Errorf("after a start with the registry off the overview is %d %s\nbefore it was %s", code, after, before)
	}
}
