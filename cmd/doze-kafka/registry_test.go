package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
)

// registryURL is the address the startup summary gives for the registry.
func summaryRegistry(t *testing.T, out *safeBuf) string {
	t.Helper()
	waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "doze-kafka is up") })
	m := regexp.MustCompile(`registry  (http://\S+)`).FindStringSubmatch(out.String())
	if m == nil {
		t.Fatalf("the summary names no registry:\n%s", out.String())
	}
	return m[1]
}

func call(t *testing.T, method, url, body, user, pass string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

// The registry is part of the binary: the summary says where it is, it speaks
// Confluent's API, and what is registered is still there after a restart (it lives
// in the metadata database, not in memory).
func TestSchemaRegistryIsServedAndSurvivesARestart(t *testing.T) {
	data := t.TempDir()
	out, stop := start(t, "--data-dir", data)
	url := summaryRegistry(t, out)

	if code, body := call(t, "GET", url+"/subjects", "", "", ""); code != 200 || body != "[]" {
		t.Fatalf("GET /subjects = %d %s, want 200 []", code, body)
	}
	schema := `{"schema":"{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}"}`
	code, body := call(t, "POST", url+"/subjects/orders-value/versions", schema, "", "")
	if code != 200 || body != `{"id":1}` {
		t.Fatalf("register = %d %s, want 200 {\"id\":1}", code, body)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	out2, _ := start(t, "--data-dir", data)
	url2 := summaryRegistry(t, out2)
	if code, body := call(t, "GET", url2+"/subjects", "", "", ""); code != 200 || body != `["orders-value"]` {
		t.Errorf("after a restart GET /subjects = %d %s, want the subject back", code, body)
	}
	if code, body := call(t, "GET", url2+"/schemas/ids/1", "", "", ""); code != 200 || !strings.Contains(body, `"schema"`) {
		t.Errorf("after a restart GET /schemas/ids/1 = %d %s", code, body)
	}
	// The same schema is the same id; a new one is the next.
	if code, body := call(t, "POST", url2+"/subjects/other/versions", schema, "", ""); code != 200 || body != `{"id":1}` {
		t.Errorf("registering the same schema again = %d %s, want id 1", code, body)
	}
}

func TestSchemaRegistryCanBeTurnedOff(t *testing.T) {
	out, _ := start(t, "--schema-registry=false")
	waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "doze-kafka is up") })
	if strings.Contains(out.String(), "registry") {
		t.Errorf("--schema-registry=false but the summary names a registry:\n%s", out.String())
	}
}

// With SASL on, the registry takes the same user names and passwords as HTTP basic
// authentication; without them it refuses.
func TestSchemaRegistryUsesTheBrokersCredentials(t *testing.T) {
	out, _ := start(t, "--sasl-mechanisms", "PLAIN", "--sasl-user", "alice:secret")
	url := summaryRegistry(t, out)
	if code, _ := call(t, "GET", url+"/subjects", "", "", ""); code != 401 {
		t.Errorf("no credentials = %d, want 401", code)
	}
	if code, _ := call(t, "GET", url+"/subjects", "", "alice", "wrong"); code != 401 {
		t.Errorf("wrong password = %d, want 401", code)
	}
	if code, body := call(t, "GET", url+"/subjects", "", "alice", "secret"); code != 200 || body != "[]" {
		t.Errorf("right credentials = %d %s, want 200 []", code, body)
	}
}

func TestSchemaRegistryPortClashIsAnError(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	st, err := loadConfig([]string{"--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--schema-registry-addr", taken.Addr().String(), "--data-dir", t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = run(ctx, st, newLogger(st.cfg, io.Discard), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "schema registry: listen on") || !strings.Contains(err.Error(), "--schema-registry-addr") {
		t.Errorf("a taken registry port: %v, want an error naming the flag", err)
	}
}

// A listener on every interface is not a place to browse to ("http://[::]:8081"); the
// summary names localhost for it, and leaves a specific address as it is.
func TestHTTPURLNamesSomewhereYouCanBrowseTo(t *testing.T) {
	for in, want := range map[string]string{
		"[::]:8081":       "http://localhost:8081",
		"0.0.0.0:8081":    "http://localhost:8081",
		":8081":           "http://localhost:8081",
		"127.0.0.1:8081":  "http://127.0.0.1:8081",
		"127.0.0.3:9093":  "http://127.0.0.3:9093",
		"devbox.lan:8081": "http://devbox.lan:8081",
	} {
		if got := httpURL(in); got != want {
			t.Errorf("httpURL(%q) = %q, want %q", in, got, want)
		}
	}
}

// `doze-kafka env` hands the shell the registry's address too, so a client reads it
// from the same place as the bootstrap servers; with the registry off it says nothing.
func TestEnvExportsTheRegistryAddress(t *testing.T) {
	st, err := loadConfig([]string{"--listen", "127.0.0.1:9092", "--schema-registry-addr", "127.0.0.1:18081"})
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	envTo(&b, st.cfg)
	if !strings.Contains(b.String(), "export SCHEMA_REGISTRY_URL=http://127.0.0.1:18081\n") {
		t.Errorf("env does not export the registry:\n%s", b.String())
	}
	st, err = loadConfig([]string{"--listen", "127.0.0.1:9092", "--schema-registry=false"})
	if err != nil {
		t.Fatal(err)
	}
	b.Reset()
	envTo(&b, st.cfg)
	if strings.Contains(b.String(), "SCHEMA_REGISTRY") {
		t.Errorf("the registry is off but env exports it:\n%s", b.String())
	}
}
