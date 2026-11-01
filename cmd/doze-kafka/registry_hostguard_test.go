package main

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// callAs is call with the Host header a client, or a page whose name was rebound
// to this machine, would send to the registry's port.
func callAs(t *testing.T, method, rawURL, host, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, rawURL, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	if body != "" {
		req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s as %s: %v", method, rawURL, host, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

func portOf(t *testing.T, rawURL string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil || u.Port() == "" {
		t.Fatalf("the registry's URL %q has no port", rawURL)
	}
	return u.Port()
}

const hostguardSchema = `{"schema":"{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}"}`

// The binary's own registry listener, started as `doze-kafka` starts it: a request
// whose Host is not one of the registry's names is refused with 403 and does
// nothing; the address in the summary, localhost and a .doze name are served.
func TestTheRegistryListenerRefusesARebindingHost(t *testing.T) {
	out, _ := start(t)
	reg := summaryRegistry(t, out)
	port := portOf(t, reg)

	if code, body := callAs(t, "POST", reg+"/subjects/orders-value/versions", "127.0.0.1:"+port, hostguardSchema); code != 200 || body != `{"id":1}` {
		t.Fatalf("registering as 127.0.0.1 = %d %s, want 200 {\"id\":1}", code, body)
	}
	foreign := "attacker.example:" + port
	for _, c := range []struct{ method, path, body string }{
		{"GET", "/subjects", ""},
		{"GET", "/schemas/ids/1", ""},
		{"POST", "/subjects/pwned-value/versions", hostguardSchema},
		{"DELETE", "/subjects/orders-value", ""},
	} {
		code, body := callAs(t, c.method, reg+c.path, foreign, c.body)
		if code != http.StatusForbidden || !strings.HasPrefix(body, `{"error_code":403,"message":"`) {
			t.Errorf("%s %s with Host %s = %d %s, want 403 in the registry's error shape", c.method, c.path, foreign, code, body)
		}
	}
	if code, body := callAs(t, "GET", reg+"/subjects", "localhost:"+port, ""); code != 200 || body != `["orders-value"]` {
		t.Errorf("GET /subjects as localhost after the refused requests = %d %s, want exactly [\"orders-value\"]", code, body)
	}
	for _, host := range []string{"127.0.0.1:" + port, "[::1]:" + port, "schema.shop.doze", "schema.shop.doze:" + port} {
		if code, body := callAs(t, "GET", reg+"/subjects", host, ""); code != 200 {
			t.Errorf("GET /subjects with Host %q = %d %s, want 200", host, code, body)
		}
	}
	// A name nobody gave this instance, a container's included.
	for _, host := range []string{"host.docker.internal:" + port, "schema.shop.doze.evil.example"} {
		if code, _ := callAs(t, "GET", reg+"/subjects", host, ""); code != http.StatusForbidden {
			t.Errorf("GET /subjects with Host %q = %d, want 403", host, code)
		}
	}
}

// The compatibility matrix's containers: the broker is started with
// --advertised-host host.docker.internal and the Java and Spring cells are given
// REGISTRY_URL=http://host.docker.internal:PORT, so that is the Host they send.
// --console-allow-host names any other, for the registry as for the console.
func TestTheRegistryAnswersToTheAdvertisedHostAndTheAllowedHosts(t *testing.T) {
	out, _ := start(t, "--advertised-host", "host.docker.internal", "--console=false", "--console-allow-host", "registry.dev.example")
	reg := summaryRegistry(t, out)
	port := portOf(t, reg)

	if code, body := callAs(t, "POST", reg+"/subjects/orders-value/versions", "host.docker.internal:"+port, hostguardSchema); code != 200 || body != `{"id":1}` {
		t.Errorf("registering as host.docker.internal = %d %s, want 200 {\"id\":1}", code, body)
	}
	for _, host := range []string{"host.docker.internal:" + port, "registry.dev.example:" + port, "127.0.0.1:" + port} {
		if code, body := callAs(t, "GET", reg+"/subjects", host, ""); code != 200 || body != `["orders-value"]` {
			t.Errorf("GET /subjects with Host %q = %d %s, want 200 [\"orders-value\"]", host, code, body)
		}
	}
	for _, host := range []string{"attacker.example:" + port, "host.docker.internal.evil.example:" + port} {
		if code, _ := callAs(t, "GET", reg+"/subjects", host, ""); code != http.StatusForbidden {
			t.Errorf("GET /subjects with Host %q = %d, want 403", host, code)
		}
	}
}

// With SASL on, a foreign Host is refused before the credentials are asked for.
func TestTheRegistryListenerJudgesTheHostBeforeTheCredentials(t *testing.T) {
	out, _ := start(t, "--sasl-mechanisms", "PLAIN", "--sasl-user", "alice:secret")
	reg := summaryRegistry(t, out)
	port := portOf(t, reg)
	if code, _ := callAs(t, "GET", reg+"/subjects", "attacker.example:"+port, ""); code != http.StatusForbidden {
		t.Errorf("a foreign Host with no credentials = %d, want 403 (not the 401 that asks for a password)", code)
	}
	if code, _ := call(t, "GET", reg+"/subjects", "", "", ""); code != http.StatusUnauthorized {
		t.Errorf("the registry's own address with no credentials = %d, want 401", code)
	}
}
