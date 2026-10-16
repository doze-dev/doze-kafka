package schemaregistry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

// testHosts are the Host headers the tests that drive the API in process send
// without meaning anything by it: httptest.NewRequest's "example.com" and the fuzz
// target's "registry". A test of the guard itself builds its API with Options{}.
var testHosts = []string{"example.com", "registry"}

// asHost sends one request to the API with the given Host header.
func asHost(a *API, method, path, host, body string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Host = host
	if body != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	a.ServeHTTP(rec, req)
	return rec
}

func subjectsSeenLocally(t *testing.T, a *API) []string {
	t.Helper()
	rec := asHost(a, "GET", "/subjects", "127.0.0.1:8081", "")
	if rec.Code != 200 {
		t.Fatalf("GET /subjects on loopback = %d %s", rec.Code, rec.Body)
	}
	var out []string
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// A page on another origin reaches a loopback listener by DNS rebinding: its name
// resolves to 127.0.0.1 and the browser sends that name as Host. The registry is
// unauthenticated when SASL is off, so the Host header is the only thing that tells
// such a request from the developer's own. It is refused, reads and writes alike,
// in the registry's own error shape, and nothing it asked for happens.
func TestARequestForAForeignHostIsRefusedAndChangesNothing(t *testing.T) {
	r, _ := openReg(t)
	a := NewAPI(r, Options{})
	if rec := asHost(a, "POST", "/subjects/orders/versions", "127.0.0.1:8081", schemaBody(avroUser1)); rec.Code != 200 {
		t.Fatalf("registering on loopback = %d %s", rec.Code, rec.Body)
	}

	const foreign = "attacker.example:8081"
	for _, c := range []struct{ method, path, body string }{
		{"GET", "/", ""},
		{"GET", "/subjects", ""},
		{"GET", "/schemas/ids/1", ""},
		{"GET", "/subjects/orders/versions/1", ""},
		{"GET", "/config", ""},
		{"GET", "/no/such/route", ""},
		{"POST", "/subjects/pwned/versions", schemaBody(avroUser1)},
		{"POST", "/compatibility/subjects/orders/versions/latest", schemaBody(avroUser1)},
		{"PUT", "/config", `{"compatibility":"NONE"}`},
		{"PUT", "/mode", `{"mode":"READONLY"}`},
		{"DELETE", "/subjects/orders", ""},
		{"DELETE", "/subjects/orders/versions/1", ""},
		{"OPTIONS", "/subjects", ""},
	} {
		rec := asHost(a, c.method, c.path, foreign, c.body, "Origin", "http://"+foreign)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s %s with Host %s = %d %s, want 403", c.method, c.path, foreign, rec.Code, rec.Body)
			continue
		}
		if ct := rec.Header().Get("Content-Type"); ct != contentType {
			t.Errorf("%s %s: the refusal's Content-Type = %q, want the registry's", c.method, c.path, ct)
		}
		var e struct {
			Code    *int   `json:"error_code"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Code == nil || *e.Code != 403 {
			t.Errorf("%s %s: the refusal is %s, want {\"error_code\":403,\"message\":...}", c.method, c.path, rec.Body)
		}
		if !strings.Contains(e.Message, `"attacker.example:8081"`) || !strings.Contains(e.Message, "--console-allow-host=attacker.example") {
			t.Errorf("%s %s: the refusal should name the host and say how to allow it, got %q", c.method, c.path, e.Message)
		}
		if strings.Contains(rec.Body.String(), "User") || strings.Contains(rec.Body.String(), "orders") {
			t.Errorf("%s %s: the refusal carries registry content: %s", c.method, c.path, rec.Body)
		}
	}

	// Nothing was registered, nothing was deleted, nothing was reconfigured.
	if got := subjectsSeenLocally(t, a); !slices.Equal(got, []string{"orders"}) {
		t.Errorf("after the refused requests the subjects are %v, want [orders]", got)
	}
	if rec := asHost(a, "GET", "/subjects/orders/versions/1", "127.0.0.1:8081", ""); rec.Code != 200 {
		t.Errorf("the refused DELETE removed version 1: %d %s", rec.Code, rec.Body)
	}
	if rec := asHost(a, "GET", "/config", "127.0.0.1:8081", ""); !strings.Contains(rec.Body.String(), `"BACKWARD"`) {
		t.Errorf("the refused PUT /config changed the level: %s", rec.Body)
	}
	if rec := asHost(a, "GET", "/mode", "127.0.0.1:8081", ""); !strings.Contains(rec.Body.String(), `"READWRITE"`) {
		t.Errorf("the refused PUT /mode changed the mode: %s", rec.Body)
	}
}

// The names a registry is reached by on a developer's machine, with no option set:
// a loopback literal with any port, localhost, any IP address (an address is never a
// rebinding name), and a name in the .doze zone (schema.<instance>.doze).
func TestTheRegistrysOwnHostsAreServed(t *testing.T) {
	r, _ := openReg(t)
	a := NewAPI(r, Options{})
	for _, host := range []string{
		"127.0.0.1:8081", "127.0.0.1:54321", "127.0.0.1", "localhost:8081", "localhost", "[::1]:8081", "[::1]",
		"schema.shop.doze", "schema.shop.doze:8081", "schema.doze", "192.168.1.20:8081", "0.0.0.0:8081",
		// Case and the root dot are the same name.
		"LOCALHOST:8081", "Schema.Shop.DOZE", "schema.shop.doze.", "localhost.:8081",
	} {
		if rec := asHost(a, "GET", "/subjects", host, ""); rec.Code != 200 {
			t.Errorf("GET /subjects with Host %q = %d %s, want 200", host, rec.Code, rec.Body)
		}
	}
}

// A trusted name as a prefix, a suffix or a userinfo is not that name.
func TestLookalikeHostsAreRefused(t *testing.T) {
	r, _ := openReg(t)
	a := NewAPI(r, Options{AllowedHosts: []string{"host.docker.internal"}})
	for _, host := range []string{
		"schema.shop.doze.evil.example", "schema.shop.doze.evil.example:8081", "evil.example:8081@localhost",
		"localhost:8081@evil.example", "localhost@evil.example", "localhost.evil.example", "127.0.0.1.evil.example:8081",
		"xschema.shop.dozee", "evildoze", "host.docker.internal.evil.example", "evil.host.docker.internal",
		"host.docker.internal:8081@evil.example", "localhost:8081 evil.example", "",
	} {
		if rec := asHost(a, "GET", "/subjects", host, ""); rec.Code != http.StatusForbidden {
			t.Errorf("GET /subjects with Host %q = %d, want 403", host, rec.Code)
		}
	}
}

// What the server adds: the host clients are told to dial (--advertised-host; a
// client in a container reaches the registry as host.docker.internal) and the
// operator's own list. "*" turns the check off, as it does for the console.
func TestAllowedHostsAreServedAndNoOthers(t *testing.T) {
	r, _ := openReg(t)
	a := NewAPI(r, Options{AllowedHosts: []string{"host.docker.internal", "Registry.Dev.Example."}})
	for _, host := range []string{"host.docker.internal:8081", "host.docker.internal", "HOST.docker.internal.:8081", "registry.dev.example:8081", "127.0.0.1:8081"} {
		if rec := asHost(a, "GET", "/subjects", host, ""); rec.Code != 200 {
			t.Errorf("GET /subjects with the allowed Host %q = %d %s, want 200", host, rec.Code, rec.Body)
		}
	}
	if rec := asHost(a, "GET", "/subjects", "other.example:8081", ""); rec.Code != http.StatusForbidden {
		t.Errorf("a host that is not on the list = %d, want 403", rec.Code)
	}

	open := NewAPI(r, Options{AllowedHosts: []string{"*"}})
	if rec := asHost(open, "GET", "/subjects", "anything.example:8081", ""); rec.Code != 200 {
		t.Errorf(`with "*" a foreign Host = %d, want 200`, rec.Code)
	}
}

// With SASL on the registry asks for HTTP basic credentials. The Host is judged
// first: a foreign Host gets the same 403 whether it sent no credentials, wrong ones
// or the right ones, and never the 401 with WWW-Authenticate, which would tell the
// page that credentials are wanted and have the browser ask the person for them on
// the attacker's origin.
func TestTheHostIsJudgedBeforeTheCredentials(t *testing.T) {
	r, _ := openReg(t)
	a := NewAPI(r, Options{Auth: func(u, p string) bool { return u == "alice" && p == "pw" }})
	try := func(host, user, pass string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/subjects/orders/versions", strings.NewReader(schemaBody(avroUser1)))
		req.Host = host
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		rec := httptest.NewRecorder()
		a.ServeHTTP(rec, req)
		return rec
	}
	var refusals []string
	for _, cred := range [][2]string{{"", ""}, {"alice", "nope"}, {"alice", "pw"}} {
		rec := try("attacker.example:8081", cred[0], cred[1])
		if rec.Code != http.StatusForbidden {
			t.Errorf("a foreign Host with credentials %q = %d, want 403", cred, rec.Code)
		}
		if h := rec.Header().Get("WWW-Authenticate"); h != "" {
			t.Errorf("a foreign Host with credentials %q was asked to authenticate: %q", cred, h)
		}
		refusals = append(refusals, rec.Body.String())
	}
	if refusals[0] != refusals[1] || refusals[1] != refusals[2] {
		t.Errorf("the refusal differs with the credentials, which says whether they were right: %q", refusals)
	}
	if rec := try("127.0.0.1:8081", "", ""); rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Errorf("the registry's own Host with no credentials = %d, want 401 and a challenge", rec.Code)
	}
	if rec := try("127.0.0.1:8081", "alice", "nope"); rec.Code != http.StatusUnauthorized {
		t.Errorf("the registry's own Host with a wrong password = %d, want 401", rec.Code)
	}
	if got := func() []string {
		req := httptest.NewRequest("GET", "/subjects", nil)
		req.Host = "127.0.0.1:8081"
		req.SetBasicAuth("alice", "pw")
		rec := httptest.NewRecorder()
		a.ServeHTTP(rec, req)
		var out []string
		json.Unmarshal(rec.Body.Bytes(), &out)
		return out
	}(); len(got) != 0 {
		t.Errorf("a refused request registered a subject: %v", got)
	}
	if rec := try("127.0.0.1:8081", "alice", "pw"); rec.Code != 200 {
		t.Errorf("the registry's own Host with the right credentials = %d %s, want 200", rec.Code, rec.Body)
	}
}
