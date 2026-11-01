package server_test

import (
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/embed"
	"github.com/doze-dev/doze-kafka/server"
)

// The schema registry's Host guard, through a real listener: the handler
// Server.SchemaRegistry hands out, served on a TCP port as cmd/doze-kafka serves
// it, and asked over HTTP with the Host header a client or a rebound page sends.

const registryCT = "application/vnd.schemaregistry.v1+json"

const orderSchema = `{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}"}`

// listenRegistry serves h on loopback and returns the address it bound.
func listenRegistry(t *testing.T, h http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go hs.Serve(ln) //nolint:errcheck // ends when the test closes it
	t.Cleanup(func() { hs.Close() })
	return ln.Addr().String()
}

type registryAnswer struct {
	code      int
	body      string
	challenge string
}

// askRegistry dials addr and sends one request whose Host header is host: what a
// browser does once a name it was given resolves to this machine.
func askRegistry(t *testing.T, addr, method, path, host, body string, userpass ...string) registryAnswer {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+addr+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	if body != "" {
		req.Header.Set("Content-Type", registryCT)
	}
	if len(userpass) == 2 {
		req.SetBasicAuth(userpass[0], userpass[1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s as %s: %v", method, path, host, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return registryAnswer{resp.StatusCode, strings.TrimSpace(string(b)), resp.Header.Get("WWW-Authenticate")}
}

func registryOf(t *testing.T, srv *server.Server) string {
	t.Helper()
	h, err := srv.SchemaRegistry()
	if err != nil {
		t.Fatal(err)
	}
	return listenRegistry(t, h)
}

// A page whose name was rebound to this machine reaches the port and is refused,
// whatever it asks for; nothing it asked for happened. The developer's own
// addresses keep working.
func TestTheRegistryListenerRefusesAForeignHost(t *testing.T) {
	srv := mustNew(t, server.Options{})
	defer srv.Close()
	addr := registryOf(t, srv)
	_, port, _ := net.SplitHostPort(addr)

	if a := askRegistry(t, addr, "POST", "/subjects/orders-value/versions", addr, orderSchema); a.code != 200 {
		t.Fatalf("registering on the listener's own address = %d %s", a.code, a.body)
	}

	foreign := "attacker.example:" + port
	for _, c := range []struct{ method, path, body string }{
		{"GET", "/subjects", ""},
		{"GET", "/schemas/ids/1", ""},
		{"POST", "/subjects/pwned-value/versions", orderSchema},
		{"PUT", "/config", `{"compatibility":"NONE"}`},
		{"DELETE", "/subjects/orders-value", ""},
	} {
		a := askRegistry(t, addr, c.method, c.path, foreign, c.body)
		if a.code != http.StatusForbidden || !strings.HasPrefix(a.body, `{"error_code":403,"message":"`) {
			t.Errorf("%s %s with Host %s = %d %s, want 403 in the registry's error shape", c.method, c.path, foreign, a.code, a.body)
		}
		if strings.Contains(a.body, "Order") || strings.Contains(a.body, "orders-value") {
			t.Errorf("%s %s: the refusal carries what the registry holds: %s", c.method, c.path, a.body)
		}
	}
	if a := askRegistry(t, addr, "GET", "/subjects", addr, ""); a.code != 200 || a.body != `["orders-value"]` {
		t.Errorf("after the refused requests GET /subjects = %d %s, want exactly [\"orders-value\"]", a.code, a.body)
	}
	if a := askRegistry(t, addr, "GET", "/config", addr, ""); !strings.Contains(a.body, "BACKWARD") {
		t.Errorf("the refused PUT /config took effect: %s", a.body)
	}

	for _, host := range []string{"127.0.0.1:" + port, "localhost:" + port, "[::1]:" + port, "schema.shop.doze", "schema.shop.doze:" + port, "LOCALHOST:" + port, "schema.shop.doze.:" + port} {
		if a := askRegistry(t, addr, "GET", "/subjects", host, ""); a.code != 200 {
			t.Errorf("GET /subjects with Host %q = %d %s, want 200", host, a.code, a.body)
		}
	}
	for _, host := range []string{
		"schema.shop.doze.evil.example", "schema.shop.doze.evil.example:" + port, "localhost.evil.example:" + port,
		// No advertised host was set, so the container name is nobody's here.
		"host.docker.internal:" + port,
	} {
		if a := askRegistry(t, addr, "GET", "/subjects", host, ""); a.code != http.StatusForbidden {
			t.Errorf("GET /subjects with Host %q = %d, want 403", host, a.code)
		}
	}
}

// A client in a container reaches the host's registry as host.docker.internal: the
// compatibility matrix starts the broker with --advertised-host host.docker.internal
// and --schema-registry-addr 0.0.0.0:PORT, and hands Java and Spring
// REGISTRY_URL=http://host.docker.internal:PORT. The host clients are told to dial
// is one of the registry's own names.
func TestTheRegistryAnswersToTheAdvertisedHost(t *testing.T) {
	srv := mustNew(t, server.Options{AdvertisedHost: "host.docker.internal"})
	defer srv.Close()
	addr := registryOf(t, srv)
	_, port, _ := net.SplitHostPort(addr)

	for _, host := range []string{"host.docker.internal:" + port, "host.docker.internal", "Host.Docker.Internal.:" + port} {
		if a := askRegistry(t, addr, "POST", "/subjects/orders-value/versions", host, orderSchema); a.code != 200 || a.body != `{"id":1}` {
			t.Errorf("registering with Host %q = %d %s, want 200 {\"id\":1}", host, a.code, a.body)
		}
		if a := askRegistry(t, addr, "GET", "/schemas/ids/1", host, ""); a.code != 200 || !strings.Contains(a.body, "Order") {
			t.Errorf("GET /schemas/ids/1 with Host %q = %d %s", host, a.code, a.body)
		}
	}
	// Only that name: not one that begins or ends with it, and not another.
	for _, host := range []string{"host.docker.internal.evil.example:" + port, "evil.host.docker.internal:" + port, "attacker.example:" + port, "host.docker.internal:" + port + "@evil.example"} {
		if a := askRegistry(t, addr, "GET", "/subjects", host, ""); a.code != http.StatusForbidden {
			t.Errorf("GET /subjects with Host %q = %d, want 403", host, a.code)
		}
	}
	// Loopback still works beside it (the host's own scripts use 127.0.0.1).
	if a := askRegistry(t, addr, "GET", "/subjects", "127.0.0.1:"+port, ""); a.code != 200 {
		t.Errorf("loopback beside an advertised host = %d %s", a.code, a.body)
	}
}

// The operator's list is the console's (--console-allow-host): one list of the
// names this instance is reached by. A name given as the registry's own address
// (--schema-registry-addr devbox.lan:8081) is expected to be used, as the console's
// is. "*" turns the check off for both.
func TestTheRegistryAnswersToTheOperatorsNames(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir, cfg.ListenAddr, cfg.Console = t.TempDir(), "127.0.0.1:0", false
	cfg.ConsoleAllowHosts = []string{"registry.dev.example"}
	cfg.SchemaRegistryAddr = "devbox.lan:8081"
	srv, err := embed.NewFromConfig(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	addr := registryOf(t, srv)

	for _, host := range []string{"registry.dev.example:8081", "registry.dev.example", "devbox.lan:8081", "DEVBOX.lan"} {
		if a := askRegistry(t, addr, "GET", "/subjects", host, ""); a.code != 200 {
			t.Errorf("GET /subjects with the operator's Host %q = %d %s, want 200", host, a.code, a.body)
		}
	}
	for _, host := range []string{"other.dev.example:8081", "devbox.lan.evil.example", "xdevbox.lan"} {
		if a := askRegistry(t, addr, "GET", "/subjects", host, ""); a.code != http.StatusForbidden {
			t.Errorf("GET /subjects with Host %q = %d, want 403", host, a.code)
		}
	}

	cfg.DataDir, cfg.ConsoleAllowHosts, cfg.SchemaRegistryAddr = t.TempDir(), []string{"*"}, "0.0.0.0:8081"
	open, err := embed.NewFromConfig(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	openAddr := registryOf(t, open)
	if a := askRegistry(t, openAddr, "GET", "/subjects", "anything.example:8081", ""); a.code != 200 {
		t.Errorf(`with --console-allow-host "*" a foreign Host = %d, want 200`, a.code)
	}

	// Bound to every interface on purpose, and nothing else said: the same guard.
	// A LAN address is an IP literal and passes; a name needs to be listed.
	cfg.DataDir, cfg.ConsoleAllowHosts = t.TempDir(), nil
	wide, err := embed.NewFromConfig(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer wide.Close()
	wideAddr := registryOf(t, wide)
	if a := askRegistry(t, wideAddr, "GET", "/subjects", "192.168.1.20:8081", ""); a.code != 200 {
		t.Errorf("a LAN address as Host on a wildcard bind = %d, want 200", a.code)
	}
	if a := askRegistry(t, wideAddr, "GET", "/subjects", "devbox.lan:8081", ""); a.code != http.StatusForbidden {
		t.Errorf("an unlisted name on a wildcard bind = %d, want 403", a.code)
	}
}

// With SASL on, the registry wants the broker's credentials as HTTP basic auth. A
// foreign Host is refused before they are looked at: 403 with or without them, and
// no challenge that would have the browser ask the person for a password on the
// attacker's page.
func TestTheRegistryRefusesAForeignHostBeforeAskingForCredentials(t *testing.T) {
	srv := mustNew(t, server.Options{SASLMechanisms: []string{"PLAIN"}, Credentials: map[string]string{"alice": "secret"}})
	defer srv.Close()
	addr := registryOf(t, srv)
	_, port, _ := net.SplitHostPort(addr)
	foreign := "attacker.example:" + port

	none := askRegistry(t, addr, "GET", "/subjects", foreign, "")
	wrong := askRegistry(t, addr, "GET", "/subjects", foreign, "", "alice", "nope")
	right := askRegistry(t, addr, "POST", "/subjects/pwned-value/versions", foreign, orderSchema, "alice", "secret")
	for name, a := range map[string]registryAnswer{"no credentials": none, "a wrong password": wrong, "the right password": right} {
		if a.code != http.StatusForbidden || a.challenge != "" {
			t.Errorf("a foreign Host with %s = %d (challenge %q), want 403 and no challenge", name, a.code, a.challenge)
		}
	}
	if none.body != wrong.body || wrong.body != right.body {
		t.Errorf("the refusal differs with the credentials: %q / %q / %q", none.body, wrong.body, right.body)
	}
	if a := askRegistry(t, addr, "GET", "/subjects", addr, ""); a.code != http.StatusUnauthorized || a.challenge == "" {
		t.Errorf("the registry's own Host with no credentials = %d (challenge %q), want 401 and a challenge", a.code, a.challenge)
	}
	if a := askRegistry(t, addr, "GET", "/subjects", addr, "", "alice", "secret"); a.code != 200 || a.body != "[]" {
		t.Errorf("the registry's own Host with the right credentials = %d %s, want 200 [] (the refused POST registered nothing)", a.code, a.body)
	}
}
