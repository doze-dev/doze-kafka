package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
)

// summaryLine is the address the start-up summary gives on one of its lines.
func summaryLine(t *testing.T, out *safeBuf, label string) string {
	t.Helper()
	waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "doze-kafka is up") })
	m := regexp.MustCompile(`(?m)^\s+` + label + `\s+(\S+)`).FindStringSubmatch(out.String())
	if m == nil {
		t.Fatalf("the summary has no %s line:\n%s", label, out.String())
	}
	return m[1]
}

// trusting is a TLS client configuration that trusts exactly the certificate in
// certFile, the way a client given ssl.ca.location does.
func trusting(t *testing.T, certFile string) *tls.Config {
	t.Helper()
	pem, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("the certificate did not parse")
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

const avroR = `{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}`

// A broker given a certificate serves its schema registry over TLS with it, and a
// real registry client (franz-go's, which speaks Confluent's API) works against the
// https URL the summary prints while trusting only that certificate. It used to stay
// plain HTTP: `schema.registry.url=https://...` failed, and what did work sent every
// schema in the clear beside an encrypted broker.
func TestTheRegistryIsServedOverTLSWithTheBrokersCertificate(t *testing.T) {
	cert, key := selfSigned(t)
	out, _ := start(t, "--tls-cert", cert, "--tls-key", key)
	url := summaryLine(t, out, "registry")
	if !strings.HasPrefix(url, "https://127.0.0.1:") {
		t.Fatalf("the summary gives the registry as %s, want https on the address it bound", url)
	}
	ctx := context.Background()
	cl, err := sr.NewClient(sr.URLs(url), sr.DialTLSConfig(trusting(t, cert)))
	if err != nil {
		t.Fatal(err)
	}
	created, err := cl.CreateSchema(ctx, "orders-value", sr.Schema{Schema: avroR})
	if err != nil {
		t.Fatalf("registering over https: %v", err)
	}
	if created.ID != 1 || created.Version != 1 {
		t.Errorf("registered as id %d version %d, want 1 and 1", created.ID, created.Version)
	}
	if got, err := cl.SchemaByID(ctx, created.ID); err != nil || !strings.Contains(got.Schema, `"name":"R"`) {
		t.Errorf("reading the schema back by id: %+v %v", got, err)
	}
	if subjects, err := cl.Subjects(ctx); err != nil || len(subjects) != 1 || subjects[0] != "orders-value" {
		t.Errorf("subjects = %v %v", subjects, err)
	}

	// A client that was not given the certificate does not get in on trust it never had.
	strict, err := sr.NewClient(sr.URLs(url))
	if err != nil {
		t.Fatal(err)
	}
	var unknown x509.UnknownAuthorityError
	if _, err := strict.Subjects(ctx); !errors.As(err, &unknown) {
		t.Errorf("a client without the certificate: %v, want an unknown-authority failure", err)
	}
	// And the port does not also speak plain HTTP.
	resp, err := http.Get("http://" + strings.TrimPrefix(url, "https://") + "/subjects")
	if err == nil {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 {
			t.Errorf("plain HTTP on the registry's TLS port answered 200: %s", body)
		}
	}

	// The console reaches the same registry, over the same TLS.
	console := summaryLine(t, out, "console")
	page, err := http.Get(console + "/_console/schemas")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(page.Body)
	page.Body.Close()
	if page.StatusCode != 200 || !strings.Contains(string(body), "orders-value") {
		t.Errorf("the console's Schemas page with the registry on TLS: %d, subject listed: %v\n%.400s",
			page.StatusCode, strings.Contains(string(body), "orders-value"), body)
	}
}

// With SASL as well, the registry asks for the broker's users as HTTP basic
// authentication, and over TLS those passwords are not readable on the wire.
func TestTheRegistryTakesBasicAuthOverTLS(t *testing.T) {
	cert, key := selfSigned(t)
	out, _ := start(t, "--tls-cert", cert, "--tls-key", key, "--sasl-mechanisms", "SCRAM-SHA-512", "--sasl-user", "alice:s3cret")
	url := summaryLine(t, out, "registry")
	if !strings.HasPrefix(url, "https://") {
		t.Fatalf("registry at %s, want https", url)
	}
	ctx := context.Background()
	client := func(opts ...sr.ClientOpt) *sr.Client {
		cl, err := sr.NewClient(append([]sr.ClientOpt{sr.URLs(url), sr.DialTLSConfig(trusting(t, cert))}, opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		return cl
	}
	if _, err := client(sr.BasicAuth("alice", "s3cret")).CreateSchema(ctx, "orders-value", sr.Schema{Schema: avroR}); err != nil {
		t.Fatalf("the broker's user over basic auth and TLS: %v", err)
	}
	for name, cl := range map[string]*sr.Client{
		"no credentials":   client(),
		"a wrong password": client(sr.BasicAuth("alice", "wrong")),
		"an unknown user":  client(sr.BasicAuth("mallory", "s3cret")),
	} {
		_, err := cl.Subjects(ctx)
		var re *sr.ResponseError
		if !errors.As(err, &re) || re.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: %v, want 401", name, err)
		}
	}
}

// --schema-registry-tls=false is the way back to plain HTTP beside a TLS broker,
// for a proxy that terminates TLS itself.
func TestTheRegistryCanStayPlainBesideATLSBroker(t *testing.T) {
	cert, key := selfSigned(t)
	out, _ := start(t, "--tls-cert", cert, "--tls-key", key, "--schema-registry-tls=false")
	url := summaryLine(t, out, "registry")
	if !strings.HasPrefix(url, "http://127.0.0.1:") {
		t.Fatalf("registry at %s, want plain http", url)
	}
	if code, body := call(t, "GET", url+"/subjects", "", "", ""); code != 200 || body != "[]" {
		t.Errorf("GET /subjects over plain HTTP = %d %s", code, body)
	}
	if strings.Contains(out.String(), "level=WARN") {
		t.Errorf("a plain registry on loopback is nothing to warn about:\n%s", out.String())
	}
}

// Without a certificate nothing changes: the registry is plain HTTP and the flag
// alone turns nothing on.
func TestTheRegistryIsPlainWithoutACertificate(t *testing.T) {
	out, _ := start(t)
	if url := summaryLine(t, out, "registry"); !strings.HasPrefix(url, "http://127.0.0.1:") {
		t.Fatalf("registry at %s, want plain http", url)
	}
}

// `env` gives the scheme the registry is really served on.
func TestEnvGivesTheRegistrysScheme(t *testing.T) {
	t.Chdir(t.TempDir())
	cert, key := selfSigned(t)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "export SCHEMA_REGISTRY_URL=http://127.0.0.1:8081\n"},
		{[]string{"--tls-cert", cert, "--tls-key", key}, "export SCHEMA_REGISTRY_URL=https://127.0.0.1:8081\n"},
		{[]string{"--tls-cert", cert, "--tls-key", key, "--schema-registry-tls=false"}, "export SCHEMA_REGISTRY_URL=http://127.0.0.1:8081\n"},
		{[]string{"--tls-cert", cert, "--tls-key", key, "--schema-registry-addr", ":18081"}, "export SCHEMA_REGISTRY_URL=https://localhost:18081\n"},
	} {
		code, out, errOut := run3(append([]string{"env", "--listen", "127.0.0.1:9092"}, tc.args...)...)
		if code != 0 || !strings.Contains(out, tc.want) {
			t.Errorf("env %v: code %d, stderr %q, stdout:\n%s\nwant it to contain %q", tc.args, code, errOut, out, tc.want)
		}
	}
}

// Beside a named broker the TLS registry is on the broker's own host: the front
// door cannot carry it, and that host is the one the certificate names.
func TestTheTLSRegistrysAddressBesideAName(t *testing.T) {
	for _, tc := range []struct {
		beside      bool
		host, bound string
		want        string
	}{
		{true, "kafka.harbour.doze", "127.77.1.5:8081", "https://kafka.harbour.doze:8081"},
		{true, "127.77.1.5", "127.77.1.5:8081", "https://127.77.1.5:8081"}, // the name does not resolve here
		{false, "kafka.harbour.doze", "127.0.0.1:18081", "https://127.0.0.1:18081"},
		{false, "127.0.0.1", "[::]:8081", "https://localhost:8081"},
		{true, "", "127.0.0.1:8081", "https://127.0.0.1:8081"},
	} {
		if got := registryTLSURL(tc.beside, tc.host, tc.bound); got != tc.want {
			t.Errorf("registryTLSURL(%v, %q, %q) = %q, want %q", tc.beside, tc.host, tc.bound, got, tc.want)
		}
	}
}

// The console has no login, is plain HTTP and acts as a super user. With SASL or
// TLS on, an address other machines can reach is refused as a configuration
// mistake, before anything starts, unless --console-unprotected accepts it, and
// then it is said at start-up as a warning.
func TestAnExposedConsoleIsRefusedWhenTheBrokerIsProtected(t *testing.T) {
	t.Chdir(t.TempDir())
	cert, key := selfSigned(t)
	sasl := []string{"--sasl-mechanisms", "PLAIN", "--sasl-user", "alice:s3cret"}
	tlsOn := []string{"--tls-cert", cert, "--tls-key", key}
	both := append(append([]string{}, sasl...), tlsOn...)
	with := func(base []string, more ...string) []string {
		return append(append([]string{"--listen", "127.0.0.1:0", "--schema-registry-addr", "127.0.0.1:0"}, base...), more...)
	}
	for _, tc := range []struct {
		name    string
		args    []string
		refused string // a phrase of the refusal, or "" when it must be accepted
	}{
		{"SASL, every interface", with(sasl, "--console-addr", "0.0.0.0:0"), "SASL is on: serving it on 0.0.0.0:0 would let anyone who can reach that address act on the broker as a super user, without a password"},
		{"SASL, an empty host", with(sasl, "--console-addr", ":0"), "SASL is on"},
		{"TLS, every interface", with(tlsOn, "--console-addr", "[::]:0"), "TLS is on: serving it on [::]:0 would send topics and messages unencrypted to anyone who can reach that address"},
		{"both, a LAN address", with(both, "--console-addr", "192.168.1.20:9093"), "SASL and TLS is on: serving it on 192.168.1.20:9093 would let anyone"},
		{"SASL, a host name", with(sasl, "--console-addr", "devbox.lan:9093"), "SASL is on"},
		{"SASL, loopback", with(sasl, "--console-addr", "127.0.0.1:0"), ""},
		{"SASL, the default", with(sasl), ""},
		{"SASL, localhost", with(sasl, "--console-addr", "localhost:0"), ""},
		{"SASL, IPv6 loopback", with(sasl, "--console-addr", "[::1]:0"), ""},
		{"SASL, the console off", with(sasl, "--console-addr", "0.0.0.0:0", "--console=false"), ""},
		{"neither, every interface", with(nil, "--console-addr", "0.0.0.0:0"), ""},
		{"SASL, accepted", with(sasl, "--console-addr", "0.0.0.0:0", "--console-unprotected"), ""},
	} {
		st, err := loadConfig(tc.args)
		if tc.refused == "" {
			if err != nil {
				t.Errorf("%s: refused: %v", tc.name, err)
			}
			wantWarning := tc.name == "SASL, accepted"
			if got := len(st.notices) == 1 && strings.Contains(st.notices[0], "--console-unprotected") && strings.Contains(st.notices[0], "super user"); got != wantWarning {
				t.Errorf("%s: notices %q, want a warning: %v", tc.name, st.notices, wantWarning)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		for _, want := range []string{tc.refused, "no login", "plain HTTP", "--console-unprotected", "--console=false", "loopback"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: the refusal does not mention %q: %v", tc.name, want, err)
			}
		}
		// And from the command line it is a usage error like any other: one line, exit 2.
		code, out, errOut := run3(tc.args...)
		if code != exitUsage || out != "" || strings.Count(errOut, "\n") != 1 || !strings.HasPrefix(errOut, "doze-kafka: the console has no login") {
			t.Errorf("%s: code %d, stdout %q, stderr %q; want exit 2 and one line", tc.name, code, out, errOut)
		}
	}
}

// The accepted exposure is said out loud in the log of the broker that has it.
func TestAnAcceptedExposureIsWarnedAboutAtStartUp(t *testing.T) {
	cert, key := selfSigned(t)
	out, _ := start(t, "--sasl-mechanisms", "PLAIN", "--sasl-user", "alice:s3cret", "--tls-cert", cert, "--tls-key", key,
		"--console-addr", "0.0.0.0:0", "--console-unprotected", "--schema-registry-addr", "0.0.0.0:0", "--schema-registry-tls=false")
	waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "doze-kafka is up") })
	text := out.String()
	for _, want := range []string{
		`level=WARN msg="the console is served on 0.0.0.0:0 with no login and over plain HTTP although SASL and TLS is on (--console-unprotected): that will let anyone who can reach that address act on the broker as a super user, without a password"`,
		`level=WARN msg="the schema registry is served on 0.0.0.0:0 over plain HTTP although the broker has TLS on (--schema-registry-tls=false)`,
		"basic-auth passwords",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the start-up log is missing %q:\n%s", want, text)
		}
	}
	if strings.Index(text, "level=WARN") > strings.Index(text, "doze-kafka is up") {
		t.Errorf("the warnings come after the summary, where they are not read:\n%s", text)
	}
}

// A registry that serves TLS beyond loopback is what was asked for: no warning.
func TestATLSRegistryBeyondLoopbackIsNotWarnedAbout(t *testing.T) {
	t.Chdir(t.TempDir())
	cert, key := selfSigned(t)
	st, err := loadConfig([]string{"--listen", "127.0.0.1:0", "--tls-cert", cert, "--tls-key", key, "--schema-registry-addr", "0.0.0.0:8081"})
	if err != nil || len(st.notices) != 0 {
		t.Errorf("notices %q, err %v", st.notices, err)
	}
}
