package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// D6. The secrets of a configuration are the SASL passwords, the key a TLS certificate
// is served with, and the credential the console logs in with. This plants a distinctive
// value of each and reads every surface the process has for it. The surfaces, and how
// each is known to have been reached, are listed where they are read:
// secrets_surfaces_test.go for a running broker, this file for the configuration.
//
// A surface that was not reached is a failure, never a log line: a guard that can pass
// without having looked is not one.
const (
	plantedPassword = "PLANTEDsecret-Zq7-93f1"
	plantedWrong    = "Nope-WRONGpw-51aa-Xk"
)

// consoleSecret is the console's password with the random source replaced.
func plantConsoleSecret(t *testing.T) string {
	t.Helper()
	src := []byte("CONSOLE-SECRET-xyz!") // 19 bytes; 18 are read.
	old := credentialRand
	credentialRand = sameBytes(src) // every broker this test starts gets the same one.
	t.Cleanup(func() { credentialRand = old })
	return hex.EncodeToString(src[:18])
}

// sameBytes reads as b from its start, every time it is read.
type sameBytes []byte

func (b sameBytes) Read(p []byte) (int, error) { return copy(p, b), nil }

func keyMaterial(t *testing.T, keyFile string) (whole, fragment string) {
	t.Helper()
	b, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	var body strings.Builder
	for _, l := range strings.Split(string(b), "\n") {
		if l != "" && !strings.HasPrefix(l, "-----") {
			body.WriteString(strings.TrimSpace(l))
		}
	}
	whole = body.String()
	if len(whole) < 60 {
		t.Fatalf("the key's base64 is %d bytes", len(whole))
	}
	return whole, whole[20:52]
}

// fetch is the whole answer to a GET: the status line, the headers and the body.
func fetch(t *testing.T, url string) string {
	t.Helper()
	_, text := httpGet(t, url, "", "")
	return text
}

// httpGet is fetch with HTTP Basic credentials (when user is not "") and the status.
func httpGet(t *testing.T, url, user, pass string) (status int, text string) {
	t.Helper()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	// The headers as they were sent, and the body read whole: a dump of both writes a
	// chunked body in chunks, and a secret across a chunk boundary would not be found.
	head, err := httputil.DumpResponse(resp, false)
	if err != nil {
		t.Fatalf("GET %s: reading the answer: %v", url, err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("GET %s: reading the answer: %v", url, err)
	}
	return resp.StatusCode, string(head) + string(body)
}

var linkRe = regexp.MustCompile(`(?:href|hx-get|data-href|data-url)="(/[^"#]*)"`)

// crawl is every console page reachable from the seeds by a link, read as text.
func crawl(t *testing.T, base string, seeds ...string) map[string]string {
	t.Helper()
	pages := map[string]string{}
	queue := append([]string(nil), seeds...)
	for len(queue) > 0 && len(pages) < 400 {
		p := queue[0]
		queue = queue[1:]
		if _, seen := pages[p]; seen || !strings.HasPrefix(p, "/_console") {
			continue
		}
		if strings.Contains(p, "/static/") || strings.Contains(p, "/delete") || strings.Contains(p, "/purge") {
			continue
		}
		body := fetch(t, base+p)
		pages[p] = body
		for _, m := range linkRe.FindAllStringSubmatch(body, -1) {
			link := strings.ReplaceAll(m[1], "&amp;", "&")
			if _, seen := pages[link]; !seen {
				queue = append(queue, link)
			}
		}
	}
	return pages
}

// guard is the planted secrets and every form of them a surface might carry.
type guard struct{ secrets map[string]string }

// plant is the guard for these secrets. A password is also looked for as a program
// would have re-encoded it: in base64 (an Authorization header, a SASL token written
// out) and in hex.
func plant(consolePass, keyWhole, keyFragment string) guard {
	g := guard{secrets: map[string]string{
		"a SASL password": plantedPassword, "the start of a SASL password": "PLANTEDsecret", "a wrong password": plantedWrong,
		"the console credential": consolePass, "the TLS key": keyWhole, "a piece of the TLS key": keyFragment,
	}}
	for what, who := range map[string][2]string{
		"a SASL password": {"alice", plantedPassword}, "a wrong password": {"alice", plantedWrong}, "the console credential": {consoleUserName, consolePass},
	} {
		user, pass := who[0], who[1]
		g.secrets[what+", in base64"] = base64.StdEncoding.EncodeToString([]byte(pass))
		g.secrets[what+", in hex"] = hex.EncodeToString([]byte(pass))
		g.secrets[what+", as HTTP Basic credentials"] = base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
		g.secrets[what+", as a SASL/PLAIN token in base64"] = base64.StdEncoding.EncodeToString([]byte("\x00" + user + "\x00" + pass))
	}
	return g
}

// check fails the test if text shows any of the secrets.
func (g guard) check(t *testing.T, surface, text string) {
	t.Helper()
	for what, s := range g.secrets {
		if i := strings.Index(text, s); i >= 0 {
			t.Errorf("%s shows %s: ...%q...", surface, what, text[max(0, i-60):min(len(text), i+len(s)+20)])
		}
	}
}

func TestNoSecretReachesAnyOutput(t *testing.T) {
	consolePass := plantConsoleSecret(t)
	certFile, keyFile := selfSigned(t)
	keyWhole, keyFragment := keyMaterial(t, keyFile)
	g := plant(consolePass, keyWhole, keyFragment)
	check := func(surface, text string) { t.Helper(); g.check(t, surface, text) }

	// The configuration: `config print` redacts by default; --show-secrets says it
	// outright and is the only way to the password.
	args := []string{"print", "--listen", "127.0.0.1:9092", "--sasl-mechanisms", "PLAIN", "--sasl-user", "alice:" + plantedPassword,
		"--tls-cert", certFile, "--tls-key", keyFile}
	var so, se bytes.Buffer
	if code := runConfig(args, &so, &se); code != 0 {
		t.Fatalf("config print: %d %s", code, se.String())
	}
	check("config print", so.String()+se.String())
	if !strings.Contains(so.String(), `alice = "<redacted>"`) {
		t.Errorf("config print does not say the password is redacted:\n%s", so.String())
	}
	so.Reset()
	if code := runConfig(append(args, "--show-secrets"), &so, &se); code != 0 {
		t.Fatalf("config print --show-secrets: %d %s", code, se.String())
	}
	if !strings.Contains(so.String(), plantedPassword) {
		t.Errorf("--show-secrets did not show the password:\n%s", so.String())
	}

	// A printed file is not a valid file to run from: the placeholder is refused where it
	// is loaded, by name, instead of becoming the password.
	redacted := filepath.Join(t.TempDir(), "printed.toml")
	so.Reset()
	runConfig(args, &so, &se)
	os.WriteFile(redacted, so.Bytes(), 0o600)
	if _, err := loadConfig([]string{"--config", redacted}); err == nil || !strings.Contains(err.Error(), "alice") || !strings.Contains(err.Error(), "--show-secrets") {
		t.Errorf("a printed, redacted file loaded: %v", err)
	}

	// Errors the command line and the file give for a bad value never carry the value.
	// Every one of these IS an error: a row that loaded would be a row that checked
	// nothing.
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		os.WriteFile(p, []byte(content), 0o600)
		return p
	}
	for name, a := range map[string][]string{
		"a --sasl-user with no colon":           {"--sasl-mechanisms", "PLAIN", "--sasl-user", plantedPassword},
		"a password in an unquoted TOML value":  {"--config", write("a.toml", "[credentials]\nalice = "+plantedPassword+"\n")},
		"a password where a number was written": {"--config", write("b.toml", "[credentials]\nalice = 12"+plantedPassword+"\n")},
		"a password of the wrong type":          {"--config", write("c.toml", "[credentials]\nalice = [\""+plantedPassword+"\"]\n")},
		"a password in an unknown key":          {"--config", write("d.toml", "[credentials]\nalice = \""+plantedPassword+"\"\n[other]\nx = \""+plantedPassword+"\"\n")},
		"a TLS key that is not a key":           {"--tls-cert", certFile, "--tls-key", write("k.pem", "-----BEGIN PRIVATE KEY-----\n"+keyWhole[:len(keyWhole)-8]+"\n-----END PRIVATE KEY-----\n")},
		"a mechanism that is not known":         {"--sasl-mechanisms", "NOPE", "--sasl-user", "alice:" + plantedPassword},
		"a SASL user with an empty password":    {"--sasl-mechanisms", "PLAIN", "--sasl-user", "alice:" + plantedPassword, "--sasl-user", "bob:"},
		"credentials with no mechanism":         {"--sasl-user", "alice:" + plantedPassword},
	} {
		_, err := loadConfig(a)
		if err == nil {
			t.Errorf("%s: the configuration loaded, so no error was read for it", name)
			continue
		}
		check(name+" (loadConfig)", err.Error())
		var eb bytes.Buffer
		if code := runConfig(append([]string{"print"}, a...), io.Discard, &eb); code == 0 || eb.Len() == 0 {
			t.Errorf("%s: config print exited %d and wrote %q, so no error was read for it", name, code, eb.String())
		}
		check(name+" (config print)", eb.String())
	}

	// The running broker: every log format at every level, with every mechanism.
	for _, format := range []string{"text", "json"} {
		for _, level := range []string{"debug", "info", "warn", "error"} {
			t.Run(format+" logs at "+level, func(t *testing.T) {
				runningBrokerSurfaces(t, g, format, level, certFile, keyFile)
			})
		}
	}
	t.Run("pprof", func(t *testing.T) { pprofSurfaces(t, g, certFile, keyFile) })
	t.Run("the commands", func(t *testing.T) { commandSurfaces(t, g, certFile, keyFile) })
}
