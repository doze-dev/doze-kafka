package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// D6. The secrets of a configuration are the SASL passwords, the key a TLS certificate
// is served with, and the credential the console logs in with. This plants a distinctive
// value of each and reads every surface the process has for it: `config print`, the log
// at debug (every request), the start-up summary, every console page it links to and its
// JSON, what a client is answered to DescribeConfigs and to a failed login, and the
// errors the command line and the config file give for a bad value.
const (
	plantedPassword = "PLANTEDsecret-Zq7-93f1"
	plantedWrong    = "Nope-WRONGpw-51aa-Xk"
)

// consoleSecret is the console's password with the random source replaced.
func plantConsoleSecret(t *testing.T) string {
	t.Helper()
	src := []byte("CONSOLE-SECRET-xyz!") // 19 bytes; 18 are read.
	old := credentialRand
	credentialRand = bytes.NewReader(src)
	t.Cleanup(func() { credentialRand = old })
	return hex.EncodeToString(src[:18])
}

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

func fetch(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

var linkRe = regexp.MustCompile(`(?:href|hx-get|data-href|data-url)="(/[^"#]*)"`)

// crawl is every console page reachable from the seeds by a link, read as text.
func crawl(t *testing.T, base string, seeds ...string) map[string]string {
	t.Helper()
	pages := map[string]string{}
	queue := append([]string(nil), seeds...)
	for len(queue) > 0 && len(pages) < 120 {
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

func TestNoSecretReachesAnyOutput(t *testing.T) {
	consolePass := plantConsoleSecret(t)
	certFile, keyFile := selfSigned(t)
	keyWhole, keyFragment := keyMaterial(t, keyFile)
	secrets := map[string]string{
		"a SASL password": plantedPassword, "the start of a SASL password": "PLANTEDsecret", "a wrong password": plantedWrong,
		"the console credential": consolePass, "the TLS key": keyWhole, "a piece of the TLS key": keyFragment,
	}
	check := func(surface, text string) {
		t.Helper()
		for what, s := range secrets {
			if strings.Contains(text, s) {
				i := strings.Index(text, s)
				t.Errorf("%s shows %s: ...%s...", surface, what, text[max(0, i-40):min(len(text), i+len(s)+20)])
			}
		}
	}

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
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		os.WriteFile(p, []byte(content), 0o600)
		return p
	}
	for name, a := range map[string][]string{
		"a --sasl-user with no colon": {"--sasl-mechanisms", "PLAIN", "--sasl-user", plantedPassword},
		"a password in an unquoted TOML value": {"--config", write("a.toml", "[credentials]\nalice = "+plantedPassword+"\n")},
		"a password where a number was written": {"--config", write("b.toml", "[credentials]\nalice = 12"+plantedPassword+"\n")},
		"a password of the wrong type":          {"--config", write("c.toml", "[credentials]\nalice = [\""+plantedPassword+"\"]\n")},
		"a password in an unknown key":          {"--config", write("d.toml", "[credentials]\nalice = \""+plantedPassword+"\"\n[other]\nx = \""+plantedPassword+"\"\n")},
		"a TLS key that is not a key":           {"--tls-cert", certFile, "--tls-key", write("k.pem", "-----BEGIN PRIVATE KEY-----\n"+keyWhole+"\n-----END PRIVATE KEY-----\n")},
		"a mechanism that is not known":         {"--sasl-mechanisms", "NOPE", "--sasl-user", "alice:" + plantedPassword},
		"a SASL user with an empty password":    {"--sasl-mechanisms", "PLAIN", "--sasl-user", "alice:" + plantedPassword, "--sasl-user", "bob:"},
		"credentials with no mechanism":         {"--sasl-user", "alice:" + plantedPassword},
	} {
		_, err := loadConfig(a)
		if err == nil {
			continue
		}
		check(name+" (loadConfig)", err.Error())
		var eb bytes.Buffer
		runConfig(append([]string{"print"}, a...), io.Discard, &eb)
		check(name+" (config print)", eb.String())
	}

	// The running broker, at debug, with the password used and a wrong one tried.
	out, stop := start(t, "--log-level", "debug", "--sasl-mechanisms", "PLAIN", "--sasl-user", "alice:"+plantedPassword,
		"--tls-cert", certFile, "--tls-key", keyFile)
	waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "doze-kafka is up") })
	text := out.String()
	kafkaAddr := regexp.MustCompile(`kafka +(127\.0\.0\.1:\d+)`).FindStringSubmatch(text)
	base := regexp.MustCompile(`http://127\.0\.0\.1:\d+`).FindString(text)
	if kafkaAddr == nil || base == "" {
		t.Fatalf("the summary does not name the addresses:\n%s", text)
	}
	client := func(pass string) *kgo.Client {
		cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr[1]), kgo.DialTLSConfig(&tls.Config{InsecureSkipVerify: true}),
			kgo.SASL(plain.Auth{User: "alice", Pass: pass}.AsMechanism()), kgo.WithLogger(nil))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cl.Close)
		return cl
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var answers strings.Builder
	bad := client(plantedWrong)
	if _, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, bad); err != nil {
		answers.WriteString(err.Error())
	} else {
		t.Error("a wrong password was accepted")
	}
	good := client(plantedPassword)
	ct := kmsg.NewCreateTopicsRequest()
	ctt := kmsg.NewCreateTopicsRequestTopic()
	ctt.Topic, ctt.NumPartitions, ctt.ReplicationFactor = "orders", 1, 1
	ct.Topics = append(ct.Topics, ctt)
	if _, err := ct.RequestWith(ctx, good); err != nil {
		t.Fatal(err)
	}
	md, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, good)
	if err != nil || len(md.Brokers) == 0 {
		t.Fatalf("metadata: %v", err)
	}
	dc := kmsg.NewDescribeConfigsRequest()
	for _, r := range []struct {
		typ  kmsg.ConfigResourceType
		name string
	}{{kmsg.ConfigResourceTypeBroker, fmt.Sprint(md.Brokers[0].NodeID)}, {kmsg.ConfigResourceTypeTopic, "orders"}} {
		res := kmsg.NewDescribeConfigsRequestResource()
		res.ResourceType, res.ResourceName = r.typ, r.name
		dc.Resources = append(dc.Resources, res)
	}
	resp, err := dc.RequestWith(ctx, good)
	if err != nil {
		t.Fatal(err)
	}
	answers.WriteString(fmt.Sprintf("%+v", resp))
	check("DescribeConfigs and a failed login", answers.String())
	for _, r := range resp.Resources {
		for _, c := range r.Configs {
			low := strings.ToLower(c.Name)
			if (strings.Contains(low, "password") || strings.Contains(low, "key") || strings.Contains(low, "jaas")) && c.Value != nil && *c.Value != "" {
				t.Errorf("DescribeConfigs gives a value for %s on %s %q", c.Name, r.ResourceType, r.ResourceName)
			}
		}
	}

	// The console, from the cluster page and the traffic page outward.
	pages := crawl(t, base, "/_console/cluster", "/_console/traffic", "/_console/traffic/live", "/_console/connect",
		"/_console/access", "/_console/topics", "/_console/groups", "/_console/schemas", "/_console/api/palette", "/_console/api/counts")
	if len(pages) < 8 {
		t.Errorf("the crawl reached only %d pages", len(pages))
	}
	for p, body := range pages {
		check("the console page "+p, body)
	}
	if !strings.Contains(strings.Join(mapValues(pages), "\n"), "SaslAuthenticate") {
		t.Log("the traffic page did not list a SaslAuthenticate request; the crawl still covered the pages it reached")
	}

	if err := stop(); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	check("the log and the summary", out.String())
}

func mapValues(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}
