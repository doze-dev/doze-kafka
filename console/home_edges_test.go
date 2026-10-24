package console

import (
	"context"
	"crypto/tls"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func factsOf(t *testing.T, c *Console, tlsReq bool) connectFacts {
	t.Helper()
	r := httptest.NewRequest("GET", "/_console/connect", nil)
	r.Host = "console.test:7070"
	if tlsReq {
		r.TLS = &tls.ConnectionState{}
	}
	return c.connectFacts(r)
}

func TestConnectFactsDescribeHowToReachTheBroker(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Facts.KafkaMajor = 3; o.DisplayAddr = "shown.example:9092" })
	f := factsOf(t, c, false)
	if f.Bootstrap != "shown.example:9092" || f.Protocol != "PLAINTEXT" || f.SASL || f.TLS || f.CommandConfig != "" || f.Compat != "Kafka 3.0 compat" || f.Registry != "" {
		t.Errorf("open broker: %+v", f)
	}
	if f.Console != "http://console.test:7070/_console" {
		t.Errorf("console %q", f.Console)
	}
	if strings.Join(f.Properties, "\n") != "bootstrap.servers=shown.example:9092" || len(f.Env) != 2 || f.Env[0] != "export KAFKA_BOOTSTRAP_SERVERS=shown.example:9092" {
		t.Errorf("properties %v env %v", f.Properties, f.Env)
	}
	if f = factsOf(t, c, true); f.Console != "https://console.test:7070/_console" {
		t.Errorf("console over TLS %q", f.Console)
	}

	// The listener asks for a login: from what it serves, or from how the console signs in.
	c.be.opts.SASLUser = "u"
	f = factsOf(t, c, false)
	if !f.SASL || f.Protocol != "SASL_PLAINTEXT" || strings.Join(f.Mechanisms, ",") != "PLAIN" || f.CommandConfig != "--command-config client.properties " {
		t.Errorf("signed in with PLAIN: %+v", f)
	}
	if !strings.Contains(strings.Join(f.Properties, "\n"), "PlainLoginModule") || !strings.Contains(strings.Join(f.Env, "\n"), "export KAFKA_SASL_MECHANISM=PLAIN") {
		t.Errorf("properties %v env %v", f.Properties, f.Env)
	}
	c.be.opts.SASLMechanism = "SCRAM-SHA-512"
	f = factsOf(t, c, false)
	if strings.Join(f.Mechanisms, ",") != "SCRAM-SHA-512" || !strings.Contains(strings.Join(f.Properties, "\n"), "ScramLoginModule") {
		t.Errorf("signed in with SCRAM: %+v", f)
	}
	c.be.opts.SASLUser, c.be.opts.SASLMechanism = "", ""

	c.facts.SASLMechanisms = []string{"PLAIN", "SCRAM-SHA-256"}
	f = factsOf(t, c, false)
	if !f.SASL || len(f.Mechanisms) != 2 || !strings.Contains(strings.Join(f.Properties, "\n"), "# the listener also takes SCRAM-SHA-256") {
		t.Errorf("two mechanisms: %+v", f)
	}
	c.facts.SASLMechanisms = nil

	c.be.opts.TLSConfig = &tls.Config{}
	f = factsOf(t, c, false)
	if !f.TLS || f.Protocol != "SSL" || f.CommandConfig != "--command-config client.properties " || !strings.Contains(strings.Join(f.Properties, "\n"), "security.protocol=SSL") {
		t.Errorf("TLS: %+v", f)
	}
	c.facts.SASLMechanisms = []string{"PLAIN"}
	if f = factsOf(t, c, false); f.Protocol != "SASL_SSL" {
		t.Errorf("TLS and SASL: %s", f.Protocol)
	}
	c.be.opts.TLSConfig = nil
}

func TestConnectFactsRegistryAddress(t *testing.T) {
	c, _, ts := newRegistryConsole(t, func(o *Options) { o.Facts.RegistryURL = "http://public.example:8081" })
	f := factsOf(t, c, false)
	if f.Registry != "http://public.example:8081" || !strings.Contains(strings.Join(f.Env, "\n"), "export SCHEMA_REGISTRY_URL=http://public.example:8081") {
		t.Errorf("the address clients are told: %q (the console reaches it at %s)", f.Registry, ts.URL)
	}
	c.facts.RegistryURL = ""
	if f = factsOf(t, c, false); f.Registry != ts.URL {
		t.Errorf("without a public address the console's own: %q, want %s", f.Registry, ts.URL)
	}
	c.facts.SASLMechanisms = []string{"PLAIN"}
	if f = factsOf(t, c, false); !strings.Contains(strings.Join(f.Properties, "\n"), "basic.auth.user.info=<user>:<password>") {
		t.Errorf("a registry behind a login: %v", f.Properties)
	}
}

// The first-run page is for a broker with nothing in it and nobody who has talked to it.
func TestFirstRunNeedsAnEmptyAndUntouchedBroker(t *testing.T) {
	src := &fakeTraffic{}
	c, _, _ := newRegistryConsole(t, func(o *Options) { o.Traffic = src })
	ctx := context.Background()
	if !c.isFirstRun(ctx) {
		t.Fatal("an empty broker is not first run")
	}
	src.clients = []TrafficClient{{Conn: 1, Remote: "r", ClientID: "a", Opened: time.Now(), Last: time.Now()}}
	if c.isFirstRun(ctx) {
		t.Error("a broker with a client is first run")
	}
	src.clients = nil
	src.events = []TrafficEvent{{ID: "r1", Seq: 1, First: time.Now(), Time: time.Now(), Count: 1, ClientID: "a", API: "Metadata"}}
	if c.isFirstRun(ctx) {
		t.Error("a broker that has been asked something is first run")
	}
	src.events = nil
	register(t, c, "s-value", userV1)
	if c.isFirstRun(ctx) {
		t.Error("a broker with a schema is first run")
	}
	other := newTestConsole(t)
	topicWith(t, other, "t", 1, 0)
	if other.isFirstRun(ctx) {
		t.Error("a broker with a topic is first run")
	}
}

func TestHomeSectionFollowsTheRecorder(t *testing.T) {
	if got := newTestConsole(t).homeSection(); got != "topics" {
		t.Errorf("without a recorder the home is %q", got)
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = &fakeTraffic{} })
	if got := c.homeSection(); got != "traffic" {
		t.Errorf("with a recorder the home is %q", got)
	}
}
