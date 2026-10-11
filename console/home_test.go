package console

import (
	"net/url"
	"strings"
	"testing"
)

// A broker with nothing in it and no one talking to it is the first-run page
// (Sy-08), at the console's own address; the moment it has a topic it is not.
func TestAnEmptyBrokerIsTheFirstRunPageAtTheHome(t *testing.T) {
	c, addr := newTestConsoleAddr(t, func(o *Options) {
		o.Facts = BrokerFacts{Workspace: "harbour", KafkaMajor: 4, DataDir: "./data", AutoCreateTopics: true, DefaultPartitions: 1}
	})
	body := req(t, c, "GET", "/_console/", nil).Body.String()
	for _, want := range []string{
		"Your broker is up", "Create a topic", "Connect an app", "Watch it arrive",
		"kafka-topics.sh", "--bootstrap-server " + addr, "data in ./data", "Kafka 4.0 compat",
		"Auto-create is also on", "waiting for the first request",
		`<span class="sub" id="workspace-label">harbour</span>`, `id="compat-pill"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the first-run page does not say %q", want)
		}
	}
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders"}, "partitions": {"1"}})
	body = req(t, c, "GET", "/_console/", nil).Body.String()
	if strings.Contains(body, "Your broker is up") {
		t.Error("a broker with a topic is still shown the first-run page")
	}
}

// The Connect page is the broker's own facts: the bootstrap address it is reached
// at, the security protocol, and a note (never a value) for a secret.
func TestConnectPageIsFilledInFromTheBroker(t *testing.T) {
	c, addr := newTestConsoleAddr(t, func(o *Options) {
		o.Facts = BrokerFacts{SASLMechanisms: []string{"SCRAM-SHA-512"}, RegistryURL: "http://127.0.0.1:8081"}
	})
	body := req(t, c, "GET", "/_console/connect", nil).Body.String()
	for _, want := range []string{
		"Connect an app", "bootstrap.servers", addr, "security.protocol", "SASL_PLAINTEXT",
		"sasl.mechanism", "SCRAM-SHA-512", "schema.registry.url", "http://127.0.0.1:8081",
		"never shown here", "export KAFKA_BOOTSTRAP_SERVERS=" + addr, "export KAFKA_SASL_MECHANISM=SCRAM-SHA-512",
		`class="nav on" aria-current="page"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the connect page does not say %q", want)
		}
	}
	open := newTestConsole(t)
	body = req(t, open, "GET", "/_console/connect", nil).Body.String()
	for _, bad := range []string{"sasl.mechanism", "username", "password", "schema.registry.url"} {
		if strings.Contains(body, bad) {
			t.Errorf("an open broker with no registry shows %q on its connect page", bad)
		}
	}
	if !strings.Contains(body, "PLAINTEXT") {
		t.Error("an open broker's connect page does not say PLAINTEXT")
	}
}

// The Trace page without an id asks for one (Tc-Home) instead of refusing; a pasted
// header value is read as the trace id it carries.
func TestTraceWithoutAnIDAsksForOne(t *testing.T) {
	c := newTestConsole(t)
	rec := req(t, c, "GET", "/_console/trace", nil)
	if rec.Code != 200 {
		t.Fatalf("GET /trace = %d, want the page that asks for an id", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"Follow one request through every topic it touched", `name="id"`, "Header formats it reads",
		"It only shows what your apps propagate", `class="nav on" aria-current="page"`} {
		if !strings.Contains(body, want) {
			t.Errorf("the trace home does not say %q", want)
		}
	}
	for raw, want := range map[string]string{
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01": "4bf92f3577b34da6a3ce929d0e0e4736",
		"463ac35c9f6413ad-00f067aa0ba902b7-1":                     "463ac35c9f6413ad",
		"  4bf92f3577b34da6a3ce929d0e0e4736 ":                     "4bf92f3577b34da6a3ce929d0e0e4736",
		"my-request-id":                                           "my-request-id",
	} {
		if got := traceIDFrom(raw); got != want {
			t.Errorf("traceIDFrom(%q) = %q, want %q", raw, got, want)
		}
	}
}
