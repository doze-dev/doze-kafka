package console

// The console's own address is Traffic (Main, "the home page"): what the apps are
// asking the broker is the first thing a person wants to see. A broker that has
// nothing in it yet and has heard from no one is the first-run page (Sy-08) instead.
// Without a traffic recorder (the console running apart from the broker) the home is
// Topics, because Traffic is not offered at all.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// homeSection is the rail section "/" belongs to.
func (c *Console) homeSection() string {
	if c.traffic != nil {
		return "traffic"
	}
	return "topics"
}

// home answers the console's own address.
func (c *Console) home(w http.ResponseWriter, r *http.Request) {
	if c.isFirstRun(r.Context()) {
		c.firstRunPage(w, r)
		return
	}
	if c.traffic != nil {
		c.trafficPage(w, r)
		return
	}
	c.topics(w, r)
}

// isFirstRun is true for a broker that has nothing in it (no topic, no group, no
// subject) and has had no request or connection from anyone: the state a person is
// in the first time they open the console. A broker that cannot be asked is not
// "empty": the home page then shows the error.
func (c *Console) isFirstRun(ctx context.Context) bool {
	topics, groups, err := c.be.Counts(ctx)
	if err != nil || topics != 0 || groups != 0 {
		return false
	}
	if c.reg != nil {
		if names := c.reg.SubjectNames(ctx); len(names) != 0 {
			return false
		}
	}
	if c.traffic != nil {
		if len(c.traffic.Events(TrafficFilter{}, 1)) != 0 || len(c.traffic.Clients()) != 0 {
			return false
		}
	}
	return true
}

// connectFacts is everything the Connect page and the first-run page say about how
// to reach this broker: read from the broker's own configuration and from how this
// console is itself connected, never typed.
type connectFacts struct {
	Bootstrap  string
	Protocol   string   // security.protocol: PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL.
	Mechanisms []string // sasl.mechanism, as the listener serves it; empty when it is open.
	SASL, TLS  bool
	Registry   string // where clients find the schema registry, or "".
	Console    string // where this console is, as this request reached it.
	DataDir    string
	Compat     string // "Kafka 4.0 compat", or "".
	AutoCreate bool
	AutoParts  int
	// Env is what `doze-kafka env` prints for this broker, line by line.
	Env []string
	// Properties is a client.properties for it, line by line.
	Properties []string
	// CommandConfig is the flag the Kafka command-line tools need when the listener
	// is not open: "--command-config client.properties ", or "".
	CommandConfig string
}

func (c *Console) connectFacts(r *http.Request) connectFacts {
	f := connectFacts{
		Bootstrap: c.be.displayAddr(),
		Registry:  c.facts.RegistryURL,
		DataDir:   c.facts.DataDir, AutoCreate: c.facts.AutoCreateTopics, AutoParts: c.facts.DefaultPartitions,
	}
	if f.Registry == "" && c.reg != nil {
		f.Registry = c.reg.base
	}
	if m := c.facts.KafkaMajor; m > 0 {
		f.Compat = fmt.Sprintf("Kafka %d.0 compat", m)
	}
	f.Mechanisms = append([]string(nil), c.facts.SASLMechanisms...)
	if len(f.Mechanisms) == 0 && c.be.opts.SASLUser != "" {
		m := c.be.opts.SASLMechanism
		if m == "" {
			m = "PLAIN"
		}
		f.Mechanisms = []string{m}
	}
	f.SASL = len(f.Mechanisms) > 0
	f.TLS = c.be.opts.TLSConfig != nil
	switch {
	case f.SASL && f.TLS:
		f.Protocol = "SASL_SSL"
	case f.SASL:
		f.Protocol = "SASL_PLAINTEXT"
	case f.TLS:
		f.Protocol = "SSL"
	default:
		f.Protocol = "PLAINTEXT"
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	f.Console = scheme + "://" + r.Host + c.prefix
	f.Env = []string{"export KAFKA_BOOTSTRAP_SERVERS=" + f.Bootstrap, "export KAFKA_BROKERS=" + f.Bootstrap}
	if f.SASL {
		f.Env = append(f.Env, "export KAFKA_SASL_MECHANISM="+f.Mechanisms[0])
	}
	if f.Registry != "" {
		f.Env = append(f.Env, "export SCHEMA_REGISTRY_URL="+f.Registry)
	}
	f.Properties = []string{"bootstrap.servers=" + f.Bootstrap}
	if f.Protocol != "PLAINTEXT" {
		f.Properties = append(f.Properties, "security.protocol="+f.Protocol)
	}
	if f.SASL {
		f.Properties = append(f.Properties, "sasl.mechanism="+f.Mechanisms[0])
		f.CommandConfig = "--command-config client.properties "
	} else if f.TLS {
		f.CommandConfig = "--command-config client.properties "
	}
	return f
}

// Console as the address a person reads, without the scheme.
func (f connectFacts) ConsoleHost() string {
	return strings.TrimPrefix(strings.TrimPrefix(f.Console, "https://"), "http://")
}

// firstRunPage is Sy-08: "Your broker is up", with the three steps that get a
// message through, each a command that is real for this broker.
func (c *Console) firstRunPage(w http.ResponseWriter, r *http.Request) {
	data := map[string]any{"Facts": c.connectFacts(r), "Title": "Your broker is up", "Page": c.homeSection(), "HasTrafficLink": c.traffic != nil}
	c.render(w, r, "first_run", data)
}

// connectPage is Cn-01: the settings a Kafka client needs for this broker, filled in
// from how the broker is running right now.
func (c *Console) connectPage(w http.ResponseWriter, r *http.Request) {
	f := c.connectFacts(r)
	var rows []connectRow
	rows = append(rows, connectRow{Key: "bootstrap.servers", Value: f.Bootstrap, Copy: true})
	rows = append(rows, connectRow{Key: "security.protocol", Value: f.Protocol, Copy: true})
	if f.SASL {
		rows = append(rows, connectRow{Key: "sasl.mechanism", Value: strings.Join(f.Mechanisms, " or "), Copy: true},
			connectRow{Key: "username", Note: "one of the users under [credentials] in doze-kafka.toml, or --sasl-user"},
			connectRow{Key: "password", Note: "set with that user; never shown here"})
	}
	if f.Registry != "" {
		rows = append(rows, connectRow{Key: "schema.registry.url", Value: f.Registry, Copy: true})
	}
	rows = append(rows, connectRow{Key: "console", Value: f.Console, Copy: true})
	c.render(w, r, "connect_detail", map[string]any{"Facts": f, "Rows": rows, "Page": "connect", "Title": "Connect an app"})
}

// connectRow is one setting on the Connect page: a value to copy, or a note where
// the value is a secret or the console does not know it.
type connectRow struct {
	Key, Value, Note string
	Copy             bool
}
