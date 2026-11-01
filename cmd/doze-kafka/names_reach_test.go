package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	names "github.com/doze-dev/doze-names"
)

// probe is a stand-in for names.Reachable that records what it was asked.
type probe struct {
	answers bool
	asked   []string
}

func (p *probe) reach(host string, port int) bool {
	p.asked = append(p.asked, fmt.Sprintf("%s:%d", host, port))
	return p.answers
}

// claimed is a zone holding kafka.<instance>.doze in a registry of its own, with the
// reachability check replaced.
func claimed(t *testing.T, instance string, p *probe) *zone {
	t.Helper()
	reg := names.Open(t.TempDir(), "doze-kafka-test")
	own, err := reg.Claim(names.Qualified("kafka", instance))
	if err != nil {
		t.Fatalf("claiming a name in a private registry: %v", err)
	}
	z := &zone{reg: reg, own: own, reach: p.reach}
	t.Cleanup(z.close)
	return z
}

// The console's URL in the startup summary is the name only when something answers at
// the name. A URL that does not open is worse than an address that does.
func TestTheConsoleURLIsTheNameOnlyWhenTheNameAnswers(t *testing.T) {
	var log bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&log, nil))

	up := &probe{answers: true}
	z := claimed(t, "shop", up)
	got := z.routeConsole(logger, "127.0.0.9:9093")
	if !strings.HasPrefix(got, "http://kafka.shop.doze") {
		t.Errorf("a name that answers: console URL = %q, want it by name", got)
	}
	if len(up.asked) != 1 || !strings.HasPrefix(up.asked[0], "kafka.shop.doze:") {
		t.Errorf("the name was not asked exactly once before it was printed: %v", up.asked)
	}
	if log.Len() != 0 {
		t.Errorf("nothing to explain when the name answers:\n%s", log.String())
	}

	down := &probe{}
	z = claimed(t, "shop", down)
	got = z.routeConsole(logger, "127.0.0.9:9093")
	if got != "http://127.0.0.9:9093" {
		t.Errorf("a name that does not answer: console URL = %q, want the address it bound", got)
	}
	if strings.Contains(got, ".doze") {
		t.Errorf("a name nothing answers at was printed: %q", got)
	}
	// Said once, at a level a default run shows, with the name and where to look.
	text := log.String()
	if strings.Count(text, "level=INFO") != 1 || !strings.Contains(text, "kafka.shop.doze") ||
		!strings.Contains(text, "127.0.0.9:9093") || !strings.Contains(text, "doze-kafka doctor") {
		t.Errorf("the reason was not said once at info, with the name, the address and the fix:\n%s", text)
	}

	// A wildcard console address is not somewhere to browse to.
	z = claimed(t, "shop", &probe{})
	if got := z.routeConsole(logger, "0.0.0.0:9093"); got != "http://127.0.0.1:9093" {
		t.Errorf("wildcard console, name not answering: %q, want http://127.0.0.1:9093", got)
	}
}

// The same for the registry's own name, schema.<instance>.doze.
func TestTheRegistryURLIsTheNameOnlyWhenTheNameAnswers(t *testing.T) {
	var log bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&log, nil))

	up := &probe{answers: true}
	z := claimed(t, "shop", up)
	got := z.routeRegistry(logger, "shop", "127.0.0.9:8081")
	if !strings.HasPrefix(got, "http://schema.shop.doze") {
		t.Errorf("a name that answers: registry URL = %q, want it by name", got)
	}
	if len(up.asked) != 1 || !strings.HasPrefix(up.asked[0], "schema.shop.doze:") {
		t.Errorf("the registry's name was not asked exactly once: %v", up.asked)
	}

	z = claimed(t, "shop", &probe{})
	got = z.routeRegistry(logger, "shop", "127.0.0.9:8081")
	if got != "http://127.0.0.9:8081" {
		t.Errorf("a name that does not answer: registry URL = %q, want the address it bound", got)
	}
	if text := log.String(); strings.Count(text, "level=INFO") != 1 || !strings.Contains(text, "schema.shop.doze") {
		t.Errorf("the reason was not said once at info with the name:\n%s", text)
	}
	// The route is still published: the name works the moment it resolves.
	if e := z.reg.Snapshot()["schema.shop.doze"]; e.Target != "127.0.0.9:8081" {
		t.Errorf("the registry's route = %+v, want it published whatever was printed", e)
	}
}

// The broker's own name: resolving here is necessary and not enough. On a Mac the
// name can resolve to its address while the network daemon that carries packets to
// it is gone, and then the name is an address nothing answers at.
func TestTheBrokerNameMustResolveHereAndAnswer(t *testing.T) {
	here := func(z *zone) lookupFunc {
		return func(context.Context, string) ([]string, error) { return []string{z.ip()}, nil }
	}
	nowhere := func(context.Context, string) ([]string, error) { return nil, errors.New("no such host") }

	up := &probe{answers: true}
	z := claimed(t, "shop", up)
	if why := z.whyNotByName(here(z), kafkaPort); why != "" {
		t.Errorf("a name that resolves here and answers was refused: %q", why)
	}
	if len(up.asked) != 1 || up.asked[0] != "kafka.shop.doze:9092" {
		t.Errorf("asked %v, want kafka.shop.doze:9092 once", up.asked)
	}

	down := &probe{}
	z = claimed(t, "shop", down)
	if why := z.whyNotByName(here(z), kafkaPort); !strings.Contains(why, "answer") {
		t.Errorf("a name that resolves but does not answer: why = %q, want it to say nothing answers", why)
	}

	// A name that does not resolve is not dialled at all: the lookup already said no,
	// and a second wait would only make startup slower.
	unasked := &probe{answers: true}
	z = claimed(t, "shop", unasked)
	if why := z.whyNotByName(nowhere, kafkaPort); !strings.Contains(why, "resolve") {
		t.Errorf("a name that does not resolve: why = %q", why)
	}
	if len(unasked.asked) != 0 {
		t.Errorf("a name that does not resolve was dialled anyway: %v", unasked.asked)
	}
}

// `doze-kafka env` and the banner must agree, and env runs as its own process: it
// asks the zone's registry where the running instance is, then asks the name itself.
func TestEnvPrintsTheNameOnlyWhenItAnswers(t *testing.T) {
	const instance = "envreach"
	reg := names.Open(names.Home(), "doze-kafka")
	own, err := reg.Claim(names.Qualified("kafka", instance))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer own.Release()
	schema, err := reg.Claim(names.Qualified("schema", instance))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer schema.Release()
	ip := own.IP.String()
	if err := own.Route(consoleAddrFor(ip)); err != nil {
		t.Fatal(err)
	}
	if err := schema.Route(registryAddrFor(ip)); err != nil {
		t.Fatal(err)
	}

	oldLookup, oldReach := machineLookup, nameReachable
	defer func() { machineLookup, nameReachable = oldLookup, oldReach }()
	machineLookup = func(_ context.Context, host string) ([]string, error) {
		if host == "schema."+instance+".doze" {
			return []string{schema.IP.String()}, nil
		}
		return []string{ip}, nil
	}

	cfg := config.Default()
	cfg.Name = instance
	env := func(answers bool) string {
		nameReachable = func(string, int) bool { return answers }
		var b strings.Builder
		envTo(&b, cfg)
		return b.String()
	}

	out := env(true)
	for _, want := range []string{
		"export KAFKA_BOOTSTRAP_SERVERS=kafka.envreach.doze:9092\n",
		"export SCHEMA_REGISTRY_URL=http://schema.envreach.doze",
		"# Console: http://kafka.envreach.doze",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the names answer, and env is missing %q:\n%s", want, out)
		}
	}

	out = env(false)
	if strings.Contains(out, ".doze") {
		t.Errorf("no name answers, and env still prints one:\n%s", out)
	}
	for _, want := range []string{
		"export KAFKA_BOOTSTRAP_SERVERS=" + ip + ":9092\n",
		"export SCHEMA_REGISTRY_URL=http://" + registryAddrFor(ip) + "\n",
		"# Console: http://" + consoleAddrFor(ip) + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("no name answers, and env is missing the address %q:\n%s", want, out)
		}
	}
}
