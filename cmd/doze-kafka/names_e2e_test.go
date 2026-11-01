package main

// The by-name path, end to end, on a machine where .doze is set up.
//
// Everything else in this package stands in for the machine: a lookup that says yes,
// a reachability check that says no. This file does not. It starts the server the
// way `doze-kafka` with no flags starts it, and then reaches it the way a person
// would: the broker at kafka.<name>.doze:9092, the console at http://kafka.<name>.doze
// and the registry at http://schema.<name>.doze, each resolved by the machine's own
// resolver.
//
// # Where it runs
//
// Where `doze-kafka dns-setup` has been run. Anywhere else it skips and says why,
// unless DOZE_NAMES_REQUIRED is set, which turns every such skip in this package
// into a failure: CI sets it after its setup step, so a runner that silently lost
// the zone fails the job and does not go green on tests that never ran.
//
// It never sets the machine up itself. The server would (as root it installs
// without asking), so the test looks first and does not start a server on a machine
// that is not ready.

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	names "github.com/doze-dev/doze-names"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// envNamesRequired makes a by-name test that cannot run a failure, not a skip.
const envNamesRequired = "DOZE_NAMES_REQUIRED"

func namesRequired() bool { return os.Getenv(envNamesRequired) != "" }

// skipOrFail is how a by-name test says it cannot run here.
func skipOrFail(t *testing.T, format string, args ...any) {
	t.Helper()
	if namesRequired() {
		t.Fatalf(envNamesRequired+" is set, and this test cannot run: "+format, args...)
	}
	t.Skipf(format, args...)
}

// needZone stops a test that needs the machine's .doze setup where there is none.
// It looks and changes nothing.
func needZone(t *testing.T) {
	t.Helper()
	if names.Disabled() {
		skipOrFail(t, "%s switches the zone off", names.EnvZone)
	}
	if st := names.Check(); !st.OK() {
		skipOrFail(t, ".doze is not set up on this machine (%s); run `doze-kafka dns-setup`:\n%s", st.Platform, st)
	}
}

func TestByNameEndToEnd(t *testing.T) {
	needZone(t)

	instance := fmt.Sprintf("byname%d", os.Getpid())
	kafkaName, schemaName := "kafka."+instance+".doze", "schema."+instance+".doze"

	dir := t.TempDir()
	t.Chdir(dir)
	st, err := loadConfig([]string{"--name", instance, "--data-dir", filepath.Join(dir, "data"), "--config", ""})
	if err != nil {
		t.Fatal(err)
	}
	out := &safeBuf{}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, st, newLogger(st.cfg, out), out) }()
	stopped := false
	defer func() {
		stop()
		if !stopped {
			<-done
		}
	}()
	waitFor(t, "the summary", func() bool {
		select {
		case err := <-done:
			stopped = true
			skipOrFail(t, "the server did not start by name: %v\n%s", err, out.String())
		default:
		}
		return strings.Contains(out.String(), "doze-kafka is up")
	})
	summary := out.String()

	// The broker, by name: what the summary says, what a client bootstraps on, and
	// what Metadata sends that client back to.
	if !strings.Contains(summary, "kafka     "+kafkaName+":9092\n") {
		skipOrFail(t, "the summary does not give the broker by name, so the name does not resolve or answer on this machine:\n%s", summary)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaName + ":9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	meta, err := kmsg.NewPtrMetadataRequest().RequestWith(reqCtx, cl)
	if err != nil {
		t.Fatalf("Metadata through %s:9092: %v", kafkaName, err)
	}
	if len(meta.Brokers) != 1 || meta.Brokers[0].Host != kafkaName || meta.Brokers[0].Port != 9092 {
		t.Errorf("Metadata brokers = %+v, want one at %s:9092", meta.Brokers, kafkaName)
	}

	// The console and the registry, each at the URL the summary printed. Port-less
	// means through the shared front door, by Host header: the registry's name is
	// the second claim, and it is routed the way the console's is.
	consoleURL := regexp.MustCompile(`console   (http://\S+)`).FindStringSubmatch(summary)
	registryURL := regexp.MustCompile(`registry  (http://\S+)`).FindStringSubmatch(summary)
	if consoleURL == nil || registryURL == nil {
		t.Fatalf("the summary is missing the console or the registry:\n%s", summary)
	}
	for _, c := range []struct{ what, url, name string }{
		{"console", consoleURL[1], kafkaName},
		{"registry", registryURL[1], schemaName},
	} {
		switch {
		case c.url == "http://"+c.name:
		case strings.HasPrefix(c.url, "http://"+c.name+":") && !namesRequired():
			t.Logf("the %s is by name with its port (%s): no front door is up on this machine", c.what, c.url)
		default:
			t.Errorf("the %s is at %s, want http://%s (port-less, through the front door)", c.what, c.url, c.name)
		}
	}
	get := func(url string) (int, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	if code, body := get(consoleURL[1] + "/_console/topics"); code != 200 || !strings.Contains(body, "Topics") {
		t.Errorf("GET %s/_console/topics = %d %.120q, want the console's Topics page", consoleURL[1], code, body)
	}
	if code, body := get(registryURL[1] + "/subjects"); code != 200 || strings.TrimSpace(body) != "[]" {
		t.Errorf("GET %s/subjects = %d %.120q, want 200 and an empty list from the registry", registryURL[1], code, body)
	}
	// The two names are two routes: the registry's name must not serve the console.
	if code, body := get(registryURL[1] + "/_console/topics"); code == 200 && strings.Contains(body, "Topics") {
		t.Errorf("%s served the console: the second claim is routed to the first one's target", registryURL[1])
	}

	// `doze-kafka env` is another process's view of the same instance, and agrees.
	var env strings.Builder
	envTo(&env, st.cfg)
	for _, want := range []string{
		"export KAFKA_BOOTSTRAP_SERVERS=" + kafkaName + ":9092\n",
		"export SCHEMA_REGISTRY_URL=" + registryURL[1] + "\n",
		"# Console: " + consoleURL[1] + "\n",
	} {
		if !strings.Contains(env.String(), want) {
			t.Errorf("env disagrees with the summary: missing %q:\n%s", want, env.String())
		}
	}

	// And names.Reachable, the check the summary rests on, says what was just seen:
	// yes for the name that answered, no for a name nobody claimed.
	if !names.Reachable(kafkaName, 9092) {
		t.Errorf("names.Reachable(%s, 9092) = false for a broker that just answered there", kafkaName)
	}
	if names.Reachable("kafka.nobody-"+instance+".doze", 9092) {
		t.Error("names.Reachable is true for a name nothing claimed")
	}

	stop()
	if err := <-done; err != nil {
		t.Errorf("a clean shutdown returned %v", err)
	}
	stopped = true
	if _, err := net.DefaultResolver.LookupHost(context.Background(), kafkaName); err == nil && names.Reachable(kafkaName, 9092) {
		t.Errorf("%s still answers after the instance stopped", kafkaName)
	}
}
