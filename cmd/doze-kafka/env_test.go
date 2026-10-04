package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// `eval "$(doze-kafka env)"` must give a client somewhere to connect: an empty
// KAFKA_BOOTSTRAP_SERVERS would send every client to localhost:9092 and a "broker
// down" it cannot explain.
func TestEnvPointsAtTheInstance(t *testing.T) {
	code, out, errOut := run3("env", "--name", "harbour", "--config", "")
	if code != 0 {
		t.Fatalf("env exited %d: %s", code, errOut)
	}
	if strings.Contains(out, "KAFKA_BOOTSTRAP_SERVERS=\n") {
		t.Errorf("the bootstrap is empty:\n%s", out)
	}
	// A running instance would make this the address it bound; with nothing running it
	// is the name this instance will claim, or the address if that does not resolve
	// here. Either way it names the instance's own port.
	if !strings.Contains(out, "export KAFKA_BOOTSTRAP_SERVERS=") || !strings.Contains(out, ":9092") {
		t.Errorf("KAFKA_BOOTSTRAP_SERVERS does not name a port-9092 address:\n%s", out)
	}
}

// Under --listen there is no name; the address given is what clients use.
func TestEnvUnderListenUsesTheAddress(t *testing.T) {
	code, out, errOut := run3("env", "--listen", "127.0.0.1:19092", "--config", "")
	if code != 0 {
		t.Fatalf("env exited %d: %s", code, errOut)
	}
	if !strings.Contains(out, "export KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092") {
		t.Errorf("not the listen address:\n%s", out)
	}
	if strings.Contains(out, ".doze") {
		t.Errorf("a .doze name under --listen resolves to nothing:\n%s", out)
	}
}

// An explicit advertised host is what clients are told to use, so it is what env prints.
func TestEnvHonoursTheAdvertisedHost(t *testing.T) {
	_, out, _ := run3("env", "--listen", ":19092", "--advertised-host", "host.docker.internal", "--config", "")
	if !strings.Contains(out, "export KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:19092") {
		t.Errorf("env ignored --advertised-host:\n%s", out)
	}
}

// The instance is named for its project directory unless --name says otherwise, so two
// checkouts are two instances with nobody configuring anything, and the server, `env`
// and `doctor` (which all load the configuration the same way) agree on which.
func TestTheInstanceIsNamedForTheProjectDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "harbour")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	st, err := loadConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.cfg.Name != "harbour" {
		t.Errorf("name = %q, want the directory's name", st.cfg.Name)
	}
	if st, _ = loadConfig([]string{"--name", "atlas"}); st.cfg.Name != "atlas" {
		t.Errorf("--name did not win: %q", st.cfg.Name)
	}
}
