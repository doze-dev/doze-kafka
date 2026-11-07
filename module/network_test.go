//go:build unix

package kafka

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/doze-dev/doze-sdk/engine"
	"github.com/doze-dev/doze-sdk/enginetest"
)

// A broker on the declared port, a console on the instance's name, and the
// schema registry on its child registry.<name>.
func TestTheKafkaNetwork(t *testing.T) {
	inst := engine.Instance{Name: "events", Type: "kafka"}
	enginetest.CheckNetwork(t, Driver{}, inst)
	n := Driver{}.Network(inst)
	want := []engine.Listener{
		{Name: engine.MainListener, Protocol: engine.ProtocolTCP},
		{Name: "console", Port: consolePort, Protocol: engine.ProtocolHTTP, Routes: []string{engine.RouteSelf}},
		{Name: "registry", Port: registryPort, Protocol: engine.ProtocolHTTP, Routes: []string{"registry"}},
	}
	if !slices.EqualFunc(n.Listeners, want, func(a, b engine.Listener) bool {
		return a.Name == b.Name && a.Port == b.Port && a.Protocol == b.Protocol && slices.Equal(a.Routes, b.Routes)
	}) {
		t.Fatalf("network = %+v", n.Listeners)
	}
}

func grantFor(dir string) engine.Grant {
	return engine.Grant{Host: "events.shop.doze", Listeners: map[string]engine.GrantedListener{
		engine.MainListener: {Addr: "events.shop.doze:9092", Backend: filepath.Join(dir, "kafka.sock")},
		"console":           {Addr: "events.shop.doze:9093", URL: "http://events.shop.doze", Hosts: []string{"events.shop.doze"}, Backend: filepath.Join(dir, "console.sock")},
		"registry":          {Addr: "events.shop.doze:8081", URL: "http://registry.events.shop.doze", Hosts: []string{"registry.events.shop.doze"}, Backend: filepath.Join(dir, "registry.sock")},
	}}
}

// With a grant, the broker advertises its name, and the console and registry
// listen on the sockets doze forwards their ports to; the brokers' clients are
// told the name.
func TestPlanFollowsTheGrant(t *testing.T) {
	dir := t.TempDir()
	inst := engine.Instance{Name: "events", SocketDir: dir, DataDir: filepath.Join(dir, "d"), Endpoint: engine.Endpoint{TCPAddr: "events.shop.doze:9092"}, Grant: grantFor(dir)}
	plan, err := Driver{}.Plan(context.Background(), inst, engine.Toolchain{Full: "4"})
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(plan.Specs[0].Args, " ")
	for _, want := range []string{
		"--advertise events.shop.doze:9092",
		"--console-socket " + filepath.Join(dir, "console.sock"),
		"--registry-socket " + filepath.Join(dir, "registry.sock"),
		"--registry-url http://registry.events.shop.doze",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("args lack %q: %s", want, args)
		}
	}
	if strings.Contains(args, "--console-addr") {
		t.Errorf("with a grant the console is not bound by the module: %s", args)
	}
	if _, u := (Driver{}).ConnString(inst, inst.Endpoint); u != "events.shop.doze:9092" {
		t.Errorf("KAFKA_BROKERS = %q", u)
	}

	// An older doze gives no grant: the console one port up, as before, and no registry.
	old := engine.Instance{Name: "events", SocketDir: dir, DataDir: inst.DataDir, Endpoint: engine.Endpoint{TCPAddr: "events.shop.doze:9092"}}
	plan, _ = Driver{}.Plan(context.Background(), old, engine.Toolchain{Full: "4"})
	args = strings.Join(plan.Specs[0].Args, " ")
	if !strings.Contains(args, "--console-addr events.shop.doze:9093") || strings.Contains(args, "--registry-socket") {
		t.Errorf("without a grant: %s", args)
	}
}

// Served as Plan writes it with a grant: the console and the schema registry
// answer on their sockets.
func TestTheConsoleAndRegistryAnswerOnTheirSockets(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "ks") // a unix socket's path is short.
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	g := grantFor(dir)
	argv := []string{"__serve", "--socket", g.Listeners[engine.MainListener].Backend, "--datadir", filepath.Join(dir, "data"), "--version", "4",
		"--advertise", "events.shop.doze:9092",
		"--console-socket", g.Listeners["console"].Backend,
		"--registry-socket", g.Listeners["registry"].Backend,
		"--registry-url", "http://registry.events.shop.doze"}
	done := make(chan error, 1)
	go func() { done <- ServeFromArgs(argv) }()
	t.Cleanup(func() {
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("ServeFromArgs did not return after SIGTERM")
		}
	})
	get := func(sock, host, path string) (int, string) {
		c := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		}}}
		deadline := time.Now().Add(10 * time.Second)
		for {
			resp, err := c.Get("http://" + host + path)
			if err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				return resp.StatusCode, string(b)
			}
			if time.Now().After(deadline) {
				t.Fatalf("GET %s on %s: %v", path, sock, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	// Through doze's front door each arrives with its name for a Host.
	if code, body := get(g.Listeners["registry"].Backend, "registry.events.shop.doze", "/subjects"); code != 200 || strings.TrimSpace(body) != "[]" {
		t.Errorf("registry /subjects = %d %q", code, body)
	}
	if code, body := get(g.Listeners["console"].Backend, "events.shop.doze", "/_console/"); code != 200 || !strings.Contains(body, "doze-kafka") {
		t.Errorf("console = %d %.200q", code, body)
	}
}
