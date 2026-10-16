//go:build unix

package kafka

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/telemetry"
)

// served runs the module's broker exactly as doze does: `__serve` with the flags
// Plan writes, through ServeFromArgs, until the process is sent SIGTERM. It returns
// the broker's socket and, when asked for one, the console's base URL.
func served(t *testing.T, withConsole bool) (socket, consoleURL string) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ks") // a unix socket's path is short.
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket = filepath.Join(dir, "k.sock")
	argv := []string{"__serve", "--socket", socket, "--datadir", filepath.Join(dir, "data"), "--version", "4",
		"--advertise", "kafka.local:9092", "--auto-create", "true"}
	if withConsole {
		free, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := free.Addr().String()
		free.Close()
		argv = append(argv, "--console-addr", addr)
		consoleURL = "http://" + addr + "/_console"
	}
	done := make(chan error, 1)
	go func() { done <- ServeFromArgs(argv) }()
	t.Cleanup(func() {
		// ServeFromArgs serves until SIGTERM, which it has asked to be told of (it is
		// accepting, so it has): this ends it and nothing else.
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			t.Errorf("SIGTERM: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("ServeFromArgs returned %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("ServeFromArgs did not return after SIGTERM")
		}
	})
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if c, err := net.Dial("unix", socket); err == nil {
			c.Close()
			return socket, consoleURL
		}
		select {
		case err := <-done:
			done <- err
			t.Fatalf("ServeFromArgs returned before it was accepting: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the broker's socket never accepted")
		}
	}
}

// offersTelemetry asks the broker which APIs it offers.
func offersTelemetry(t *testing.T, cl *kgo.Client) (get, push bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := cl.Request(ctx, kmsg.NewPtrApiVersionsRequest())
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range resp.(*kmsg.ApiVersionsResponse).ApiKeys {
		switch k.ApiKey {
		case int16(kmsg.GetTelemetrySubscriptions):
			get = true
		case int16(kmsg.PushTelemetry):
			push = true
		}
	}
	return get, push
}

// page fetches a console page, waiting until want is in it.
func page(t *testing.T, url string, want ...string) string {
	t.Helper()
	var body string
	var status int
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		resp, err := http.Get(url)
		if err != nil {
			continue // the console's listener is started beside the broker's.
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		body, status = string(raw), resp.StatusCode
		missing := false
		for _, w := range want {
			missing = missing || !strings.Contains(body, w)
		}
		if status == http.StatusOK && !missing {
			return body
		}
	}
	t.Fatalf("GET %s: status %d, and the page never held all of %q (%d bytes)", url, status, want, len(body))
	return ""
}

// The console the module starts has the Traffic page and the clients' own metrics,
// as the doze-kafka command's console has: the broker beside it keeps the record of
// requests and offers the two client-telemetry APIs. (It was built as a broker with
// no console, so the console had neither and /traffic was not there at all.)
func TestTheModulesConsoleHasTrafficAndClientMetrics(t *testing.T) {
	socket, console := served(t, true)
	cl, err := unixClient(socket, kgo.ClientID("module-test-app"), kgo.DefaultProduceTopic("orders"), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if get, push := offersTelemetry(t, cl); !get || !push {
		t.Errorf("a broker with a console offers GetTelemetrySubscriptions=%v PushTelemetry=%v, want both", get, push)
	}
	if err := cl.ProduceSync(ctx, kgo.StringRecord("hello")).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// The request the app just made is on the Traffic page, with who made it.
	// So is the note that is only there when clients' own metrics are kept.
	page(t, console+"/traffic", "module-test-app", "Produce", "Clients that report their own metrics")
	page(t, console+"/traffic?view=clients", "module-test-app")

	// A client that reports is taken, and its report reaches the console.
	id := [16]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x47, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x01}
	sub := kmsg.NewPtrGetTelemetrySubscriptionsRequest()
	sub.ClientInstanceID = id
	sresp, err := cl.Request(ctx, sub)
	if err != nil {
		t.Fatalf("GetTelemetrySubscriptions: %v", err)
	}
	subscription := sresp.(*kmsg.GetTelemetrySubscriptionsResponse)
	if subscription.ErrorCode != 0 {
		t.Fatalf("GetTelemetrySubscriptions: error %d", subscription.ErrorCode)
	}
	push := kmsg.NewPtrPushTelemetryRequest()
	push.ClientInstanceID, push.SubscriptionID = id, subscription.SubscriptionID
	push.Metrics = telemetry.Encode(telemetry.Payload{
		Resource: map[string]string{"service.name": "module-test-service"},
		Metrics: []telemetry.Metric{{Name: "org.apache.kafka.producer.node.request.latency.avg", Unit: "ms", Kind: telemetry.Gauge,
			Points: []telemetry.Point{{Attrs: "node_id=1", Time: time.Now(), Value: 4.5}}}},
	})
	presp, err := cl.Request(ctx, push)
	if err != nil {
		t.Fatalf("PushTelemetry: %v", err)
	}
	if code := presp.(*kmsg.PushTelemetryResponse).ErrorCode; code != 0 {
		t.Fatalf("PushTelemetry: error %d", code)
	}
	// Each connected client opens an inspector; the one that pushed shows what it
	// reported.
	traffic := page(t, console+"/traffic", "PushTelemetry")
	origin := strings.TrimSuffix(console, "/_console")
	var inspectors int
	var reported bool
	for _, part := range strings.Split(traffic, `hx-get="`)[1:] {
		path, _, _ := strings.Cut(part, `"`)
		if !strings.Contains(path, "/inspect/client?") {
			continue
		}
		inspectors++
		resp, err := http.Get(origin + strings.ReplaceAll(path, "&amp;", "&"))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		reported = reported || strings.Contains(string(raw), "module-test-service") || strings.Contains(string(raw), "request.latency")
	}
	if inspectors == 0 {
		t.Fatal("the Traffic page lists no connected client to open")
	}
	if !reported {
		t.Errorf("none of the %d client inspectors shows what the client pushed", inspectors)
	}
}

// With no console there is nobody to read either, so the module's broker keeps
// neither and offers no client-telemetry APIs: the rule of docs/observability.md.
func TestTheModulesBrokerWithoutAConsoleOffersNoTelemetry(t *testing.T) {
	socket, _ := served(t, false)
	cl, err := unixClient(socket, kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if get, push := offersTelemetry(t, cl); get || push {
		t.Errorf("a broker with no console offers GetTelemetrySubscriptions=%v PushTelemetry=%v", get, push)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("hello")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}
