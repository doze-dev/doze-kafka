package server_test

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
	"github.com/doze-dev/doze-kafka/server"
)

// advertisesTelemetry asks a bound server which APIs it offers.
func advertisesTelemetry(t *testing.T, srv *server.Server) (get, push bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, err := srv.Bind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ctx)
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr.String()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
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
	return
}

// Telemetry is read by the console. A broker with none keeps no store and offers no
// APIs for clients to push into it (TRF-14): every Java client of an embedded broker
// in a Go test otherwise pushed its metrics every five seconds to nobody.
func TestEmbeddedBrokerDoesNotAdvertiseTelemetry(t *testing.T) {
	srv, err := server.New(server.Options{DataDir: t.TempDir(), Version: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if serverhook.ClientMetrics(srv) != nil {
		t.Error("an embedded broker keeps a telemetry store nobody will read")
	}
	if get, push := advertisesTelemetry(t, srv); get || push {
		t.Errorf("an embedded broker advertises get=%v push=%v", get, push)
	}
}

func TestNoConsoleMeansNoTelemetry(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir, cfg.ListenAddr, cfg.Console = t.TempDir(), "127.0.0.1:0", false
	cfg.ClientTelemetry = true // the default: it is the console that decides.
	srv, err := server.NewFromConfig(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if serverhook.ClientMetrics(srv) != nil {
		t.Error("--console=false still keeps a telemetry store")
	}
	if get, push := advertisesTelemetry(t, srv); get || push {
		t.Errorf("--console=false advertises get=%v push=%v", get, push)
	}

	cfg.Console, cfg.DataDir = true, t.TempDir()
	with, err := server.NewFromConfig(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer with.Close()
	if get, push := advertisesTelemetry(t, with); !get || !push {
		t.Errorf("a broker with a console and --client-telemetry advertises get=%v push=%v", get, push)
	}
}
