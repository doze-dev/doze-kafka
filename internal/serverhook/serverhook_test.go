package serverhook_test

import (
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
	"github.com/doze-dev/doze-kafka/server"
)

// The hooks are variables the server package fills in when it loads. Nothing
// but that assignment stands between a command that imports this package and a
// nil function call, so it is held here: every hook is set once server is
// linked in, New builds a *server.Server, and the two accessors answer for it.
func TestTheServerPackageRegistersEveryHook(t *testing.T) {
	if serverhook.New == nil || serverhook.Traffic == nil || serverhook.ClientMetrics == nil {
		t.Fatalf("a hook is not registered: New %v, Traffic %v, ClientMetrics %v",
			serverhook.New != nil, serverhook.Traffic != nil, serverhook.ClientMetrics != nil)
	}

	cfg := config.Default()
	cfg.DataDir, cfg.ListenAddr = t.TempDir(), "127.0.0.1:0"
	built, err := serverhook.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv, ok := built.(*server.Server)
	if !ok {
		t.Fatalf("New built a %T, want a *server.Server", built)
	}
	defer srv.Close()
	// The default config has a console, and the console is what reads both.
	if serverhook.Traffic(srv) == nil {
		t.Error("a server with a console has no request recorder")
	}
	if serverhook.ClientMetrics(srv) == nil {
		t.Error("a server with a console and client telemetry on has no metrics store")
	}

	cfg.DataDir, cfg.Console = t.TempDir(), false
	built, err = serverhook.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	quiet := built.(*server.Server)
	defer quiet.Close()
	if serverhook.Traffic(quiet) != nil || serverhook.ClientMetrics(quiet) != nil {
		t.Error("a server with no console keeps a recorder or a metrics store nobody reads")
	}
}
