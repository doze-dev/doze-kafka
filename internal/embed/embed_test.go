package embed_test

import (
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/embed"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
	"github.com/doze-dev/doze-kafka/server"
)

// NewFromConfig hands back a server built from the config it was given: the
// data directory is the one named, and the server can be closed.
func TestNewFromConfigBuildsAServerFromTheConfig(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir, cfg.ListenAddr, cfg.Console = t.TempDir(), "127.0.0.1:0", false
	srv, err := embed.NewFromConfig(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if srv == nil {
		t.Fatal("no error and no server")
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A config that cannot be served is an error and no server: the error is the
// builder's own, not a nil dereference on the way out.
func TestNewFromConfigReturnsTheBuildersError(t *testing.T) {
	cfg := config.Default()
	// A data directory that is a file: nothing can be opened under it.
	cfg.DataDir, cfg.ListenAddr, cfg.Console = t.TempDir()+"/not-a-dir", "127.0.0.1:0", false
	if err := writeFile(cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	srv, err := embed.NewFromConfig(cfg, nil)
	if err == nil {
		srv.Close()
		t.Fatal("a data directory that is a file must be an error")
	}
	if srv != nil {
		t.Errorf("an error came with a server: %v", srv)
	}
	if !strings.Contains(err.Error(), "not-a-dir") {
		t.Errorf("the error does not name the directory: %v", err)
	}
}

// NewWithConsole builds a broker that keeps what a console reads (the request
// recorder and the client-metrics store); server.New keeps neither. A required
// option missing is the builder's error, and no server.
func TestNewWithConsoleKeepsWhatTheConsoleReads(t *testing.T) {
	srv, err := embed.NewWithConsole(server.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if serverhook.Traffic(srv) == nil || serverhook.ClientMetrics(srv) == nil {
		t.Errorf("a broker built for a console keeps no recorder (%v) or no metrics store (%v)",
			serverhook.Traffic(srv) != nil, serverhook.ClientMetrics(srv) != nil)
	}
	plain, err := server.New(server.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if serverhook.Traffic(plain) != nil {
		t.Error("server.New keeps a recorder; the comparison above proves nothing")
	}
	if s, err := embed.NewWithConsole(server.Options{}); err == nil {
		s.Close()
		t.Error("no DataDir must be an error")
	}
}
