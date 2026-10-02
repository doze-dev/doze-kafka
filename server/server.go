// Package server is doze-kafka's embeddable API: construct a broker from a
// plain options struct and serve it, either on its own TCP listener or on a
// listener the caller provides (a unix socket, for the doze proxy). It wraps
// exactly the wiring cmd/doze-kafka does — so the CLI and embedders can't
// drift — while keeping internal/ internal.
//
//	srv, err := server.New(server.Options{DataDir: "./data", Version: 4})
//	if err != nil { ... }
//	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
//	defer stop()
//	srv.Serve(ctx) // flushes logs + closes storage on return
package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/doze-dev/doze-kafka/internal/broker"
	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// Options configures an embedded broker. Zero values pick doze-kafka's
// defaults (see config.Default); only DataDir is effectively required.
type Options struct {
	// Version is the Kafka major to emulate (1–4); 0 uses the default.
	Version int

	// DataDir holds partition logs. Required.
	DataDir string
	// MetaDB is the metadata database path; empty defaults to
	// <DataDir>/meta.db so a single DataDir is self-contained.
	MetaDB string

	// ListenAddr is the host:port to bind when ServeListener is not used;
	// empty binds 127.0.0.1 on a free port (see Addr).
	ListenAddr string
	// AdvertisedHost/Port are what clients are told to dial in Metadata —
	// set AdvertisedHost when the broker sits behind a proxy so clients reach
	// the address they can actually connect to.
	AdvertisedHost string
	AdvertisedPort int

	// Topic behavior.
	AutoCreateTopics  *bool // nil = default (true)
	DefaultPartitions int   // 0 = default (1)

	// Retention (0 disables the bound).
	RetentionBytes int64
	RetentionMs    int64

	// FsyncInterval is the coarse fsync cadence; 0 uses the default, a
	// negative value disables periodic fsync.
	FsyncInterval time.Duration

	// Auth (all optional). SASL and TLS are independent and composable.
	SASLMechanisms []string
	Credentials    map[string]string
	TLSConfig      *tls.Config

	// Logger; nil discards.
	Logger *slog.Logger
}

// Server is an embedded broker. Construct with New, then Serve.
type Server struct {
	cfg   config.Config
	log   *slog.Logger
	meta  meta.Store
	logs  *log.Manager
	srv   *protocol.Server
	b     *broker.Broker
	extLn net.Listener

	mu    sync.Mutex
	bound net.Addr // set by Bind.
}

// New validates the options and opens storage + broker + router. It does not
// bind a listener yet — call Serve (or Bind) to start accepting.
func New(opts Options) (*Server, error) {
	cfg := config.Default()
	if opts.Version != 0 {
		cfg.Version = opts.Version
	}
	if opts.DataDir != "" {
		cfg.DataDir = opts.DataDir
	}
	cfg.MetaDB = opts.MetaDB
	if cfg.MetaDB == "" {
		cfg.MetaDB = filepath.Join(cfg.DataDir, "meta.db")
	}
	// An embedder asked for no particular address; the CLI default (the :9092
	// wildcard) is the wrong answer for a library, which would fight over a fixed
	// port with every other test run and listen on every interface. Loopback on
	// an ephemeral port is always free and never exposed — Addr reports it.
	cfg.ListenAddr = "127.0.0.1:0"
	if opts.ListenAddr != "" {
		cfg.ListenAddr = opts.ListenAddr
	}
	if opts.AdvertisedHost != "" {
		cfg.AdvertisedHost = opts.AdvertisedHost
	}
	cfg.AdvertisedPort = opts.AdvertisedPort
	if opts.AutoCreateTopics != nil {
		cfg.AutoCreateTopics = *opts.AutoCreateTopics
	}
	if opts.DefaultPartitions != 0 {
		cfg.DefaultPartitions = opts.DefaultPartitions
	}
	cfg.RetentionBytes = opts.RetentionBytes
	cfg.RetentionMs = opts.RetentionMs
	if opts.FsyncInterval != 0 {
		cfg.FsyncInterval = opts.FsyncInterval
		if opts.FsyncInterval < 0 {
			cfg.FsyncInterval = 0
		}
	}
	cfg.SASLMechanisms = opts.SASLMechanisms
	cfg.Credentials = opts.Credentials

	s, err := NewFromConfig(cfg, opts.Logger)
	if err != nil {
		return nil, err
	}
	// A caller-supplied *tls.Config (rather than cert file paths) is the one
	// thing NewFromConfig can't see, so apply it here.
	if opts.TLSConfig != nil {
		s.srv.UseTLS(opts.TLSConfig)
	}
	return s, nil
}

// NewFromConfig builds a Server from an already-resolved config.Config — the
// seam cmd/doze-kafka uses so the CLI and the embed API share one wiring path.
// (config.Config is an internal type, so only in-module callers can use this;
// external embedders use New(Options).)
func NewFromConfig(cfg config.Config, logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// bbolt creates the file but not its directory, and the log manager creates
	// the data dir only after the metadata store is open — so a fresh data dir
	// failed with "no such file or directory", including the example in this
	// package's doc comment.
	for _, dir := range []string{cfg.DataDir, filepath.Dir(cfg.MetaDB)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create %s: %w", dir, err)
		}
	}
	metaStore, err := meta.Open(cfg.MetaDB)
	if err != nil {
		return nil, err
	}
	logs, err := log.OpenManager(cfg.DataDir, log.ManagerOptions{
		SegmentBytes:       cfg.SegmentBytes,
		IndexIntervalBytes: cfg.IndexIntervalBytes,
		FsyncInterval:      cfg.FsyncInterval,
		Retention:          cfg.Retention(),
		Logger:             logger,
	})
	if err != nil {
		metaStore.Close()
		return nil, err
	}
	b, err := broker.New(cfg, logger, logs, metaStore)
	if err != nil {
		logs.Close()
		metaStore.Close()
		return nil, err
	}
	router := protocol.NewRouter(logger)
	b.Register(router)

	srv := protocol.NewServer(cfg.ListenAddr, router, logger)
	if cfg.TLSEnabled() {
		tlsCfg, err := cfg.TLSConfig()
		if err != nil {
			logs.Close()
			metaStore.Close()
			return nil, err
		}
		srv.UseTLS(tlsCfg)
	}
	if cfg.AuthEnabled() {
		srv.RequireAuth()
	}
	return &Server{cfg: cfg, log: logger, meta: metaStore, logs: logs, srv: srv, b: b}, nil
}

// Bind opens the TCP listener without accepting and returns the bound address
// (useful with a ":0" ListenAddr). Ignored when ServeListener is used.
func (s *Server) Bind(ctx context.Context) (net.Addr, error) {
	addr, err := s.srv.Bind(ctx)
	if err != nil {
		return nil, err
	}
	s.b.SetBoundAddr(addr)
	s.mu.Lock()
	s.bound = addr
	s.mu.Unlock()
	return addr, nil
}

// Addr is the address the listener is bound to, or nil before Bind (or Serve)
// has run. Metadata advertises this port, so a client dialling it is told to
// stay on it.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bound
}

// Serve accepts connections until ctx is cancelled, drains in-flight
// connections, then flushes partition logs and closes the metadata store.
func (s *Server) Serve(ctx context.Context) (err error) {
	defer s.closeStorage(&err)
	if s.extLn != nil {
		s.srv.UseListener(s.extLn)
	} else if s.Addr() == nil {
		// Bind here rather than letting protocol.Server do it, so the broker
		// learns the real port before the first client connects.
		if _, err := s.Bind(ctx); err != nil {
			return err
		}
	}
	return s.srv.Serve(ctx)
}

// ServeListener is Serve on a caller-provided listener — the doze module passes
// a unix-socket listener the proxy splices TCP clients onto. Kafka framing is
// transport-agnostic, so this works unmodified.
func (s *Server) ServeListener(ctx context.Context, ln net.Listener) (err error) {
	s.srv.UseListener(ln)
	defer s.closeStorage(&err)
	return s.srv.Serve(ctx)
}

// closeStorage flushes and closes the partition logs and the metadata store, and
// folds a failure into the Serve result. A shutdown whose final fsync failed has
// not made the data durable, and exiting 0 over that hid it from every script
// that checks the exit status.
func (s *Server) closeStorage(err *error) {
	if cerr := s.meta.Close(); cerr != nil && *err == nil {
		*err = fmt.Errorf("close metadata store: %w", cerr)
	}
	if cerr := s.logs.Close(); cerr != nil && *err == nil {
		*err = fmt.Errorf("flush partition logs on shutdown: %w", cerr)
	}
}

// Config returns the effective configuration (read-only view).
func (s *Server) Config() config.Config { return s.cfg }
