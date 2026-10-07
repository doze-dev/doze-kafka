// Package server is doze-kafka's embeddable API: construct a broker from a
// plain options struct and serve it, either on its own TCP listener or on a
// listener the caller provides (a unix socket, for the doze proxy). It wraps
// exactly the wiring cmd/doze-kafka does — so the CLI and embedders can't
// drift — while keeping internal/ internal.
//
// A broker for a test:
//
//	srv, err := server.New(server.Options{DataDir: t.TempDir()})
//	if err != nil { ... }
//	ctx, stop := context.WithCancel(context.Background())
//	done := make(chan error, 1)
//	go func() { done <- srv.Serve(ctx) }()
//	<-srv.Ready()                       // the listener is bound
//	brokers := srv.Addr().String()      // 127.0.0.1:<free port>
//	defer func() { stop(); <-done }()   // flushes logs and closes storage
//
// With no ListenAddr the broker binds loopback on a free port, so parallel tests
// never collide; Addr reports where. See docs/embedding.md.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.etcd.io/bbolt"

	"github.com/doze-dev/doze-kafka/internal/broker"
	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// Options configures an embedded broker. Zero values pick doze-kafka's
// defaults; only DataDir must be set.
type Options struct {
	// Version is the Kafka major to emulate (1–4); 0 uses the default.
	Version int

	// DataDir holds partition logs, and by default the metadata database.
	// Required: New fails without it rather than writing to ./data in whatever
	// directory the calling program happens to be started from.
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

	// Storage tuning. Zero uses the default (128 MiB segments, a sparse index
	// entry per 4 KiB). Tests that exercise segment rolling or retention set
	// SegmentBytes small.
	SegmentBytes       int64
	IndexIntervalBytes int64

	// FsyncInterval is the coarse fsync cadence for active segments. An options
	// struct's zero value means "the default" (500ms), so — unlike the CLI's
	// --fsync-interval 0 — zero does not disable it; a negative value does.
	// acks=all always fsyncs before acknowledging, whatever this is.
	FsyncInterval time.Duration

	// ConsumerSessionTimeout is how long a consumer group member may go without a
	// heartbeat before it is evicted; 0 uses the default (45s). Tests of failure
	// handling set it low.
	ConsumerSessionTimeout time.Duration

	// Auth (all optional). SASL and TLS are independent and composable.
	SASLMechanisms []string
	Credentials    map[string]string
	TLSConfig      *tls.Config

	// Logger; nil discards.
	Logger *slog.Logger
}

// Server is an embedded broker. Construct with New, then Serve.
type Server struct {
	cfg  config.Config
	log  *slog.Logger
	meta meta.Store
	logs *log.Manager
	srv  *protocol.Server
	b    *broker.Broker
	tls  bool // the listener terminates TLS, from cert files or Options.TLSConfig.

	mu      sync.Mutex
	bound   net.Addr // set by Bind.
	claimed bool     // Serve or ServeListener has been called.
	closed  bool     // Close has been called.

	dirLock io.Closer // held for the life of the server: one broker per data directory.

	closeOnce sync.Once
	closeErr  error

	ready     chan struct{} // closed once the listener is bound, or Serve has returned.
	readyOnce sync.Once
}

// New validates the options and opens storage + broker + router. It does not
// bind a listener yet — call Serve (or Bind) to start accepting.
func New(opts Options) (*Server, error) {
	if opts.DataDir == "" {
		return nil, errors.New("server: Options.DataDir is required (a test can use t.TempDir())")
	}
	cfg := config.Default()
	if opts.Version != 0 {
		cfg.Version = opts.Version
	}
	cfg.DataDir = opts.DataDir
	cfg.MetaDB = opts.MetaDB // empty is resolved to <DataDir>/meta.db by NewFromConfig.
	// An embedder asked for no particular address; the CLI default (port 9092) is
	// the wrong answer for a library, which would fight over a fixed port with
	// every other test run. Loopback on an ephemeral port is always free and
	// never exposed — Addr reports it.
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
	if opts.SegmentBytes != 0 {
		cfg.SegmentBytes = opts.SegmentBytes
	}
	if opts.IndexIntervalBytes != 0 {
		cfg.IndexIntervalBytes = opts.IndexIntervalBytes
	}
	if opts.ConsumerSessionTimeout != 0 {
		cfg.ConsumerSessionTimeout = opts.ConsumerSessionTimeout
	}
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
		s.tls = true
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
	// A config built without a metadata path keeps it with the data: the same
	// default the CLI resolves, so a data directory is self-contained.
	if cfg.MetaDB == "" {
		cfg.MetaDB = filepath.Join(cfg.DataDir, config.MetaDBFile)
	}
	cfg.ResolveAdvertisedHost()
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
	// One broker per data directory. The metadata database is locked by whoever
	// opens it, but it can live elsewhere (--meta-db), and two brokers with
	// different databases over one data directory would write the same log files.
	dirLock, err := lockDataDir(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	metaStore, err := meta.Open(cfg.MetaDB)
	if err != nil {
		dirLock.Close()
		return nil, err
	}
	// Topics with a segment size or retention of their own, in place before the
	// log manager's first retention sweep.
	registered, err := metaStore.ListTopics()
	if err != nil {
		metaStore.Close()
		dirLock.Close()
		return nil, err
	}
	topicConfigs := make(map[string]map[string]string, len(registered))
	for _, t := range registered {
		topicConfigs[t.Name] = t.Config
	}
	logs, err := log.OpenManager(cfg.DataDir, log.ManagerOptions{
		TopicLimits:        broker.InitialTopicLimits(cfg, topicConfigs),
		SegmentBytes:       cfg.SegmentBytes,
		IndexIntervalBytes: cfg.IndexIntervalBytes,
		FsyncInterval:      cfg.FsyncInterval,
		Retention:          broker.RetentionPolicy(cfg),
		Logger:             logger,
	})
	if err != nil {
		metaStore.Close()
		dirLock.Close()
		return nil, err
	}
	if err := reconcileDisk(cfg, logger, logs, metaStore); err != nil {
		logs.Close()
		metaStore.Close()
		return nil, err
	}
	b, err := broker.New(cfg, logger, logs, metaStore)
	if err != nil {
		logs.Close()
		metaStore.Close()
		return nil, err
	}
	router := protocol.NewRouter()
	b.Register(router)
	b.WarnIfACLs()

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
	return &Server{cfg: cfg, log: logger, meta: metaStore, logs: logs, srv: srv, b: b, tls: cfg.TLSEnabled(), dirLock: dirLock, ready: make(chan struct{})}, nil
}

// reconcileDisk makes the data directory and the registry agree at start-up.
//
// Every registered topic gets its partition directories (a partition is only
// opened if its directory exists, so one that was never written to must still
// have one). Directories whose topic the registry does not know are reported, not
// touched: they are what is left when the metadata database was lost or the broker
// was pointed at a different one, and saying nothing made that look like an empty
// broker.
func reconcileDisk(cfg config.Config, logger *slog.Logger, logs *log.Manager, store meta.Store) error {
	topics, err := store.ListTopics()
	if err != nil {
		return err
	}
	counts := make(map[string]int32, len(topics))
	for _, t := range topics {
		counts[t.Name] = t.Partitions
		if err := logs.EnsurePartitionDirs(t.Name, t.Partitions); err != nil {
			logger.Warn("could not create partition directories", "topic", t.Name, "err", err)
		}
	}
	stray := logs.StrayDirs(func(topic string) (int32, bool) { n, ok := counts[topic]; return n, ok })
	if len(stray) > 0 {
		sample := stray
		if len(sample) > 5 {
			sample = sample[:5]
		}
		logger.Warn("the data directory holds partition data for topics the metadata database does not know; "+
			"if the database was moved, point at it with --meta-db (the data is left untouched until a topic of the same name is created)",
			"data_dir", cfg.DataDir, "meta_db", cfg.MetaDB, "directories", len(stray), "examples", strings.Join(sample, ", "))
	}
	return nil
}

// Bind opens the TCP listener without accepting and returns the bound address
// (useful with a ":0" ListenAddr). Serve does this itself if it was not called;
// it is not used with ServeListener.
func (s *Server) Bind(ctx context.Context) (net.Addr, error) {
	addr, err := s.srv.Bind(ctx)
	if err != nil {
		return nil, err
	}
	s.b.SetBoundAddr(addr)
	s.mu.Lock()
	s.bound = addr
	s.mu.Unlock()
	s.markReady()
	return addr, nil
}

func (s *Server) markReady() { s.readyOnce.Do(func() { close(s.ready) }) }

// Ready is closed once the broker's listener is bound, so Addr is valid and
// clients can connect: a goroutine running Serve can be waited on without a
// sleep or a retry loop. It is also closed if Serve returns first (a failed bind,
// an immediate cancel), so waiting on it never hangs; check Addr for nil.
func (s *Server) Ready() <-chan struct{} { return s.ready }

// AlsoListen opens another TCP listener, on addr, that serves this same broker —
// for a second address the broker should answer on (the shared .doze zone's
// kafka.doze, say) when the main one is narrower than the wildcard. Call it
// before Serve: once Serve has started it returns an error, because Serve is what
// starts accepting on each listener. Clients are still told the main listener's
// port.
func (s *Server) AlsoListen(ctx context.Context, addr string) (net.Addr, error) {
	return s.srv.AlsoListen(ctx, addr)
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
// connections, then flushes partition logs and closes the metadata store. A
// Server is served once: a second call (or a call after Close) returns an error
// and leaves the first undisturbed.
func (s *Server) Serve(ctx context.Context) (err error) {
	if err := s.claim(); err != nil {
		return err
	}
	defer s.closeStorage(&err)
	defer s.markReady()
	if s.Addr() == nil {
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
// transport-agnostic, so this works unmodified. With a TCP listener, clients are
// told its port (unless AdvertisedPort says otherwise), as with Serve.
func (s *Server) ServeListener(ctx context.Context, ln net.Listener) (err error) {
	if err := s.claim(); err != nil {
		return err
	}
	defer s.closeStorage(&err)
	defer s.markReady()
	s.srv.UseListener(ln)
	if _, ok := ln.Addr().(*net.TCPAddr); ok {
		// Metadata used to advertise the default port (9092) for a listener the
		// broker did not bind itself, so a client that bootstrapped fine was then
		// sent to a port nothing was listening on.
		s.b.SetBoundAddr(ln.Addr())
	}
	s.mu.Lock()
	s.bound = ln.Addr()
	s.mu.Unlock()
	s.markReady()
	return s.srv.Serve(ctx)
}

// claim marks the server as being served, once.
func (s *Server) claim() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.closed:
		return errors.New("server: Serve called after Close")
	case s.claimed:
		return errors.New("server: already serving; a Server is served once")
	}
	s.claimed = true
	return nil
}

// closeStorage flushes and closes the partition logs and the metadata store, and
// folds a failure into the Serve result. A shutdown whose final fsync failed has
// not made the data durable, and exiting 0 over that hid it from every script
// that checks the exit status. It runs once, whoever gets there first.
func (s *Server) closeStorage(err *error) {
	s.closeOnce.Do(func() {
		s.b.Stop()
		if cerr := s.meta.Close(); cerr != nil {
			s.closeErr = fmt.Errorf("close metadata store: %w", cerr)
		}
		if cerr := s.logs.Close(); cerr != nil && s.closeErr == nil {
			s.closeErr = fmt.Errorf("flush partition logs on shutdown: %w", cerr)
		}
		if s.dirLock != nil {
			s.dirLock.Close()
		}
	})
	if *err == nil {
		*err = s.closeErr
	}
}

// Close releases everything a server holds that was built but never served — a
// start-up step failed after New or Bind succeeded, say: the listeners it bound,
// its storage and the lock on its data directory. It is safe to call more than
// once and after Serve (which closes storage itself). To stop a server that is
// serving, cancel the context passed to Serve instead.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	// The listeners first: Bind opens one, and it used to stay bound (and
	// accepting into a backlog nobody served) for the life of the process.
	lerr := s.srv.Close()
	var err error
	s.closeStorage(&err)
	if err == nil {
		err = lerr
	}
	return err
}

// Config is the effective configuration of a Server, as far as an embedder needs
// to know it.
type Config struct {
	KafkaVersion   int    // the Kafka major being emulated.
	ListenAddr     string // the address asked for; Addr has the one actually bound.
	AdvertisedHost string // what clients are told to dial.
	AdvertisedPort int    // 0 means the bound port.
	DataDir        string
	MetaDB         string
	SASL           bool // SASL authentication is required.
	TLS            bool // the listener terminates TLS.
}

// Config returns the effective configuration.
func (s *Server) Config() Config {
	return Config{
		KafkaVersion: s.cfg.Version, ListenAddr: s.cfg.ListenAddr,
		AdvertisedHost: s.cfg.AdvertisedHost, AdvertisedPort: s.cfg.AdvertisedPort,
		DataDir: s.cfg.DataDir, MetaDB: s.cfg.MetaDB,
		SASL: s.cfg.AuthEnabled(), TLS: s.tls,
	}
}

// dataDirLockFile is the lock taken inside the data directory.
const dataDirLockFile = ".doze-kafka.lock"

// lockDataDir takes an exclusive lock on the data directory, or explains who is in
// the way. It is a small bbolt file because bbolt already carries a file lock that
// works on every platform this ships for, with a timeout.
func lockDataDir(dir string) (io.Closer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create %s: %w", dir, err)
	}
	db, err := bbolt.Open(filepath.Join(dir, dataDirLockFile), 0o600, &bbolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("data directory %s is in use by another doze-kafka (or its lock could not be taken: %w); "+
			"stop the other broker or choose another --data-dir", dir, err)
	}
	return db, nil
}
