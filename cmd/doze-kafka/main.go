// Command doze-kafka runs the single-node, disk-backed Kafka-protocol broker.
//
// It is a local-development fixture: it speaks enough of the real Kafka wire
// protocol that unmodified clients (franz-go, kcat, Sarama, the Java client)
// connect and work, stores data on disk, and survives restarts — while
// deliberately omitting every distributed-systems concern (replication, KRaft,
// transactions). See docs/limitations.md for the boundary.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof on the default mux; only served if --pprof-addr is set.
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/doze-dev/doze-kafka/console"
	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/server"
)

// version is the build version, injected by the release tooling
// (-ldflags "-X main.version=..."). It defaults to "dev" for local builds.
var version = "dev"

func main() {
	// `doze-kafka version` prints doze-kafka's own version — deliberately prefixed
	// with the name (and the Kafka range it speaks) so it isn't mistaken for a Kafka
	// version. It's a subcommand rather than a flag because --version already selects
	// the emulated Kafka major.
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Printf("doze-kafka %s — speaks Kafka 1–4 (select with --version)\n", version)
		return
	}

	// `doze-kafka dns-setup` is the one-time machine setup for the shared .doze
	// zone. It lives in every doze binary because the setup belongs to the zone
	// rather than to any one of them — whichever you installed first can do it.
	if len(os.Args) > 1 && os.Args[1] == "dns-setup" {
		os.Exit(runDNSSetup(os.Args[2:]))
	}

	// `doze-kafka config print` writes the effective configuration (defaults, then
	// the config file, then flags) as TOML — a ready-to-edit starting point.
	if len(os.Args) > 1 && os.Args[1] == "config" {
		if len(os.Args) < 3 || os.Args[2] != "print" {
			fmt.Fprintln(os.Stderr, "usage: doze-kafka config print [flags]")
			os.Exit(2)
		}
		st, err := loadConfig(os.Args[3:])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := config.WriteTOML(os.Stdout, st.cfg); err != nil {
			fmt.Fprintln(os.Stderr, "config print:", err)
			os.Exit(1)
		}
		return
	}

	st, err := loadConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	// version is doze-kafka's own; kafka_compat is the emulated Kafka major.
	logger.Info("doze-kafka", "version", version, "kafka_compat", st.cfg.Version)
	if st.configFile != "" {
		logger.Info("loaded config file", "path", st.configFile)
	}

	if st.pprofAddr != "" {
		go func() {
			logger.Info("pprof listening", "addr", st.pprofAddr)
			if err := http.ListenAndServe(st.pprofAddr, nil); err != nil { //nolint:gosec // debug-only, local
				logger.Warn("pprof server stopped", "err", err)
			}
		}()
	}

	if err := run(st, logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// startup holds everything resolved from the command line before the broker runs.
type startup struct {
	cfg         config.Config
	pprofAddr   string
	configFile  string // the config file actually loaded, or "" if none.
	console     bool
	consoleAddr string
}

// loadConfig resolves the effective configuration with flags > file > defaults
// precedence. It parses once to discover --config (falling back to
// ./doze-kafka.toml if present), overlays that file onto the defaults, then parses
// the flags again on top so any flag still wins for a one-off override.
func loadConfig(args []string) (startup, error) {
	probe := config.Default()
	fl, _ := parseFlags(args, &probe)
	configPath := fl.config
	if configPath == "" {
		if _, err := os.Stat(config.DefaultConfigFile); err == nil {
			configPath = config.DefaultConfigFile
		}
	}

	c := config.Default()
	if configPath != "" {
		if err := config.LoadFile(configPath, &c); err != nil {
			return startup{}, err
		}
	}
	fl, err := parseFlags(args, &c)
	if err != nil {
		return startup{}, err
	}
	return startup{
		cfg: c, pprofAddr: fl.pprofAddr, configFile: configPath,
		console: fl.console, consoleAddr: fl.consoleAddr,
	}, nil
}

// cliFlags holds the flags that aren't part of config.Config itself.
type cliFlags struct {
	config      string
	pprofAddr   string
	console     bool
	consoleAddr string
}

// parseFlags binds the broker flags onto dst and parses args.
// Because the flags' defaults are dst's current field values, keys already
// set from a config file survive unless the corresponding flag is explicitly
// passed.
func parseFlags(args []string, dst *config.Config) (cliFlags, error) {
	fs := flag.NewFlagSet("doze-kafka", flag.ExitOnError)
	var fl cliFlags
	fs.StringVar(&fl.config, "config", "", "path to a TOML config file (default: ./doze-kafka.toml if present)")
	fs.StringVar(&fl.pprofAddr, "pprof-addr", "", "if set, serve net/http/pprof here (goroutine/heap profiling for soak/debug)")
	fs.BoolVar(&fl.console, "console", true, "serve the web console (topics, messages, consumer groups)")
	fs.StringVar(&fl.consoleAddr, "console-addr", "127.0.0.1:9093", "host:port for the web console — distinct from --listen, since Kafka's wire protocol can't share a port with HTTP")
	registerFlags(fs, dst)
	if err := fs.Parse(args); err != nil {
		return cliFlags{}, err
	}
	return fl, nil
}

// run builds the broker via the shared server package and serves until
// interrupted, so the CLI and the embed API can't drift.
func run(st startup, logger *slog.Logger) error {
	cfg := st.cfg

	// The console has no trusted-caller bypass to reach for (doze-kafka gates
	// every connection, loopback included, behind the same RequireAuth check
	// once SASL is configured) — so if auth is on, it needs a real credential
	// of its own. Provisioned here, before the server is built, so it lands
	// in cfg.Credentials before SASL verifiers are set up.
	var consoleUser, consolePass, consoleMech string
	if st.console && cfg.AuthEnabled() {
		var err error
		consoleUser, consolePass, consoleMech, err = provisionConsoleCredential(&cfg)
		if err != nil {
			return err
		}
	}

	srv, err := server.NewFromConfig(cfg, logger)
	if err != nil {
		return err
	}
	if cfg.TLSEnabled() {
		logger.Info("TLS enabled on listener", "cert", cfg.TLSCertFile)
	}
	if cfg.AuthEnabled() {
		logger.Info("SASL required", "mechanisms", cfg.SASLMechanisms)
	}

	// Stop accepting, then let in-flight work drain, on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Bind before anything advertises an address. With a ":0" listen address the
	// real port is not known until now, and both the zone and the console need
	// it; Serve picks up the listener Bind opened.
	bound, err := srv.Bind(ctx)
	if err != nil {
		return fmt.Errorf("bind: %w", err)
	}

	// Join the shared .doze zone. Additive: the configured address above is the
	// contract, and every failure in here is a log line rather than a refusal
	// to start.
	z := joinZone(ctx, logger)
	defer z.close()
	if addr := z.brokerURL(logger, bound); addr != "" {
		logger.Info("broker also reachable by name", "addr", addr)
	}

	if st.console {
		closeConsole, err := startConsole(ctx, st, cfg, bound, logger, consoleUser, consolePass, consoleMech)
		if err != nil {
			return fmt.Errorf("console: %w", err)
		}
		defer closeConsole()
		if u := z.routeConsole(logger, st.consoleAddr); u != "" {
			logger.Info("console also reachable by name", "url", u)
		}
	}

	if err := srv.Serve(ctx); err != nil {
		return err
	}
	// Serve has returned: accepting stopped and connections drained; storage was
	// flushed and closed on the way out.
	logger.Info("shutting down: flushing logs to disk")
	return nil
}

// provisionConsoleCredential generates a random password for a reserved
// "__console" SASL user and injects it into cfg.Credentials — a real
// credential, not a bypass, using whichever mechanism the broker already has
// enabled (PLAIN preferred for simplicity, else SCRAM). Never logged.
func provisionConsoleCredential(cfg *config.Config) (user, pass, mech string, err error) {
	buf := make([]byte, 18)
	if _, err := rand.Read(buf); err != nil {
		return "", "", "", fmt.Errorf("generate console credential: %w", err)
	}
	mech = ""
	for _, m := range cfg.SASLMechanisms {
		if m == "PLAIN" {
			mech = m
			break
		}
	}
	if mech == "" && len(cfg.SASLMechanisms) > 0 {
		mech = cfg.SASLMechanisms[0]
	}
	user, pass = "__console", hex.EncodeToString(buf)
	if cfg.Credentials == nil {
		cfg.Credentials = map[string]string{}
	}
	cfg.Credentials[user] = pass
	return user, pass, mech, nil
}

// startConsole binds a console HTTP listener on st.consoleAddr and dials the
// broker as a real Kafka client at its already-bound address. Returns a func
// that shuts the console down; the caller defers it.
func startConsole(ctx context.Context, st startup, cfg config.Config, bound net.Addr, logger *slog.Logger, user, pass, mech string) (func(), error) {
	dialAddr, err := consoleDialAddr(cfg, bound)
	if err != nil {
		return nil, err
	}

	var tlsCfg *tls.Config
	if cfg.TLSEnabled() {
		if tlsCfg, err = cfg.TLSConfig(); err != nil {
			return nil, err
		}
	}

	con, err := console.New(console.Options{
		Addr: dialAddr, SASLUser: user, SASLPass: pass, SASLMechanism: mech, TLSConfig: tlsCfg,
	})
	if err != nil {
		return nil, err
	}

	httpSrv := &http.Server{Addr: st.consoleAddr, Handler: con}
	go func() {
		logger.Info("console listening", "addr", st.consoleAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Warn("console server stopped", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		httpSrv.Close()
	}()

	return func() { con.Close() }, nil
}

// consoleDialAddr is the address the console's own Kafka client dials — the
// broker's real bound port, on cfg.AdvertisedHost (the same host doze-kafka
// tells every other client to use, so the console never guesses at a
// different answer than a real SDK would get from Metadata).
func consoleDialAddr(cfg config.Config, bound net.Addr) (string, error) {
	_, port, err := net.SplitHostPort(bound.String())
	if err != nil {
		return "", fmt.Errorf("console: parse bound addr %q: %w", bound.String(), err)
	}
	host := cfg.AdvertisedHost
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port), nil
}

// registerFlags binds command-line flags onto cfg, using its current values as
// defaults. See config.Default for the meaning of each field.
func registerFlags(fs *flag.FlagSet, cfg *config.Config) {
	fs.IntVar(&cfg.Version, "version", cfg.Version, "Kafka major version to emulate (1-4)")
	fs.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "host:port to listen on")
	fs.StringVar(&cfg.AdvertisedHost, "advertised-host", cfg.AdvertisedHost, "host advertised to clients in Metadata")
	fs.IntVar(&cfg.AdvertisedPort, "advertised-port", cfg.AdvertisedPort, "port advertised to clients (0 = derive from --listen)")
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "directory for partition logs")
	fs.StringVar(&cfg.MetaDB, "meta-db", cfg.MetaDB, "path to the metadata database file")
	fs.Int64Var(&cfg.SegmentBytes, "segment-bytes", cfg.SegmentBytes, "roll a segment once it exceeds this size")
	fs.Int64Var(&cfg.IndexIntervalBytes, "index-interval-bytes", cfg.IndexIntervalBytes, "add one sparse index entry per this many bytes of log")
	fs.Int64Var(&cfg.RetentionBytes, "retention-bytes", cfg.RetentionBytes, "delete old segments once a partition exceeds this size (0 = unbounded)")
	fs.Int64Var(&cfg.RetentionMs, "retention-ms", cfg.RetentionMs, "delete segments older than this many ms (0 = unbounded)")
	fs.DurationVar(&cfg.FsyncInterval, "fsync-interval", cfg.FsyncInterval, "coarse fsync cadence for active segments (0 = disable)")
	fs.DurationVar(&cfg.ConsumerSessionTimeout, "consumer-session-timeout", cfg.ConsumerSessionTimeout, "KIP-848 group session timeout before evicting a silent member")
	fs.BoolVar(&cfg.AutoCreateTopics, "auto-create-topics", cfg.AutoCreateTopics, "create unknown topics on first reference")
	fs.IntVar(&cfg.DefaultPartitions, "default-partitions", cfg.DefaultPartitions, "partition count for auto-created topics")
	fs.IntVar(&cfg.MaxPartitions, "max-partitions", cfg.MaxPartitions, "most partitions one topic may have (each is a directory and open files)")

	// Auth.
	if cfg.Credentials == nil {
		cfg.Credentials = map[string]string{}
	}
	fs.Var(stringSliceFlag{&cfg.SASLMechanisms}, "sasl-mechanisms", "comma-separated SASL mechanisms to enable (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)")
	fs.Var(credsFlag{cfg.Credentials}, "sasl-user", "SASL credential as user:password (repeatable)")
	fs.StringVar(&cfg.TLSCertFile, "tls-cert", cfg.TLSCertFile, "PEM certificate file (enables TLS together with --tls-key)")
	fs.StringVar(&cfg.TLSKeyFile, "tls-key", cfg.TLSKeyFile, "PEM private key file")
}

// stringSliceFlag collects a comma-separated (and/or repeated) flag into a slice.
type stringSliceFlag struct{ vals *[]string }

func (f stringSliceFlag) String() string {
	if f.vals == nil {
		return ""
	}
	return strings.Join(*f.vals, ",")
}

func (f stringSliceFlag) Set(s string) error {
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*f.vals = append(*f.vals, part)
		}
	}
	return nil
}

// credsFlag collects repeated user:password pairs into a map.
type credsFlag struct{ m map[string]string }

func (f credsFlag) String() string { return "" }

func (f credsFlag) Set(s string) error {
	user, pass, ok := strings.Cut(s, ":")
	if !ok || user == "" {
		return fmt.Errorf("expected user:password, got %q", s)
	}
	f.m[user] = pass
	return nil
}
