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
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof on the default mux; only served if --pprof-addr is set.
	"os"
	"os/signal"
	"strings"
	"syscall"

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

	if err := run(st.cfg, logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// startup holds everything resolved from the command line before the broker runs.
type startup struct {
	cfg        config.Config
	pprofAddr  string
	configFile string // the config file actually loaded, or "" if none.
}

// loadConfig resolves the effective configuration with flags > file > defaults
// precedence. It parses once to discover --config (falling back to
// ./doze-kafka.toml if present), overlays that file onto the defaults, then parses
// the flags again on top so any flag still wins for a one-off override.
func loadConfig(args []string) (startup, error) {
	probe := config.Default()
	configPath, _ := parseFlags(args, &probe)
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
	_, pprofAddr := parseFlags(args, &c)
	return startup{cfg: c, pprofAddr: pprofAddr, configFile: configPath}, nil
}

// parseFlags binds the broker flags onto dst and parses args, returning the
// --config path and --pprof-addr value. Because the flags' defaults are dst's
// current field values, keys already set from a config file survive unless the
// corresponding flag is explicitly passed.
func parseFlags(args []string, dst *config.Config) (configPath, pprofAddr string) {
	fs := flag.NewFlagSet("doze-kafka", flag.ExitOnError)
	cp := fs.String("config", "", "path to a TOML config file (default: ./doze-kafka.toml if present)")
	pp := fs.String("pprof-addr", "", "if set, serve net/http/pprof here (goroutine/heap profiling for soak/debug)")
	registerFlags(fs, dst)
	fs.Parse(args) //nolint:errcheck // flag.ExitOnError exits on a parse error.
	return *cp, *pp
}

// run builds the broker via the shared server package and serves until
// interrupted, so the CLI and the embed API can't drift.
func run(cfg config.Config, logger *slog.Logger) error {
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

	if err := srv.Serve(ctx); err != nil {
		return err
	}
	// Serve has returned: accepting stopped and connections drained; storage was
	// flushed and closed on the way out.
	logger.Info("shutting down: flushing logs to disk")
	return nil
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
