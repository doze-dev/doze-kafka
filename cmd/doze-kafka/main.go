// Command doze-kafka runs the single-node, disk-backed Kafka-protocol broker.
//
// It is a local-development fixture: it speaks enough of the real Kafka wire
// protocol that unmodified clients (franz-go, kcat, Sarama, the Java client)
// connect and work, stores data on disk, and survives restarts — while
// deliberately omitting every distributed-systems concern (replication, KRaft).
// See docs/limitations.md for the boundary.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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

func main() { os.Exit(realMain(os.Args[1:], os.Stdout, os.Stderr)) }

// Exit codes: 0 success, 1 the broker failed while running, 2 it was started wrong
// (a bad flag, a bad config file, an invalid setting).
const (
	exitRuntime = 1
	exitUsage   = 2
)

// realMain is main with its inputs and outputs passed in, so the command line can
// be tested without a process.
func realMain(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		switch args[0] {
		case "version":
			printVersion(stdout)
			return 0
		case "help":
			fs, _ := newFlagSet(&config.Config{})
			printHelp(stdout, fs)
			return 0
		case "dns-setup":
			// The one-time machine setup for the shared .doze zone. It lives in
			// every doze binary because the setup belongs to the zone rather than
			// to any one of them: whichever you installed first can do it.
			return runDNSSetup(args[1:])
		case "config":
			return runConfig(args[1:], stdout, stderr)
		default:
			// A flag typed without its dashes is the likeliest mistake.
			d := config.Default()
			if fs, _ := newFlagSet(&d); fs.Lookup(strings.SplitN(args[0], "=", 2)[0]) != nil {
				fmt.Fprintf(stderr, "doze-kafka: unexpected argument %q — did you mean --%s?\n", args[0], args[0])
				return exitUsage
			}
			fmt.Fprintf(stderr, "doze-kafka: unknown command %q — the commands are version, config, dns-setup and help (doze-kafka --help)\n", args[0])
			return exitUsage
		}
	}

	st, err := loadConfig(args)
	switch {
	case errors.Is(err, errHelp):
		d := config.Default()
		fs, _ := newFlagSet(&d)
		printHelp(stdout, fs)
		return 0
	case errors.Is(err, errShowVersion):
		printVersion(stdout)
		return 0
	case err != nil:
		return usageError(stderr, err)
	}

	logger := newLogger(st.cfg, stderr)
	for _, n := range st.notices {
		logger.Warn(n)
	}
	// version is doze-kafka's own; kafka_compat is the emulated Kafka major.
	logger.Info("doze-kafka", "version", version, "kafka_compat", st.cfg.Version)
	if st.configFile != "" {
		logger.Info("loaded config file", "path", st.configFile)
	}

	if st.cfg.PprofAddr != "" {
		go func() {
			logger.Info("pprof listening", "addr", st.cfg.PprofAddr)
			if err := http.ListenAndServe(st.cfg.PprofAddr, nil); err != nil { //nolint:gosec // debug-only, local
				logger.Warn("pprof server stopped", "err", err)
			}
		}()
	}

	if err := run(st, logger); err != nil {
		logger.Error("fatal", "err", err)
		return exitRuntime
	}
	return 0
}

// printVersion prints doze-kafka's own version, deliberately prefixed with the
// name (and the Kafka range it speaks) so it isn't mistaken for a Kafka version.
func printVersion(w io.Writer) {
	fmt.Fprintf(w, "doze-kafka %s — speaks Kafka 1–4 (select with --kafka-version)\n", version)
}

// usageError reports a configuration mistake: one line, always the same shape,
// always the same exit code, before anything has started.
func usageError(w io.Writer, err error) int {
	fmt.Fprintf(w, "doze-kafka: %s\n", strings.TrimPrefix(err.Error(), "config: "))
	if !strings.HasPrefix(err.Error(), "config:") {
		fmt.Fprintln(w, "run doze-kafka --help for the flags")
	}
	return exitUsage
}

// runConfig is `doze-kafka config print`: the effective configuration (defaults,
// then the config file, then flags) as TOML, a ready-to-edit starting point. It
// validates first, so what it prints is something the broker would accept.
func runConfig(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "print" {
		fmt.Fprintln(stderr, "usage: doze-kafka config print [flags]")
		return exitUsage
	}
	st, err := loadConfig(args[1:])
	if err != nil {
		if errors.Is(err, errHelp) {
			d := config.Default()
			fs, _ := newFlagSet(&d)
			printHelp(stdout, fs)
			return 0
		}
		return usageError(stderr, err)
	}
	if err := config.WriteTOML(stdout, st.cfg); err != nil {
		fmt.Fprintln(stderr, "doze-kafka: config print:", err)
		return exitRuntime
	}
	return 0
}

// newLogger builds the process logger from --log-level and --log-format.
func newLogger(cfg config.Config, w io.Writer) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	if cfg.LogFormat == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
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
	if cfg.Console && cfg.AuthEnabled() {
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
	var z *zone
	if cfg.Zone {
		z = joinZone(ctx, logger)
		defer z.close()
	}
	if addr := z.brokerURL(logger, bound); addr != "" {
		logger.Info("broker also reachable by name", "addr", addr)
	}

	if cfg.Console {
		closeConsole, err := startConsole(ctx, st, cfg, bound, logger, consoleUser, consolePass, consoleMech)
		if err != nil {
			return fmt.Errorf("console: %w", err)
		}
		defer closeConsole()
		if u := z.routeConsole(logger, cfg.ConsoleAddr); u != "" {
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
		AllowedHosts: consoleHosts(cfg),
	})
	if err != nil {
		return nil, err
	}

	httpSrv := &http.Server{Addr: cfg.ConsoleAddr, Handler: con}
	go func() {
		logger.Info("console listening", "addr", cfg.ConsoleAddr)
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

// consoleHosts is every extra host name the console should answer to: whatever
// the operator listed, plus the name in --console-addr when it is a name rather
// than an address (choosing to bind to "devbox.local" means expecting to be
// reached as it).
func consoleHosts(cfg config.Config) []string {
	hosts := append([]string(nil), cfg.ConsoleAllowHosts...)
	if h, _, err := net.SplitHostPort(cfg.ConsoleAddr); err == nil && h != "" && net.ParseIP(h) == nil {
		hosts = append(hosts, h)
	}
	return hosts
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
