// Command doze-kafka runs the single-node, disk-backed Kafka-protocol broker.
//
// It is a local-development fixture: it speaks enough of the real Kafka wire
// protocol that unmodified clients (franz-go, kcat, Sarama, the Java client)
// connect and work, stores data on disk, and survives restarts — while
// deliberately omitting every distributed-systems concern (replication, KRaft).
// See docs/limitations.md for the boundary.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
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
	"path/filepath"
	"strings"
	"syscall"
	"time"

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

	// Stop accepting, then let in-flight work drain, on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, st, logger, stderr); err != nil {
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
// interrupted, so the CLI and the embed API can't drift. out is where the
// start-up summary goes.
//
// Everything that can fail to start (the broker's bind, the console's bind) is
// done before anything is announced, so the summary is printed only once it is
// true, and a port clash is an error line rather than a "listening" that was a lie.
func run(ctx context.Context, st startup, logger *slog.Logger, out io.Writer) error {
	cfg := st.cfg

	// The console has no trusted-caller bypass to reach for (doze-kafka gates
	// every connection, loopback included, behind the same RequireAuth check
	// once SASL is configured) — so if auth is on, it needs a real credential
	// of its own. Provisioned here, before the server is built, so it lands
	// in cfg.Credentials before SASL verifiers are set up. The configuration was
	// validated before this point, so the user's own credentials were checked
	// first.
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
	// Until Serve takes over (and closes storage on its way out), a failure below
	// must release what NewFromConfig opened.
	serving := false
	defer func() {
		if !serving {
			srv.Close()
		}
	}()

	// Bind before anything advertises an address. With a ":0" listen address the
	// real port is not known until now, and both the zone and the console need
	// it; Serve picks up the listener Bind opened.
	bound, err := srv.Bind(ctx)
	if err != nil {
		return fmt.Errorf("listen on %s: %w (is another broker running there? choose another with --listen)", cfg.ListenAddr, err)
	}

	// Join the shared .doze zone. Additive: the configured address above is the
	// contract, and every failure in here is a log line rather than a refusal
	// to start.
	var z *zone
	ready := readyInfo{version: version, kafka: cfg.Version, bootstrap: bound.String(), dataDir: absPath(cfg.DataDir),
		auth: authSummary(cfg), configFile: st.configFile}
	if cfg.Zone {
		z = joinZone(ctx, logger)
		defer z.close()
		ready.bootstrapName = z.reachBroker(ctx, logger, srv, bound)
	}

	if cfg.Console {
		closeConsole, consoleAddr, err := startConsole(ctx, cfg, bound, logger, consoleUser, consolePass, consoleMech)
		if err != nil {
			return err
		}
		defer closeConsole()
		ready.consoleURL = "http://" + consoleAddr
		ready.consoleName = z.routeConsole(logger, consoleAddr)
	}

	announce(out, logger, cfg, ready)
	serving = true
	if err := srv.Serve(ctx); err != nil {
		return err
	}
	// Serve has returned: accepting stopped and connections drained; storage was
	// flushed and closed on the way out.
	logger.Info("shutting down: flushing logs to disk")
	return nil
}

// readyInfo is what the start-up summary reports.
type readyInfo struct {
	version, bootstrap, bootstrapName string
	kafka                             int
	consoleURL, consoleName           string
	dataDir, auth, configFile         string
}

// authSummary is one phrase for how the listener is protected.
func authSummary(cfg config.Config) string {
	var parts []string
	if cfg.AuthEnabled() {
		parts = append(parts, "SASL "+strings.Join(cfg.SASLMechanisms, ", "))
	} else {
		parts = append(parts, "no auth")
	}
	if cfg.TLSEnabled() {
		parts = append(parts, "TLS")
	} else {
		parts = append(parts, "plaintext")
	}
	return strings.Join(parts, " · ")
}

// announce prints the one block a developer reads to learn how to connect: what to
// point a client at, where the console is, where the data lives. As text it is a
// few aligned lines; as JSON it is a single "ready" record; it stays quiet when
// the log level asked for quiet.
func announce(out io.Writer, logger *slog.Logger, cfg config.Config, r readyInfo) {
	if cfg.LogLevel == "warn" || cfg.LogLevel == "error" {
		return
	}
	if cfg.LogFormat == "json" {
		logger.Info("ready", "bootstrap", r.bootstrap, "bootstrap_name", r.bootstrapName, "console", r.consoleURL,
			"console_name", r.consoleName, "data_dir", r.dataDir, "auth", r.auth, "kafka_compat", r.kafka)
		return
	}
	line := func(label, value, alt string) {
		if value == "" {
			return
		}
		if alt != "" {
			value += "   (also " + alt + ")"
		}
		fmt.Fprintf(out, "  %-10s %s\n", label, value)
	}
	fmt.Fprintf(out, "\ndoze-kafka %s ready — emulating Kafka %d\n", r.version, r.kafka)
	line("Kafka", r.bootstrap, r.bootstrapName)
	line("Console", r.consoleURL, r.consoleName)
	line("Data", r.dataDir, "")
	line("Security", r.auth, "")
	if r.configFile != "" {
		line("Config", r.configFile, "")
	}
	fmt.Fprintln(out)
}

// absPath is p made absolute for display, or p itself if that fails.
func absPath(p string) string {
	if a, err := filepath.Abs(p); err == nil {
		return a
	}
	return p
}

// reachable reports whether something accepts a TCP connection at hostport. It is
// how a by-name address earns its place in the start-up summary: a name that does
// not resolve, or does not answer, is not worth advertising.
func reachable(hostport string) bool {
	c, err := net.DialTimeout("tcp", hostport, 400*time.Millisecond)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// consoleUserName is the SASL user the console logs in as.
const consoleUserName = "__console"

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
	user, pass = consoleUserName, hex.EncodeToString(buf)
	if _, taken := cfg.Credentials[user]; taken {
		// The console's password would have replaced the configured one without a
		// word, and that user could then no longer log in.
		return "", "", "", fmt.Errorf("the SASL user name %q is reserved for the web console; rename that user, or turn the console off with --console=false", user)
	}
	if cfg.Credentials == nil {
		cfg.Credentials = map[string]string{}
	}
	cfg.Credentials[user] = pass
	return user, pass, mech, nil
}

// startConsole binds the console's HTTP listener and dials the broker as a real
// Kafka client at its already-bound address. The bind happens here, before it
// returns, so a port that is already taken is reported as the error it is. It
// returns a func that shuts the console down (the caller defers it) and the
// address the listener actually got.
func startConsole(ctx context.Context, cfg config.Config, bound net.Addr, logger *slog.Logger, user, pass, mech string) (func(), string, error) {
	dialAddr, err := consoleDialAddr(cfg, bound)
	if err != nil {
		return nil, "", err
	}

	var tlsCfg *tls.Config
	if cfg.TLSEnabled() {
		serverTLS, err := cfg.TLSConfig()
		if err != nil {
			return nil, "", err
		}
		tlsCfg = consoleClientTLS(serverTLS)
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.ConsoleAddr)
	if err != nil {
		return nil, "", fmt.Errorf("console: listen on %s: %w (choose another with --console-addr, or turn the console off with --console=false)", cfg.ConsoleAddr, err)
	}

	con, err := console.New(console.Options{
		Addr: dialAddr, SASLUser: user, SASLPass: pass, SASLMechanism: mech, TLSConfig: tlsCfg,
		AllowedHosts: consoleHosts(cfg),
	})
	if err != nil {
		ln.Close()
		return nil, "", err
	}

	// /healthz answers 200 whenever the process is up, for scripts and compose
	// healthchecks that wait on the broker before starting an app. It sits outside
	// the console's Host check on purpose: it reveals nothing.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "ok")
	})
	mux.Handle("/", con)
	httpSrv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Warn("console server stopped", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		httpSrv.Close()
	}()

	return func() { con.Close() }, ln.Addr().String(), nil
}

// consoleClientTLS is the TLS configuration the console's own Kafka client uses to
// reach the broker in this process.
//
// The console used to be handed the broker's server configuration, which has a
// certificate to present and no roots to trust, so with TLS on every console page
// failed ("bad certificate") while the start-up summary still advertised it.
// Trusting the system roots would not help either: a development certificate is
// usually self-signed. The console is talking to its own broker, so it accepts
// exactly one certificate: the one that broker was started with.
func consoleClientTLS(server *tls.Config) *tls.Config {
	var own []byte
	if len(server.Certificates) > 0 && len(server.Certificates[0].Certificate) > 0 {
		own = server.Certificates[0].Certificate[0]
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		// Verification is not skipped; it is done below, by identity rather than
		// by chain and host name.
		InsecureSkipVerify: true, //nolint:gosec
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 || own == nil || !bytes.Equal(raw[0], own) {
				return errors.New("console: the broker presented a certificate other than its own")
			}
			return nil
		},
	}
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
