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
	"slices"
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

// shutdownSignals are the signals that stop the broker cleanly.
var shutdownSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP}

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
		case "doctor":
			return runDoctor(args[1:], stdout, stderr)
		case "env":
			return runEnv(args[1:], stdout, stderr)
		case "config":
			return runConfig(args[1:], stdout, stderr)
		default:
			// A flag typed without its dashes is the likeliest mistake.
			d := config.Default()
			if fs, _ := newFlagSet(&d); fs.Lookup(strings.SplitN(args[0], "=", 2)[0]) != nil {
				fmt.Fprintf(stderr, "doze-kafka: unexpected argument %q — did you mean --%s?\n", args[0], args[0])
				return exitUsage
			}
			fmt.Fprintf(stderr, "doze-kafka: unknown command %q — the commands are version, env, config, dns-setup, doctor and help (doze-kafka --help)\n", args[0])
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

	// Stop accepting, then let in-flight work drain, on SIGINT/SIGTERM — and on
	// SIGHUP, which is what closing the terminal sends. Unhandled, it killed the
	// broker with no flush and no clean close of its storage.
	ctx, stop := signal.NotifyContext(context.Background(), shutdownSignals...)
	defer stop()
	if err := run(ctx, st, logger, stdout); err != nil {
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
	switch {
	case errors.Is(err, errHelp):
		d := config.Default()
		fs, _ := newFlagSet(&d)
		printHelp(stdout, fs)
		return 0
	case errors.Is(err, errShowVersion):
		// It used to print the sentinel itself: "doze-kafka: show version".
		printVersion(stdout)
		return 0
	case err != nil:
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

	// Where this instance answers is decided BEFORE the broker is built, because
	// what Metadata tells clients to dial depends on where it ended up listening.
	//
	// The two modes are exclusive. Without --listen this instance is its .doze name
	// and nothing else; with --listen it is that address and nothing else: no name
	// is claimed, no DNS check runs, and the registry records nothing. One instance,
	// one way to reach it. See listen.go.
	z, err := prepareZone(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer z.close()

	bind, err := openListener(cfg, z, logger, net.DefaultResolver.LookupHost)
	if err != nil {
		if !cfg.ByName() {
			return fmt.Errorf("listen on %s: %w (is another broker running there? choose another with --listen)", cfg.ListenAddr, err)
		}
		return err
	}
	// Until Serve takes over (and closes storage on its way out), a failure below
	// must release what was opened above.
	serving := false
	defer func() {
		if !serving {
			bind.ln.Close()
		}
	}()
	if cfg.ByName() {
		// What clients are sent back to is the name, or the address where the name
		// does not resolve here: openListener already asked. The port is the bound one.
		cfg.AdvertisedHost = bind.host
	}

	srv, err := server.NewFromConfig(cfg, logger)
	if err != nil {
		return err
	}
	defer func() {
		if !serving {
			srv.Close()
		}
	}()

	bootstrap := bind.bootstrap
	if !cfg.ByName() {
		bootstrap = dialable(bind.ln.Addr(), cfg.AdvertisedHost)
	}
	ready := readyInfo{version: version, kafka: cfg.Version, bootstrap: bootstrap, dataDir: absPath(cfg.DataDir),
		auth: authSummary(cfg), configFile: st.configFile}

	if cfg.Console {
		if cfg.ConsoleAddr == "" {
			cfg.ConsoleAddr = consoleAddrFor(z.ip())
		}
		closeConsole, consoleAddr, err := startConsole(ctx, cfg, bind.ln.Addr(), logger, consoleUser, consolePass, consoleMech)
		if err != nil {
			return err
		}
		defer closeConsole()
		// A name's URL is port-less when the shared front door is up; where it is not
		// (or this instance has no name) the address it actually bound is the URL.
		ready.consoleURL = z.routeConsole(logger, consoleAddr)
		if ready.consoleURL == "" {
			ready.consoleURL = httpURL(consoleAddr)
		}
	}

	if cfg.SchemaRegistry {
		if cfg.SchemaRegistryAddr == "" {
			cfg.SchemaRegistryAddr = registryAddrFor(z.ip())
		}
		closeRegistry, registryAddr, err := startRegistry(ctx, cfg, srv, logger)
		if err != nil {
			return err
		}
		defer closeRegistry()
		ready.registryURL = z.routeRegistry(logger, cfg.InstanceName(), registryAddr)
		if ready.registryURL == "" {
			ready.registryURL = httpURL(registryAddr)
		}
	}

	announce(out, logger, cfg, ready)
	serving = true
	if err := srv.ServeListener(ctx, bind.ln); err != nil {
		return err
	}
	// Serve has returned: accepting stopped and connections drained; storage was
	// flushed and closed on the way out.
	logger.Info("shutting down: flushing logs to disk")
	return nil
}

// readyInfo is what the start-up summary reports.
type readyInfo struct {
	version, bootstrap, consoleURL, registryURL string
	kafka                                       int
	dataDir, auth, configFile                   string
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
// point a client at, where the console is, where the data lives. As text it is a few
// aligned lines on stdout (logs stay on stderr); as JSON it is a single "ready"
// record; it stays quiet when the log level asked for quiet. The text matches
// doze-aws's: one experience across the doze tools.
func announce(out io.Writer, logger *slog.Logger, cfg config.Config, r readyInfo) {
	if cfg.LogLevel == "warn" || cfg.LogLevel == "error" {
		return
	}
	if cfg.LogFormat == "json" {
		logger.Info("ready", "bootstrap", r.bootstrap, "console", r.consoleURL, "schema_registry", r.registryURL,
			"data_dir", r.dataDir, "auth", r.auth, "kafka_compat", r.kafka, "instance", cfg.InstanceName())
		return
	}
	var b strings.Builder
	b.WriteString("\n  doze-kafka is up.\n\n")
	fmt.Fprintf(&b, "    kafka     %s\n", r.bootstrap)
	if r.consoleURL != "" {
		fmt.Fprintf(&b, "    console   %s   topics, messages and consumer groups, live\n", r.consoleURL)
	}
	if r.registryURL != "" {
		fmt.Fprintf(&b, "    registry  %s   Confluent-compatible schema registry\n", r.registryURL)
	}
	fmt.Fprintf(&b, "    data      %s\n", r.dataDir)
	fmt.Fprintf(&b, "    serving   Kafka %d protocol · %s\n", r.kafka, r.auth)
	if r.configFile != "" {
		fmt.Fprintf(&b, "    config    %s\n", r.configFile)
	}
	b.WriteString("\n")
	fmt.Fprintf(&b, "    %-24s point this shell at it\n", `eval "$(doze-kafka env)"`)
	fmt.Fprintf(&b, "    %-24s when something looks wrong\n\n", "doze-kafka doctor")
	io.WriteString(out, b.String()) //nolint:errcheck // a banner on stdout
}

// httpURL is the URL to open for an HTTP listener at addr. A wildcard listener's own
// address ("[::]:8081") is where it listens, not somewhere to browse to; for that the
// URL names localhost.
func httpURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// dialable is the bound address as something to paste into a client. A wildcard
// listener's own address ("[::]:9092", "0.0.0.0:9092") is where it listens, not
// somewhere to connect to; for that the summary gives the advertised host, which
// is where clients are sent after bootstrapping anyway, and says it is listening
// on every interface.
func dialable(bound net.Addr, advertisedHost string) string {
	host, port, err := net.SplitHostPort(bound.String())
	if err != nil {
		return bound.String()
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		return net.JoinHostPort(advertisedHost, port) + "   (listening on every interface)"
	}
	return bound.String()
}

// absPath is p made absolute for display, or p itself if that fails.
func absPath(p string) string {
	if a, err := filepath.Abs(p); err == nil {
		return a
	}
	return p
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
	// The console is the operator's own tool, and it has no way to be granted access
	// topic by topic: it is never refused, whatever ACLs the broker holds.
	cfg.ACLSuperUsers = append(slices.Clone(cfg.ACLSuperUsers), "User:"+user)
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
