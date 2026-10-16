package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/config"
	names "github.com/doze-dev/doze-names"
)

// startup is everything resolved from the command line and config file before the
// broker runs.
type startup struct {
	cfg        config.Config
	configFile string   // the config file actually loaded, or "" if none.
	notices    []string // one-line heads-ups to log once logging is up (deprecations, legacy paths).
}

var (
	// errHelp is returned when --help was asked for: not a failure, nothing to run.
	errHelp = flag.ErrHelp
	// errShowVersion is returned for a bare --version / -v.
	errShowVersion = errors.New("show version")
)

// normalizeArgs handles the one flag that changed meaning. --version used to
// select the emulated Kafka major (--version 3) and now follows the universal
// convention of printing the program's version; the old use is --kafka-version.
// A --version with a number is still understood and rewritten, with a notice, so
// existing scripts keep working while they are updated.
//
// Only flags are looked at. The word after a flag that takes a value is that
// value, whatever it looks like: `--data-dir -v` names a directory called "-v",
// and used to print the version instead.
func normalizeArgs(args []string) (out []string, showVersion bool, notices []string) {
	isNumber := func(s string) bool { _, err := strconv.Atoi(s); return err == nil }
	d := config.Default()
	fs, _ := newFlagSet(&d)
	takesValue := func(name string) bool {
		f := fs.Lookup(name)
		if f == nil {
			return false
		}
		b, isBool := f.Value.(interface{ IsBoolFlag() bool })
		return !isBool || !b.IsBoolFlag()
	}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" || !strings.HasPrefix(a, "-") || a == "-" {
			// "--" ends the flags, and so does the first bare word: the flag
			// package stops there too.
			out = append(out, args[i:]...)
			break
		}
		name, val, hasVal := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if name != "version" && name != "v" {
			out = append(out, a)
			if !hasVal && takesValue(name) && i+1 < len(args) {
				i++
				out = append(out, args[i])
			}
			continue
		}
		switch {
		case hasVal && isNumber(val) && name == "version":
			out = append(out, "--kafka-version="+val)
			notices = append(notices, "--version "+val+" is deprecated and now spelled --kafka-version "+val+"; --version prints the doze-kafka version")
		case !hasVal && name == "version" && i+1 < len(args) && isNumber(args[i+1]):
			out = append(out, "--kafka-version", args[i+1])
			notices = append(notices, "--version "+args[i+1]+" is deprecated and now spelled --kafka-version "+args[i+1]+"; --version prints the doze-kafka version")
			i++
		default:
			showVersion = true
		}
	}
	return out, showVersion, notices
}

// helpGroups is how --help lays the flags out. Every flag must be in exactly one
// group; TestEveryFlagIsInTheHelp keeps a new flag from being left out.
var helpGroups = []struct {
	title string
	flags []string
}{
	{"Network", []string{"name", "listen", "advertised-host", "advertised-port", "zone"}},
	{"Storage", []string{"data-dir", "meta-db", "segment-bytes", "index-interval-bytes"}},
	{"Retention and durability", []string{"retention-bytes", "retention-ms", "fsync-interval"}},
	{"Topics and groups", []string{"kafka-version", "auto-create-topics", "default-partitions", "max-partitions", "message-max-bytes", "consumer-session-timeout", "group-initial-rebalance-delay", "offsets-retention", "client-telemetry"}},
	{"Security", []string{"sasl-mechanisms", "sasl-user", "tls-cert", "tls-key", "acl-default", "acl-super-user"}},
	{"Console", []string{"console", "console-addr", "console-prefix", "console-allow-host", "console-unprotected", "trace-header"}},
	{"Schema registry", []string{"schema-registry", "schema-registry-addr", "schema-registry-tls"}},
	{"Process", []string{"config", "log-level", "log-format", "pprof-addr"}},
}

// printHelp writes the usage text. It goes to stdout when asked for, so
// `doze-kafka --help | less` works.
func printHelp(w io.Writer, fs *flag.FlagSet) {
	fmt.Fprint(w, `doze-kafka — a single-node Kafka-compatible broker for local development.

Usage:
  doze-kafka [flags]            run the broker
  doze-kafka <command> [args]

Commands:
  version                       print the doze-kafka version
  env                           print the shell block that points a client at this instance
  config print [--show-secrets] [flags]
                                print the effective configuration as TOML (passwords redacted)
  dns-setup [--check|--uninstall|--print]
                                one-time setup for the shared .doze names
  doctor                        report what is set up for .doze names, and what is not
  help                          show this help

Every flag can also be set in doze-kafka.toml (see --config); flags win over the
file, the file over the defaults.
`)
	for _, g := range helpGroups {
		fmt.Fprintf(w, "\n%s:\n", g.title)
		for _, name := range g.flags {
			f := fs.Lookup(name)
			if f == nil {
				continue
			}
			typ, usage := flag.UnquoteUsage(f)
			head := "  --" + f.Name
			if typ != "" && typ != "value" {
				head += " " + typ
			}
			fmt.Fprintf(w, "%s\n", head)
			def := ""
			if f.DefValue != "" && f.DefValue != "false" && f.DefValue != "0" && f.DefValue != "[]" && f.DefValue != "0s" {
				def = " (default " + f.DefValue + ")"
			}
			fmt.Fprintf(w, "        %s%s\n", usage, def)
		}
	}
	fmt.Fprint(w, `
Examples:
  doze-kafka                                   serve kafka.<dir>.doze:9092, console at http://kafka.<dir>.doze
  doze-kafka --listen 127.0.0.1:9092           serve an address instead of the name (console 127.0.0.1:9093)
  doze-kafka --listen :9092 --advertised-host host.docker.internal
                                               reachable from containers on this machine
  doze-kafka --data-dir /tmp/kafka --retention-ms 3600000
`)
}

// loadConfig resolves the effective configuration with flags > file > defaults
// precedence. It parses once to discover --config (falling back to
// ./doze-kafka.toml if present), overlays that file onto the defaults, then parses
// the flags again on top so any flag still wins for a one-off override.
//
// The result is validated here, before anything starts, so every configuration
// mistake is reported the same way at the same moment.
func loadConfig(args []string) (startup, error) { return loadConfigFor(args, false) }

// loadEnvConfig is loadConfig for `doze-kafka env`, which prints where a client
// connects and never a password: there SASL mechanisms without users are
// accepted, so `env --sasl-mechanisms SCRAM-SHA-512` (what the start-up summary
// prints under SASL) works without the passwords on a command line.
func loadEnvConfig(args []string) (startup, error) { return loadConfigFor(args, true) }

func loadConfigFor(args []string, forEnv bool) (startup, error) {
	args, showVersion, notices := normalizeArgs(args)
	if showVersion {
		return startup{}, errShowVersion
	}

	probe := config.Default()
	configPath, configSet, err := parseFlags(args, &probe)
	if err != nil {
		return startup{}, err
	}
	// Without --config, a doze-kafka.toml in the working directory is used. An
	// explicit --config "" means "no file", and is how to ignore that one.
	if !configSet {
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
	if _, _, err = parseFlags(args, &c); err != nil {
		return startup{}, err
	}
	// The instance name is resolved HERE rather than in Default, so the server, doctor
	// and env, which all run in the project directory and all go through loadConfig,
	// give the same answer.
	if c.Name == "" {
		wd, err := os.Getwd()
		if err != nil {
			return startup{}, fmt.Errorf("config: cannot read the working directory to name this instance: %w", err)
		}
		c.Name = config.DeriveName(wd)
	}
	if n := c.ResolveMetaDB(); n != "" {
		notices = append(notices, n)
	}
	c.ResolveAdvertisedHost()
	c.ResolveConsoleAddr()
	if c.ConsolePrefix == "" {
		// The console's own fallback, resolved here so that `config print` shows the
		// path it is really served under and Validate checks it like a flag.
		c.ConsolePrefix = os.Getenv("DOZE_KAFKA_CONSOLE_PREFIX")
	}
	if c.ConsolePrefix == "" {
		c.ConsolePrefix = config.DefaultConsolePrefix
	}
	check := c
	if forEnv && check.AuthEnabled() && len(check.Credentials) == 0 {
		check.Credentials = map[string]string{"env": "not printed"}
	}
	if err := check.Validate(); err != nil {
		return startup{}, err
	}
	if c.ByName() && c.Zone && names.Disabled() {
		// The same refusal as --zone=false, for the switch every doze binary reads.
		return startup{}, fmt.Errorf("config: DOZE_ZONE=off turns .doze names off, and no --listen was set, so there is nothing to listen on; give --listen host:port (127.0.0.1:9092 for this machine only), or unset DOZE_ZONE")
	}
	notices = append(notices, c.Warnings()...)
	return startup{cfg: c, configFile: configPath, notices: notices}, nil
}

// parseFlags binds the flags onto dst and parses args, returning the --config
// path and whether --config was given at all. The flags' defaults are dst's
// current field values, so keys already set from a config file survive unless the
// corresponding flag is explicitly passed.
func parseFlags(args []string, dst *config.Config) (configPath string, configSet bool, err error) {
	fs, path := newFlagSet(dst)
	if err := fs.Parse(args); err != nil {
		return "", false, flagError(fs, err)
	}
	if fs.NArg() > 0 {
		return "", false, unexpectedArgument(fs, fs.Arg(0))
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "config" {
			configSet = true
		}
	})
	return *path, configSet, nil
}

// flagError rewords the flag package's errors. Its own name flags with one dash
// ("flag provided but not defined: -listn"), which is not how they are written
// here or in --help, and it has nothing to say about a near miss.
func flagError(fs *flag.FlagSet, err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return err
	}
	msg := err.Error()
	if name, ok := strings.CutPrefix(msg, "flag provided but not defined: -"); ok {
		name = strings.TrimLeft(name, "-")
		if near := nearestFlag(fs, name); near != "" {
			return fmt.Errorf("unknown flag --%s — did you mean --%s?", name, near)
		}
		return fmt.Errorf("unknown flag --%s", name)
	}
	if name, ok := strings.CutPrefix(msg, "flag needs an argument: -"); ok {
		return fmt.Errorf("--%s needs a value", strings.TrimLeft(name, "-"))
	}
	if rest, ok := strings.CutPrefix(msg, "invalid value "); ok {
		// invalid value "x" for flag -name: reason
		if val, tail, ok := strings.Cut(rest, " for flag -"); ok {
			name, reason, _ := strings.Cut(tail, ": ")
			if strings.TrimLeft(name, "-") == "sasl-user" {
				// The value is user:password, or a password on its own that lost its
				// user: not repeated, here or in the reason.
				return fmt.Errorf("--sasl-user: not a valid value (expected user:password)")
			}
			return fmt.Errorf("--%s: %s is not a valid value (%s)", strings.TrimLeft(name, "-"), val, reason)
		}
	}
	if rest, ok := strings.CutPrefix(msg, "invalid boolean value "); ok {
		if val, name, ok := strings.Cut(rest, " for -"); ok {
			name, _, _ = strings.Cut(name, ":")
			return fmt.Errorf("--%s: %s is not true or false", strings.TrimLeft(name, "-"), val)
		}
	}
	return err
}

// nearestFlag is the flag closest to name, if any is close: a shared prefix of
// three or more letters, or a letter or two different.
func nearestFlag(fs *flag.FlagSet, name string) string {
	best, bestDist := "", 3
	fs.VisitAll(func(f *flag.Flag) {
		if len(name) >= 3 && strings.HasPrefix(f.Name, name) && best == "" {
			best, bestDist = f.Name, 0
			return
		}
		if d := editDistance(name, f.Name); d < bestDist {
			best, bestDist = f.Name, d
		}
	})
	return best
}

// editDistance is the Levenshtein distance between a and b.
func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur := make([]int, len(b)+1)
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev = cur
	}
	return prev[len(b)]
}

// newFlagSet registers every flag onto dst. Errors and usage are not printed by
// the flag package: the caller decides how and where.
func newFlagSet(dst *config.Config) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet("doze-kafka", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("config", "", "path to a TOML config file (default: ./doze-kafka.toml if present; \"\" to use none)")
	registerFlags(fs, dst)
	return fs, path
}

// unexpectedArgument explains a stray positional. Every setting is a flag, so a
// bare word is nearly always a flag missing its dashes or a mistyped command, and
// silently ignoring it (as the flag package does) starts the broker with defaults
// the user thought they had overridden.
func unexpectedArgument(fs *flag.FlagSet, arg string) error {
	name, _, _ := strings.Cut(arg, "=")
	if fs.Lookup(name) != nil {
		return fmt.Errorf("unexpected argument %q — did you mean --%s?", arg, arg)
	}
	var names []string
	fs.VisitAll(func(f *flag.Flag) { names = append(names, f.Name) })
	sort.Strings(names)
	for _, n := range names {
		if strings.HasPrefix(n, name) && len(name) >= 3 {
			return fmt.Errorf("unexpected argument %q — did you mean --%s?", arg, n)
		}
	}
	return fmt.Errorf("unexpected argument %q — doze-kafka takes flags (--name value) and the commands version, env, config, dns-setup, doctor and help", arg)
}

// registerFlags binds command-line flags onto cfg, using its current values as
// defaults. See config.Default for the meaning of each field.
func registerFlags(fs *flag.FlagSet, cfg *config.Config) {
	// Network.
	fs.StringVar(&cfg.Name, "name", cfg.Name, "this instance's name: it answers on kafka.<name>.doze (default: the project directory's name)")
	fs.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "serve on this host:port INSTEAD of the .doze name (no name is claimed); use :9092 to accept connections from other machines or containers")
	fs.StringVar(&cfg.AdvertisedHost, "advertised-host", cfg.AdvertisedHost, "host advertised to clients in Metadata (default: derived from --listen: the address itself, or localhost for a wildcard)")
	fs.IntVar(&cfg.AdvertisedPort, "advertised-port", cfg.AdvertisedPort, "port advertised to clients (0 = the port actually bound)")

	// Storage.
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "directory for partition logs")
	fs.StringVar(&cfg.MetaDB, "meta-db", cfg.MetaDB, "path to the metadata database file (default <data-dir>/meta.db)")
	fs.Int64Var(&cfg.SegmentBytes, "segment-bytes", cfg.SegmentBytes, "roll a segment once it exceeds this size")
	fs.Int64Var(&cfg.IndexIntervalBytes, "index-interval-bytes", cfg.IndexIntervalBytes, "add one sparse index entry per this many bytes of log")

	// Retention and durability.
	fs.Int64Var(&cfg.RetentionBytes, "retention-bytes", cfg.RetentionBytes, "delete old segments once a partition exceeds this size (0 = unbounded)")
	fs.Int64Var(&cfg.RetentionMs, "retention-ms", cfg.RetentionMs, "delete segments older than this many ms (0 = unbounded)")
	fs.DurationVar(&cfg.FsyncInterval, "fsync-interval", cfg.FsyncInterval, "coarse fsync cadence for active segments (0 = disable)")

	// Topics and groups.
	fs.IntVar(&cfg.Version, "kafka-version", cfg.Version, "Kafka major version to emulate (1-4)")
	fs.BoolVar(&cfg.AutoCreateTopics, "auto-create-topics", cfg.AutoCreateTopics, "create unknown topics on first reference")
	fs.IntVar(&cfg.DefaultPartitions, "default-partitions", cfg.DefaultPartitions, "partition count for auto-created topics")
	fs.IntVar(&cfg.MaxPartitions, "max-partitions", cfg.MaxPartitions, "most partitions one topic may have (each is a directory and open files)")
	fs.Int64Var(&cfg.MessageMaxBytes, "message-max-bytes", cfg.MessageMaxBytes, "largest record batch a topic accepts unless it sets max.message.bytes, as Kafka's message.max.bytes")
	fs.DurationVar(&cfg.ConsumerSessionTimeout, "consumer-session-timeout", cfg.ConsumerSessionTimeout, "KIP-848 group session timeout before evicting a silent member")
	fs.DurationVar(&cfg.GroupInitialRebalanceDelay, "group-initial-rebalance-delay", cfg.GroupInitialRebalanceDelay, "how long an empty classic group's first rebalance waits for more members, as Kafka's group.initial.rebalance.delay.ms (Kafka's default is 3s; 0 = no wait)")
	fs.DurationVar(&cfg.OffsetsRetention, "offsets-retention", cfg.OffsetsRetention, "how long a group's committed offsets are kept once nothing is reading them, as Kafka's offsets.retention.minutes (0 = for ever)")

	// Security.
	if cfg.Credentials == nil {
		cfg.Credentials = map[string]string{}
	}
	fs.Var(&stringSliceFlag{vals: &cfg.SASLMechanisms}, "sasl-mechanisms", "comma-separated SASL mechanisms to enable (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)")
	fs.Var(credsFlag{cfg.Credentials}, "sasl-user", "SASL credential as user:password (repeatable); needs --sasl-mechanisms")
	fs.StringVar(&cfg.TLSCertFile, "tls-cert", cfg.TLSCertFile, "PEM certificate file (enables TLS together with --tls-key)")
	fs.StringVar(&cfg.TLSKeyFile, "tls-key", cfg.TLSKeyFile, "PEM private key file")
	fs.StringVar(&cfg.ACLDefault, "acl-default", cfg.ACLDefault, "what a resource with no ACL allows: allow, or deny (Kafka's default). ACLs you create are enforced either way")
	fs.Var(&stringSliceFlag{vals: &cfg.ACLSuperUsers}, "acl-super-user", "principal ACLs never refuse, as Type:name such as User:admin (comma-separated or repeated)")

	// Console.
	fs.BoolVar(&cfg.Console, "console", cfg.Console, "serve the web console (topics, messages, consumer groups)")
	fs.BoolVar(&cfg.SchemaRegistry, "schema-registry", cfg.SchemaRegistry, "serve a Confluent-compatible schema registry (Avro, JSON Schema, Protobuf)")
	fs.StringVar(&cfg.SchemaRegistryAddr, "schema-registry-addr", cfg.SchemaRegistryAddr, "host:port for the schema registry (default: 8081 beside the broker, on its name's address or on loopback; a free loopback port when --listen is on port 0)")
	fs.BoolVar(&cfg.SchemaRegistryTLS, "schema-registry-tls", cfg.SchemaRegistryTLS, "serve the schema registry over TLS, with the broker's certificate, when --tls-cert and --tls-key are set; --schema-registry-tls=false keeps it plain HTTP")
	fs.BoolVar(&cfg.Zone, "zone", cfg.Zone, "join the shared .doze zone (names and the console front door); --zone=false is the same as DOZE_ZONE=off")
	fs.BoolVar(&cfg.ClientTelemetry, "client-telemetry", cfg.ClientTelemetry, "accept the metrics clients push about themselves (KIP-714) and show them in the console; --client-telemetry=false stops advertising the two APIs")
	fs.StringVar(&cfg.TraceHeader, "trace-header", cfg.TraceHeader, "record header that holds a trace id (a request or correlation id), beside W3C traceparent and B3, for the console's \"follow this message\"")
	fs.StringVar(&cfg.ConsoleAddr, "console-addr", cfg.ConsoleAddr, "host:port for the web console (default: 9093 beside the broker, on its name's address or on loopback; a free loopback port when --listen is on port 0)")
	fs.StringVar(&cfg.ConsolePrefix, "console-prefix", cfg.ConsolePrefix, "URL path the web console is served under, for a reverse proxy that mounts it below a path (default /_console)")
	fs.BoolVar(&cfg.ConsoleUnprotected, "console-unprotected", cfg.ConsoleUnprotected, "serve the console beyond loopback although SASL or TLS is on: it has no login, is plain HTTP and acts as a super user, so without this that start is refused")
	fs.Var(&stringSliceFlag{vals: &cfg.ConsoleAllowHosts}, "console-allow-host", "extra host name the console answers to, besides localhost, IP addresses and *.doze (comma-separated or repeated); guards against DNS rebinding")

	// Process.
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log verbosity: debug, info, warn or error (debug logs every request)")
	fs.StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "log format: text or json")
	fs.StringVar(&cfg.PprofAddr, "pprof-addr", cfg.PprofAddr, "if set, serve net/http/pprof here (goroutine/heap profiling for soak/debug)")
}

// stringSliceFlag collects a comma-separated (and/or repeated) flag into a slice.
// The first use replaces whatever the slice held (a value from the config file),
// as every other flag does; only repeats within the command line accumulate.
// Appending to the file's value meant --sasl-mechanisms PLAIN could never turn
// off a mechanism the file had enabled.
type stringSliceFlag struct {
	vals    *[]string
	touched bool
}

func (f *stringSliceFlag) String() string {
	if f == nil || f.vals == nil {
		return ""
	}
	return strings.Join(*f.vals, ",")
}

func (f *stringSliceFlag) Set(s string) error {
	if !f.touched {
		*f.vals = nil
		f.touched = true
	}
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
		return fmt.Errorf("expected user:password") // the value is not echoed: it may be a password.
	}
	f.m[user] = pass
	return nil
}
