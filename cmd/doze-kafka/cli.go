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
func normalizeArgs(args []string) (out []string, showVersion bool, notices []string) {
	isNumber := func(s string) bool { _, err := strconv.Atoi(s); return err == nil }
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			out = append(out, args[i:]...)
			break
		}
		name, val, hasVal := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if !strings.HasPrefix(a, "-") || (name != "version" && name != "v") {
			out = append(out, a)
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
	{"Network", []string{"listen", "advertised-host", "advertised-port"}},
	{"Storage", []string{"data-dir", "meta-db", "segment-bytes", "index-interval-bytes"}},
	{"Retention and durability", []string{"retention-bytes", "retention-ms", "fsync-interval"}},
	{"Topics and groups", []string{"kafka-version", "auto-create-topics", "default-partitions", "max-partitions", "consumer-session-timeout"}},
	{"Security", []string{"sasl-mechanisms", "sasl-user", "tls-cert", "tls-key"}},
	{"Console", []string{"console", "console-addr", "console-allow-host"}},
	{"Process", []string{"config", "zone", "log-level", "log-format", "pprof-addr"}},
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
  config print [flags]          print the effective configuration as TOML
  dns-setup [--check|--uninstall|--print]
                                one-time setup for the shared .doze names
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
  doze-kafka                                   run on 127.0.0.1:9092, console on :9093
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
func loadConfig(args []string) (startup, error) {
	args, showVersion, notices := normalizeArgs(args)
	if showVersion {
		return startup{}, errShowVersion
	}

	probe := config.Default()
	configPath, err := parseFlags(args, &probe)
	if err != nil {
		return startup{}, err
	}
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
	if _, err = parseFlags(args, &c); err != nil {
		return startup{}, err
	}
	if n := c.ResolveMetaDB(); n != "" {
		notices = append(notices, n)
	}
	if err := c.Validate(); err != nil {
		return startup{}, err
	}
	return startup{cfg: c, configFile: configPath, notices: notices}, nil
}

// parseFlags binds the flags onto dst and parses args, returning the --config
// path (if any). The flags' defaults are dst's current field values, so keys
// already set from a config file survive unless the corresponding flag is
// explicitly passed.
func parseFlags(args []string, dst *config.Config) (configPath string, err error) {
	fs, path := newFlagSet(dst)
	if err := fs.Parse(args); err != nil {
		return "", err
	}
	if fs.NArg() > 0 {
		return "", unexpectedArgument(fs, fs.Arg(0))
	}
	return *path, nil
}

// newFlagSet registers every flag onto dst. Errors and usage are not printed by
// the flag package: the caller decides how and where.
func newFlagSet(dst *config.Config) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet("doze-kafka", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("config", "", "path to a TOML config file (default: ./doze-kafka.toml if present)")
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
	return fmt.Errorf("unexpected argument %q — doze-kafka takes flags (--name value) and the commands version, config, dns-setup and help", arg)
}

// registerFlags binds command-line flags onto cfg, using its current values as
// defaults. See config.Default for the meaning of each field.
func registerFlags(fs *flag.FlagSet, cfg *config.Config) {
	// Network.
	fs.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "host:port to listen on; use :9092 to accept connections from other machines or containers")
	fs.StringVar(&cfg.AdvertisedHost, "advertised-host", cfg.AdvertisedHost, "host advertised to clients in Metadata")
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
	fs.DurationVar(&cfg.ConsumerSessionTimeout, "consumer-session-timeout", cfg.ConsumerSessionTimeout, "KIP-848 group session timeout before evicting a silent member")

	// Security.
	if cfg.Credentials == nil {
		cfg.Credentials = map[string]string{}
	}
	fs.Var(&stringSliceFlag{vals: &cfg.SASLMechanisms}, "sasl-mechanisms", "comma-separated SASL mechanisms to enable (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)")
	fs.Var(credsFlag{cfg.Credentials}, "sasl-user", "SASL credential as user:password (repeatable); needs --sasl-mechanisms")
	fs.StringVar(&cfg.TLSCertFile, "tls-cert", cfg.TLSCertFile, "PEM certificate file (enables TLS together with --tls-key)")
	fs.StringVar(&cfg.TLSKeyFile, "tls-key", cfg.TLSKeyFile, "PEM private key file")

	// Console.
	fs.BoolVar(&cfg.Console, "console", cfg.Console, "serve the web console (topics, messages, consumer groups)")
	fs.StringVar(&cfg.ConsoleAddr, "console-addr", cfg.ConsoleAddr, "host:port for the web console (Kafka's wire protocol cannot share a port with HTTP)")
	fs.Var(&stringSliceFlag{vals: &cfg.ConsoleAllowHosts}, "console-allow-host", "extra host name the console answers to, besides localhost, IP addresses and *.doze (comma-separated or repeated); guards against DNS rebinding")

	// Process.
	fs.BoolVar(&cfg.Zone, "zone", cfg.Zone, "join the shared .doze name zone (kafka.doze); --zone=false to opt out")
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
		return fmt.Errorf("expected user:password, got %q", s)
	}
	f.m[user] = pass
	return nil
}
