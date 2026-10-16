// Package config defines the broker's runtime configuration and its defaults.
package config

import (
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// The Kafka majors that can be emulated (--kafka-version). The API table in
// internal/protocol has a column for each; a test holds the two to each other.
// They are stated here, rather than asked of that package, so that config depends
// on nothing else in the broker and anything may depend on it.
const MinKafkaMajor, MaxKafkaMajor = 1, 4

// SASL mechanisms this broker can serve.
const (
	SASLPlain       = "PLAIN"
	SASLScramSHA256 = "SCRAM-SHA-256"
	SASLScramSHA512 = "SCRAM-SHA-512"
)

// knownSASLMechanisms is the set accepted in config.
var knownSASLMechanisms = map[string]bool{
	SASLPlain:       true,
	SASLScramSHA256: true,
	SASLScramSHA512: true,
}

// Config holds every tunable for a broker instance. Construct it with Default,
// override fields (e.g. from flags), then call Validate before use.
type Config struct {
	// Protocol.
	Version int // declared Kafka major version to emulate (see internal/protocol profiles).

	// Network.
	ListenAddr     string // host:port the broker listens on.
	AdvertisedHost string // host returned to clients in Metadata responses; "" means derive it from ListenAddr (see ResolveAdvertisedHost).
	AdvertisedPort int    // port returned to clients; 0 means "derive from ListenAddr".

	// Storage.
	DataDir            string // directory holding per-partition log segments.
	MetaDB             string // path to the metadata database (topics, offsets, PIDs).
	SegmentBytes       int64  // roll the active segment once it exceeds this size.
	IndexIntervalBytes int64  // add one sparse index entry per this many bytes of log.

	// Retention (delete-only; zero disables the bound).
	RetentionBytes int64
	RetentionMs    int64

	// Durability.
	FsyncInterval time.Duration // coarse fsync cadence; never fsync per append.

	// Groups.
	ConsumerSessionTimeout time.Duration // KIP-848 session timeout: evict a member after this long without a heartbeat.
	// OffsetsRetention is how long a group's committed offsets are kept once nothing is
	// reading them (offsets.retention.minutes: 7 days). Zero keeps them for ever.
	OffsetsRetention time.Duration
	// OffsetsRetentionCheckInterval is how often the retention is applied
	// (offsets.retention.check.interval.ms). Zero is a hundredth of the retention,
	// between 50 ms and ten minutes. It has no flag or file key: the conformance
	// replays set it long, and apply the retention at the points of a script where
	// Kafka's check has run (serverhook.CheckOffsets), so that what they compare
	// does not depend on when a ticker fires.
	OffsetsRetentionCheckInterval time.Duration

	// Topics.
	AutoCreateTopics  bool
	DefaultPartitions int // partition count for auto-created topics.
	MaxPartitions     int // most partitions a single topic may have; every one is open files and a directory.
	// MessageMaxBytes is the largest record batch a topic accepts unless it sets
	// max.message.bytes itself: Kafka's message.max.bytes, with Kafka's default.
	MessageMaxBytes int64

	// Auth (all optional; empty = disabled). SASL gates the listener behind a
	// handshake; TLS encrypts it. The two are independent — SASL over plaintext,
	// TLS without SASL, or both — matching how real Kafka listeners compose.
	SASLMechanisms []string          // enabled mechanisms; empty disables SASL.
	Credentials    map[string]string // username -> password.
	TLSCertFile    string            // PEM cert; with TLSKeyFile, enables TLS.
	TLSKeyFile     string            // PEM private key.

	// Authorization. ACLs created through CreateAcls are enforced. A resource with no
	// ACL at all follows ACLDefault: "allow" (the default, so nothing changes until
	// ACLs are created) or "deny" (Kafka's own default: only what an ACL allows).
	// ACLSuperUsers ("User:admin") are never refused anything.
	ACLDefault    string
	ACLSuperUsers []string

	// Console: the web UI, served over HTTP beside the broker.
	Console           bool     // serve the console.
	ConsoleAddr       string   // host:port for the console's HTTP listener.
	ConsolePrefix     string   // URL path the console is mounted under; "" is DefaultConsolePrefix.
	ConsoleAllowHosts []string // extra Host names the console answers to (DNS-rebinding guard).
	// ConsoleUnprotected accepts serving the console beyond loopback while SASL or
	// TLS is on. The console has no login and acts on the broker as a super user, so
	// without this that combination is refused (see Validate).
	ConsoleUnprotected bool
	TraceHeader        string // a record header holding a trace id, for the console's message lineage.

	// ClientTelemetry accepts the metrics clients push about themselves (KIP-714) and
	// keeps the last few minutes in memory for the console.
	ClientTelemetry bool
	// Zone joins the shared .doze zone (names, the front door). False is the same
	// switch as DOZE_ZONE=off, so a CI job or container can say it once.
	Zone bool

	// SchemaRegistry: a Confluent-compatible schema registry, served over HTTP beside
	// the broker. Its records live in the metadata database.
	SchemaRegistry     bool   // serve the registry.
	SchemaRegistryAddr string // host:port for the registry's HTTP listener.
	// SchemaRegistryTLS serves the registry over TLS, with the broker's certificate,
	// whenever the broker has one. False keeps it plain HTTP (see RegistryTLS).
	SchemaRegistryTLS bool

	// Name is this instance's name in the shared .doze zone. It answers on
	// kafka.<name>.doze, so two projects on one machine are two instances that
	// never contend for an address or a port. Empty means derive one from the
	// working directory (DeriveName), which the configuration loader does.
	Name string

	// Process.
	PprofAddr string // if set, serve net/http/pprof here.
	LogLevel  string // debug, info, warn or error.
	LogFormat string // text or json.
}

const (
	// There is no default address. An instance is its .doze name, and an address
	// is the opt-in (--listen) for the places a name cannot serve: a container
	// reaching this one over a compose network, CI, anywhere without DNS. The
	// two are exclusive, as they are in doze-aws: one instance, one way to
	// reach it.
	//
	// A name resolves to a loopback address of its own, so the broker is not
	// reachable from the network it happens to be plugged into, and two
	// instances can both be on port 9092. An address that is not loopback
	// (--listen :9092) is a choice, and docs/configuration.md says what else to
	// set when making it.
	defaultListenAddr = ""
	// The console is HTTP, which cannot share the broker's port. In name mode it
	// sits beside the broker on the name's own address, port 9093; under --listen
	// it is on loopback unless --console-addr says otherwise.
	defaultConsoleAddr = ""
	listenModeConsole  = "127.0.0.1:9093"
	// The registry is HTTP too, on Confluent's own port, beside the broker on the
	// name's address in name mode and on loopback under --listen.
	listenModeRegistry = "127.0.0.1:8081"
	// Under a --listen on port 0, both take a port the system chooses.
	listenModeAnyPort   = "127.0.0.1:0"
	defaultSegmentBytes = 128 << 20 // 128 MiB
	defaultIndexStride  = 4 << 10   // 4 KiB

	// DefaultMessageMaxBytes is Kafka's default for message.max.bytes: the largest
	// record batch a topic with no limit of its own accepts. A development broker
	// that took any size let an application ship a batch size production refuses
	// (recorded against apache/kafka:4.3.1: 1048588 bytes is stored, 1048589 is
	// MESSAGE_TOO_LARGE; conformance/recordings/produce-validation.json).
	DefaultMessageMaxBytes = 1048588

	// defaultMaxPartitions bounds one topic, not the broker. A partition is a
	// directory and two open files, so an unbounded count lets a typo take the
	// process out: a 5,000-partition topic measured ~10,000 file descriptors and
	// a 25-30s shutdown. A real Kafka cluster rarely goes past a few hundred for
	// a single topic, and local development never needs more.
	defaultMaxPartitions = 1000
)

// Default returns a Config suitable for zero-config local development.
func Default() Config {
	// Windows has no .doze zone (doze-names does not build there), so an instance
	// cannot be its name: it listens on the address a --listen would have named.
	listen := defaultListenAddr
	if runtime.GOOS == "windows" {
		listen = "127.0.0.1:9092"
	}
	return Config{
		Version:                MaxKafkaMajor,
		ListenAddr:             listen,
		AdvertisedHost:         "", // derived from the listen address: see ResolveAdvertisedHost.
		DataDir:                DefaultDataDir,
		MetaDB:                 "", // empty = <data-dir>/meta.db; see ResolveMetaDB.
		SegmentBytes:           defaultSegmentBytes,
		IndexIntervalBytes:     defaultIndexStride,
		FsyncInterval:          500 * time.Millisecond,
		ConsumerSessionTimeout: 45 * time.Second,
		OffsetsRetention:       7 * 24 * time.Hour,
		AutoCreateTopics:       true,
		DefaultPartitions:      1,
		MaxPartitions:          defaultMaxPartitions,
		MessageMaxBytes:        DefaultMessageMaxBytes,
		Console:                true,
		ClientTelemetry:        true,
		Zone:                   true,
		SchemaRegistry:         true,
		SchemaRegistryTLS:      true,
		ACLDefault:             ACLAllow,
		ConsoleAddr:            defaultConsoleAddr,
		LogLevel:               "info",
		LogFormat:              "text",
	}
}

// DefaultName is the instance name used when nothing better can be derived: a
// directory whose name has no letters or digits in it at all.
const DefaultName = "local"

// InstanceName is the name to claim and to report, and is never empty.
//
// Name is normally filled in when the configuration is loaded, from the project
// directory. This is the floor for a Config built directly in code: without it an
// empty name renders as kafka..doze, which resolves to nothing and reads like a
// bug in the zone rather than a missing setting.
func (c Config) InstanceName() string {
	if c.Name == "" {
		return DefaultName
	}
	return c.Name
}

// DeriveName picks an instance name from a project directory: its base name.
//
// The result is not sanitised here. doze-names reduces a name to a DNS label on
// claim, and duplicating that rule is the kind of mirror that drifts; what this
// checks is only whether anything would SURVIVE the reduction, since a directory
// called "~" would otherwise claim the empty label.
func DeriveName(dir string) string {
	base := filepath.Base(dir)
	for _, r := range base {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return base
		}
	}
	return DefaultName
}

// ByName reports whether this instance answers on its .doze name. It does unless
// --listen gave it an address, which replaces the name rather than adding to it.
func (c Config) ByName() bool { return c.ListenAddr == "" }

// ResolveConsoleAddr fills in the console's and the registry's addresses under
// --listen. In name mode they stay empty until the name's own address is known
// (see cmd/doze-kafka).
//
// A --listen on port 0 asks the system for a port, which is how a script or a
// test runs a broker that cannot collide with another: the console and the
// registry then take system-chosen ports too, on loopback, rather than fixed ones
// that a second copy would find taken. A fixed --listen keeps the fixed ports
// (9093 and Confluent's 8081), which are what a person types.
func (c *Config) ResolveConsoleAddr() {
	if c.ByName() {
		return
	}
	console, registry := listenModeConsole, listenModeRegistry
	if _, port, err := net.SplitHostPort(c.ListenAddr); err == nil && port == "0" {
		console, registry = listenModeAnyPort, listenModeAnyPort
	}
	if c.ConsoleAddr == "" {
		c.ConsoleAddr = console
	}
	if c.SchemaRegistryAddr == "" {
		c.SchemaRegistryAddr = registry
	}
}

// DefaultConsolePrefix is the URL path the console is served under by default.
const DefaultConsolePrefix = "/_console"

// DefaultDataDir is where a bare `doze-kafka` keeps its data.
const DefaultDataDir = "./data"

// ResolveAdvertisedHost fills in AdvertisedHost when it was not given, from where
// the broker listens: a client is told to reconnect to an address that actually
// reaches the listener.
//
//   - a loopback address is advertised as itself ("127.0.0.1", "::1"). It used to be
//     "localhost" whatever the listener was, and on a machine where that name
//     resolves to ::1 before 127.0.0.1, a client of a 127.0.0.1 listener was sent
//     to a port nothing listened on, and tools built on librdkafka (kcat among them)
//     gave up at the first refusal instead of trying the other address.
//   - another specific address is advertised as itself: "localhost" would not reach it.
//   - the wildcard (":9092") listens on every address, loopback included, so
//     "localhost" is right.
//
// An advertised host that was set is never changed.
func (c *Config) ResolveAdvertisedHost() {
	if c.AdvertisedHost != "" || c.ByName() {
		return // set, or the instance's name, which is only known once it is claimed.
	}
	c.AdvertisedHost = "localhost"
	host, _, err := net.SplitHostPort(c.ListenAddr)
	if err != nil {
		return
	}
	if ip := net.ParseIP(host); ip != nil && !ip.IsUnspecified() {
		c.AdvertisedHost = ip.String()
	} else if host == "localhost" {
		c.AdvertisedHost = "127.0.0.1" // that is what the listener binds to.
	}
}

// MetaDBFile is the metadata database's file name inside the data directory.
const MetaDBFile = "meta.db"

// ResolveMetaDB fills in MetaDB when it was not given: <data-dir>/meta.db, so a
// data directory is self-contained and moving or deleting it takes the topic
// registry and committed offsets with it.
//
// Earlier versions defaulted the database to ./meta.db in the working directory,
// whatever the data directory was. A set-up like that keeps working: when the data
// directory has no database of its own, already holds partition data, and a
// ./meta.db file exists, that file is used and the returned notice says so. The
// test is the layout on disk, not how the data directory was spelled: comparing
// the path to "./data" missed `--data-dir data`, absolute paths and every custom
// directory, and those upgrades silently started with an empty registry.
func (c *Config) ResolveMetaDB() (notice string) {
	if c.MetaDB != "" {
		return ""
	}
	inside := filepath.Join(c.DataDir, MetaDBFile)
	c.MetaDB = inside
	if _, err := os.Stat(inside); err == nil {
		return ""
	}
	if st, err := os.Stat(MetaDBFile); err != nil || !st.Mode().IsRegular() || st.Size() == 0 {
		return ""
	}
	if !holdsPartitionData(c.DataDir) {
		return "" // nothing here for an old database to describe.
	}
	c.MetaDB = MetaDBFile
	return "using ./" + MetaDBFile + " from an earlier version, which kept it beside the data directory; move it to " + inside + " (with the broker stopped) to make the data directory self-contained"
}

// holdsPartitionData reports whether dir contains at least one partition
// directory ("<topic>-<n>").
func holdsPartitionData(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		name := e.Name()
		i := strings.LastIndexByte(name, '-')
		if !e.IsDir() || i <= 0 || i == len(name)-1 {
			continue
		}
		if _, err := strconv.Atoi(name[i+1:]); err == nil {
			return true
		}
	}
	return false
}

// validHeaderName reports whether s can be the name of a record header the console
// looks a trace id up in: a short token, so a typo is caught rather than silently
// matching nothing.
func validHeaderName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}

// Validate reports the first problem that would prevent the broker from starting.
func (c Config) Validate() error {
	if c.Version < MinKafkaMajor || c.Version > MaxKafkaMajor {
		return fmt.Errorf("config: unsupported Kafka version %d; supported majors are %d to %d", c.Version, MinKafkaMajor, MaxKafkaMajor)
	}
	switch {
	case c.ListenAddr != "" && !validHostPort(c.ListenAddr): // "" is name mode.
		return fmt.Errorf("config: listen address %q is not host:port (for example 127.0.0.1:9092 or :9092)", c.ListenAddr)
	case c.Console && c.ConsoleAddr != "" && !validHostPort(c.ConsoleAddr):
		return fmt.Errorf("config: console address %q is not host:port (for example 127.0.0.1:9093)", c.ConsoleAddr)
	case c.ConsolePrefix != "" && !validURLPrefix(c.ConsolePrefix):
		return fmt.Errorf("config: console prefix %q is not a URL path such as /_console (it starts with /, and has no spaces, ?, #, %% or .. in it)", c.ConsolePrefix)
	case c.SchemaRegistry && c.SchemaRegistryAddr != "" && !validHostPort(c.SchemaRegistryAddr):
		return fmt.Errorf("config: schema registry address %q is not host:port (for example 127.0.0.1:8081)", c.SchemaRegistryAddr)
	case c.TraceHeader != "" && !validHeaderName(c.TraceHeader):
		return fmt.Errorf("config: trace header %q is not a header name (letters, digits and - _ . only, at most 64)", c.TraceHeader)
	case c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" && c.LogLevel != "error":
		return fmt.Errorf("config: log level %q is not one of debug, info, warn, error", c.LogLevel)
	case c.LogFormat != "text" && c.LogFormat != "json":
		return fmt.Errorf("config: log format %q is not text or json", c.LogFormat)
	case c.DataDir == "":
		return fmt.Errorf("config: data dir is empty")
	case c.SegmentBytes <= 0:
		return fmt.Errorf("config: segment-bytes must be positive, got %d", c.SegmentBytes)
	case c.IndexIntervalBytes <= 0:
		return fmt.Errorf("config: index-interval-bytes must be positive, got %d", c.IndexIntervalBytes)
	case c.DefaultPartitions <= 0:
		return fmt.Errorf("config: default-partitions must be positive, got %d", c.DefaultPartitions)
	case c.MaxPartitions <= 0:
		return fmt.Errorf("config: max-partitions must be positive, got %d", c.MaxPartitions)
	case c.DefaultPartitions > c.MaxPartitions:
		return fmt.Errorf("config: default-partitions (%d) is above max-partitions (%d)", c.DefaultPartitions, c.MaxPartitions)
	case c.MessageMaxBytes <= 0:
		return fmt.Errorf("config: message-max-bytes must be positive, got %d (Kafka's default is %d)", c.MessageMaxBytes, DefaultMessageMaxBytes)
	case c.RetentionBytes < 0:
		return fmt.Errorf("config: retention-bytes must be non-negative (0 disables), got %d", c.RetentionBytes)
	case c.RetentionMs < 0:
		return fmt.Errorf("config: retention-ms must be non-negative (0 disables), got %d", c.RetentionMs)
	case c.RetentionMs > maxRetentionMs:
		// As a time.Duration this overflows to a negative age, which read as
		// "retention off": a number meant to keep everything for ever kept nothing
		// bounded, and a typo'd one did the same without a word.
		return fmt.Errorf("config: retention-ms %d is too large (the most is %d, about 292 years); use 0 to keep records for ever", c.RetentionMs, maxRetentionMs)
	case c.FsyncInterval < 0:
		return fmt.Errorf("config: fsync-interval must be non-negative (0 disables), got %s", c.FsyncInterval)
	case c.FsyncInterval > 0 && c.FsyncInterval < time.Millisecond:
		return fmt.Errorf("config: fsync-interval %s is below 1ms; a bare number is nanoseconds, so write a unit (500ms, 2s), or 0 to disable", c.FsyncInterval)
	case c.AdvertisedPort < 0 || c.AdvertisedPort > 65535:
		return fmt.Errorf("config: advertised-port %d is not a port (1–65535, or 0 for the port actually bound)", c.AdvertisedPort)
	case c.AdvertisedHost != "" && !validAdvertisedHost(c.AdvertisedHost): // "" is "derive it": see ResolveAdvertisedHost.
		return fmt.Errorf("config: advertised-host %q is not a host name or an IP address (no scheme, port or spaces; the port is --advertised-port)", c.AdvertisedHost)
	case c.OffsetsRetention < 0:
		return fmt.Errorf("config: offsets-retention must not be negative, got %s (0 keeps offsets for ever)", c.OffsetsRetention)
	case c.OffsetsRetentionCheckInterval < 0:
		return fmt.Errorf("config: the offsets retention check interval must not be negative, got %s", c.OffsetsRetentionCheckInterval)
	case c.ConsumerSessionTimeout <= 0:
		return fmt.Errorf("config: consumer-session-timeout must be positive, got %s (a member would be evicted the moment it joined)", c.ConsumerSessionTimeout)
	case c.PprofAddr != "" && !validHostPort(c.PprofAddr):
		return fmt.Errorf("config: pprof address %q is not host:port (for example 127.0.0.1:6060)", c.PprofAddr)
	}

	switch c.ACLDefault {
	case ACLAllow, ACLDeny:
	default:
		return fmt.Errorf("config: acl-default %q must be %q or %q", c.ACLDefault, ACLAllow, ACLDeny)
	}
	for _, u := range c.ACLSuperUsers {
		if i := strings.IndexByte(u, ':'); i <= 0 || i == len(u)-1 {
			return fmt.Errorf("config: acl super user %q must be written Type:name, such as User:admin", u)
		}
	}

	if !c.AuthEnabled() && len(c.Credentials) > 0 {
		return fmt.Errorf("config: SASL users are configured but no mechanism is enabled; add --sasl-mechanisms (PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512), or drop the users")
	}
	if c.AuthEnabled() {
		for _, m := range c.SASLMechanisms {
			if !knownSASLMechanisms[m] {
				return fmt.Errorf("config: unknown SASL mechanism %q; supported: %s, %s, %s", m, SASLPlain, SASLScramSHA256, SASLScramSHA512)
			}
		}
		if len(c.Credentials) == 0 {
			return fmt.Errorf("config: SASL is enabled but no credentials are configured")
		}
		for user, pass := range c.Credentials {
			if user == "" {
				return fmt.Errorf("config: SASL credential has an empty username")
			}
			if pass == Redacted {
				return fmt.Errorf("config: SASL user %q has the placeholder %q for a password: `config print` hides passwords, so a file it wrote is not one to run from; put the password back, or print with --show-secrets", user, Redacted)
			}
			if pass == "" {
				return fmt.Errorf("config: SASL user %q has an empty password", user)
			}
		}
	}
	if !c.Zone && c.ByName() {
		// Without the zone there is no name, and without a name or an address there
		// is nothing to listen on: a configuration that cannot start, so it is
		// refused here like any other, not after the broker has begun starting.
		return fmt.Errorf("config: --zone=false (zone = false) turns .doze names off, and no --listen was set, so there is nothing to listen on; give --listen host:port (127.0.0.1:9092 for this machine only), or drop --zone=false")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return fmt.Errorf("config: TLS needs both --tls-cert and --tls-key, or neither")
	}
	if c.consoleExposed() && !c.ConsoleUnprotected {
		// Refused rather than warned about. Turning SASL or TLS on is the operator
		// saying who may reach this broker and how; the console has no login, is plain
		// HTTP and acts as a super user, so on an address other machines can reach it
		// undoes both without anyone having asked for that. A warning scrolls past in
		// a log; a start that does not happen is read.
		return fmt.Errorf("config: the console has no login and is plain HTTP, and %s is on: serving it on %s would %s. Keep --console-addr on loopback (and put a proxy that authenticates in front of it), turn it off with --console=false, or accept this with --console-unprotected", c.protection(), c.ConsoleAddr, c.exposure())
	}
	// Loaded here, with everything else that can be wrong, so a missing or
	// mismatched certificate is reported before anything starts (and by `config
	// print`) rather than as a failure after the data directory has been opened.
	if _, err := c.TLSConfig(); err != nil {
		return err
	}
	return nil
}

// maxRetentionMs is the longest retention that fits in a time.Duration.
const maxRetentionMs = math.MaxInt64 / int64(time.Millisecond)

// validAdvertisedHost reports whether h can be handed to clients as the host to
// dial: a host name or an IP address, with nothing else attached. "localhost:9092"
// and "http://host" were accepted, and every client then failed to resolve them.
func validAdvertisedHost(h string) bool {
	if h == "" || strings.ContainsAny(h, " \t/@") || strings.Contains(h, "://") {
		return false
	}
	if net.ParseIP(h) != nil {
		return true
	}
	return !strings.Contains(h, ":") // a colon that is not an IPv6 address is a port.
}

// The values of ACLDefault.
const (
	ACLAllow = "allow"
	ACLDeny  = "deny"
)

// AuthEnabled reports whether SASL authentication is configured.
func (c Config) AuthEnabled() bool { return len(c.SASLMechanisms) > 0 }

// RegistryTLS reports whether the schema registry is served over TLS: it is on,
// the broker has a certificate, and --schema-registry-tls was not turned off.
//
// The registry is a network API that Confluent's clients reach with
// schema.registry.url, and a deployment that encrypts the broker encrypts it too:
// with SASL its requests carry the same passwords as HTTP basic authentication.
// So the certificate the broker was given serves both, as the default.
func (c Config) RegistryTLS() bool { return c.SchemaRegistry && c.TLSEnabled() && c.SchemaRegistryTLS }

// protection names what the operator turned on to restrict the broker.
func (c Config) protection() string {
	switch {
	case c.AuthEnabled() && c.TLSEnabled():
		return "SASL and TLS"
	case c.AuthEnabled():
		return "SASL"
	}
	return "TLS"
}

// exposure is what serving the console beyond loopback gives away, as the rest
// of a sentence that begins "serving it there would".
func (c Config) exposure() string {
	if c.AuthEnabled() {
		// It logs in as a user of its own that no ACL refuses.
		return "let anyone who can reach that address act on the broker as a super user, without a password"
	}
	return "send topics and messages unencrypted to anyone who can reach that address"
}

// consoleExposed reports whether the console would be served beyond loopback
// while SASL or TLS is on.
func (c Config) consoleExposed() bool {
	return c.Console && (c.AuthEnabled() || c.TLSEnabled()) && c.ConsoleAddr != "" && BeyondLoopback(c.ConsoleAddr)
}

// BeyondLoopback reports whether a listener on addr (host:port) can be reached
// from another machine: a wildcard or empty host, an address that is not
// loopback, or a host name other than localhost (a name is not resolved here;
// one that is not localhost is taken at its word).
func BeyondLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return !ip.IsLoopback()
	}
	return !strings.EqualFold(host, "localhost")
}

// Warnings are the things about a valid configuration that the operator should be
// told once at start-up: each is a choice they made that leaves something open.
func (c Config) Warnings() []string {
	var out []string
	if c.consoleExposed() && c.ConsoleUnprotected {
		out = append(out, fmt.Sprintf("the console is served on %s with no login and over plain HTTP although %s is on (--console-unprotected): that will %s", c.ConsoleAddr, c.protection(), c.exposure()))
	}
	if c.SchemaRegistry && c.TLSEnabled() && !c.SchemaRegistryTLS && c.SchemaRegistryAddr != "" && BeyondLoopback(c.SchemaRegistryAddr) {
		what := "its requests and answers"
		if c.AuthEnabled() {
			what = "its requests, answers and the basic-auth passwords (the broker's SASL users)"
		}
		out = append(out, fmt.Sprintf("the schema registry is served on %s over plain HTTP although the broker has TLS on (--schema-registry-tls=false): %s cross the network unencrypted", c.SchemaRegistryAddr, what))
	}
	return out
}

// TLSEnabled reports whether a TLS certificate/key pair is configured.
func (c Config) TLSEnabled() bool { return c.TLSCertFile != "" && c.TLSKeyFile != "" }

// TLSConfig loads the configured certificate into a *tls.Config, or returns nil
// when TLS is disabled.
func (c Config) TLSConfig() (*tls.Config, error) {
	if !c.TLSEnabled() {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("config: cannot load the TLS certificate and key (--tls-cert %s, --tls-key %s): %w", c.TLSCertFile, c.TLSKeyFile, err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

// validHostPort reports whether addr is host:port, with an empty host (":9092")
// and port 0 allowed.
func validHostPort(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if port == "" {
		return false
	}
	_, err = net.LookupPort("tcp", port)
	return err == nil
}

// validURLPrefix reports whether p is a URL path the console can be mounted under.
func validURLPrefix(p string) bool {
	if !strings.HasPrefix(p, "/") || strings.ContainsAny(p, " ?#%\\\t\n") {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." || seg == "." {
			return false
		}
	}
	return p == "/" || !strings.HasPrefix(p, "//") && p != "/healthz" && !strings.HasPrefix(p, "/healthz/")
}
