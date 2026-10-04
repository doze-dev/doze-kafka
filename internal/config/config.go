// Package config defines the broker's runtime configuration and its defaults.
package config

import (
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
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
	AdvertisedHost string // host returned to clients in Metadata responses.
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

	// Topics.
	AutoCreateTopics  bool
	DefaultPartitions int // partition count for auto-created topics.
	MaxPartitions     int // most partitions a single topic may have; every one is open files and a directory.

	// Auth (all optional; empty = disabled). SASL gates the listener behind a
	// handshake; TLS encrypts it. The two are independent — SASL over plaintext,
	// TLS without SASL, or both — matching how real Kafka listeners compose.
	SASLMechanisms []string          // enabled mechanisms; empty disables SASL.
	Credentials    map[string]string // username -> password.
	TLSCertFile    string            // PEM cert; with TLSKeyFile, enables TLS.
	TLSKeyFile     string            // PEM private key.

	// Console: the web UI, served over HTTP beside the broker.
	Console           bool     // serve the console.
	ConsoleAddr       string   // host:port for the console's HTTP listener.
	ConsoleAllowHosts []string // extra Host names the console answers to (DNS-rebinding guard).

	// Process.
	Zone      bool   // join the shared .doze name zone (kafka.doze).
	PprofAddr string // if set, serve net/http/pprof here.
	LogLevel  string // debug, info, warn or error.
	LogFormat string // text or json.
}

const (
	// The default listener is loopback: a development broker with no auth should
	// not be reachable from the network it happens to be plugged into. Listening
	// beyond this machine is a choice (--listen :9092) and docs/configuration.md
	// says what else to set when making it.
	defaultListenAddr   = "127.0.0.1:9092"
	defaultConsoleAddr  = "127.0.0.1:9093"
	defaultSegmentBytes = 128 << 20 // 128 MiB
	defaultIndexStride  = 4 << 10   // 4 KiB

	// defaultMaxPartitions bounds one topic, not the broker. A partition is a
	// directory and two open files, so an unbounded count lets a typo take the
	// process out: a 5,000-partition topic measured ~10,000 file descriptors and
	// a 25-30s shutdown. A real Kafka cluster rarely goes past a few hundred for
	// a single topic, and local development never needs more.
	defaultMaxPartitions = 1000
)

// Default returns a Config suitable for zero-config local development.
func Default() Config {
	return Config{
		Version:                MaxKafkaMajor,
		ListenAddr:             defaultListenAddr,
		AdvertisedHost:         "localhost",
		DataDir:                DefaultDataDir,
		MetaDB:                 "", // empty = <data-dir>/meta.db; see ResolveMetaDB.
		SegmentBytes:           defaultSegmentBytes,
		IndexIntervalBytes:     defaultIndexStride,
		FsyncInterval:          500 * time.Millisecond,
		ConsumerSessionTimeout: 45 * time.Second,
		AutoCreateTopics:       true,
		DefaultPartitions:      1,
		MaxPartitions:          defaultMaxPartitions,
		Console:                true,
		ConsoleAddr:            defaultConsoleAddr,
		Zone:                   true,
		LogLevel:               "info",
		LogFormat:              "text",
	}
}

// DefaultDataDir is where a bare `doze-kafka` keeps its data.
const DefaultDataDir = "./data"

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

// Validate reports the first problem that would prevent the broker from starting.
func (c Config) Validate() error {
	if c.Version < MinKafkaMajor || c.Version > MaxKafkaMajor {
		return fmt.Errorf("config: unsupported Kafka version %d; supported majors are %d to %d", c.Version, MinKafkaMajor, MaxKafkaMajor)
	}
	switch {
	case c.ListenAddr == "":
		return fmt.Errorf("config: listen address is empty")
	case !validHostPort(c.ListenAddr):
		return fmt.Errorf("config: listen address %q is not host:port (for example 127.0.0.1:9092 or :9092)", c.ListenAddr)
	case c.Console && !validHostPort(c.ConsoleAddr):
		return fmt.Errorf("config: console address %q is not host:port (for example 127.0.0.1:9093)", c.ConsoleAddr)
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
	case !validAdvertisedHost(c.AdvertisedHost):
		return fmt.Errorf("config: advertised-host %q is not a host name or an IP address (no scheme, port or spaces; the port is --advertised-port)", c.AdvertisedHost)
	case c.ConsumerSessionTimeout <= 0:
		return fmt.Errorf("config: consumer-session-timeout must be positive, got %s (a member would be evicted the moment it joined)", c.ConsumerSessionTimeout)
	case c.PprofAddr != "" && !validHostPort(c.PprofAddr):
		return fmt.Errorf("config: pprof address %q is not host:port (for example 127.0.0.1:6060)", c.PprofAddr)
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
			if pass == "" {
				return fmt.Errorf("config: SASL user %q has an empty password", user)
			}
		}
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return fmt.Errorf("config: TLS needs both --tls-cert and --tls-key, or neither")
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

// AuthEnabled reports whether SASL authentication is configured.
func (c Config) AuthEnabled() bool { return len(c.SASLMechanisms) > 0 }

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
