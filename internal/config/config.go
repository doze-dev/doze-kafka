// Package config defines the broker's runtime configuration and its defaults.
package config

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
)

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

	// Auth (all optional; empty = disabled). SASL gates the listener behind a
	// handshake; TLS encrypts it. The two are independent — SASL over plaintext,
	// TLS without SASL, or both — matching how real Kafka listeners compose.
	SASLMechanisms []string          // enabled mechanisms; empty disables SASL.
	Credentials    map[string]string // username -> password.
	TLSCertFile    string            // PEM cert; with TLSKeyFile, enables TLS.
	TLSKeyFile     string            // PEM private key.
}

const (
	defaultListenAddr   = ":9092"
	defaultSegmentBytes = 128 << 20 // 128 MiB
	defaultIndexStride  = 4 << 10   // 4 KiB
)

// Default returns a Config suitable for zero-config local development.
func Default() Config {
	return Config{
		Version:                protocol.DefaultKafkaMajor,
		ListenAddr:             defaultListenAddr,
		AdvertisedHost:         "localhost",
		DataDir:                "./data",
		MetaDB:                 "./meta.db",
		SegmentBytes:           defaultSegmentBytes,
		IndexIntervalBytes:     defaultIndexStride,
		FsyncInterval:          500 * time.Millisecond,
		ConsumerSessionTimeout: 45 * time.Second,
		AutoCreateTopics:       true,
		DefaultPartitions:      1,
	}
}

// Validate reports the first problem that would prevent the broker from starting.
func (c Config) Validate() error {
	if _, ok := protocol.ProfileFor(c.Version); !ok {
		return fmt.Errorf("config: unsupported Kafka version %d; supported majors are %v", c.Version, protocol.SupportedKafkaMajors())
	}
	switch {
	case c.ListenAddr == "":
		return fmt.Errorf("config: listen address is empty")
	case c.DataDir == "":
		return fmt.Errorf("config: data dir is empty")
	case c.MetaDB == "":
		return fmt.Errorf("config: meta db path is empty")
	case c.SegmentBytes <= 0:
		return fmt.Errorf("config: segment-bytes must be positive, got %d", c.SegmentBytes)
	case c.IndexIntervalBytes <= 0:
		return fmt.Errorf("config: index-interval-bytes must be positive, got %d", c.IndexIntervalBytes)
	case c.DefaultPartitions <= 0:
		return fmt.Errorf("config: default-partitions must be positive, got %d", c.DefaultPartitions)
	case c.RetentionBytes < 0:
		return fmt.Errorf("config: retention-bytes must be non-negative (0 disables), got %d", c.RetentionBytes)
	case c.RetentionMs < 0:
		return fmt.Errorf("config: retention-ms must be non-negative (0 disables), got %d", c.RetentionMs)
	case c.FsyncInterval < 0:
		return fmt.Errorf("config: fsync-interval must be non-negative (0 disables), got %s", c.FsyncInterval)
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
		for user := range c.Credentials {
			if user == "" {
				return fmt.Errorf("config: SASL credential has an empty username")
			}
		}
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return fmt.Errorf("config: TLS needs both --tls-cert and --tls-key, or neither")
	}
	return nil
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
		return nil, fmt.Errorf("config: load TLS keypair: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

// Retention converts the retention config into a storage-layer policy.
func (c Config) Retention() log.RetentionPolicy {
	return log.RetentionPolicy{
		MaxBytes: c.RetentionBytes,
		MaxAge:   time.Duration(c.RetentionMs) * time.Millisecond,
	}
}
