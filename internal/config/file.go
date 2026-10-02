package config

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// DefaultConfigFile is loaded automatically from the working directory when no
// --config flag is given and the file exists.
const DefaultConfigFile = "doze-kafka.toml"

// fileConfig is the on-disk (TOML) shape of Config. Its keys match the command-line
// flags one-to-one. Every scalar is a pointer (and slices/maps are nil-able) so a
// key that is absent from the file leaves the corresponding Config value untouched —
// that is what makes the flags > file > defaults precedence work: the file overlays
// only what it names. Durations are written as human strings ("500ms", "45s").
type fileConfig struct {
	Version                *int              `toml:"version"`
	Listen                 *string           `toml:"listen"`
	AdvertisedHost         *string           `toml:"advertised-host"`
	AdvertisedPort         *int              `toml:"advertised-port"`
	DataDir                *string           `toml:"data-dir"`
	MetaDB                 *string           `toml:"meta-db"`
	SegmentBytes           *int64            `toml:"segment-bytes"`
	IndexIntervalBytes     *int64            `toml:"index-interval-bytes"`
	RetentionBytes         *int64            `toml:"retention-bytes"`
	RetentionMs            *int64            `toml:"retention-ms"`
	FsyncInterval          *string           `toml:"fsync-interval"`
	ConsumerSessionTimeout *string           `toml:"consumer-session-timeout"`
	AutoCreateTopics       *bool             `toml:"auto-create-topics"`
	DefaultPartitions      *int              `toml:"default-partitions"`
	MaxPartitions          *int              `toml:"max-partitions"`
	SASLMechanisms         []string          `toml:"sasl-mechanisms"`
	Credentials            map[string]string `toml:"credentials"`
	TLS                    *tlsFile          `toml:"tls"`
}

type tlsFile struct {
	Cert *string `toml:"cert"`
	Key  *string `toml:"key"`
}

// LoadFile reads a TOML config file and overlays it onto cfg: keys present in the
// file replace cfg's current values; absent keys are left as-is. Unknown keys are
// rejected so a typo ("retention_ms") fails loudly instead of being silently ignored.
func LoadFile(path string, cfg *Config) error {
	var fc fileConfig
	md, err := toml.DecodeFile(path, &fc)
	if err != nil {
		return fmt.Errorf("config: read %s: %w", path, err)
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		keys := make([]string, len(undecoded))
		for i, k := range undecoded {
			keys[i] = k.String()
		}
		return fmt.Errorf("config: %s: unknown key(s): %s", path, strings.Join(keys, ", "))
	}
	return fc.applyTo(cfg)
}

func (fc fileConfig) applyTo(cfg *Config) error {
	if fc.Version != nil {
		cfg.Version = *fc.Version
	}
	if fc.Listen != nil {
		cfg.ListenAddr = *fc.Listen
	}
	if fc.AdvertisedHost != nil {
		cfg.AdvertisedHost = *fc.AdvertisedHost
	}
	if fc.AdvertisedPort != nil {
		cfg.AdvertisedPort = *fc.AdvertisedPort
	}
	if fc.DataDir != nil {
		cfg.DataDir = *fc.DataDir
	}
	if fc.MetaDB != nil {
		cfg.MetaDB = *fc.MetaDB
	}
	if fc.SegmentBytes != nil {
		cfg.SegmentBytes = *fc.SegmentBytes
	}
	if fc.IndexIntervalBytes != nil {
		cfg.IndexIntervalBytes = *fc.IndexIntervalBytes
	}
	if fc.RetentionBytes != nil {
		cfg.RetentionBytes = *fc.RetentionBytes
	}
	if fc.RetentionMs != nil {
		cfg.RetentionMs = *fc.RetentionMs
	}
	if fc.FsyncInterval != nil {
		d, err := time.ParseDuration(*fc.FsyncInterval)
		if err != nil {
			return fmt.Errorf("config: fsync-interval: %w", err)
		}
		cfg.FsyncInterval = d
	}
	if fc.ConsumerSessionTimeout != nil {
		d, err := time.ParseDuration(*fc.ConsumerSessionTimeout)
		if err != nil {
			return fmt.Errorf("config: consumer-session-timeout: %w", err)
		}
		cfg.ConsumerSessionTimeout = d
	}
	if fc.AutoCreateTopics != nil {
		cfg.AutoCreateTopics = *fc.AutoCreateTopics
	}
	if fc.DefaultPartitions != nil {
		cfg.DefaultPartitions = *fc.DefaultPartitions
	}
	if fc.MaxPartitions != nil {
		cfg.MaxPartitions = *fc.MaxPartitions
	}
	if fc.SASLMechanisms != nil {
		cfg.SASLMechanisms = fc.SASLMechanisms
	}
	if fc.Credentials != nil {
		if cfg.Credentials == nil {
			cfg.Credentials = map[string]string{}
		}
		for user, pass := range fc.Credentials {
			cfg.Credentials[user] = pass
		}
	}
	if fc.TLS != nil {
		if fc.TLS.Cert != nil {
			cfg.TLSCertFile = *fc.TLS.Cert
		}
		if fc.TLS.Key != nil {
			cfg.TLSKeyFile = *fc.TLS.Key
		}
	}
	return nil
}

// WriteTOML renders cfg as a TOML document — the effective configuration in the same
// shape LoadFile reads, so its output is a valid config file. Used by
// `doze-kafka config print`.
func WriteTOML(w io.Writer, cfg Config) error {
	fc := fileConfig{
		Version:                &cfg.Version,
		Listen:                 &cfg.ListenAddr,
		AdvertisedHost:         &cfg.AdvertisedHost,
		AdvertisedPort:         &cfg.AdvertisedPort,
		DataDir:                &cfg.DataDir,
		MetaDB:                 &cfg.MetaDB,
		SegmentBytes:           &cfg.SegmentBytes,
		IndexIntervalBytes:     &cfg.IndexIntervalBytes,
		RetentionBytes:         &cfg.RetentionBytes,
		RetentionMs:            &cfg.RetentionMs,
		FsyncInterval:          strp(cfg.FsyncInterval.String()),
		ConsumerSessionTimeout: strp(cfg.ConsumerSessionTimeout.String()),
		AutoCreateTopics:       &cfg.AutoCreateTopics,
		DefaultPartitions:      &cfg.DefaultPartitions,
		MaxPartitions:          &cfg.MaxPartitions,
	}
	if len(cfg.SASLMechanisms) > 0 {
		fc.SASLMechanisms = cfg.SASLMechanisms
	}
	if len(cfg.Credentials) > 0 {
		fc.Credentials = cfg.Credentials
	}
	if cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" {
		fc.TLS = &tlsFile{Cert: &cfg.TLSCertFile, Key: &cfg.TLSKeyFile}
	}
	return toml.NewEncoder(w).Encode(fc)
}

func strp(s string) *string { return &s }
