package config

import (
	"fmt"
	"io"
	"path/filepath"
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
	KafkaVersion           *int              `toml:"kafka-version"`
	Version                *int              `toml:"version"` // the earlier spelling of kafka-version; still read.
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
	Console                *bool             `toml:"console"`
	ConsoleAddr            *string           `toml:"console-addr"`
	ConsoleAllowHost       []string          `toml:"console-allow-host"`
	PprofAddr              *string           `toml:"pprof-addr"`
	Zone                   *bool             `toml:"zone"`
	LogLevel               *string           `toml:"log-level"`
	LogFormat              *string           `toml:"log-format"`
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
	if fc.KafkaVersion != nil {
		cfg.Version = *fc.KafkaVersion
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
	if fc.Console != nil {
		cfg.Console = *fc.Console
	}
	if fc.ConsoleAddr != nil {
		cfg.ConsoleAddr = *fc.ConsoleAddr
	}
	if fc.ConsoleAllowHost != nil {
		cfg.ConsoleAllowHosts = fc.ConsoleAllowHost
	}
	if fc.PprofAddr != nil {
		cfg.PprofAddr = *fc.PprofAddr
	}
	if fc.Zone != nil {
		cfg.Zone = *fc.Zone
	}
	if fc.LogLevel != nil {
		cfg.LogLevel = *fc.LogLevel
	}
	if fc.LogFormat != nil {
		cfg.LogFormat = *fc.LogFormat
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
		KafkaVersion:           &cfg.Version,
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
		Console:                &cfg.Console,
		ConsoleAddr:            &cfg.ConsoleAddr,
		Zone:                   &cfg.Zone,
		LogLevel:               &cfg.LogLevel,
		LogFormat:              &cfg.LogFormat,
	}
	// MetaDB is only written when it differs from the default, so a printed file
	// stays valid after the data directory moves.
	if cfg.MetaDB == filepath.Join(cfg.DataDir, MetaDBFile) {
		fc.MetaDB = nil
	}
	if len(cfg.ConsoleAllowHosts) > 0 {
		fc.ConsoleAllowHost = cfg.ConsoleAllowHosts
	}
	if cfg.PprofAddr != "" {
		fc.PprofAddr = &cfg.PprofAddr
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
