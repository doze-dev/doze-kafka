package config

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"regexp"
	"strconv"
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
	KafkaVersion               *int              `toml:"kafka-version"`
	Version                    *int              `toml:"version"` // the earlier spelling of kafka-version; still read.
	Listen                     *string           `toml:"listen"`
	AdvertisedHost             *string           `toml:"advertised-host"`
	AdvertisedPort             *int              `toml:"advertised-port"`
	DataDir                    *string           `toml:"data-dir"`
	MetaDB                     *string           `toml:"meta-db"`
	SegmentBytes               *int64            `toml:"segment-bytes"`
	IndexIntervalBytes         *int64            `toml:"index-interval-bytes"`
	RetentionBytes             *int64            `toml:"retention-bytes"`
	RetentionMs                *int64            `toml:"retention-ms"`
	FsyncInterval              *string           `toml:"fsync-interval"`
	ConsumerSessionTimeout     *string           `toml:"consumer-session-timeout"`
	GroupInitialRebalanceDelay *string           `toml:"group-initial-rebalance-delay"`
	OffsetsRetention           *string           `toml:"offsets-retention"`
	AutoCreateTopics           *bool             `toml:"auto-create-topics"`
	DefaultPartitions          *int              `toml:"default-partitions"`
	MaxPartitions              *int              `toml:"max-partitions"`
	MessageMaxBytes            *int64            `toml:"message-max-bytes"`
	SASLMechanisms             []string          `toml:"sasl-mechanisms"`
	Credentials                map[string]string `toml:"credentials"`
	TLS                        *tlsFile          `toml:"tls"`
	ACLDefault                 *string           `toml:"acl-default"`
	ACLSuperUsers              []string          `toml:"acl-super-user"`
	Console                    *bool             `toml:"console"`
	ConsoleAddr                *string           `toml:"console-addr"`
	ConsolePrefix              *string           `toml:"console-prefix"`
	ConsoleAllowHost           []string          `toml:"console-allow-host"`
	ConsoleUnprotected         *bool             `toml:"console-unprotected"`
	TraceHeader                *string           `toml:"trace-header"`
	ClientTelemetry            *bool             `toml:"client-telemetry"`
	Zone                       *bool             `toml:"zone"`
	SchemaRegistry             *bool             `toml:"schema-registry"`
	SchemaRegistryAddr         *string           `toml:"schema-registry-addr"`
	SchemaRegistryTLS          *bool             `toml:"schema-registry-tls"`
	PprofAddr                  *string           `toml:"pprof-addr"`
	Name                       *string           `toml:"name"`
	LogLevel                   *string           `toml:"log-level"`
	LogFormat                  *string           `toml:"log-format"`
}

type tlsFile struct {
	Cert *string `toml:"cert"`
	Key  *string `toml:"key"`
}

// LoadFile reads a TOML config file and overlays it onto cfg: keys present in the
// file replace cfg's current values; absent keys are left as-is. Unknown keys are
// rejected so a typo ("retention_ms") fails loudly instead of being silently ignored.
//
// No error it returns repeats what the file says: see withoutValues.
func LoadFile(path string, cfg *Config) error {
	var fc fileConfig
	md, err := toml.DecodeFile(path, &fc)
	if err != nil {
		return fmt.Errorf("config: read %s: %w", path, withoutValues(err))
	}
	// The TOML library takes any value where a table of strings goes and decodes
	// nothing from it: `credentials = "hunter2"` loaded as a broker with no users.
	if md.IsDefined("credentials") && md.Type("credentials") != "Hash" {
		return fmt.Errorf("config: %s: credentials is not a table: write [credentials] and under it one user = \"password\" line for each user%s", path, notRepeated)
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		keys := make([]string, len(undecoded))
		for i, k := range undecoded {
			keys[i] = k.String()
		}
		return fmt.Errorf("config: %s: unknown key(s): %s", path, unknownKeys(keys))
	}
	// A user with an empty password is refused by Validate, which names the user. Here
	// the name may be a password typed in the wrong place ("hunter2" = ""), so the file
	// refuses it first and names nobody.
	empty := 0
	for _, pass := range fc.Credentials {
		if pass == "" {
			empty++
		}
	}
	if empty > 0 {
		noun := "user under [credentials] has"
		if empty > 1 {
			noun = "users under [credentials] have"
		}
		return fmt.Errorf("config: %s: %d %s an empty password (not named: a name there may be a password)%s", path, empty, noun, notRepeated)
	}
	if err := fc.applyTo(cfg); err != nil {
		return fmt.Errorf("config: %s: %w", path, err)
	}
	return nil
}

// unknownKeys lists keys the file has and the configuration does not. A key is named,
// because its name is how the typo in it is found; one under [credentials] is counted
// instead (every key there is a user's name, so none should be unknown, and a name
// there may be a password typed in the wrong place).
func unknownKeys(keys []string) string {
	var named []string
	hidden := 0
	for _, k := range keys {
		if underCredentials(k) {
			hidden++
			continue
		}
		named = append(named, k)
	}
	if hidden > 0 {
		named = append(named, fmt.Sprintf("%d under [credentials] (not named: a name there may be a password)", hidden))
	}
	return strings.Join(named, ", ")
}

// underCredentials says a dotted key is the credentials table or inside it.
func underCredentials(key string) bool {
	return key == "credentials" || strings.HasPrefix(key, "credentials.")
}

// durationOf reads a duration written in the file under key.
func durationOf(key, written string) (time.Duration, error) {
	d, err := time.ParseDuration(written)
	if err != nil {
		// time's own error quotes what was written.
		return 0, fmt.Errorf("%s: not a duration (one is written \"500ms\" or \"45s\")%s", key, notRepeated)
	}
	return d, nil
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
		d, err := durationOf("fsync-interval", *fc.FsyncInterval)
		if err != nil {
			return err
		}
		cfg.FsyncInterval = d
	}
	if fc.ConsumerSessionTimeout != nil {
		d, err := durationOf("consumer-session-timeout", *fc.ConsumerSessionTimeout)
		if err != nil {
			return err
		}
		cfg.ConsumerSessionTimeout = d
	}
	if fc.GroupInitialRebalanceDelay != nil {
		d, err := time.ParseDuration(*fc.GroupInitialRebalanceDelay)
		if err != nil {
			return fmt.Errorf("config: group-initial-rebalance-delay: %w", err)
		}
		cfg.GroupInitialRebalanceDelay = d
	}
	if fc.OffsetsRetention != nil {
		d, err := durationOf("offsets-retention", *fc.OffsetsRetention)
		if err != nil {
			return err
		}
		cfg.OffsetsRetention = d
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
	if fc.MessageMaxBytes != nil {
		cfg.MessageMaxBytes = *fc.MessageMaxBytes
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
	if fc.ACLDefault != nil {
		cfg.ACLDefault = *fc.ACLDefault
	}
	if fc.ACLSuperUsers != nil {
		cfg.ACLSuperUsers = fc.ACLSuperUsers
	}
	if fc.Console != nil {
		cfg.Console = *fc.Console
	}
	if fc.ConsoleAddr != nil {
		cfg.ConsoleAddr = *fc.ConsoleAddr
	}
	if fc.SchemaRegistry != nil {
		cfg.SchemaRegistry = *fc.SchemaRegistry
	}
	if fc.SchemaRegistryAddr != nil {
		cfg.SchemaRegistryAddr = *fc.SchemaRegistryAddr
	}
	if fc.SchemaRegistryTLS != nil {
		cfg.SchemaRegistryTLS = *fc.SchemaRegistryTLS
	}
	if fc.ConsoleUnprotected != nil {
		cfg.ConsoleUnprotected = *fc.ConsoleUnprotected
	}
	if fc.ClientTelemetry != nil {
		cfg.ClientTelemetry = *fc.ClientTelemetry
	}
	if fc.Zone != nil {
		cfg.Zone = *fc.Zone
	}
	if fc.TraceHeader != nil {
		cfg.TraceHeader = *fc.TraceHeader
	}
	if fc.ConsolePrefix != nil {
		cfg.ConsolePrefix = *fc.ConsolePrefix
	}
	if fc.ConsoleAllowHost != nil {
		cfg.ConsoleAllowHosts = fc.ConsoleAllowHost
	}
	if fc.PprofAddr != nil {
		cfg.PprofAddr = *fc.PprofAddr
	}
	if fc.Name != nil {
		cfg.Name = *fc.Name
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

// Redacted is what a password is printed as: `config print` does not write a secret
// unless it is asked to (--show-secrets), and a file that still holds this where a
// password goes is refused where it is loaded, by user, so the placeholder can never
// become a password.
const Redacted = "<redacted>"

// WriteTOML renders cfg as a TOML document, the effective configuration in the shape
// LoadFile reads, with every password written as Redacted. Used by
// `doze-kafka config print`.
func WriteTOML(w io.Writer, cfg Config) error { return writeTOML(w, cfg, false) }

// WriteTOMLWithSecrets is WriteTOML with the passwords in clear, so its output is a file
// the broker can be started from. It is what `config print --show-secrets` writes.
func WriteTOMLWithSecrets(w io.Writer, cfg Config) error { return writeTOML(w, cfg, true) }

func writeTOML(w io.Writer, cfg Config, secrets bool) error {
	fc := fileConfig{
		KafkaVersion:               &cfg.Version,
		Listen:                     &cfg.ListenAddr,
		AdvertisedHost:             &cfg.AdvertisedHost,
		AdvertisedPort:             &cfg.AdvertisedPort,
		DataDir:                    &cfg.DataDir,
		MetaDB:                     &cfg.MetaDB,
		SegmentBytes:               &cfg.SegmentBytes,
		IndexIntervalBytes:         &cfg.IndexIntervalBytes,
		RetentionBytes:             &cfg.RetentionBytes,
		RetentionMs:                &cfg.RetentionMs,
		FsyncInterval:              strp(cfg.FsyncInterval.String()),
		ConsumerSessionTimeout:     strp(cfg.ConsumerSessionTimeout.String()),
		OffsetsRetention:           strp(cfg.OffsetsRetention.String()),
		GroupInitialRebalanceDelay: strp(cfg.GroupInitialRebalanceDelay.String()),
		AutoCreateTopics:           &cfg.AutoCreateTopics,
		DefaultPartitions:          &cfg.DefaultPartitions,
		MaxPartitions:              &cfg.MaxPartitions,
		MessageMaxBytes:            &cfg.MessageMaxBytes,
		Console:                    &cfg.Console,
		ConsoleAddr:                &cfg.ConsoleAddr,
		TraceHeader:                &cfg.TraceHeader,
		ClientTelemetry:            &cfg.ClientTelemetry,
		Zone:                       &cfg.Zone,
		SchemaRegistry:             &cfg.SchemaRegistry,
		SchemaRegistryAddr:         &cfg.SchemaRegistryAddr,
		SchemaRegistryTLS:          &cfg.SchemaRegistryTLS,
		ConsoleUnprotected:         &cfg.ConsoleUnprotected,
		Name:                       &cfg.Name,
		LogLevel:                   &cfg.LogLevel,
		LogFormat:                  &cfg.LogFormat,
	}
	// MetaDB is only written when it differs from the default, so a printed file
	// stays valid after the data directory moves.
	if cfg.MetaDB == filepath.Join(cfg.DataDir, MetaDBFile) {
		fc.MetaDB = nil
	}
	if len(cfg.ConsoleAllowHosts) > 0 {
		fc.ConsoleAllowHost = cfg.ConsoleAllowHosts
	}
	prefix := cfg.ConsolePrefix
	if prefix == "" {
		prefix = DefaultConsolePrefix
	}
	fc.ConsolePrefix = &prefix
	fc.ACLDefault = &cfg.ACLDefault
	if len(cfg.ACLSuperUsers) > 0 {
		fc.ACLSuperUsers = cfg.ACLSuperUsers
	}
	if cfg.PprofAddr != "" {
		fc.PprofAddr = &cfg.PprofAddr
	}
	if len(cfg.SASLMechanisms) > 0 {
		fc.SASLMechanisms = cfg.SASLMechanisms
	}
	if len(cfg.Credentials) > 0 {
		fc.Credentials = cfg.Credentials
		if !secrets {
			fc.Credentials = make(map[string]string, len(cfg.Credentials))
			for user := range cfg.Credentials {
				fc.Credentials[user] = Redacted
			}
		}
	}
	if cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" {
		fc.TLS = &tlsFile{Cert: &cfg.TLSCertFile, Key: &cfg.TLSKeyFile}
	}
	if err := toml.NewEncoder(w).Encode(fc); err != nil {
		return err
	}
	if fc.MetaDB == nil {
		// Said, as a comment: written as a key it would stay behind if data-dir were
		// changed in the printed file, and the database would no longer be beside
		// the data it describes.
		_, err := fmt.Fprintf(w, "# meta-db = %q  (unset: <data-dir>/%s, following data-dir)\n", filepath.Join(cfg.DataDir, MetaDBFile), MetaDBFile)
		return err
	}
	return nil
}

func strp(s string) *string { return &s }

// notRepeated ends every error about what a config file says.
const notRepeated = "; what was written is not repeated, because this file can hold passwords"

var (
	// decodeError is the text of an error the TOML library gives for a value of the
	// wrong type, which is not a toml.ParseError and carries its line and key only so.
	decodeError = regexp.MustCompile(`(?s)^toml: line (\d+) \(last key "(.*?)"\): (.*)$`)
	// incompatible is the library saying which type it found and which it wanted.
	notATable    = regexp.MustCompile(`^type mismatch for \S+: expected table but found (\S+)$`)
	incompatible = regexp.MustCompile(`^incompatible types: TOML value has type (\S+); destination has type (\S+)$`)
)

// tomlTypes are the library's names for a type, as a person would say them.
var tomlTypes = map[string]string{
	"string": "a string", "int64": "a number", "integer": "a number", "float64": "a number", "float": "a number",
	"bool": "true or false", "boolean": "true or false", "[]any": "a list", "slice": "a list", "array": "a list",
	"map[string]any": "a table", "map": "a table", "struct": "a table", "[]map[string]any": "a list of tables",
}

// withoutValues is the error for a file that could not be read as the configuration,
// with nothing of what the file says in it. The TOML library quotes what it found
// ("expected value but found \"hunter2\" instead", "but got 'h' instead"), and the
// file that fails this way is very often one with a password in the wrong place:
// unquoted, on a line of its own, where the user's name goes. So the line is given,
// the key is named, and the library's sentence is replaced by one of three that say
// what kind of mistake it is. Under [credentials] the key is not named either: it is
// where a user's name goes, and a password on a line of its own is read as one.
func withoutValues(err error) error {
	var notThere *fs.PathError
	if errors.As(err, &notThere) {
		return err // about the path, which the caller chose; nothing was read.
	}
	line, key, said := 0, "", err.Error()
	var pe toml.ParseError
	if errors.As(err, &pe) {
		line, key, said = pe.Position.Line, pe.LastKey, pe.Message
	} else if m := decodeError.FindStringSubmatch(said); m != nil {
		line, _ = strconv.Atoi(m[1])
		key, said = m[2], m[3]
	}

	what := "this is not valid TOML (a string is written in quotes)"
	if m := incompatible.FindStringSubmatch(said); m != nil {
		found, wanted := tomlTypes[m[1]], tomlTypes[m[2]]
		if found == "" {
			found = "of another type"
		}
		if wanted == "" {
			wanted = "another type"
		}
		what = "the value is " + found + " where " + wanted + " is expected"
	} else if m := notATable.FindStringSubmatch(said); m != nil {
		found := tomlTypes[m[1]]
		if found == "" {
			found = "of another type"
		}
		what = "the value is " + found + " where a table is expected"
	} else if strings.Contains(said, "has already been defined") {
		what = "the key is set twice"
	} else if strings.Contains(said, "out of range") || strings.Contains(said, "out of the safe") {
		what = "the number is too large for this key"
	}

	where := ""
	if line > 0 {
		where = " line " + strconv.Itoa(line)
	}
	switch {
	case underCredentials(key):
		where += ` (under [credentials], where an entry is user = "password")`
	case key != "":
		where += fmt.Sprintf(" (key %q)", key)
	}
	return fmt.Errorf("toml:%s: %s%s", where, what, notRepeated)
}
