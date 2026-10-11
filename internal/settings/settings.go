// Package settings is the one table of every setting the broker, the registry and
// the console talk about: its name, unit, range, default, whether the broker acts
// on it or only keeps it, what enforces it, and where the claim comes from.
//
// One accessor reads it. DescribeConfigs, IncrementalAlterConfigs and CreateTopics
// validation in the broker, and the console's forms and teaching lines, all ask
// here, so what the page says and what the broker does cannot drift apart. A test
// fails for a topic config the broker accepts that has no record here.
//
// The table is built on first use (sync.Once), not at start-up: a broker that
// nobody asks about its settings holds none of it.
package settings

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// Kind is which family a setting belongs to.
type Kind string

const (
	Topic    Kind = "topic"    // a topic's own config, as Kafka names it.
	Broker   Kind = "broker"   // a flag or config-file key of this broker (reported under Kafka's name where it has one).
	Registry Kind = "registry" // the schema registry's compatibility levels and modes.
	Console  Kind = "console"  // a limit the console itself keeps.
	Group    Kind = "group"    // a field of the consumer-group pages.
)

// Effect says whether the broker acts on a setting.
type Effect string

const (
	Applied          Effect = "applied"            // in force here.
	AppliedCompacted Effect = "applied-if-compact" // in force on a topic whose cleanup.policy compacts.
	Stored           Effect = "stored"             // validated, kept and reported; changes nothing.
	StartupOnly      Effect = "startup-only"       // read at start; changing it needs a restart.
	NotImplemented   Effect = "not-implemented"    // named, and refused.
	Informational    Effect = "informational"      // shown by the console, not a setting anyone changes.
)

// Source is where a claim in a record comes from (plan/01-protocol.md, the
// evidence standard).
type Source string

const (
	Spec Source = "spec" // the wire protocol's own definitions.
	Doc  Source = "doc"  // Kafka's documentation or a KIP.
	Run  Source = "run"  // reproduced against a built binary or held by a test.
	Read Source = "read" // from the code.
)

// Setting is one record.
type Setting struct {
	Name string
	Kind Kind
	// Unit is "ms", "bytes", "ratio", "count", "" (none) and so on.
	Unit string
	// Range is the accepted values in words ("1024 or more", "0 to 1").
	Range string
	// Choices, when set, are the only values.
	Choices []string
	// Default is this broker's default as DescribeConfigs reports it, or nil when
	// the config has none to report (it is shown only when the topic sets it).
	Default func(config.Config) string
	// DefaultNote says the default when it is not a plain value ("no limit").
	DefaultNote string
	// KafkaDefault is Apache Kafka's own default, when it differs from here.
	KafkaDefault string
	Effect       Effect
	// Enforcer names what acts on it ("log retention sweep", "produce handler").
	Enforcer string
	Source   Source
	// Flag and Key are the command-line flag and config-file key, for a Broker setting.
	Flag, Key string
	// Meaning is one sentence for a person.
	Meaning string
	// List marks a comma-separated list value, the only kind AppendTo / SubtractFrom may change.
	List bool
	// Wire marks a Broker record DescribeConfigs reports for the broker resource
	// under Name (Kafka's own name for it).
	Wire bool
	// Check validates a value; nil accepts any.
	Check func(value string) error
}

// Teach is the line shown beneath a field: unit, range, default, and whether the
// broker acts on it, generated from the record and never typed into a template.
func (s Setting) Teach(cfg config.Config) string {
	var parts []string
	if s.Unit != "" {
		parts = append(parts, "unit: "+s.Unit)
	}
	switch {
	case len(s.Choices) > 0:
		parts = append(parts, "one of "+strings.Join(s.Choices, ", "))
	case s.Range != "":
		parts = append(parts, "range: "+s.Range)
	}
	switch d, ok := s.DefaultValue(cfg); {
	case ok:
		parts = append(parts, "default: "+d)
	}
	if s.KafkaDefault != "" {
		parts = append(parts, "Kafka's default: "+s.KafkaDefault)
	}
	parts = append(parts, s.Effect.Phrase())
	return strings.Join(parts, "; ")
}

// Rule is Teach without the broker's own default, for a reader that does not
// know the flags the broker was started with (the console shows the effective
// value beside it): unit, range, Kafka's default and whether the broker acts.
func (s Setting) Rule() string {
	var parts []string
	if s.Unit != "" {
		parts = append(parts, "unit: "+s.Unit)
	}
	switch {
	case len(s.Choices) > 0:
		parts = append(parts, "one of "+strings.Join(s.Choices, ", "))
	case s.Range != "":
		parts = append(parts, "range: "+s.Range)
	}
	if s.DefaultNote != "" {
		parts = append(parts, "default: "+s.DefaultNote)
	}
	if s.KafkaDefault != "" {
		parts = append(parts, "Kafka's default: "+s.KafkaDefault)
	}
	parts = append(parts, s.Effect.Phrase())
	return strings.Join(parts, "; ")
}

// DefaultValue is the default for cfg, and whether there is one to state.
func (s Setting) DefaultValue(cfg config.Config) (string, bool) {
	switch {
	case s.Default != nil:
		return s.Default(cfg), true
	case s.DefaultNote != "":
		return s.DefaultNote, true
	}
	return "", false
}

// Phrase is the effect in words, as the console says it.
func (e Effect) Phrase() string {
	switch e {
	case Applied:
		return "applied"
	case AppliedCompacted:
		return "applied on a compacted topic, stored otherwise"
	case Stored:
		return "stored only: it changes nothing here"
	case StartupOnly:
		return "set at start-up; a restart changes it"
	case NotImplemented:
		return "not implemented"
	case Informational:
		return "shown, not a setting"
	}
	return string(e)
}

// Acts reports whether the broker does something with the setting.
func (e Effect) Acts() bool { return e == Applied || e == AppliedCompacted || e == StartupOnly }

var (
	once  sync.Once
	byKey map[string]Setting
	order []Setting
)

func build() {
	all := slicesConcat(topicSettings(), brokerSettings(), registrySettings(), consoleSettings(), groupSettings())
	byKey = make(map[string]Setting, len(all))
	for _, s := range all {
		k := key(s.Kind, s.Name)
		if _, dup := byKey[k]; dup {
			panic("settings: duplicate record " + k)
		}
		byKey[k] = s
	}
	order = all
}

func slicesConcat(parts ...[]Setting) []Setting {
	var out []Setting
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func key(k Kind, name string) string { return string(k) + "\x00" + name }

// Lookup is the accessor: the record for a setting, building the table on first use.
func Lookup(kind Kind, name string) (Setting, bool) {
	once.Do(build)
	s, ok := byKey[key(kind, name)]
	return s, ok
}

// TopicConfig is Lookup for a topic config.
func TopicConfig(name string) (Setting, bool) { return Lookup(Topic, name) }

// All returns every record of a kind, in table order.
func All(kind Kind) []Setting {
	once.Do(build)
	var out []Setting
	for _, s := range order {
		if s.Kind == kind {
			out = append(out, s)
		}
	}
	return out
}

// Names returns the names of a kind, sorted.
func Names(kind Kind) []string {
	var out []string
	for _, s := range All(kind) {
		out = append(out, s.Name)
	}
	sort.Strings(out)
	return out
}

// ValidateTopic reports the first key that is not a Kafka topic config, or whose
// value Kafka would refuse. It is what CreateTopics and both alter calls run.
func ValidateTopic(config map[string]string) error {
	keys := make([]string, 0, len(config))
	for k := range config {
		keys = append(keys, k)
	}
	sort.Strings(keys) // a stable first error.
	for _, k := range keys {
		s, known := TopicConfig(k)
		if !known {
			return fmt.Errorf("unknown topic config %q", k)
		}
		if s.Check != nil {
			if err := s.Check(config[k]); err != nil {
				return fmt.Errorf("invalid value for %s: %v", k, err)
			}
		}
	}
	return nil
}

// IsListTopicConfig reports whether a topic config's value is a list, which
// IncrementalAlterConfigs may append to or subtract from.
func IsListTopicConfig(name string) bool {
	s, ok := TopicConfig(name)
	return ok && s.List
}

// ---- value checks ----

type check = func(string) error

func isInt(min int64) check {
	return func(v string) error {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("%q is not a number", v)
		}
		if n < min {
			return fmt.Errorf("%d is below the minimum of %d", n, min)
		}
		return nil
	}
}

func isFloat(min, max float64) check {
	return func(v string) error {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < min || f > max {
			return fmt.Errorf("%q is not a number between %v and %v", v, min, max)
		}
		return nil
	}
}

func isBool(v string) error {
	if v != "true" && v != "false" {
		return fmt.Errorf("%q is not true or false", v)
	}
	return nil
}

func oneOf(allowed ...string) check {
	return func(v string) error {
		for _, a := range allowed {
			if v == a {
				return nil
			}
		}
		return fmt.Errorf("%q is not one of %s", v, strings.Join(allowed, ", "))
	}
}

func cleanupPolicy(v string) error {
	seen := map[string]bool{}
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		if p != "delete" && p != "compact" || seen[p] {
			return fmt.Errorf("%q is not delete, compact, or both", v)
		}
		seen[p] = true
	}
	return nil
}

// MinSegmentBytes is the smallest per-topic segment size. Kafka itself asks for a
// megabyte since 4.0; this is lower, so retention can be tried out on a handful
// of records, but not so low that every record becomes a file of its own: each
// segment is two open files for as long as its partition is in use.
const MinSegmentBytes = 1024

// MaxRetentionMs is the longest retention that fits in a time.Duration.
const MaxRetentionMs = math.MaxInt64 / int64(time.Millisecond)

func retentionMs(v string) error {
	if err := isInt(-1)(v); err != nil {
		return err
	}
	if n, _ := strconv.ParseInt(v, 10, 64); n > MaxRetentionMs {
		return fmt.Errorf("%d is too large (the most is %d, about 292 years); use -1 to keep records for ever", n, MaxRetentionMs)
	}
	return nil
}

// limit renders Kafka's way of saying "no bound": -1, where the config uses 0.
func limit(v int64) string {
	if v <= 0 {
		return "-1"
	}
	return strconv.FormatInt(v, 10)
}

// ---- topic configs ----

func topicSettings() []Setting {
	t := func(name, unit, rng string, c check, eff Effect, enforcer, meaning string) Setting {
		return Setting{Name: name, Kind: Topic, Unit: unit, Range: rng, Check: c, Effect: eff, Enforcer: enforcer, Source: Spec, Meaning: meaning}
	}
	stored := func(name, unit, rng string, c check, meaning string) Setting {
		return t(name, unit, rng, c, Stored, "", meaning)
	}
	compacted := func(name, unit, rng string, c check, meaning string) Setting {
		return t(name, unit, rng, c, AppliedCompacted, "log cleaner", meaning)
	}
	choice := func(s Setting, choices ...string) Setting { s.Choices = choices; return s }
	def := func(s Setting, f func(config.Config) string) Setting { s.Default = f; return s }

	out := []Setting{
		func() Setting {
			s := compacted("cleanup.policy", "", "delete, compact, or both", cleanupPolicy, "what the broker does with old records")
			s.Effect = Applied
			s.Enforcer = "retention sweep (delete), log cleaner (compact)"
			s.List = true
			s.Default = func(config.Config) string { return "delete" }
			return s
		}(),
		choice(stored("compression.type", "", "", oneOf("uncompressed", "zstd", "lz4", "snappy", "gzip", "producer"), "the codec of records the cleaner rewrites"),
			"uncompressed", "zstd", "lz4", "snappy", "gzip", "producer"),
		stored("compression.gzip.level", "", "-1 or more", isInt(-1), "gzip level when the broker compresses"),
		stored("compression.lz4.level", "", "1 or more", isInt(1), "lz4 level when the broker compresses"),
		stored("compression.zstd.level", "", "-131072 or more", isInt(-131072), "zstd level when the broker compresses"),
		compacted("delete.retention.ms", "ms", "0 or more", isInt(0), "how long a tombstone survives a compaction pass"),
		stored("file.delete.delay.ms", "ms", "0 or more", isInt(0), "delay before a deleted segment's file is removed"),
		stored("flush.messages", "records", "1 or more", isInt(1), "force an fsync after this many records"),
		stored("flush.ms", "ms", "0 or more", isInt(0), "force an fsync after this long"),
		func() Setting {
			s := stored("follower.replication.throttled.replicas", "", "", nil, "replicas whose replication is throttled")
			s.List = true
			return s
		}(),
		def(stored("index.interval.bytes", "bytes", "0 or more", isInt(0), "how often an index entry is added"),
			func(c config.Config) string { return strconv.FormatInt(c.IndexIntervalBytes, 10) }),
		func() Setting {
			s := stored("leader.replication.throttled.replicas", "", "", nil, "replicas whose replication is throttled")
			s.List = true
			return s
		}(),
		stored("local.retention.bytes", "bytes", "-2 or more", isInt(-2), "tiered storage: size kept locally"),
		stored("local.retention.ms", "ms", "-2 or more", isInt(-2), "tiered storage: age kept locally"),
		compacted("max.compaction.lag.ms", "ms", "1 or more", isInt(1), "the longest a record may stay uncompacted"),
		func() Setting {
			s := t("max.message.bytes", "bytes", "0 or more", isInt(0), Applied, "produce handler (MESSAGE_TOO_LARGE)", "refuse a record batch larger than this")
			s.DefaultNote = "no limit below the 100 MiB request cap"
			s.KafkaDefault = "1048588"
			return s
		}(),
		stored("message.downconversion.enable", "", "true or false", isBool, "convert records for old consumers"),
		stored("message.format.version", "", "", nil, "on-disk message format (ignored since Kafka 4.0)"),
		stored("message.timestamp.after.max.ms", "ms", "0 or more", isInt(0), "refuse a record stamped too far ahead"),
		stored("message.timestamp.before.max.ms", "ms", "0 or more", isInt(0), "refuse a record stamped too far behind"),
		stored("message.timestamp.difference.max.ms", "ms", "0 or more", isInt(0), "refuse a record stamped too far from the broker's clock"),
		def(choice(t("message.timestamp.type", "", "", oneOf("CreateTime", "LogAppendTime"), Applied, "produce handler", "whose clock stamps a record"),
			"CreateTime", "LogAppendTime"), func(config.Config) string { return "CreateTime" }),
		func() Setting {
			s := compacted("min.cleanable.dirty.ratio", "ratio", "0 to 1", isFloat(0, 1), "share of the log that must be dirty before a compaction pass")
			s.DefaultNote = "0: a segment is cleaned as soon as it has rolled"
			s.KafkaDefault = "0.5"
			return s
		}(),
		compacted("min.compaction.lag.ms", "ms", "0 or more", isInt(0), "the shortest a record stays uncompacted"),
		stored("min.insync.replicas", "replicas", "1 or more", isInt(1), "acks=all needs this many in-sync replicas (there is one)"),
		stored("preallocate", "", "true or false", isBool, "preallocate a segment's file"),
		stored("remote.log.copy.disable", "", "true or false", isBool, "tiered storage"),
		stored("remote.log.delete.on.disable", "", "true or false", isBool, "tiered storage"),
		stored("remote.storage.enable", "", "true or false", isBool, "tiered storage"),
		def(t("retention.bytes", "bytes", "-1 or more", isInt(-1), Applied, "retention sweep", "delete the oldest segments once a partition exceeds this (-1: no limit)"),
			func(c config.Config) string { return limit(c.RetentionBytes) }),
		func() Setting {
			s := def(t("retention.ms", "ms", "-1 or more", retentionMs, Applied, "retention sweep", "delete segments older than this (-1: keep for ever)"),
				func(c config.Config) string { return limit(c.RetentionMs) })
			s.KafkaDefault = "604800000 (7 days)"
			return s
		}(),
		func() Setting {
			s := def(t("segment.bytes", "bytes", fmt.Sprintf("%d or more", MinSegmentBytes), isInt(MinSegmentBytes), Applied, "log segment roll", "roll to a new segment at this size; retention removes whole segments"),
				func(c config.Config) string { return strconv.FormatInt(c.SegmentBytes, 10) })
			s.KafkaDefault = "1073741824 (1 GiB), minimum 1048576"
			return s
		}(),
		stored("segment.index.bytes", "bytes", "4 or more", isInt(4), "size of a segment's offset index"),
		stored("segment.jitter.ms", "ms", "0 or more", isInt(0), "random delay added to a segment roll"),
		stored("segment.ms", "ms", "1 or more", isInt(1), "roll a segment after this long"),
		stored("unclean.leader.election.enable", "", "true or false", isBool, "elect an out-of-sync replica (there is one replica)"),
	}
	for i := range out {
		if out[i].Check == nil {
			out[i].Check = func(string) error { return nil }
		}
	}
	return out
}

// ---- broker settings ----

func brokerSettings() []Setting {
	// The eight keys DescribeConfigs reports for the broker resource, under Kafka's
	// names.
	d := func(name, flag, key, unit string, f func(config.Config) string, meaning string) Setting {
		return Setting{Name: name, Kind: Broker, Wire: true, Unit: unit, Default: f, Effect: StartupOnly, Enforcer: "set with --" + flag + "; read-only over the wire",
			Source: Read, Flag: flag, Key: key, Meaning: meaning}
	}
	f := func(flag, unit, rng string, eff Effect, meaning string) Setting {
		return Setting{Name: flag, Kind: Broker, Unit: unit, Range: rng, Effect: eff, Source: Read, Flag: flag, Key: flag, Meaning: meaning}
	}
	out := []Setting{
		d("log.retention.bytes", "retention-bytes", "retention-bytes", "bytes", func(c config.Config) string { return limit(c.RetentionBytes) }, "partition size above which the oldest segments go"),
		d("log.retention.ms", "retention-ms", "retention-ms", "ms", func(c config.Config) string { return limit(c.RetentionMs) }, "age above which a segment goes"),
		d("log.segment.bytes", "segment-bytes", "segment-bytes", "bytes", func(c config.Config) string { return strconv.FormatInt(c.SegmentBytes, 10) }, "segment size"),
		d("log.index.interval.bytes", "index-interval-bytes", "index-interval-bytes", "bytes", func(c config.Config) string { return strconv.FormatInt(c.IndexIntervalBytes, 10) }, "bytes of log per sparse index entry"),
		d("num.partitions", "default-partitions", "default-partitions", "partitions", func(c config.Config) string { return strconv.Itoa(c.DefaultPartitions) }, "partitions of an auto-created topic"),
		d("auto.create.topics.enable", "auto-create-topics", "auto-create-topics", "", func(c config.Config) string { return strconv.FormatBool(c.AutoCreateTopics) }, "create an unknown topic on first reference"),
		{Name: "allow.everyone.if.no.acl.found", Kind: Broker, Wire: true, Effect: StartupOnly, Source: Doc, Flag: "acl-default", Key: "acl-default",
			Enforcer: "authorizer", Meaning: "what a resource with no ACL allows",
			Default: func(c config.Config) string { return strconv.FormatBool(c.ACLDefault != config.ACLDeny) }},
		{Name: "super.users", Kind: Broker, Wire: true, Effect: StartupOnly, Source: Doc, Flag: "acl-super-user", Key: "acl-super-user",
			Enforcer: "authorizer", Meaning: "principals ACLs never refuse",
			Default: func(c config.Config) string { return strings.Join(c.ACLSuperUsers, ";") }},
	}
	// The flags whose value the console shows (Cluster page, Connect page) with
	// their units and whether anything but a restart changes them. Names are the
	// flag's own.
	flags := []Setting{
		func() Setting {
			s := f("max-partitions", "partitions", "1 or more", StartupOnly, "most partitions one topic may have; each is a directory and open files")
			s.Default = func(c config.Config) string { return strconv.Itoa(c.MaxPartitions) }
			s.Enforcer = "CreateTopics and CreatePartitions (INVALID_PARTITIONS)"
			s.Unit = "partitions"
			return s
		}(),
		withDefault(f("fsync-interval", "duration", "0 disables", StartupOnly, "coarse fsync cadence for active segments"), func(c config.Config) string { return c.FsyncInterval.String() }),
		withDefault(f("consumer-session-timeout", "duration", "", StartupOnly, "KIP-848 group session timeout before a silent member is evicted"), func(c config.Config) string { return c.ConsumerSessionTimeout.String() }),
		withDefault(f("kafka-version", "major version", "1 to 4", StartupOnly, "the Kafka major version emulated"), func(c config.Config) string { return strconv.Itoa(c.Version) }),
		withDefault(f("log-level", "", "debug, info, warn, error", StartupOnly, "log verbosity"), func(c config.Config) string { return c.LogLevel }),
		withDefault(f("client-telemetry", "", "true or false", StartupOnly, "accept the metrics clients push about themselves (KIP-714)"), func(c config.Config) string { return strconv.FormatBool(c.ClientTelemetry) }),
		withDefault(f("schema-registry", "", "true or false", StartupOnly, "serve the built-in schema registry"), func(c config.Config) string { return strconv.FormatBool(c.SchemaRegistry) }),
		withDefault(f("console", "", "true or false", StartupOnly, "serve the web console"), func(c config.Config) string { return strconv.FormatBool(c.Console) }),
		f("listen", "host:port", "", StartupOnly, "serve on this address instead of the .doze name"),
		f("data-dir", "path", "", StartupOnly, "directory for partition logs"),
		f("trace-header", "header name", "", StartupOnly, "a record header holding a trace id"),
		f("sasl-mechanisms", "", "PLAIN, SCRAM-SHA-256, SCRAM-SHA-512", StartupOnly, "SASL mechanisms enabled"),
		f("tls-cert", "path", "", StartupOnly, "PEM certificate; with --tls-key enables TLS"),
	}
	flags[0].Name = "max-partitions"
	out = append(out, flags...)
	return out
}

func withDefault(s Setting, f func(config.Config) string) Setting { s.Default = f; return s }

// ---- registry ----

// Levels and modes of the built-in schema registry, as Confluent names them.
func registrySettings() []Setting {
	lvl := func(name, meaning string) Setting {
		return Setting{Name: name, Kind: Registry, Effect: Applied, Enforcer: "registry compatibility check", Source: Run, Meaning: meaning}
	}
	mode := func(name string, eff Effect, meaning string) Setting {
		return Setting{Name: "mode:" + name, Kind: Registry, Effect: eff, Enforcer: "registry write path", Source: Run, Meaning: meaning}
	}
	return []Setting{
		lvl("BACKWARD", "a new version can read data written with the previous one (the default)"),
		lvl("BACKWARD_TRANSITIVE", "a new version can read data written with every earlier one"),
		lvl("FORWARD", "the previous version can read data written with the new one"),
		lvl("FORWARD_TRANSITIVE", "every earlier version can read data written with the new one"),
		lvl("FULL", "both BACKWARD and FORWARD, against the previous version"),
		lvl("FULL_TRANSITIVE", "both BACKWARD and FORWARD, against every earlier version"),
		lvl("NONE", "no check: any schema registers"),
		mode("READWRITE", Applied, "registrations and deletes are accepted (the default)"),
		mode("READONLY", Applied, "registrations and deletes are refused with error_code 42205"),
		mode("IMPORT", NotImplemented, "Confluent's mode for loading schemas with their ids; not offered here"),
	}
}

// CompatLevels is the registry's compatibility levels in the order shown.
func CompatLevels() []string {
	var out []string
	for _, s := range All(Registry) {
		if !strings.HasPrefix(s.Name, "mode:") {
			out = append(out, s.Name)
		}
	}
	return out
}

// ---- console ----

func consoleSettings() []Setting {
	c := func(name, unit, def, meaning string) Setting {
		return Setting{Name: name, Kind: Console, Unit: unit, DefaultNote: def, Effect: Informational, Enforcer: "console", Source: Read, Meaning: meaning}
	}
	return []Setting{
		c("trace.max-records", "records", "50000", "the most records one lineage scan reads"),
		c("trace.max-time", "seconds", "8", "the longest one lineage scan runs"),
		c("request.timeout", "seconds", "15", "the deadline of one console request, broker calls included"),
		c("traffic.ring", "requests", "2000", "requests the traffic page remembers"),
		c("map.live-window", "seconds", "60", "an edge is live when its last request is this recent"),
		c("map.recent-window", "minutes", "15", "an edge is recent when its last request is this recent"),
		c("map.keep", "minutes", "60", "a wire edge is dropped this long after its last request"),
		c("map.edges", "edges", "2000", "wire edges kept; the least recently seen is evicted"),
	}
}

// ---- group fields ----

func groupSettings() []Setting {
	g := func(name, unit, rng string, eff Effect, meaning string) Setting {
		return Setting{Name: name, Kind: Group, Unit: unit, Range: rng, Effect: eff, Source: Read, Meaning: meaning, Enforcer: "group coordinator"}
	}
	return []Setting{
		func() Setting {
			s := g("reset-to", "", "earliest, latest, timestamp or specific", Applied, "where a group's offsets move to")
			s.Choices = []string{"earliest", "latest", "timestamp", "specific"}
			return s
		}(),
		g("new-offset", "offset", "log start to log end", Applied, "the committed offset to set for one partition"),
		g("session-timeout", "ms", "per client", Informational, "how long a member may be silent; set by the client, shown here"),
		g("rebalance-timeout", "ms", "per client", Informational, "how long a rebalance waits for members; set by the client, shown here"),
		g("consumer-session-timeout", "duration", "", StartupOnly, "the broker's own session timeout for KIP-848 members (--consumer-session-timeout)"),
	}
}
