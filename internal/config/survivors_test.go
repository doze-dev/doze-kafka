package config

// What the mutation run found nothing pinned (plan/evidence/mutation-survivors.md,
// internal/config): Validate at each boundary, the defaults' values, the name and
// header-name rules, the console's addresses under --listen, the old meta.db
// layout, and a round trip of every optional key.

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestDefaultsAreTheDocumentedValues(t *testing.T) {
	d := Default()
	if d.FsyncInterval != 500*time.Millisecond || d.OffsetsRetention != 168*time.Hour || d.ConsumerSessionTimeout != 45*time.Second || d.GroupInitialRebalanceDelay != 200*time.Millisecond {
		t.Errorf("fsync %s, offsets retention %s, session %s", d.FsyncInterval, d.OffsetsRetention, d.ConsumerSessionTimeout)
	}
	want := ""
	if runtime.GOOS == "windows" {
		want = "127.0.0.1:9092"
	}
	if d.ListenAddr != want {
		t.Errorf("default listen %q, want %q on %s", d.ListenAddr, want, runtime.GOOS)
	}
	if err := d.Validate(); err != nil {
		t.Errorf("the defaults do not validate: %v", err)
	}
}

// Each limit of Validate: the last value it takes, and the first it refuses.
func TestValidateAtEachBoundary(t *testing.T) {
	cases := []struct {
		name     string
		ok, bad  func(*Config)
		mentions string
	}{
		{"version low", func(c *Config) { c.Version = MinKafkaMajor }, func(c *Config) { c.Version = MinKafkaMajor - 1 }, "Kafka version"},
		{"version high", func(c *Config) { c.Version = MaxKafkaMajor }, func(c *Config) { c.Version = MaxKafkaMajor + 1 }, "Kafka version"},
		{"log level", func(c *Config) { c.LogLevel = "error" }, func(c *Config) { c.LogLevel = "trace" }, "log level"},
		{"log level debug", func(c *Config) { c.LogLevel = "debug" }, func(c *Config) { c.LogLevel = "" }, "log level"},
		{"log level warn", func(c *Config) { c.LogLevel = "warn" }, func(c *Config) { c.LogLevel = "WARN" }, "log level"},
		{"log format", func(c *Config) { c.LogFormat = "json" }, func(c *Config) { c.LogFormat = "xml" }, "log format"},
		{"segment", func(c *Config) { c.SegmentBytes = 1 }, func(c *Config) { c.SegmentBytes = 0 }, "segment-bytes"},
		{"index", func(c *Config) { c.IndexIntervalBytes = 1 }, func(c *Config) { c.IndexIntervalBytes = 0 }, "index-interval-bytes"},
		{"partitions", func(c *Config) { c.DefaultPartitions = 1 }, func(c *Config) { c.DefaultPartitions = 0 }, "default-partitions"},
		{"max partitions", func(c *Config) { c.MaxPartitions, c.DefaultPartitions = 1, 1 }, func(c *Config) { c.MaxPartitions = 0 }, "max-partitions must be positive"},
		{"default above max", func(c *Config) { c.MaxPartitions, c.DefaultPartitions = 3, 3 }, func(c *Config) { c.MaxPartitions, c.DefaultPartitions = 3, 4 }, "above max-partitions"},
		{"message max", func(c *Config) { c.MessageMaxBytes = 1 }, func(c *Config) { c.MessageMaxBytes = 0 }, "message-max-bytes"},
		{"retention ms", func(c *Config) { c.RetentionMs = maxRetentionMs }, func(c *Config) { c.RetentionMs = maxRetentionMs + 1 }, "too large"},
		{"fsync zero", func(c *Config) { c.FsyncInterval = 0 }, func(c *Config) { c.FsyncInterval = -1 }, "fsync-interval"},
		{"fsync a millisecond", func(c *Config) { c.FsyncInterval = time.Millisecond }, func(c *Config) { c.FsyncInterval = time.Millisecond - 1 }, "below 1ms"},
		{"advertised port", func(c *Config) { c.AdvertisedPort = 65535 }, func(c *Config) { c.AdvertisedPort = 65536 }, "advertised-port"},
		{"advertised port low", func(c *Config) { c.AdvertisedPort = 0 }, func(c *Config) { c.AdvertisedPort = -1 }, "advertised-port"},
		{"offsets retention", func(c *Config) { c.OffsetsRetention = 0 }, func(c *Config) { c.OffsetsRetention = -1 }, "offsets-retention"},
		{"initial rebalance delay", func(c *Config) { c.GroupInitialRebalanceDelay = 0 }, func(c *Config) { c.GroupInitialRebalanceDelay = -1 }, "group-initial-rebalance-delay"},
		{"assignment interval", func(c *Config) { c.ConsumerAssignmentInterval = 0 }, func(c *Config) { c.ConsumerAssignmentInterval = -1 }, "assignment interval"},
		{"session timeout", func(c *Config) { c.ConsumerSessionTimeout = 1 }, func(c *Config) { c.ConsumerSessionTimeout = 0 }, "consumer-session-timeout"},
		{"super user", func(c *Config) { c.ACLSuperUsers = []string{"User:a"} }, func(c *Config) { c.ACLSuperUsers = []string{"User:"} }, "Type:name"},
		{"super user type", func(c *Config) { c.ACLSuperUsers = []string{"U:a"} }, func(c *Config) { c.ACLSuperUsers = []string{":a"} }, "Type:name"},
		{"trace header", func(c *Config) { c.TraceHeader = strings.Repeat("a", 64) }, func(c *Config) { c.TraceHeader = strings.Repeat("a", 65) }, "trace header"},
		{"trace header chars", func(c *Config) { c.TraceHeader = "aZ09-_.zA" }, func(c *Config) { c.TraceHeader = "x/y" }, "trace header"},
	}
	for _, c := range cases {
		good := Default()
		c.ok(&good)
		if err := good.Validate(); err != nil {
			t.Errorf("%s: the last good value is refused: %v", c.name, err)
		}
		bad := Default()
		c.bad(&bad)
		if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), c.mentions) {
			t.Errorf("%s: the first bad value: %v, want an error naming %q", c.name, err, c.mentions)
		}
	}
	// Every character class a header name takes, one at a time at its edges.
	for _, r := range "azAZ09-_." {
		if !validHeaderName(string(r)) {
			t.Errorf("%q refused as a header name", r)
		}
	}
	for _, r := range "`{@[/:" {
		if validHeaderName(string(r)) {
			t.Errorf("%q taken as a header name", r)
		}
	}
	if validHeaderName("") {
		t.Error("an empty header name is taken")
	}
}

func TestInstanceNamesAndDerivedNames(t *testing.T) {
	if (Config{}).InstanceName() != DefaultName || (Config{Name: "shop"}).InstanceName() != "shop" {
		t.Error("InstanceName")
	}
	for dir, want := range map[string]string{"/x/shop": "shop", "/x/~": DefaultName, "/x/a": "a", "/x/z": "z", "/x/A": "A", "/x/Z": "Z", "/x/0": "0", "/x/9": "9", "/x/@[`{": DefaultName} {
		if got := DeriveName(dir); got != want {
			t.Errorf("DeriveName(%q) = %q, want %q", dir, got, want)
		}
	}
}

func TestConsoleAddressesUnderListen(t *testing.T) {
	c := Config{ListenAddr: "127.0.0.1:0"}
	c.ResolveConsoleAddr()
	if c.ConsoleAddr != listenModeAnyPort || c.SchemaRegistryAddr != listenModeAnyPort {
		t.Errorf("port 0: %q %q", c.ConsoleAddr, c.SchemaRegistryAddr)
	}
	c = Config{ListenAddr: "127.0.0.1:9092"}
	c.ResolveConsoleAddr()
	if c.ConsoleAddr != listenModeConsole || c.SchemaRegistryAddr != listenModeRegistry {
		t.Errorf("fixed port: %q %q", c.ConsoleAddr, c.SchemaRegistryAddr)
	}
	c = Config{ListenAddr: "127.0.0.1:0", ConsoleAddr: "1.2.3.4:5", SchemaRegistryAddr: "1.2.3.4:6"}
	c.ResolveConsoleAddr()
	if c.ConsoleAddr != "1.2.3.4:5" || c.SchemaRegistryAddr != "1.2.3.4:6" {
		t.Errorf("given addresses were replaced: %q %q", c.ConsoleAddr, c.SchemaRegistryAddr)
	}
	c = Config{}
	c.ResolveConsoleAddr()
	if c.ConsoleAddr != "" || c.SchemaRegistryAddr != "" {
		t.Errorf("name mode filled addresses in: %q %q", c.ConsoleAddr, c.SchemaRegistryAddr)
	}
}

// The layout an earlier version left: ./meta.db beside a data directory with
// partitions in it and no database of its own.
func TestAnOldMetaDBBesideTheDataIsUsedAndSaidSo(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	c := Config{DataDir: "data"}
	if n := c.ResolveMetaDB(); n != "" || c.MetaDB != filepath.Join("data", MetaDBFile) {
		t.Fatalf("nothing on disk: %q %q", n, c.MetaDB)
	}
	os.MkdirAll(filepath.Join("data", "orders-0"), 0o755)
	os.WriteFile(MetaDBFile, []byte("x"), 0o644)
	c = Config{DataDir: "data"}
	n := c.ResolveMetaDB()
	if c.MetaDB != MetaDBFile || n != "using ./"+MetaDBFile+" from an earlier version, which kept it beside the data directory; move it to "+filepath.Join("data", MetaDBFile)+" (with the broker stopped) to make the data directory self-contained" {
		t.Errorf("old layout: %q %q", c.MetaDB, n)
	}
	// An empty ./meta.db, or one that is a directory, is not a database.
	os.WriteFile(MetaDBFile, nil, 0o644)
	if c = (Config{DataDir: "data"}); c.ResolveMetaDB() != "" {
		t.Error("an empty ./meta.db was taken")
	}
	os.WriteFile(MetaDBFile, []byte("x"), 0o644)
	// A database inside the data directory wins.
	os.WriteFile(filepath.Join("data", MetaDBFile), []byte("x"), 0o644)
	if c = (Config{DataDir: "data"}); c.ResolveMetaDB() != "" || c.MetaDB != filepath.Join("data", MetaDBFile) {
		t.Errorf("the data directory's own database lost: %q", c.MetaDB)
	}
	os.Remove(filepath.Join("data", MetaDBFile))
	// Given, it is kept.
	if c = (Config{DataDir: "data", MetaDB: "/elsewhere.db"}); c.ResolveMetaDB() != "" || c.MetaDB != "/elsewhere.db" {
		t.Errorf("a given meta-db was replaced: %q", c.MetaDB)
	}
	// No partition directory: nothing for an old database to describe.
	for _, name := range []string{"orders-x", "-0", "orders-", "file-0"} {
		dir := t.TempDir()
		if name == "file-0" {
			os.WriteFile(filepath.Join(dir, name), nil, 0o644)
		} else {
			os.Mkdir(filepath.Join(dir, name), 0o755)
		}
		if holdsPartitionData(dir) {
			t.Errorf("%q counted as a partition directory", name)
		}
	}
	if !holdsPartitionData("data") || holdsPartitionData(filepath.Join(dir, "missing")) {
		t.Error("holdsPartitionData")
	}
}

// Every optional key, written and read back: the file says what the config is.
func TestEveryOptionalKeyRoundTrips(t *testing.T) {
	orig := Default()
	orig.ListenAddr = "127.0.0.1:29092"
	orig.DataDir = "/d"
	orig.MetaDB = "/other/meta.db"
	orig.ConsoleAllowHosts = []string{"dev.example"}
	orig.ConsolePrefix = "/ops"
	orig.ACLSuperUsers = []string{"User:admin"}
	orig.PprofAddr = "127.0.0.1:6060"
	orig.SASLMechanisms = []string{"PLAIN"}
	orig.Credentials = map[string]string{"a": "b"}
	orig.TLSCertFile, orig.TLSKeyFile = "c.pem", "k.pem"
	var buf bytes.Buffer
	if err := WriteTOMLWithSecrets(&buf, orig); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "f.toml")
	os.WriteFile(path, buf.Bytes(), 0o644)
	got := Default()
	if err := LoadFile(path, &got); err != nil {
		t.Fatalf("%v\n%s", err, buf.String())
	}
	if got.MetaDB != orig.MetaDB || !reflect.DeepEqual(got.ConsoleAllowHosts, orig.ConsoleAllowHosts) || got.ConsolePrefix != "/ops" ||
		!reflect.DeepEqual(got.ACLSuperUsers, orig.ACLSuperUsers) || got.PprofAddr != orig.PprofAddr ||
		!reflect.DeepEqual(got.SASLMechanisms, orig.SASLMechanisms) || got.Credentials["a"] != "b" ||
		got.TLSCertFile != "c.pem" || got.TLSKeyFile != "k.pem" {
		t.Errorf("round trip lost a key:\n%s\n%+v", buf.String(), got)
	}
	// Nothing optional set: none of those keys is written, the prefix is the default
	// one, and meta-db is a comment that follows data-dir.
	var bare bytes.Buffer
	d := Default()
	d.DataDir = "/d"
	d.MetaDB = filepath.Join("/d", MetaDBFile)
	if err := WriteTOML(&bare, d); err != nil {
		t.Fatal(err)
	}
	s := bare.String()
	for _, key := range []string{"console-allow-host", "acl-super-user", "pprof-addr", "sasl-mechanisms", "[credentials]", "[tls]", "\nmeta-db"} {
		if strings.Contains(s, key) {
			t.Errorf("an unset %s is written:\n%s", key, s)
		}
	}
	if !strings.Contains(s, `console-prefix = "`+DefaultConsolePrefix+`"`) || !strings.Contains(s, "# meta-db = ") {
		t.Errorf("the default prefix or the meta-db comment is missing:\n%s", s)
	}
	// Only a cert, or only a key: the [tls] table is still written.
	for _, c := range []Config{{TLSCertFile: "c"}, {TLSKeyFile: "k"}} {
		cfg := Default()
		cfg.TLSCertFile, cfg.TLSKeyFile = c.TLSCertFile, c.TLSKeyFile
		var b bytes.Buffer
		WriteTOML(&b, cfg)
		if !strings.Contains(b.String(), "[tls]") {
			t.Errorf("a lone TLS file is not written:\n%s", b.String())
		}
	}
}

// The console counts as exposed only when it has an address beyond loopback.
func TestTheConsoleIsExposedOnlyWithAnAddress(t *testing.T) {
	c := Default()
	c.SASLMechanisms, c.Credentials = []string{"PLAIN"}, map[string]string{"a": "b"}
	c.ConsoleAddr = ""
	if c.consoleExposed() {
		t.Error("no console address, and the console counts as exposed")
	}
	c.ConsoleAddr = "0.0.0.0:9093"
	if !c.consoleExposed() {
		t.Error("a wildcard console address with SASL on is not exposed")
	}
}

// A bad password's parse error never repeats it.
func TestACredentialsParseErrorIsRedacted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.toml")
	c := Default()
	os.WriteFile(path, []byte("[credentials]\nalice = hunter2\n"), 0o644)
	err := LoadFile(path, &c)
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("a bad password: %v", err)
	}
}
