package config

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeFile writes contents to a temp file and returns its path.
func writeFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "doze-kafka.toml")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func TestLoadFile_OverlaysOnlyNamedKeys(t *testing.T) {
	path := writeFile(t, `
listen = ":19092"
retention-ms = 604800000
fsync-interval = "2s"
default-partitions = 3
auto-create-topics = false
`)
	cfg := Default()
	if err := LoadFile(path, &cfg); err != nil {
		t.Fatalf("LoadFile: %v", err)
	}

	// Named keys took the file's values.
	if cfg.ListenAddr != ":19092" {
		t.Errorf("ListenAddr = %q, want :19092", cfg.ListenAddr)
	}
	if cfg.RetentionMs != 604800000 {
		t.Errorf("RetentionMs = %d, want 604800000", cfg.RetentionMs)
	}
	if cfg.FsyncInterval != 2*time.Second {
		t.Errorf("FsyncInterval = %s, want 2s", cfg.FsyncInterval)
	}
	if cfg.DefaultPartitions != 3 {
		t.Errorf("DefaultPartitions = %d, want 3", cfg.DefaultPartitions)
	}
	if cfg.AutoCreateTopics {
		t.Error("AutoCreateTopics = true, want false")
	}

	// Unnamed keys kept their defaults.
	if cfg.DataDir != Default().DataDir {
		t.Errorf("DataDir = %q, want default %q", cfg.DataDir, Default().DataDir)
	}
	if cfg.ConsumerSessionTimeout != Default().ConsumerSessionTimeout {
		t.Errorf("ConsumerSessionTimeout = %s, want default", cfg.ConsumerSessionTimeout)
	}
	if cfg.SegmentBytes != Default().SegmentBytes {
		t.Errorf("SegmentBytes = %d, want default", cfg.SegmentBytes)
	}
}

func TestLoadFile_AuthTables(t *testing.T) {
	path := writeFile(t, `
sasl-mechanisms = ["PLAIN", "SCRAM-SHA-256"]

[credentials]
alice = "s3cret"
bob = "hunter2"

[tls]
cert = "./certs/server.pem"
key = "./certs/server-key.pem"
`)
	cfg := Default()
	if err := LoadFile(path, &cfg); err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if !cfg.AuthEnabled() {
		t.Fatal("AuthEnabled = false, want true")
	}
	if got := strings.Join(cfg.SASLMechanisms, ","); got != "PLAIN,SCRAM-SHA-256" {
		t.Errorf("SASLMechanisms = %q", got)
	}
	if cfg.Credentials["alice"] != "s3cret" || cfg.Credentials["bob"] != "hunter2" {
		t.Errorf("Credentials = %v", cfg.Credentials)
	}
	if !cfg.TLSEnabled() {
		t.Error("TLSEnabled = false, want true")
	}
	if cfg.TLSCertFile != "./certs/server.pem" || cfg.TLSKeyFile != "./certs/server-key.pem" {
		t.Errorf("TLS files = %q / %q", cfg.TLSCertFile, cfg.TLSKeyFile)
	}
	// The certificate named here does not exist, and validation says so now
	// rather than leaving it to fail once the broker is half started.
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "cannot load the TLS certificate") {
		t.Errorf("Validate with a missing certificate: %v", err)
	}
	// Everything else a file overlays onto clean defaults validates.
	cfg.TLSCertFile, cfg.TLSKeyFile = "", ""
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestLoadFile_UnknownKeyRejected(t *testing.T) {
	// A typo'd key ("retention_ms" instead of "retention-ms") must fail loudly.
	path := writeFile(t, "retention_ms = 1000\n")
	cfg := Default()
	err := LoadFile(path, &cfg)
	if err == nil {
		t.Fatal("LoadFile accepted an unknown key")
	}
	if !strings.Contains(err.Error(), "unknown key") || !strings.Contains(err.Error(), "retention_ms") {
		t.Errorf("error = %q, want it to name the unknown key", err)
	}
}

func TestLoadFile_BadDuration(t *testing.T) {
	path := writeFile(t, `fsync-interval = "banana"`)
	cfg := Default()
	if err := LoadFile(path, &cfg); err == nil {
		t.Fatal("LoadFile accepted an invalid duration")
	}
}

func TestLoadFile_MissingFile(t *testing.T) {
	cfg := Default()
	if err := LoadFile(filepath.Join(t.TempDir(), "nope.toml"), &cfg); err == nil {
		t.Fatal("LoadFile accepted a missing file")
	}
}

func TestWriteTOML_RoundTrips(t *testing.T) {
	orig := Default()
	orig.ListenAddr = ":29092"
	orig.RetentionBytes = 1 << 30
	orig.FsyncInterval = 250 * time.Millisecond
	orig.ConsumerSessionTimeout = 30 * time.Second
	orig.SASLMechanisms = []string{"SCRAM-SHA-512"}
	orig.Credentials = map[string]string{"carol": "pw"}
	orig.TLSCertFile = "c.pem"
	orig.TLSKeyFile = "k.pem"

	var buf bytes.Buffer
	if err := WriteTOML(&buf, orig); err != nil {
		t.Fatalf("WriteTOML: %v", err)
	}

	path := writeFile(t, buf.String())
	got := Default()
	if err := LoadFile(path, &got); err != nil {
		t.Fatalf("re-load emitted TOML: %v\n%s", err, buf.String())
	}
	if got.ListenAddr != orig.ListenAddr ||
		got.RetentionBytes != orig.RetentionBytes ||
		got.FsyncInterval != orig.FsyncInterval ||
		got.ConsumerSessionTimeout != orig.ConsumerSessionTimeout ||
		got.TLSCertFile != orig.TLSCertFile || got.TLSKeyFile != orig.TLSKeyFile ||
		got.Credentials["carol"] != "pw" ||
		strings.Join(got.SASLMechanisms, ",") != "SCRAM-SHA-512" {
		t.Errorf("round-trip mismatch:\n got  %+v\n want %+v\n toml:\n%s", got, orig, buf.String())
	}
}

// exampleWithAuth is the shipped example file with its commented-out auth
// settings switched on, as someone enabling SASL and TLS would edit it.
func exampleWithAuth(t *testing.T, example string) string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(example, "\n") {
		for _, key := range []string{"sasl-mechanisms =", "[credentials]", "alice", "bob", "[tls]", "cert", "key "} {
			if strings.HasPrefix(line, "# "+key) {
				line = strings.TrimPrefix(line, "# ")
			}
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// In TOML every key after a [table] header belongs to that table. The example
// file had [credentials] and [tls] in the middle, so uncommenting them, as its
// own comments invite, turned "console = true" into a SASL user or an unknown
// key under [tls], and the broker refused to start.
func TestExampleFileSurvivesUncommentingItsTables(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "doze-kafka.example.toml"))
	if err != nil {
		t.Fatal(err)
	}
	// As shipped, it is the defaults.
	cfg := Default()
	if err := LoadFile(writeFile(t, string(raw)), &cfg); err != nil {
		t.Fatalf("the example file as shipped: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("the example file as shipped does not validate: %v", err)
	}

	cfg = Default()
	if err := LoadFile(writeFile(t, exampleWithAuth(t, string(raw))), &cfg); err != nil {
		t.Fatalf("the example file with its auth sections uncommented: %v", err)
	}
	if len(cfg.Credentials) != 2 || cfg.Credentials["alice"] != "s3cret" {
		t.Errorf("credentials = %v, want exactly alice and bob", cfg.Credentials)
	}
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || len(cfg.SASLMechanisms) != 1 {
		t.Errorf("auth settings were not read: cert %q key %q mechanisms %v", cfg.TLSCertFile, cfg.TLSKeyFile, cfg.SASLMechanisms)
	}
	// And nothing else was swallowed by a table.
	d := Default()
	if cfg.Console != d.Console || cfg.ConsoleAddr != d.ConsoleAddr || cfg.Zone != d.Zone || cfg.LogLevel != d.LogLevel || cfg.LogFormat != d.LogFormat {
		t.Errorf("settings after the tables changed: console %v %q, zone %v, log %q %q", cfg.Console, cfg.ConsoleAddr, cfg.Zone, cfg.LogLevel, cfg.LogFormat)
	}
}
