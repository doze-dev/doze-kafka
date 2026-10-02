package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

const sampleTOML = `
listen = ":19092"
retention-ms = 1000
fsync-interval = "2s"
sasl-mechanisms = ["PLAIN"]

[credentials]
alice = "pw"
`

func writeTOML(t *testing.T, dir, contents string) string {
	t.Helper()
	path := filepath.Join(dir, "doze-kafka.toml")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadConfig_FileOverridesDefaults(t *testing.T) {
	path := writeTOML(t, t.TempDir(), sampleTOML)

	st, err := loadConfig([]string{"--config", path})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if st.configFile != path {
		t.Errorf("configFile = %q, want %q", st.configFile, path)
	}
	if st.cfg.ListenAddr != ":19092" {
		t.Errorf("ListenAddr = %q, want file value :19092", st.cfg.ListenAddr)
	}
	if st.cfg.FsyncInterval != 2*time.Second {
		t.Errorf("FsyncInterval = %s, want 2s", st.cfg.FsyncInterval)
	}
	if st.cfg.DataDir != "./data" {
		t.Errorf("DataDir = %q, want default ./data", st.cfg.DataDir)
	}
}

func TestLoadConfig_FlagBeatsFile(t *testing.T) {
	path := writeTOML(t, t.TempDir(), sampleTOML)

	st, err := loadConfig([]string{"--config", path, "--listen", ":29092"})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	// Flag wins where given...
	if st.cfg.ListenAddr != ":29092" {
		t.Errorf("ListenAddr = %q, want flag value :29092", st.cfg.ListenAddr)
	}
	// ...file value survives where no flag was passed.
	if st.cfg.RetentionMs != 1000 {
		t.Errorf("RetentionMs = %d, want file value 1000", st.cfg.RetentionMs)
	}
}

func TestLoadConfig_AutoDiscoversInWorkingDir(t *testing.T) {
	dir := t.TempDir()
	writeTOML(t, dir, sampleTOML)
	t.Chdir(dir) // ./doze-kafka.toml should be picked up with no --config.

	st, err := loadConfig(nil)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if st.configFile == "" {
		t.Fatal("did not auto-discover ./doze-kafka.toml")
	}
	if st.cfg.ListenAddr != ":19092" {
		t.Errorf("ListenAddr = %q, want auto-loaded :19092", st.cfg.ListenAddr)
	}
}

func TestLoadConfig_NoFileIsJustDefaultsAndFlags(t *testing.T) {
	t.Chdir(t.TempDir()) // ensure no stray doze-kafka.toml is discovered.

	st, err := loadConfig([]string{"--listen", ":39092"})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if st.configFile != "" {
		t.Errorf("configFile = %q, want empty", st.configFile)
	}
	if st.cfg.ListenAddr != ":39092" {
		t.Errorf("ListenAddr = %q, want :39092", st.cfg.ListenAddr)
	}
	if st.cfg.DataDir != "./data" {
		t.Errorf("DataDir = %q, want default", st.cfg.DataDir)
	}
}

func TestConsoleAllowHostFlagAndImpliedHosts(t *testing.T) {
	st, err := loadConfig([]string{"--console-allow-host", "dev.example.com,box.lan", "--console-allow-host", "x.test",
		"--console-addr", "devbox.local:9093"})
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	got := consoleHosts(st)
	want := []string{"dev.example.com", "box.lan", "x.test", "devbox.local"}
	if len(got) != len(want) {
		t.Fatalf("consoleHosts = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("consoleHosts = %v, want %v", got, want)
		}
	}

	// An address is not a name: binding to an IP implies nothing to allow (IP
	// literals are always accepted).
	st, err = loadConfig([]string{"--console-addr", "0.0.0.0:9093"})
	if err != nil {
		t.Fatal(err)
	}
	if h := consoleHosts(st); len(h) != 0 {
		t.Errorf("consoleHosts for an IP bind = %v, want none", h)
	}
}
