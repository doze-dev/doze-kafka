package main

import (
	"os"
	"testing"

	names "github.com/doze-dev/doze-names"
)

// TestMain points the whole package at a throwaway .doze home. The registry is a
// file in the developer's home directory shared with every doze binary on the
// machine: without this, a test that claims a name disturbs a broker the
// developer is running, and a test that asks where an instance is finds theirs.
// DOZE_HOME is doze-names' own override, and the binaries these tests spawn
// inherit it.
func TestMain(m *testing.M) {
	home, err := os.MkdirTemp("", "doze-home")
	if err != nil {
		panic(err)
	}
	os.Setenv(names.EnvHome, home)
	code := m.Run()
	os.RemoveAll(home)
	if binDir != "" {
		os.RemoveAll(binDir)
	}
	os.Exit(code)
}
