package main

import (
	"os"
	"runtime"
	"testing"

	names "github.com/doze-dev/doze-names"
)

// TestMain points the whole package at a throwaway .doze home. The registry is a
// file in the developer's home directory shared with every doze binary on the
// machine: without this, a test that claims a name disturbs a broker the
// developer is running, and a test that asks where an instance is finds theirs.
// DOZE_HOME is doze-names' own override, and the binaries these tests spawn
// inherit it.
//
// The one exception is a Mac asked for the by-name tests for real
// (DOZE_NAMES_REQUIRED, see names_e2e_test.go). There the zone's network daemon
// carries packets only for the home it was installed for, so a throwaway home has no
// network at all and nothing by name can run in it: the machine's own home is used,
// and the run is meant to be narrowed to those tests (-run 'ByName|NameMode'), which
// claim names of their own and release them. On Linux a name's address is bound
// directly and the throwaway home is a whole zone, so it is kept there.
func TestMain(m *testing.M) {
	if runtime.GOOS == "darwin" && namesRequired() {
		os.Exit(m.Run())
	}
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
