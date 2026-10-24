package log

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A partition directory that cannot be removed is an error, and the directory stays.
func TestB_RemoveTopicReportsADirectoryTheSystemWillNotRemove(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root removes what a mode forbids")
	}
	m, _ := openLoggedManager(t, ManagerOptions{})
	defer m.Close()
	if err := m.EnsurePartitionDirs("t", 1); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(m.dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(m.dir, 0o755)
	if err := m.RemoveTopic("t", 1); err == nil {
		t.Error("RemoveTopic reported nothing for a directory it could not remove")
	}
	if _, err := os.Stat(filepath.Join(m.dir, "t-0")); err != nil {
		t.Errorf("t-0 is gone: %v", err)
	}
}

// The maintenance tick says when it cannot record which partitions have been checked,
// and says nothing when it can.
func TestB_MaintenanceTickReportsAnUnvalidatedListItCouldNotWrite(t *testing.T) {
	root := t.TempDir()
	m, err := OpenManager(root, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	fillSegments(t, p, 1)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	// A data directory from before the list was kept: every partition on disk is on it.
	reopen := func(out *lockedBuffer) *Manager {
		t.Helper()
		if err := os.Remove(filepath.Join(root, unvalidatedFile)); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		opts := testOpts()
		opts.Logger = slog.New(slog.NewTextHandler(out, nil))
		m, err := OpenManager(root, opts)
		if err != nil {
			t.Fatal(err)
		}
		p, err := m.Partition("t", 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Sync(); err != nil {
			t.Fatal(err)
		}
		return m
	}

	var quiet lockedBuffer
	m = reopen(&quiet)
	time.Sleep(2500 * time.Millisecond) // two ticks of the maintenance loop.
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if got := quiet.String(); got != "" {
		t.Errorf("healthy ticks logged:\n%s", got)
	}

	var loud lockedBuffer
	m = reopen(&loud)
	restore := SetFSHookForTest(func(e FSEvent) error {
		if !e.Done && e.Op == FSRename && strings.HasSuffix(e.To, unvalidatedFile) {
			return os.ErrPermission
		}
		return nil
	})
	deadline := time.Now().Add(15 * time.Second)
	for !strings.Contains(loud.String(), "could not record which partitions have been checked") {
		if time.Now().After(deadline) {
			restore()
			m.Close()
			t.Fatalf("no warning within 15 s; the loop logged:\n%s", loud.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	restore()
	m.Close()
}
