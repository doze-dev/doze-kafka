package log

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// failAt installs a file-seam hook that fails the operations match accepts, and returns
// the error it fails them with.
func failAt(t *testing.T, match func(FSEvent) bool) error {
	t.Helper()
	boom := errors.New("injected")
	t.Cleanup(SetFSHookForTest(func(e FSEvent) error {
		if !e.Done && match(e) {
			return boom
		}
		return nil
	}))
	return boom
}

// A topic's directories are not acknowledged unless the flush that makes them durable
// worked, and neither is their removal.
func TestB_ManagerReportsADirectoryFlushOrRemovalThatFailed(t *testing.T) {
	t.Run("ensure", func(t *testing.T) {
		m, _ := openLoggedManager(t, ManagerOptions{})
		defer m.Close()
		boom := failAt(t, func(e FSEvent) bool { return e.Op == FSDirSync && e.Path == m.dir })
		if err := m.EnsurePartitionDirs("t", 2); !errors.Is(err, boom) {
			t.Errorf("EnsurePartitionDirs = %v, want the flush error", err)
		}
	})
	t.Run("remove topic, flush", func(t *testing.T) {
		m, _ := openLoggedManager(t, ManagerOptions{})
		defer m.Close()
		if err := m.EnsurePartitionDirs("t", 2); err != nil {
			t.Fatal(err)
		}
		boom := failAt(t, func(e FSEvent) bool { return e.Op == FSDirSync && e.Path == m.dir })
		if err := m.RemoveTopic("t", 2); !errors.Is(err, boom) {
			t.Errorf("RemoveTopic = %v, want the flush error", err)
		}
	})
	t.Run("remove topic, removal", func(t *testing.T) {
		m, _ := openLoggedManager(t, ManagerOptions{})
		defer m.Close()
		if err := m.EnsurePartitionDirs("t", 2); err != nil {
			t.Fatal(err)
		}
		boom := failAt(t, func(e FSEvent) bool { return e.Op == FSRemove && filepath.Base(e.Path) == "t-0" })
		if err := m.RemoveTopic("t", 2); !errors.Is(err, boom) {
			t.Errorf("RemoveTopic = %v, want the removal error", err)
		}
		if _, err := os.Stat(filepath.Join(m.dir, "t-0")); err != nil {
			t.Errorf("t-0 should be there: its removal failed: %v", err)
		}
		if _, err := os.Stat(filepath.Join(m.dir, "t-1")); !os.IsNotExist(err) {
			t.Errorf("t-1 should have gone on past t-0's failure: %v", err)
		}
	})
	t.Run("remove topic, unsafe name", func(t *testing.T) {
		m, _ := openLoggedManager(t, ManagerOptions{})
		defer m.Close()
		if err := m.RemoveTopic("..", 1); !errors.Is(err, ErrUnsafeTopicName) {
			t.Errorf("RemoveTopic(..) = %v, want ErrUnsafeTopicName", err)
		}
	})
	t.Run("remove stray dirs", func(t *testing.T) {
		m, _ := openLoggedManager(t, ManagerOptions{})
		defer m.Close()
		for _, d := range []string{"s-0", "s-1", "keep-0"} {
			if err := os.MkdirAll(filepath.Join(m.dir, d), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		boom := failAt(t, func(e FSEvent) bool { return e.Op == FSDirSync && e.Path == m.dir })
		if n, err := m.RemoveStrayTopicDirs("s"); !errors.Is(err, boom) || n != 2 {
			t.Errorf("RemoveStrayTopicDirs = %d, %v, want 2 removed and the flush error", n, err)
		}
		if _, err := os.Stat(filepath.Join(m.dir, "keep-0")); err != nil {
			t.Errorf("another topic's directory was removed: %v", err)
		}
	})
}

// A stop whose marker cannot be written is a failed stop, and a stop that cannot record
// which partitions are now checked says so.
func TestB_ManagerCloseReportsABookkeepingFileItCouldNotWrite(t *testing.T) {
	t.Run("clean-shutdown", func(t *testing.T) {
		m, _ := openLoggedManager(t, ManagerOptions{})
		p, err := m.Partition("t", 0)
		if err != nil {
			t.Fatal(err)
		}
		fillSegments(t, p, 1)
		boom := failAt(t, func(e FSEvent) bool { return e.Op == FSRename && strings.HasSuffix(e.To, cleanStopFile) })
		if err := m.Close(); !errors.Is(err, boom) {
			t.Errorf("Close = %v, want the marker's write error", err)
		}
		if _, err := os.Stat(filepath.Join(m.dir, cleanStopFile)); !os.IsNotExist(err) {
			t.Errorf("a marker that was not written is there: %v", err)
		}
	})
	t.Run("unvalidated", func(t *testing.T) {
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
		if err := os.Remove(filepath.Join(root, unvalidatedFile)); err != nil {
			t.Fatal(err)
		}
		m, err = OpenManager(root, testOpts())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := m.Partition("t", 0); err != nil {
			t.Fatal(err)
		}
		boom := failAt(t, func(e FSEvent) bool { return e.Op == FSRename && strings.HasSuffix(e.To, unvalidatedFile) })
		if err := m.Close(); !errors.Is(err, boom) {
			t.Errorf("Close = %v, want the list's write error", err)
		}
	})
}
