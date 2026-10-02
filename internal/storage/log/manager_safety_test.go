package log

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A topic name becomes a directory name. These names would resolve outside the
// data dir; the manager must refuse them rather than clean them, both to open
// and — the dangerous half — to delete.
func TestManagerRefusesNamesThatEscapeTheDataDir(t *testing.T) {
	root := t.TempDir()
	victim := filepath.Join(root, "victim-0")
	if err := os.MkdirAll(victim, 0o755); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(victim, "keep.txt")
	if err := os.WriteFile(keep, []byte("not yours to delete"), 0o644); err != nil {
		t.Fatal(err)
	}

	m, err := OpenManager(filepath.Join(root, "data"), ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	for _, name := range []string{"../victim", "..\\victim", "a/b", "/abs", "", ".", "..", "nul\x00byte"} {
		if _, err := m.Partition(name, 0); !errors.Is(err, ErrUnsafeTopicName) {
			t.Errorf("Partition(%q) = %v, want ErrUnsafeTopicName", name, err)
		}
		if err := m.RemoveTopic(name, 1); !errors.Is(err, ErrUnsafeTopicName) {
			t.Errorf("RemoveTopic(%q) = %v, want ErrUnsafeTopicName", name, err)
		}
	}

	if b, err := os.ReadFile(keep); err != nil || string(b) != "not yours to delete" {
		t.Fatalf("a file outside the data dir was touched: %v %q", err, b)
	}
	// Nothing was created next to the data dir either.
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		if e.Name() != "victim-0" && e.Name() != "data" {
			t.Errorf("unexpected entry %q created beside the data dir", e.Name())
		}
	}

	// Ordinary names still work, including legacy-looking ones that are merely odd.
	for _, name := range []string{"orders", "bad name!", ".hidden", "..x"} {
		if _, err := m.Partition(name, 0); err != nil {
			t.Errorf("Partition(%q): %v", name, err)
		}
	}
}
