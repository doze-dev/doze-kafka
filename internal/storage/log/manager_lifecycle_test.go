package log

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func lifecycleManager(t *testing.T) *Manager {
	t.Helper()
	m, err := OpenManager(t.TempDir(), ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

// A request that resolved its topic just before DeleteTopics must not re-create the
// partition directory behind the delete: the next topic of that name would start
// with the old topic's late write in it.
func TestExistingPartitionDoesNotResurrectADeletedTopic(t *testing.T) {
	m := lifecycleManager(t)
	if err := m.EnsurePartitionDirs("t", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ExistingPartition("t", 0); err != nil {
		t.Fatalf("a created partition should open: %v", err)
	}
	if err := m.RemoveTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ExistingPartition("t", 0); !errors.Is(err, ErrNoSuchPartition) {
		t.Fatalf("opening a deleted partition = %v, want ErrNoSuchPartition", err)
	}
	if _, err := os.Stat(filepath.Join(m.dir, "t-0")); !os.IsNotExist(err) {
		t.Error("the deleted partition's directory came back")
	}
	if _, err := m.ExistingPartition("never", 0); !errors.Is(err, ErrNoSuchPartition) {
		t.Errorf("a partition that never existed = %v", err)
	}
}

// Leftover directories for a name are cleared, whatever their partition numbers,
// and nothing else is touched.
func TestRemoveStrayTopicDirsAndStrayDirs(t *testing.T) {
	m := lifecycleManager(t)
	for _, d := range []string{"orders-0", "orders-7", "orders-eu-0", "orders-x", "other-0"} {
		os.MkdirAll(filepath.Join(m.dir, d), 0o755)
	}
	os.WriteFile(filepath.Join(m.dir, "meta.db"), nil, 0o644)

	stray := m.StrayDirs(func(topic string) (int32, bool) {
		if topic == "other" {
			return 1, true
		}
		return 0, false
	})
	if len(stray) != 3 { // orders-0, orders-7, orders-eu-0; "orders-x" is not a partition directory.
		t.Errorf("StrayDirs = %v, want the three partition directories of unknown topics", stray)
	}
	n, err := m.RemoveStrayTopicDirs("orders")
	if err != nil || n != 2 {
		t.Fatalf("RemoveStrayTopicDirs(orders) = %d, %v; want 2", n, err)
	}
	for d, want := range map[string]bool{"orders-0": false, "orders-7": false, "orders-eu-0": true, "orders-x": true, "other-0": true} {
		_, err := os.Stat(filepath.Join(m.dir, d))
		if (err == nil) != want {
			t.Errorf("%s exists=%v, want %v", d, err == nil, want)
		}
	}
	if _, err := m.RemoveStrayTopicDirs("../x"); err == nil {
		t.Error("an unsafe topic name should be refused")
	}
}

// Nothing may be opened after Close (nothing would flush it), and closing twice is
// not an error.
func TestManagerRefusesUseAfterCloseAndClosesOnce(t *testing.T) {
	m := lifecycleManager(t)
	p, err := m.Partition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(makeBatch(1, []byte("x"))); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if _, err := m.Partition("t", 0); !errors.Is(err, ErrManagerClosed) {
		t.Errorf("Partition after Close = %v, want ErrManagerClosed", err)
	}
	if _, err := m.Partition("new", 0); !errors.Is(err, ErrManagerClosed) {
		t.Errorf("opening a new partition after Close = %v, want ErrManagerClosed", err)
	}
	if n := m.OpenPartitions(); n != 0 {
		t.Errorf("%d partitions still open after Close", n)
	}
}
