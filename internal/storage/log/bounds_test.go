package log

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// Bounds on a closed partition must agree with what opening it reports, in every
// state a partition's files can be in, and must not open it.
func TestBoundsMatchAnOpenedPartition(t *testing.T) {
	type setup struct {
		name  string
		build func(t *testing.T, dir string)
	}
	write := func(t *testing.T, dir string, opts ManagerOptions, batches int) {
		p, err := openPartition(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < batches; i++ {
			if _, err := p.Append(makeBatch(3, bytes.Repeat([]byte{'x'}, 50))); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.close(); err != nil {
			t.Fatal(err)
		}
	}
	small := ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 200}
	rolling := ManagerOptions{SegmentBytes: 600, IndexIntervalBytes: 200}

	cases := []setup{
		{"no directory at all", func(t *testing.T, dir string) { os.RemoveAll(dir) }},
		{"empty directory", func(t *testing.T, dir string) {}},
		{"one segment", func(t *testing.T, dir string) { write(t, dir, small, 20) }},
		{"many segments", func(t *testing.T, dir string) { write(t, dir, rolling, 40) }},
		{"start trimmed by retention", func(t *testing.T, dir string) {
			write(t, dir, rolling, 40)
			bases, _ := listSegmentBases(dir)
			for _, b := range bases[:len(bases)/2] {
				removeSegmentFiles(dir, b)
			}
		}},
		{"torn tail", func(t *testing.T, dir string) {
			write(t, dir, small, 20)
			lp := filepath.Join(dir, segmentName(0)+".log")
			info, _ := os.Stat(lp)
			os.Truncate(lp, info.Size()-9)
		}},
		{"index missing", func(t *testing.T, dir string) {
			write(t, dir, small, 20)
			os.Remove(filepath.Join(dir, segmentName(0)+".index"))
		}},
		{"index that does not line up with the log", func(t *testing.T, dir string) {
			write(t, dir, small, 20)
			ip := filepath.Join(dir, segmentName(0)+".index")
			b, _ := os.ReadFile(ip)
			b[len(b)-1] += 3 // the last entry's position now points mid-batch.
			os.WriteFile(ip, b, 0o644)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "t-0")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			tc.build(t, dir)

			m, err := OpenManager(root, small)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			start, end, err := m.Bounds("t", 0)
			if err != nil {
				t.Fatalf("Bounds: %v", err)
			}
			if n := m.OpenPartitions(); n != 0 {
				t.Fatalf("Bounds opened the partition (%d open)", n)
			}

			p, err := m.Partition("t", 0)
			if err != nil {
				t.Fatal(err)
			}
			if start != p.LogStartOffset() || end != p.LogEndOffset() {
				t.Fatalf("Bounds = [%d,%d), the opened partition reports [%d,%d)", start, end, p.LogStartOffset(), p.LogEndOffset())
			}
			// And an open partition answers from memory with the same numbers.
			s2, e2, _ := m.Bounds("t", 0)
			if s2 != start || e2 != end {
				t.Fatalf("Bounds on the open partition = [%d,%d), want [%d,%d)", s2, e2, start, end)
			}
		})
	}
}

func TestEnsurePartitionDirsCreatesWithoutOpening(t *testing.T) {
	root := t.TempDir()
	m, err := OpenManager(root, testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if err := m.EnsurePartitionDirs("wide", 50); err != nil {
		t.Fatal(err)
	}
	if n := m.OpenPartitions(); n != 0 {
		t.Fatalf("creating directories opened %d partitions", n)
	}
	for p := 0; p < 50; p++ {
		if info, err := os.Stat(filepath.Join(root, "wide-"+itoa(p))); err != nil || !info.IsDir() {
			t.Fatalf("partition %d directory missing: %v", p, err)
		}
	}
	// A partition that was only a directory opens normally on first use.
	p, err := m.Partition("wide", 7)
	if err != nil || p.LogEndOffset() != 0 {
		t.Fatalf("first open of a directory-only partition: %v %v", p, err)
	}
	if err := m.EnsurePartitionDirs("../evil", 1); err == nil {
		t.Fatal("an unsafe topic name must be refused")
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
