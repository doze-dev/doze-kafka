package console

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiskSizeText(t *testing.T) {
	for n, want := range map[int64]string{
		0: "0 B", 1: "1 B", 1023: "1023 B", 1024: "1 KiB", 1536: "1.5 KiB", 9420: "9.1 KiB", 10239: "9.9 KiB", 10240: "10 KiB", 41 * 1024: "41 KiB",
		1048575: "1023 KiB", 1 << 20: "1 MiB", 5872025: "5.5 MiB", 1 << 30: "1 GiB", 1 << 40: "1 TiB", 1 << 50: "1024 TiB", 1023*1024 + 512: "1023 KiB",
	} {
		if got := diskSize(n); got != want {
			t.Errorf("diskSize(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestCountWord(t *testing.T) {
	for n, want := range map[int]string{0: "No", 1: "One", 2: "Two", 10: "Ten", 11: "11", 25: "25", -1: "-1"} {
		if got := countWord(n); got != want {
			t.Errorf("countWord(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestTopicDiskReadsPartitionDirectories(t *testing.T) {
	dir := t.TempDir()
	write := func(path string, n int) {
		full := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("orders-0/00000000000000000000.log", 1000)
	write("orders-0/00000000000000000500.log", 200)
	write("orders-0/00000000000000000000.index", 30)
	write("orders-1/00000000000000000040.log", 50)
	write("orders-1/log-start", 4)
	if err := os.MkdirAll(filepath.Join(dir, "orders-1", "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	write("orders-2/notes.log", 7)
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Facts.DataDir = dir })
	d, ok := c.topicDisk("orders", 2)
	if !ok || d.Bytes != 1000+200+30+50+4 || d.LogBytes != 1250 || len(d.Bases) != 2 || d.Bases[0] != 0 || d.Bases[1] != 40 {
		t.Errorf("%+v %v", d, ok)
	}
	d, ok = c.topicDisk("orders", 3)
	if !ok || d.Bases[2] != -1 || d.LogBytes != 1257 {
		t.Errorf("a partition whose only log has no offset in its name: %+v %v", d, ok)
	}
	for _, tc := range []struct {
		topic string
		parts int
	}{{"orders", 4}, {"orders", 0}, {"orders", -1}, {"../orders", 1}, {"a/b", 1}, {`a\b`, 1}, {"missing", 1}} {
		if _, ok := c.topicDisk(tc.topic, tc.parts); ok {
			t.Errorf("topicDisk(%q, %d) is ok", tc.topic, tc.parts)
		}
	}
	none := newTestConsole(t)
	none.facts.DataDir = ""
	if _, ok := none.topicDisk("orders", 1); ok {
		t.Error("a console not told its data directory knows a topic's size")
	}
}

func TestDataDirSize(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for path, n := range map[string]int{"a.bin": 100, "sub/b.bin": 20, "sub/meta.db": 5} {
		if err := os.WriteFile(filepath.Join(dir, path), make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Facts.DataDir = dir })
	if total, meta, ok := c.dataDirSize(); !ok || total != 125 || meta != -1 {
		t.Errorf("no meta.db at the top: %d %d %v", total, meta, ok)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.db"), make([]byte, 33), 0o644); err != nil {
		t.Fatal(err)
	}
	if total, meta, ok := c.dataDirSize(); !ok || total != 158 || meta != 33 {
		t.Errorf("with meta.db: %d %d %v", total, meta, ok)
	}
	c.facts.DataDir = ""
	if total, meta, ok := c.dataDirSize(); ok || total != 0 || meta != -1 {
		t.Errorf("no data directory: %d %d %v", total, meta, ok)
	}
}
