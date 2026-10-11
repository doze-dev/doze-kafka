package config

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Foundation B12: doze-kafka.toml is read from the working directory of whoever
// runs the binary, so it is a parser of bytes nobody vetted. Whatever the file
// holds: no panic; and a file that loads and validates survives being written
// back out and read again as the same configuration (what `config print` is for).
func FuzzConfigFile(f *testing.F) {
	dir := f.TempDir()
	f.Add([]byte(""))
	f.Add([]byte("kafka-version = 4\nlisten = \"127.0.0.1:9092\"\nretention-ms = 60000\n"))
	f.Add([]byte("fsync-interval = \"250ms\"\nauto-create-topics = false\n[credentials]\nalice = \"pw\"\n"))
	f.Add([]byte("sasl-mechanisms = [\"PLAIN\"]\nconsole-allow-host = [\"a.lan\"]\n"))
	f.Add([]byte("name = \"x\"\nzone = false\n"))
	f.Add([]byte("= = =\n[[\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(dir, "doze-kafka.toml")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		cfg := Default()
		if err := LoadFile(path, &cfg); err != nil {
			return
		}
		if cfg.Validate() != nil {
			return
		}
		var out bytes.Buffer
		if err := WriteTOML(&out, cfg); err != nil {
			t.Fatalf("a valid configuration cannot be written: %v", err)
		}
		if err := os.WriteFile(path, out.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
		again := Default()
		if err := LoadFile(path, &again); err != nil {
			t.Fatalf("the file `config print` writes does not load: %v\n%s", err, out.Bytes())
		}
		if err := again.Validate(); err != nil {
			t.Fatalf("the file `config print` writes does not validate: %v\n%s", err, out.Bytes())
		}
		// Compared as written (an empty list and an absent one are the same file).
		var second bytes.Buffer
		if err := WriteTOML(&second, again); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out.Bytes(), second.Bytes()) {
			t.Fatalf("written and read back, the configuration changed:\n%s\nbecame\n%s", out.Bytes(), second.Bytes())
		}
	})
}
