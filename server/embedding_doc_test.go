package server_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// docs/embedding.md is the first thing someone embedding the broker copies from,
// so its code is compiled and run here: each ```go block that starts with an
// import is a complete test file, written into a scratch package inside the
// module (so it resolves the same dependencies) and run with `go test`.
func TestEmbeddingDocCompiles(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the go tool")
	}
	raw, err := os.ReadFile(filepath.Join("..", "docs", "embedding.md"))
	if err != nil {
		t.Fatal(err)
	}
	var blocks []string
	for rest := string(raw); ; {
		i := strings.Index(rest, "```go\n")
		if i < 0 {
			break
		}
		rest = rest[i+len("```go\n"):]
		j := strings.Index(rest, "```")
		if j < 0 {
			t.Fatal("unterminated code block in docs/embedding.md")
		}
		blocks = append(blocks, rest[:j])
		rest = rest[j+3:]
	}
	if len(blocks) == 0 {
		t.Fatal("docs/embedding.md has no ```go block to compile")
	}
	// A dot-prefixed directory is ignored by ./... and by the build cache's
	// package listing, so a crash here cannot leave a package behind that the
	// suite would pick up.
	dir, err := os.MkdirTemp("..", ".embeddoc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	for i, b := range blocks {
		src := b
		if !strings.HasPrefix(strings.TrimSpace(src), "package ") {
			src = "package embeddoc\n\n" + src
		}
		name := filepath.Join(dir, "doc"+string(rune('a'+i))+"_test.go")
		if err := os.WriteFile(name, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("go", "test", "-count=1", "./"+filepath.Base(dir))
	cmd.Dir = ".."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the code in docs/embedding.md does not compile or fails:\n%s", out)
	}
}
