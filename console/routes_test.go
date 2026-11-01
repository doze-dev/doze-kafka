package console

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The route table (routes.go) is the single source: every row is a method and a
// path once, a POST mutates, and the spec a row names exists.
func TestRouteTableIsWellFormed(t *testing.T) {
	seen := map[string]bool{}
	rows := Routes()
	if len(rows) < 30 {
		t.Fatalf("the table has %d rows; it lost some", len(rows))
	}
	for _, r := range rows {
		key := r.Method + " " + r.Path
		if seen[key] {
			t.Errorf("%s is listed twice", key)
		}
		seen[key] = true
		switch r.Method {
		case "GET":
			if r.Mutates {
				t.Errorf("%s is marked as mutating: a GET that does work is a design decision to make in the open, not a flag", key)
			}
		case "POST":
			if !r.Mutates {
				t.Errorf("%s is a POST that does not say it mutates", key)
			}
		default:
			t.Errorf("%s: method %q is neither GET nor POST", key, r.Method)
		}
		if r.Note == "" {
			t.Errorf("%s has no note", key)
		}
		if _, err := os.Stat(filepath.Join("..", "e2e", "tests", r.Test)); err != nil {
			t.Errorf("%s names spec %q, which does not exist in e2e/tests", key, r.Test)
		}
	}
}

// console.go registers from the table and from nothing else: no handler is put on
// the mux by hand, so no route can exist without a row.
func TestConsoleRegistersOnlyFromTheTable(t *testing.T) {
	src, err := os.ReadFile("console.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{".Handle(", ".HandleFunc("} {
		if strings.Contains(string(src), bad) {
			t.Errorf("console.go registers a handler with %s: add a row to the route table instead", bad)
		}
	}
	// And the table is what answers: every GET row, with a topic that does not
	// exist, is a page or a refusal and never the mux's own 404 for "no such route".
	c := newTestConsole(t)
	for _, r := range Routes() {
		if r.Method != "GET" || strings.Contains(r.Path, "/static/") {
			continue
		}
		path := "/_console" + strings.NewReplacer("{name}", "x", "{id}", "x", "{subject}", "x").Replace(r.Path)
		rec := req(t, c, "GET", path, nil)
		if rec.Code == 405 {
			t.Errorf("GET %s is in the table and the mux says method not allowed", path)
		}
	}
}

// E9: no route spawns a process. The console talks to the broker as a Kafka
// client and to the registry over HTTP; nothing it serves runs a command.
func TestNoConsoleSourceSpawnsAProcess(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range file.Imports {
			if imp.Path.Value == `"os/exec"` {
				t.Errorf("%s imports os/exec: a console route must not spawn a process", f)
			}
		}
	}
}
