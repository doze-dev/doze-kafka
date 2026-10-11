package docs

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Foundation B12: every parser of untrusted bytes has a fuzz target, and the three
// places that say so agree: the Fuzz functions in the source, testdata/fuzz-targets.txt
// (which `task fuzz` and the weekly workflow read), and the parser table in
// architecture.md.
func TestEveryFuzzTargetIsRunAndDocumented(t *testing.T) {
	root := ".."
	fuzzFunc := regexp.MustCompile(`(?m)^func (Fuzz\w+)\(\w+ \*testing\.F\)`)
	inSource := map[string]bool{} // "./pkg Name"
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); path != root && strings.HasPrefix(n, ".") {
				return filepath.SkipDir // .git, .claude (agent worktrees), .audit
			}
			switch d.Name() {
			case "node_modules", ".git", "kmsg", "module", "e2e", "testdata":
				return filepath.SkipDir
			}
			if strings.HasPrefix(d.Name(), ".embeddoc-") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range fuzzFunc.FindAllSubmatch(raw, -1) {
			rel, _ := filepath.Rel(root, filepath.Dir(path))
			inSource["./"+filepath.ToSlash(rel)+" "+string(m[1])] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(inSource) < 20 {
		t.Fatalf("found %d fuzz targets in the source; the walk is broken", len(inSource))
	}

	listed := map[string]bool{}
	raw, err := os.ReadFile(filepath.Join(root, "testdata", "fuzz-targets.txt"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		listed[strings.Join(strings.Fields(line), " ")] = true
	}

	var problems []string
	for k := range inSource {
		if !listed[k] {
			problems = append(problems, "is a Fuzz function and is not in testdata/fuzz-targets.txt: "+k)
		}
	}
	for k := range listed {
		if !inSource[k] {
			problems = append(problems, "is in testdata/fuzz-targets.txt and no such Fuzz function exists: "+k)
		}
	}
	arch, err := os.ReadFile("architecture.md")
	if err != nil {
		t.Fatal(err)
	}
	for k := range inSource {
		name := strings.Fields(k)[1]
		if !strings.Contains(string(arch), "`"+name+"`") {
			problems = append(problems, "is not in the parser table of docs/architecture.md: "+name)
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Error(p)
	}
}

// The weekly job reads the list; it must give each target at least a minute.
func TestWeeklyFuzzWorkflowRunsTheListForAtLeastAMinute(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", ".github", "workflows", "fuzz.yml"))
	if err != nil {
		t.Fatal(err)
	}
	w := string(raw)
	if !strings.Contains(w, "testdata/fuzz-targets.txt") {
		t.Error("fuzz.yml does not read testdata/fuzz-targets.txt")
	}
	m := regexp.MustCompile(`inputs\.fuzztime \|\| '(\d+)s'`).FindStringSubmatch(w)
	if m == nil {
		t.Fatal("fuzz.yml has no default fuzztime")
	}
	if secs := atoi(m[1]); secs < 60 {
		t.Errorf("the weekly run gives each target %ds, the plan says at least 60", secs)
	}
	if d := regexp.MustCompile(`default: "(\d+)s"`).FindStringSubmatch(w); d == nil || atoi(d[1]) < 60 {
		t.Error("the on-demand default is under 60s")
	}
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	return n
}
