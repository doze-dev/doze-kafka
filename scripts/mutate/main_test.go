package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Every non-test file of a split package is in exactly one group, so a new file
// cannot silently fall out of the baseline.
func TestFileGroupsPartitionTheirPackage(t *testing.T) {
	byDir := map[string]map[string]string{} // dir -> file -> unit
	for _, u := range Units {
		if len(u.Only) == 0 {
			continue
		}
		if byDir[u.Dir] == nil {
			byDir[u.Dir] = map[string]string{}
		}
		for _, n := range u.Only {
			if prev, dup := byDir[u.Dir][n]; dup {
				t.Errorf("%s/%s.go is in both %s and %s", u.Dir, n, prev, u.Name)
			}
			byDir[u.Dir][n] = u.Name
		}
	}
	for dir, in := range byDir {
		files, err := filepath.Glob(filepath.Join("..", "..", dir, "*.go"))
		if err != nil || len(files) == 0 {
			t.Fatalf("%s: %v (%d files)", dir, err, len(files))
		}
		seen := map[string]bool{}
		for _, f := range files {
			base := strings.TrimSuffix(filepath.Base(f), ".go")
			if strings.HasSuffix(base, "_test") {
				continue
			}
			seen[base] = true
			if _, ok := in[base]; !ok {
				t.Errorf("%s/%s.go is in no mutation group: add it to a unit in scripts/mutate/main.go", dir, base)
			}
		}
		for n := range in {
			if !seen[n] {
				t.Errorf("%s/%s.go is named in a mutation group and does not exist", dir, n)
			}
		}
	}
}

// Every unit's directory exists and has tests; a unit that pointed nowhere would
// be an error at run time, and this says so in the push gate.
func TestEveryUnitIsAPackageWithTests(t *testing.T) {
	for _, u := range Units {
		tests, _ := filepath.Glob(filepath.Join("..", "..", u.Dir, "*_test.go"))
		if len(tests) == 0 {
			t.Errorf("%s: %s has no test files", u.Name, u.Dir)
		}
		if u.Shards < 1 {
			t.Errorf("%s: Shards must be at least 1", u.Name)
		}
	}
}

func TestSelectTakesPackagesAndUnitNames(t *testing.T) {
	us, err := Select([]string{"./internal/broker/..."})
	if err != nil || len(us) != 4 {
		t.Fatalf("the broker has 4 units, got %d (%v)", len(us), err)
	}
	us, err = Select([]string{"internal/broker:data", "./console"})
	if err != nil || len(us) != 2 {
		t.Fatalf("got %d, %v", len(us), err)
	}
	if _, err := Select([]string{"./nope"}); err == nil {
		t.Error("an unknown package must be an error, not an empty run")
	}
}

const sampleSource = `package p

var wait = 2 * 3

func (s *Store) Put(a, b int) int {
	if a < b {
		return a - b
	}
	for i := 0; i < a; i++ {
		b = -b
	}
	return b
}

func Free(a int) bool { return a == 1 || a < 0 }
`

func TestDiscoverNamesEachMutantByItsDeclaration(t *testing.T) {
	ms, err := discoverFile("p/a.go", []byte(sampleSource))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, m := range ms {
		got = append(got, m.At()+" "+m.Key()+" "+m.From+">"+m.To)
	}
	want := []string{
		"p/a.go:3:14:ARITHMETIC_BASE p/a.go:<package>:ARITHMETIC_BASE#1 *>/",
		"p/a.go:6:7:CONDITIONALS_BOUNDARY p/a.go:(*Store).Put:CONDITIONALS_BOUNDARY#1 <><=",
		"p/a.go:6:7:CONDITIONALS_NEGATION p/a.go:(*Store).Put:CONDITIONALS_NEGATION#1 <>>=",
		"p/a.go:7:12:ARITHMETIC_BASE p/a.go:(*Store).Put:ARITHMETIC_BASE#1 ->+",
		"p/a.go:9:16:CONDITIONALS_BOUNDARY p/a.go:(*Store).Put:CONDITIONALS_BOUNDARY#2 <><=",
		"p/a.go:9:16:CONDITIONALS_NEGATION p/a.go:(*Store).Put:CONDITIONALS_NEGATION#2 <>>=",
		"p/a.go:9:22:INCREMENT_DECREMENT p/a.go:(*Store).Put:INCREMENT_DECREMENT#1 ++>--",
		"p/a.go:10:7:INVERT_NEGATIVES p/a.go:(*Store).Put:INVERT_NEGATIVES#1 ->+",
		"p/a.go:15:34:CONDITIONALS_NEGATION p/a.go:Free:CONDITIONALS_NEGATION#1 ==>!=",
		"p/a.go:15:44:CONDITIONALS_BOUNDARY p/a.go:Free:CONDITIONALS_BOUNDARY#1 <><=",
		"p/a.go:15:44:CONDITIONALS_NEGATION p/a.go:Free:CONDITIONALS_NEGATION#2 <>>=",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	// Applying one changes that token and nothing else.
	out, err := ms[3].Apply([]byte(sampleSource))
	if err != nil || !strings.Contains(string(out), "return a + b") || len(out) != len(sampleSource) {
		t.Errorf("apply: %v\n%s", err, out)
	}
	if _, err := ms[3].Apply([]byte("package p\n")); err == nil {
		t.Error("a mutant must not be applied to a file that no longer has its token")
	}
}

// A key does not move when lines are added above the declaration; a line number does.
func TestAKeySurvivesAnEditAbove(t *testing.T) {
	a, _ := discoverFile("p/a.go", []byte(sampleSource))
	b, _ := discoverFile("p/a.go", []byte(strings.Replace(sampleSource, "var wait", "// a comment\n\nfunc New() int { return 1 + 1 }\n\nvar wait", 1)))
	keys := map[string]bool{}
	for _, m := range b {
		keys[m.Key()] = true
	}
	for _, m := range a {
		if !keys[m.Key()] {
			t.Errorf("%s is gone after an edit that did not touch it", m.Key())
		}
	}
	if a[1].Line == b[2].Line {
		t.Fatal("the edit was meant to move the lines")
	}
}

func mutant(file string, line int, typ string, st Status) Mutant {
	return Mutant{File: file, Line: line, Col: 1, Type: typ, From: "<", To: "<=", Func: "F", Ord: line, Status: st}
}

func TestCompare(t *testing.T) {
	lived := mutant("p/a.go", 9, "CONDITIONALS_BOUNDARY", Lived)
	uncovered := mutant("p/a.go", 12, "ARITHMETIC_BASE", NotCovered)
	killed := mutant("p/a.go", 20, "CONDITIONALS_NEGATION", Killed)
	base, added := Record(UnitBase{}, &Run{Mutants: []Mutant{lived, uncovered, killed}})
	if len(added) != 2 || base.Open != 2 || base.Lived != 1 || base.NotCovered != 1 || base.Killed != 1 || len(base.Debt) != 2 {
		t.Fatalf("recorded %+v, added %v", base, added)
	}

	check := func(name string, wantFail int, wantIn string, ms ...Mutant) Comparison {
		t.Helper()
		c := Compare(base, &Run{Mutants: ms})
		if len(c.Failures) != wantFail || (wantIn != "" && !strings.Contains(strings.Join(c.Failures, "\n"), wantIn)) {
			t.Errorf("%s: want %d failures containing %q, got %v", name, wantFail, wantIn, c.Failures)
		}
		return c
	}
	if c := check("the baseline against itself", 0, "", lived, uncovered, killed); c.Open != 2 {
		t.Errorf("open debt is reported: got %d, want 2", c.Open)
	}

	// The case counts could not see: one survivor killed, another born.
	nowKilled := lived
	nowKilled.Status = Killed
	born := mutant("p/a.go", 20, "CONDITIONALS_NEGATION", Lived)
	c := check("one killed and one born", 2, "NEW lived p/a.go:20:1:CONDITIONALS_NEGATION", nowKilled, uncovered, born)
	if !strings.Contains(strings.Join(c.Failures, "\n"), "is listed as lived and is now killed") {
		t.Errorf("a listed mutant that is now killed must be dropped from the list: %v", c.Failures)
	}

	check("a new uncovered mutant", 1, "NEW notCovered", lived, uncovered, killed, mutant("p/a.go", 30, "ARITHMETIC_BASE", NotCovered))

	// Neither a timeout nor a failed setup is a kill, listed or not.
	for _, st := range []Status{TimedOut, SetupFailed} {
		m := killed
		m.Status = st
		check(string(st)+" unlisted", 1, "Neither is a kill", lived, uncovered, m)
		l := lived
		l.Status = st
		check(string(st)+" where lived is listed", 1, "Neither is a kill", l, uncovered, killed)
	}
	withTimeout, _ := Record(base, &Run{Mutants: []Mutant{lived, uncovered, mutant("p/a.go", 20, "CONDITIONALS_NEGATION", TimedOut)}})
	timedOut := mutant("p/a.go", 20, "CONDITIONALS_NEGATION", TimedOut)
	if c := Compare(withTimeout, &Run{Mutants: []Mutant{lived, uncovered, timedOut}}); len(c.Failures) != 1 || !strings.Contains(c.Failures[0], "no reason") {
		t.Errorf("a timeout listed as \"open\" has no reason: %v", c.Failures)
	}
	for i := range withTimeout.Debt {
		if withTimeout.Debt[i].Status == TimedOut {
			withTimeout.Debt[i].Reason = "an infinite loop: the mutant never returns, and no test can tell that from slow"
		}
	}
	if c := Compare(withTimeout, &Run{Mutants: []Mutant{lived, uncovered, timedOut}}); len(c.Failures) != 0 {
		t.Errorf("a timeout listed with a reason passes: %v", c.Failures)
	}
	// ... and that reason does not cover the mutant living.
	if c := Compare(withTimeout, &Run{Mutants: []Mutant{lived, uncovered, born}}); len(c.Failures) != 1 || !strings.Contains(c.Failures[0], "NEW lived") {
		t.Errorf("listed as timed out and now lived: %v", c.Failures)
	}

	// A mutant with no status is a broken run, not a pass.
	check("no status", 1, "the run is broken", lived, uncovered, mutant("p/a.go", 40, "X", ""))

	// A shard is held to its own mutants only.
	if c := Compare(base, &Run{Shard: "1/2", Mutants: []Mutant{lived}}); len(c.Failures) != 0 || len(c.Notes) != 0 {
		t.Errorf("a shard: %+v", c)
	}
	if c := Compare(base, &Run{Mutants: []Mutant{lived}}); len(c.Notes) != 1 {
		t.Errorf("a listed mutant that is no longer in the code is noted: %+v", c)
	}
}

func TestRecordKeepsReasonsAndNamesWhatIsNew(t *testing.T) {
	lived := mutant("p/a.go", 9, "CONDITIONALS_BOUNDARY", Lived)
	old, _ := Record(UnitBase{}, &Run{Mutants: []Mutant{lived}})
	old.Debt[0].Reason = "equivalent: both sides are the same slice length here"
	moved := lived
	moved.Line = 40 // the same declaration, further down the file.
	born := mutant("p/a.go", 12, "ARITHMETIC_BASE", Lived)
	nb, added := Record(old, &Run{Mutants: []Mutant{moved, born}})
	if len(added) != 1 || added[0].At != born.At() || nb.Open != 1 {
		t.Fatalf("added %v, open %d", added, nb.Open)
	}
	for _, d := range nb.Debt {
		if d.Key == lived.Key() && (d.Reason != old.Debt[0].Reason || d.At != moved.At()) {
			t.Errorf("the reason stays and the position is refreshed: %+v", d)
		}
	}
}

func TestBaselineRoundTripsOneMutantALine(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	u, _ := Record(UnitBase{}, &Run{Platform: "x/y", Workers: 2, Tests: 3, Seconds: 12.34, Mutants: []Mutant{
		mutant("p/a.go", 9, "CONDITIONALS_BOUNDARY", Lived), mutant("p/a.go", 12, "ARITHMETIC_BASE", NotCovered), mutant("p/a.go", 13, "ARITHMETIC_BASE", Killed)}})
	in := Baseline{Tool: toolName, Units: map[string]UnitBase{"p": u, "q": {Debt: []Debt{}}}}
	if err := writeBaseline(in); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(baselinePath)
	if n := strings.Count(string(raw), "\n        {\"at\""); n != 2 {
		t.Errorf("want each of the 2 entries on its own line, got %d:\n%s", n, raw)
	}
	out, err := readBaseline()
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(in)
	b, _ := json.Marshal(out)
	if string(a) != string(b) {
		t.Errorf("round trip:\n%s\n%s", a, b)
	}
	if rep := Report(out); !strings.Contains(rep, "p/a.go:9:1") || !strings.Contains(rep, "*open:* no test puts the two sides equal") {
		t.Errorf("the report names the survivor and what lets it live:\n%s", rep)
	}
}

func TestMatrixHasEveryShardOfEveryUnit(t *testing.T) {
	got := Matrix([]Unit{{Name: "a", Shards: 2}, {Name: "b"}})
	if got != `[{"unit":"a","shard":"1/2"},{"unit":"a","shard":"2/2"},{"unit":"b","shard":"1/1"}]` {
		t.Error(got)
	}
	if _, _, err := parseShard("3/2"); err == nil {
		t.Error("shard 3 of 2 is an error")
	}
	if i, n, err := parseShard("2/5"); err != nil || i != 2 || n != 5 {
		t.Error(i, n, err)
	}
}

// Changed, in a throwaway repository: a unit is picked when a Go file of its
// package differs from the merge base, a test file included; and a clone too
// shallow to have a merge base is an error, not "nothing changed".
func TestChangedPicksUnitsFromTheMergeBase(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repo := t.TempDir()
	git := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	write := func(rel, body string) {
		t.Helper()
		p := filepath.Join(repo, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git(repo, "init", "-q", "-b", "main")
	write("internal/traffic/a.go", "package traffic\n")
	write("console/a.go", "package console\n")
	write("README.md", "x\n")
	git(repo, "add", "-A")
	git(repo, "commit", "-q", "-m", "base")
	git(repo, "checkout", "-q", "-b", "work")
	write("README.md", "y\n")
	git(repo, "commit", "-q", "-am", "docs only")

	names := func(us []Unit) string {
		var s []string
		for _, u := range us {
			s = append(s, u.Name)
		}
		return strings.Join(s, ",")
	}
	us, err := Changed(repo, "main")
	if err != nil || len(us) != 0 {
		t.Fatalf("a change to a document picks no unit: %v %v", names(us), err)
	}
	write("internal/traffic/a_test.go", "package traffic\n")
	git(repo, "add", "-A")
	git(repo, "commit", "-q", "-m", "a test")
	write("console/b.go", "package console\n") // not committed, not even added.
	us, err = Changed(repo, "main")
	if err != nil || names(us) != "internal/traffic,console" {
		t.Fatalf("got %q, %v", names(us), err)
	}

	shallow := filepath.Join(t.TempDir(), "shallow")
	git(repo, "clone", "-q", "--depth", "1", "--branch", "work", "file://"+repo, shallow)
	git(shallow, "fetch", "-q", "--depth", "1", "origin", "main:refs/remotes/origin/main")
	if us, err := Changed(shallow, "origin/main"); err == nil {
		t.Fatalf("a clone with no merge base must be an error, got units %q", names(us))
	}
}

func TestProfileSaysInstrumentedAndCovered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.cov")
	os.WriteFile(path, []byte("mode: set\nexample.com/m/p/a.go:5.20,7.3 1 1\nexample.com/m/p/a.go:7.3,9.2 1 0\n"), 0o644)
	p, err := readProfile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		line, col           int
		instrumented, cover bool
	}{{6, 4, true, true}, {5, 19, false, false}, {7, 2, true, true}, {7, 3, true, false}, {8, 1, true, false}, {9, 2, false, false}, {3, 1, false, false}} {
		if i, c2 := p.at("a.go", c.line, c.col); i != c.instrumented || c2 != c.cover {
			t.Errorf("%d:%d: instrumented %v covered %v, want %v %v", c.line, c.col, i, c2, c.instrumented, c.cover)
		}
	}
	os.WriteFile(path, []byte("mode: set\nexample.com/m/p/a.go:7.3,9.2 1 1\n"), 0o644)
	q, _ := readProfile(path)
	p.union(q)
	if _, c := p.at("a.go", 8, 1); !c {
		t.Error("union marks what the other profile ran")
	}
}
