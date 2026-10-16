package main

import (
	"bytes"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/scripts/internal/githist"
)

// The coverage gate's own regression test: the gate is run, as CI runs it, in a
// copy of testdata/fixture that is a git repository of its own.

type repo struct {
	t   *testing.T
	dir string
}

func newRepo(t *testing.T) *repo {
	t.Helper()
	if testing.Short() {
		t.Skip("runs go test in a throwaway module")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	r := &repo{t: t, dir: t.TempDir()}
	err := filepath.WalkDir(filepath.Join("testdata", "fixture"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(filepath.Join("testdata", "fixture"), path)
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		r.write(filepath.ToSlash(rel), string(raw))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	r.git("init", "-q", "-b", "main")
	return r
}

func (r *repo) git(args ...string) {
	r.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = r.dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	if out, err := cmd.CombinedOutput(); err != nil {
		r.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func (r *repo) commit(msg string) {
	r.t.Helper()
	r.git("add", "-A")
	r.git("commit", "-q", "-m", msg)
}

func (r *repo) write(rel, body string) {
	r.t.Helper()
	p := filepath.Join(r.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		r.t.Fatal(err)
	}
}

func (r *repo) floors() string {
	raw, err := os.ReadFile(filepath.Join(r.dir, filepath.FromSlash(floorFile)))
	if err != nil {
		r.t.Fatal(err)
	}
	return string(raw)
}

// gate runs the gate and returns its exit code and everything it printed.
func (r *repo) gate(mode, lower string) (int, string) {
	var out bytes.Buffer
	rc := run(r.dir, mode, lower, "testos", &out, &out)
	return rc, out.String()
}

func (r *repo) expect(name string, rc int, out string, wantRC int, wantOut ...string) {
	r.t.Helper()
	if rc != wantRC {
		r.t.Errorf("%s: exit %d, want %d:\n%s", name, rc, wantRC, out)
	}
	for _, w := range wantOut {
		if !strings.Contains(out, w) {
			r.t.Errorf("%s: output does not contain %q:\n%s", name, w, out)
		}
	}
}

const noTests = "# no-tests: tool: a command with nothing to assert on in this fixture\n"

// recorded is a repository with its floors recorded and committed.
func recorded(t *testing.T) *repo {
	r := newRepo(t)
	r.write(floorFile, "# The fixture's floors.\n"+noTests)
	rc, out := r.gate("record", "")
	r.expect("record", rc, out, 0, "recorded testos floors")
	if got := r.floors(); !strings.Contains(got, "\na 100 testos\n") {
		t.Fatalf("recorded floors:\n%s", got)
	}
	r.commit("floors")
	return r
}

// B4's done-when: "a deliberately deleted test turns CI red".
func TestSelfTest_ADeletedTestTurnsTheGateRed(t *testing.T) {
	t.Parallel()
	r := recorded(t)
	rc, out := r.gate("check", "")
	r.expect("as recorded", rc, out, 0, "ok       a@testos: 100% (floor 100%)")

	if err := os.Remove(filepath.Join(r.dir, "a", "other_test.go")); err != nil {
		t.Fatal(err)
	}
	rc, out = r.gate("check", "")
	r.expect("with TestTriple deleted", rc, out, 1, "FELL     a@testos: 50% < floor 100%")

	// And `record` does not quietly make it green again.
	rc, out = r.gate("record", "")
	r.expect("record after the deletion", rc, out, 1, "refusing to lower a floor without a reason", "a@testos: 50% is below its floor 100%")
	if !strings.Contains(r.floors(), "\na 100 testos\n") {
		t.Errorf("a refused record must leave the file alone:\n%s", r.floors())
	}
	// With a reason it lowers, and the reason is in the file where the history rule reads it.
	rc, out = r.gate("record", "TestTriple was removed with the feature it tested")
	r.expect("record -lower", rc, out, 0, "every floor that ever went down")
	if got := r.floors(); !strings.Contains(got, "\na 50 testos\n") || !strings.Contains(got, " a@testos 100 -> 50: TestTriple was removed with the feature it tested\n") {
		t.Errorf("lowered floors:\n%s", got)
	}
	rc, out = r.gate("check", "")
	r.expect("after the reasoned lowering", rc, out, 0, "ok       a@testos: 50% (floor 50%)")
}

// The fault this rule is for: floors were edited down by hand three times, and
// nothing but `record` looked.
func TestSelfTest_AFloorLoweredByHandFailsUntilTheFileSaysWhy(t *testing.T) {
	t.Parallel()
	r := recorded(t)
	lowered := strings.Replace(r.floors(), "\na 100 testos\n", "\na 99.5 testos\n", 1)
	r.write(floorFile, lowered)
	rc, out := r.gate("check", "")
	r.expect("edited, not committed", rc, out, 1, "the working tree lowered the floor of a@testos from 100 to 99.5", "# lowered: ")

	r.commit("the stable value is 99.5")
	rc, out = r.gate("check", "")
	r.expect("edited and committed", rc, out, 1, "lowered the floor of a@testos from 100 to 99.5")
	if strings.Contains(out, "the working tree") {
		t.Errorf("the commit is named once it is one:\n%s", out)
	}
	// Raising it back does not undo the history: the lowering happened.
	r.write(floorFile, strings.Replace(lowered, "\na 99.5 testos\n", "\na 100 testos\n", 1))
	r.commit("back up")
	rc, out = r.gate("history", "")
	r.expect("raised again", rc, out, 1, "from 100 to 99.5")

	// A reason that is no reason does not parse; a real one passes.
	r.write(floorFile, r.floors()+"# lowered: 2026-10-12 a@testos 100 -> 99.5: flaky\n")
	rc, out = r.gate("history", "")
	r.expect("a five-letter reason", rc, out, 1, "reason of at least 15 characters")
	r.write(floorFile, strings.Replace(r.floors(), ": flaky\n", ": one run in ten misses the retry branch; see TestRetry\n", 1))
	rc, out = r.gate("history", "")
	r.expect("with its reason", rc, out, 0, "every floor that ever went down")
	// The line is for that step only: another step down needs its own.
	r.commit("the reason")
	r.write(floorFile, strings.Replace(r.floors(), "\na 100 testos\n", "\na 99 testos\n", 1))
	rc, out = r.gate("history", "")
	r.expect("a second lowering", rc, out, 1, "from 100 to 99 ")
}

func TestSelfTest_ARemovedFloorIsALoweredFloor(t *testing.T) {
	t.Parallel()
	r := recorded(t)
	r.write(floorFile, strings.Replace(r.floors(), "\na 100 testos\n", "\n", 1))
	rc, out := r.gate("check", "")
	r.expect("the line deleted", rc, out, 1, "removed the floor of a@testos (100)", "a@testos 100 -> none")
	r.write(floorFile, r.floors()+"# lowered: 2026-10-12 a@testos 100 -> none: the package moved to another module\n")
	rc, out = r.gate("history", "")
	r.expect("with its reason", rc, out, 0)
	// The history is satisfied; the package still has tests and no floor.
	rc, out = r.gate("check", "")
	r.expect("no floor", rc, out, 1, "NO FLOOR a: 100% on testos")
}

func TestSelfTest_AShallowCloneFails(t *testing.T) {
	t.Parallel()
	r := recorded(t)
	r.write(floorFile, strings.Replace(r.floors(), "\na 100 testos\n", "\na 10 testos\n", 1))
	r.commit("lowered by hand")
	clone := &repo{t: t, dir: filepath.Join(t.TempDir(), "clone")}
	r.git("clone", "-q", "--depth", "1", "file://"+r.dir, clone.dir)
	rc, out := clone.gate("check", "")
	clone.expect("depth 1", rc, out, 1, "shallow")
}

func TestSelfTest_FileFloorsUntestedPackagesAndPlatforms(t *testing.T) {
	t.Parallel()
	r := recorded(t)
	// A floor on one file, for every platform, as a stage sets a target.
	r.write(floorFile, r.floors()+"a/other.go 100\n")
	rc, out := r.gate("check", "")
	r.expect("file floor met", rc, out, 0, "ok       a/other.go: 100% (floor 100%)")
	// Half of other.go's test gone: the package is still above a lower floor, the file is not.
	r.write("a/other_test.go", "package a\n\nimport \"testing\"\n\nfunc TestTriple(t *testing.T) {\n\tif Triple(2) != 6 {\n\t\tt.Fatal(\"Triple\")\n\t}\n}\n")
	r.write(floorFile, strings.Replace(r.floors(), "\na 100 testos\n", "\na 80 testos\n# lowered: 2026-10-12 a@testos 100 -> 80: half of the test was removed in this step\n", 1))
	rc, out = r.gate("check", "")
	r.expect("file floor missed", rc, out, 1, "FELL     a/other.go: 66.7% < floor 100%", "ok       a@testos: 83.3% (floor 80%)")

	// A floor for another platform does not apply here, and does not stand in for ours.
	r.write(floorFile, "# floors\na 100 otheros\n"+noTests)
	m := &Measured{Pct: map[string]float64{"a": 50}}
	f, err := Parse([]byte(r.floors()))
	if err != nil {
		t.Fatal(err)
	}
	fails, _ := Check(f, m, "testos")
	if len(fails) != 1 || !strings.Contains(fails[0], "NO FLOOR a: 50% on testos") {
		t.Errorf("only another platform's floor: %v", fails)
	}
	if fails, _ := Check(f, m, "otheros"); len(fails) != 1 || !strings.Contains(fails[0], "FELL     a@otheros") {
		t.Errorf("on the platform the floor names: %v", fails)
	}

	// A package with no tests needs its sentence; one that gained tests loses it.
	f, _ = Parse([]byte("a 50\n"))
	fails, _ = Check(f, &Measured{Pct: map[string]float64{"a": 50}, Untested: []string{"tool"}}, "testos")
	if len(fails) != 1 || !strings.Contains(fails[0], "NO TESTS tool") {
		t.Errorf("an untested package: %v", fails)
	}
	f, _ = Parse([]byte("a 50\n# no-tests: a: it had none when this was written\n"))
	fails, _ = Check(f, &Measured{Pct: map[string]float64{"a": 50}}, "testos")
	if len(fails) != 1 || !strings.Contains(fails[0], "STALE    a has tests now") {
		t.Errorf("a stale no-tests line: %v", fails)
	}
	// A floor whose package is gone is not a pass.
	f, _ = Parse([]byte("a 50\ngone 80\n"))
	fails, _ = Check(f, &Measured{Pct: map[string]float64{"a": 50}}, "testos")
	if len(fails) != 1 || !strings.Contains(fails[0], "MISSING  gone") {
		t.Errorf("a floor with nothing measured: %v", fails)
	}
	// The tolerance is a tenth of a point and no more.
	f, _ = Parse([]byte("a 50\n"))
	if fails, _ := Check(f, &Measured{Pct: map[string]float64{"a": 49.9}}, "testos"); len(fails) != 0 {
		t.Errorf("0.1 below is inside the tolerance: %v", fails)
	}
	if fails, _ := Check(f, &Measured{Pct: map[string]float64{"a": 49.8}}, "testos"); len(fails) != 1 {
		t.Errorf("0.2 below is a fall: %v", fails)
	}
}

func TestParseRefusesWhatItCannotRead(t *testing.T) {
	for _, bad := range []string{
		"a\n", "a b\n", "a 101\n", "a 50 linux extra\n", "a 50\na 60\n",
		"# lowered: yesterday a 60 -> 50: a reason long enough\n",
		"# lowered: 2026-10-12 a 60 50: a reason long enough\n",
		"# lowered: 2026-10-12 a 60 -> 50: short\n",
		"# no-tests: tool\n", "# no-tests: tool: why\n",
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("%q must not parse", bad)
		}
	}
	f, err := Parse([]byte("# head\n\na 50\na 60 linux\nb/x.go 92\n# lowered: 2026-10-12 a@linux 70 -> 60: the tests moved to another package\n# lowered: 2026-10-12 c 70 -> none: the package was deleted with its feature\n# no-tests: tool: a command; its work is the suite's\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Entries) != 3 || len(f.Lowered) != 2 || !math.IsNaN(f.Lowered[1].To) || f.NoTests["tool"] == "" || len(f.Head) != 1 {
		t.Errorf("parsed %+v", f)
	}
	again, err := Parse(f.Format())
	if err != nil || len(again.Entries) != 3 || len(again.Lowered) != 2 || len(again.NoTests) != 1 {
		t.Errorf("formatted and parsed again: %v\n%s", err, f.Format())
	}
}

// A floor with no platform holds everywhere, so replacing it with per-platform
// lines lowers nothing only if every platform's line is at least as high.
func TestHistoryComparesTheFloorInForceOnEachPlatform(t *testing.T) {
	change := func(old, now string) []githist.Change {
		return []githist.Change{{Commit: "abc1234", Date: "2026-10-12", Old: []byte(old), New: []byte(now)}}
	}
	now := "a 85 darwin\na 84.8 linux\n"
	f, _ := Parse([]byte(now))
	bad := History(f, change("a 84.9\n", now))
	if len(bad) != 1 || !strings.Contains(bad[0], "lowered the floor of a@linux from 84.9 to 84.8") {
		t.Errorf("want only linux flagged: %v", bad)
	}
	now += "# lowered: 2026-10-12 a@linux 84.9 -> 84.8: measured in a container; 84.9 was the macOS number\n"
	f, _ = Parse([]byte(now))
	if bad := History(f, change("a 84.9\n", now)); len(bad) != 0 {
		t.Errorf("explained: %v", bad)
	}
	// A version that does not parse holds no floor: it cannot hide a step down.
	f, _ = Parse([]byte("a 10\n"))
	steps := []githist.Change{
		{Commit: "abc1234", Date: "2026-10-12", Old: []byte("a 90\n"), New: []byte("not a floor file at all\n")},
		{Commit: "def5678", Date: "2026-10-12", Old: []byte("not a floor file at all\n"), New: []byte("a 10\n")},
	}
	if bad := History(f, steps); len(bad) != 1 || !strings.Contains(bad[0], "removed the floor of a (90)") {
		t.Errorf("garbage as a step on the way down: %v", bad)
	}
}

func TestReadProfile(t *testing.T) {
	raw := "mode: set\nexample.com/m/a/a.go:1.1,2.2 3 1\nexample.com/m/a/a.go:3.1,4.2 1 0\nexample.com/m/a/b.go:1.1,2.2 2 0\nexample.com/m/a/b.go:1.1,2.2 2 1\nexample.com/m/main.go:1.1,2.2 1 0\n"
	got, err := ReadProfile([]byte(raw), "example.com/m")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"a/a.go": 75, "a/b.go": 100, "a": 83.3, "main.go": 0, ".": 0}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %v, want %v (all: %v)", k, got[k], v, got)
		}
	}
}
