package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/scripts/internal/githist"
)

// The lightness gate's own regression test. The gate this replaced allowed 5%
// over a budget recorded on another platform: a binary 1 MiB heavier and an idle
// heap 64 KiB heavier both passed, and main sat above both budgets inside that
// slack. Here the gate is run, as CI runs it, in a copy of testdata/fixture (a
// stand-in broker), and then both of those are planted.
func TestSelfTest_AHeavierBinaryAndAHeavierHeapFail(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and starts the fixture's binary")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	dir := t.TempDir()
	copyTree(t, filepath.Join("testdata", "fixture"), dir)
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com",
			"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	gate := func(mode, reason string) (int, string) {
		var out bytes.Buffer
		// Fewer starts than the real gate's 5 and 20: the fixture's heap does not vary.
		runs := 3
		if mode == "record" {
			runs = 6
		}
		rc := run(dir, mode, reason, "the gate's own test", runs, &out, &out)
		return rc, out.String()
	}
	expect := func(name string, rc int, out string, wantRC int, want ...string) {
		t.Helper()
		if rc != wantRC {
			t.Errorf("%s: exit %d, want %d:\n%s", name, rc, wantRC, out)
		}
		for _, w := range want {
			if !strings.Contains(out, w) {
				t.Errorf("%s: output does not contain %q:\n%s", name, w, out)
			}
		}
	}
	git("init", "-q", "-b", "main")

	platform := runtime.GOOS + "/" + runtime.GOARCH
	rc, out := gate("check", "")
	expect("no budget file", rc, out, 1)
	rc, out = gate("record", "")
	expect("record", rc, out, 0, "recorded "+platform)
	// The stand-in's heap moves by a few KB from start to start when the machine is
	// busy (its pprof handlers allocate while they answer: 121,728 to 129,224 bytes
	// over three starts in a loaded Linux container), more than six starts at record
	// time always see. A 16 KiB allowance keeps this test about the gate, and the 64
	// KiB planted below is still four times over it.
	path := filepath.Join(dir, filepath.FromSlash(budgetFile))
	raw0, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b0, err := ParseBudget(raw0)
	if err != nil {
		t.Fatal(err)
	}
	p0 := b0.Platforms[platform]
	p0.IdleHeapNoiseBytes = max(p0.IdleHeapNoiseBytes, 16<<10)
	b0.Platforms[platform] = p0
	if err := os.WriteFile(path, b0.format(), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", "-A")
	git("commit", "-q", "-m", "budget")
	rc, out = gate("check", "")
	expect("as recorded", rc, out, 0, "ok       "+platform+" binaryBytes", "ok       "+platform+" idleHeapBytes", "ok       modules: 1 (budget 1)")

	// 64 KiB more heap.
	t.Setenv("FIXTURE_BALLAST", "65536")
	rc, out = gate("check", "")
	expect("64 KiB more idle heap", rc, out, 1, "OVER     "+platform+" idleHeapBytes")
	rc, out = gate("record", "")
	expect("record without a reason", rc, out, 1, "refusing to raise a budget without a reason", "idleHeapBytes")
	t.Setenv("FIXTURE_BALLAST", "")

	// 1 MiB more binary: a megabyte of data the program really uses.
	big := "package main\n\nimport \"os\"\n\nfunc init() {\n\tif len(os.Args) > 99 {\n\t\tos.Stdout.WriteString(megabyte)\n\t}\n}\n\nconst megabyte = \"" + strings.Repeat("0123456789abcdef", 65536) + "\"\n"
	if err := os.WriteFile(filepath.Join(dir, "cmd", "doze-kafka", "big.go"), []byte(big), 0o644); err != nil {
		t.Fatal(err)
	}
	rc, out = gate("check", "")
	expect("1 MiB more binary", rc, out, 1, "OVER     "+platform+" binaryBytes")
	// Recorded with a reason, the reason is in the file and the history rule is satisfied.
	rc, out = gate("record", "the fixture now carries a megabyte of help text")
	expect("record with a reason", rc, out, 0, "every budget that ever went up")
	raw, _ := os.ReadFile(filepath.Join(dir, filepath.FromSlash(budgetFile)))
	if !strings.Contains(string(raw), "the fixture now carries a megabyte of help text") || !strings.Contains(string(raw), platform+" binaryBytes") {
		t.Errorf("the raise is not in the file:\n%s", raw)
	}
	git("add", "-A")
	git("commit", "-q", "-m", "heavier, with its reason")

	// A budget raised by hand, with no entry: the history rule fails, committed or not.
	edited := strings.Replace(string(raw), `"modules": 1`, `"modules": 9`, 1)
	if edited == string(raw) {
		t.Fatalf("the budget file has no modules line to edit:\n%s", raw)
	}
	if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(budgetFile)), []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
	rc, out = gate("history", "")
	expect("modules raised by hand", rc, out, 1, "the working tree raised modules from 1 to 9", `"raises"`)
	git("commit", "-q", "-am", "more modules are fine")
	rc, out = gate("check", "")
	expect("raised by hand and committed", rc, out, 1, "raised modules from 1 to 9")

	// A shallow clone cannot show the history, so it fails.
	clone := filepath.Join(t.TempDir(), "clone")
	git("clone", "-q", "--depth", "1", "file://"+dir, clone)
	var buf bytes.Buffer
	if rc := run(clone, "history", "", "", 0, &buf, &buf); rc != 1 || !strings.Contains(buf.String(), "shallow") {
		t.Errorf("a shallow clone: exit %d\n%s", rc, buf.String())
	}
}

func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		dst := filepath.Join(to, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		return os.WriteFile(dst, raw, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func budget(platform string, binary, heap, noise int64) *Budget {
	return &Budget{Build: buildLine, Run: runLine, Modules: 10, Platforms: map[string]Platform{
		platform: {BinaryBytes: binary, IdleHeapBytes: heap, IdleHeapNoiseBytes: noise, Go: "go1.27.2"}}}
}

func TestCompare(t *testing.T) {
	b := budget("linux/amd64", 28_000_000, 1_400_000, 6_000)
	m := func(binary, modules int64, heaps ...int64) *Measured {
		return &Measured{Platform: "linux/amd64", Go: "go1.27.2", BinaryBytes: binary, Modules: modules, Heaps: heaps}
	}
	cases := []struct {
		name string
		m    *Measured
		want string // "" passes.
	}{
		{"exactly the budget", m(28_000_000, 10, 1_370_000, 1_398_000, 1_400_000), ""},
		{"one byte of binary over", m(28_000_001, 10, 1_400_000), "OVER     linux/amd64 binaryBytes: 28000001 > budget 28000000 (1 over)"},
		{"the heap inside what was measured as variation", m(28_000_000, 10, 1_370_000, 1_406_000), ""},
		{"the heap just past it", m(28_000_000, 10, 1_370_000, 1_406_001), "OVER     linux/amd64 idleHeapBytes"},
		{"64 KiB more heap", m(28_000_000, 10, 1_465_536), "OVER     linux/amd64 idleHeapBytes"},
		{"one module more", m(28_000_000, 11, 1_400_000), "OVER     modules: 11 > budget 10"},
		{"lighter everywhere", m(27_000_000, 9, 1_300_000), ""},
		{"another toolchain", &Measured{Platform: "linux/amd64", Go: "go1.28.0", BinaryBytes: 28_000_000, Modules: 10, Heaps: []int64{1_400_000}}, "TOOLCHAIN linux/amd64"},
		{"a platform with no budget", &Measured{Platform: "darwin/arm64", Go: "go1.27.2", BinaryBytes: 1, Modules: 1, Heaps: []int64{1}}, "NO BUDGET for darwin/arm64 (recorded: linux/amd64)"},
	}
	for _, c := range cases {
		fails, _ := Compare(b, c.m)
		if c.want == "" && len(fails) != 0 {
			t.Errorf("%s: want a pass, got %v", c.name, fails)
		}
		if c.want != "" && (len(fails) != 1 || !strings.Contains(fails[0], c.want)) {
			t.Errorf("%s: want one failure containing %q, got %v", c.name, c.want, fails)
		}
	}
	// A budget for another build is no budget for this one.
	other := budget("linux/amd64", 28_000_000, 1_400_000, 6_000)
	other.Build = "go build ./cmd/doze-kafka"
	if fails, _ := Compare(other, m(1, 1, 1)); len(fails) != 1 || !strings.Contains(fails[0], "another build") {
		t.Errorf("another build line: %v", fails)
	}
}

func TestRecordOnlyGoesDownWithoutAReason(t *testing.T) {
	b := budget("linux/amd64", 28_000_000, 1_400_000, 6_000)
	lighter := &Measured{Platform: "linux/amd64", Go: "go1.27.2", BinaryBytes: 27_000_000, Modules: 9, Heaps: []int64{1_290_000, 1_300_000, 1_301_000}}
	if refused := Record(b, lighter, "ci", "", "2026-10-12"); len(refused) != 0 {
		t.Fatalf("lighter needs no reason: %v", refused)
	}
	p := b.Platforms["linux/amd64"]
	if p.BinaryBytes != 27_000_000 || p.IdleHeapBytes != 1_301_000 || p.IdleHeapNoiseBytes != 1_000 || b.Modules != 9 || len(b.Raises) != 0 {
		t.Errorf("recorded %+v, modules %d, raises %v", p, b.Modules, b.Raises)
	}
	// The same heap as far as it can be measured does not move the budget.
	same := &Measured{Platform: "linux/amd64", Go: "go1.27.2", BinaryBytes: 27_000_000, Modules: 9, Heaps: []int64{1_300_500, 1_301_900}}
	Record(b, same, "ci", "", "2026-10-12")
	if got := b.Platforms["linux/amd64"].IdleHeapBytes; got != 1_301_000 {
		t.Errorf("a re-record inside the variation walked the budget to %d", got)
	}
	heavier := &Measured{Platform: "linux/amd64", Go: "go1.27.2", BinaryBytes: 27_000_100, Modules: 10, Heaps: []int64{1_400_000}}
	refused := Record(b, heavier, "ci", "", "2026-10-12")
	if len(refused) != 3 {
		t.Fatalf("binary, heap and modules all went up: %v", refused)
	}
	if b.Platforms["linux/amd64"].BinaryBytes != 27_000_000 || b.Modules != 9 {
		t.Error("a refused record must change nothing")
	}
	if refused := Record(b, heavier, "ci", "the registry now links a JSON Schema validator", "2026-10-12"); len(refused) != 0 {
		t.Fatal(refused)
	}
	if len(b.Raises) != 3 || b.Platforms["linux/amd64"].BinaryBytes != 27_000_100 || b.Modules != 10 {
		t.Errorf("after the reasoned record: %+v", b)
	}
	// Another platform is recorded beside it, not over it.
	mac := &Measured{Platform: "darwin/arm64", Go: "go1.27.2", BinaryBytes: 26_000_000, Modules: 10, Heaps: []int64{1_000_000}}
	if refused := Record(b, mac, "a laptop", "", "2026-10-12"); len(refused) != 0 || len(b.Platforms) != 2 {
		t.Errorf("a second platform: %v, %d platforms", refused, len(b.Platforms))
	}
}

func TestHistory(t *testing.T) {
	step := func(old, now string) githist.Change {
		return githist.Change{Commit: "abc1234", Date: "2026-10-11", Old: []byte(old), New: []byte(now)}
	}
	// The shape the file had before budgets were per platform is read as macOS/arm64's.
	v1 := `{"binaryBytes": 100, "idleHeapBytes": 50, "modules": 4, "slackPercent": 5}`
	v2 := `{"binaryBytes": 120, "idleHeapBytes": 40, "modules": 5, "slackPercent": 5}`
	now := &Budget{Modules: 5, Platforms: map[string]Platform{"darwin/arm64": {BinaryBytes: 120, IdleHeapBytes: 40}}}
	bad := History(now, []githist.Change{step(v1, v2)})
	if len(bad) != 2 || !strings.Contains(bad[0], "raised darwin/arm64 binaryBytes from 100 to 120") || !strings.Contains(bad[1], "raised modules from 4 to 5") {
		t.Fatalf("want the binary and the modules, got %v", bad)
	}
	now.Raises = []Raise{
		{Key: "darwin/arm64 binaryBytes", From: 100, To: 120, Reason: "the console's phone layout and its assets"},
		{Key: "modules", From: 4, To: 5, Reason: "short"},
	}
	if bad := History(now, []githist.Change{step(v1, v2)}); len(bad) != 1 || !strings.Contains(bad[0], "modules") {
		t.Errorf("a reason under 15 characters is not one: %v", bad)
	}
	now.Raises[1].Reason = "the mutation tool and what it depends on"
	if bad := History(now, []githist.Change{step(v1, v2)}); len(bad) != 0 {
		t.Errorf("explained: %v", bad)
	}
	// A platform's budget that disappears is a budget raised without bound.
	gone := fmt.Sprintf(`{"build": %q, "run": %q, "modules": 5, "platforms": {}}`, buildLine, runLine)
	if bad := History(&Budget{}, []githist.Change{step(v2, gone)}); len(bad) != 2 || !strings.Contains(bad[0], "removed the budget darwin/arm64 binaryBytes (120)") {
		t.Errorf("a removed platform: %v", bad)
	}
}
