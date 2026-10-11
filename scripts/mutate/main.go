// Command mutate runs gremlins (a go tool) over the packages whose tests must
// notice when the code is wrong, and keeps the survivors in testdata/mutation.json.
//
//	go run ./scripts/mutate record ./internal/protocol ...   write the baseline
//	go run ./scripts/mutate check  ./internal/protocol ...   fail on a new survivor
//	go run ./scripts/mutate list                            the units and their files
//
// A unit is a package, or one file group of a package that is too slow to mutate
// whole (internal/broker). Naming a package runs all its units.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Unit is what one gremlins run covers.
type Unit struct {
	Name string   // key in the baseline.
	Dir  string   // package directory, slash-separated, relative to the module root.
	Only []string // file names (without .go) to mutate; empty means all of them.
	// Timeout is gremlins' timeout coefficient: a mutant gets this many times the
	// package's own test time, and that time includes compiling it. A package
	// whose tests take under a second needs a large one, or a loaded machine
	// calls a surviving mutant "timed out".
	Timeout int
}

// Units is every package the plan names, split by file group where the package's
// tests take long enough that one run would pass ten minutes.
var Units = []Unit{
	{Name: "internal/protocol", Dir: "internal/protocol", Timeout: 200},
	{Name: "internal/storage/log", Dir: "internal/storage/log", Timeout: 40},
	{Name: "internal/storage/meta", Dir: "internal/storage/meta", Timeout: 200},
	{Name: "internal/traffic", Dir: "internal/traffic", Timeout: 200},
	{Name: "internal/telemetry", Dir: "internal/telemetry", Timeout: 200},
	{Name: "schemaregistry", Dir: "schemaregistry", Timeout: 200},
	{Name: "console", Dir: "console"},
	// The broker by what the file does: each group is one run.
	{Name: "internal/broker:groups", Dir: "internal/broker", Only: []string{
		"group", "group848", "group848_assign", "groups", "groups848", "groupadmin", "groupdelete", "groupevents", "grouptimer", "offsets"}},
	{Name: "internal/broker:data", Dir: "internal/broker", Only: []string{
		"produce", "fetch", "idempotency", "listoffsets", "deleterecords", "producerid", "txn", "txnapis"}},
	{Name: "internal/broker:admin", Dir: "internal/broker", Only: []string{
		"topics", "topicconfig", "topiclimits", "configs", "alterconfigs", "createpartitions", "metadata", "apiversions", "telemetry"}},
	{Name: "internal/broker:auth", Dir: "internal/broker", Only: []string{
		"auth", "authorizer", "acls", "scram", "broker"}},
}

// Result is the baseline's record of one unit.
type Result struct {
	Killed     int      `json:"killed"`
	TimedOut   int      `json:"timedOut"`
	Lived      int      `json:"lived"`
	NotCovered int      `json:"notCovered"`
	Survivors  []string `json:"survivors"` // file:line:TYPE of each mutant that lived.
}

// Baseline is testdata/mutation.json.
type Baseline struct {
	Tool  string            `json:"tool"`
	Units map[string]Result `json:"units"`
}

const baselinePath = "testdata/mutation.json"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	mode := os.Args[1]
	switch mode {
	case "list":
		for _, u := range Units {
			fmt.Printf("%-26s %s %v\n", u.Name, u.Dir, u.Only)
		}
		return
	case "record", "check":
	default:
		usage()
	}
	units, err := Select(os.Args[2:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	base := Baseline{Tool: "gremlins", Units: map[string]Result{}}
	if raw, err := os.ReadFile(baselinePath); err == nil {
		if err := json.Unmarshal(raw, &base); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", baselinePath, err)
			os.Exit(1)
		}
		if base.Units == nil {
			base.Units = map[string]Result{}
		}
	}
	failed := false
	for _, u := range units {
		got, err := Run(u)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", u.Name, err)
			os.Exit(1)
		}
		fmt.Printf("%-26s killed %d, timed out %d, lived %d, not covered %d\n", u.Name, got.Killed, got.TimedOut, got.Lived, got.NotCovered)
		if mode == "record" {
			base.Units[u.Name] = got
			continue
		}
		want, ok := base.Units[u.Name]
		if !ok {
			fmt.Fprintf(os.Stderr, "NO BASELINE %s: run `task mutate:record -- ./%s`\n", u.Name, u.Dir)
			failed = true
			continue
		}
		for _, s := range Diff(want, got) {
			fmt.Fprintf(os.Stderr, "NEW      %s: %s\n", u.Name, s)
			failed = true
		}
	}
	if mode == "record" {
		if err := write(base); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("recorded", baselinePath)
		return
	}
	if failed {
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mutate record|check [./package ...]   (no package: every unit)\n       mutate list")
	os.Exit(2)
}

// Select maps command-line arguments (unit names or package directories) to units.
func Select(args []string) ([]Unit, error) {
	if len(args) == 0 {
		return Units, nil
	}
	var out []Unit
	seen := map[string]bool{}
	for _, a := range args {
		a = strings.TrimSuffix(strings.TrimPrefix(filepath.ToSlash(a), "./"), "/...")
		a = strings.TrimSuffix(a, "/")
		n := 0
		for _, u := range Units {
			if u.Name == a || u.Dir == a {
				n++
				if !seen[u.Name] {
					seen[u.Name] = true
					out = append(out, u)
				}
			}
		}
		if n == 0 {
			return nil, fmt.Errorf("%q is not a package with a mutation baseline (see: mutate list)", a)
		}
	}
	return out, nil
}

// Diff says what is worse in got than in want: a survivor the baseline does not
// have, or more mutants that no test reaches.
func Diff(want, got Result) []string {
	had := map[string]bool{}
	for _, s := range want.Survivors {
		had[s] = true
	}
	var out []string
	for _, s := range got.Survivors {
		if !had[s] {
			out = append(out, "survivor "+s+" (a test should fail when this line is mutated; or `task mutate:record` if it is understood)")
		}
	}
	if got.NotCovered > want.NotCovered {
		out = append(out, fmt.Sprintf("%d mutants are reached by no test, baseline %d", got.NotCovered, want.NotCovered))
	}
	return out
}

// gremlins' machine-readable report.
type report struct {
	Files []struct {
		Name      string `json:"file_name"`
		Mutations []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
			Line   int    `json:"line"`
			Column int    `json:"column"`
		} `json:"mutations"`
	} `json:"files"`
}

// Run mutates one unit.
func Run(u Unit) (Result, error) {
	tmp, err := os.CreateTemp("", "gremlins-*.json")
	if err != nil {
		return Result{}, err
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	args := []string{"tool", "gremlins", "unleash", "./" + u.Dir,
		"--workers", env("MUTATE_WORKERS", "4"), "--test-cpu", "2",
		// A mutant that loops forever is killed by timeout; a generous coefficient
		// keeps a loaded machine from calling a slow test a survivor.
		"--timeout-coefficient", env("MUTATE_TIMEOUT", fmt.Sprint(max(u.Timeout, 20))),
		"-o", tmp.Name()}
	if len(u.Only) > 0 {
		files, err := filepath.Glob(filepath.Join(u.Dir, "*.go"))
		if err != nil {
			return Result{}, err
		}
		keep := map[string]bool{}
		for _, n := range u.Only {
			keep[n] = true
		}
		for _, f := range files {
			base := strings.TrimSuffix(filepath.Base(f), ".go")
			if strings.HasSuffix(base, "_test") || keep[base] {
				continue
			}
			args = append(args, "-E", "(^|/)"+regexp.QuoteMeta(filepath.Base(f))+"$")
		}
	}
	cmd := exec.Command("go", args...)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		// gremlins exits non-zero below its thresholds; the report is still written.
		if _, ok := err.(*exec.ExitError); !ok {
			return Result{}, err
		}
		_ = out
	}
	raw, err := os.ReadFile(tmp.Name())
	if err != nil || len(raw) == 0 {
		return Result{}, fmt.Errorf("gremlins wrote no report: %v\n%s", err, out)
	}
	return Parse(raw, u.Dir)
}

// Parse turns gremlins' report into a Result.
func Parse(raw []byte, dir string) (Result, error) {
	var rep report
	if err := json.Unmarshal(raw, &rep); err != nil {
		return Result{}, err
	}
	res := Result{Survivors: []string{}}
	for _, f := range rep.Files {
		for _, m := range f.Mutations {
			switch m.Status {
			case "KILLED":
				res.Killed++
			case "TIMED OUT":
				res.TimedOut++
			case "LIVED":
				res.Lived++
				res.Survivors = append(res.Survivors, fmt.Sprintf("%s/%s:%d:%s", dir, f.Name, m.Line, m.Type))
			case "NOT COVERED":
				res.NotCovered++
			}
		}
	}
	sort.Strings(res.Survivors)
	return res, nil
}

func write(b Baseline) error {
	raw, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(baselinePath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(baselinePath, append(raw, '\n'), 0o644)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
