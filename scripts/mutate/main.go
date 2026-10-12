// Command mutate asks whether the tests notice when the code is wrong: it swaps
// one operator at a time in a package (a `<` for a `<=`, a `+` for a `-`), runs
// the package's tests against each such mutant, and keeps what the tests did not
// notice in testdata/mutation.json.
//
//	go run ./scripts/mutate list                          the units, their files and shards
//	go run ./scripts/mutate check  [flags] [unit ...]     fail on anything the baseline does not list
//	go run ./scripts/mutate record [flags] [unit ...]     write the baseline (new entries are "open")
//	go run ./scripts/mutate report                        write plan/evidence/mutation-survivors.md
//	go run ./scripts/mutate changed <git ref>             the units whose package differs from <ref>
//
// Flags: -shard i/n (check only: every n-th mutant), -changed <ref> (only the
// units `changed` prints), -workers n, -whole (the whole suite for every mutant).
//
// A unit is a package, or one file group of a package (internal/broker). Naming
// a package runs all its units. The engine (engine.go) says how a mutant is
// judged; the short of it is that only a test that started and failed kills one.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Unit is what one run covers.
type Unit struct {
	Name string   // key in the baseline.
	Dir  string   // package directory, slash-separated, relative to the module root.
	Only []string // file names (without .go) to mutate; empty means all of them.
	// Shards is how many jobs the weekly workflow splits the unit into, so that
	// none passes the plan's ten minutes by much on a four-core runner. The
	// figures beside each are this machine's (18 cores, 4 workers), measured.
	Shards int
	// Whole runs the whole suite for each mutant. For a package whose tests do
	// not all pass on their own.
	Whole bool
}

// Units is every package the plan names (B3), the broker by file group, and the
// three small packages the binary's behaviour starts in.
var Units = []Unit{
	{Name: "internal/protocol", Dir: "internal/protocol", Shards: 1},
	{Name: "internal/storage/log", Dir: "internal/storage/log", Shards: 1},
	{Name: "internal/storage/meta", Dir: "internal/storage/meta", Shards: 1},
	{Name: "internal/traffic", Dir: "internal/traffic", Shards: 1},
	{Name: "internal/telemetry", Dir: "internal/telemetry", Shards: 1},
	{Name: "internal/settings", Dir: "internal/settings", Shards: 1},
	{Name: "internal/config", Dir: "internal/config", Shards: 1},
	{Name: "schemaregistry", Dir: "schemaregistry", Shards: 1},
	{Name: "console", Dir: "console", Shards: 1},
	{Name: "cmd/doze-kafka", Dir: "cmd/doze-kafka", Shards: 1},
	// The broker by what the file does: each group is one unit.
	{Name: "internal/broker:groups", Dir: "internal/broker", Shards: 1, Only: []string{
		"group", "group848", "group848_assign", "groups", "groups848", "groupadmin", "groupdelete", "groupevents", "grouptimer", "groupmigrate", "offsetexpiry", "offsets"}},
	{Name: "internal/broker:data", Dir: "internal/broker", Shards: 1, Only: []string{
		"produce", "fetch", "idempotency", "listoffsets", "deleterecords", "producerid", "txn", "txnapis"}},
	{Name: "internal/broker:admin", Dir: "internal/broker", Shards: 1, Only: []string{
		"topics", "topicconfig", "topiclimits", "configs", "alterconfigs", "createpartitions", "metadata", "apiversions", "telemetry"}},
	{Name: "internal/broker:auth", Dir: "internal/broker", Shards: 1, Only: []string{
		"auth", "authorizer", "acls", "scram", "broker"}},
}

// Debt is one mutant the tests do not notice, as the baseline keeps it.
type Debt struct {
	At     string `json:"at"`     // file:line:col:TYPE when it was recorded.
	Key    string `json:"key"`    // file:declaration:TYPE#n, which survives edits elsewhere in the file.
	Change string `json:"change"` // "< became <=".
	Source string `json:"source"` // the line, when it was recorded.
	Status Status `json:"status"` // lived, notCovered, timedOut or setupFailed.
	// Reached names tests that execute the line (a lived mutant only).
	Reached string `json:"reached,omitempty"`
	// Reason is why it is accepted, written by a person who checked, or "open":
	// nobody has looked yet.
	Reason string `json:"reason"`
}

// Open is the reason of an entry nobody has examined.
const Open = "open"

// UnitBase is the baseline's record of one unit.
type UnitBase struct {
	Platform     string  `json:"platform"`
	Recorded     string  `json:"recorded"`
	Commit       string  `json:"commit"`
	Workers      int     `json:"workers"`
	Tests        int     `json:"tests"`
	SuiteSeconds float64 `json:"suiteSeconds"`
	Seconds      float64 `json:"seconds"`
	Mutants      int     `json:"mutants"`
	Killed       int     `json:"killed"`
	Lived        int     `json:"lived"`
	NotCovered   int     `json:"notCovered"`
	TimedOut     int     `json:"timedOut"`
	NotViable    int     `json:"notViable"`
	SetupFailed  int     `json:"setupFailed"`
	Open         int     `json:"open"` // entries of Debt whose reason is "open".
	Debt         []Debt  `json:"debt"`
}

// Baseline is testdata/mutation.json.
type Baseline struct {
	Tool  string              `json:"tool"`
	Units map[string]UnitBase `json:"units"`
}

const (
	baselinePath = "testdata/mutation.json"
	cacheDir     = ".mutate"
	reportPath   = "plan/evidence/mutation-survivors.md"
	toolName     = "scripts/mutate: go test per mutant; ARITHMETIC_BASE, CONDITIONALS_BOUNDARY, CONDITIONALS_NEGATION, INCREMENT_DECREMENT, INVERT_NEGATIVES"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	mode := os.Args[1]
	switch mode {
	case "list":
		for _, u := range Units {
			fmt.Printf("%-26s %d %s %v\n", u.Name, max(u.Shards, 1), u.Dir, u.Only)
		}
		return
	case "matrix":
		// One line of JSON for a workflow matrix: every unit and shard, or with a
		// ref only the units whose package changed since it.
		units := Units
		if len(os.Args) > 2 {
			var err error
			if units, err = Changed(".", os.Args[2]); err != nil {
				fatal(err)
			}
		}
		fmt.Println(Matrix(units))
		return
	case "changed":
		if len(os.Args) != 3 {
			usage()
		}
		us, err := Changed(".", os.Args[2])
		if err != nil {
			fatal(err)
		}
		for _, u := range us {
			fmt.Println(u.Name)
		}
		return
	case "report":
		base, err := readBaseline()
		if err != nil {
			fatal(err)
		}
		if err := os.WriteFile(reportPath, []byte(Report(base)), 0o644); err != nil {
			fatal(err)
		}
		fmt.Println("wrote", reportPath)
		return
	case "record", "check":
	default:
		usage()
	}

	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	shard := fs.String("shard", "", "i/n: every n-th mutant, starting at the i-th (check only)")
	changed := fs.String("changed", "", "a git ref: only the units whose package differs from it")
	workers := fs.Int("workers", envInt("MUTATE_WORKERS", 4), "mutants run at once")
	whole := fs.Bool("whole", os.Getenv("MUTATE_WHOLE") == "1", "run the whole suite for every mutant")
	coef := fs.Float64("timeout", float64(envInt("MUTATE_TIMEOUT", 4)), "a mutant's run may take this many times the tests' own time")
	fs.Parse(os.Args[2:])

	units, err := Select(fs.Args())
	if err != nil {
		fatal(err)
	}
	if *changed != "" {
		if len(fs.Args()) > 0 {
			fatal(fmt.Errorf("-changed picks the units itself: name none"))
		}
		if units, err = Changed(".", *changed); err != nil {
			fatal(err)
		}
		if len(units) == 0 {
			fmt.Println("no package with a mutation baseline differs from", *changed)
			return
		}
	}
	sh, shs := 1, 1
	if *shard != "" {
		if mode == "record" {
			fatal(fmt.Errorf("a baseline is recorded from a whole unit, not a shard"))
		}
		if sh, shs, err = parseShard(*shard); err != nil {
			fatal(err)
		}
	}
	base, err := readBaseline()
	if err != nil {
		fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	failed := false
	for _, u := range units {
		e := &Engine{Root: ".", Dir: u.Dir, Only: u.Only, Workers: *workers, Coef: *coef, Whole: *whole || u.Whole, Shard: sh, Shards: shs, Log: os.Stderr}
		run, err := e.Run(ctx)
		if err != nil {
			// Not a result: the gate cannot say anything, and that is a failure.
			fmt.Fprintf(os.Stderr, "FAILED   %s: %v\n", u.Name, err)
			os.Exit(1)
		}
		saveRun(u, run)
		fmt.Printf("%-26s %d mutants: killed %d, lived %d, not covered %d, timed out %d, not viable %d, setup failed %d (%.0fs, suite %.1fs, %d workers)\n",
			u.Name, len(run.Mutants), run.Count(Killed), run.Count(Lived), run.Count(NotCovered), run.Count(TimedOut), run.Count(NotViable), run.Count(SetupFailed),
			run.Seconds, run.SuiteSeconds, run.Workers)
		if mode == "record" {
			nb, added := Record(base.Units[u.Name], run)
			nb.Commit = commit()
			base.Units[u.Name] = nb
			for _, d := range added {
				fmt.Printf("NEW DEBT %s: %s %s (%s), reason \"open\"\n", u.Name, d.Status, d.At, d.Change)
			}
			// Written after every unit: a recording of all of them takes hours.
			if err := writeBaseline(base); err != nil {
				fatal(err)
			}
			continue
		}
		want, ok := base.Units[u.Name]
		if !ok {
			fmt.Fprintf(os.Stderr, "NO BASELINE %s: run `task mutate:record -- %s`\n", u.Name, u.Name)
			failed = true
			continue
		}
		c := Compare(want, run)
		for _, s := range c.Notes {
			fmt.Printf("note     %s: %s\n", u.Name, s)
		}
		if c.Open > 0 {
			fmt.Printf("OPEN     %s: %d recorded mutants nobody has examined yet (reason \"open\" in %s; %s)\n", u.Name, c.Open, baselinePath, reportPath)
		}
		for _, s := range c.Failures {
			fmt.Fprintf(os.Stderr, "FAIL     %s: %s\n", u.Name, s)
			failed = true
		}
	}
	if mode == "record" {
		fmt.Println("recorded", baselinePath)
		return
	}
	if failed {
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mutate check|record [-shard i/n] [-changed ref] [-workers n] [-whole] [unit|./package ...]\n       mutate list | matrix [ref] | changed <ref> | report")
	os.Exit(2)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
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

func parseShard(s string) (int, int, error) {
	a, b, ok := strings.Cut(s, "/")
	i, err1 := strconv.Atoi(a)
	n, err2 := strconv.Atoi(b)
	if !ok || err1 != nil || err2 != nil || n < 1 || i < 1 || i > n {
		return 0, 0, fmt.Errorf("-shard %q: want i/n with 1 <= i <= n", s)
	}
	return i, n, nil
}

// Matrix is the workflow matrix: one entry per unit and shard.
func Matrix(units []Unit) string {
	type entry struct {
		Unit  string `json:"unit"`
		Shard string `json:"shard"`
	}
	out := []entry{}
	for _, u := range units {
		n := max(u.Shards, 1)
		for i := 1; i <= n; i++ {
			out = append(out, entry{u.Name, fmt.Sprintf("%d/%d", i, n)})
		}
	}
	raw, _ := json.Marshal(out)
	return string(raw)
}

// Changed is the units whose package has a Go file that differs between the
// merge base of ref and HEAD, and the working tree. A test file counts: a
// deleted assertion is how a mutant starts to live.
func Changed(root, ref string) ([]Unit, error) {
	git := func(args ...string) (string, error) {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			return "", fmt.Errorf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
		}
		return strings.TrimSpace(string(out)), nil
	}
	base, err := git("merge-base", ref, "HEAD")
	if err != nil {
		// No common history is not "nothing changed": refuse to guess.
		return nil, fmt.Errorf("no merge base with %s (a shallow clone? fetch more history): %v", ref, err)
	}
	names, err := git("diff", "--name-only", base, "--")
	if err != nil {
		return nil, err
	}
	untracked, err := git("ls-files", "--others", "--exclude-standard")
	if err != nil {
		return nil, err
	}
	dirs := map[string]bool{}
	for _, f := range strings.Split(names+"\n"+untracked, "\n") {
		f = filepath.ToSlash(strings.TrimSpace(f))
		if strings.HasSuffix(f, ".go") || strings.Contains(f, "/testdata/") {
			dirs[pathDir(f)] = true
		}
	}
	var out []Unit
	for _, u := range Units {
		for d := range dirs {
			if d == u.Dir || strings.HasPrefix(d, u.Dir+"/") {
				out = append(out, u)
				break
			}
		}
	}
	return out, nil
}

func pathDir(f string) string {
	if i := strings.LastIndex(f, "/"); i >= 0 {
		return f[:i]
	}
	return "."
}

// Record turns a run into the unit's baseline. An entry the old baseline has
// keeps its reason; one it does not have is "open" and is returned, so that a
// recording cannot quietly make the list longer.
func Record(old UnitBase, run *Run) (UnitBase, []Debt) {
	reasons := map[string]Debt{}
	for _, d := range old.Debt {
		reasons[d.Key] = d
	}
	nb := UnitBase{
		Platform: run.Platform, Recorded: time.Now().UTC().Format("2006-01-02"), Workers: run.Workers, Tests: run.Tests,
		SuiteSeconds: round1(run.SuiteSeconds), Seconds: round1(run.Seconds), Mutants: len(run.Mutants),
		Killed: run.Count(Killed), Lived: run.Count(Lived), NotCovered: run.Count(NotCovered),
		TimedOut: run.Count(TimedOut), NotViable: run.Count(NotViable), SetupFailed: run.Count(SetupFailed),
		Debt: []Debt{},
	}
	var added []Debt
	for _, m := range run.Mutants {
		if !isDebt(m.Status) {
			continue
		}
		d := Debt{At: m.At(), Key: m.Key(), Change: m.From + " became " + m.To, Source: m.Source, Status: m.Status, Reason: Open}
		if m.Status == Lived {
			d.Reached = reached(m.Tests)
		}
		if prev, ok := reasons[d.Key]; ok && prev.Status == d.Status {
			d.Reason = prev.Reason
		} else {
			added = append(added, d)
		}
		if d.Reason == Open {
			nb.Open++
		}
		nb.Debt = append(nb.Debt, d)
	}
	sort.SliceStable(nb.Debt, func(i, j int) bool { return nb.Debt[i].Key < nb.Debt[j].Key })
	return nb, added
}

func isDebt(s Status) bool {
	return s == Lived || s == NotCovered || s == TimedOut || s == SetupFailed
}

func reached(tests []string) string {
	if len(tests) == 0 {
		return ""
	}
	if len(tests) > 3 {
		return fmt.Sprintf("%s (and %d more)", strings.Join(tests[:3], ", "), len(tests)-3)
	}
	return strings.Join(tests, ", ")
}

func round1(f float64) float64 { return float64(int(f*10+0.5)) / 10 }

// Comparison is a run held against the baseline.
type Comparison struct {
	Failures []string
	Notes    []string
	Open     int
}

// Compare says what in run the baseline does not allow.
//
//   - A mutant that lived, or that no test reaches, must be listed (as either).
//   - A mutant that timed out, or whose run failed before a test started, must be
//     listed as exactly that with a written reason: "open" is not one.
//   - A listed mutant that is now killed fails too: the list only shrinks by
//     being recorded, so a kill that is lost again is seen.
//
// Only the mutants of the run are judged, so a shard is held to its own part of
// the list.
func Compare(want UnitBase, run *Run) Comparison {
	var c Comparison
	listed := map[string]Debt{}
	for _, d := range want.Debt {
		listed[d.Key] = d
	}
	inRun := map[string]bool{}
	for _, m := range run.Mutants {
		inRun[m.Key()] = true
		d, ok := listed[m.Key()]
		what := fmt.Sprintf("%s (%s became %s: %s)", m.At(), m.From, m.To, m.Source)
		switch m.Status {
		case Killed, NotViable:
			if ok {
				c.Failures = append(c.Failures, fmt.Sprintf("%s is listed as %s and is now %s: `task mutate:record` drops it, so that losing the kill again is noticed", what, d.Status, m.Status))
			}
		case Lived, NotCovered:
			switch {
			case !ok:
				hint := "no test executes the line"
				if m.Status == Lived {
					hint = "the tests pass with the code wrong"
					if len(m.Tests) > 0 {
						hint += "; reached by " + reached(m.Tests)
					}
				}
				c.Failures = append(c.Failures, fmt.Sprintf("NEW %s %s: %s", m.Status, what, hint))
			case d.Status != Lived && d.Status != NotCovered:
				c.Failures = append(c.Failures, fmt.Sprintf("NEW %s %s: the baseline lists it as %s, and that reason is not about this", m.Status, what, d.Status))
			default:
				if d.Status != m.Status {
					c.Notes = append(c.Notes, fmt.Sprintf("%s was %s and is now %s", m.At(), d.Status, m.Status))
				}
				if d.Reason == Open || strings.TrimSpace(d.Reason) == "" {
					c.Open++
				}
			}
		case TimedOut, SetupFailed:
			switch {
			case !ok || d.Status != m.Status:
				c.Failures = append(c.Failures, fmt.Sprintf("%s %s: %s. Neither is a kill; it passes only when listed as %s with a written reason", m.Status, what, firstLines(m.Detail, 2), m.Status))
			case d.Reason == Open || strings.TrimSpace(d.Reason) == "":
				c.Failures = append(c.Failures, fmt.Sprintf("%s %s is listed with no reason (\"open\" is not one for a mutant that was never judged)", m.Status, what))
			}
		default:
			c.Failures = append(c.Failures, fmt.Sprintf("%s has no result (%q): the run is broken", what, m.Status))
		}
	}
	if run.Shard == "" {
		gone := 0
		for k := range listed {
			if !inRun[k] {
				gone++
			}
		}
		if gone > 0 {
			c.Notes = append(c.Notes, fmt.Sprintf("%d listed mutants are no longer in the code (the declaration changed or went): `task mutate:record` drops them", gone))
		}
	}
	sort.Strings(c.Failures)
	return c
}

func readBaseline() (Baseline, error) {
	base := Baseline{Tool: toolName, Units: map[string]UnitBase{}}
	raw, err := os.ReadFile(baselinePath)
	if os.IsNotExist(err) {
		return base, nil
	}
	if err != nil {
		return base, err
	}
	if err := json.Unmarshal(raw, &base); err != nil {
		return base, fmt.Errorf("%s: %v", baselinePath, err)
	}
	if base.Units == nil {
		base.Units = map[string]UnitBase{}
	}
	base.Tool = toolName
	return base, nil
}

// writeBaseline keeps one mutant per line, so a diff of the file reads as the
// mutants that came and went.
func writeBaseline(b Baseline) error {
	var w bytes.Buffer
	names := make([]string, 0, len(b.Units))
	for n := range b.Units {
		names = append(names, n)
	}
	sort.Strings(names)
	tool, _ := json.Marshal(b.Tool)
	fmt.Fprintf(&w, "{\n  \"tool\": %s,\n  \"units\": {\n", tool)
	for i, n := range names {
		u := b.Units[n]
		debt := u.Debt
		u.Debt = nil
		head, err := json.Marshal(u)
		if err != nil {
			return err
		}
		// {"platform":...,"debt":null} -> fields, then the list a line each.
		fields := strings.TrimSuffix(string(head), `,"debt":null}`)
		key, _ := json.Marshal(n)
		fmt.Fprintf(&w, "    %s: %s,\n      \"debt\": [", key, fields)
		for j, d := range debt {
			line, err := json.Marshal(d)
			if err != nil {
				return err
			}
			sep := ","
			if j == len(debt)-1 {
				sep = ""
			}
			fmt.Fprintf(&w, "\n        %s%s", line, sep)
		}
		if len(debt) > 0 {
			w.WriteString("\n      ")
		}
		w.WriteString("]}")
		if i < len(names)-1 {
			w.WriteString(",")
		}
		w.WriteString("\n")
	}
	w.WriteString("  }\n}\n")
	var check Baseline
	if err := json.Unmarshal(w.Bytes(), &check); err != nil {
		return fmt.Errorf("the baseline would not parse: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(baselinePath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(baselinePath, w.Bytes(), 0o644)
}

// saveRun keeps the whole run (every mutant, what killed it) under .mutate/ for
// a person to read; it is not committed.
func saveRun(u Unit, run *Run) {
	name := strings.NewReplacer("/", "_", ":", "_").Replace(u.Name)
	if run.Shard != "" {
		name += "." + strings.ReplaceAll(run.Shard, "/", "of")
	}
	raw, err := json.MarshalIndent(run, "", " ")
	if err != nil {
		return
	}
	if os.MkdirAll(cacheDir, 0o755) == nil {
		os.WriteFile(filepath.Join(cacheDir, name+".json"), raw, 0o644)
	}
}

func commit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	c := strings.TrimSpace(string(out))
	if st, err := exec.Command("git", "status", "--porcelain", "--", "*.go").Output(); err == nil && len(bytes.TrimSpace(st)) > 0 {
		c += "+changes"
	}
	return c
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}
