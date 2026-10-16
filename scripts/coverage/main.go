// Command coverage holds statement coverage to the committed floors in
// testdata/coverage.txt.
//
//	go run ./scripts/coverage                       check (what CI runs)
//	go run ./scripts/coverage record                raise the floors of this platform to today's numbers
//	go run ./scripts/coverage record -lower "why"   ... and lower those that fell, each with a "# lowered:" line
//	go run ./scripts/coverage history               only the history rule, no tests run
//
// A floor line is `<package or file> <percent> [<goos>]`:
//
//	internal/broker 89.9 darwin               what `record` writes: this package, on this platform
//	internal/protocol/summary.go 92           a file, on every platform: a target a stage set by hand
//
// The gate fails when
//
//   - a package or file is more than 0.1 below a floor that applies here;
//   - a package has tests and no floor for this platform, or has no tests and
//     no "# no-tests: <package>: <reason>" line;
//   - the file's git history has a floor that went down, or went away, with no
//     "# lowered: <date> <key>[@<goos>] <from> -> <to|none>: <reason>" line in the
//     file as it is now. Editing a floor down by hand therefore fails until the
//     reason is written where the next reader finds it. A shallow clone fails:
//     it cannot show the history.
package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/doze-dev/doze-kafka/scripts/internal/githist"
)

const (
	floorFile = "testdata/coverage.txt"
	tolerance = 0.1
)

// Entry is one floor.
type Entry struct {
	Key   string // package directory or file, slash-separated, relative to the module root.
	Floor float64
	GOOS  string // "" applies everywhere.
}

func (e Entry) id() string {
	if e.GOOS == "" {
		return e.Key
	}
	return e.Key + "@" + e.GOOS
}

// Lowered is a "# lowered:" line.
type Lowered struct {
	ID     string // key or key@goos.
	From   float64
	To     float64 // NaN: the floor was removed.
	Reason string
}

// Floors is testdata/coverage.txt.
type Floors struct {
	Head    []string // the comment block before the first entry.
	Entries []Entry
	Tail    []string // every other comment line, in order ("# lowered:", "# no-tests:").
	Lowered []Lowered
	NoTests map[string]string
}

// Parse reads a floor file. A line that is neither a comment nor an entry is an
// error: a typo must not silently drop a floor.
func Parse(raw []byte) (*Floors, error) {
	f := &Floors{NoTests: map[string]string{}}
	seen := map[string]bool{}
	sc := bufio.NewScanner(bytes.NewReader(raw))
	n := 0
	for sc.Scan() {
		n++
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "":
			continue
		case strings.HasPrefix(line, "#"):
			body := strings.TrimSpace(strings.TrimPrefix(line, "#"))
			switch {
			case strings.HasPrefix(body, "lowered:"):
				l, err := parseLowered(strings.TrimPrefix(body, "lowered:"))
				if err != nil {
					return nil, fmt.Errorf("line %d: %v", n, err)
				}
				f.Lowered = append(f.Lowered, l)
			case strings.HasPrefix(body, "no-tests:"):
				pkg, reason, ok := strings.Cut(strings.TrimSpace(strings.TrimPrefix(body, "no-tests:")), ":")
				if !ok || len(strings.TrimSpace(reason)) < 15 {
					return nil, fmt.Errorf("line %d: want \"# no-tests: <package>: <reason of at least 15 characters>\"", n)
				}
				f.NoTests[strings.TrimSpace(pkg)] = strings.TrimSpace(reason)
			}
			if len(f.Entries) == 0 && !strings.HasPrefix(body, "lowered:") && !strings.HasPrefix(body, "no-tests:") {
				f.Head = append(f.Head, line)
			} else {
				f.Tail = append(f.Tail, line)
			}
		default:
			parts := strings.Fields(line)
			if len(parts) < 2 || len(parts) > 3 {
				return nil, fmt.Errorf("line %d: want \"<package or file> <percent> [<goos>]\", got %q", n, line)
			}
			v, err := strconv.ParseFloat(parts[1], 64)
			if err != nil || v < 0 || v > 100 {
				return nil, fmt.Errorf("line %d: %q is not a percentage", n, parts[1])
			}
			e := Entry{Key: parts[0], Floor: v}
			if len(parts) == 3 {
				e.GOOS = parts[2]
			}
			if seen[e.id()] {
				return nil, fmt.Errorf("line %d: a second floor for %s", n, e.id())
			}
			seen[e.id()] = true
			f.Entries = append(f.Entries, e)
		}
	}
	return f, sc.Err()
}

// parseLowered reads "<date> <id> <from> -> <to|none>: <reason>".
func parseLowered(s string) (Lowered, error) {
	const want = "want \"# lowered: <date> <key>[@<goos>] <from> -> <to|none>: <reason of at least 15 characters>\""
	head, reason, ok := strings.Cut(s, ": ")
	parts := strings.Fields(head)
	if !ok || len(parts) != 5 || parts[3] != "->" || len(strings.TrimSpace(reason)) < 15 {
		return Lowered{}, errors.New(want)
	}
	if _, err := time.Parse("2006-01-02", parts[0]); err != nil {
		return Lowered{}, errors.New(want)
	}
	from, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return Lowered{}, errors.New(want)
	}
	to := math.NaN()
	if parts[4] != "none" {
		if to, err = strconv.ParseFloat(parts[4], 64); err != nil {
			return Lowered{}, errors.New(want)
		}
	}
	return Lowered{ID: parts[1], From: from, To: to, Reason: strings.TrimSpace(reason)}, nil
}

// Format writes the file: the head comments, the floors sorted, the rest.
func (f *Floors) Format() []byte {
	var w bytes.Buffer
	for _, l := range f.Head {
		w.WriteString(l + "\n")
	}
	es := append([]Entry(nil), f.Entries...)
	sort.SliceStable(es, func(i, j int) bool {
		if es[i].Key != es[j].Key {
			return es[i].Key < es[j].Key
		}
		return es[i].GOOS < es[j].GOOS
	})
	for _, e := range es {
		fmt.Fprintf(&w, "%s %s", e.Key, pct(e.Floor))
		if e.GOOS != "" {
			w.WriteString(" " + e.GOOS)
		}
		w.WriteString("\n")
	}
	for _, l := range f.Tail {
		w.WriteString(l + "\n")
	}
	return w.Bytes()
}

func pct(v float64) string { return strconv.FormatFloat(math.Round(v*10)/10, 'f', -1, 64) }

func same(a, b float64) bool {
	if math.IsNaN(a) || math.IsNaN(b) {
		return math.IsNaN(a) && math.IsNaN(b)
	}
	return math.Abs(a-b) < 0.0001
}

// History holds the file's git history against its "# lowered:" lines: every
// floor that went down or away must have one in the file as it is now.
func History(now *Floors, changes []githist.Change) []string {
	explained := func(id string, from, to float64) bool {
		for _, l := range now.Lowered {
			if l.ID == id && same(l.From, from) && same(l.To, to) {
				return true
			}
		}
		return false
	}
	var out []string
	said := map[string]bool{}
	for _, c := range changes {
		if c.Old == nil {
			continue
		}
		old, err := Parse(c.Old)
		if err != nil {
			continue // a version that held no readable floor has none to lower.
		}
		// A version that cannot be read holds no floor at all: every floor before
		// it reads as removed, so garbage cannot be a step on the way down.
		next := &Floors{}
		if c.New != nil {
			if parsed, err := Parse(c.New); err == nil {
				next = parsed
			} else if c.Commit == "" {
				out = append(out, fmt.Sprintf("%s: %v", floorFile, err))
				continue
			}
		}
		// A floor with no platform holds on every platform, so "the floor of a key
		// on a platform" is its own line if it has one, else the line with none.
		// Compared on every platform either version names (or as the one
		// unnamed platform when neither names any).
		platforms := map[string]bool{}
		for _, fl := range []*Floors{old, next} {
			for _, e := range fl.Entries {
				if e.GOOS != "" {
					platforms[e.GOOS] = true
				}
			}
		}
		if len(platforms) == 0 {
			platforms[""] = true
		}
		effective := func(fl *Floors, key, goos string) (float64, bool) {
			v, found := 0.0, false
			for _, e := range fl.Entries {
				if e.Key == key && (e.GOOS == "" || e.GOOS == goos) && (!found || e.Floor > v) {
					v, found = e.Floor, true
				}
			}
			return v, found
		}
		keys := map[string]bool{}
		for _, e := range old.Entries {
			keys[e.Key] = true
		}
		for key := range keys {
			for goos := range platforms {
				from, had := effective(old, key, goos)
				if !had {
					continue
				}
				id := Entry{Key: key, GOOS: goos}.id()
				to, ok := effective(next, key, goos)
				if !ok {
					to = math.NaN()
				} else if to >= from-0.0001 {
					continue
				}
				if explained(id, from, to) {
					continue
				}
				toText := "none"
				what := "removed the floor of " + id + " (" + pct(from) + ")"
				if ok {
					toText = pct(to)
					what = "lowered the floor of " + id + " from " + pct(from) + " to " + toText
				}
				date := c.Date
				if date == "" {
					date = time.Now().Format("2006-01-02")
				}
				msg := fmt.Sprintf("%s %s and %s has no line saying why. Add:\n    # lowered: %s %s %s -> %s: <the reason>", c.Where(), what, floorFile, date, id, pct(from), toText)
				if !said[msg] {
					said[msg] = true
					out = append(out, msg)
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// Measured is coverage as the profile has it: per package directory and per file.
type Measured struct {
	Pct      map[string]float64 // key -> percent, to a tenth.
	Untested []string           // packages with Go files and no test files.
}

// ReadProfile computes statement coverage per file and per package from a
// coverage profile; module is the module path its file names begin with.
func ReadProfile(raw []byte, module string) (map[string]float64, error) {
	type count struct{ stmts, hit int }
	blocks := map[string]map[string]count{} // file -> block -> count
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		colon := strings.LastIndex(line, ":")
		f := strings.Fields(line[colon+1:])
		if colon < 0 || len(f) != 3 {
			return nil, fmt.Errorf("coverage profile line %q", line)
		}
		stmts, err1 := strconv.Atoi(f[1])
		hits, err2 := strconv.Atoi(f[2])
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("coverage profile line %q", line)
		}
		file := strings.TrimPrefix(strings.TrimPrefix(line[:colon], module), "/")
		if blocks[file] == nil {
			blocks[file] = map[string]count{}
		}
		c := blocks[file][f[0]]
		c.stmts = stmts
		if hits > 0 {
			c.hit = stmts
		}
		blocks[file][f[0]] = c
	}
	total := map[string]count{}
	for file, bs := range blocks {
		dir := "."
		if i := strings.LastIndex(file, "/"); i >= 0 {
			dir = file[:i]
		}
		for _, c := range bs {
			for _, k := range []string{file, dir} {
				t := total[k]
				t.stmts += c.stmts
				t.hit += c.hit
				total[k] = t
			}
		}
	}
	out := map[string]float64{}
	for k, t := range total {
		if t.stmts > 0 {
			out[k] = math.Round(1000*float64(t.hit)/float64(t.stmts)) / 10
		}
	}
	return out, sc.Err()
}

// Measure runs the suite in root with a profile.
func Measure(root string) (*Measured, error) {
	run := func(args ...string) ([]byte, error) {
		cmd := exec.Command("go", args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOWORK=off")
		return cmd.CombinedOutput()
	}
	mod, err := run("list", "-m")
	if err != nil {
		return nil, fmt.Errorf("go list -m: %v\n%s", err, mod)
	}
	module := strings.TrimSpace(string(mod))
	list, err := run("list", "-f", "{{.ImportPath}} {{len .GoFiles}} {{len .TestGoFiles}} {{len .XTestGoFiles}}", "./...")
	if err != nil {
		return nil, fmt.Errorf("go list: %v\n%s", err, list)
	}
	m := &Measured{}
	tested := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(list)), "\n") {
		f := strings.Fields(line)
		if len(f) != 4 {
			continue
		}
		dir := strings.TrimPrefix(strings.TrimPrefix(f[0], module), "/")
		if dir == "" {
			dir = "."
		}
		if f[2] != "0" || f[3] != "0" {
			tested[dir] = true
		} else if f[1] != "0" {
			m.Untested = append(m.Untested, dir)
		}
	}
	prof, err := os.CreateTemp("", "cover-*.out")
	if err != nil {
		return nil, err
	}
	prof.Close()
	defer os.Remove(prof.Name())
	// -short is not used: the floor is for the full suite.
	if out, err := run("test", "-count=1", "-coverprofile="+prof.Name(), "./..."); err != nil {
		var fails []string
		for _, l := range strings.Split(string(out), "\n") {
			if strings.HasPrefix(l, "FAIL") || strings.HasPrefix(l, "--- FAIL") || strings.HasPrefix(l, "panic:") {
				fails = append(fails, "  "+l)
			}
		}
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		if len(lines) > 60 {
			lines = lines[len(lines)-60:]
		}
		return nil, fmt.Errorf("go test failed (%v). Failing packages and tests:\n%s\n---- tail of the test output ----\n%s", err, strings.Join(fails, "\n"), strings.Join(lines, "\n"))
	}
	raw, err := os.ReadFile(prof.Name())
	if err != nil {
		return nil, err
	}
	all, err := ReadProfile(raw, module)
	if err != nil {
		return nil, err
	}
	// A package without tests is in the profile at 0%: it is Untested, not a number.
	m.Pct = map[string]float64{}
	for k, v := range all {
		dir := k
		if strings.HasSuffix(k, ".go") {
			dir = filepath.ToSlash(filepath.Dir(k))
		}
		if tested[dir] {
			m.Pct[k] = v
		}
	}
	sort.Strings(m.Untested)
	return m, nil
}

// Check holds what was measured against the floors that apply on goos.
func Check(f *Floors, m *Measured, goos string) (failures, notes []string) {
	floored := map[string]bool{}
	for _, e := range f.Entries {
		if e.GOOS != "" && e.GOOS != goos {
			continue
		}
		floored[e.Key] = true
		got, ok := m.Pct[e.Key]
		switch {
		case !ok:
			failures = append(failures, fmt.Sprintf("MISSING  %s (floor %s%%): no coverage reported; the package or file is gone, renamed, or has no tests now", e.id(), pct(e.Floor)))
		case got < e.Floor-tolerance-0.0001:
			failures = append(failures, fmt.Sprintf("FELL     %s: %s%% < floor %s%%", e.id(), pct(got), pct(e.Floor)))
		default:
			notes = append(notes, fmt.Sprintf("ok       %s: %s%% (floor %s%%)", e.id(), pct(got), pct(e.Floor)))
		}
	}
	var keys []string
	for k := range m.Pct {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !strings.HasSuffix(k, ".go") && !floored[k] {
			failures = append(failures, fmt.Sprintf("NO FLOOR %s: %s%% on %s and nothing holds it there (run: task coverage:record)", k, pct(m.Pct[k]), goos))
		}
	}
	for _, p := range m.Untested {
		if _, ok := f.NoTests[p]; !ok {
			failures = append(failures, fmt.Sprintf("NO TESTS %s: write one, or say why there is none: \"# no-tests: %s: <reason>\" in %s", p, p, floorFile))
		}
	}
	for p := range f.NoTests {
		if _, ok := m.Pct[p]; ok {
			failures = append(failures, fmt.Sprintf("STALE    %s has tests now: drop its \"# no-tests:\" line and record a floor", p))
		}
	}
	sort.Strings(failures)
	return failures, notes
}

// Record moves this platform's package floors to what was measured. A floor
// only goes down with a reason, and then the reason is written into the file.
func Record(f *Floors, m *Measured, goos, lowerReason, today string) (refused []string) {
	byID := map[string]int{}
	for i, e := range f.Entries {
		byID[e.id()] = i
	}
	var keys []string
	for k := range m.Pct {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		got := m.Pct[k]
		e := Entry{Key: k, Floor: got, GOOS: goos}
		i, ok := byID[e.id()]
		if g, generic := byID[k]; !ok && generic && !strings.HasSuffix(k, ".go") {
			// A package floor with no platform is from before floors were per
			// platform: it becomes this platform's, and is then held as such.
			f.Entries[g].GOOS = goos
			delete(byID, k)
			byID[e.id()] = g
			i, ok = g, true
		}
		if !ok {
			// A file gets a floor only by hand; a package always has one.
			if !strings.HasSuffix(k, ".go") {
				f.Entries = append(f.Entries, e)
			}
			continue
		}
		old := f.Entries[i].Floor
		switch {
		case got >= old:
			f.Entries[i].Floor = got
		case got >= old-tolerance-0.0001:
			// Inside the tolerance: the floor stays where it is.
		case lowerReason == "":
			refused = append(refused, fmt.Sprintf("%s: %s%% is below its floor %s%%", e.id(), pct(got), pct(old)))
		default:
			f.Entries[i].Floor = got
			line := fmt.Sprintf("# lowered: %s %s %s -> %s: %s", today, e.id(), pct(old), pct(got), lowerReason)
			f.Tail = append(f.Tail, line)
			f.Lowered = append(f.Lowered, Lowered{ID: e.id(), From: old, To: got, Reason: lowerReason})
		}
	}
	return refused
}

func main() {
	mode := "check"
	args := os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		mode, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	lower := fs.String("lower", "", "record: the reason floors that fell are lowered (written into the file)")
	fs.Parse(args)
	if mode != "check" && mode != "record" && mode != "history" {
		fmt.Fprintln(os.Stderr, "usage: coverage [check | record [-lower \"reason\"] | history]")
		os.Exit(2)
	}
	os.Exit(run(".", mode, *lower, runtime.GOOS, os.Stdout, os.Stderr))
}

func run(root, mode, lower, goos string, stdout, stderr io.Writer) int {
	path := filepath.Join(root, filepath.FromSlash(floorFile))
	raw, err := os.ReadFile(path)
	if err != nil && !(os.IsNotExist(err) && mode == "record") {
		fmt.Fprintln(stderr, err)
		return 1
	}
	floors, err := Parse(raw)
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", floorFile, err)
		return 1
	}
	history := func(f *Floors) int {
		changes, err := githist.Changes(root, floorFile)
		if err != nil {
			fmt.Fprintln(stderr, "HISTORY ", err)
			return 1
		}
		bad := History(f, changes)
		for _, s := range bad {
			fmt.Fprintln(stderr, "LOWERED ", s)
		}
		if len(bad) > 0 {
			return 1
		}
		fmt.Fprintf(stdout, "ok       every floor that ever went down in %s has its reason there (%d changes read)\n", floorFile, len(changes))
		return 0
	}
	if mode == "history" {
		return history(floors)
	}
	// The cheap rule first: a hand-lowered floor fails before the suite is run.
	if mode == "check" {
		if rc := history(floors); rc != 0 {
			return rc
		}
	}
	m, err := Measure(root)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if mode == "record" {
		refused := Record(floors, m, goos, strings.TrimSpace(lower), time.Now().Format("2006-01-02"))
		if len(refused) > 0 {
			fmt.Fprintln(stderr, "refusing to lower a floor without a reason (record -lower \"<why>\" writes it into the file):")
			for _, s := range refused {
				fmt.Fprintln(stderr, "  "+s)
			}
			return 1
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err := os.WriteFile(path, floors.Format(), 0o644); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprintf(stdout, "recorded %s floors in %s\n", goos, floorFile)
		return history(floors)
	}
	failures, notes := Check(floors, m, goos)
	for _, s := range notes {
		fmt.Fprintln(stdout, s)
	}
	for _, s := range failures {
		fmt.Fprintln(stderr, s)
	}
	if len(failures) > 0 {
		return 1
	}
	return 0
}
