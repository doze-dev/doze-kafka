package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Engine mutates one package and runs its tests against every mutant.
//
// How a mutant is judged, and why each step is there:
//
//  1. The module is copied once per worker, so a mutant is a changed file in a
//     copy: workers cannot see each other's mutants, a test that builds the
//     binary builds the mutant, and nothing a mutated test deletes is ours.
//  2. The unmutated package is built and run in every copy at once with the very
//     function (and so the very arguments) a mutant is run with. If that does not
//     pass, the run stops: a test command that cannot pass would call every
//     mutant killed.
//  3. A coverage run says which lines the tests execute at all, and (unless
//     Whole) one run per test says which tests execute which lines, so a mutant
//     is run against the tests that reach it and not the whole suite. A test
//     that never executes the line cannot notice it changed.
//  4. Per mutant: build (a failure is NOT VIABLE, never a kill), run, read the
//     output. KILLED needs a test to have started and the run to have failed;
//     a run that fails before any test starts is SETUP FAILED; one that runs out
//     of time is run once more alone with twice the time before it is called
//     TIMED OUT.
type Engine struct {
	Root    string   // module root.
	Dir     string   // package directory, slash-separated, relative to Root.
	Only    []string // file names (without .go) to mutate; empty means all.
	Workers int
	// Coef is how many times the tests' own time a mutant's run may take.
	Coef float64
	// Whole runs the whole suite for every mutant instead of the tests that
	// reach it.
	Whole bool
	// Shard and Shards take every Shards-th mutant, starting at Shard (from 1).
	Shard, Shards int
	// MinTimeout is the least time a mutant's run gets; zero means 30 seconds.
	MinTimeout time.Duration
	Log        io.Writer
}

// Run is what one Engine run found.
type Run struct {
	Platform     string   `json:"platform"`
	Workers      int      `json:"workers"`
	Whole        bool     `json:"whole,omitempty"`
	Shard        string   `json:"shard,omitempty"`
	Tests        int      `json:"tests"`
	SuiteSeconds float64  `json:"suiteSeconds"` // the unmutated suite, the slowest of the concurrent runs.
	Seconds      float64  `json:"seconds"`      // the whole run, wall clock.
	Mutants      []Mutant `json:"mutants"`
}

// Count is how many mutants have a status.
func (r *Run) Count(s Status) int {
	n := 0
	for _, m := range r.Mutants {
		if m.Status == s {
			n++
		}
	}
	return n
}

type outcome struct {
	status Status
	detail string
	tail   string // the last lines the run printed.
	dur    time.Duration
}

func (e *Engine) logf(format string, a ...any) {
	if e.Log != nil {
		fmt.Fprintf(e.Log, format+"\n", a...)
	}
}

// Run does the whole thing.
func (e *Engine) Run(ctx context.Context) (*Run, error) {
	start := time.Now()
	if e.Workers < 1 {
		e.Workers = 1
	}
	if e.Coef <= 0 {
		e.Coef = 4
	}
	if e.MinTimeout <= 0 {
		e.MinTimeout = 30 * time.Second
	}
	if e.Shards < 1 {
		e.Shard, e.Shards = 1, 1
	}
	all, err := Discover(e.Root, e.Dir, e.Only)
	if err != nil {
		return nil, err
	}
	var mutants []Mutant
	for i, m := range all {
		if i%e.Shards == e.Shard-1 {
			mutants = append(mutants, m)
		}
	}
	run := &Run{Platform: runtime.GOOS + "/" + runtime.GOARCH, Workers: e.Workers, Whole: e.Whole}
	if e.Shards > 1 {
		run.Shard = fmt.Sprintf("%d/%d", e.Shard, e.Shards)
	}

	tmp, err := os.MkdirTemp("", "mutate-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	files, err := sourceFiles(e.Root)
	if err != nil {
		return nil, err
	}
	trees := make([]string, e.Workers)
	for i := range trees {
		trees[i] = filepath.Join(tmp, "w"+strconv.Itoa(i))
		if err := copyTree(e.Root, trees[i], files); err != nil {
			return nil, err
		}
	}

	// Step 2: the unmutated package, in every copy at once, as a mutant is run.
	e.logf("%s: %d mutants in this run (%d in the package); building and running the unmutated tests in %d copies", e.Dir, len(mutants), len(all), e.Workers)
	type base struct {
		out outcome
		err error
	}
	bases := make([]base, e.Workers)
	var wg sync.WaitGroup
	for i := range trees {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if msg, err := e.build(ctx, trees[i], false); err != nil {
				bases[i].err = fmt.Errorf("the unmutated package does not build: %v\n%s", err, msg)
				return
			}
			// No mutant's time is known yet: the unmutated run gets ten minutes.
			bases[i].out = e.test(ctx, trees[i], testBin, nil, 10*time.Minute, "")
		}(i)
	}
	wg.Wait()
	var suite time.Duration
	for i, b := range bases {
		if b.err != nil {
			return nil, b.err
		}
		if b.out.status != Lived {
			return nil, fmt.Errorf("%s: the UNMUTATED tests do not pass under the command every mutant is run with (copy %d of %d at once: %s %s). Nothing was mutated: with this command every mutant would read as killed.\n%s",
				e.Dir, i+1, e.Workers, b.out.status, b.out.detail, b.out.tail)
		}
		suite = max(suite, b.out.dur)
	}
	run.SuiteSeconds = suite.Seconds()
	wholeTimeout := max(time.Duration(e.Coef*float64(suite)), e.MinTimeout)

	// Step 3: coverage.
	if msg, err := e.build(ctx, trees[0], true); err != nil {
		return nil, fmt.Errorf("the coverage build failed: %v\n%s", err, msg)
	}
	names, err := e.listTests(ctx, trees[0])
	if err != nil {
		return nil, err
	}
	run.Tests = len(names)
	fullProf := filepath.Join(tmp, "full.cov")
	if o := e.test(ctx, trees[0], coverBin, nil, 10*time.Minute, fullProf); o.status != Lived {
		return nil, fmt.Errorf("%s: the unmutated tests do not pass with coverage on (%s %s)\n%s", e.Dir, o.status, o.detail, o.tail)
	}
	cov, err := readProfile(fullProf)
	if err != nil {
		return nil, err
	}
	var perTest []testCover
	if !e.Whole {
		e.logf("%s: suite %.1fs; one coverage run for each of %d tests", e.Dir, suite.Seconds(), len(names))
		perTest, err = e.coverEachTest(ctx, trees, tmp, names)
		if err != nil {
			return nil, err
		}
		for _, tc := range perTest {
			cov.union(tc.prof)
		}
	}

	// Which mutants are reached, and by which tests.
	type job struct {
		idx     int
		tests   []string // nil: the whole suite.
		timeout time.Duration
	}
	var jobs []job
	for i := range mutants {
		m := &mutants[i]
		base := filepath.Base(m.File)
		instrumented, covered := cov.at(base, m.Line, m.Col)
		if instrumented && !covered {
			m.Status = NotCovered
			continue
		}
		j := job{idx: i, timeout: wholeTimeout}
		if !e.Whole && instrumented {
			var sum time.Duration
			for _, tc := range perTest {
				if _, c := tc.prof.at(base, m.Line, m.Col); c {
					j.tests = append(j.tests, tc.name)
					sum += tc.dur
				}
			}
			// Reached by the suite and by no test on its own (a line only a
			// background goroutine or TestMain runs): the whole suite decides.
			if len(j.tests) > 0 {
				j.timeout = min(wholeTimeout, max(time.Duration(e.Coef*float64(sum)), e.MinTimeout))
				m.Tests = j.tests
			}
		}
		jobs = append(jobs, j)
	}
	e.logf("%s: %d mutants to run, %d not covered", e.Dir, len(jobs), len(mutants)-len(jobs))

	// Step 4.
	ch := make(chan job)
	var done int
	var mu sync.Mutex
	for w := range trees {
		wg.Add(1)
		go func(tree string) {
			defer wg.Done()
			for j := range ch {
				m := &mutants[j.idx]
				o := e.runMutant(ctx, tree, *m, j.tests, j.timeout)
				m.Status, m.Detail = o.status, o.detail
				mu.Lock()
				done++
				if done%25 == 0 || done == len(jobs) {
					e.logf("%s: %d/%d mutants, %.0fs", e.Dir, done, len(jobs), time.Since(start).Seconds())
				}
				mu.Unlock()
			}
		}(trees[w])
	}
	for _, j := range jobs {
		if ctx.Err() != nil {
			break
		}
		ch <- j
	}
	close(ch)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A timeout under load is not a verdict: once more, alone, with twice the time.
	for _, j := range jobs {
		m := &mutants[j.idx]
		if m.Status != TimedOut {
			continue
		}
		o := e.runMutant(ctx, trees[0], *m, j.tests, 2*j.timeout)
		m.Status, m.Detail = o.status, o.detail
		if o.status == TimedOut {
			m.Detail = fmt.Sprintf("no result in %s with the others running, nor in %s alone", j.timeout.Round(time.Second), (2 * j.timeout).Round(time.Second))
		}
	}
	run.Mutants = mutants
	run.Seconds = time.Since(start).Seconds()
	return run, nil
}

const (
	testBin  = ".mutate.test"
	coverBin = ".mutate.cover.test"
)

// build compiles the package's test binary in tree. Its combined output comes
// back for the error message.
func (e *Engine) build(ctx context.Context, tree string, cover bool) (string, error) {
	args := []string{"test", "-c", "-o", filepath.Join(tree, testBin)}
	if cover {
		args = []string{"test", "-c", "-cover", "-covermode=set", "-o", filepath.Join(tree, coverBin)}
	}
	args = append(args, "./"+e.Dir)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = tree
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (e *Engine) listTests(ctx context.Context, tree string) ([]string, error) {
	cmd := exec.CommandContext(ctx, filepath.Join(tree, coverBin), "-test.list", ".")
	cmd.Dir = filepath.Join(tree, filepath.FromSlash(e.Dir))
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("listing the tests of %s: %v", e.Dir, err)
	}
	var names []string
	for _, l := range strings.Split(string(out), "\n") {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "Test") || strings.HasPrefix(l, "Fuzz") || strings.HasPrefix(l, "Example") {
			names = append(names, l)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("%s has no tests: every mutant would live", e.Dir)
	}
	return names, nil
}

var (
	failLine  = regexp.MustCompile(`^\s*--- FAIL: (\S+)`)
	runLine   = regexp.MustCompile(`^=== RUN\s+(\S+)`)
	timedLine = "panic: test timed out after"
)

// test runs a test binary of tree in the package's directory. This one function
// runs the unmutated package and every mutant, so the two cannot differ.
func (e *Engine) test(ctx context.Context, tree, bin string, tests []string, timeout time.Duration, coverProfile string) outcome {
	args := []string{"-test.v", "-test.failfast", "-test.count=1", "-test.timeout=" + timeout.String()}
	if len(tests) > 0 {
		quoted := make([]string, len(tests))
		for i, t := range tests {
			quoted[i] = regexp.QuoteMeta(t)
		}
		args = append(args, "-test.run=^("+strings.Join(quoted, "|")+")$")
	}
	if coverProfile != "" {
		args = append(args, "-test.coverprofile="+coverProfile)
	}
	// The test's own timeout prints the stuck goroutines; ours, a little later,
	// is for a process that does not even do that.
	cctx, cancel := context.WithTimeout(ctx, timeout+20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, filepath.Join(tree, bin), args...)
	cmd.Dir = filepath.Join(tree, filepath.FromSlash(e.Dir))
	cmd.Env = append(os.Environ(), "GOWORK=off")
	cmd.WaitDelay = 5 * time.Second
	ownGroup(cmd)
	pr, pw := io.Pipe()
	cmd.Stdout, cmd.Stderr = pw, pw

	var firstFail, lastRun string
	var sawTimeout bool
	tail := newTail(40)
	scanned := make(chan struct{})
	go func() {
		defer close(scanned)
		r := bufio.NewReaderSize(pr, 1<<16)
		for {
			line, err := r.ReadString('\n')
			if line != "" {
				line = strings.TrimRight(line, "\n")
				if m := runLine.FindStringSubmatch(line); m != nil {
					lastRun = m[1]
				} else if m := failLine.FindStringSubmatch(line); m != nil && firstFail == "" {
					firstFail = m[1]
				} else if strings.HasPrefix(line, timedLine) {
					sawTimeout = true
				}
				tail.add(line)
			}
			if err != nil {
				io.Copy(io.Discard, pr)
				return
			}
		}
	}()
	began := time.Now()
	err := cmd.Run()
	pw.Close()
	<-scanned
	o := outcome{dur: time.Since(began), tail: tail.String()}
	switch {
	case err == nil:
		o.status = Lived
	case errors.Is(cctx.Err(), context.DeadlineExceeded) || sawTimeout:
		o.status, o.detail = TimedOut, "no result in "+timeout.String()
	case firstFail != "":
		o.status, o.detail = Killed, firstFail
	case lastRun != "":
		// No test reported a failure and the process still died after one began:
		// a panic on another goroutine, a fatal error, an os.Exit.
		o.status, o.detail = Killed, "the test process died during "+lastRun
	default:
		o.status, o.detail = SetupFailed, fmt.Sprintf("%v before any test started: %s", err, firstLines(o.tail, 3))
	}
	return o
}

// runMutant writes the mutant into tree, builds, tests and puts the file back.
func (e *Engine) runMutant(ctx context.Context, tree string, m Mutant, tests []string, timeout time.Duration) outcome {
	path := filepath.Join(tree, filepath.FromSlash(m.File))
	orig, err := os.ReadFile(path)
	if err != nil {
		return outcome{status: SetupFailed, detail: err.Error()}
	}
	mutated, err := m.Apply(orig)
	if err != nil {
		return outcome{status: SetupFailed, detail: err.Error()}
	}
	if err := os.WriteFile(path, mutated, 0o644); err != nil {
		return outcome{status: SetupFailed, detail: err.Error()}
	}
	defer os.WriteFile(path, orig, 0o644)
	if msg, err := e.build(ctx, tree, false); err != nil {
		if ctx.Err() != nil {
			return outcome{status: SetupFailed, detail: ctx.Err().Error()}
		}
		return outcome{status: NotViable, detail: firstLines(msg, 3)}
	}
	return e.test(ctx, tree, testBin, tests, timeout, "")
}

type testCover struct {
	name string
	dur  time.Duration
	prof *profile
}

// coverEachTest runs every test on its own with coverage on.
func (e *Engine) coverEachTest(ctx context.Context, trees []string, tmp string, names []string) ([]testCover, error) {
	out := make([]testCover, len(names))
	errs := make([]error, len(names))
	ch := make(chan int)
	var wg sync.WaitGroup
	for w := range trees {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// The coverage binary is the first copy's; the test runs in this worker's.
			if w != 0 {
				if err := os.Link(filepath.Join(trees[0], coverBin), filepath.Join(trees[w], coverBin)); err != nil {
					if err := copyFile(filepath.Join(trees[0], coverBin), filepath.Join(trees[w], coverBin), 0o755); err != nil {
						for i := range ch {
							errs[i] = err
						}
						return
					}
				}
			}
			for i := range ch {
				prof := filepath.Join(tmp, fmt.Sprintf("t%d.cov", i))
				o := e.test(ctx, trees[w], coverBin, []string{names[i]}, 10*time.Minute, prof)
				if o.status != Lived {
					errs[i] = fmt.Errorf("%s: %s does not pass when it is run on its own, unmutated (%s). A mutant is run against the tests that reach it, so each must pass alone; fix the test, or run the unit whole (MUTATE_WHOLE=1).\n%s", e.Dir, names[i], o.status+" "+Status(o.detail), o.tail)
					continue
				}
				p, err := readProfile(prof)
				os.Remove(prof)
				if err != nil {
					errs[i] = err
					continue
				}
				out[i] = testCover{name: names[i], dur: o.dur, prof: p}
			}
		}(w)
	}
	for i := range names {
		ch <- i
	}
	close(ch)
	wg.Wait()
	return out, errors.Join(errs...)
}

// profile is a coverage profile of one package: per file (base name), its
// blocks and whether each was executed.
type profile struct {
	files map[string][]block
}

type block struct {
	sl, sc, el, ec int
	hit            bool
}

func readProfile(path string) (*profile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	p := &profile{files: map[string][]block{}}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		// path/file.go:12.34,15.2 3 1
		colon := strings.LastIndex(line, ":")
		if colon < 0 {
			return nil, fmt.Errorf("coverage profile line %q", line)
		}
		var b block
		var stmts, count int
		if _, err := fmt.Sscanf(line[colon+1:], "%d.%d,%d.%d %d %d", &b.sl, &b.sc, &b.el, &b.ec, &stmts, &count); err != nil {
			return nil, fmt.Errorf("coverage profile line %q: %v", line, err)
		}
		b.hit = count > 0
		name := line[:colon]
		name = name[strings.LastIndex(name, "/")+1:]
		p.files[name] = append(p.files[name], b)
	}
	for _, bs := range p.files {
		sort.Slice(bs, func(i, j int) bool {
			if bs[i].sl != bs[j].sl {
				return bs[i].sl < bs[j].sl
			}
			return bs[i].sc < bs[j].sc
		})
	}
	return p, nil
}

func (b block) holds(line, col int) bool {
	if line < b.sl || line > b.el {
		return false
	}
	if line == b.sl && col < b.sc {
		return false
	}
	if line == b.el && col >= b.ec {
		return false
	}
	return true
}

// at says whether (line, col) of file is inside an instrumented block, and
// whether that block ran. A token outside every block (a package-level
// initialiser, a case expression) is "not instrumented": coverage cannot say,
// so the tests are run.
func (p *profile) at(file string, line, col int) (instrumented, covered bool) {
	for _, b := range p.files[file] {
		if b.holds(line, col) {
			instrumented = true
			if b.hit {
				return true, true
			}
		}
	}
	return instrumented, false
}

// union marks in p every block that ran in q.
func (p *profile) union(q *profile) {
	for name, qs := range q.files {
		ps := p.files[name]
		idx := map[[4]int]int{}
		for i, b := range ps {
			idx[[4]int{b.sl, b.sc, b.el, b.ec}] = i
		}
		for _, b := range qs {
			if !b.hit {
				continue
			}
			if i, ok := idx[[4]int{b.sl, b.sc, b.el, b.ec}]; ok {
				ps[i].hit = true
			}
		}
	}
}

// sourceFiles lists what to copy: the files git knows or would add (so no build
// output, no node_modules), or, outside a repository, everything but the usual
// heavy directories.
func sourceFiles(root string) ([]string, error) {
	cmd := exec.Command("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	cmd.Dir = root
	if out, err := cmd.Output(); err == nil && len(out) > 0 {
		var files []string
		for _, f := range strings.Split(strings.TrimRight(string(out), "\x00"), "\x00") {
			if st, err := os.Lstat(filepath.Join(root, f)); err == nil && st.Mode().IsRegular() {
				files = append(files, f)
			}
		}
		return files, nil
	}
	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", ".tmp", "target", ".venv", "bin", "dist":
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type().IsRegular() {
			rel, _ := filepath.Rel(root, path)
			files = append(files, rel)
		}
		return nil
	})
	return files, err
}

func copyTree(root, dst string, files []string) error {
	for _, f := range files {
		st, err := os.Stat(filepath.Join(root, f))
		if err != nil {
			return err
		}
		to := filepath.Join(dst, f)
		if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
			return err
		}
		if err := copyFile(filepath.Join(root, f), to, st.Mode().Perm()); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(from, to string, perm os.FileMode) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// tail keeps the last n lines of a run for an error message.
type tail struct {
	lines []string
	n     int
}

func newTail(n int) *tail { return &tail{n: n} }

func (t *tail) add(l string) {
	if len(l) > 400 {
		l = l[:400] + "…"
	}
	t.lines = append(t.lines, l)
	if len(t.lines) > t.n {
		t.lines = t.lines[1:]
	}
}

func (t *tail) String() string { return strings.Join(t.lines, "\n") }

func firstLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}
