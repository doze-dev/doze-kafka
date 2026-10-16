// Command lightness holds the broker's weight to testdata/lightness.json: the
// size of the binary, the heap it holds when idle, and how many modules it is
// built from.
//
//	go run ./scripts/lightness                       check this platform's budget (what CI runs)
//	go run ./scripts/lightness record                record this platform; a budget only goes down
//	go run ./scripts/lightness record -reason "why"  ... or up, with the reason written into the file
//	go run ./scripts/lightness history               only the history rule: nothing is built
//	go run ./scripts/lightness measure -runs 20      print the numbers and their spread, change nothing
//
// A budget is the measured value, per platform (GOOS/GOARCH), not a figure with
// room in it:
//
//   - binaryBytes: the build is reproducible (the file's "build" says exactly
//     how: no cgo, -trimpath, no VCS stamp, the toolchain go.mod names), so one
//     byte over the budget fails;
//   - idleHeapBytes: the live heap after three forced collections (see
//     idleHeap for why three), the largest of several starts. It varies from
//     start to start by a few KB (7.7 KB across 20 starts on macOS/arm64); so
//     the budget is the largest of 20 starts, the check takes the largest of
//     5, and the gate allows what the largest sat above the median when
//     recorded (idleHeapNoiseBytes) and no more;
//   - modules: `go list -m all`, exact.
//
// A platform with no budget fails: a budget recorded on macOS says nothing
// about Linux. And every time a number in the file went up in git history, the
// file as it is now must say why (its "raises"); a shallow clone fails.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
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
	budgetFile = "testdata/lightness.json"
	// How the binary is built and started. Both are written into the file, and
	// the gate fails when the file's differ from these: a budget is for one
	// exact build.
	buildLine = "CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' ./cmd/doze-kafka"
	runLine   = "GOMAXPROCS=2 doze-kafka --listen 127.0.0.1:0 --console-addr 127.0.0.1:0 --schema-registry-addr 127.0.0.1:0 --data-dir <empty> --pprof-addr 127.0.0.1:<free>; heap = HeapAlloc of the third /debug/pprof/heap?gc=1, one second after pprof answers"
	checkRuns = 5
)

// Platform is one platform's budget.
type Platform struct {
	BinaryBytes        int64  `json:"binaryBytes"`
	IdleHeapBytes      int64  `json:"idleHeapBytes"`
	IdleHeapNoiseBytes int64  `json:"idleHeapNoiseBytes"`
	Go                 string `json:"go"`
	Recorded           string `json:"recorded"`
	Where              string `json:"where"`
}

// Raise is the written reason for one number having gone up.
type Raise struct {
	Date   string `json:"date"`
	Key    string `json:"key"` // "modules", or "<goos>/<goarch> binaryBytes" / "... idleHeapBytes".
	From   int64  `json:"from"`
	To     int64  `json:"to"` // -1: the budget was removed.
	Reason string `json:"reason"`
}

// Note explains a weight the budget carries on purpose (plan B9).
type Note struct {
	What   string `json:"what"`
	Commit string `json:"commit"`
	Cost   string `json:"cost"`
	Why    string `json:"why"`
}

// Budget is testdata/lightness.json.
type Budget struct {
	Build     string              `json:"build"`
	Run       string              `json:"run"`
	Modules   int64               `json:"modules"`
	Platforms map[string]Platform `json:"platforms"`
	Carries   []Note              `json:"carries"`
	Raises    []Raise             `json:"raises"`
}

// ParseBudget reads the file, or the one-platform shape it had before budgets
// were per platform (recorded on macOS/arm64, with a 5% slack).
func ParseBudget(raw []byte) (*Budget, error) {
	var b Budget
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, err
	}
	if b.Platforms == nil {
		var old struct {
			BinaryBytes   int64 `json:"binaryBytes"`
			IdleHeapBytes int64 `json:"idleHeapBytes"`
			Modules       int64 `json:"modules"`
		}
		if err := json.Unmarshal(raw, &old); err != nil {
			return nil, err
		}
		b.Modules = old.Modules
		b.Platforms = map[string]Platform{}
		if old.BinaryBytes > 0 || old.IdleHeapBytes > 0 {
			b.Platforms["darwin/arm64"] = Platform{BinaryBytes: old.BinaryBytes, IdleHeapBytes: old.IdleHeapBytes}
		}
	}
	return &b, nil
}

// numbers is every budget in the file by key.
func (b *Budget) numbers() map[string]int64 {
	out := map[string]int64{}
	if b.Modules > 0 {
		out["modules"] = b.Modules
	}
	for name, p := range b.Platforms {
		if p.BinaryBytes > 0 {
			out[name+" binaryBytes"] = p.BinaryBytes
		}
		if p.IdleHeapBytes > 0 {
			out[name+" idleHeapBytes"] = p.IdleHeapBytes
		}
	}
	return out
}

func (b *Budget) format() []byte {
	if b.Carries == nil {
		b.Carries = []Note{}
	}
	if b.Raises == nil {
		b.Raises = []Raise{}
	}
	raw, _ := json.MarshalIndent(b, "", "  ")
	return append(raw, '\n')
}

// History holds every number that went up, or away, in the file's git history
// against the reasons the file gives now.
func History(now *Budget, changes []githist.Change) []string {
	explained := func(key string, from, to int64) bool {
		for _, r := range now.Raises {
			if r.Key == key && r.From == from && r.To == to && len(strings.TrimSpace(r.Reason)) >= 15 {
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
		old, err := ParseBudget(c.Old)
		if err != nil {
			continue
		}
		// A version that cannot be read holds no budget: everything before it
		// reads as removed.
		next := &Budget{}
		if c.New != nil {
			if parsed, err := ParseBudget(c.New); err == nil {
				next = parsed
			} else if c.Commit == "" {
				out = append(out, fmt.Sprintf("%s: %v", budgetFile, err))
				continue
			}
		}
		have := next.numbers()
		for key, from := range old.numbers() {
			to, ok := have[key]
			if !ok {
				to = -1
			} else if to <= from {
				continue
			}
			if explained(key, from, to) {
				continue
			}
			what := fmt.Sprintf("raised %s from %d to %d", key, from, to)
			if !ok {
				what = fmt.Sprintf("removed the budget %s (%d)", key, from)
			}
			date := c.Date
			if date == "" {
				date = time.Now().Format("2006-01-02")
			}
			msg := fmt.Sprintf("%s %s and %s has no entry in \"raises\" saying why. Add:\n    {\"date\": %q, \"key\": %q, \"from\": %d, \"to\": %d, \"reason\": \"<at least 15 characters>\"}", c.Where(), what, budgetFile, date, key, from, to)
			if !said[msg] {
				said[msg] = true
				out = append(out, msg)
			}
		}
	}
	sort.Strings(out)
	return out
}

// Measured is this platform's weight now.
type Measured struct {
	Platform    string
	Go          string
	BinaryBytes int64
	Modules     int64
	Heaps       []int64 // one per start, sorted.
}

func (m *Measured) median() int64 { return m.Heaps[len(m.Heaps)/2] }

// heap is the idle heap as the gate reads it: the largest of the starts.
func (m *Measured) heap() int64 { return m.Heaps[len(m.Heaps)-1] }

// noise is how far the largest start sat above the median one.
func (m *Measured) noise() int64 { return m.heap() - m.median() }

// Compare holds what was measured against a platform's budget.
func Compare(b *Budget, m *Measured) (failures, notes []string) {
	if b.Build != buildLine || b.Run != runLine {
		failures = append(failures, fmt.Sprintf("%s was recorded for another build or start (its \"build\" and \"run\" are not this tool's): record again", budgetFile))
	}
	p, ok := b.Platforms[m.Platform]
	if !ok {
		var have []string
		for name := range b.Platforms {
			have = append(have, name)
		}
		sort.Strings(have)
		return append(failures, fmt.Sprintf("NO BUDGET for %s (recorded: %s). A budget is per platform: run `task lightness:record` on this one", m.Platform, strings.Join(have, ", "))), notes
	}
	if p.Go != m.Go {
		failures = append(failures, fmt.Sprintf("TOOLCHAIN %s: the budget was recorded with %s and this is %s; a binary's size is a fact about one toolchain. Record again", m.Platform, p.Go, m.Go))
	}
	judge := func(name string, got, budget, slack int64) {
		switch {
		case got > budget+slack:
			over := fmt.Sprintf("%d over", got-budget)
			if slack > 0 {
				over += fmt.Sprintf("; allowed for start-to-start variation, as measured when recorded: %d", slack)
			}
			failures = append(failures, fmt.Sprintf("OVER     %s %s: %d > budget %d (%s)", m.Platform, name, got, budget, over))
		case got < budget-slack:
			notes = append(notes, fmt.Sprintf("ok       %s %s: %d, %d under its budget %d (`task lightness:record` takes the budget down)", m.Platform, name, got, budget-got, budget))
		default:
			notes = append(notes, fmt.Sprintf("ok       %s %s: %d (budget %d)", m.Platform, name, got, budget))
		}
	}
	judge("binaryBytes", m.BinaryBytes, p.BinaryBytes, 0)
	judge("idleHeapBytes", m.heap(), p.IdleHeapBytes, p.IdleHeapNoiseBytes)
	switch {
	case m.Modules > b.Modules:
		failures = append(failures, fmt.Sprintf("OVER     modules: %d > budget %d", m.Modules, b.Modules))
	default:
		notes = append(notes, fmt.Sprintf("ok       modules: %d (budget %d)", m.Modules, b.Modules))
	}
	return failures, notes
}

// Record writes what was measured as this platform's budget. A number that
// went up needs a reason, and the reason goes into the file; the idle heap
// counts as "up" only beyond the variation it was recorded with.
func Record(b *Budget, m *Measured, where, reason, today string) (refused []string) {
	if b.Platforms == nil {
		b.Platforms = map[string]Platform{}
	}
	old, had := b.Platforms[m.Platform]
	now := Platform{BinaryBytes: m.BinaryBytes, IdleHeapBytes: m.heap(), IdleHeapNoiseBytes: m.noise(), Go: m.Go, Recorded: today, Where: where}
	raise := func(key string, from, to int64) {
		if reason == "" {
			refused = append(refused, fmt.Sprintf("%s: %d is over its budget %d", key, to, from))
			return
		}
		b.Raises = append(b.Raises, Raise{Date: today, Key: key, From: from, To: to, Reason: reason})
	}
	if had && b.Build == buildLine && b.Run == runLine {
		if now.BinaryBytes > old.BinaryBytes && old.BinaryBytes > 0 {
			raise(m.Platform+" binaryBytes", old.BinaryBytes, now.BinaryBytes)
		}
		switch {
		case old.IdleHeapBytes == 0:
		case now.IdleHeapBytes > old.IdleHeapBytes+old.IdleHeapNoiseBytes:
			raise(m.Platform+" idleHeapBytes", old.IdleHeapBytes, now.IdleHeapBytes)
		case now.IdleHeapBytes >= old.IdleHeapBytes-old.IdleHeapNoiseBytes:
			// The same heap as far as it can be measured: the budget stays, so
			// that recording twice does not walk it up a few bytes at a time.
			now.IdleHeapBytes, now.IdleHeapNoiseBytes = old.IdleHeapBytes, max(old.IdleHeapNoiseBytes, now.IdleHeapNoiseBytes)
		}
	} else if had {
		// Another build or start: the old numbers are not comparable, and the
		// history rule will ask for the reason of any that went up.
		if now.BinaryBytes > old.BinaryBytes && old.BinaryBytes > 0 {
			raise(m.Platform+" binaryBytes", old.BinaryBytes, now.BinaryBytes)
		}
		if now.IdleHeapBytes > old.IdleHeapBytes && old.IdleHeapBytes > 0 {
			raise(m.Platform+" idleHeapBytes", old.IdleHeapBytes, now.IdleHeapBytes)
		}
	}
	if b.Modules > 0 && m.Modules > b.Modules {
		raise("modules", b.Modules, m.Modules)
	}
	if len(refused) > 0 {
		return refused
	}
	b.Build, b.Run, b.Modules = buildLine, runLine, m.Modules
	b.Platforms[m.Platform] = now
	return nil
}

// Measure builds the binary in root and starts it runs times.
func Measure(root string, runs int, log io.Writer) (*Measured, error) {
	tmp, err := os.MkdirTemp("", "lightness-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	bin := filepath.Join(tmp, "doze-kafka")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	goCmd := func(env []string, args ...string) ([]byte, error) {
		cmd := exec.Command("go", args...)
		cmd.Dir = root
		cmd.Env = append(append(os.Environ(), "GOWORK=off"), env...)
		return cmd.CombinedOutput()
	}
	if out, err := goCmd([]string{"CGO_ENABLED=0"}, "build", "-trimpath", "-buildvcs=false", "-ldflags=-s -w", "-o", bin, "./cmd/doze-kafka"); err != nil {
		return nil, fmt.Errorf("build: %v\n%s", err, out)
	}
	st, err := os.Stat(bin)
	if err != nil {
		return nil, err
	}
	mods, err := goCmd(nil, "list", "-m", "all")
	if err != nil {
		return nil, fmt.Errorf("go list -m all: %v\n%s", err, mods)
	}
	ver, err := goCmd(nil, "env", "GOVERSION")
	if err != nil {
		return nil, fmt.Errorf("go env GOVERSION: %v\n%s", err, ver)
	}
	m := &Measured{
		Platform: runtime.GOOS + "/" + runtime.GOARCH, Go: strings.TrimSpace(string(ver)),
		BinaryBytes: st.Size(), Modules: int64(len(strings.Split(strings.TrimSpace(string(mods)), "\n"))),
	}
	for i := 0; i < runs; i++ {
		h, err := idleHeap(bin, filepath.Join(tmp, "data"+strconv.Itoa(i)))
		if err != nil {
			return nil, err
		}
		m.Heaps = append(m.Heaps, h)
		if log != nil {
			fmt.Fprintf(log, "start %d of %d: idle heap %d bytes\n", i+1, runs, h)
		}
	}
	sort.Slice(m.Heaps, func(i, j int) bool { return m.Heaps[i] < m.Heaps[j] })
	return m, nil
}

// idleHeap starts the binary over an empty data directory and reads its live
// heap after a forced collection.
func idleHeap(bin, dataDir string) (int64, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	addr := l.Addr().String()
	l.Close()
	var logs strings.Builder
	cmd := exec.Command(bin, "--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--schema-registry-addr", "127.0.0.1:0",
		"--data-dir", dataDir, "--pprof-addr", addr)
	// A fixed GOMAXPROCS: what the runtime sets up per processor is not the
	// broker's weight, and it differs between a laptop and a CI runner.
	cmd.Env = append(os.Environ(), "GOMAXPROCS=2")
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
	}()
	up := false
	for i := 0; i < 200 && !up; i++ {
		if resp, err := http.Get("http://" + addr + "/debug/pprof/"); err == nil {
			resp.Body.Close()
			up = resp.StatusCode == http.StatusOK
		}
		if !up {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !up {
		return 0, fmt.Errorf("the broker's pprof listener did not answer on %s:\n%s", addr, logs.String())
	}
	time.Sleep(time.Second)
	// Three collections, and the heap after the last. One is not enough: what a
	// sync.Pool holds survives the first collection after it was put there, so
	// after a single forced collection the heap reads some 35 KB more or less
	// depending on whether the runtime happened to collect on its own during
	// start-up (two values, 13 to 17 starts in 20 on the higher one on macOS,
	// 2 in 20 on Linux).
	var heap int64 = -1
	for i := 0; i < heapCollections; i++ {
		h, err := heapAlloc(addr)
		if err != nil {
			return 0, fmt.Errorf("%v\n%s", err, logs.String())
		}
		heap = h
	}
	return heap, nil
}

const heapCollections = 3

func heapAlloc(addr string) (int64, error) {
	resp, err := http.Get("http://" + addr + "/debug/pprof/heap?gc=1&debug=1")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), "# HeapAlloc = "); ok {
			return strconv.ParseInt(strings.TrimSpace(rest), 10, 64)
		}
	}
	return 0, fmt.Errorf("no HeapAlloc line in the heap profile")
}

func main() {
	mode := "check"
	args := os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		mode, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	reason := fs.String("reason", "", "record: why a number went up (written into the file)")
	where := fs.String("where", "", "record: the machine, in a few words (\"macOS 27, M-series, native\")")
	runs := fs.Int("runs", 0, "how many times the broker is started (check: 5, record and measure: 20)")
	fs.Parse(args)
	os.Exit(run(".", mode, *reason, *where, *runs, os.Stdout, os.Stderr))
}

func run(root, mode, reason, where string, runs int, stdout, stderr io.Writer) int {
	path := filepath.Join(root, filepath.FromSlash(budgetFile))
	raw, err := os.ReadFile(path)
	budget := &Budget{}
	if err == nil {
		if budget, err = ParseBudget(raw); err != nil {
			fmt.Fprintf(stderr, "%s: %v\n", budgetFile, err)
			return 1
		}
	} else if !(os.IsNotExist(err) && mode != "check" && mode != "history") {
		fmt.Fprintln(stderr, err)
		return 1
	}
	history := func(b *Budget) int {
		changes, err := githist.Changes(root, budgetFile)
		if err != nil {
			fmt.Fprintln(stderr, "HISTORY ", err)
			return 1
		}
		bad := History(b, changes)
		for _, s := range bad {
			fmt.Fprintln(stderr, "RAISED  ", s)
		}
		if len(bad) > 0 {
			return 1
		}
		fmt.Fprintf(stdout, "ok       every budget that ever went up in %s has its reason there (%d changes read)\n", budgetFile, len(changes))
		return 0
	}
	switch mode {
	case "history":
		return history(budget)
	case "check", "record", "measure":
	default:
		fmt.Fprintln(stderr, "usage: lightness [check | record [-reason \"why\"] [-where \"machine\"] | history | measure] [-runs n]")
		return 2
	}
	if mode == "check" {
		if rc := history(budget); rc != 0 {
			return rc
		}
	}
	if runs <= 0 {
		runs = checkRuns
		if mode != "check" {
			runs = 20
		}
	}
	var log io.Writer
	if mode != "check" {
		log = stderr
	}
	m, err := Measure(root, runs, log)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "%s, %s: binary %d bytes, idle heap %d bytes (the largest of %d starts; smallest %d, median %d), %d modules\n",
		m.Platform, m.Go, m.BinaryBytes, m.heap(), len(m.Heaps), m.Heaps[0], m.median(), m.Modules)
	switch mode {
	case "measure":
		return 0
	case "record":
		if strings.TrimSpace(where) == "" {
			if p, ok := budget.Platforms[m.Platform]; ok {
				where = p.Where
			}
		}
		if strings.TrimSpace(where) == "" {
			fmt.Fprintln(stderr, "record needs -where \"<the machine, in a few words>\" the first time a platform is recorded")
			return 2
		}
		if refused := Record(budget, m, where, strings.TrimSpace(reason), time.Now().Format("2006-01-02")); len(refused) > 0 {
			fmt.Fprintln(stderr, "refusing to raise a budget without a reason (record -reason \"<why>\" writes it into the file):")
			for _, s := range refused {
				fmt.Fprintln(stderr, "  "+s)
			}
			return 1
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err := os.WriteFile(path, budget.format(), 0o644); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprintf(stdout, "recorded %s in %s\n", m.Platform, budgetFile)
		return history(budget)
	}
	failures, notes := Compare(budget, m)
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
