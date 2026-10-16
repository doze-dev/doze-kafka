package docs

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// What varies between the reader's run and this one, and nothing else, is
// normalised before a documented output is compared with a real one: the time
// of a log line, an address (name or IP, and port), a URL, an absolute path,
// and the width of the white space between columns.
var (
	logTime   = regexp.MustCompile(`^time=\S+\s+`)
	anURL     = regexp.MustCompile(`https?://\S+`)
	anAddress = regexp.MustCompile(`\b[\w.-]+:\d{2,5}\b`)
	aPath     = regexp.MustCompile(`(^|\s)/\S+`)
)

func normalise(line string) string {
	line = logTime.ReplaceAllString(line, "")
	line = anURL.ReplaceAllString(line, "<url>")
	line = anAddress.ReplaceAllString(line, "<address>")
	line = aPath.ReplaceAllString(line, "$1<path>")
	return strings.Join(strings.Fields(line), " ")
}

// compareExact reports how a documented output differs from the real one, line by
// line after normalising both. Blank lines at either end do not count.
func compareExact(doc, got []string) []string {
	trim := func(ls []string) []string {
		var out []string
		for _, l := range ls {
			out = append(out, normalise(l))
		}
		for len(out) > 0 && out[0] == "" {
			out = out[1:]
		}
		for len(out) > 0 && out[len(out)-1] == "" {
			out = out[:len(out)-1]
		}
		return out
	}
	d, g := trim(doc), trim(got)
	var bad []string
	for i := 0; i < len(d) || i < len(g); i++ {
		switch {
		case i >= len(d):
			bad = append(bad, fmt.Sprintf("the program also printed %q, which the document does not show", g[i]))
		case i >= len(g):
			bad = append(bad, fmt.Sprintf("the document shows %q, which the program did not print", d[i]))
		case d[i] != g[i]:
			bad = append(bad, fmt.Sprintf("the document shows %q, the program printed %q", d[i], g[i]))
		}
	}
	return bad
}

// compareIncluded reports each documented line that is not among the real ones, in
// order (a log excerpt: the program logs more than the document shows).
func compareIncluded(doc, got []string) []string {
	var bad []string
	j := 0
	for _, l := range doc {
		want := normalise(l)
		if want == "" {
			continue
		}
		for j < len(got) && normalise(got[j]) != want {
			j++
		}
		if j == len(got) {
			bad = append(bad, fmt.Sprintf("the document shows %q, which the program did not log (in this order)", want))
			j = 0
			continue
		}
		j++
	}
	return bad
}

// The comparison's own test: a documented output with one false line, one missing
// line and one extra line, and the differences that are only what varies.
func TestOutputComparisonFindsAFalseLine(t *testing.T) {
	real := []string{"", "  doze-kafka is up.", "", "    kafka     127.0.0.1:40123", "    console   http://127.0.0.1:40124   live", "    data      /tmp/x/data", ""}
	same := []string{"  doze-kafka is up.", "", "    kafka     kafka.project.doze:9092", "    console   http://kafka.project.doze   live", "    data      /home/you/project/data"}
	if bad := compareExact(same, real); len(bad) != 0 {
		t.Errorf("a document that differs only in what varies was refused: %q", bad)
	}
	wrong := append([]string(nil), same...)
	wrong[2] = "    broker    kafka.project.doze:9092"
	if bad := compareExact(wrong, real); len(bad) != 1 || !strings.Contains(bad[0], "broker") {
		t.Errorf("a false line was not reported: %q", bad)
	}
	if bad := compareExact(same[:4], real); len(bad) != 1 || !strings.Contains(bad[0], "also printed") {
		t.Errorf("a line the document leaves out was not reported: %q", bad)
	}
	if bad := compareExact(append(same, "    registry  http://x"), real); len(bad) != 1 || !strings.Contains(bad[0], "did not print") {
		t.Errorf("a line the program does not print was not reported: %q", bad)
	}
	logged := []string{`time=2026-10-16T10:00:00Z level=INFO msg="received signal"`, `time=2026-10-16T10:00:00Z level=INFO msg="shutting down: flushing logs to disk"`}
	if bad := compareIncluded([]string{`level=INFO msg="shutting down: flushing logs to disk"`}, logged); len(bad) != 0 {
		t.Errorf("a true log excerpt was refused: %q", bad)
	}
	if bad := compareIncluded([]string{`level=INFO msg="shutting down cleanly"`}, logged); len(bad) != 1 {
		t.Errorf("a false log line was not reported: %q", bad)
	}
}

// The commands of getting-started.md, run in order as a reader runs them, against a
// binary built by the document's own build line; and every output block in it,
// compared with what the program printed. An output block is a fence with no shell
// language under an <!-- output: <what> --> comment, <what> being one of:
//
//	banner     what ./doze-kafka prints on stdout, exactly
//	consume    what the kcat command just before it printed, exactly
//	shutdown   lines the broker logs on Ctrl-C (an excerpt, in order); the broker is
//	           stopped with SIGINT here, where the document says to press it
//
// A fence that is neither a shell block nor marked fails: an output the test does not
// compare is an output nobody checks. The broker is given its ports by a
// doze-kafka.toml (so the command line stays exactly as written; the summary's
// "config" line that file adds is the one line not compared), and DOZE_ZONE=off (a
// test must not ask for sudo; so names print as addresses, normalised above).
func TestGettingStartedRunsAsWrittenAndPrintsWhatItShows(t *testing.T) {
	bin := binary(t)
	if _, err := exec.LookPath("kcat"); err != nil {
		if inCI() {
			t.Fatal("kcat is not installed, and CI runs the document's kcat commands as written (scripts/install-kcat.sh)")
		}
		t.Skip("kcat is not installed: the document's commands are run as written, and they are kcat")
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "docs", "getting-started.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	dir, kafka, _, _ := newWorkDir(t, bin)
	env := docEnv(dir)

	var (
		b           *broker
		bootstrap   string
		lastConsume string
		compared    = map[string]int{}
		starts      int
		ran         int
	)
	defer func() {
		if b != nil {
			b.kill()
		}
	}()
	for _, blk := range fencedBlocks(doc) {
		if !blk.isShell() {
			switch blk.marker {
			case "banner":
				if b == nil {
					t.Fatalf("getting-started.md:%d: a banner block before the broker is started", blk.line)
				}
				// The block is what a terminal shows: log lines (stderr) around the
				// summary (stdout). The summary is compared exactly; each log line must
				// be among the lines logged, in order.
				var got []string
				for _, l := range splitLines(b.stdout.String()) {
					if !strings.HasPrefix(strings.TrimSpace(l), "config ") { // the test's port file
						got = append(got, l)
					}
				}
				var summary, logLines []string
				for _, l := range blk.lines {
					if logTime.MatchString(l) || strings.HasPrefix(strings.TrimSpace(l), "level=") {
						logLines = append(logLines, l)
					} else {
						summary = append(summary, l)
					}
				}
				for _, d := range compareExact(summary, got) {
					t.Errorf("getting-started.md:%d (the start-up summary): %s", blk.line, d)
				}
				// The log goes on after the summary (the "listening" line is written
				// just after it): give it a few seconds to arrive.
				var logBad []string
				for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
					if logBad = compareIncluded(logLines, splitLines(b.stderr.String())); len(logBad) == 0 || time.Now().After(deadline) {
						break
					}
				}
				for _, d := range logBad {
					t.Errorf("getting-started.md:%d (the start-up log): %s", blk.line, d)
				}
			case "consume":
				if lastConsume == "" {
					t.Fatalf("getting-started.md:%d: a consume block with no kcat -C before it", blk.line)
				}
				for _, d := range compareExact(blk.lines, splitLines(lastConsume)) {
					t.Errorf("getting-started.md:%d (what kcat -C printed): %s", blk.line, d)
				}
				lastConsume = ""
			case "shutdown":
				if b == nil {
					t.Fatalf("getting-started.md:%d: a shutdown block with no broker running", blk.line)
				}
				logged, err := b.interrupt()
				if err != nil {
					t.Errorf("getting-started.md:%d: Ctrl-C: %v", blk.line, err)
				}
				for _, d := range compareIncluded(blk.lines, logged) {
					t.Errorf("getting-started.md:%d (the log on Ctrl-C): %s", blk.line, d)
				}
				b = nil
			default:
				t.Errorf("getting-started.md:%d: a ``` block that is neither a shell block nor an <!-- output: banner|consume|shutdown --> one: nothing checks it", blk.line)
				continue
			}
			compared[blk.marker]++
			continue
		}
		for _, c := range blk.commands() {
			ran++
			first := strings.Fields(c)[0]
			switch {
			case first == "git", first == "cd":
				t.Logf("not run (the clone needs the network, and this is the clone): %s", c)
			case c == buildLine:
				// Built once by binary(), by this line, into a temporary directory.
			case c == "./doze-kafka" || c == "./doze-kafka &":
				if b != nil {
					t.Fatalf("getting-started.md:%d: %q starts a second broker while one runs; the document stops it first", blk.line, c)
				}
				if starts > 0 && c != "./doze-kafka &" {
					t.Logf("restart: %s", c)
				}
				b = startBroker(t, dir, kafka, true)
				starts++
			case first == "printf", first == "kcat":
				if b == nil {
					t.Fatalf("getting-started.md:%d: %q runs with no broker", blk.line, c)
				}
				if bootstrap == "" {
					bootstrap = evalEnv(t, doc, dir, env)
					if bootstrap != kafka {
						t.Fatalf("`doze-kafka env` points at %q; the broker listens on %s", bootstrap, kafka)
					}
					env = append(env, "KAFKA_BOOTSTRAP_SERVERS="+bootstrap)
				}
				stdout, stderr, code := runShell(c, dir, env)
				if code != 0 {
					t.Errorf("getting-started.md:%d: `%s` exited %d\n%s%s", blk.line, c, code, stdout, stderr)
				}
				if strings.Contains(c, " -C") {
					lastConsume = stdout
				}
			case strings.HasPrefix(first, "kafka-"):
				// Java; run in apache/kafka by kafkacli_test.go.
				if !strings.Contains(c, `"$KAFKA_BOOTSTRAP_SERVERS"`) {
					t.Errorf("getting-started.md:%d: %q does not use $KAFKA_BOOTSTRAP_SERVERS", blk.line, c)
				}
			default:
				t.Errorf("getting-started.md:%d: a command this test does not know how to run: %q", blk.line, c)
			}
		}
	}
	if ran < 8 {
		t.Fatalf("found %d commands in getting-started.md: the extractor is broken", ran)
	}
	if starts < 2 {
		t.Errorf("the broker was started %d times; the durability section restarts it", starts)
	}
	for what, want := range map[string]int{"banner": 1, "consume": 2, "shutdown": 1} {
		if compared[what] != want {
			t.Errorf("getting-started.md has %d <!-- output: %s --> blocks, want %d", compared[what], what, want)
		}
	}
}

// evalEnv checks that the document tells the reader to run the eval line, runs it
// in a shell, and returns the bootstrap address it set.
func evalEnv(t *testing.T, doc, dir string, env []string) string {
	t.Helper()
	const evalLine = `eval "$(doze-kafka env)"`
	if !strings.Contains(doc, "`"+evalLine+"`") {
		t.Fatalf("getting-started.md no longer tells the reader to run %s before the client commands", evalLine)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		stdout, stderr, code := runShell(evalLine+`; echo "BOOTSTRAP=$KAFKA_BOOTSTRAP_SERVERS"`, dir, env)
		for _, l := range splitLines(stdout) {
			if v, ok := strings.CutPrefix(l, "BOOTSTRAP="); ok && v != "" && code == 0 {
				return v
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s set no KAFKA_BOOTSTRAP_SERVERS (exit %d)\n%s%s", evalLine, code, stdout, stderr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
