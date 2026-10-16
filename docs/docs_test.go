// Package docs holds the tests that keep the documents honest (foundation B14 and
// E14). One command runs them all: scripts/docs-check.sh.
//
//   - links_test.go: every relative link and #anchor in every markdown file outside
//     plan/ points at something that is there.
//   - commands_test.go: the commands of getting-started.md run, as written, against a
//     binary built by the document's own build line, and every block that shows what a
//     command prints is compared with what it printed.
//   - kafkacli_test.go: every kafka-*.sh line in the documents runs in apache/kafka.
//   - errors_test.go: every string the documents say the program prints or answers is
//     what it prints or answers.
//
// Every check is a function that returns what it found wrong, so a test can hand it
// a small document with a fault in it and require that the fault is found. A check
// that could no longer fail would fail there.
//
// This file is what they share: the repository as a tree, the parts of a markdown
// file, the built binary, and a broker started from it.
package docs

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	code := m.Run()
	shared.stop()
	if built.dir != "" {
		os.RemoveAll(built.dir)
	}
	os.Exit(code)
}

// inCI reports whether this is a CI run: there a missing tool is a failure, not a skip.
func inCI() bool {
	v := strings.ToLower(os.Getenv("CI"))
	return v != "" && v != "0" && v != "false"
}

// ---- the repository as a tree ----------------------------------------------------

func repoRoot(t testing.TB) string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("%s is not the repository root: %v", root, err)
	}
	return root
}

// A tree answers what a link can point at. Paths are slash-separated and relative to
// the root. The real one is the repository; a test hands the checks a map.
type tree interface {
	// kind reports whether the path is a file or a directory that others will have
	// too. why says so when it is neither.
	kind(rel string) (file, dir bool, why string)
	read(rel string) (string, error)
}

// fakeTree is a tree in memory: path to content.
type fakeTree map[string]string

func (f fakeTree) kind(rel string) (bool, bool, string) {
	if _, ok := f[rel]; ok {
		return true, false, ""
	}
	for p := range f {
		if strings.HasPrefix(p, rel+"/") || rel == "." {
			return false, true, ""
		}
	}
	return false, false, "does not exist"
}

func (f fakeTree) read(rel string) (string, error) {
	s, ok := f[rel]
	if !ok {
		return "", fs.ErrNotExist
	}
	return s, nil
}

// diskTree is the repository. With git it knows which files are tracked or about to
// be (not ignored): a link to a file that exists only on this machine, such as build
// output, is dead for everyone else. Without git (a source archive, a container
// whose .git is elsewhere) it is the files on disk.
type diskTree struct {
	root   string
	shared map[string]bool // files git tracks or would add; nil without git
	dirs   map[string]bool
}

func (d *diskTree) kind(rel string) (bool, bool, string) {
	st, err := os.Stat(filepath.Join(d.root, filepath.FromSlash(rel)))
	if err != nil {
		return false, false, "does not exist"
	}
	if d.shared == nil {
		return !st.IsDir(), st.IsDir(), ""
	}
	if st.IsDir() {
		if rel == "." || d.dirs[rel] {
			return false, true, ""
		}
		return false, false, "exists here but holds nothing git tracks (it is ignored): nobody else has it"
	}
	if d.shared[rel] {
		return true, false, ""
	}
	return false, false, "exists here but git ignores it: nobody else has it"
}

func (d *diskTree) read(rel string) (string, error) {
	b, err := os.ReadFile(filepath.Join(d.root, filepath.FromSlash(rel)))
	return string(b), err
}

// gitFiles lists what git tracks, plus what is new and not ignored, under root.
func gitFiles(root string, patterns ...string) ([]string, error) {
	args := append([]string{"ls-files", "-z", "--cached", "--others", "--exclude-standard", "--"}, patterns...)
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-files: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	var files []string
	for _, f := range strings.Split(string(out), "\x00") {
		if f == "" || strings.HasSuffix(f, "/") { // a nested checkout is listed as a directory
			continue
		}
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(f))); err != nil {
			continue // tracked, and deleted here
		}
		files = append(files, f)
	}
	sort.Strings(files)
	return files, nil
}

func repoTree(t testing.TB) (*diskTree, bool) {
	t.Helper()
	d := &diskTree{root: repoRoot(t)}
	files, err := gitFiles(d.root)
	if err != nil {
		t.Logf("git is not usable here (%v): links are checked against the files on disk", err)
		return d, false
	}
	d.shared, d.dirs = map[string]bool{}, map[string]bool{}
	for _, f := range files {
		d.shared[f] = true
		for dir := path.Dir(f); dir != "."; dir = path.Dir(dir) {
			d.dirs[dir] = true
		}
	}
	return d, true
}

// notDocuments are directories whose markdown is not the project's: dependencies,
// build output, scratch space, other checkouts. plan/ is working notes that point at
// things that come and go, and is left out on purpose.
var (
	notDocumentsAtRoot   = map[string]bool{"plan": true, "data": true, "bin": true, "dist": true}
	notDocumentsAnywhere = map[string]bool{
		".git": true, ".mutate": true, ".claude": true, "node_modules": true, ".venv": true,
		"target": true, ".tmp": true, "test-results": true, "playwright-report": true,
	}
)

// isNotDocument reports whether a file (by its path from the root) is under one of them.
func isNotDocument(rel string) bool {
	parts := strings.Split(rel, "/")
	for i, p := range parts[:len(parts)-1] {
		if notDocumentsAnywhere[p] || (i == 0 && notDocumentsAtRoot[p]) {
			return true
		}
	}
	return false
}

// walkMarkdown finds the markdown files with no help from git.
func walkMarkdown(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if e.IsDir() {
			if rel != "." && isNotDocument(rel+"/x") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(rel, ".md") {
			files = append(files, rel)
		}
		return nil
	})
	sort.Strings(files)
	return files, err
}

// repoMarkdown is every markdown file of the project outside plan/: what git tracks
// (or would add), or with no git, what a walk finds.
func repoMarkdown(t testing.TB, root string, withGit bool) []string {
	t.Helper()
	if !withGit {
		files, err := walkMarkdown(root)
		if err != nil {
			t.Fatal(err)
		}
		return files
	}
	all, err := gitFiles(root, "*.md")
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, f := range all {
		if !isNotDocument(f) {
			files = append(files, f)
		}
	}
	return files
}

// userDocs are the documents a user reads: the guides and the README. The checks of
// what the program prints and says are over these.
func userDocs(t testing.TB) map[string]string {
	t.Helper()
	root := repoRoot(t)
	names, err := filepath.Glob(filepath.Join(root, "docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	names = append(names, filepath.Join(root, "README.md"))
	docs := map[string]string{}
	for _, n := range names {
		b, err := os.ReadFile(n)
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(root, n)
		docs[filepath.ToSlash(rel)] = string(b)
	}
	if len(docs) < 15 {
		t.Fatalf("found only %d user documents", len(docs))
	}
	return docs
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---- the parts of a markdown file ----------------------------------------------

// A block is one fenced block.
type block struct {
	info   string   // the word after the opening fence: sh, go, text, or nothing
	marker string   // the text of an <!-- output: ... --> comment on the line above, if any
	lines  []string // the content
	line   int      // the line of the opening fence, from 1
}

var outputMarker = regexp.MustCompile(`^<!--\s*output:\s*(.*?)\s*-->$`)

// fencedBlocks returns the fenced blocks of a document, in order.
func fencedBlocks(md string) []block {
	var blocks []block
	var cur *block
	last := "" // the last line outside a fence that is not blank
	for i, l := range strings.Split(md, "\n") {
		t := strings.TrimSpace(l)
		if strings.HasPrefix(t, "```") {
			if cur != nil {
				blocks = append(blocks, *cur)
				cur, last = nil, ""
				continue
			}
			cur = &block{info: strings.TrimSpace(strings.TrimPrefix(t, "```")), line: i + 1}
			if m := outputMarker.FindStringSubmatch(last); m != nil {
				cur.marker = m[1]
			}
			continue
		}
		if cur != nil {
			cur.lines = append(cur.lines, l)
		} else if t != "" {
			last = t
		}
	}
	return blocks
}

// isShell reports whether a block is commands for a shell.
func (b block) isShell() bool { return b.info == "sh" || b.info == "bash" || b.info == "shell" }

// commands returns the commands of a shell block, one per logical line (a trailing
// backslash continues), with comment lines and blank lines gone.
func (b block) commands() []string {
	var cmds []string
	cur := ""
	for _, l := range b.lines {
		t := strings.TrimSpace(l)
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		if strings.HasSuffix(t, "\\") {
			cur += strings.TrimSuffix(t, "\\") + " "
			continue
		}
		cmds = append(cmds, cur+t)
		cur = ""
	}
	return cmds
}

// shellCommands is every command in the shell blocks of a document.
func shellCommands(md string) []string {
	var cmds []string
	for _, b := range fencedBlocks(md) {
		if b.isShell() {
			cmds = append(cmds, b.commands()...)
		}
	}
	return cmds
}

// blankFences returns the document with the content of every fenced block, and the
// fences, replaced by empty lines: the prose, with its line numbers kept.
func blankFences(md string) string {
	lines := strings.Split(md, "\n")
	fenced := false
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "```") {
			fenced = !fenced
			lines[i] = ""
		} else if fenced {
			lines[i] = ""
		}
	}
	return strings.Join(lines, "\n")
}

// A span is one inline code span of the prose.
type span struct {
	text       string // its content, with runs of white space (a line break too) as one space
	line       int
	start, end int // where it is in the text blankFences returned
}

var codeSpan = regexp.MustCompile("(?s)`+.+?`+")

// codeSpans returns the inline code spans of the prose of a document.
func codeSpans(prose string) []span {
	var out []span
	for _, m := range codeSpan.FindAllStringIndex(prose, -1) {
		raw := prose[m[0]:m[1]]
		inner := strings.Trim(raw, "`")
		if strings.Contains(inner, "\n\n") {
			continue // a stray backtick, not a span: a span does not cross a paragraph
		}
		out = append(out, span{
			text: strings.Join(strings.Fields(inner), " "),
			line: 1 + strings.Count(prose[:m[0]], "\n"), start: m[0], end: m[1],
		})
	}
	return out
}

// ---- the binary -------------------------------------------------------------------

// buildLine is the command getting-started.md gives for building the binary. The
// tests build with it, changing only where the output goes.
const buildLine = "go build -o doze-kafka ./cmd/doze-kafka"

var built struct {
	once sync.Once
	dir  string
	bin  string
	err  error
}

// binary builds doze-kafka once, by the document's own build line, straight into a
// temporary directory. Nothing is written in the repository: the line's `-o
// doze-kafka` becomes `-o <temporary directory>/doze-kafka`, and that is the only
// change to it.
func binary(t testing.TB) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds the binary and runs it")
	}
	if runtime.GOOS == "windows" {
		t.Skip("the documents' commands are for sh")
	}
	root := repoRoot(t)
	built.once.Do(func() {
		doc, err := os.ReadFile(filepath.Join(root, "docs", "getting-started.md"))
		if err != nil {
			built.err = err
			return
		}
		found := false
		for _, c := range shellCommands(string(doc)) {
			found = found || c == buildLine
		}
		if !found {
			built.err = fmt.Errorf("getting-started.md no longer has the build line %q; these tests build with it", buildLine)
			return
		}
		built.dir, built.err = os.MkdirTemp("", "doze-kafka-docs-")
		if built.err != nil {
			return
		}
		built.bin, built.err = buildInto(root, built.dir)
	})
	if built.err != nil {
		t.Fatal(built.err)
	}
	return built.bin
}

// buildInto runs the build line with its output in dir, and fails if the build put a
// binary in the repository root (where the line, typed as written, would).
func buildInto(root, dir string) (string, error) {
	inRepo := filepath.Join(root, "doze-kafka")
	_, before := os.Stat(inRepo)
	args := strings.Fields(buildLine)
	bin := filepath.Join(dir, "doze-kafka")
	for i, a := range args {
		if a == "-o" {
			args[i+1] = bin
		}
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("`%s` (output to %s) failed: %v\n%s", buildLine, dir, err, out)
	}
	if _, err := os.Stat(bin); err != nil {
		return "", fmt.Errorf("`%s` built no binary: %v", buildLine, err)
	}
	if _, after := os.Stat(inRepo); before != nil && after == nil {
		return "", fmt.Errorf("the build left %s in the repository", inRepo)
	}
	return bin, nil
}

// ---- a broker started from it -----------------------------------------------------

func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// A broker is the built binary, running.
type broker struct {
	cmd                      *exec.Cmd
	stdout, stderr           *syncBuf
	dir                      string
	kafka, console, registry string // host:port
	exited                   chan error
}

// newWorkDir makes a directory to run the broker in as the documents do: the binary
// is ./doze-kafka there, and a doze-kafka.toml picks free loopback ports and nothing
// else, so the documents' command line stays exactly as written.
func newWorkDir(t testing.TB, bin string) (dir string, kafka, console, registry string) {
	t.Helper()
	dir = t.TempDir()
	if err := os.Symlink(bin, filepath.Join(dir, "doze-kafka")); err != nil {
		t.Fatal(err)
	}
	kafka = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	console = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	registry = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	toml := fmt.Sprintf("listen = %q\nconsole-addr = %q\nschema-registry-addr = %q\n", kafka, console, registry)
	if err := os.WriteFile(filepath.Join(dir, "doze-kafka.toml"), []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, kafka, console, registry
}

// docEnv is the environment the documents' commands run in: the built binary first
// on PATH, and DOZE_ZONE=off so no .doze name is asked for (that needs a one-time
// sudo, which a test must not ask for).
func docEnv(binDir string, extra ...string) []string {
	env := append(os.Environ(), "DOZE_ZONE=off", "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return append(env, extra...)
}

// bannerEnd is the last line of the start-up summary.
const bannerEnd = "when something looks wrong"

// startBroker runs ./doze-kafka in dir and waits until it answers on kafka and, when
// wantBanner, has printed its start-up summary.
func startBroker(t testing.TB, dir, kafka string, wantBanner bool, args ...string) *broker {
	t.Helper()
	cmd := exec.Command("./doze-kafka", args...)
	cmd.Dir = dir
	cmd.Env = docEnv(dir)
	b := &broker{cmd: cmd, stdout: &syncBuf{}, stderr: &syncBuf{}, dir: dir, kafka: kafka, exited: make(chan error, 1)}
	cmd.Stdout, cmd.Stderr = b.stdout, b.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { b.exited <- cmd.Wait() }()
	// The machine may be busy with other work: a minute, not a few seconds.
	deadline := time.Now().Add(60 * time.Second)
	for {
		select {
		case err := <-b.exited:
			b.exited <- err
			t.Fatalf("the broker exited at once: %v\n%s%s", err, b.stdout, b.stderr)
		default:
		}
		up := false
		if c, err := net.DialTimeout("tcp", kafka, 200*time.Millisecond); err == nil {
			c.Close()
			up = true
		}
		if up && (!wantBanner || strings.Contains(b.stdout.String(), bannerEnd)) {
			return b
		}
		if time.Now().After(deadline) {
			b.kill()
			t.Fatalf("the broker did not come up on %s (summary wanted: %v)\n%s%s", kafka, wantBanner, b.stdout, b.stderr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (b *broker) kill() {
	b.cmd.Process.Kill()
	select {
	case err := <-b.exited:
		b.exited <- err
	case <-time.After(10 * time.Second):
	}
}

// interrupt is Ctrl-C: it returns what the broker logged from then until it exited,
// and an error if it did not exit 0.
func (b *broker) interrupt() (logged []string, err error) {
	before := len(b.stderr.String())
	if err := b.cmd.Process.Signal(syscall.SIGINT); err != nil {
		return nil, err
	}
	select {
	case err = <-b.exited:
		b.exited <- err
	case <-time.After(60 * time.Second):
		b.kill()
		return nil, errors.New("the broker did not stop within a minute of Ctrl-C")
	}
	logged = splitLines(b.stderr.String()[before:])
	if err != nil {
		return logged, fmt.Errorf("the broker did not exit 0 on Ctrl-C: %v", err)
	}
	return logged, nil
}

// shared is one broker for the checks that only read from it: its start-up summary,
// its console and its registry.
var shared sharedBrokers

type sharedBrokers struct {
	mu    sync.Mutex
	plain *broker
	sasl  *broker
}

// The credentials of the SASL broker: the ones auth-and-tls.md uses.
const saslUser, saslPassword = "alice", "s3cret"

func (s *sharedBrokers) get(t testing.TB, sasl bool) *broker {
	t.Helper()
	bin := binary(t)
	s.mu.Lock()
	defer s.mu.Unlock()
	slot, args := &s.plain, []string(nil)
	if sasl {
		slot, args = &s.sasl, []string{"--sasl-mechanisms", "PLAIN", "--sasl-user", saslUser + ":" + saslPassword}
	}
	if *slot != nil {
		return *slot
	}
	// Not t.TempDir: this outlives the test that asked first.
	dir, err := os.MkdirTemp(filepath.Dir(bin), "broker-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(bin, filepath.Join(dir, "doze-kafka")); err != nil {
		t.Fatal(err)
	}
	kafka := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	console := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	registry := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	toml := fmt.Sprintf("listen = %q\nconsole-addr = %q\nschema-registry-addr = %q\n", kafka, console, registry)
	if err := os.WriteFile(filepath.Join(dir, "doze-kafka.toml"), []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}
	b := startBroker(t, dir, kafka, true, args...)
	b.console, b.registry = console, registry
	*slot = b
	return b
}

func (s *sharedBrokers) stop() {
	for _, b := range []*broker{s.plain, s.sasl} {
		if b != nil {
			b.kill()
		}
	}
}

// runShell runs one command line with sh, in dir, and returns what it printed and
// its exit status. A command that has not ended in two minutes is killed (-1).
func runShell(command, dir string, env []string) (stdout, stderr string, exit int) {
	cmd := exec.Command("sh", "-c", command)
	cmd.Dir = dir
	cmd.Env = env
	var o, e bytes.Buffer
	cmd.Stdout, cmd.Stderr = &o, &e
	if err := cmd.Start(); err != nil {
		return "", err.Error(), -1
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
		return o.String(), e.String(), cmd.ProcessState.ExitCode()
	case <-time.After(2 * time.Minute):
		cmd.Process.Kill()
		<-done
		return o.String(), e.String() + "\n(killed: still running after two minutes)", -1
	}
}

// splitLines is the lines of some output, without the empty one a final newline makes.
func splitLines(s string) []string {
	s = strings.TrimSuffix(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// syncBuf is a buffer a process writes to while the test reads it.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// writeFile writes a file under dir, making its directories.
func writeFile(t testing.TB, dir, rel, body string) string {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}
