// Package docs holds the tests that keep the documents honest (foundation B14):
// a link that points nowhere fails, and so does a getting-started command that
// does not run against a built binary.
package docs

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// markdownFiles is docs/*.md and the README, relative to the docs directory.
func markdownFiles(t testing.TB) []string {
	files, err := filepath.Glob("*.md")
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, filepath.Join("..", "README.md"))
	return files
}

// stripCode removes fenced code blocks and inline code spans, where a bracketed
// word followed by a parenthesis is code, not a link.
func stripCode(md string) string {
	var out []string
	fenced := false
	for _, line := range strings.Split(md, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			fenced = !fenced
			continue
		}
		if !fenced {
			out = append(out, line)
		}
	}
	return regexp.MustCompile("`[^`]*`").ReplaceAllString(strings.Join(out, "\n"), "")
}

var (
	inlineLink = regexp.MustCompile(`\]\(([^)\s]+)(?:\s+"[^"]*")?\)`)
	refLink    = regexp.MustCompile(`(?m)^\s*\[[^\]]+\]:\s*(\S+)`)
	htmlHref   = regexp.MustCompile(`(?:href|src)="([^"]+)"`)
)

// links returns every link target in a markdown document.
func links(md string) []string {
	md = stripCode(md)
	var out []string
	for _, re := range []*regexp.Regexp{inlineLink, refLink, htmlHref} {
		for _, m := range re.FindAllStringSubmatch(md, -1) {
			out = append(out, m[1])
		}
	}
	return out
}

// anchors is the set of heading anchors GitHub generates for a markdown file.
func anchors(md string) map[string]bool {
	set := map[string]bool{}
	seen := map[string]int{}
	fenced := false
	for _, line := range strings.Split(md, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			fenced = !fenced
			continue
		}
		if fenced || !strings.HasPrefix(line, "#") {
			continue
		}
		text := strings.TrimSpace(strings.TrimLeft(line, "#"))
		text = regexp.MustCompile("[`*_]|\\[([^\\]]*)\\]\\([^)]*\\)").ReplaceAllStringFunc(text, func(s string) string {
			if m := regexp.MustCompile(`\[([^\]]*)\]`).FindStringSubmatch(s); m != nil {
				return m[1]
			}
			return ""
		})
		slug := strings.ToLower(text)
		slug = regexp.MustCompile(`[^\p{L}\p{N} _-]`).ReplaceAllString(slug, "")
		slug = strings.ReplaceAll(slug, " ", "-")
		if n := seen[slug]; n > 0 {
			set[fmt.Sprintf("%s-%d", slug, n)] = true
		} else {
			set[slug] = true
		}
		seen[slug]++
	}
	return set
}

// checkLinks reports every link in md (a file at path) that points at a file or a
// heading that does not exist. Absolute URLs are not fetched.
func checkLinks(path, md string, read func(string) (string, error)) []string {
	var bad []string
	for _, l := range links(md) {
		switch {
		case strings.Contains(l, "://"), strings.HasPrefix(l, "mailto:"), strings.HasPrefix(l, "tel:"):
			continue
		}
		file, frag, _ := strings.Cut(l, "#")
		target := path
		if file != "" {
			target = filepath.Join(filepath.Dir(path), filepath.FromSlash(file))
		}
		st, err := os.Stat(target)
		if err != nil {
			bad = append(bad, fmt.Sprintf("%s: link to %q: %s does not exist", path, l, target))
			continue
		}
		if frag == "" || st.IsDir() || !strings.HasSuffix(target, ".md") {
			continue
		}
		body, err := read(target)
		if err != nil {
			bad = append(bad, fmt.Sprintf("%s: link to %q: %v", path, l, err))
			continue
		}
		if !anchors(body)[strings.ToLower(frag)] {
			bad = append(bad, fmt.Sprintf("%s: link to %q: no heading #%s in %s", path, l, frag, target))
		}
	}
	return bad
}

func TestNoDeadRelativeLinks(t *testing.T) {
	read := func(p string) (string, error) {
		b, err := os.ReadFile(p)
		return string(b), err
	}
	files := markdownFiles(t)
	if len(files) < 10 {
		t.Fatalf("found only %d markdown files", len(files))
	}
	n := 0
	for _, f := range files {
		md, err := read(f)
		if err != nil {
			t.Fatal(err)
		}
		n += len(links(md))
		for _, b := range checkLinks(f, md, read) {
			t.Error(b)
		}
	}
	if n < 50 {
		t.Fatalf("found only %d links in %d files: the extractor is broken", n, len(files))
	}
}

func TestCheckLinksFindsADeadLinkAndADeadAnchor(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	other := write("other.md", "# Other\n\n## A heading, with `code`\n")
	main := write("main.md", "[ok](other.md) [ok](other.md#a-heading-with-code) [dead](gone.md) [badanchor](other.md#nope)\n"+
		"[web](https://example.com/x) `[code](notalink.md)`\n```\n[fenced](x.md)\n```\n[self](#top)\n\n# Top\n")
	_ = other
	read := func(p string) (string, error) { b, err := os.ReadFile(p); return string(b), err }
	md, _ := read(main)
	got := checkLinks(main, md, read)
	if len(got) != 2 || !strings.Contains(got[0], "gone.md") || !strings.Contains(got[1], "#nope") {
		t.Errorf("want exactly the dead file and the dead anchor, got %q", got)
	}
}

// ---- getting-started.md: the commands run ----------------------------------

// shellCommands returns the commands in the ```sh blocks of a document, one per
// logical line (a trailing backslash continues), with comments and blanks gone.
func shellCommands(md string) []string {
	var cmds []string
	in := false
	var cur string
	for _, line := range strings.Split(md, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "```") {
			in = !in && (t == "```sh" || t == "```bash" || t == "```shell")
			continue
		}
		if !in || t == "" || strings.HasPrefix(t, "#") {
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

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// server is a built binary run the way the document says.
type server struct {
	cmd *exec.Cmd
	out *syncBuf
}

func startServer(t *testing.T, dir string) *server {
	t.Helper()
	cmd := exec.Command("./doze-kafka")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "DOZE_ZONE=off") // no .doze names: they need a one-time sudo
	out := &syncBuf{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := &server{cmd: cmd, out: out}
	t.Cleanup(func() { s.cmd.Process.Kill(); s.cmd.Wait() })
	return s
}

func (s *server) stop(t *testing.T) {
	t.Helper()
	s.cmd.Process.Signal(syscall.SIGINT)
	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Ctrl-C did not stop the broker cleanly: %v\n%s", err, s.out)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("the broker did not stop on Ctrl-C\n%s", s.out)
	}
}

func waitUp(t *testing.T, addr string, s *server) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			c.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("nothing listening on %s\n%s", addr, s.out)
}

// The commands in getting-started.md, run in order against a binary built by the
// document's own `go build` line. The broker is started with a config file that
// only picks free ports (the document's command line stays exactly as written),
// and DOZE_ZONE=off so no .doze names are asked for.
func TestGettingStartedCommandsRunAgainstABuiltBinary(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary and runs it")
	}
	if runtime.GOOS == "windows" {
		t.Skip("the commands are sh")
	}
	raw, err := os.ReadFile("getting-started.md")
	if err != nil {
		t.Fatal(err)
	}
	cmds := shellCommands(string(raw))
	if len(cmds) < 8 {
		t.Fatalf("found %d commands in getting-started.md; the extractor is broken", len(cmds))
	}

	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	kafka, console, registry := freePort(t), freePort(t), freePort(t)
	toml := fmt.Sprintf("listen = \"127.0.0.1:%d\"\nconsole-addr = \"127.0.0.1:%d\"\nschema-registry-addr = \"127.0.0.1:%d\"\n", kafka, console, registry)
	if err := os.WriteFile(filepath.Join(work, "doze-kafka.toml"), []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}

	var (
		srv       *server
		env       []string
		bootstrap string
		produced  bool
		kcat      = func() bool { _, err := exec.LookPath("kcat"); return err == nil }()
		consumed  = 0
		starts    = 0
	)
	run := func(c string, dir string, extra []string) string {
		t.Helper()
		cmd := exec.Command("sh", "-c", c)
		cmd.Dir = dir
		cmd.Env = append(append(os.Environ(), "DOZE_ZONE=off", "PATH="+work+string(os.PathListSeparator)+os.Getenv("PATH")), extra...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Errorf("`%s` failed: %v\n%s%s", c, err, stdout.String(), stderr.String())
		}
		return stdout.String() // what the document's output blocks show; kcat chatters on stderr
	}

	for _, c := range cmds {
		first := strings.Fields(c)[0]
		switch {
		case first == "git", first == "cd":
			t.Logf("not run (needs the network / the clone): %s", c)

		case first == "go":
			// The build line, run as written from the repository root. The output
			// lands in the root (git ignores it); it is moved into the work dir.
			if c != "go build -o doze-kafka ./cmd/doze-kafka" {
				t.Fatalf("the build command changed: %q; this test builds it as written", c)
			}
			run(c, root, nil)
			bin := filepath.Join(root, "doze-kafka")
			if err := os.Rename(bin, filepath.Join(work, "doze-kafka")); err != nil {
				t.Fatalf("the build command left no ./doze-kafka: %v", err)
			}

		case c == "./doze-kafka" || c == "./doze-kafka &":
			if srv != nil {
				// The prose between the two starts says: stop it with Ctrl-C.
				srv.stop(t)
				if !strings.Contains(srv.out.String(), "shutting down: flushing logs to disk") {
					t.Errorf("the document shows a shutdown line the broker does not log:\n%s", srv.out)
				}
				srv = nil
				starts++
			}
			srv = startServer(t, work)
			waitUp(t, fmt.Sprintf("127.0.0.1:%d", kafka), srv)
			if !strings.Contains(srv.out.String(), "doze-kafka is up") && !waitFor(srv.out, "doze-kafka is up") {
				t.Errorf("the banner the document shows is not printed:\n%s", srv.out)
			}

		case first == "printf", first == "kcat", strings.HasPrefix(first, "kafka-console-"):
			if bootstrap == "" {
				// The document says, in a sentence, to run the eval line in the client
				// terminal; it must say it, and the line must do what it claims.
				const evalLine = `eval "$(doze-kafka env)"`
				if !strings.Contains(string(raw), "`"+evalLine+"`") {
					t.Fatalf("getting-started.md no longer tells the reader to run %s before the client commands", evalLine)
				}
				out := run(evalLine+`; echo "BOOTSTRAP=$KAFKA_BOOTSTRAP_SERVERS"`, work, nil)
				for _, l := range strings.Split(out, "\n") {
					if v, ok := strings.CutPrefix(l, "BOOTSTRAP="); ok {
						bootstrap = v
					}
				}
				if bootstrap != fmt.Sprintf("127.0.0.1:%d", kafka) {
					t.Fatalf("`doze-kafka env` points at %q, the broker listens on 127.0.0.1:%d", bootstrap, kafka)
				}
				env = []string{"KAFKA_BOOTSTRAP_SERVERS=" + bootstrap}
			}
			if first == "kafka-console-producer.sh" || first == "kafka-console-consumer.sh" {
				// Interactive and Java: only checked to name the same variable.
				if !strings.Contains(c, `"$KAFKA_BOOTSTRAP_SERVERS"`) {
					t.Errorf("%q does not use $KAFKA_BOOTSTRAP_SERVERS", c)
				}
				continue
			}
			if kcat {
				out := run(c, work, env)
				if strings.Contains(c, "-C") {
					consumed++
					if got := strings.Fields(out); strings.Join(got, ",") != "one,two,three" {
						t.Errorf("`%s` printed %q, the document says one two three", c, got)
					}
				}
				continue
			}
			// kcat is not installed: do what the commands do, with a Go client, so
			// the document's claim (three messages, back in order, after a restart)
			// is still checked.
			if strings.HasPrefix(first, "printf") {
				produceDemo(t, bootstrap)
				produced = true
			} else if strings.Contains(c, "-C") {
				consumed++
				if got := consumeDemo(t, bootstrap); strings.Join(got, ",") != "one,two,three" {
					t.Errorf("read %q back from topic demo, the document says one two three", got)
				}
			}

		default:
			t.Errorf("getting-started.md has a command this test does not know how to run: %q", c)
		}
	}
	if srv == nil {
		t.Fatal("getting-started.md never starts the broker")
	}
	_ = produced
	if starts < 1 {
		t.Error("the document does not restart the broker, and its durability section is about the restart")
	}
	if consumed < 2 {
		t.Errorf("the document reads the topic %d times; it should read it before and after a restart", consumed)
	}
	srv.stop(t)
	if !strings.Contains(srv.out.String(), "shutting down: flushing logs to disk") {
		t.Errorf("the document shows a shutdown line the broker does not log:\n%s", srv.out)
	}
}

func waitFor(buf *syncBuf, s string) bool {
	for i := 0; i < 50; i++ {
		if strings.Contains(buf.String(), s) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func produceDemo(t *testing.T, addr string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sc := bufio.NewScanner(strings.NewReader("one\ntwo\nthree\n"))
	for sc.Scan() {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "demo", Value: []byte(sc.Text())}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func consumeDemo(t *testing.T, addr string) []string {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics("demo"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var got []string
	for len(got) < 3 {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		fs.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	return got
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
