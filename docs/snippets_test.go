package docs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	dkserver "github.com/doze-dev/doze-kafka/server"
)

// docHost is the broker address the snippets are written against; the test points
// it at a broker of its own.
const docHost = "kafka.myproject.doze:9092"

// goBlocks is every ```go block in a markdown file.
func goBlocks(t *testing.T, file string) []string {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, m := range regexp.MustCompile("(?s)```go\n(.*?)```").FindAllStringSubmatch(string(raw), -1) {
		out = append(out, m[1])
	}
	return out
}

// The Go connection snippets in clients.md are run as written, each against a
// fresh broker of its own, and what they produce has to be there afterwards: the
// franz-go one failed verbatim with UNKNOWN_TOPIC_OR_PARTITION, because franz-go
// does not ask the broker to create a topic it has not heard of.
// (embedding.md's code is compiled and run by server's TestEmbeddingDocCompiles;
// the README and frameworks.md have no Go.)
func TestClientsGoSnippetsRunAsWritten(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the go tool")
	}
	for _, f := range []string{"../README.md", "frameworks.md"} {
		if n := len(goBlocks(t, f)); n != 0 {
			t.Errorf("%s has %d Go blocks and this test runs only clients.md's: run them too", f, n)
		}
	}
	blocks := goBlocks(t, "clients.md")
	if len(blocks) == 0 {
		t.Fatal("clients.md has no Go snippets")
	}

	// A dot-prefixed directory inside the module: ignored by ./..., and the
	// snippets build with the module's own pinned client versions.
	dir, err := os.MkdirTemp("..", ".clientsdoc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	var src strings.Builder
	src.WriteString(`package clientsdoc

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/doze-dev/doze-kafka/server"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	_ = sarama.NewConfig
	_ = kgo.SeedBrokers
)

// broker is a fresh in-process broker, as the binary's defaults have it.
func broker(t *testing.T) (context.Context, string) {
	srv, err := server.New(server.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	<-srv.Ready()
	t.Cleanup(func() { stop(); <-done })
	return ctx, srv.Addr().String()
}

// produced reads topic demo from the start and wants the record "hi" in it.
func produced(t *testing.T, ctx context.Context, addr string) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics("demo"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	fctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		fs := cl.PollFetches(fctx)
		if fctx.Err() != nil {
			t.Fatal("the snippet produced nothing to topic demo")
		}
		for _, r := range fs.Records() {
			if string(r.Value) == "hi" {
				return
			}
		}
	}
}
`)
	for i, b := range blocks {
		if !strings.Contains(b, docHost) {
			t.Errorf("clients.md Go snippet %d does not use %s, which this test replaces:\n%s", i+1, docHost, b)
			continue
		}
		body := strings.ReplaceAll(b, `"`+docHost+`"`, "addr")
		fmt.Fprintf(&src, "\nfunc TestSnippet%d(t *testing.T) {\n\tctx, addr := broker(t)\n\tfunc() {\n%s\t}()\n\tproduced(t, ctx, addr)\n}\n", i+1, body)
	}
	if err := os.WriteFile(filepath.Join(dir, "snippets_test.go"), []byte(src.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-count=1", "./"+filepath.Base(dir))
	cmd.Dir = ".."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("a Go snippet in clients.md does not compile or does not do what it shows:\n%s", out)
	}
}

// The kcat lines in clients.md and the README, run as written against a broker of
// this test's own with $KAFKA_BOOTSTRAP_SERVERS set: a produce line reads "hi" from
// stdin unless it pipes its own input, the consume line must print what was
// produced, and the metadata line must list a broker. The Kafka command-line tools
// are interactive Java and are only held to the same variable; the README's
// install lines fetch from the network and are not run. (The kafkajs snippet is
// run as written by compat/kafkajs. frameworks.md's properties are the settings
// the Spring and Connect compat cells use, written there by hand rather than read
// from the page.)
func TestShellSnippetsRunAsWritten(t *testing.T) {
	if testing.Short() {
		t.Skip("runs kcat")
	}
	if _, err := exec.LookPath("kcat"); err != nil {
		t.Skip("kcat is not installed: the shell snippets are not run here")
	}
	for file, wantLines := range map[string]int{"clients.md": 3, "../README.md": 2} {
		t.Run(filepath.Base(file), func(t *testing.T) {
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var lines []string
			for _, m := range regexp.MustCompile("(?s)```sh\n(.*?)```").FindAllStringSubmatch(string(raw), -1) {
				for _, l := range strings.Split(strings.TrimSpace(m[1]), "\n") {
					l = strings.TrimSpace(strings.SplitN(l, "  #", 2)[0])
					if !strings.HasPrefix(l, "#") && (strings.Contains(l, "kcat ") || strings.Contains(l, "kafka-console-")) {
						lines = append(lines, l)
					}
				}
			}
			addr := inProcessBroker(t)
			env := append(os.Environ(), "KAFKA_BOOTSTRAP_SERVERS="+addr)
			ran, produced := 0, ""
			for _, l := range lines {
				if !strings.Contains(l, `"$KAFKA_BOOTSTRAP_SERVERS"`) {
					t.Errorf("%s: %q does not use \"$KAFKA_BOOTSTRAP_SERVERS\"", file, l)
				}
				if !strings.Contains(l, "kcat ") {
					continue
				}
				cmd := exec.Command("sh", "-c", l)
				cmd.Env = env
				if strings.Contains(l, " -P") {
					produced = "hi"
					if m := regexp.MustCompile(`^echo "([^"]*)" \|`).FindStringSubmatch(l); m != nil {
						produced = m[1]
					} else {
						cmd.Stdin = strings.NewReader("hi\n")
					}
				}
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("%s: %v\n%s", l, err, out)
				}
				switch {
				case strings.Contains(l, " -C"):
					if produced == "" || !regexp.MustCompile(`(?m)^`+regexp.QuoteMeta(produced)+`$`).Match(out) {
						t.Errorf("%s printed %q, not the record the produce line wrote (%q)", l, out, produced)
					}
				case strings.Contains(l, " -L"):
					if !strings.Contains(string(out), "1 brokers:") {
						t.Errorf("%s lists no broker:\n%s", l, out)
					}
				}
				ran++
			}
			if ran != wantLines {
				t.Errorf("ran %d kcat lines from %s, want %d", ran, file, wantLines)
			}
		})
	}
}

// inProcessBroker is a fresh broker with the binary's defaults, until the test ends.
func inProcessBroker(t *testing.T) string {
	t.Helper()
	srv, err := dkserver.New(dkserver.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	<-srv.Ready()
	t.Cleanup(func() { stop(); <-done })
	return srv.Addr().String()
}
