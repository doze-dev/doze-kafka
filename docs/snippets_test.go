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

// The kcat lines in clients.md, run as written against a broker of this test's
// own with $KAFKA_BOOTSTRAP_SERVERS set: the produce line is given "hi" on stdin,
// the consume line must print it back, and the metadata line must list a broker.
// The Kafka command-line tools are interactive Java; they are only held to the
// same variable. (The kafkajs snippet is run as written by compat/kafkajs.
// frameworks.md's properties are the settings the Spring and Connect compat cells
// use, written there by hand rather than read from the page.)
func TestClientsShellSnippetsRunAsWritten(t *testing.T) {
	if testing.Short() {
		t.Skip("runs kcat")
	}
	raw, err := os.ReadFile("clients.md")
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, m := range regexp.MustCompile("(?s)```sh\n(.*?)```").FindAllStringSubmatch(string(raw), -1) {
		for _, l := range strings.Split(strings.TrimSpace(m[1]), "\n") {
			lines = append(lines, strings.TrimSpace(strings.SplitN(l, "  #", 2)[0]))
		}
	}
	if len(lines) == 0 {
		t.Fatal("clients.md has no shell snippets")
	}
	if _, err := exec.LookPath("kcat"); err != nil {
		t.Skip("kcat is not installed: the shell snippets are not run here")
	}

	addr := inProcessBroker(t)
	env := append(os.Environ(), "KAFKA_BOOTSTRAP_SERVERS="+addr)
	ran := 0
	for _, l := range lines {
		if !strings.Contains(l, `"$KAFKA_BOOTSTRAP_SERVERS"`) {
			t.Errorf("clients.md: %q does not use \"$KAFKA_BOOTSTRAP_SERVERS\"", l)
		}
		if !strings.HasPrefix(l, "kcat ") {
			continue
		}
		cmd := exec.Command("sh", "-c", l)
		cmd.Env = env
		if strings.Contains(l, " -P") {
			cmd.Stdin = strings.NewReader("hi\n")
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", l, err, out)
		}
		switch {
		case strings.Contains(l, " -C"):
			if !regexp.MustCompile(`(?m)^hi$`).Match(out) {
				t.Errorf("%s printed %q, not the record the produce line wrote", l, out)
			}
		case strings.Contains(l, " -L"):
			if !strings.Contains(string(out), "1 brokers:") {
				t.Errorf("%s lists no broker:\n%s", l, out)
			}
		}
		ran++
	}
	if ran != 3 {
		t.Errorf("ran %d kcat lines, want the 3 clients.md shows", ran)
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
