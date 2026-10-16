package docs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaImage is the oracle the conformance recordings were made against; the
// documents' Apache Kafka commands are run in it.
const kafkaImage = "apache/kafka:4.3.1"

// envRequireKafkaCLI makes a missing Docker a failure rather than a skip: the compat
// job sets it, so the documents' kafka-*.sh lines are run on every push there.
const envRequireKafkaCLI = "DOCS_KAFKA_CLI"

// A command line is an Apache Kafka tool when its first word is kafka-<something>.sh.
func isKafkaTool(c string) bool {
	f := strings.Fields(c)
	return len(f) > 0 && strings.HasPrefix(f[0], "kafka-") && strings.HasSuffix(f[0], ".sh")
}

// kafkaToolLines is every Apache Kafka tool line in the shell blocks of the user
// documents, with the document it is in.
func kafkaToolLines(t testing.TB) (lines []string, where map[string]string) {
	docs := userDocs(t)
	where = map[string]string{}
	for _, name := range sortedKeys(docs) {
		for _, c := range shellCommands(docs[name]) {
			if isKafkaTool(c) {
				if _, seen := where[c]; !seen {
					lines = append(lines, c)
				}
				where[c] += " " + name
			}
		}
	}
	return lines, where
}

// Every kafka-*.sh line in the documents, run as written in apache/kafka against the
// built binary. Two things are supplied that a reader supplies: the bootstrap
// address ($KAFKA_BOOTSTRAP_SERVERS, and a literal localhost:9092, are the test
// broker's address as the container reaches it), and input for the console producer
// (three lines, as getting-started.md types). The console consumer runs until it is
// stopped, as for a reader; it is stopped once it has printed what was produced. The
// group the consumer-groups lines name is made by a real client first.
func TestKafkaToolLinesRunInApacheKafka(t *testing.T) {
	bin := binary(t)
	lines, where := kafkaToolLines(t)
	if len(lines) < 5 {
		t.Fatalf("found %d kafka-*.sh lines in the documents: the extractor is broken", len(lines))
	}
	required := os.Getenv(envRequireKafkaCLI) == "1"
	skip := func(why string) {
		if required {
			t.Fatalf("%s, and %s=1 says these lines must run", why, envRequireKafkaCLI)
		}
		t.Skipf("%s (set %s=1 to make that a failure)", why, envRequireKafkaCLI)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		skip("docker is not installed")
	}
	if out, err := exec.Command("docker", "image", "inspect", kafkaImage).CombinedOutput(); err != nil {
		if !required {
			skip(fmt.Sprintf("%s is not pulled here (%s)", kafkaImage, strings.TrimSpace(string(out))))
		}
		if out, err := exec.Command("docker", "pull", kafkaImage).CombinedOutput(); err != nil {
			t.Fatalf("docker pull %s: %v\n%s", kafkaImage, err, out)
		}
	}

	// A broker the container can reach: every interface, advertising the name the
	// container has for this host (as compat/run.sh does for its Docker clients).
	dir := t.TempDir()
	if err := os.Symlink(bin, dir+"/doze-kafka"); err != nil {
		t.Fatal(err)
	}
	port := freePort(t)
	local := fmt.Sprintf("127.0.0.1:%d", port)
	b := startBroker(t, dir, local, false, "--listen", fmt.Sprintf("0.0.0.0:%d", port), "--advertised-host", "host.docker.internal",
		"--console=false", "--schema-registry=false")
	defer b.kill()
	inContainer := fmt.Sprintf("host.docker.internal:%d", port)

	name := fmt.Sprintf("doze-docs-kafka-cli-%d", time.Now().UnixNano())
	if out, err := exec.Command("docker", "run", "-d", "--rm", "--name", name, "--add-host=host.docker.internal:host-gateway",
		"--entrypoint", "sleep", kafkaImage, "900").CombinedOutput(); err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}
	defer exec.Command("docker", "rm", "-f", name).Run()

	run := func(line, stdin string, stopWhen func(string) bool) (string, error) {
		line = strings.ReplaceAll(line, "localhost:9092", inContainer)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		args := []string{"exec", "-i", "-e", "KAFKA_BOOTSTRAP_SERVERS=" + inContainer, "-e", "PATH=/opt/kafka/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", name, "sh", "-c", line}
		cmd := exec.CommandContext(ctx, "docker", args...)
		cmd.Stdin = strings.NewReader(stdin)
		var out syncBuf
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			return "", err
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		if stopWhen == nil {
			err := <-done
			return out.String(), err
		}
		for {
			select {
			case err := <-done:
				if stopWhen(out.String()) {
					return out.String(), nil
				}
				return out.String(), fmt.Errorf("ended (%v) before printing what was produced", err)
			case <-time.After(200 * time.Millisecond):
				if stopWhen(out.String()) {
					exec.Command("docker", "exec", name, "pkill", "-f", "ConsoleConsumer").Run()
					<-done
					return out.String(), nil
				}
			case <-ctx.Done():
				<-done
				return out.String(), fmt.Errorf("still running after two minutes")
			}
		}
	}

	// The order a reader goes in: make the topic, write to it, read it back, then look
	// at a group.
	rank := func(c string) int {
		switch strings.Fields(c)[0] {
		case "kafka-topics.sh":
			return 0
		case "kafka-console-producer.sh":
			return 1
		case "kafka-console-consumer.sh":
			return 2
		}
		return 3
	}
	ran := map[string]bool{}
	for r := 0; r <= 3; r++ {
		if r == 3 {
			makeGroup(t, local, "demo", "workers")
		}
		for _, c := range lines {
			if rank(c) != r {
				continue
			}
			var out string
			var err error
			switch r {
			case 1:
				out, err = run(c, "one\ntwo\nthree\n", nil)
			case 2:
				out, err = run(c, "", func(s string) bool { return strings.Contains(s, "one\ntwo\nthree") })
			default:
				out, err = run(c, "", nil)
			}
			if err != nil {
				t.Errorf("%s: `%s` in %s: %v\n%s", strings.TrimSpace(where[c]), c, kafkaImage, err, out)
			} else if strings.Contains(out, "ERROR") || strings.Contains(out, "Exception") {
				t.Errorf("%s: `%s` printed an error:\n%s", strings.TrimSpace(where[c]), c, out)
			} else if strings.Contains(c, "--describe --group workers") && !strings.Contains(out, "workers") {
				t.Errorf("%s: `%s` did not describe the group:\n%s", strings.TrimSpace(where[c]), c, out)
			}
			ran[c] = true
		}
	}
	for _, c := range lines {
		if !ran[c] {
			t.Errorf("not run: %s", c)
		}
	}
}

// makeGroup gives a group a committed offset on topic, as the application the
// consumer-groups document is about would have.
func makeGroup(t *testing.T, addr, topic, group string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for got := 0; got == 0 && ctx.Err() == nil; {
		got = cl.PollFetches(ctx).NumRecords()
	}
	if err := cl.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("committing for group %s: %v", group, err)
	}
}
