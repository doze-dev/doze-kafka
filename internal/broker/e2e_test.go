package broker_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// This is the "does it actually work when someone runs it" test: it drives the real
// compiled binary out-of-process — real signal handling, real flags, the actual
// artifact — which every in-process test bypasses.

// buildBinary compiles cmd/doze-kafka to a temp path.
func buildBinary(t *testing.T) string {
	t.Helper()
	skipShort(t)
	bin := filepath.Join(t.TempDir(), "doze-kafka")
	args := []string{"build", "-o", bin}
	if os.Getenv("SOAK_RACE") == "1" {
		args = append(args, "-race") // the soak's binary under the race detector: see runSoak.
	}
	cmd := exec.Command("go", append(args, "github.com/doze-dev/doze-kafka/cmd/doze-kafka")...)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build binary: %v\n%s", err, out)
	}
	return bin
}

// startBinary launches the binary and waits until it accepts connections.
func startBinary(t *testing.T, bin, addr, dataDir, metaDB string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	var log bytes.Buffer
	cmd := exec.Command(bin, "-listen", addr, "-data-dir", dataDir, "-meta-db", metaDB,
		// Only what the test is about: no console on the default port (the suite
		// failed whenever 9093 was busy). --listen means no name is claimed, so
		// nothing lands in the developer's real zone.
		"-console=false")
	cmd.Stdout = &log
	cmd.Stderr = &log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start binary: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			c.Close()
			return cmd, &log
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			t.Fatalf("binary never started listening on %s\n%s", addr, log.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// stopBinary sends SIGTERM and asserts a clean, graceful exit.
func stopBinary(t *testing.T, cmd *exec.Cmd, log *bytes.Buffer) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("binary exited non-zero on SIGTERM: %v\n%s", err, log.String())
	}
	if !strings.Contains(log.String(), "flushing logs to disk") {
		t.Fatalf("graceful shutdown did not flush; log:\n%s", log.String())
	}
}

// TestBinary_RealProcessDurabilityAndSignals runs the actual binary, produces to it
// with franz, stops it with SIGTERM (graceful flush), restarts it over the same data
// dir, and confirms the data survived — exercising the real artifact, its flags, and
// its signal handling end-to-end (everything the in-process tests bypass).
func TestBinary_RealProcessDurabilityAndSignals(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	metaDB := filepath.Join(dir, "meta.db")
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd, log := startBinary(t, bin, addr, dataDir, metaDB)

	producer := newClient(t, addr)
	mustCreateTopic(t, ctx, producer, "e2e", 1)
	const n = 40
	for i := range n {
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: "e2e", Value: fmt.Appendf(nil, "e%d", i)}).FirstErr(); err != nil {
			t.Fatalf("produce e%d: %v", i, err)
		}
	}
	producer.Close()

	stopBinary(t, cmd, log) // graceful SIGTERM, must flush.

	// Restart the real binary over the same data dir; the acked data must survive.
	cmd2, log2 := startBinary(t, bin, addr, dataDir, metaDB)
	defer func() { cmd2.Process.Signal(syscall.SIGTERM); cmd2.Wait() }()
	_ = log2

	got := consumeFromStart(t, ctx, addr, "e2e", n)
	for i := range n {
		if want := fmt.Sprintf("e%d", i); got[i] != want {
			t.Fatalf("after real-process restart, record %d = %q, want %q", i, got[i], want)
		}
	}
}
