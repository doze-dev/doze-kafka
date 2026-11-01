//go:build faultinject

package broker_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The whole binary, built with the fault seam, failing on command. The faults
// are armed through a file the broker re-reads on every hit, so one test can
// break the disk and mend it without a restart.
func TestFaultSeam_TheBinaryFailsOnCommand(t *testing.T) {
	skipShort(t)
	bin := filepath.Join(t.TempDir(), "doze-kafka")
	build := exec.Command("go", "build", "-tags", "faultinject", "-o", bin, "github.com/doze-dev/doze-kafka/cmd/doze-kafka")
	build.Env = append(os.Environ(), "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	faults := filepath.Join(t.TempDir(), "faults")
	n := startNode(t, nodeSubprocess(), nodeBinary(bin), nodeEnv("DOZE_FAULTS_FILE="+faults))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(n.Addr), kgo.DisableIdempotentWrite(), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "seam", 1)
	produce := func(v string) error {
		return cl.ProduceSync(ctx, &kgo.Record{Topic: "seam", Value: []byte(v)}).FirstErr()
	}
	arm := func(spec string) {
		t.Helper()
		if err := os.WriteFile(faults, []byte(spec), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if err := produce("before"); err != nil {
		t.Fatalf("healthy: %v", err)
	}

	// A write error: refused as a storage error, and nothing left behind.
	arm("log.write=enospc")
	if err := produce("lost-1"); !errors.Is(err, kerr.KafkaStorageError) {
		t.Fatalf("write fault: %v, want KAFKA_STORAGE_ERROR", err)
	}
	// A sync error: acks=all tells the producer.
	arm("log.sync=error:input/output error")
	if err := produce("lost-2"); !errors.Is(err, kerr.KafkaStorageError) {
		t.Fatalf("sync fault: %v, want KAFKA_STORAGE_ERROR", err)
	}
	// A slow sync: the write is slow and then succeeds.
	arm("log.sync=delay:400ms")
	start := time.Now()
	if err := produce("slow"); err != nil {
		t.Fatalf("slow sync: %v", err)
	}
	if d := time.Since(start); d < 400*time.Millisecond {
		t.Fatalf("a 400ms sync fault answered in %v", d)
	}
	// Mended.
	arm("")
	if err := produce("after"); err != nil {
		t.Fatalf("mended: %v", err)
	}

	// A write that failed leaves nothing behind. One whose sync failed was
	// written and not made durable: the producer was told, so it is allowed to be
	// there (it will be retried), and it is the only thing allowed to be.
	got := consumeFromStart(t, ctx, n.Addr, "seam", 4)
	want := []string{"before", "lost-2", "slow", "after"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("records %v, want %v: a refused write must leave nothing behind", got, want)
		}
	}
}

// BAD-3, through the real binary: after the fsync call itself fails the
// partition is offline for writes, even once the fault is mended, until the
// broker restarts; then it serves what is really on disk and writes resume.
func TestFaultSeam_AFailedFsyncTakesThePartitionOffline(t *testing.T) {
	skipShort(t)
	bin := filepath.Join(t.TempDir(), "doze-kafka")
	build := exec.Command("go", "build", "-tags", "faultinject", "-o", bin, "github.com/doze-dev/doze-kafka/cmd/doze-kafka")
	build.Env = append(os.Environ(), "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	faults := filepath.Join(t.TempDir(), "faults")
	n := startNode(t, nodeSubprocess(), nodeBinary(bin), nodeEnv("DOZE_FAULTS_FILE="+faults))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(n.Addr), kgo.DisableIdempotentWrite(), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "offline", 1)
	produce := func(v string) error {
		return cl.ProduceSync(ctx, &kgo.Record{Topic: "offline", Value: []byte(v)}).FirstErr()
	}
	if err := produce("before"); err != nil {
		t.Fatalf("healthy: %v", err)
	}
	if err := os.WriteFile(faults, []byte("log.fsync=error:input/output error"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := produce("lost"); !errors.Is(err, kerr.KafkaStorageError) {
		t.Fatalf("failing fsync: %v, want KAFKA_STORAGE_ERROR", err)
	}
	// The disk is "mended": the kernel's retry would now succeed. The partition
	// must still refuse.
	if err := os.WriteFile(faults, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := produce("after-mend"); !errors.Is(err, kerr.KafkaStorageError) {
		t.Fatalf("after a failed fsync, with the fault gone: %v, want KAFKA_STORAGE_ERROR until a restart", err)
	}
	// A restart recovers from what is on disk, and writes resume.
	n.Restart()
	cl2, err := kgo.NewClient(kgo.SeedBrokers(n.Addr), kgo.DisableIdempotentWrite(), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()
	if err := cl2.ProduceSync(ctx, &kgo.Record{Topic: "offline", Value: []byte("after-restart")}).FirstErr(); err != nil {
		t.Fatalf("after the restart: %v", err)
	}
}
