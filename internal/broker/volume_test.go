package broker_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// smallVolume mounts a size-limited filesystem of mib MiB and returns its mount
// point, unmounting it when the test ends. The kernel, not a mock, says "no
// space left on device": tmpfs on Linux (which needs root, or passwordless
// sudo, as CI has), a RAM disk formatted HFS+ on macOS (which needs neither).
// Where neither can be made the test is skipped, with the reason.
func smallVolume(t *testing.T, mib int) string {
	t.Helper()
	mnt := filepath.Join(t.TempDir(), "vol")
	if err := os.Mkdir(mnt, 0o755); err != nil {
		t.Fatal(err)
	}
	switch runtime.GOOS {
	case "linux":
		opts := fmt.Sprintf("size=%dm", mib)
		cmd := exec.Command("mount", "-t", "tmpfs", "-o", opts, "doze-test", mnt)
		if out, err := cmd.CombinedOutput(); err != nil {
			cmd = exec.Command("sudo", "-n", "mount", "-t", "tmpfs", "-o", opts, "doze-test", mnt)
			if out2, err2 := cmd.CombinedOutput(); err2 != nil {
				t.Skipf("cannot mount a tmpfs (need root or passwordless sudo): %s / %s", strings.TrimSpace(string(out)), strings.TrimSpace(string(out2)))
			}
		}
		t.Cleanup(func() {
			if exec.Command("umount", mnt).Run() != nil {
				exec.Command("sudo", "-n", "umount", mnt).Run()
			}
		})
	case "darwin":
		out, err := exec.Command("hdiutil", "attach", "-nomount", fmt.Sprintf("ram://%d", mib*2048)).Output()
		if err != nil {
			t.Skipf("cannot create a RAM disk: %v", err)
		}
		dev := strings.TrimSpace(string(out))
		if i := strings.Index(dev, "/dev/"); i >= 0 {
			dev = strings.Fields(dev[i:])[0]
		}
		detach := func() { exec.Command("hdiutil", "detach", "-force", dev).Run() }
		if out, err := exec.Command("newfs_hfs", "-v", "dozevol", dev).CombinedOutput(); err != nil {
			detach()
			t.Skipf("cannot format %s: %v %s", dev, err, out)
		}
		if out, err := exec.Command("mount", "-t", "hfs", "-o", "nobrowse", dev, mnt).CombinedOutput(); err != nil {
			detach()
			t.Skipf("cannot mount %s: %v %s", dev, err, out)
		}
		t.Cleanup(func() {
			exec.Command("umount", "-f", mnt).Run()
			detach()
		})
	default:
		t.Skipf("no size-limited volume helper for %s", runtime.GOOS)
	}
	return mnt
}

// A full disk is the most ordinary way a development machine goes wrong. The
// broker must say so (KAFKA_STORAGE_ERROR, not a hang or a dropped connection),
// keep serving every record it already acknowledged, and leave no trace of the
// write it refused. The disk is a real 12 MiB volume, not a mock.
func TestFullVolume_ReadsKeepWorkingAndWritesAreRefusedAsStorageErrors(t *testing.T) {
	mnt := smallVolume(t, 12)
	n := startNode(t, nodeDir(mnt))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(n.Addr), kgo.DisableIdempotentWrite(), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(0), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.ProduceRequestTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "fill", 1)

	payload := make([]byte, 64<<10)
	for i := range payload {
		payload[i] = byte(rand.Intn(256))
	}
	var acked int
	var failure error
	for i := 0; i < 400; i++ {
		payload[0], payload[1] = byte(i), byte(i>>8)
		rec := &kgo.Record{Topic: "fill", Value: append([]byte(nil), payload...)}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			failure = err
			break
		}
		acked++
	}
	if failure == nil {
		t.Fatalf("400 records of 64 KiB (25 MiB) were all acknowledged by a 12 MiB volume")
	}
	if !errors.Is(failure, kerr.KafkaStorageError) {
		t.Fatalf("the write that did not fit failed with %v, want KAFKA_STORAGE_ERROR", failure)
	}
	t.Logf("%d records acknowledged before the volume was full: %v", acked, failure)
	if acked < 20 {
		t.Fatalf("only %d records fitted a 12 MiB volume", acked)
	}

	// Writes keep being refused, and still with the same code.
	for range 3 {
		err := cl.ProduceSync(ctx, &kgo.Record{Topic: "fill", Value: payload}).FirstErr()
		if !errors.Is(err, kerr.KafkaStorageError) {
			t.Fatalf("a write to a full volume: %v, want KAFKA_STORAGE_ERROR", err)
		}
	}

	// Reads: every acknowledged record is still there, and the broker answers
	// offsets as usual.
	got := consumeFromStart(t, ctx, n.Addr, "fill", acked)
	for i, v := range got {
		if len(v) != len(payload) || v[0] != byte(i) || v[1] != byte(i>>8) {
			t.Fatalf("record %d came back different after the volume filled", i)
		}
	}
	if end := listOffset(t, ctx, cl, "fill", 0, -1); end != int64(acked) {
		t.Fatalf("log end offset %d, want %d: a refused write must leave no trace", end, acked)
	}

	// And a restart over the same full volume still serves it.
	n.Restart()
	if got := consumeFromStart(t, ctx, n.Addr, "fill", acked); len(got) != acked {
		t.Fatalf("after a restart on a full volume: %d records, want %d", len(got), acked)
	}
}
