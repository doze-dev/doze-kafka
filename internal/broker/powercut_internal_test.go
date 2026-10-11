package broker

// BAD-13 of plan/stages/bad-days.md: a power-cut model, which SIGKILL cannot be.

import (
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// powerCut models a power loss: every .log and .index file keeps only what was
// fsynced. It sees each fsync through log.SetFsyncForTest and records the file's
// length just before the sync (what that fsync is sure to have made durable).
type powerCut struct {
	mu     sync.Mutex
	synced map[string]int64
}

func newPowerCut(t *testing.T) *powerCut {
	pc := &powerCut{synced: map[string]int64{}}
	t.Cleanup(log.SetFsyncForTest(func(f *os.File) error {
		st, err := f.Stat()
		if err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		pc.mu.Lock()
		pc.synced[f.Name()] = max(pc.synced[f.Name()], st.Size())
		pc.mu.Unlock()
		return nil
	}))
	return pc
}

// cut copies dir to a new directory, applying the power loss: each log and index
// file is cut to its last fsynced length. mode "truncate" drops the unsynced
// tail, "zero" keeps the file's length with the unsynced bytes zeroed, "prefix"
// keeps a random part of the unsynced tail (pages reach the disk in any order and
// in part).
func (pc *powerCut) cut(t *testing.T, dir, mode string, rng *rand.Rand) string {
	t.Helper()
	pc.mu.Lock()
	defer pc.mu.Unlock()
	out := t.TempDir()
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		dst := filepath.Join(out, rel)
		if info.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.HasSuffix(path, ".log") || strings.HasSuffix(path, ".index") {
			keep := min(pc.synced[path], int64(len(raw)))
			switch mode {
			case "zero":
				for i := keep; i < int64(len(raw)); i++ {
					raw[i] = 0
				}
			case "prefix":
				raw = raw[:keep+rng.Int63n(int64(len(raw))-keep+1)]
			default:
				raw = raw[:keep]
			}
		}
		return os.WriteFile(dst, raw, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// BAD-13. SIGKILL keeps the page cache, so a kill test passes whether or not
// acks=all fsyncs. A power cut does not: this one keeps only what was fsynced and
// then reopens the log. Every acks=all record must be there; an acks=1 record
// survives only if some later sync happened to cover it.
func TestPowerCutKeepsEveryAcksAllRecord(t *testing.T) {
	for _, mode := range []string{"truncate", "zero", "prefix"} {
		t.Run(mode, func(t *testing.T) {
			pc := newPowerCut(t)
			dir := t.TempDir()
			metaStore, err := meta.Open(filepath.Join(dir, "meta.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer metaStore.Close()
			data := filepath.Join(dir, "data")
			logs, err := log.OpenManager(data, log.ManagerOptions{SegmentBytes: 600, IndexIntervalBytes: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer logs.Close()
			cfg := config.Default()
			cfg.FsyncInterval = 0
			b, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), logs, metaStore)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := metaStore.CreateTopic("t", 1, nil); err != nil {
				t.Fatal(err)
			}
			if err := logs.EnsurePartitionDirs("t", 1); err != nil {
				t.Fatal(err)
			}

			// Interleave: plain and idempotent, acks=1 and acks=all, across segment
			// rolls (600 bytes holds about nine of these batches).
			var lastAll int64 = -1
			var seq int32
			for i := 0; i < 40; i++ {
				var records []byte
				idem := i%2 == 0
				if idem {
					records = idemBatch(11, 0, seq, 1, 0)
					seq++
				} else {
					records = plainBatch()
				}
				acks := int16(1)
				if i%3 == 0 {
					acks = -1
				}
				p := produceOnce(t, b, acks, records).Topics[0].Partitions[0]
				if p.ErrorCode != 0 {
					t.Fatalf("produce %d: code %d", i, p.ErrorCode)
				}
				if acks == -1 {
					lastAll = p.BaseOffset
				}
			}
			// Then a tail of acks=1 records and an acks=1 first send whose acks=all
			// retry is acknowledged (BAD-1 through the power cut).
			for i := 0; i < 3; i++ {
				produceOnce(t, b, 1, plainBatch())
			}
			produceOnce(t, b, 1, idemBatch(12, 0, 0, 1, 0))
			p := produceOnce(t, b, -1, idemBatch(12, 0, 0, 1, 0)).Topics[0].Partitions[0]
			if p.ErrorCode != 0 {
				t.Fatalf("duplicate retry: code %d", p.ErrorCode)
			}
			lastAll = max(lastAll, p.BaseOffset)
			end := func() int64 { part, _ := logs.Partition("t", 0); return part.LogEndOffset() }()

			cut := pc.cut(t, data, mode, rand.New(rand.NewSource(7)))
			reopened, err := log.OpenManager(cut, log.ManagerOptions{SegmentBytes: 600, IndexIntervalBytes: 1})
			if err != nil {
				t.Fatalf("reopen after a power cut: %v", err)
			}
			defer reopened.Close()
			part, err := reopened.Partition("t", 0)
			if err != nil {
				t.Fatal(err)
			}
			got := part.LogEndOffset()
			if got < lastAll+1 {
				t.Fatalf("after the power cut the log ends at %d, but an acks=all record was acknowledged at offset %d (of %d written): acknowledged data was lost", got, lastAll, end)
			}
			if mode == "truncate" && got != lastAll+1 {
				t.Errorf("log ends at %d, want exactly %d: acks=1 records after the last sync should be gone", got, lastAll+1)
			}
			// And what is left reads back whole.
			if _, err := part.Read(0, 1<<22); err != nil {
				t.Fatalf("read after recovery: %v", err)
			}
		})
	}
}
