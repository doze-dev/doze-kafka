package log

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/powercut"
)

// latestByKey reads the whole partition and returns, for every key, the offset
// and value of its latest record: what compaction must never change. An error is
// a read that failed.
func latestByKey(p *Partition) (map[string]row, error) {
	out := map[string]row{}
	next := p.LogStartOffset()
	for next < p.LogEndOffset() {
		data, err := p.Read(next, 1<<20)
		if err != nil {
			return nil, fmt.Errorf("read at %d: %w", next, err)
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("read at %d returned nothing before the end %d", next, p.LogEndOffset())
		}
		for pos := 0; pos < len(data); {
			info, ok := parseHeader(data[pos:])
			if !ok {
				return nil, fmt.Errorf("a batch at %d cannot be parsed", next)
			}
			batch := data[pos : pos+int(info.totalBytes)]
			if attrsOf(batch)&batchControlBit == 0 {
				recs, err := DecodeRecords(batch)
				if err != nil {
					return nil, err
				}
				for _, r := range recs {
					if off := info.baseOffset + int64(r.OffsetDelta); off >= next {
						out[string(r.Key)] = row{off, string(r.Key), string(r.Value)}
					}
				}
			}
			next = info.baseOffset + int64(info.recordCount)
			pos += int(info.totalBytes)
		}
	}
	return out, nil
}

// swapFixture is a partition with superseded records in sealed segments, ready to clean.
func swapFixture(t *testing.T, dir string) *Partition {
	t.Helper()
	pad := strings.Repeat("p", 120)
	p := smallPartition(t, dir)
	// The first segment keeps one record of three (c), so its swap writes a
	// cleaned log with something in it; the second is cleaned away entirely.
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}, kv{"b", "1"}, kv{"c", "1" + pad}))
	sealSome(t, p, 1)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "2" + pad}, kv{"d", "1" + pad}))
	sealSome(t, p, 3)
	mustAppend(t, p, kvBatch(codecNone, kv{"b", "2" + pad}, kv{"d", "2" + pad}))
	sealSome(t, p, 3)
	return p
}

// F5. A cleaning pass that fails at any one operation on the file system leaves a
// log that reads, with every key's latest record where it was, without a restart;
// the next pass succeeds; and the log opens again afterwards with the same
// records. The swap used to close the old segment first and return with it closed
// in the list on any later failure: every read of it failed ("file already
// closed"), and so did every later pass, until a restart.
func TestACleaningPassThatFailsAtAnyStepLeavesALogThatReads(t *testing.T) {
	// The operations a successful pass makes, in order.
	var ops []FSEvent
	{
		dir := t.TempDir()
		p := swapFixture(t, dir)
		restore := SetFSHookForTest(func(e FSEvent) error {
			if !e.Done {
				ops = append(ops, e)
			}
			return nil
		})
		compact(t, p, eager)
		restore()
		p.close()
	}
	if len(ops) == 0 {
		t.Fatal("a cleaning pass made no file operations")
	}
	swapSteps := 0
	seen := map[string]int{}
	for k, op := range ops {
		if op.Op == FSRename || strings.HasSuffix(op.Path, swapMarkExt) {
			swapSteps++
		}
		key := opKey(op)
		occurrence := seen[key]
		seen[key]++
		t.Run(fmt.Sprintf("%02d-%s-%s", k, op.Op, shortName(op.Path)), func(t *testing.T) {
			dir := t.TempDir()
			p := swapFixture(t, dir)
			defer func() { p.close() }()
			want, err := latestByKey(p)
			if err != nil {
				t.Fatal(err)
			}
			hits, failed := 0, false
			restore := SetFSHookForTest(func(e FSEvent) error {
				if e.Done || opKey(e) != key {
					return nil
				}
				hits++
				if hits == occurrence+1 {
					failed = true
					return &fsError{op: string(e.Op), err: syscall.ENOSPC}
				}
				return nil
			})
			_, cerr := p.Compact(eager, time.Now().Add(time.Hour))
			restore()
			if !failed {
				t.Fatalf("the pass never made the operation the recorded pass made")
			}
			got, err := latestByKey(p)
			if err != nil {
				t.Fatalf("after a pass that failed at %s %s (%v): %v", op.Op, op.Path, cerr, err)
			}
			if !sameRows(got, want) {
				t.Fatalf("after a pass that failed at %s: latest records %v, want %v", op.Op, got, want)
			}
			if _, err := p.Compact(eager, time.Now().Add(time.Hour)); err != nil && !errors.Is(err, ErrStorageOffline) {
				t.Fatalf("the next pass, with nothing failing: %v", err)
			}
			if got, err := latestByKey(p); err != nil || !sameRows(got, want) {
				t.Fatalf("after the next pass: %v, %v", got, err)
			}
			p.close()
			p = smallPartition(t, dir)
			if got, err := latestByKey(p); err != nil || !sameRows(got, want) {
				t.Fatalf("after a reopen: %v, %v", got, err)
			}
		})
	}
	if swapSteps < 3 {
		t.Fatalf("the pass made %d swap steps (the marker and two renames expected): %v", swapSteps, ops)
	}
}

type fsError struct {
	op  string
	err error
}

func (e *fsError) Error() string { return "injected " + e.op + ": " + e.err.Error() }
func (e *fsError) Unwrap() error { return e.err }

// opKey names an operation the same way in two runs over different directories.
func opKey(e FSEvent) string { return string(e.Op) + " " + shortName(e.Path) + " " + shortName(e.To) }

func shortName(path string) string {
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		return path[i+1:]
	}
	return path
}

func sameRows(a, b map[string]row) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// F5, F6. The power goes right after any one operation of a cleaning pass: the
// log that the next start finds reads, and has every key's latest record where
// it was, under every way the unflushed bytes can be lost.
func TestAPowerCutAtAnyStepOfACleaningPassLosesNothing(t *testing.T) {
	// How many operations a pass starts.
	total := 0
	{
		p := swapFixture(t, t.TempDir())
		restore := SetFSHookForTest(func(e FSEvent) error {
			if !e.Done {
				total++
			}
			return nil
		})
		compact(t, p, eager)
		restore()
		p.close()
	}
	for _, mode := range powercut.Modes {
		for k := 1; k <= total; k++ {
			t.Run(fmt.Sprintf("%s-after-%02d", mode, k), func(t *testing.T) {
				root := t.TempDir()
				p := swapFixture(t, filepath.Join(root, "t-0"))
				defer p.close()
				if err := p.Sync(); err != nil {
					t.Fatal(err)
				}
				want, err := latestByKey(p)
				if err != nil {
					t.Fatal(err)
				}
				pc := powercut.New(root)
				pc.AssumeDurable()
				restore := SetFSHookForTest(func(ev FSEvent) error {
					return pc.Observe(string(ev.Op), ev.Path, ev.To, ev.Done)
				})
				pc.CutAfter(k)
				compact(t, p, eager)
				restore()
				kept := pc.Kept()
				if kept == nil {
					// The operation did not complete (the removal of a file that was
					// not there): the disk is as it was after the one before.
					t.Skipf("operation %d did not complete", k)
				}
				cut, err := kept.Cut(t.TempDir(), mode, rand.New(rand.NewSource(int64(k))))
				if err != nil {
					t.Fatal(err)
				}
				after, err := openPartition(filepath.Join(cut, "t-0"), ManagerOptions{SegmentBytes: 400, IndexIntervalBytes: 64})
				if err != nil {
					t.Fatalf("open after the cut: %v", err)
				}
				defer after.close()
				got, err := latestByKey(after)
				if err != nil {
					t.Fatalf("after the cut: %v", err)
				}
				if !sameRows(got, want) {
					t.Fatalf("after the cut: latest records %v, want %v", got, want)
				}
			})
		}
	}
}

// F5 on a real volume with no room: DOZE_FULL_VOLUME_DIR names an empty directory
// on a small volume of its own (macOS: hdiutil create -size 24m -fs APFS ...;
// Linux: a tmpfs). The fixture is made, the volume is filled to the last byte,
// and a cleaning pass runs: whatever it answers, the log reads; once the filler
// is gone the next pass succeeds and the log opens again the same.
func TestACleaningPassOnAFullVolumeLeavesALogThatReads(t *testing.T) {
	root := os.Getenv("DOZE_FULL_VOLUME_DIR")
	if root == "" {
		t.Skip("set DOZE_FULL_VOLUME_DIR to a directory on a small volume of its own")
	}
	dir := filepath.Join(root, "t-0")
	p := swapFixture(t, dir)
	defer func() { p.close() }()
	want, err := latestByKey(p)
	if err != nil {
		t.Fatal(err)
	}
	// The volume is filled at the moment the pass starts its swap (the cleaned
	// files are written by then): the swap's own steps meet a real ENOSPC.
	filler := filepath.Join(root, "filler")
	fill := func() {
		f, err := os.Create(filler)
		if err != nil {
			return
		}
		defer f.Close()
		for _, chunk := range []int{1 << 20, 4 << 10, 1} {
			buf := make([]byte, chunk)
			for i := 0; i < 1<<16; i++ {
				if _, err := f.Write(buf); err != nil {
					break
				}
			}
		}
		// And a few empty files, until the directory has no room for another name.
		for i := 0; i < 10000; i++ {
			g, err := os.Create(filepath.Join(root, fmt.Sprintf("name-%d", i)))
			if err != nil {
				break
			}
			g.Close()
		}
	}
	filled := false
	restore := SetFSHookForTest(func(e FSEvent) error {
		if !e.Done && !filled && strings.HasSuffix(e.Path, swapMarkExt) {
			filled = true
			fill()
		}
		return nil
	})
	defer restore()
	defer os.Remove(filler)
	_, cerr := p.Compact(eager, time.Now().Add(time.Hour))
	t.Logf("the pass on the full volume: %v", cerr)
	if cerr == nil {
		t.Log("the volume still took every step of the swap (APFS keeps a reserve); this run proves nothing about a failed swap")
	}
	if got, err := latestByKey(p); err != nil || !sameRows(got, want) {
		t.Fatalf("after the pass on the full volume: %v, %v", got, err)
	}
	restore()
	if !filled {
		t.Fatal("the pass never reached its swap")
	}
	if err := os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Compact(eager, time.Now().Add(time.Hour)); err != nil && !errors.Is(err, ErrStorageOffline) {
		t.Fatalf("the next pass, with room again: %v", err)
	}
	if got, err := latestByKey(p); err != nil || !sameRows(got, want) {
		t.Fatalf("after the next pass: %v, %v", got, err)
	}
	p.close()
	p = smallPartition(t, dir)
	if got, err := latestByKey(p); err != nil || !sameRows(got, want) {
		t.Fatalf("after a reopen: %v, %v", got, err)
	}
}
