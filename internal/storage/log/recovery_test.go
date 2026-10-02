package log

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

const (
	crashBatches  = 20
	crashRecords  = 3 // records per batch, so batch i holds offsets [3i, 3i+3).
	crashPayload  = 40
	crashInterval = 200 // index entry roughly every other batch.
)

func crashOpts() ManagerOptions {
	return ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: crashInterval}
}

// buildCrashLog writes crashBatches batches, closes cleanly, and returns the
// partition directory with each batch's start position and total size.
func buildCrashLog(t *testing.T) (dir string, starts []int64, size int64) {
	t.Helper()
	dir = filepath.Join(t.TempDir(), "t-0")
	p, err := openPartition(dir, crashOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < crashBatches; i++ {
		starts = append(starts, p.active().sizeBytes)
		if _, err := p.Append(makeBatch(crashRecords, bytes.Repeat([]byte{byte('a' + i%26)}, crashPayload))); err != nil {
			t.Fatal(err)
		}
	}
	size = p.active().sizeBytes
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	return dir, starts, size
}

func segFiles(dir string) (logPath, indexPath string) {
	base := filepath.Join(dir, segmentName(0))
	return base + ".log", base + ".index"
}

func mutateFile(t *testing.T, path string, fn func(b []byte) []byte) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, fn(b), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestRecoveryCrashMatrix damages the log or its index the ways a crash or a bad
// disk does, then opens it. Whatever the damage, the result must be a log that
// (a) kept every batch before the damage and none after, (b) serves every
// retained offset from the right batch, and (c) takes new appends at the right
// offset — and keeps doing so after another reopen.
func TestRecoveryCrashMatrix(t *testing.T) {
	cases := []struct {
		name string
		keep int // batches expected to survive.
		// damage mutates the files; starts are batch start positions, size the log size.
		damage func(t *testing.T, logPath, indexPath string, starts []int64, size int64)
	}{
		{"torn body", crashBatches - 1, func(t *testing.T, lp, _ string, _ []int64, size int64) {
			if err := os.Truncate(lp, size-10); err != nil {
				t.Fatal(err)
			}
		}},
		{"torn header", crashBatches - 1, func(t *testing.T, lp, _ string, starts []int64, _ int64) {
			if err := os.Truncate(lp, starts[crashBatches-1]+20); err != nil {
				t.Fatal(err)
			}
		}},
		{"zero-filled tail", crashBatches, func(t *testing.T, lp, _ string, _ []int64, _ int64) {
			mutateFile(t, lp, func(b []byte) []byte { return append(b, make([]byte, 4096)...) })
		}},
		{"garbage tail", crashBatches, func(t *testing.T, lp, _ string, _ []int64, _ int64) {
			mutateFile(t, lp, func(b []byte) []byte { return append(b, bytes.Repeat([]byte{0xA5, 0x5A, 0xFF}, 200)...) })
		}},
		{"huge batchLength on the last batch", crashBatches - 1, func(t *testing.T, lp, _ string, starts []int64, _ int64) {
			mutateFile(t, lp, func(b []byte) []byte {
				binary.BigEndian.PutUint32(b[starts[crashBatches-1]+batchLengthOff:], 0x7fffffff)
				return b
			})
		}},
		{"bad CRC on the last batch", crashBatches - 1, func(t *testing.T, lp, _ string, starts []int64, size int64) {
			mutateFile(t, lp, func(b []byte) []byte { b[size-5] ^= 0xFF; return b })
		}},
		{"bad CRC mid-tail drops everything after it", crashBatches - 3, func(t *testing.T, lp, _ string, starts []int64, _ int64) {
			mutateFile(t, lp, func(b []byte) []byte { b[starts[crashBatches-3]+batchFullHeaderBytes+5] ^= 0xFF; return b })
		}},
		{"wrong magic on the last batch", crashBatches - 1, func(t *testing.T, lp, _ string, starts []int64, _ int64) {
			mutateFile(t, lp, func(b []byte) []byte { b[starts[crashBatches-1]+batchMagicOff] = 1; return b })
		}},
		{"zero record count on the last batch", crashBatches - 1, func(t *testing.T, lp, _ string, starts []int64, _ int64) {
			mutateFile(t, lp, func(b []byte) []byte {
				binary.BigEndian.PutUint32(b[starts[crashBatches-1]+batchLastOffsetDeltaOff:], 0xFFFFFFFF)
				return b
			})
		}},

		// The log is intact; only the index is wrong.
		{"index missing", crashBatches, func(t *testing.T, _, ip string, _ []int64, _ int64) {
			if err := os.Remove(ip); err != nil {
				t.Fatal(err)
			}
		}},
		{"index empty", crashBatches, func(t *testing.T, _, ip string, _ []int64, _ int64) {
			if err := os.Truncate(ip, 0); err != nil {
				t.Fatal(err)
			}
		}},
		{"index with a partial trailing entry", crashBatches, func(t *testing.T, _, ip string, _ []int64, _ int64) {
			mutateFile(t, ip, func(b []byte) []byte { return b[:len(b)-5] })
		}},
		{"index entry pointing past the end of the log", crashBatches, func(t *testing.T, _, ip string, _ []int64, _ int64) {
			mutateFile(t, ip, func(b []byte) []byte {
				e := make([]byte, indexEntryBytes)
				binary.BigEndian.PutUint64(e[0:], 999)
				binary.BigEndian.PutUint64(e[8:], 999999)
				return append(b, e...)
			})
		}},
		{"index entry pointing into the middle of a batch", crashBatches, func(t *testing.T, _, ip string, _ []int64, _ int64) {
			mutateFile(t, ip, func(b []byte) []byte {
				last := len(b) - 8
				binary.BigEndian.PutUint64(b[last:], binary.BigEndian.Uint64(b[last:])+7)
				return b
			})
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir, starts, size := buildCrashLog(t)
			lp, ip := segFiles(dir)
			tc.damage(t, lp, ip, starts, size)

			p, err := openPartition(dir, crashOpts())
			if err != nil {
				t.Fatalf("open after damage: %v", err)
			}
			wantLEO := int64(tc.keep * crashRecords)
			if got := p.LogEndOffset(); got != wantLEO {
				t.Fatalf("LEO = %d, want %d (%d batches kept)", got, wantLEO, tc.keep)
			}
			checkEveryOffset(t, p, wantLEO)

			// New appends continue exactly where the surviving log ends.
			off, err := p.Append(makeBatch(crashRecords, []byte("after-recovery")))
			if err != nil || off != wantLEO {
				t.Fatalf("Append after recovery = %d, %v; want offset %d", off, err, wantLEO)
			}
			checkEveryOffset(t, p, wantLEO+crashRecords)
			if err := p.close(); err != nil {
				t.Fatal(err)
			}

			// And the repaired log stays repaired across another open.
			p2, err := openPartition(dir, crashOpts())
			if err != nil {
				t.Fatalf("second open: %v", err)
			}
			defer p2.close()
			if got := p2.LogEndOffset(); got != wantLEO+crashRecords {
				t.Fatalf("LEO after reopening = %d, want %d", got, wantLEO+crashRecords)
			}
			checkEveryOffset(t, p2, wantLEO+crashRecords)
		})
	}
}

// checkEveryOffset reads each offset below leo and verifies the batch returned
// is the one that contains it. A stale index shows up here: a position pointing
// mid-batch or past the end yields an empty read or misaligned bytes.
func checkEveryOffset(t *testing.T, p *Partition, leo int64) {
	t.Helper()
	for o := int64(0); o < leo; o++ {
		data, err := p.Read(o, 1<<20)
		if err != nil || len(data) == 0 {
			t.Fatalf("Read(%d) = %d bytes, %v; want the batch containing it", o, len(data), err)
		}
		info, ok := parseHeader(data)
		if !ok || info.baseOffset > o || o >= info.baseOffset+int64(info.recordCount) {
			t.Fatalf("Read(%d) returned the batch at offsets [%d,%d), which does not contain it", o, info.baseOffset, info.baseOffset+int64(info.recordCount))
		}
		if _, err := checkBatchShape(data[:info.totalBytes]); err != nil || !crcOK(data[:info.totalBytes]) {
			t.Fatalf("Read(%d) returned a batch that fails validation: %v", o, err)
		}
	}
	if data, err := p.Read(leo, 1<<20); err != nil || len(data) != 0 {
		t.Fatalf("Read at the log end = %d bytes, %v; want empty", len(data), err)
	}
}

// ValidateBatches is what keeps a bad batch out of the log in the first place.
func TestValidateBatches(t *testing.T) {
	good := makeBatch(2, []byte("payload"))
	two := append(append([]byte{}, good...), makeBatch(1, []byte("x"))...)

	if err := ValidateBatches(nil); err != nil {
		t.Errorf("an empty payload should be fine: %v", err)
	}
	if err := ValidateBatches(good); err != nil {
		t.Errorf("a valid batch: %v", err)
	}
	if err := ValidateBatches(two); err != nil {
		t.Errorf("two valid batches: %v", err)
	}

	bad := map[string]func() []byte{
		"flipped payload byte": func() []byte { b := append([]byte{}, good...); b[len(b)-1] ^= 1; return b },
		"flipped header byte":  func() []byte { b := append([]byte{}, good...); b[batchAttributesOff] ^= 1; return b },
		"wrong magic":          func() []byte { b := append([]byte{}, good...); b[batchMagicOff] = 1; return b },
		"zero records": func() []byte {
			b := append([]byte{}, good...)
			binary.BigEndian.PutUint32(b[batchLastOffsetDeltaOff:], 0xFFFFFFFF)
			sealCRC(b)
			return b
		},
		"count disagrees": func() []byte {
			b := append([]byte{}, good...)
			binary.BigEndian.PutUint32(b[batchRecordCountOff:], 9)
			sealCRC(b)
			return b
		},
		"truncated":           func() []byte { return append([]byte{}, good[:len(good)-3]...) },
		"trailing junk":       func() []byte { return append(append([]byte{}, good...), 1, 2, 3) },
		"second batch is bad": func() []byte { b := append([]byte{}, two...); b[len(b)-1] ^= 1; return b },
		"shorter than header": func() []byte { return good[:30] },
	}
	for name, mk := range bad {
		if err := ValidateBatches(mk()); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
