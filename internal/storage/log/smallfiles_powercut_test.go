package log

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/powercut"
)

// F6. A log start that DeleteRecords has answered for is still the log start
// after a power cut, in every mode: the records below it stay deleted. The file
// that holds it is replaced through a temporary file that is fsynced, renamed and
// whose directory is fsynced; without either flush the cut brings back the old
// start (or none), and the deleted records with it.
func TestALogStartThatWasAnsweredSurvivesAPowerCut(t *testing.T) {
	for _, mode := range powercut.Modes {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			pc := watchDisk(t, root)
			opts := ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 64}
			p, err := openPartition(filepath.Join(root, "t-0"), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer p.close()
			for range 4 {
				mustAppend(t, p, makeBatch(1, []byte(strings.Repeat("x", 40))))
			}
			if err := p.Sync(); err != nil {
				t.Fatal(err)
			}
			for _, start := range []int64{1, 3} {
				if got, err := p.DeleteBefore(start); err != nil || got != start {
					t.Fatalf("DeleteBefore(%d): %d, %v", start, got, err)
				}
				after := afterCut(t, pc, "t-0", mode, opts)
				if got := after.LogStartOffset(); got != start {
					t.Fatalf("DeleteRecords to %d was answered, the power went, and the log starts at %d", start, got)
				}
			}
		})
	}
}

// F6. A partition that holds a transaction still knows it after a power cut: the
// open transaction's records stay above the last stable offset. The mark that
// says "scan for transactions" is a file whose name has to be on disk before the
// first transactional batch is; without the directory flush the cut keeps the
// batch and loses the mark, and a read-committed consumer is handed the open
// transaction's records.
func TestATransactionalPartitionIsStillOneAfterAPowerCut(t *testing.T) {
	for _, mode := range powercut.Modes {
		t.Run(string(mode), func(t *testing.T) {
			root := t.TempDir()
			pc := watchDisk(t, root)
			opts := ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 64}
			p, err := openPartition(filepath.Join(root, "t-0"), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer p.close()
			mustAppend(t, p, makeBatch(1, []byte("plain")))
			if err := p.Sync(); err != nil { // the partition and its directory are on disk.
				t.Fatal(err)
			}
			mustAppend(t, p, txKVBatch(7, 0, kv{"k", "open"}))
			if err := p.Sync(); err != nil { // what acks=all does.
				t.Fatal(err)
			}
			after := afterCut(t, pc, "t-0", mode, opts)
			if after.LogEndOffset() != 2 {
				t.Fatalf("setup: the log ends at %d after the cut, want 2", after.LogEndOffset())
			}
			_, lso, _, err := after.ReadIsolated(0, 1<<20, true)
			if err != nil {
				t.Fatal(err)
			}
			if lso != 1 {
				t.Fatalf("after the cut the last stable offset is %d, want 1: the open transaction at offset 1 is visible to read_committed", lso)
			}
		})
	}
}
