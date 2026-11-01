package log

import (
	"encoding/binary"
	"testing"
	"time"
)

// The manager's own surface that the broker calls and this package's tests had not: a
// topic's own limits, the durable-ack sync and its counter, a compaction pass and the
// stamp of the broker's time on a batch.
func TestManagerLimitsSyncAndCompactionPass(t *testing.T) {
	m := lifecycleManager(t)
	if err := m.EnsurePartitionDirs("t", 1); err != nil {
		t.Fatal(err)
	}
	p, err := m.ExistingPartition("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(makeBatch(1, []byte("x"))); err != nil {
		t.Fatal(err)
	}

	m.SetTopicLimits("t", TopicLimits{SegmentBytes: 4096, Compaction: CompactionPolicy{Enabled: true}})
	p.mu.Lock()
	got := p.segmentBytes
	p.mu.Unlock()
	if got != 4096 {
		t.Errorf("the topic's segment size is %d, want 4096", got)
	}
	if m.compactionLocked("t").Enabled != true || m.compactionLocked("other").Enabled {
		t.Error("compaction policy is not per topic")
	}
	if err := m.EnforceCompaction(); err != nil {
		t.Errorf("a compaction pass: %v", err)
	}
	if res := p.LastClean(); !res.Ran && res.Complete {
		t.Errorf("last clean = %+v", res)
	}
	m.ClearTopicLimits("t")
	p.mu.Lock()
	got = p.segmentBytes
	p.mu.Unlock()
	if got != 1<<20 {
		t.Errorf("after clearing, the segment size is %d, want the manager's 1 MiB", got)
	}

	before := m.SyncOps()
	if err := m.SyncPartition("t", 0); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncPartition("nobody", 0); err != nil { // not open: nothing to do.
		t.Fatal(err)
	}
	if m.SyncOps() != before+1 {
		t.Errorf("sync ops %d, want %d", m.SyncOps(), before+1)
	}
}

func TestStampLogAppendTimeAndRequireKeys(t *testing.T) {
	b := makeBatch(1, []byte("x"))
	StampLogAppendTime(b, time.Now().UnixMilli())
	if binary.BigEndian.Uint16(b[batchAttributesOff:])&batchLogAppendTimeBit == 0 {
		t.Error("the batch is not marked as carrying the broker's time")
	}
	if got := int64(binary.BigEndian.Uint64(b[batchMaxTimestampOff:])); got == 0 {
		t.Error("the maximum timestamp was not set")
	}
	// A payload cut short is left alone, not mangled.
	StampLogAppendTime(b[:10], 1)
	if err := RequireKeys(b[:10]); err != nil {
		t.Errorf("a short payload: %v", err)
	}
}
