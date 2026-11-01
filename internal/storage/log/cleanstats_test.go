package log

import (
	"testing"
	"time"
)

// TOP-28: the cleaner's passes add up since the partition was opened, the state says
// when one is running, and CleanedTo is where the log has been cleaned to.
func TestCleanStatsAddUpPassesSinceOpen(t *testing.T) {
	p := onePerSegment(t, 64)
	if st := p.CleanStats(); st.Running || st.Last.Ran || st.Since.Passes != 0 || st.Since.RecordsRemoved != 0 {
		t.Fatalf("before any pass: %+v", st)
	}
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}, kv{"c", "1"})) // 0-1
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "2"}))               // 2
	mustAppend(t, p, kvBatch(codecNone, kv{"z", "1"}))               // 3: active.
	ageAll(p)
	t0 := time.Now()
	first := compactAt(t, p, eager, t0.Add(time.Hour))
	if first.RecordsRemoved != 1 {
		t.Fatalf("first pass: %+v", first)
	}
	mustAppend(t, p, kvBatch(codecNone, kv{"c", "2"})) // 4: the previous active is sealed.
	mustAppend(t, p, kvBatch(codecNone, kv{"y", "1"})) // 5: active.
	ageAll(p)
	second := compactAt(t, p, eager, t0.Add(2*time.Hour))
	st := p.CleanStats()
	if st.Since.Passes != 2 || st.Since.RecordsRemoved != first.RecordsRemoved+second.RecordsRemoved {
		t.Fatalf("totals %+v after passes removing %d and %d", st.Since, first.RecordsRemoved, second.RecordsRemoved)
	}
	if st.Last != second || st.Running || st.CleanedTo <= 0 {
		t.Fatalf("state %+v, last pass %+v", st, second)
	}
}
