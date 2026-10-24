package console

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func waitJob(t *testing.T, j *traceJob) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		j.mu.Lock()
		done := j.phase == "done"
		j.mu.Unlock()
		if done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the scan did not finish")
}

func TestGroupDigitsAtEveryRemainder(t *testing.T) {
	for in, want := range map[int64]string{100000: "100,000", 123456: "123,456", 999999: "999,999", 1000000: "1,000,000", 12345678: "12,345,678"} {
		if got := groupDigits(in); got != want {
			t.Errorf("groupDigits(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestPreviewKey(t *testing.T) {
	forty := strings.Repeat("é", 40)
	for _, tc := range []struct {
		in   []byte
		want string
	}{
		{nil, ""},
		{[]byte("abc"), "abc"},
		{[]byte(forty), forty},
		{[]byte(forty + "x"), forty + "…"},
		{[]byte{0xff, 0xfe, 0xfd}, "3 bytes"},
	} {
		if got := previewKey(tc.in); got != tc.want {
			t.Errorf("previewKey(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// The hits come out oldest first and, at one instant, by topic, partition and offset;
// each is placed along the time axis; each group's column says whether it has read past.
func TestFinishTraceOrdersLaysOutAndMarksReaders(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "ta", 2, 3)
	commitFor(t, addr, "g1", "ta", map[int32]int64{0: 2})
	T := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	hits := []traceHit{
		{Topic: "ta", Partition: 0, Offset: 9, Time: T.Add(time.Second)},
		{Topic: "tb", Partition: 0, Offset: 5, Time: T},
		{Topic: "ta", Partition: 1, Offset: 0, Time: T},
		{Topic: "ta", Partition: 0, Offset: 2, Time: T},
		{Topic: "zz", Partition: 0, Offset: 0, Time: T.Add(500 * time.Millisecond)},
		{Topic: "ta", Partition: 0, Offset: 1, Time: T},
	}
	j := &traceJob{}
	c.finishTrace(context.Background(), j, hits)
	var order []string
	for _, h := range j.hits {
		order = append(order, fmt.Sprintf("%s/%d/%d", h.Topic, h.Partition, h.Offset))
	}
	if got, want := strings.Join(order, " "), "ta/0/1 ta/0/2 ta/1/0 tb/0/5 zz/0/0 ta/0/9"; got != want {
		t.Fatalf("order %s, want %s", got, want)
	}
	left := []int{0, 0, 0, 0, 48, 96}
	delta := []string{"start", "+0 ms", "+0 ms", "+0 ms", "+500 ms", "+1.0 s"}
	for i, h := range j.hits {
		if h.Left != left[i] || h.Delta != delta[i] {
			t.Errorf("hit %d (%s/%d/%d): left %d delta %q, want %d %q", i, h.Topic, h.Partition, h.Offset, h.Left, h.Delta, left[i], delta[i])
		}
	}
	// g1 committed offset 2 on ta/0: it has read offset 1, not offset 2.
	if cs := j.hits[0].Consumers; len(cs) != 1 || cs[0].Group != "g1" || !cs[0].Past {
		t.Errorf("offset 1 under a commit of 2: %+v", cs)
	}
	if cs := j.hits[1].Consumers; len(cs) != 1 || cs[0].Past {
		t.Errorf("offset 2 under a commit of 2: %+v", cs)
	}
	if cs := j.hits[3].Consumers; len(cs) != 0 {
		t.Errorf("a partition nobody committed on has consumers %+v", cs)
	}
	// One hit: no division by a zero span.
	one := &traceJob{}
	c.finishTrace(context.Background(), one, []traceHit{{Topic: "ta", Time: T}})
	if h := one.hits[0]; h.Left != 0 || h.Delta != "start" {
		t.Errorf("a single hit: %+v", h)
	}
}

// A window with nothing in it plans nothing; a window with nothing after it reads to the end.
func TestPlanTraceRanges(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	for _, n := range []string{"after", "inside"} {
		if err := c.be.CreateTopic(ctx, n, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Now().Add(-3 * time.Hour).Truncate(time.Millisecond)
	bulkProduce(t, cl, "after", 0, 4, vals("a"), at.Add(2*time.Hour))
	bulkProduce(t, cl, "inside", 0, 4, vals("i"), at.Add(time.Minute))
	plan, err := c.planTrace(ctx, at, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range plan {
		got = append(got, fmt.Sprintf("%s/%d:[%d,%d)", r.Topic, r.Partition, r.From, r.To))
	}
	if strings.Join(got, " ") != "inside/0:[0,4)" {
		t.Errorf("plan %v, want only inside/0:[0,4)", got)
	}
}

// How the scan is budgeted: whole ranges, smallest first and the starting record's
// partition before them; what is cut is named.
func TestTraceBudgetCutsSmallestFirstAndNamesTheRest(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	for _, n := range []string{"small", "large", "even1", "even2"} {
		if err := c.be.CreateTopic(ctx, n, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().Truncate(time.Millisecond)
	prod := func(topic string, n int) {
		recs := make([]*kgo.Record, n)
		for i := range recs {
			recs[i] = &kgo.Record{Topic: topic, Value: []byte("v"), Timestamp: now.Add(-time.Duration(n-i) * time.Second), Headers: tp(traceA)}
		}
		if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	prod("small", 3)
	prod("large", 8)
	prod("even1", 5)
	prod("even2", 5)
	run := func(budget int, anchor tpOffset, window time.Duration) *traceJob {
		old := traceMaxRecords
		traceMaxRecords = budget
		defer func() { traceMaxRecords = old }()
		j := c.startTrace(traceA, anchor, now, window)
		waitJob(t, j)
		return j
	}
	unread := func(j *traceJob) string {
		j.mu.Lock()
		defer j.mu.Unlock()
		u := append([]string(nil), j.unread...)
		sort.Strings(u)
		return strings.Join(u, "; ")
	}
	// Two ranges of 5 and 5 and 3 and 8, budget 21: everything fits exactly, nothing is left.
	j := run(21, tpOffset{}, 3*time.Hour)
	if j.stopped != "" || unread(j) != "" || len(j.hits) != 21 || j.planned.Load() != 21 {
		t.Errorf("a budget of exactly the records: stopped %q, unread %q, %d hits, planned %d", j.stopped, unread(j), len(j.hits), j.planned.Load())
	}
	// Budget 8: small (3) and even1 or even2 (5) fit exactly; the rest are left whole.
	j = run(8, tpOffset{}, 3*time.Hour+time.Second)
	u := unread(j)
	if strings.Contains(u, " of ") || !strings.Contains(u, "large partition 0: 8 records") || j.planned.Load() != 8 {
		t.Errorf("budget 8: unread %q, planned %d", u, j.planned.Load())
	}
	// Budget 5: small is read whole, then the part of the next that fits.
	j = run(5, tpOffset{}, 3*time.Hour+2*time.Second)
	u = unread(j)
	if !strings.Contains(u, "large partition 0: 8 records") || strings.Count(u, "partition 0: 3 of 5 records") != 1 || strings.Count(u, "partition 0: 5 records") != 1 || strings.Contains(u, "small") {
		t.Errorf("budget 5: unread %q", u)
	}
	if j.stopped != "records" {
		t.Errorf("budget 5: stopped %q", j.stopped)
	}
	// With the starting record on the large topic, it is read first.
	j = run(5, tpOffset{tpKey: tpKey{"large", 0}, Offset: 0, Set: true}, 3*time.Hour+3*time.Second)
	u = unread(j)
	if !strings.Contains(u, "large partition 0: 3 of 8 records") || !strings.Contains(u, "small partition 0: 3 records") {
		t.Errorf("budget 5 from the large topic: unread %q", u)
	}
	// An anchor that is set but elsewhere does not reorder the others.
	j = run(5, tpOffset{tpKey: tpKey{"nowhere", 0}, Set: true}, 3*time.Hour+4*time.Second)
	if u = unread(j); !strings.Contains(u, "large partition 0: 8 records") || strings.Count(u, "partition 0: 3 of 5 records") != 1 {
		t.Errorf("budget 5, anchor elsewhere: unread %q", u)
	}
}

// What the job table keeps, reuses and forgets.
func TestStartTraceKeepsReusesAndForgetsJobs(t *testing.T) {
	c := newTestConsole(t)
	now := time.Now()
	at := now.Add(-time.Minute)
	key := func(trace string) string {
		return fmt.Sprintf("%s|%d|%d|%s|%d", trace, at.UnixMilli(), time.Minute, "", 0)
	}
	seed := func(id, k string, started, finished time.Time) *traceJob {
		j := &traceJob{id: id, key: k, started: started, finished: finished, phase: "done"}
		c.traces.mu.Lock()
		if c.traces.byID == nil {
			c.traces.byID = map[string]*traceJob{}
		}
		c.traces.byID[id] = j
		c.traces.mu.Unlock()
		return j
	}
	has := func(id string) bool { return c.traces.get(id) != nil }

	old := seed("old", "k-old", now.Add(-time.Hour), now.Add(-traceJobTTL-time.Minute))
	recent := seed("recent", "k-recent", now.Add(-time.Hour), now.Add(-traceJobTTL+time.Minute))
	stale := seed("stale", key("stale-trace"), now.Add(-time.Hour), now.Add(-traceReuseFor-30*time.Second))
	fresh := seed("fresh", key("fresh-trace"), now.Add(-time.Hour), now.Add(-traceReuseFor+30*time.Second))
	running := seed("running", key("running-trace"), now.Add(-time.Hour), time.Time{})
	_, _ = old, recent

	if j := c.startTrace("fresh-trace", tpOffset{}, at, time.Minute); j != fresh {
		t.Error("a scan finished half a minute ago is not reused for the same question")
	}
	if j := c.startTrace("running-trace", tpOffset{}, at, time.Minute); j != running {
		t.Error("a scan still running is not reused for the same question")
	}
	newest := c.startTrace("stale-trace", tpOffset{}, at, time.Minute)
	if newest == stale || newest.trace != "stale-trace" {
		t.Error("a scan finished long ago answers a reload")
	}
	if has("old") {
		t.Error("a scan older than the time to live is kept")
	}
	if !has("recent") || !has("fresh") {
		t.Error("a scan inside the time to live was forgotten")
	}
	m := regexp.MustCompile(`^t(\d+)-1$`).FindStringSubmatch(newest.id)
	if m == nil {
		t.Fatal("job ids are not t<milliseconds>-<sequence>")
	}
	if ms, _ := strconv.ParseInt(m[1], 10, 64); ms < now.UnixMilli() || ms > time.Now().UnixMilli()+1 {
		t.Errorf("job id carries %d, not the time in milliseconds (%d)", ms, now.UnixMilli())
	}
	if id := c.startTrace("another", tpOffset{}, at, time.Minute).id; !strings.HasSuffix(id, "-2") {
		t.Errorf("the second new job is %s, want sequence 2", id)
	}
}

func TestStartTraceDropsTheOldestPastTheLimit(t *testing.T) {
	c := newTestConsole(t)
	now := time.Now()
	c.traces.byID = map[string]*traceJob{}
	for i := 0; i < traceJobsKept; i++ {
		id := fmt.Sprintf("j%02d", i)
		// Started in a shuffled order, so the oldest is not the first looked at.
		c.traces.byID[id] = &traceJob{id: id, key: id, started: now.Add(-time.Duration((i*7)%traceJobsKept+1) * time.Minute), finished: now, phase: "done"}
	}
	oldest := ""
	for id, j := range c.traces.byID {
		if oldest == "" || j.started.Before(c.traces.byID[oldest].started) {
			oldest = id
		}
	}
	j := c.startTrace("new", tpOffset{}, now, time.Minute)
	if n := len(c.traces.byID); n != traceJobsKept {
		t.Errorf("%d jobs kept, want %d", n, traceJobsKept)
	}
	if c.traces.get(oldest) != nil || c.traces.get(j.id) == nil {
		t.Errorf("the oldest job %s survives or the new one is missing", oldest)
	}
}

// Reading the planned ranges: only what is inside them, every partition counted as it
// completes, in the time a scan takes and not a poll's patience longer.
func TestScanRangesReadsExactlyTheRanges(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "sr", 2, nil); err != nil {
		t.Fatal(err)
	}
	mk := func(p int32, n int) []*kgo.Record {
		recs := make([]*kgo.Record, n)
		for i := range recs {
			recs[i] = &kgo.Record{Topic: "sr", Partition: p, Value: []byte("v"), Key: []byte(fmt.Sprintf("p%d-%d", p, i)), Headers: tp(traceA)}
		}
		return recs
	}
	if err := cl.ProduceSync(ctx, append(mk(0, 6), mk(1, 6)...)...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	j := &traceJob{trace: traceA}
	start := time.Now()
	hits, timedOut, unread, err := c.scanRanges(ctx, j, []partRange{{tpKey{"sr", 0}, 1, 3}, {tpKey{"sr", 1}, 0, 2}})
	if err != nil || timedOut || len(unread) != 0 {
		t.Fatalf("scan: timedOut %v, unread %v, err %v", timedOut, unread, err)
	}
	if el := time.Since(start); el > 1400*time.Millisecond {
		t.Errorf("the scan took %v: it waited for records that were not coming", el)
	}
	var keys []string
	for _, h := range hits {
		keys = append(keys, h.Key)
	}
	sort.Strings(keys)
	if got := strings.Join(keys, " "); got != "p0-1 p0-2 p1-0 p1-1" {
		t.Errorf("records read: %s", got)
	}
	if j.partsDone != 2 || j.scanned.Load() != 4 {
		t.Errorf("partitions done %d, records scanned %d; want 2 and 4", j.partsDone, j.scanned.Load())
	}
}

func TestScanRangesNamesWhatATimeoutLeft(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "to", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "to", 0, 6, vals("v"), time.Time{})
	old := traceMaxTime
	traceMaxTime = time.Nanosecond
	defer func() { traceMaxTime = old }()
	_, timedOut, unread, err := c.scanRanges(ctx, &traceJob{trace: traceA}, []partRange{{tpKey{"to", 0}, 2, 6}})
	if err != nil || !timedOut || len(unread) != 1 || unread[0] != "to partition 0: 4 of 4 records" {
		t.Errorf("timedOut %v, unread %q, err %v", timedOut, unread, err)
	}
}

// A range larger than one fetch is read through to its end.
func TestScanRangesFollowsSeveralFetches(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "lg", 1, nil); err != nil {
		t.Fatal(err)
	}
	val := strings.Repeat("x", 3000)
	const n = 6000 // 18 MB: more than one fetch of the reader's 8 MB.
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: "lg", Value: []byte(val), Headers: tp(traceA)}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	j := &traceJob{trace: traceA}
	hits, timedOut, unread, err := c.scanRanges(ctx, j, []partRange{{tpKey{"lg", 0}, 0, n}})
	if err != nil || timedOut || len(unread) != 0 || len(hits) != n || j.scanned.Load() != n {
		t.Errorf("hits %d, scanned %d, timedOut %v, unread %v, err %v", len(hits), j.scanned.Load(), timedOut, unread, err)
	}
}
