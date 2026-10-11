package console

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The scan's budget. A trace is looked for by reading, and reading everything would
// make "show me this message's trail" a minutes-long job on a topic with a day of
// data. These bound it; a scan that reaches either says so rather than reporting what
// it found as everything there is.
var (
	traceMaxRecords = 50_000
	traceMaxTime    = 8 * time.Second
)

// tpKey names one partition.
type tpKey struct {
	Topic     string
	Partition int32
}

// partRange is the records of one partition to look at: offsets [From, To).
type partRange struct {
	tpKey
	From, To int64
}

func (p partRange) size() int64 { return p.To - p.From }

// traceHit is one record that carries the trace id.
type traceHit struct {
	Topic     string
	Partition int32
	Offset    int64
	Time      time.Time
	Key       string
	Ref       traceRef
	Anchor    bool // the record the person started from.

	Consumers []traceConsumer

	// Layout, filled in once the scan is done.
	Delta string
	Left  int // percent along the time axis.
}

// traceConsumer says whether a group has read past a record.
type traceConsumer struct {
	Group string
	Past  bool
}

// traceJob is one scan, running or finished. The page polls it for progress.
type traceJob struct {
	id      string
	key     string
	trace   string
	anchor  tpOffset
	at      time.Time
	window  time.Duration
	started time.Time

	planned atomic.Int64 // records the scan means to read.
	scanned atomic.Int64 // records read so far.

	mu         sync.Mutex
	phase      string // "planning", "scanning", "done".
	partsTotal int
	partsDone  int
	hits       []traceHit
	stopped    string   // "", or why it stopped short: "records", "time".
	unread     []string // what was not read, when it stopped short.
	err        error
	finished   time.Time
}

// tpOffset is where the person started: a record.
type tpOffset struct {
	tpKey
	Offset int64
	Set    bool
}

type traceJobs struct {
	mu   sync.Mutex
	byID map[string]*traceJob
	seq  int
}

const (
	traceJobsKept  = 24
	traceReuseFor  = time.Minute // a finished scan answers a reload for this long.
	traceJobTTL    = 20 * time.Minute
	tracePollEvery = 600 * time.Millisecond
)

// start returns the job for this question: a running or recently finished one if it
// is the same question (a reload must not scan again), else a new one, started.
func (c *Console) startTrace(trace string, anchor tpOffset, at time.Time, window time.Duration) *traceJob {
	js := &c.traces
	key := fmt.Sprintf("%s|%d|%d|%s|%d", trace, at.UnixMilli(), window, anchor.Topic, anchor.Offset)
	js.mu.Lock()
	defer js.mu.Unlock()
	if js.byID == nil {
		js.byID = map[string]*traceJob{}
	}
	now := time.Now()
	for id, j := range js.byID {
		j.mu.Lock()
		old := !j.finished.IsZero() && now.Sub(j.finished) > traceJobTTL
		j.mu.Unlock()
		if old {
			delete(js.byID, id)
		}
	}
	for _, j := range js.byID {
		j.mu.Lock()
		fresh := j.finished.IsZero() || now.Sub(j.finished) < traceReuseFor
		j.mu.Unlock()
		if j.key == key && fresh {
			return j
		}
	}
	for len(js.byID) >= traceJobsKept { // drop the oldest.
		var oldest string
		for id, j := range js.byID {
			if oldest == "" || j.started.Before(js.byID[oldest].started) {
				oldest = id
			}
		}
		delete(js.byID, oldest)
	}
	js.seq++
	j := &traceJob{id: fmt.Sprintf("t%d-%d", now.UnixNano()/1e6, js.seq), key: key, trace: trace, anchor: anchor, at: at, window: window, started: now, phase: "planning"}
	js.byID[j.id] = j
	go c.runTrace(j)
	return j
}

func (js *traceJobs) get(id string) *traceJob {
	js.mu.Lock()
	defer js.mu.Unlock()
	return js.byID[id]
}

// offsetsAt is, for every partition of the topics, the offset of the first record at
// or after ts (milliseconds), or -1 where there is none; ts of -1 asks for each
// partition's end. One round trip however many partitions.
func (b *backend) offsetsAt(ctx context.Context, topics []TopicSummary, ts int64) (map[tpKey]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	for _, t := range topics {
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = t.Name
		for p := int32(0); p < t.Partitions; p++ {
			rp := kmsg.NewListOffsetsRequestTopicPartition()
			rp.Partition, rp.Timestamp = p, ts
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	out := map[tpKey]int64{}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode == kerr.UnknownTopicOrPartition.Code {
				continue // deleted since the topic list was read.
			}
			if p.ErrorCode != 0 {
				return nil, kerr.ErrorForCode(p.ErrorCode)
			}
			out[tpKey{t.Topic, p.Partition}] = p.Offset
		}
	}
	return out, nil
}

// planTrace is what to read: for every partition, the records written between at-window
// and at+window, found with the broker's time index rather than by reading.
func (c *Console) planTrace(ctx context.Context, at time.Time, window time.Duration) ([]partRange, error) {
	topics, err := c.be.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	from, err := c.be.offsetsAt(ctx, topics, at.Add(-window).UnixMilli())
	if err != nil {
		return nil, err
	}
	to, err := c.be.offsetsAt(ctx, topics, at.Add(window).UnixMilli())
	if err != nil {
		return nil, err
	}
	end, err := c.be.offsetsAt(ctx, topics, -1)
	if err != nil {
		return nil, err
	}
	var out []partRange
	for k, f := range from {
		if f < 0 {
			continue // nothing at or after the start of the window.
		}
		t := to[k]
		if t < 0 {
			t = end[k] // nothing after the window: read to the end of the log.
		}
		if t > f {
			out = append(out, partRange{k, f, t})
		}
	}
	return out, nil
}

// runTrace plans, scans and finishes one job.
func (c *Console) runTrace(j *traceJob) {
	ctx, cancel := context.WithTimeout(context.Background(), traceMaxTime+20*time.Second)
	defer cancel()
	fail := func(err error) {
		j.mu.Lock()
		j.err, j.phase, j.finished = err, "done", time.Now()
		j.mu.Unlock()
	}

	plan, err := c.planTrace(ctx, j.at, j.window)
	if err != nil {
		fail(err)
		return
	}
	// The record's own partition first, then the smallest ranges: with a budget, more
	// partitions finish that way than if a large one is read first.
	sort.SliceStable(plan, func(a, b int) bool {
		ha, hb := j.anchor.Set && plan[a].tpKey == j.anchor.tpKey, j.anchor.Set && plan[b].tpKey == j.anchor.tpKey
		if ha != hb {
			return ha
		}
		return plan[a].size() < plan[b].size()
	})
	// Cut the plan to the budget. Whole ranges while they fit, then the part of the
	// next that does, then nothing: what was left out is named afterwards.
	var left []string
	budget := int64(traceMaxRecords)
	kept := plan[:0:0]
	for _, r := range plan {
		switch {
		case budget <= 0:
			left = append(left, fmt.Sprintf("%s partition %d: %s records", r.Topic, r.Partition, groupDigits(r.size())))
		case r.size() > budget:
			cut := r
			cut.To = cut.From + budget
			kept = append(kept, cut)
			left = append(left, fmt.Sprintf("%s partition %d: %s of %s records", r.Topic, r.Partition, groupDigits(r.size()-budget), groupDigits(r.size())))
			budget = 0
		default:
			kept = append(kept, r)
			budget -= r.size()
		}
	}
	var planned int64
	for _, r := range kept {
		planned += r.size()
	}
	j.planned.Store(planned)
	j.mu.Lock()
	j.phase, j.partsTotal = "scanning", len(kept)
	if len(left) > 0 {
		j.stopped, j.unread = "records", left
	}
	j.mu.Unlock()

	hits, timedOut, unread, err := c.scanRanges(ctx, j, kept)
	if err != nil {
		fail(err)
		return
	}
	j.mu.Lock()
	if timedOut {
		j.stopped = "time"
	}
	j.unread = append(j.unread, unread...)
	j.mu.Unlock()

	c.finishTrace(ctx, j, hits)
}

// scanRanges reads the ranges with an ordinary consumer and keeps the records that
// carry the job's trace id.
func (c *Console) scanRanges(ctx context.Context, j *traceJob, ranges []partRange) (hits []traceHit, timedOut bool, unread []string, err error) {
	if len(ranges) == 0 {
		return nil, false, nil, nil
	}
	want := map[tpKey]partRange{}
	assign := map[string]map[int32]kgo.Offset{}
	for _, r := range ranges {
		want[r.tpKey] = r
		if assign[r.Topic] == nil {
			assign[r.Topic] = map[int32]kgo.Offset{}
		}
		assign[r.Topic][r.Partition] = kgo.NewOffset().At(r.From)
	}
	cl, err := c.be.newReader(assign)
	if err != nil {
		return nil, false, nil, err
	}
	defer cl.Close()

	sctx, cancel := context.WithTimeout(ctx, traceMaxTime)
	defer cancel()
	reached := map[tpKey]int64{} // the next offset not yet seen, per partition.
	for k, r := range want {
		reached[k] = r.From
	}
	remaining := len(want)
	for remaining > 0 {
		// An empty poll within the second means the log has a gap where records were
		// planned (compaction, retention since the plan): stop waiting for them.
		pctx, pcancel := context.WithTimeout(sctx, 1500*time.Millisecond)
		fs := cl.PollFetches(pctx)
		pcancel()
		if fs.IsClientClosed() {
			break
		}
		got := 0
		fs.EachRecord(func(r *kgo.Record) {
			k := tpKey{r.Topic, r.Partition}
			rg, ok := want[k]
			if !ok || reached[k] >= rg.To || r.Offset >= rg.To {
				return
			}
			got++
			j.scanned.Add(1)
			reached[k] = r.Offset + 1
			hdrs := make([]MessageHeader, len(r.Headers))
			for i, h := range r.Headers {
				hdrs[i] = MessageHeader{Key: h.Key, Value: h.Value}
			}
			if ref, ok := carries(hdrs, c.traceHeader, j.trace); ok {
				hits = append(hits, traceHit{
					Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Time: r.Timestamp, Key: previewKey(r.Key), Ref: ref,
					Anchor: j.anchor.Set && j.anchor.tpKey == k && j.anchor.Offset == r.Offset,
				})
			}
			if reached[k] >= rg.To {
				remaining--
				j.mu.Lock()
				j.partsDone++
				j.mu.Unlock()
				cl.PauseFetchPartitions(map[string][]int32{r.Topic: {r.Partition}})
			}
		})
		if sctx.Err() != nil {
			timedOut = sctx.Err() == context.DeadlineExceeded
			break
		}
		if got == 0 {
			break
		}
	}
	for k, rg := range want {
		if reached[k] < rg.To && timedOut {
			unread = append(unread, fmt.Sprintf("%s partition %d: %s of %s records", k.Topic, k.Partition, groupDigits(rg.To-reached[k]), groupDigits(rg.size())))
		}
	}
	sort.Strings(unread)
	return hits, timedOut, unread, nil
}

// previewKey is a record's key as short text for a row, or "" for none.
func previewKey(k []byte) string {
	if len(k) == 0 {
		return ""
	}
	if !utf8.Valid(k) {
		return fmt.Sprintf("%d bytes", len(k))
	}
	s := string(k)
	if r := []rune(s); len(r) > 40 {
		s = string(r[:40]) + "…"
	}
	return s
}

// finishTrace orders the hits, says which groups have read past each, lays out the
// time axis, and marks the job done.
func (c *Console) finishTrace(ctx context.Context, j *traceJob, hits []traceHit) {
	sort.SliceStable(hits, func(a, b int) bool {
		if !hits[a].Time.Equal(hits[b].Time) {
			return hits[a].Time.Before(hits[b].Time)
		}
		if hits[a].Topic != hits[b].Topic {
			return hits[a].Topic < hits[b].Topic
		}
		if hits[a].Partition != hits[b].Partition {
			return hits[a].Partition < hits[b].Partition
		}
		return hits[a].Offset < hits[b].Offset
	})

	committed := c.committedOffsets(ctx)
	for i := range hits {
		h := &hits[i]
		for _, g := range committed.groups {
			if off, ok := committed.by[g][tpKey{h.Topic, h.Partition}]; ok {
				h.Consumers = append(h.Consumers, traceConsumer{Group: g, Past: off > h.Offset})
			}
		}
	}
	if len(hits) > 0 {
		first, last := hits[0].Time, hits[len(hits)-1].Time
		span := last.Sub(first)
		for i := range hits {
			h := &hits[i]
			h.Delta = "+" + shortDuration(h.Time.Sub(first))
			if i == 0 {
				h.Delta = "start"
			}
			if span > 0 {
				h.Left = int(h.Time.Sub(first) * 96 / span)
			}
		}
	}
	j.mu.Lock()
	j.hits, j.phase, j.finished = hits, "done", time.Now()
	j.mu.Unlock()
}

// groupCommits is every group's committed offsets, by partition.
type groupCommits struct {
	groups []string
	by     map[string]map[tpKey]int64
}

// committedOffsets reads every group's committed offsets. A group that cannot be read
// is left out: the trace is still right, only without that group's column.
func (c *Console) committedOffsets(ctx context.Context) groupCommits {
	out := groupCommits{by: map[string]map[tpKey]int64{}}
	groups, err := c.be.ListGroups(ctx)
	if err != nil {
		return out
	}
	for _, g := range groups {
		offs, err := c.be.GroupOffsets(ctx, g.ID)
		if err != nil || len(offs) == 0 {
			continue
		}
		m := map[tpKey]int64{}
		for _, o := range offs {
			m[tpKey{o.Topic, o.Partition}] = o.Committed
		}
		out.groups = append(out.groups, g.ID)
		out.by[g.ID] = m
	}
	sort.Strings(out.groups)
	return out
}

// groupDigits writes n with thousands separators: 12,400.
func groupDigits(n int64) string {
	s := fmt.Sprint(n)
	if n < 1000 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
	}
	for i := pre; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}
