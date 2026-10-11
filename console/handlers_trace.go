package console

import (
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// traceWindows are the spans the page offers to look across, either side of the message.
var traceWindows = []struct {
	Key   string
	Label string
	D     time.Duration
}{
	{"1m", "1 minute", time.Minute},
	{"5m", "5 minutes", 5 * time.Minute},
	{"30m", "30 minutes", 30 * time.Minute},
	{"3h", "3 hours", 3 * time.Hour},
}

const defaultTraceWindow = "5m"

// traceURL is the page for a record's trace: the id, the moment to look around, and the
// record to mark as the one the person started from.
func (c *Console) traceURL(id string, at time.Time, topic string, partition int32, offset int64, window string) string {
	q := url.Values{"id": {id}, "at": {strconv.FormatInt(at.UnixMilli(), 10)}}
	if topic != "" {
		q.Set("topic", topic)
		q.Set("partition", strconv.Itoa(int(partition)))
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	if window != "" && window != defaultTraceWindow {
		q.Set("window", window)
	}
	return c.prefix + "/trace?" + q.Encode()
}

// traceParams reads what the page was asked: the id, when, how wide, and which record.
func (c *Console) traceParams(r *http.Request) (id string, at time.Time, window time.Duration, windowKey string, anchor tpOffset, err error) {
	q := r.URL.Query()
	id = traceIDFrom(q.Get("id")) // a pasted header value is read as the id it carries.
	if id == "" || len(id) > 128 {
		return "", time.Time{}, 0, "", anchor, badInput("a trace id is needed: open a record that carries one and choose Trace")
	}
	at = time.Now()
	if raw := q.Get("at"); raw != "" {
		ms, perr := strconv.ParseInt(raw, 10, 64)
		if perr != nil || ms < 0 {
			return "", time.Time{}, 0, "", anchor, badInput("at must be a time in milliseconds since the epoch")
		}
		at = time.UnixMilli(ms)
	}
	windowKey = q.Get("window")
	if windowKey == "" {
		windowKey = defaultTraceWindow
	}
	for _, w := range traceWindows {
		if w.Key == windowKey {
			window = w.D
		}
	}
	if window == 0 {
		return "", time.Time{}, 0, "", anchor, badInput("window must be one of 1m, 5m, 30m, 3h")
	}
	if topic := q.Get("topic"); topic != "" {
		p, perr := strconv.ParseInt(q.Get("partition"), 10, 32)
		o, oerr := strconv.ParseInt(q.Get("offset"), 10, 64)
		if perr != nil || oerr != nil || p < 0 || o < 0 {
			return "", time.Time{}, 0, "", anchor, badInput("partition and offset must be whole numbers")
		}
		anchor = tpOffset{tpKey: tpKey{Topic: topic, Partition: int32(p)}, Offset: o, Set: true}
	}
	return id, at, window, windowKey, anchor, nil
}

// traceIDPatterns find a trace id in whatever was pasted: a bare id, a W3C
// traceparent header value, or a B3 single header.
var traceIDPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^\s*[0-9a-f]{2}-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}\s*$`),
	regexp.MustCompile(`(?i)^\s*([0-9a-f]{16}|[0-9a-f]{32})-[0-9a-f]{16}(?:-[01d](?:-[0-9a-f]{16})?)?\s*$`),
}

// traceIDFrom reads the trace id out of a pasted value: "00-4bf9…-00f0…-01" is the
// trace id 4bf9…; anything else is taken as the id as typed.
func traceIDFrom(raw string) string {
	for _, re := range traceIDPatterns {
		if m := re.FindStringSubmatch(raw); m != nil {
			return strings.ToLower(m[1])
		}
	}
	return strings.TrimSpace(raw)
}

// traceHome is Tc-Home: a field for a trace id (or a whole header value), the
// formats it reads and what it can and cannot find. It runs nothing until asked.
func (c *Console) traceHome(w http.ResponseWriter, r *http.Request) {
	data := map[string]any{"Page": "trace", "Title": "Trace", "TraceHeader": c.traceHeader,
		"MaxRecords": groupDigits(int64(traceMaxRecords)), "MaxSeconds": int(traceMaxTime / time.Second)}
	c.render(w, r, "trace_home", data)
}

// traceData is the job as the page shows it, taken under its lock.
func (c *Console) traceData(j *traceJob) map[string]any {
	j.mu.Lock()
	defer j.mu.Unlock()
	running := j.phase != "done"
	planned, scanned := j.planned.Load(), j.scanned.Load()

	topics := map[string]struct{}{}
	for _, h := range j.hits {
		topics[h.Topic] = struct{}{}
	}
	h := fnv.New64a()
	fmt.Fprint(h, j.phase, scanned, j.partsDone, len(j.hits), j.stopped, j.err != nil)

	data := map[string]any{
		"JobID": j.id, "Running": running, "Phase": j.phase,
		"Planned": planned, "Scanned": scanned,
		"PlannedText": groupDigits(planned), "ScannedText": groupDigits(scanned),
		"PartsTotal": j.partsTotal, "PartsDone": j.partsDone,
		"Hits": j.hits, "TopicCount": len(topics),
		"Stopped": j.stopped, "Unread": j.unread,
		"MaxRecords": groupDigits(int64(traceMaxRecords)), "MaxSeconds": int(traceMaxTime / time.Second),
		"Hash": fmt.Sprintf("%x", h.Sum64()),
	}
	if j.err != nil {
		f := refusalFor(j.err)
		f.Message = "The scan failed: " + f.Message
		data["ScanFailed"], data["Refusal"] = true, f
	}
	if !running && !j.finished.IsZero() {
		data["Took"] = shortDuration(j.finished.Sub(j.started))
	}
	data["TraceHeader"] = c.traceHeader
	return data
}

func (c *Console) tracePage(w http.ResponseWriter, r *http.Request) {
	// No id yet: the page asks for one (Tc-Home) rather than refusing.
	if strings.TrimSpace(r.URL.Query().Get("id")) == "" {
		c.traceHome(w, r)
		return
	}
	id, at, window, windowKey, anchor, err := c.traceParams(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	job := c.startTrace(id, anchor, at, window)
	data := c.traceData(job)
	data["Trace"], data["At"], data["WindowKey"] = id, at, windowKey
	for _, win := range traceWindows {
		if win.Key == windowKey {
			data["WindowLabel"] = win.Label
		}
	}
	type opt struct {
		Label, URL string
		On         bool
	}
	var opts []opt
	for _, win := range traceWindows {
		opts = append(opts, opt{Label: win.Label, URL: c.traceURL(id, at, anchor.Topic, anchor.Partition, anchor.Offset, win.Key), On: win.Key == windowKey})
	}
	if !anchor.Set {
		for i := range opts {
			opts[i].URL = c.traceURL(id, at, "", 0, 0, traceWindows[i].Key)
		}
	}
	data["Windows"], data["Page"], data["Title"] = opts, "trace", "Trace "+id
	c.render(w, r, "trace_detail", data)
}

// traceLive is the polled region: progress while the scan runs, the result when it is done.
func (c *Console) traceLive(w http.ResponseWriter, r *http.Request) {
	j := c.traces.get(r.URL.Query().Get("job"))
	if j == nil {
		c.fail(w, r, fmt.Errorf("%w: this scan is no longer kept; reload the page to run it again", errNotFound))
		return
	}
	data := c.traceData(j)
	if liveUnchanged(w, r, data["Hash"].(string)) {
		return
	}
	c.partial(w, "trace_live", data)
}
