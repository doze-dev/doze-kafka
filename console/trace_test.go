package console

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func hdr(k, v string) MessageHeader { return MessageHeader{Key: k, Value: []byte(v)} }

const (
	traceA  = "4bf92f3577b34da6a3ce929d0e0e4736"
	spanA   = "00f067aa0ba902b7"
	parentA = "a2fb4a1d1a96d312"
)

func TestTraceIDsAreReadFromEveryConvention(t *testing.T) {
	for _, tc := range []struct {
		name    string
		h       []MessageHeader
		custom  string
		want    string // the id of the first reference, "" for none.
		kind    string
		span    string
		parent  string
		allKind []string
	}{
		{"w3c", []MessageHeader{hdr("traceparent", "00-"+traceA+"-"+spanA+"-01")}, "", traceA, "W3C traceparent", spanA, "", nil},
		{"w3c, any case", []MessageHeader{hdr("TraceParent", "00-"+strings.ToUpper(traceA)+"-"+strings.ToUpper(spanA)+"-01")}, "", traceA, "W3C traceparent", spanA, "", nil},
		{"w3c all zeros is no trace", []MessageHeader{hdr("traceparent", "00-00000000000000000000000000000000-"+spanA+"-01")}, "", "", "", "", "", nil},
		{"w3c malformed", []MessageHeader{hdr("traceparent", "not-a-trace")}, "", "", "", "", "", nil},
		{"b3 single with a parent", []MessageHeader{hdr("b3", traceA+"-"+spanA+"-1-"+parentA)}, "", traceA, "B3", spanA, parentA, nil},
		{"b3 single, 64-bit id", []MessageHeader{hdr("b3", "463ac35c9f6413ad-"+spanA)}, "", "463ac35c9f6413ad", "B3", spanA, "", nil},
		{"b3 sampling only", []MessageHeader{hdr("b3", "0")}, "", "", "", "", "", nil},
		{"b3 multi", []MessageHeader{hdr("X-B3-TraceId", traceA), hdr("X-B3-SpanId", spanA), hdr("X-B3-ParentSpanId", parentA)}, "", traceA, "B3", spanA, parentA, nil},
		{"configured header", []MessageHeader{hdr("x-request-id", "req-7781")}, "X-Request-Id", "req-7781", "header X-Request-Id", "", "", nil},
		{"configured header unset", []MessageHeader{hdr("x-request-id", "req-7781")}, "", "", "", "", "", nil},
		{"w3c wins over b3", []MessageHeader{hdr("b3", "463ac35c9f6413ad-"+spanA), hdr("traceparent", "00-"+traceA+"-"+spanA+"-01")}, "", traceA, "W3C traceparent", spanA, "", []string{"W3C traceparent", "B3"}},
		{"binary value is no id", []MessageHeader{{Key: "traceparent", Value: []byte{0xff, 0xfe, 0x00}}}, "", "", "", "", "", nil},
		{"null value", []MessageHeader{{Key: "traceparent"}}, "", "", "", "", "", nil},
		{"no headers", nil, "x", "", "", "", "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref := traceOf(tc.h, tc.custom)
			if tc.want == "" {
				if ref != nil {
					t.Fatalf("found %+v, want nothing", ref)
				}
				return
			}
			if ref == nil || ref.ID != tc.want || ref.Kind != tc.kind || ref.Span != tc.span || ref.Parent != tc.parent {
				t.Fatalf("got %+v, want id %s kind %s span %s parent %s", ref, tc.want, tc.kind, tc.span, tc.parent)
			}
			if tc.allKind != nil {
				var kinds []string
				for _, r := range traceRefs(tc.h, tc.custom) {
					kinds = append(kinds, r.Kind)
				}
				if strings.Join(kinds, ",") != strings.Join(tc.allKind, ",") {
					t.Errorf("all kinds = %v, want %v", kinds, tc.allKind)
				}
			}
		})
	}
}

// B3 may carry the 64-bit form of a 128-bit id: the two name one trace.
func TestAShortB3IDMatchesTheLongOne(t *testing.T) {
	long := "0000000000000000463ac35c9f6413ad"
	if !sameTrace("463ac35c9f6413ad", long) || !sameTrace(long, "463ac35c9f6413ad") {
		t.Error("a 64-bit B3 id does not match its 128-bit form")
	}
	if sameTrace("463ac35c9f6413ad", "1111111111111111463ac35c9f6413ad") {
		t.Error("a 64-bit id matched a 128-bit id whose upper half is not zero")
	}
	if _, ok := carries([]MessageHeader{hdr("b3", "463ac35c9f6413ad-"+spanA)}, "", long); !ok {
		t.Error("a record with a 64-bit B3 id is not found by the 128-bit id")
	}
}

func TestGroupDigits(t *testing.T) {
	for in, want := range map[int64]string{0: "0", 999: "999", 1000: "1,000", 12400: "12,400", 1234567: "1,234,567"} {
		if got := groupDigits(in); got != want {
			t.Errorf("groupDigits(%d) = %q, want %q", in, got, want)
		}
	}
}

// ---- against a real broker ----

type sent struct {
	topic string
	at    time.Time
	hdr   []kgo.RecordHeader
	key   string
}

// writeAll produces the records, one at a time and in order, with the timestamps given.
func writeAll(t *testing.T, addr string, recs []sent) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, r := range recs {
		rec := &kgo.Record{Topic: r.topic, Partition: 0, Timestamp: r.at, Key: []byte(r.key), Value: []byte("v"), Headers: r.hdr}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce to %s: %v", r.topic, err)
		}
	}
}

func tp(id string) []kgo.RecordHeader {
	return []kgo.RecordHeader{{Key: "traceparent", Value: []byte("00-" + id + "-" + spanA + "-01")}}
}

// waitTrace loads the page until the scan is done (a reload reuses the running job,
// so this does not start another) and returns the finished page.
func waitTrace(t *testing.T, c *Console, path string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		rec := req(t, c, "GET", path, nil)
		if rec.Code != 200 {
			t.Fatalf("GET %s = %d: %s", path, rec.Code, rec.Body)
		}
		if body := rec.Body.String(); !strings.Contains(body, `data-live="`) || strings.Contains(body, `class="honest warn" role="alert"`) {
			return body
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the scan of %s did not finish", path)
	return ""
}

func traceQuery(id string, at time.Time, extra url.Values) string {
	q := url.Values{"id": {id}, "at": {fmt.Sprint(at.UnixMilli())}}
	for k, v := range extra {
		q[k] = v
	}
	return "/_console/trace?" + q.Encode()
}

// One request that fans out across three topics: the page finds all of its records, in
// the order they were written, none of the others, and says which groups have read each.
func TestTraceFindsTheRecordsOfOneTraceAcrossTopicsInOrder(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	// Named so that alphabetical order is the reverse of the order things were written: the
	// scan meets topics by name, and the list must come out in time order regardless.
	for _, name := range []string{"zz-orders", "mm-payments", "aa-shipments"} {
		topicWith(t, c, name, 1, 0)
	}
	base := time.Now().Add(-2 * time.Minute).Truncate(time.Millisecond)
	other := "11111111111111111111111111111111"
	writeAll(t, addr, []sent{
		{"zz-orders", base, tp(traceA), "ord-1"},
		{"zz-orders", base.Add(20 * time.Millisecond), tp(other), "ord-2"},     // another trace.
		{"mm-payments", base.Add(120 * time.Millisecond), tp(traceA), "pay-1"}, // the same trace, another topic.
		{"zz-orders", base.Add(130 * time.Millisecond), nil, "no-header"},      // no header at all: invisible.
		{"aa-shipments", base.Add(400 * time.Millisecond), []kgo.RecordHeader{{Key: "b3", Value: []byte(traceA + "-" + spanA + "-1-" + parentA)}}, "ship-1"}, // B3, same id.
		{"mm-payments", base.Add(500 * time.Millisecond), tp(other), "pay-2"},
	})
	// orders-workers has read the first order and nothing after; payments-workers has read all payments.
	commitFor(t, addr, "orders-workers", "zz-orders", map[int32]int64{0: 1})
	commitFor(t, addr, "payments-workers", "mm-payments", map[int32]int64{0: 2})

	body := waitTrace(t, c, traceQuery(traceA, base.Add(100*time.Millisecond), url.Values{"topic": {"aa-shipments"}, "partition": {"0"}, "offset": {"0"}}))
	// (The scan reads the starting record's partition first, so the newest record is found
	// first: the list must still come out in the order things were written.)

	list := between(body, `<ol class="tracelist"`, `</ol>`)
	if list == "" {
		t.Fatalf("no trace list in the page:\n%s", body)
	}
	rows := strings.Split(list, `<li class="tr`)[1:]
	if len(rows) != 3 {
		t.Fatalf("found %d records, want 3 (orders, payments, shipments):\n%s", len(rows), list)
	}
	for i, want := range []string{">zz-orders<", ">mm-payments<", ">aa-shipments<"} {
		if !strings.Contains(rows[i], want) {
			t.Errorf("row %d should be %s:\n%s", i+1, want, rows[i])
		}
	}
	for _, leak := range []string{"ord-2", "pay-2", "no-header"} {
		if strings.Contains(list, leak) {
			t.Errorf("the trace includes %q, which is not part of it", leak)
		}
	}
	// What the page offers about each record.
	for i, want := range []string{"key ord-1", "key pay-1", "key ship-1"} {
		if !strings.Contains(rows[i], want) {
			t.Errorf("row %d does not show %q", i+1, want)
		}
	}
	if !strings.Contains(rows[0], "start") || !strings.Contains(rows[1], "&#43;120 ms") || !strings.Contains(rows[2], "&#43;400 ms") {
		t.Errorf("the offsets from the first record are wrong:\n%s", list)
	}
	if !strings.Contains(rows[2], "this message") || strings.Contains(rows[0], "this message") || strings.Contains(rows[1], "this message") {
		t.Error("the record the person started from is not marked, or another is")
	}
	if !strings.Contains(rows[2], "parent "+parentA) || !strings.Contains(rows[0], "span "+spanA) {
		t.Error("span and parent ids are not shown where the headers carry them")
	}
	// Which groups have read past each record.
	if !strings.Contains(rows[0], "orders-workers · read") {
		t.Errorf("orders-workers committed past the first order:\n%s", rows[0])
	}
	if !strings.Contains(rows[1], "payments-workers · read") {
		t.Errorf("payments-workers committed past the payment:\n%s", rows[1])
	}
	if !strings.Contains(rows[2], "no group has committed on this partition") {
		t.Errorf("nobody has committed on shipments:\n%s", rows[2])
	}
	if strings.Contains(body, `id="trace-partial"`) {
		t.Error("a complete scan says it is partial")
	}
	if !strings.Contains(body, "3 messages") || !strings.Contains(body, "in 3 topics") {
		t.Errorf("the summary line is wrong:\n%s", between(body, `class="tr-status"`, `</div>`))
	}
}

func TestTraceLooksOnlyWithinTheChosenSpan(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "orders", 1, 0)
	// Looking around a moment three hours ago: one record before the span, one inside
	// it, one after it.
	at := time.Now().Add(-3 * time.Hour).Truncate(time.Millisecond)
	writeAll(t, addr, []sent{
		{"orders", at.Add(-30 * time.Minute), tp(traceA), "before"},
		{"orders", at.Add(time.Minute), tp(traceA), "inside"},
		{"orders", at.Add(2 * time.Hour), tp(traceA), "after"},
	})
	narrow := waitTrace(t, c, traceQuery(traceA, at, url.Values{"window": {"5m"}}))
	list := between(narrow, `<ol class="tracelist"`, `</ol>`)
	if !strings.Contains(list, "key inside") || strings.Contains(list, "key before") || strings.Contains(list, "key after") {
		t.Errorf("a five-minute span should hold only the record inside it:\n%s", list)
	}
	wide := waitTrace(t, c, traceQuery(traceA, at, url.Values{"window": {"3h"}}))
	for _, k := range []string{"key before", "key inside", "key after"} {
		if !strings.Contains(wide, k) {
			t.Errorf("a three-hour span should hold %q", k)
		}
	}
	none := waitTrace(t, c, traceQuery("9999999999999999aaaaaaaaaaaaaaaa", at, nil))
	if !strings.Contains(none, "No record with this trace id was found") {
		t.Error("a trace id nothing carries does not say so")
	}
}

// A scan that stops at its budget says so and names what it left, rather than showing
// what it found as the whole story.
func TestTraceSaysWhenItStoppedAtItsBudget(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "orders", 1, 0)
	now := time.Now().Truncate(time.Millisecond)
	var recs []sent
	for i := 0; i < 30; i++ {
		recs = append(recs, sent{"orders", now.Add(-time.Duration(30-i) * time.Second), tp(traceA), fmt.Sprintf("k%02d", i)})
	}
	writeAll(t, addr, recs)

	old := traceMaxRecords
	traceMaxRecords = 10
	defer func() { traceMaxRecords = old }()
	body := waitTrace(t, c, traceQuery(traceA, now, nil))
	if !strings.Contains(body, `id="trace-partial"`) || !strings.Contains(body, "This is not everything") {
		t.Fatalf("a scan cut short does not say so:\n%s", body)
	}
	if !strings.Contains(body, "orders partition 0: 20 of 30 records") {
		t.Errorf("it does not name what it left unread:\n%s", between(body, `id="trace-partial"`, `</ul>`))
	}
	if !strings.Contains(body, "10-record budget") {
		t.Errorf("it does not say which budget it hit:\n%s", between(body, `id="trace-partial"`, `Choose`))
	}
	if n := strings.Count(between(body, `<ol class="tracelist"`, `</ol>`), `<li class="tr`); n != 10 {
		t.Errorf("found %d records, want the 10 the budget allowed", n)
	}
}

func TestTraceSaysWhenItRanOutOfTime(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "orders", 1, 0)
	now := time.Now().Truncate(time.Millisecond)
	writeAll(t, addr, []sent{{"orders", now.Add(-time.Second), tp(traceA), "k"}})
	old := traceMaxTime
	traceMaxTime = time.Nanosecond
	defer func() { traceMaxTime = old }()
	body := waitTrace(t, c, traceQuery(traceA, now, nil))
	if !strings.Contains(body, `id="trace-partial"`) || !strings.Contains(body, "second budget") {
		t.Errorf("a scan that ran out of time does not say so:\n%s", between(body, `id="trace-live"`, `</div>`))
	}
}

func TestTracePageRefusesWhatItCannotUse(t *testing.T) {
	c := newTestConsole(t)
	for _, path := range []string{
		"/_console/trace?id=" + traceA + "&at=x", // not a time.
		"/_console/trace?id=" + traceA + "&window=2d",
		"/_console/trace?id=" + traceA + "&topic=orders&partition=a&offset=1",
	} {
		if rec := req(t, c, "GET", path, nil); rec.Code != 400 {
			t.Errorf("GET %s = %d, want 400", path, rec.Code)
		}
	}
	if rec := req(t, c, "GET", "/_console/trace/live?job=nope", nil); rec.Code != 404 {
		t.Errorf("a scan that is not kept answered %d, want 404", rec.Code)
	}
}

// The way in: a record that carries a trace id offers to follow it; one that does not
// offers nothing.
func TestTheInspectorOffersToFollowARecordOnlyIfItCarriesATrace(t *testing.T) {
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.TraceHeader = "x-request-id" })
	topicWith(t, c, "orders", 1, 0)
	now := time.Now().Truncate(time.Millisecond)
	writeAll(t, addr, []sent{
		{"orders", now, tp(traceA), "traced"},
		{"orders", now.Add(time.Millisecond), nil, "plain"},
		{"orders", now.Add(2 * time.Millisecond), []kgo.RecordHeader{{Key: "x-request-id", Value: []byte("req-42")}}, "custom"},
	})
	traced := req(t, c, "GET", "/_console/topics/orders/message?partition=0&offset=0", nil).Body.String()
	m := regexp.MustCompile(`id="inspector-trace" href="([^"]+)"`).FindStringSubmatch(traced)
	if m == nil {
		t.Fatalf("a record with a traceparent offers no way to follow it:\n%s", traced)
	}
	for _, want := range []string{"/_console/trace?", "id=" + traceA, "topic=orders", "partition=0", "offset=0", fmt.Sprintf("at=%d", now.UnixMilli())} {
		if !strings.Contains(strings.ReplaceAll(m[1], "&amp;", "&"), want) {
			t.Errorf("the link %q lacks %q", m[1], want)
		}
	}
	if plain := req(t, c, "GET", "/_console/topics/orders/message?partition=0&offset=1", nil).Body.String(); strings.Contains(plain, "inspector-trace") {
		t.Error("a record with no trace header offers to follow one")
	}
	if custom := req(t, c, "GET", "/_console/topics/orders/message?partition=0&offset=2", nil).Body.String(); !strings.Contains(custom, "req-42") {
		t.Error("a record with the configured header offers no trace")
	}
}
