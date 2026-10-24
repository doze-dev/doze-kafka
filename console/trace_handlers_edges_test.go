package console

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestTraceURL(t *testing.T) {
	c := newTestConsole(t)
	at := time.UnixMilli(1760000000123)
	if got, want := c.traceURL("abc", at, "", 0, 0, ""), "/_console/trace?at=1760000000123&id=abc"; got != want {
		t.Errorf("%q, want %q", got, want)
	}
	if got := c.traceURL("abc", at, "", 0, 0, "5m"); strings.Contains(got, "window") {
		t.Errorf("the default window is spelled out: %q", got)
	}
	if got := c.traceURL("abc", at, "", 0, 0, "3h"); !strings.Contains(got, "window=3h") {
		t.Errorf("a window is dropped: %q", got)
	}
	got := c.traceURL("abc", at, "orders", 2, 17, "1m")
	u, _ := url.Parse(got)
	q := u.Query()
	if q.Get("topic") != "orders" || q.Get("partition") != "2" || q.Get("offset") != "17" || q.Get("window") != "1m" || q.Get("id") != "abc" {
		t.Errorf("%q", got)
	}
}

func TestTraceParams(t *testing.T) {
	c := newTestConsole(t)
	parse := func(q string) (string, time.Time, time.Duration, string, tpOffset, error) {
		return c.traceParams(httptest.NewRequest("GET", "/_console/trace?"+q, nil))
	}
	id128 := strings.Repeat("a", 128)
	if id, _, _, _, _, err := parse("id=" + id128); err != nil || id != id128 {
		t.Errorf("a 128-character id: %v", err)
	}
	if _, _, _, _, _, err := parse("id=" + id128 + "b"); !errors.Is(err, errBadInput) {
		t.Errorf("a 129-character id: %v", err)
	}
	if _, _, _, _, _, err := parse("id="); err == nil {
		t.Error("no id")
	}
	if _, at, _, _, _, err := parse("id=x&at=0"); err != nil || !at.Equal(time.UnixMilli(0)) {
		t.Errorf("at=0 is the epoch: %v %v", at, err)
	}
	for _, bad := range []string{"at=-1", "at=x", "window=2h", "window=0", "topic=t&partition=-1&offset=0", "topic=t&partition=0&offset=-1", "topic=t&partition=x&offset=0", "topic=t&partition=0&offset=x"} {
		if _, _, _, _, _, err := parse("id=x&" + bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	for key, want := range map[string]time.Duration{"": 5 * time.Minute, "1m": time.Minute, "5m": 5 * time.Minute, "30m": 30 * time.Minute, "3h": 3 * time.Hour} {
		_, _, w, k, _, err := parse("id=x&window=" + key)
		if err != nil || w != want || (key != "" && k != key) || (key == "" && k != "5m") {
			t.Errorf("window %q: %v %q %v", key, w, k, err)
		}
	}
	_, _, _, _, anchor, err := parse("id=x&topic=orders&partition=3&offset=0")
	if err != nil || !anchor.Set || anchor.Topic != "orders" || anchor.Partition != 3 || anchor.Offset != 0 {
		t.Errorf("anchor %+v %v", anchor, err)
	}
	if _, _, _, _, anchor, _ = parse("id=x"); anchor.Set {
		t.Error("an anchor with no topic")
	}
	// A pasted header value is read as the id it carries.
	if id, _, _, _, _, _ := parse("id=" + url.QueryEscape("00-"+traceA+"-"+spanA+"-01")); id != traceA {
		t.Errorf("traceparent: %q", id)
	}
}

func TestTraceHomeStatesItsBudget(t *testing.T) {
	c := newTestConsole(t)
	body := readable(req(t, c, "GET", "/_console/trace", nil).Body.String())
	if !strings.Contains(body, "50,000") || !strings.Contains(body, "8 second") {
		t.Errorf("the home page does not say what a scan reads:\n%s", body)
	}
	old, oldT := traceMaxRecords, traceMaxTime
	traceMaxRecords, traceMaxTime = 1234, 3*time.Second
	defer func() { traceMaxRecords, traceMaxTime = old, oldT }()
	body = readable(req(t, c, "GET", "/_console/trace", nil).Body.String())
	if !strings.Contains(body, "1,234") || !strings.Contains(body, "3 second") {
		t.Errorf("the budget is not the one in force:\n%s", body)
	}
}

func TestTraceDataOfAFailedScan(t *testing.T) {
	c := newTestConsole(t)
	j := &traceJob{id: "j", phase: "done", err: errors.New("the broker went away"), started: time.Now().Add(-time.Second), finished: time.Now()}
	d := c.traceData(j)
	if d["ScanFailed"] != true || d["Running"] != false || d["Took"] == nil {
		t.Errorf("%v", d)
	}
	if f := d["Refusal"].(Refusal); !strings.HasPrefix(f.Message, "The scan failed: ") {
		t.Errorf("message %q", f.Message)
	}
	run := &traceJob{id: "k", phase: "scanning"}
	d = c.traceData(run)
	if d["Running"] != true || d["ScanFailed"] != nil || d["Took"] != nil {
		t.Errorf("a running scan: %v", d)
	}
	// What the poll compares changes with the error and with progress.
	h1 := c.traceData(&traceJob{id: "a", phase: "done"})["Hash"]
	h2 := c.traceData(&traceJob{id: "a", phase: "done", err: errors.New("x")})["Hash"]
	if h1 == h2 {
		t.Error("a scan that failed hashes like one that did not")
	}
}

// The page offers the other spans, each one linking to the same question.
func TestTracePageOffersTheOtherWindows(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "orders", 1, 0)
	now := time.Now().Truncate(time.Millisecond)
	writeAll(t, addr, []sent{{"orders", now.Add(-time.Second), tp(traceA), "k"}})
	path := fmt.Sprintf("/_console/trace?id=%s&at=%d&window=30m&topic=orders&partition=0&offset=0", traceA, now.UnixMilli())
	body := waitTrace(t, c, path)
	var on []string
	for _, m := range segOnRe.FindAllStringSubmatch(body, -1) {
		on = append(on, m[1])
	}
	if len(on) != 1 || on[0] != "30 minutes" {
		t.Errorf("windows on: %v", on)
	}
	for _, key := range []string{"1m", "30m", "3h"} {
		if !strings.Contains(body, "window="+key) {
			t.Errorf("no link for the %s window", key)
		}
	}
	if !strings.Contains(body, "topic=orders") {
		t.Error("the links forget the record the person started from")
	}
	// No starting record: the links do not invent one.
	plain := waitTrace(t, c, fmt.Sprintf("/_console/trace?id=%s&at=%d", traceA, now.UnixMilli()))
	if strings.Contains(between(plain, `class="seg-link`, "</div>"), "topic=") {
		t.Error("window links name a topic although none was chosen")
	}
}

var segOnRe = regexp.MustCompile(`<a class="seg-link on" href="[^"]*" aria-current="true">([^<]+)</a>`)
