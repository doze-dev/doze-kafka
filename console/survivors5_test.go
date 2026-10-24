package console

import (
	"context"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"
)

// The answers to a produce, a resend and a poll of the tape are the newest window of a log
// that is longer than a window: no earlier page, no row picked out.
func TestTapeAnswersOfALongLogAreTheNewestWindow(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(context.Background(), "long", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "long", 0, 120, vals("m"), time.Time{})
	offsets := func(body string) (lowest, highest int, highlighted bool) {
		lowest = 1 << 30
		for _, m := range regexp.MustCompile(`id="msg-(\d+)"`).FindAllStringSubmatch(body, -1) {
			n := 0
			for _, ch := range m[1] {
				n = n*10 + int(ch-'0')
			}
			lowest, highest = min(lowest, n), max(highest, n)
		}
		return lowest, highest, strings.Contains(body, "data-hl")
	}
	check := func(what, body string, top int) {
		t.Helper()
		lo, hi, hl := offsets(body)
		if hi != top || lo != top-59 || hl {
			t.Errorf("%s: rows %d..%d (highlighted: %v), want %d..%d", what, lo, hi, hl, top-59, top)
		}
	}
	produce := req(t, c, "POST", "/_console/topics/long/produce", url.Values{"partition": {"0"}, "value": {"x"}, "acks": {"1"}})
	if produce.Code != 200 {
		t.Fatalf("produce: %d", produce.Code)
	}
	check("produce", produce.Body.String(), 120)
	resend := req(t, c, "POST", "/_console/topics/long/resend", url.Values{"partition": {"0"}, "offset": {"3"}})
	if resend.Code != 200 {
		t.Fatalf("resend: %d", resend.Code)
	}
	check("resend", resend.Body.String(), 121)
	tape := req(t, c, "GET", "/_console/topics/long/tape", nil)
	if tape.Code != 200 {
		t.Fatalf("tape: %d", tape.Code)
	}
	check("tape", tape.Body.String(), 121)
}

// ...and on a short log, in which every row is on the page, none is picked out.
func TestTapeAnswersOfAShortLogPickOutNoRow(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "short", 1, 5)
	for what, rec := range map[string]*httptest.ResponseRecorder{
		"produce": req(t, c, "POST", "/_console/topics/short/produce", url.Values{"partition": {"0"}, "value": {"x"}, "acks": {"1"}}),
		"resend":  req(t, c, "POST", "/_console/topics/short/resend", url.Values{"partition": {"0"}, "offset": {"2"}}),
		"tape":    req(t, c, "GET", "/_console/topics/short/tape", nil),
	} {
		body := rec.Body.String()
		if rec.Code != 200 || strings.Contains(body, "data-hl") || !strings.Contains(body, `id="msg-0"`) {
			t.Errorf("%s: %d, a row picked out: %v, row 0 shown: %v", what, rec.Code, strings.Contains(body, "data-hl"), strings.Contains(body, `id="msg-0"`))
		}
	}
}

func TestACLInspectorCountsItsNeighboursWithTheRightSign(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = &fakeTraffic{} })
	for _, op := range []string{"read", "write", "delete"} {
		req(t, c, "POST", "/_console/new/acl", url.Values{"principal": {"User:p"}, "host": {"*"}, "resource": {"topic"}, "name": {"t"}, "pattern": {"literal"}, "operation": {op}, "permission": {"allow"}})
	}
	a := ACLRow{Principal: "User:p", Host: "*", Resource: "topic", Name: "t", Pattern: "literal", Operation: "read", Permission: "allow"}
	body := readable(req(t, c, "GET", "/_console/inspect/acl?key="+url.QueryEscape(a.Key()), nil).Body.String())
	if !regexp.MustCompile(`(^|\s)2 other ACLs are about it too\.`).MatchString(body) {
		t.Errorf("%s", body)
	}
}

func TestVersionsTableCountsChangedLinesWithTheRightSign(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-value", userV2)
	text := readable(req(t, c, "GET", "/_console/schemas/orders-value", nil).Body.String())
	if !regexp.MustCompile(`(^|\s)8 lines changed from v1`).MatchString(text) {
		t.Errorf("%s", between(text, "VersionIdWhat", "Schema v2"))
	}
}
