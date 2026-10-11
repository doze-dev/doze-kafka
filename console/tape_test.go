package console

import (
	"context"
	"encoding/base64"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func produceTo(t *testing.T, c *Console, topic string, partition int, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		rec := req(t, c, "POST", "/_console/topics/"+topic+"/produce", url.Values{
			"partition": {itoa(partition)}, "value": {"v" + itoa(i)}, "acks": {"1"},
		})
		if rec.Code != 200 {
			t.Fatalf("produce: %d %s", rec.Code, rec.Body)
		}
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

// Kafka prints a topic's id as URL-safe base64 without padding (22 characters);
// the console showed raw hex, which matches nothing a developer sees elsewhere.
func TestTopicIDIsPrintedTheWayKafkaPrintsIt(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"1"}})
	body := req(t, c, "GET", "/_console/topics/t", nil).Body.String()
	m := regexp.MustCompile(`class="cc-val" title="([^"]+)"`).FindStringSubmatch(body)
	if m == nil {
		t.Fatal("no topic id chip on the page")
	}
	if len(m[1]) != 22 || strings.ContainsAny(m[1], "+/=") {
		t.Fatalf("topic id %q: want 22 URL-safe base64 characters", m[1])
	}
	raw, err := base64.RawURLEncoding.DecodeString(m[1])
	if err != nil || len(raw) != 16 {
		t.Fatalf("topic id %q does not decode to a 16-byte UUID: %v", m[1], err)
	}
	detail, _ := c.be.DescribeTopic(context.Background(), "t")
	if detail.ID != m[1] {
		t.Errorf("page shows %q, backend says %q", m[1], detail.ID)
	}
}

func TestTabsCarryPerPartitionCountsAndTheTapeSaysRecordsNotTotal(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"3"}})
	produceTo(t, c, "t", 1, 2)
	produceTo(t, c, "t", 2, 1)

	body := req(t, c, "GET", "/_console/topics/t?partition=1", nil).Body.String()
	for _, want := range []string{
		`>P1<span class="pc">2</span>`, `>P2<span class="pc">1</span>`, // counts on the tabs that have records.
		"2 records", "log end", "log start 0",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the topic page is missing %q", want)
		}
	}
	if strings.Contains(body, `>P0<span class="pc">`) {
		t.Error("an empty partition should not show a count")
	}
	if strings.Contains(body, " total</span>") {
		t.Error("the tape still says 'total', which was the log-end offset, not a count")
	}
}

func TestManyPartitionsGetASelectNotOverflowingTabs(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"wide"}, "partitions": {"40"}})
	body := req(t, c, "GET", "/_console/topics/wide", nil).Body.String()
	if !strings.Contains(body, `class="input psel"`) || strings.Contains(body, `class="ptabs"`) {
		t.Error("a 40-partition topic should use a partition select, not 40 tabs")
	}
	if !strings.Contains(body, "P39 · 0 records") {
		t.Error("every partition should be selectable")
	}
}

// Producing to ANY partition must change what the live tab polls, or the tabs'
// counts would only refresh when the viewed partition happened to change.
func TestLiveHashCoversEveryPartition(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"2"}})
	produceTo(t, c, "t", 0, 1)

	first := doReq(t, c, "GET", "/_console/topics/t/tape?partition=0", map[string]string{"HX-Request": "true"})
	hash := first.Header().Get("HX-Live-Hash")
	if hash == "" {
		t.Fatal("no live hash")
	}
	if rec := doReq(t, c, "GET", "/_console/topics/t/tape?partition=0&h="+url.QueryEscape(hash), map[string]string{"HX-Request": "true"}); rec.Code != 204 {
		t.Fatalf("an unchanged tape should answer 204, got %d", rec.Code)
	}
	produceTo(t, c, "t", 1, 1) // a different partition.
	if rec := doReq(t, c, "GET", "/_console/topics/t/tape?partition=0&h="+url.QueryEscape(hash), map[string]string{"HX-Request": "true"}); rec.Code != 200 {
		t.Fatalf("a produce to another partition should refresh the tabs, got %d", rec.Code)
	}
}

func TestJumpToOffsetSaysWhatItDid(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"2"}})
	produceTo(t, c, "t", 0, 5)

	in := req(t, c, "GET", "/_console/topics/t?partition=0&jump=3", nil).Body.String()
	if !strings.Contains(in, `id="msg-3"`) || !strings.Contains(in, `class="hl" data-hl`) || strings.Contains(in, "jumpnote") {
		t.Error("jumping to a held offset should highlight its row and say nothing")
	}
	past := req(t, c, "GET", "/_console/topics/t?partition=0&jump=99", nil).Body.String()
	if !strings.Contains(past, "Offset 99 has not been written yet (the newest is 4)") {
		t.Errorf("a jump past the end should say so, with the newest offset:\n%s", extractNote(past))
	}
	empty := req(t, c, "GET", "/_console/topics/t?partition=1&jump=0", nil).Body.String()
	if !strings.Contains(empty, "no messages to jump to") {
		t.Error("a jump on an empty partition should say there is nothing to jump to")
	}
	// The control itself is a form with a button, not Enter-only script.
	if !strings.Contains(in, `<form class="jump" method="get"`) || !strings.Contains(in, `jump-go`) {
		t.Error("jump-to-offset should be a form with a Go button")
	}
}

func extractNote(body string) string {
	i := strings.Index(body, "jumpnote")
	if i < 0 {
		return "(no note)"
	}
	return body[i : i+200]
}

func TestInspectorIsADialogWithNeighbours(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"m"}, "partitions": {"1"}})
	produceTo(t, c, "m", 0, 3)

	page := req(t, c, "GET", "/_console/topics/m", nil).Body.String()
	for _, want := range []string{`role="dialog"`, `aria-modal="true"`, `aria-labelledby="inspector-title"`, `data-inspector-open`, `tabindex="0"`} {
		if !strings.Contains(page, want) {
			t.Errorf("the page is missing %s", want)
		}
	}
	if !strings.Contains(page, `id="compose-head" data-dock-toggle aria-expanded="false"`) {
		t.Error("the composer toggle should be a button that reports expanded/collapsed")
	}

	mid := inspectorFor(t, c, "1")
	// " disabled" is the bare attribute (hx-disabled-elt does not count).
	if !strings.Contains(mid, "offset=0") || !strings.Contains(mid, "offset=2") || strings.Contains(mid, `" disabled`) {
		t.Error("a middle record should link to both neighbours")
	}
	if first := inspectorFor(t, c, "0"); !strings.Contains(first, `aria-label="Older record"`) || !strings.Contains(first, `" disabled`) {
		t.Error("the oldest record has no older neighbour: that button should be disabled")
	}
	if last := inspectorFor(t, c, "2"); strings.Count(last, `" disabled`) != 1 {
		t.Error("the newest record has no newer neighbour: exactly that button should be disabled")
	}
}
