package console

import (
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseAt(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		in   string
		want time.Time
		bad  bool
	}{
		{"15m", now.Add(-15 * time.Minute), false},
		{"2h", now.Add(-2 * time.Hour), false},
		{"1d", now.Add(-24 * time.Hour), false},
		{"90s", now.Add(-90 * time.Second), false},
		{"15m ago", now.Add(-15 * time.Minute), false},
		{" 3 h ", now.Add(-3 * time.Hour), false},
		{"2026-10-07 10:30", time.Date(2026, 10, 7, 10, 30, 0, 0, time.UTC), false},
		{"2026-10-07T10:30:15", time.Date(2026, 10, 7, 10, 30, 15, 0, time.UTC), false},
		{"2026-10-07", time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC), false},
		{"2026-10-07T10:30:00+05:30", time.Date(2026, 10, 7, 5, 0, 0, 0, time.UTC), false},
		{"1760000000000", time.UnixMilli(1760000000000).UTC(), false},
		{"1760000000", time.Unix(1760000000, 0).UTC(), false},
		{"", time.Time{}, true},
		{"soon", time.Time{}, true},
		{"-5m", time.Time{}, true},
		{"-1", time.Time{}, true},
		{"2026-13-45", time.Time{}, true},
		{"m", time.Time{}, true},
	} {
		got, err := parseAt(tc.in, now)
		if tc.bad != (err != nil) || (!tc.bad && !got.Equal(tc.want)) {
			t.Errorf("parseAt(%q) = %v, %v; want %v (bad=%v)", tc.in, got, err, tc.want, tc.bad)
		}
	}
}

// "Go to time" lands on the first message at or after the time, says so, and says
// plainly when there is none or when what was typed is not a time.
func TestGoToTime(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"when"}, "partitions": {"1"}})
	for _, v := range []string{"a", "b", "c"} {
		req(t, c, "POST", "/_console/topics/when/produce", url.Values{"partition": {"0"}, "value": {v}, "acks": {"1"}})
	}

	page := func(at string) string {
		return req(t, c, "GET", "/_console/topics/when?at="+url.QueryEscape(at), nil).Body.String()
	}
	body := page("1h")
	if !strings.Contains(body, "Showing from the first message at or after") || !strings.Contains(body, "(offset 0)") {
		t.Errorf("an hour ago: no landing note\n%s", noteOf(body))
	}
	if !strings.Contains(body, `id="jump-at"`) {
		t.Error("the page has no go-to-time box")
	}
	future := strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10)
	if body := page(future); !strings.Contains(body, "No message on this partition is at or after") {
		t.Errorf("an hour from now: %s", noteOf(body))
	}
	if body := page("whenever"); !strings.Contains(body, "That is not a time") {
		t.Errorf("garbage: %s", noteOf(body))
	}
}

// noteOf is the jump note of a page, for a failure message.
func noteOf(body string) string {
	i := strings.Index(body, `class="jumpnote"`)
	if i < 0 {
		return "(no note)"
	}
	return body[i:min(i+300, len(body))]
}
