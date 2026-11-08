package console

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Nothing formats a time for a page but the one formatter: a template that calls
// .Format, reads a pre-formatted time field, or computes its own "ago" fails here.
func TestEveryTemplateFormatsTimeWithTheOneFormatter(t *testing.T) {
	forbidden := []*regexp.Regexp{
		regexp.MustCompile(`\.Format\b`),
		regexp.MustCompile(`\.TimeStr\b`),
		regexp.MustCompile(`\.TimeFull\b`),
		regexp.MustCompile(`\.AtStr\b`),
		regexp.MustCompile(`\.UnixMilli\b`),
		regexp.MustCompile(`\{\{\s*ago\b`),
		regexp.MustCompile(`\(ago\b`),
		regexp.MustCompile(`\.RFC3339`),
	}
	files, _ := filepath.Glob("templates/*.html")
	for _, f := range files {
		b, _ := os.ReadFile(f)
		for i, line := range strings.Split(string(b), "\n") {
			for _, re := range forbidden {
				if re.MatchString(line) {
					t.Errorf("%s:%d formats time on its own (%s): use {{when t \"style\"}}", f, i+1, re)
				}
			}
		}
	}
	// And the Go that feeds them does not hand a template a string it formatted:
	// the views carry the time.Time, and the formatter writes it. The one place that
	// states a time in a sentence says it is UTC.
	allowed := map[string]string{"timefmt.go": "the formatter itself", "gototime.go": "a sentence that quotes the UTC time the person typed"}
	goFiles, _ := filepath.Glob("*.go")
	re := regexp.MustCompile(`\.Format\("`)
	for _, f := range goFiles {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		if _, ok := allowed[f]; ok {
			continue
		}
		if b, _ := os.ReadFile(f); re.Match(b) {
			t.Errorf("%s formats a time itself; use whenText or {{when}}", f)
		}
	}
	// Every style a template names is one the formatter knows.
	use := regexp.MustCompile(`\{\{\s*(?:when|whenText) [^}]*?"([a-z]+)"\s*\}\}|\(whenText [^)]*?"([a-z]+)"\)`)
	for _, f := range files {
		b, _ := os.ReadFile(f)
		for _, m := range use.FindAllStringSubmatch(string(b), -1) {
			style := m[1] + m[2]
			if _, ok := timeStyles[style]; !ok {
				t.Errorf("%s uses the time style %q, which the formatter does not have", f, style)
			}
		}
	}
}

// The server's text for a moment is UTC and says so; the instant and the style are
// in the element for the browser to show in its own zone.
func TestWhenWritesTheInstantAndAUTCFallback(t *testing.T) {
	at := time.Date(2026, 3, 14, 23, 59, 7, 123_000_000, time.UTC)
	got := string(when(at.In(time.FixedZone("elsewhere", 5*3600+1800)), StyleClockMs))
	for _, want := range []string{`datetime="2026-03-14T23:59:07.123Z"`, `data-style="clockms"`, ">23:59:07.123 UTC</time>", `title="2026-03-14 23:59:07.123 UTC"`} {
		if !strings.Contains(got, want) {
			t.Errorf("when() = %s, missing %s", got, want)
		}
	}
	if got := whenText(at, StyleDateTime); got != "2026-03-14 23:59:07 UTC" {
		t.Errorf("whenText = %q", got)
	}
	if got := string(when(time.Time{}, StyleClock)); !strings.Contains(got, "—") {
		t.Errorf("a zero time shows %q", got)
	}
	if got := whenText(time.Now().Add(-3*time.Minute), StyleAgo); got != "3m ago" {
		t.Errorf("ago = %q", got)
	}
	// An unknown style is the clock, not a broken page.
	if got := string(when(at, "nonsense")); !strings.Contains(got, `data-style="clock"`) {
		t.Errorf("unknown style: %s", got)
	}
}

// The page: a record's time reaches the browser as an instant, not as the server's
// local text, so a server in any zone shows the same thing.
func TestARecordsTimeIsAnInstantNotTheServersLocalText(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "clock", 1, 1)
	page := req(t, c, "GET", "/_console/topics/clock", nil).Body.String()
	if !regexp.MustCompile(`<time class="when" datetime="\d{4}-\d\d-\d\dT[\d:.]+Z" data-style="clock"`).MatchString(page) {
		t.Errorf("the record's time is not an instant:\n%s", between(page, `cell-time`, `</td>`))
	}
	ins := req(t, c, "GET", "/_console/topics/clock/message?partition=0&offset=0", nil).Body.String()
	if !strings.Contains(ins, `data-style="datetimems"`) {
		t.Error("the inspector's timestamp is not the formatter's")
	}
}

// The Schemas boards' "Registered": by the day it fell on.
func TestDayText(t *testing.T) {
	now := time.Date(2026, 11, 8, 14, 36, 0, 0, time.UTC)
	for at, want := range map[time.Time]string{
		time.Date(2026, 11, 8, 14, 2, 0, 0, time.UTC):  "14:02 today",
		time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC):   "00:00 today",
		time.Date(2026, 11, 7, 16, 48, 0, 0, time.UTC): "yesterday 16:48",
		time.Date(2026, 11, 5, 23, 59, 0, 0, time.UTC): "3 days ago",
		time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC):   "2026-11-01",
		time.Date(2026, 11, 9, 1, 0, 0, 0, time.UTC):   "01:00 today", // a clock ahead is not "tomorrow"
	} {
		if got := dayText(at, now); got != want {
			t.Errorf("%v: %q, want %q", at, got, want)
		}
	}
	if got := whenText(time.Now().Add(-time.Minute), StyleDay); !strings.HasSuffix(got, " today UTC") && !strings.HasPrefix(got, "yesterday") {
		t.Errorf("the server's own text: %q", got)
	}
}
