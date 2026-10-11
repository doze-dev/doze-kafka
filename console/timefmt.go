package console

// One formatter for time. Every moment a template shows goes through `when`, which
// writes a <time> element that carries the instant (datetime=, in UTC) and the
// style it is shown in. The server's text is in UTC and says so, so a page without
// scripts is correct; shell.js (dozeTime) then shows it in the browser's own zone,
// puts the zone and the UTC instant in the hover title, and keeps "ago" and
// "since" counting in the browser. The record table, the request table, the group
// timeline and the lineage view all use it, so they agree.
//
// Nothing else formats a time for a page: a test fails for a template that calls
// .Format or reads a pre-formatted time (timefmt_test.go).

import (
	"fmt"
	"html"
	"html/template"
	"time"
)

// Time styles. shell.js implements the same set.
const (
	StyleClock    = "clock"    // 15:04:05
	StyleClockMs  = "clockms"  // 15:04:05.000
	StyleDateTime = "datetime" // 2006-01-02 15:04:05
	StyleDate     = "date"     // 2006-01-02
	StyleAgo      = "ago"      // 3m ago, counted in the browser
	StyleSince    = "since"    // 3m 10s, counted in the browser
)

var timeStyles = map[string]string{
	StyleClock:    "15:04:05",
	StyleClockMs:  "15:04:05.000",
	StyleDateTime: "2006-01-02 15:04:05",
	StyleDate:     "2006-01-02",
	StyleAgo:      "",
	StyleSince:    "",
}

// whenText is the server's own text for a moment: UTC, and it says so. It is the
// fallback inside <time>, and the text of an attribute (an aria-label) that markup
// cannot go in.
func whenText(t time.Time, style string) string {
	if t.IsZero() {
		return "—"
	}
	t = t.UTC()
	switch style {
	case StyleAgo:
		return agoText(time.Since(t))
	case StyleSince:
		return spanText(time.Since(t))
	}
	layout, ok := timeStyles[style]
	if !ok {
		layout = timeStyles[StyleClock]
	}
	return t.Format(layout) + " UTC"
}

func agoText(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
	return fmt.Sprintf("%dd ago", int(d.Hours()/24))
}

// when is the template function: {{when .Time "clockms"}}.
func when(t time.Time, style string) template.HTML {
	if t.IsZero() {
		return `<span class="dim">—</span>`
	}
	if _, ok := timeStyles[style]; !ok {
		style = StyleClock
	}
	u := t.UTC()
	return template.HTML(fmt.Sprintf(`<time class="when" datetime="%s" data-style="%s" title="%s">%s</time>`,
		u.Format("2006-01-02T15:04:05.000Z"), style, html.EscapeString(u.Format("2006-01-02 15:04:05.000")+" UTC"),
		html.EscapeString(whenText(t, style))))
}
