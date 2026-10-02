package console

import (
	"net/url"
	"regexp"
	"strings"
	"testing"
)

// litRailItem returns the label of the rail item the server rendered as active.
func litRailItem(t *testing.T, body string) string {
	t.Helper()
	m := regexp.MustCompile(`(?s)<a href="[^"]*" class="ri on">.*?<span class="ri-nm">([^<]*)</span>`).FindStringSubmatch(body)
	if m == nil {
		return ""
	}
	return m[1]
}

func TestPagesAreStandardsModeDocuments(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"1"}})
	for _, path := range []string{"/_console/", "/_console/topics/t", "/_console/groups", "/_console/cluster", "/_console/new/topic"} {
		body := req(t, c, "GET", path, nil).Body.String()
		if !strings.HasPrefix(strings.TrimSpace(body), "<!DOCTYPE html>") {
			t.Errorf("%s does not start with a doctype (quirks mode):\n%.120s", path, body)
		}
		for _, want := range []string{`<html lang="en">`, "<head>", "</head>", "<body>", "</body>", "</html>"} {
			if !strings.Contains(body, want) {
				t.Errorf("%s is missing %s", path, want)
			}
		}
	}
}

// Back/Forward must be a real page load, or htmx restores a snapshot into <body>
// and every node shell.js holds goes stale.
func TestBackNavigationIsAFullPageLoad(t *testing.T) {
	c := newTestConsole(t)
	body := req(t, c, "GET", "/_console/topics", nil).Body.String()
	m := regexp.MustCompile(`<meta name="htmx-config" content='([^']*)'>`).FindStringSubmatch(body)
	if m == nil {
		t.Fatal("no htmx-config meta: history restore is left at htmx's default")
	}
	if !strings.Contains(m[1], `"historyCacheSize":0`) || !strings.Contains(m[1], `"refreshOnHistoryMiss":true`) {
		t.Fatalf("htmx-config = %s; want history caching off and reload on a miss", m[1])
	}
	if strings.Contains(body, "idiomorph") {
		t.Error("the page still loads idiomorph, which nothing uses")
	}
}

// The rail's highlight is rendered by the server for the first paint (and for
// anyone without scripts); a handler's Page used to be overwritten with the
// template name, so detail pages lit nothing.
func TestRailHighlightIsRightOnEveryPage(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"1"}})
	for path, want := range map[string]string{
		"/_console/":          "Topics",
		"/_console/topics":    "Topics",
		"/_console/topics/t":  "Topics",
		"/_console/new/topic": "Topics",
		"/_console/groups":    "Consumer groups",
		"/_console/cluster":   "Cluster",
	} {
		if got := litRailItem(t, req(t, c, "GET", path, nil).Body.String()); got != want {
			t.Errorf("%s lights %q in the rail, want %q", path, got, want)
		}
	}
}

func TestTapeHasAPauseControl(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"1"}})
	body := req(t, c, "GET", "/_console/topics/t", nil).Body.String()
	if !strings.Contains(body, `data-live-toggle`) || !strings.Contains(body, `aria-pressed="false"`) {
		t.Error("the live tape should offer a pause toggle, unpressed by default")
	}
}
