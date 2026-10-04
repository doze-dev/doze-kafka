package console

import (
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// litRailItem returns the label of the rail item the server rendered as active.
func litRailItem(t *testing.T, body string) string {
	t.Helper()
	m := regexp.MustCompile(`(?s)<a href="[^"]*" class="ri on"[^>]*>.*?<span class="ri-nm">([^<]*)</span>`).FindStringSubmatch(body)
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

// The dialogs and controls a keyboard or screen-reader user depends on are named in
// the markup the server sends, so they hold without any script running.
func TestShellMarkupIsLabelled(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"1"}})
	page := req(t, c, "GET", "/_console/topics/t", nil).Body.String()
	for _, want := range []string{
		`role="dialog" aria-modal="true" aria-label="Command palette"`,
		`role="alertdialog" aria-modal="true" aria-labelledby="confirm-title" aria-describedby="confirm-msg"`,
		`class="ri on" aria-label="Topics"`, // the slim rail hides the text; the name must remain.
		`<nav class="rail" aria-label="Sections"`,
		`<main class="detail`,
		`aria-label="Filter Topics"`,
		`data-confirm-title="Delete topic"`,
	} {
		if !strings.Contains(page, want) {
			t.Errorf("the topic page is missing %q", want)
		}
	}
	if strings.Contains(page, `<div class="tick`) {
		t.Error("tape ticks should be buttons, not clickable divs")
	}
}

// The confirm dialog's default answer is Cancel; Enter on a destructive dialog
// must not be the dangerous one.
func TestConfirmFocusesCancel(t *testing.T) {
	js, err := staticFS.ReadFile("static/shell.js")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(js), "no.focus();") || strings.Contains(string(js), "yes.focus();") {
		t.Error("the confirm dialog should focus Cancel, not the destructive button")
	}
}

// The palette's catalogue comes from the server, views and actions included.
func TestPaletteCatalogueIsServedWithViewsAndActions(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"a"}, "partitions": {"1"}})
	body := req(t, c, "GET", "/_console/api/palette", nil).Body.String()
	for _, want := range []string{`"n":"Topics","u":"/_console/topics","k":"view"`, `"n":"Consumer groups"`, `"n":"Cluster"`,
		`"n":"Create topic","u":"/_console/new/topic","k":"action"`, `"n":"a","u":"/_console/topics/a","k":"topic"`} {
		if !strings.Contains(body, want) {
			t.Errorf("the palette catalogue is missing %s in %s", want, body)
		}
	}
	js, _ := staticFS.ReadFile("static/shell.js")
	if strings.Contains(string(js), `"Create topic"`) {
		t.Error("shell.js still carries its own copy of the palette's actions")
	}
}

// Static files revalidate: an ETag on the first request, a 304 on the second.
func TestStaticFilesRevalidate(t *testing.T) {
	c := newTestConsole(t)
	first := req(t, c, "GET", "/_console/static/app.css", nil)
	tag := first.Header().Get("ETag")
	if first.Code != 200 || tag == "" || first.Header().Get("Cache-Control") != "no-cache" {
		t.Fatalf("first fetch: %d etag %q cache-control %q", first.Code, tag, first.Header().Get("Cache-Control"))
	}
	again := doReq(t, c, "GET", "/_console/static/app.css", map[string]string{"If-None-Match": tag})
	if again.Code != http.StatusNotModified || again.Body.Len() != 0 {
		t.Errorf("revalidation: %d with %d body bytes, want 304 and none", again.Code, again.Body.Len())
	}
}

// Dev mode reads templates and static files from disk, live, so editing the console
// does not need a rebuild.
func TestDevModeServesFromDisk(t *testing.T) {
	dir := t.TempDir()
	for _, sub := range []string{"templates", "static"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// A copy of the real templates, then one edited after the console started.
	entries, _ := templateFS.ReadDir("templates")
	for _, e := range entries {
		b, _ := templateFS.ReadFile("templates/" + e.Name())
		os.WriteFile(filepath.Join(dir, "templates", e.Name()), b, 0o644)
	}
	os.WriteFile(filepath.Join(dir, "static", "app.css"), []byte("/* v1 */"), 0o644)

	t.Setenv("DOZE_KAFKA_CONSOLE_DEV", dir)
	c := newTestConsole(t)
	if body := req(t, c, "GET", "/_console/static/app.css", nil).Body.String(); body != "/* v1 */" {
		t.Fatalf("static file not read from disk: %q", body)
	}
	os.WriteFile(filepath.Join(dir, "static", "app.css"), []byte("/* v2 */"), 0o644)
	rec := req(t, c, "GET", "/_console/static/app.css", nil)
	if rec.Body.String() != "/* v2 */" || rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("an edit did not show: %q, cache-control %q", rec.Body.String(), rec.Header().Get("Cache-Control"))
	}
	p := filepath.Join(dir, "templates", "cluster.html")
	b, _ := os.ReadFile(p)
	os.WriteFile(p, []byte(strings.Replace(string(b), "One broker.", "One DEV broker.", 1)), 0o644)
	if !strings.Contains(req(t, c, "GET", "/_console/cluster", nil).Body.String(), "One DEV broker.") {
		t.Error("an edited template did not show without a restart")
	}
}
