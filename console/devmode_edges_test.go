package console

import (
	"bytes"
	"io/fs"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *lockedBuf) String() string               { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

// copyTemplates puts the embedded templates in a directory, as a developer's tree has them.
func copyTemplates(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "templates"), 0o755); err != nil {
		t.Fatal(err)
	}
	ents, err := fs.ReadDir(templateFS, "templates")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		b, err := fs.ReadFile(templateFS, "templates/"+e.Name())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "templates", e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// Developing the console: the templates are read from the directory on every request,
// and one that does not parse leaves the last good set in use, and says so in the log.
func TestDevModeReadsTemplatesFromDiskEveryTime(t *testing.T) {
	dir := copyTemplates(t)
	t.Setenv("DOZE_KAFKA_CONSOLE_DEV", dir)
	buf := &lockedBuf{}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Logger = slog.New(slog.NewTextHandler(buf, nil)) })
	if c.templates().Lookup("zz_dev") != nil {
		t.Fatal("a template that is not there is found")
	}
	if err := os.WriteFile(filepath.Join(dir, "templates", "zz_dev.html"), []byte(`{{define "zz_dev"}}from disk{{end}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if c.templates().Lookup("zz_dev") == nil {
		t.Error("a template added to the directory is not seen on the next request")
	}
	if err := os.WriteFile(filepath.Join(dir, "templates", "zz_dev.html"), []byte(`{{define "zz_dev"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if c.templates().Lookup("topics_home") == nil {
		t.Error("a template that does not parse leaves the page with no templates")
	}
	if !strings.Contains(buf.String(), "the templates do not parse") || !strings.Contains(buf.String(), "serving the last good set") {
		t.Errorf("the given logger was not told:\n%s", buf.String())
	}
	if rec := req(t, c, "GET", "/_console/topics", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "Topics") {
		t.Errorf("a page in dev mode: %d", rec.Code)
	}
}

// Not in dev mode, the embedded set is parsed once and the directory is not looked at.
func TestEmbeddedTemplatesAreParsedOnce(t *testing.T) {
	t.Setenv("DOZE_KAFKA_CONSOLE_DEV", "")
	c := newTestConsole(t)
	if c.dev != "" {
		t.Fatalf("dev mode with the variable empty: %q", c.dev)
	}
	if a, b := c.templates(), c.templates(); a != b {
		t.Error("the embedded templates are parsed on every request")
	}
	if c.templates().Lookup("topics_home") == nil {
		t.Error("no embedded templates")
	}
}

// A console given no logger logs to the default one, and is not undone by a template
// that does not parse.
func TestConsoleWithoutALoggerUsesTheDefault(t *testing.T) {
	dir := copyTemplates(t)
	if err := os.WriteFile(filepath.Join(dir, "templates", "zz.html"), []byte(`{{define "zz"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOZE_KAFKA_CONSOLE_DEV", dir)
	buf := &lockedBuf{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	defer slog.SetDefault(old)
	c := newTestConsole(t)
	if c.log == nil {
		t.Fatal("no logger")
	}
	if rec := req(t, c, "GET", "/_console/topics", nil); rec.Code != 200 && rec.Code != 500 {
		t.Errorf("%d", rec.Code)
	}
	if !strings.Contains(buf.String(), "do not parse") {
		t.Errorf("the default logger was not used:\n%s", buf.String())
	}
}

func TestRedirectAndFlash(t *testing.T) {
	c := newTestConsole(t)
	w := httptest.NewRecorder()
	c.redirect(w, httptest.NewRequest("POST", "/x", nil), "/_console/topics", "")
	if w.Code != 303 || w.Header().Get("Location") != "/_console/topics" {
		t.Errorf("no flash: %d %s", w.Code, w.Header().Get("Location"))
	}
	w = httptest.NewRecorder()
	c.redirect(w, httptest.NewRequest("POST", "/x", nil), "/_console/topics", "Done & dusted")
	if got := w.Header().Get("Location"); got != "/_console/topics?flash="+url.QueryEscape("Done & dusted") {
		t.Errorf("flash: %s", got)
	}
	w = httptest.NewRecorder()
	c.redirect(w, httptest.NewRequest("POST", "/x", nil), "/_console/topics?partition=2", "Hi")
	if got := w.Header().Get("Location"); got != "/_console/topics?partition=2&flash=Hi" {
		t.Errorf("flash after a query: %s", got)
	}
	r := httptest.NewRequest("POST", "/x", nil)
	r.Header.Set("HX-Request", "true")
	w = httptest.NewRecorder()
	c.redirect(w, r, "/_console/topics", "Hi")
	if w.Code != 200 || w.Header().Get("HX-Redirect") != "/_console/topics?flash=Hi" {
		t.Errorf("htmx: %d %s", w.Code, w.Header().Get("HX-Redirect"))
	}
	// A page shows the flash it was sent with, and none otherwise.
	with := req(t, c, "GET", "/_console/topics?flash=Topic+created", nil).Body.String()
	without := req(t, c, "GET", "/_console/topics", nil).Body.String()
	if !strings.Contains(with, "Topic created") || strings.Contains(without, "Topic created") {
		t.Error("the flash banner")
	}
}

func TestOriginMatchesHost(t *testing.T) {
	for _, tc := range []struct {
		origin, host string
		want         bool
	}{
		{"http://127.0.0.1:9093", "127.0.0.1:9093", true},
		{"http://127.0.0.1:9094", "127.0.0.1:9093", false},
		{"http://evil.example", "127.0.0.1:9093", false},
		{"null", "127.0.0.1:9093", false},
		{"", "127.0.0.1:9093", false},
		{"://bad", "127.0.0.1:9093", false},
		{"https://console.test", "console.test", true},
	} {
		if got := originMatchesHost(tc.origin, tc.host); got != tc.want {
			t.Errorf("originMatchesHost(%q, %q) = %v", tc.origin, tc.host, got)
		}
	}
}
