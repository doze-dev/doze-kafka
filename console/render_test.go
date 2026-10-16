package console

import (
	"bytes"
	"html/template"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
)

// A page whose template fails halfway is drawn into a buffer first, so the
// browser gets a whole error page with status 500 and none of the half page, and
// the failure goes to the console's logger. render used to stream the template
// straight to the response and then call http.Error: a half page with the raw
// error text appended, and "superfluous response.WriteHeader" in the server log.
func TestAFailingTemplateGivesACleanErrorPageAndNoPartialOutput(t *testing.T) {
	c := newTestConsole(t)
	var logged bytes.Buffer
	c.log = slog.New(slog.NewTextHandler(&logged, nil))
	base := c.templates()
	broken := template.Must(template.Must(base.Clone()).Parse(`{{define "boom"}}<p>HALF-A-PAGE</p>{{call .Missing}}{{end}}`))
	c.tmpl = broken

	for name, draw := range map[string]func(*httptest.ResponseRecorder){
		"render": func(w *httptest.ResponseRecorder) {
			c.render(w, httptest.NewRequest("GET", "/_console/x", nil), "boom", nil)
		},
		"partial": func(w *httptest.ResponseRecorder) { c.partial(w, "boom", nil) },
	} {
		w := httptest.NewRecorder()
		draw(w)
		body := w.Body.String()
		if w.Code != 500 {
			t.Errorf("%s: status %d, want 500", name, w.Code)
		}
		if strings.Contains(body, "HALF-A-PAGE") {
			t.Errorf("%s: the half-drawn page reached the browser:\n%s", name, body)
		}
		if strings.Contains(body, "Missing") || strings.Contains(body, "template:") {
			t.Errorf("%s: the template error reached the browser:\n%s", name, body)
		}
		if !strings.Contains(body, "could not draw") {
			t.Errorf("%s: no error page:\n%s", name, body)
		}
	}
	if !strings.Contains(logged.String(), "boom") || !strings.Contains(logged.String(), "level=ERROR") {
		t.Errorf("the failure was not logged with the page's name:\n%s", logged.String())
	}
}
