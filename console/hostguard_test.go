package console

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestHostAllowed(t *testing.T) {
	allowed := []string{
		"127.0.0.1:9093", "127.0.0.1", "[::1]:9093", "[::1]", "192.168.1.20:9093", "10.0.0.5",
		"localhost:9093", "LOCALHOST", "localhost.", "app.localhost:9093",
		"kafka.doze", "events.demo.doze:9092", "doze",
	}
	for _, h := range allowed {
		if !hostAllowed(h, nil) {
			t.Errorf("hostAllowed(%q) = false, want true", h)
		}
	}
	// These are the shapes a rebinding page, or a lookalike, would present.
	denied := []string{
		"", "evil.example", "evil.example:9093", "127.0.0.1.evil.example", "localhost.evil.example",
		"kafka.doze.evil.example", "notdoze", "evil-doze", "xlocalhost", "[evil]:80",
	}
	for _, h := range denied {
		if hostAllowed(h, nil) {
			t.Errorf("hostAllowed(%q) = true, want false", h)
		}
	}
	if !hostAllowed("dev.example.com:9093", []string{"dev.example.com"}) {
		t.Error("an operator-allowed host should pass")
	}
	if hostAllowed("other.example.com", []string{"dev.example.com"}) {
		t.Error("only the listed host should pass")
	}
	if !hostAllowed("anything.example", []string{"*"}) {
		t.Error(`"*" should disable the guard`)
	}
}

// A rebound request arrives with a perfectly consistent Origin and Host, both
// naming the attacker. The Origin check passes it; only the Host guard stops it.
func TestRebindingRequestsAreRefused(t *testing.T) {
	c := newTestConsole(t)

	do := func(method, path, host, origin string, form url.Values) *httptest.ResponseRecorder {
		var r *http.Request
		if form != nil {
			r = httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		} else {
			r = httptest.NewRequest(method, path, nil)
		}
		r.Host = host
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		serveInto(t, c, rec, r)
		return rec
	}

	// Reads: the page, the JSON index of every topic and group, a static file.
	for _, path := range []string{"/_console/", "/_console/topics", "/_console/api/palette", "/_console/static/app.css"} {
		if rec := do("GET", path, "evil.example:9093", "", nil); rec.Code != http.StatusForbidden {
			t.Errorf("GET %s with a foreign Host = %d, want 403", path, rec.Code)
		}
	}
	// A state-changing request whose Origin matches its (attacker-chosen) Host.
	rec := do("POST", "/_console/new/topic", "evil.example:9093", "http://evil.example:9093", url.Values{"name": {"pwned"}, "partitions": {"1"}})
	if rec.Code != http.StatusForbidden {
		t.Errorf("a rebound POST = %d, want 403", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "--console-allow-host=evil.example") {
		t.Errorf("the refusal should say how to allow the name, got %q", rec.Body.String())
	}
	if got := do("GET", "/_console/topics/pwned", "127.0.0.1:9093", "", nil); got.Code == http.StatusOK {
		t.Error("the refused POST created a topic")
	}

	// The legitimate hosts still work, and the Origin check still bites.
	for _, host := range []string{"127.0.0.1:9093", "localhost:9093", "kafka.doze"} {
		if rec := do("GET", "/_console/topics", host, "", nil); rec.Code != http.StatusOK {
			t.Errorf("GET with Host %q = %d, want 200", host, rec.Code)
		}
	}
	if rec := do("POST", "/_console/new/topic", "127.0.0.1:9093", "http://evil.example", url.Values{"name": {"x"}, "partitions": {"1"}}); rec.Code != http.StatusForbidden {
		t.Errorf("a cross-origin POST to an allowed host = %d, want 403", rec.Code)
	}
}
