package console

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// route is one registration in console.go's route table.
type route struct{ method, path string }

// consoleRoutes reads the route table (routes.go), the single source console.go
// registers from, so a route added there is covered by the sweeps below without
// anyone remembering to list it.
func consoleRoutes(t *testing.T) []route {
	t.Helper()
	var out []route
	for _, r := range Routes() {
		out = append(out, route{r.Method, r.Path})
	}
	return out
}

// fill turns a route pattern into a request path for a given topic and group.
func (r route) fill(topic, group string) string {
	p := strings.ReplaceAll(r.path, "{name}", topic)
	p = strings.ReplaceAll(p, "{id}", group)
	p = strings.ReplaceAll(p, "{subject}", "orders-value")
	return "/_console" + p
}

// A template that fails halfway through still sends a 200 and a truncated page.
// Nothing else in the suite notices; this does.
func assertRendered(t *testing.T, what string, rec *httptest.ResponseRecorder) {
	t.Helper()
	body := rec.Body.String()
	for _, bad := range []string{"template:", "executing \"", "<no value>", "%!"} {
		if strings.Contains(body, bad) {
			i := strings.Index(body, bad)
			end := i + 120
			if end > len(body) {
				end = len(body)
			}
			t.Errorf("%s rendered %q: …%s…", what, bad, body[i:end])
		}
	}
	// A misspelled key in a template is not an error to html/template: it renders
	// nothing. A heading with nothing in it is the visible result.
	if m := emptyHeading.FindString(body); m != "" {
		t.Errorf("%s rendered an empty heading: %s", what, m)
	}
}

var emptyHeading = regexp.MustCompile(`<h[12][^>]*>\s*</h[12]>`)

// Every page and partial renders, with data and with nothing: a topic that has
// messages, a group that has commits, and a brand-new broker.
func TestEverySurfaceRenders(t *testing.T) {
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.Traffic = sampleTraffic() })
	topicWith(t, c, "orders", 2, 4)
	commitFor(t, addr, "billing", "orders", map[int32]int64{0: 1})

	for _, rt := range consoleRoutes(t) {
		if rt.method != "GET" || strings.Contains(rt.path, "/static/") {
			continue
		}
		path := rt.fill("orders", "billing")
		switch {
		case strings.HasSuffix(path, "/message"), strings.HasSuffix(path, "/message/raw"):
			path += "?partition=0&offset=1"
		case strings.HasSuffix(path, "/traffic/request"):
			path += "?seq=8"
		case strings.HasSuffix(path, "/trace"):
			path += "?id=" + traceA
		case strings.HasSuffix(path, "/trace/live"):
			// A scan to ask about: the page names the one it started.
			page := req(t, c, "GET", "/_console/trace?id="+traceA, nil).Body.String()
			m := regexp.MustCompile(`data-job="([^"]+)"`).FindStringSubmatch(page)
			if m == nil {
				t.Fatalf("the trace page names no scan:\n%s", page)
			}
			path += "?job=" + m[1]
		}
		rec := req(t, c, "GET", path, nil)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d", path, rec.Code)
			continue
		}
		assertRendered(t, "GET "+path, rec)
		// A page about a resource names it.
		body := rec.Body.String()
		for pattern, name := range map[string]string{"{name}": "orders", "{id}": "billing"} {
			if strings.HasSuffix(rt.path, pattern) && !strings.Contains(body, name) {
				t.Errorf("GET %s does not mention %q anywhere", path, name)
			}
		}
	}
}

// With the broker gone every surface still answers with a page: an error the
// shell renders, never a panic, a hang, or half a template.
func TestEverySurfaceAnswersWhenTheBrokerIsDown(t *testing.T) {
	old := requestTimeout
	requestTimeout = 300 * time.Millisecond
	defer func() { requestTimeout = old }()
	c, err := New(Options{Addr: "127.0.0.1:1"}) // nothing listens on port 1.
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for _, rt := range consoleRoutes(t) {
		if rt.method != "GET" || strings.Contains(rt.path, "/static/") {
			continue
		}
		path := rt.fill("orders", "billing")
		start := time.Now()
		rec := req(t, c, "GET", path, nil)
		if took := time.Since(start); took > 3*time.Second {
			t.Errorf("GET %s took %v with the broker down", path, took)
		}
		if rec.Code >= 500 && rec.Code != http.StatusBadGateway && rec.Code != http.StatusServiceUnavailable && rec.Code != http.StatusGatewayTimeout {
			t.Errorf("GET %s = %d with the broker down; want a 2xx page, a 4xx, or 502/503/504", path, rec.Code)
		}
		assertRendered(t, "GET "+path, rec)
	}
}

// Every state-changing route, swept: refused from another origin, a clean 4xx for
// a resource that does not exist or a form that is empty, and never a 5xx or a
// panic. A new POST route is picked up from console.go and held to the same bar.
func TestEveryMutationRouteIsGuardedAndFailsCleanly(t *testing.T) {
	c := newTestConsole(t)
	var posts []string
	for _, rt := range consoleRoutes(t) {
		if rt.method != "POST" {
			continue
		}
		path := rt.fill("no-such-topic", "no-such-group")
		posts = append(posts, path)

		foreign := httptest.NewRequest("POST", path, strings.NewReader(""))
		foreign.Host = "127.0.0.1:9093"
		foreign.Header.Set("Origin", "http://evil.example")
		rec := httptest.NewRecorder()
		serveInto(t, c, rec, foreign)
		if rec.Code != http.StatusForbidden {
			t.Errorf("POST %s from a foreign origin = %d, want 403", path, rec.Code)
		}

		rec = req(t, c, "POST", path, url.Values{})
		if rec.Code < 400 || rec.Code >= 500 {
			t.Errorf("POST %s with an empty form for a missing resource = %d, want a 4xx", path, rec.Code)
		}
		assertRendered(t, "POST "+path, rec)
	}
	sort.Strings(posts)
	if len(posts) < 6 {
		t.Fatalf("swept only %d mutation routes: %v", len(posts), posts)
	}
}

// A bad query string is the caller's mistake, and says so.
func TestInspectorRefusesBadCoordinates(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "t", 1, 2)
	for _, q := range []string{"", "?partition=0", "?partition=0&offset=abc", "?partition=0&offset=-5", "?partition=x&offset=0", "?partition=-1&offset=0"} {
		if rec := req(t, c, "GET", "/_console/topics/t/message"+q, nil); rec.Code != http.StatusBadRequest {
			t.Errorf("GET message%s = %d, want 400", q, rec.Code)
		}
	}
}
