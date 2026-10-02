package console

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		err  error
		want string
		code int
	}{
		{fmt.Errorf("%w: topic x", errNotFound), "notfound", 404},
		{kerr.UnknownTopicOrPartition, "notfound", 404},
		{kerr.GroupIDNotFound, "notfound", 404},
		{context.DeadlineExceeded, "unreachable", 502},
		{errors.New("unable to dial: dial tcp 127.0.0.1:1: connect: connection refused"), "unreachable", 502},
		{kerr.TopicAlreadyExists, "refused", 400},
		{kerr.InvalidTopicException, "refused", 400},
		{errors.New("something odd"), "internal", 500},
	}
	for _, tc := range cases {
		if got := classify(tc.err); got.kind != tc.want || got.status != tc.code {
			t.Errorf("classify(%v) = %s/%d, want %s/%d", tc.err, got.kind, got.status, tc.want, tc.code)
		}
	}
}

func TestEditDistance(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want int
	}{{"", "", 0}, {"orders", "orders", 0}, {"orders", "ordres", 2}, {"order", "orders", 1}, {"kitten", "sitting", 3}, {"", "abc", 3}} {
		if got := editDistance(tc.a, tc.b); got != tc.want {
			t.Errorf("editDistance(%q,%q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// doReq is req with extra headers, for telling a navigation from a partial.
func doReq(t *testing.T, c *Console, method, path string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, nil)
	r.Host = "127.0.0.1:9093"
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, r)
	return rec
}

// A missing topic or group is a page in the shell with a way out, not a bare
// fragment on a white screen, and it offers the names that do exist.
func TestNotFoundIsAHelpfulPageInTheShell(t *testing.T) {
	c := newTestConsole(t)
	for _, n := range []string{"orders", "payments"} {
		req(t, c, "POST", "/_console/new/topic", url.Values{"name": {n}, "partitions": {"1"}})
	}

	rec := doReq(t, c, "GET", "/_console/topics/ordres", nil) // a typo.
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"<!DOCTYPE html>", "Topic not found", `named &#34;ordres&#34;`, "Did you mean", `href="/_console/topics/orders"`, "Back to the list", `class="rail"`} {
		if !strings.Contains(body, want) {
			t.Errorf("the not-found page is missing %q", want)
		}
	}
	if i := strings.Index(body, `class="didyoumean"`); i >= 0 {
		block := body[i : i+strings.Index(body[i:], "</div>")]
		if strings.Contains(block, "payments") {
			t.Error("an unrelated topic was suggested for a typo of orders")
		}
	}
	if rec.Header().Get("X-Doze-Page") != "error" {
		t.Error("the page must say it is a page to show, or htmx will refuse to swap a 404")
	}

	if rec := doReq(t, c, "GET", "/_console/groups/ghost", nil); rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "Group not found") {
		t.Errorf("unknown group: %d %.80s", rec.Code, rec.Body.String())
	}

	// A boosted link is a navigation too.
	if rec := doReq(t, c, "GET", "/_console/topics/ordres", map[string]string{"HX-Request": "true", "HX-Boosted": "true"}); !strings.Contains(rec.Body.String(), "Topic not found") {
		t.Error("a boosted navigation should get the error page")
	}
	// A partial (the tape's poll, say) must NOT get a whole page to swap into a region.
	rec = doReq(t, c, "GET", "/_console/topics/ordres/tape", map[string]string{"HX-Request": "true"})
	if rec.Code != http.StatusNotFound || strings.Contains(rec.Body.String(), "<html") || !strings.Contains(rec.Body.String(), `class="err"`) {
		t.Errorf("a partial request should get a small error fragment, got %d: %.100s", rec.Code, rec.Body.String())
	}
}

// Opening a message that is gone used to panic on a nil error; it is a not-found.
func TestOpeningAVanishedMessageIsNotACrash(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"1"}})
	rec := doReq(t, c, "GET", "/_console/topics/t/message?partition=0&offset=999", map[string]string{"HX-Request": "true"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "no record at offset 999") {
		t.Errorf("the message should say what is missing: %q", rec.Body.String())
	}
}

func TestUnreachableBrokerIsAPageWithRetry(t *testing.T) {
	old := requestTimeout
	requestTimeout = 400 * time.Millisecond
	defer func() { requestTimeout = old }()
	c, err := New(Options{Addr: "192.0.2.1:9092"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	rec := doReq(t, c, "GET", "/_console/topics", nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"Can&#39;t reach the broker", "Try again", "<!DOCTYPE html>"} {
		if !strings.Contains(body, want) {
			t.Errorf("the unreachable page is missing %q", want)
		}
	}
}

func TestEmptyListsSayWhy(t *testing.T) {
	c := newTestConsole(t)
	body := req(t, c, "GET", "/_console/topics", nil).Body.String()
	for _, want := range []string{"No topics yet", "auto-create the topic", "Select a topic to browse"} {
		if !strings.Contains(body, want) {
			t.Errorf("an empty broker's topic list is missing %q", want)
		}
	}
	groups := req(t, c, "GET", "/_console/groups", nil).Body.String()
	if !strings.Contains(groups, "No consumer groups yet") || !strings.Contains(groups, "group.id") {
		t.Error("an empty broker's group list should explain how a group appears")
	}
	// Groups cannot be created from the console, so the footer must not offer it.
	if strings.Contains(groups, `<span class="kbd">c</span> new`) {
		t.Error("the groups pane advertises a 'c' (new) shortcut that does nothing there")
	}
	if !strings.Contains(body, `<span class="kbd">c</span> new`) {
		t.Error("the topics pane should still offer 'c' for a new topic")
	}

	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders"}, "partitions": {"1"}})
	body = req(t, c, "GET", "/_console/topics", nil).Body.String()
	if strings.Contains(body, "No topics yet") {
		t.Error("the empty-list message is shown although a topic exists")
	}
	if !strings.Contains(body, "lp-nomatch") {
		t.Error("the list should carry a no-matches state for when a filter hides everything")
	}
}
