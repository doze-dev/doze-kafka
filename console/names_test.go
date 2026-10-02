package console

import (
	"context"
	"encoding/json"
	"html"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestResourcePathsEscapeNames(t *testing.T) {
	cases := map[string]string{
		"orders":         "orders",
		"a/b":            "a%2Fb",
		"a?b":            "a%3Fb",
		"a#b":            "a%23b",
		"100%":           "100%25",
		"it's":           "it%27s",
		"two words":      "two%20words",
		"héllo":          "h%C3%A9llo",
		"../x":           "..%2Fx",
		`x'+(1)+'`:       "x%27+%281%29+%27",
		"we/ird?g#r%p'x": "we%2Fird%3Fg%23r%25p%27x",
	}
	for name, want := range cases {
		// PathEscape leaves a few characters ('+', parens) that are legal in a
		// segment; the contract is that the result is ONE segment that
		// unescapes back to the name, not an exact spelling.
		for _, got := range []string{topicPath("/_console", name), groupPath("/_console", name)} {
			seg := got[strings.LastIndex(got, "/")+1:]
			if strings.ContainsAny(seg, "/?#") {
				t.Errorf("%q: %q is not a single path segment", name, got)
			}
			back, err := url.PathUnescape(seg)
			if err != nil || back != name {
				t.Errorf("%q: %q does not unescape back (%q, %v)", name, got, back, err)
			}
		}
		_ = want
	}
}

var xShowAttr = regexp.MustCompile(`x-show="([^"]*)"`)

// A group id is whatever a client chose. Interpolated into an Alpine x-show
// expression it was a script-injection point: html/template treats the
// attribute as plain HTML, so a quote in the id ended the JS string and the rest
// ran in the console's origin — which can delete topics. The expression must be
// the same fixed text no matter what the names are, with names only in data-*.
func TestHostileGroupIDsCannotReachScriptContext(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ids := []string{
		`x'+(window.__pwned=1)+'`,
		`"><img src=x onerror=alert(1)>`,
		`we/ird?g#r%p'x`,
		`two words & <b>`,
	}
	var wg sync.WaitGroup
	for _, id := range ids {
		cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup(id), kgo.ConsumeTopics("t"), kgo.AllowAutoTopicCreation())
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			cl.PollFetches(ctx) // joins the group
		}()
	}
	wg.Wait()

	// Wait for the console to list all of them.
	var body string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rec := req(t, c, "GET", "/_console/groups", nil)
		body = rec.Body.String()
		if strings.Count(body, `class="li `) >= len(ids) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	exprs := xShowAttr.FindAllStringSubmatch(body, -1)
	if len(exprs) < len(ids) {
		t.Fatalf("expected an x-show per group row, found %d", len(exprs))
	}
	// The only x-show expressions are these fixed ones: none may ever be built
	// from a resource name.
	fixed := map[string]bool{
		`!$store.filter.q || $el.dataset.name.toLowerCase().includes($store.filter.q.toLowerCase())`: true,
		`$store.filter.none($el.parentElement)`:                                                      true,
	}
	for _, m := range exprs {
		if !fixed[html.UnescapeString(m[1])] {
			t.Errorf("x-show carries something other than a fixed expression: %q", m[1])
		}
	}
	if strings.Contains(body, "x'+(window") {
		t.Error("a hostile id appears unescaped in the page")
	}
	if strings.Contains(body, "<img src=x") {
		t.Error("markup in a group id was not escaped")
	}

	// Every link in the list leads to that group's page, whatever its characters.
	hrefs := regexp.MustCompile(`href="(/_console/groups/[^"]+)"`).FindAllStringSubmatch(body, -1)
	if len(hrefs) < len(ids) {
		t.Fatalf("found %d group links, want %d", len(hrefs), len(ids))
	}
	for _, m := range hrefs {
		link := html.UnescapeString(m[1])
		rec := req(t, c, "GET", link, nil)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200 (dead link)", link, rec.Code)
			continue
		}
		if !strings.Contains(rec.Body.String(), "<h1>") {
			t.Errorf("GET %s did not render a group page", link)
		}
	}

	// And the palette's resource index points at the same working URLs.
	rec := req(t, c, "GET", "/_console/api/palette", nil)
	var resources []struct{ S, N, U string }
	if err := json.Unmarshal(rec.Body.Bytes(), &resources); err != nil {
		t.Fatal(err)
	}
	var checked int
	for _, r := range resources {
		if r.S != "groups" {
			continue
		}
		checked++
		if got := req(t, c, "GET", r.U, nil); got.Code != http.StatusOK {
			t.Errorf("palette link %s = %d", r.U, got.Code)
		}
	}
	if checked < len(ids) {
		t.Errorf("palette listed %d groups, want %d", checked, len(ids))
	}
}

// The create form used to live at /topics/create, which a topic of that name
// could never be reached past.
func TestATopicNamedCreateIsReachable(t *testing.T) {
	c := newTestConsole(t)
	if rec := req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"create"}, "partitions": {"1"}}); rec.Code >= 400 {
		t.Fatalf("creating a topic named create: %d %s", rec.Code, rec.Body.String())
	}
	rec := req(t, c, "GET", "/_console/topics/create", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<h1>create</h1>") {
		t.Fatalf("GET /topics/create = %d; want the topic's page", rec.Code)
	}
	form := req(t, c, "GET", "/_console/new/topic", nil)
	if form.Code != http.StatusOK || !strings.Contains(form.Body.String(), "Create topic") {
		t.Fatalf("GET /new/topic = %d; want the form", form.Code)
	}
}

// The broker now refuses bad names, and the console must say so legibly.
func TestCreateTopicWithInvalidNameShowsTheReason(t *testing.T) {
	c := newTestConsole(t)
	rec := req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"../victim"}, "partitions": {"1"}})
	if rec.Code < 400 {
		t.Fatalf("an invalid name was accepted: %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "INVALID_TOPIC_EXCEPTION") {
		t.Fatalf("the error should name the Kafka error, got %q", rec.Body.String())
	}
}

// A group known only by its committed offsets (every consumer gone, or the broker
// restarted) must still be listed and openable, with its lag — the offsets are
// what it is.
func TestGroupKnownOnlyByOffsetsIsShown(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders"}, "partitions": {"1"}})
	if rec := req(t, c, "POST", "/_console/topics/orders/produce", url.Values{"partition": {"0"}, "value": {"x"}, "acks": {"1"}}); rec.Code != 200 {
		t.Fatalf("produce: %d", rec.Code)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	oc := kmsg.NewPtrOffsetCommitRequest()
	oc.Group, oc.Generation = "retired", -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = "orders"
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Offset = 0
	rt.Partitions = append(rt.Partitions, rp)
	oc.Topics = append(oc.Topics, rt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := oc.RequestWith(ctx, cl); err != nil {
		t.Fatal(err)
	}

	if list := req(t, c, "GET", "/_console/groups", nil); !strings.Contains(list.Body.String(), "retired") {
		t.Fatal("a group with only committed offsets is missing from the list")
	}
	page := req(t, c, "GET", "/_console/groups/retired", nil)
	if page.Code != http.StatusOK {
		t.Fatalf("GET /groups/retired = %d: %s", page.Code, page.Body.String())
	}
	if body := page.Body.String(); !strings.Contains(body, "lag 1") || !strings.Contains(body, "Empty") {
		t.Errorf("the group page should show it Empty with lag 1:\n%s", body)
	}
}
