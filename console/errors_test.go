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

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

func TestRefusalFor(t *testing.T) {
	cases := []struct {
		err    error
		want   RefusalKind
		name   string
		status int
	}{
		{fmt.Errorf("%w: topic x", errNotFound), Refused, "", 404},
		{kerr.UnknownTopicOrPartition, Refused, "UNKNOWN_TOPIC_OR_PARTITION", 404},
		{kerr.GroupIDNotFound, Refused, "GROUP_ID_NOT_FOUND", 404},
		{context.DeadlineExceeded, Failed, "", 502},
		{errors.New("unable to dial: dial tcp 127.0.0.1:1: connect: connection refused"), Failed, "", 502},
		{kerr.TopicAlreadyExists, Refused, "TOPIC_ALREADY_EXISTS", 400},
		{kerr.InvalidTopicException, Refused, "INVALID_TOPIC_EXCEPTION", 400},
		{kerr.TopicAuthorizationFailed, Denied, "TOPIC_AUTHORIZATION_FAILED", 403},
		{kerr.SaslAuthenticationFailed, Denied, "SASL_AUTHENTICATION_FAILED", 502},
		{kerr.UnsupportedVersion, NotImplemented, "UNSUPPORTED_VERSION", 501},
		{kerr.KafkaStorageError, Failed, "KAFKA_STORAGE_ERROR", 503},
		{badInput("partitions must be a number"), Refused, "", 400},
		{&regError{Status: 422, Code: 42201, Message: "Invalid schema"}, Refused, "42201", 400},
		{&regError{Status: 404, Code: 40401, Message: "Subject not found"}, Refused, "40401", 404},
		{&regError{Status: 403, Code: 40301, Message: "forbidden"}, Denied, "40301", 403},
		{errors.New("something odd"), Failed, "", 500},
	}
	for _, tc := range cases {
		got := refusalFor(tc.err)
		if got.Kind != tc.want || got.Name != tc.name || got.Status != tc.status {
			t.Errorf("refusalFor(%v) = %s %q %d, want %s %q %d", tc.err, got.Kind, got.Name, got.Status, tc.want, tc.name, tc.status)
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
	serveInto(t, c, rec, r)
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
	if rec.Code != http.StatusNotFound || strings.Contains(rec.Body.String(), "<html") || !strings.Contains(rec.Body.String(), `class="err refusal `) {
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
	for _, want := range []string{"No topics", "Nothing exists on this broker yet.", "This broker has no topics.", "kafka-topics.sh --bootstrap-server", "--create --topic orders.created", "The console makes exactly the same CreateTopics request."} {
		if !strings.Contains(body, want) {
			t.Errorf("an empty broker's topic list is missing %q", want)
		}
	}
	groups := req(t, c, "GET", "/_console/groups", nil).Body.String()
	if !strings.Contains(groups, "No consumer groups yet") || !strings.Contains(groups, "group.id") {
		t.Error("an empty broker's group list should explain how a group appears")
	}
	// Groups cannot be created from the console, so the footer must not offer it.
	if strings.Contains(groups, `data-create`) {
		t.Error("the groups page advertises a create control that does nothing there")
	}
	if !strings.Contains(body, `data-create`) {
		t.Error("the topics page should still offer 'c' for a new topic")
	}

	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders"}, "partitions": {"1"}})
	body = req(t, c, "GET", "/_console/topics", nil).Body.String()
	if strings.Contains(body, "Nothing exists on this broker yet.") {
		t.Error("the empty-list message is shown although a topic exists")
	}
	if !strings.Contains(body, `id="list-none"`) {
		t.Error("the list should carry a no-matches state for when a filter hides everything")
	}
}

// A write refused with KAFKA_STORAGE_ERROR is not always a write that did not happen:
// when the fsync after the append failed the batch is in the log (BAD-2, pinned by
// TestFsyncFailedBatchIsEitherRolledBackOrDocumented). The console's words must not say
// the batch was not appended; they say it may be there.
func TestStorageErrorTextDoesNotClaimTheBatchWasNotAppended(t *testing.T) {
	r := refusalFor(kerr.KafkaStorageError)
	info := protocol.Explain(kerr.KafkaStorageError.Code)
	for _, s := range []string{r.Message, r.Rule, info.Meaning, info.Cause, info.Fix} {
		l := strings.ToLower(s)
		for _, bad := range []string{"not appended", "was not written", "no offset was assigned", "nothing was written"} {
			if strings.Contains(l, bad) {
				t.Errorf("%q says %q, which is false for a failed sync", s, bad)
			}
		}
	}
	if !strings.Contains(r.Message, "may already be in the log") {
		t.Errorf("the refusal does not say the batch may be in the log: %q", r.Message)
	}
	if !strings.Contains(info.Cause, "may already be in the log") {
		t.Errorf("the Traffic inspector's cause does not say it: %q", info.Cause)
	}
}
