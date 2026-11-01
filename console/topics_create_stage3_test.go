package console

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// Tp-03 offers cleanup.policy, and a topic made with it compacts (TOP-20).
func TestCreateFormSetsCleanupPolicy(t *testing.T) {
	c := newTestConsole(t)
	form := req(t, c, "GET", "/_console/new/topic?settings=1", nil).Body.String()
	if !strings.Contains(form, `name="cleanup.policy"`) {
		t.Fatal("the create form has no cleanup.policy field")
	}
	rec := req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"compacted"}, "partitions": {"1"}, "cleanup.policy": {"compact"}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	own, err := c.be.topicOverrides(context.Background(), "compacted")
	if err != nil || own["cleanup.policy"] != "compact" {
		t.Fatalf("the topic's own config: %v %v", own, err)
	}
	if page := req(t, c, "GET", "/_console/topics/compacted?tab=config", nil).Body.String(); !strings.Contains(page, `<option value="compact" selected>`) {
		t.Error("the Config tab does not show the topic as compact")
	}
	if rec := req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"bad"}, "partitions": {"1"}, "cleanup.policy": {"squash"}}); rec.Code != http.StatusBadRequest {
		t.Errorf("an unknown policy: %d", rec.Code)
	}
}

// The page opens with what the link carried: the name, the partitions and the settings.
func TestCreateFormTakesItsValuesFromTheLink(t *testing.T) {
	c := newTestConsole(t)
	page := req(t, c, "GET", "/_console/new/topic?settings=1&name=a.b&partitions=3&segment.bytes=1048576", nil).Body.String()
	for _, want := range []string{`value="a.b"`, `value="3"`, `name="segment.bytes" value="1048576"`, "--partitions 3", "segment.bytes=1048576"} {
		if !strings.Contains(page, want) {
			t.Errorf("the page lacks %q", want)
		}
	}
}
