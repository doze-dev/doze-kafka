package console

import (
	"regexp"
	"strings"
	"testing"
)

// An htmx attribute that names an element by id (hx-include, hx-target, hx-select,
// hx-disabled-elt) must find it on the page that carries it: one that does not finds
// nothing, and htmx says so in the browser's console every time it is used (A7: a
// reset on a group with offsets on one topic included the topic picker that only a
// group with several draws). Each page is rendered as a person opens it.
func TestEveryHtmxReferenceFindsItsElement(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	topicWith(t, c, "orders", 2, 3)
	topicWith(t, c, "audit", 1, 1)
	register(t, c, "orders-value", userV1)
	commitFor(t, addr, "one-topic", "orders", map[int32]int64{0: 1})
	commitFor(t, addr, "two-topics", "orders", map[int32]int64{0: 1})
	commitFor(t, addr, "two-topics", "audit", map[int32]int64{0: 1})

	ref := regexp.MustCompile(`\shx-(include|target|select|disabled-elt|indicator)="#([A-Za-z][\w-]*)"`)
	pages := []string{
		"/_console/topics", "/_console/topics/orders", "/_console/topics/orders?tab=config", "/_console/topics/orders?tab=partitions",
		"/_console/new/topic", "/_console/groups", "/_console/groups/one-topic?tab=offsets", "/_console/groups/two-topics?tab=offsets",
		"/_console/schemas", "/_console/schemas/orders-value", "/_console/schemas/orders-value?tab=register", "/_console/new/schema",
		"/_console/access", "/_console/new/acl", "/_console/cluster", "/_console/connect", "/_console/trace",
	}
	for _, p := range pages {
		rec := req(t, c, "GET", p, nil)
		if rec.Code != 200 {
			t.Errorf("%s: %d", p, rec.Code)
			continue
		}
		body := rec.Body.String()
		for _, m := range ref.FindAllStringSubmatch(body, -1) {
			if !strings.Contains(body, `id="`+m[2]+`"`) {
				t.Errorf("%s: hx-%s names #%s, which the page does not have", p, m[1], m[2])
			}
		}
	}
}
