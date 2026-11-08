package console

import (
	"net/url"
	"testing"
)

// The record a history is asked about is the latest of its key so far: it is the one
// the cleaner keeps, so its own row does not say it goes, while the ones before it do.
func TestTheRecordAskedAboutIsNotOneThatGoesOnceCleaned(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"levels"}, "partitions": {"1"}, "cleanup.policy": {"compact"}})
	sendForm(t, c, "levels", url.Values{"partition": {"0"}, "key": {"SKU-1"}, "value": {"onHand 42"}})
	sendForm(t, c, "levels", url.Values{"partition": {"0"}, "key": {"SKU-1"}, "value": {"onHand 41"}})
	m := Message{Offset: 1, Key: []byte("SKU-1"), Value: []byte("onHand 41")}
	h := c.keyHistory(t.Context(), "levels", 0, m, bytesView{Text: "SKU-1"})
	if h == nil || len(h.Rows) != 2 {
		t.Fatalf("history: %+v", h)
	}
	if r := h.Rows[0]; r.Offset != "0" || r.Note != "goes once cleaned" {
		t.Errorf("the earlier record: %+v", r)
	}
	if r := h.Rows[1]; r.Offset != "1" || r.Note != "" {
		t.Errorf("the record asked about: %+v", r)
	}
}
