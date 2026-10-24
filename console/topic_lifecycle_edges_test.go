package console

import (
	"context"
	"strings"
	"testing"
)

func TestTopicDeleteAndPurgeAnswers(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "doomed", 2, 3)
	topicWith(t, c, "emptied", 2, 3)
	ctx := context.Background()
	rec := req(t, c, "POST", "/_console/topics/emptied/purge", nil)
	if rec.Code != 303 || rec.Header().Get("Location") != "/_console/topics/emptied?flash=Topic+purged+%E2%80%94+all+messages+deleted" {
		t.Errorf("purge: %d %s", rec.Code, rec.Header().Get("Location"))
	}
	bs, err := c.be.PartitionBounds(ctx, "emptied", 2)
	if err != nil || bs[0].Count() != 3-3 || bs[0].Earliest != bs[0].Latest || bs[0].Latest != 3 {
		t.Errorf("after a purge: %+v %v", bs, err)
	}
	rec = req(t, c, "POST", "/_console/topics/doomed/delete", nil)
	if rec.Code != 303 || rec.Header().Get("Location") != "/_console/topics?flash=Topic+deleted" {
		t.Errorf("delete: %d %s", rec.Code, rec.Header().Get("Location"))
	}
	if _, err := c.be.PartitionCount(ctx, "doomed"); err == nil {
		t.Error("the topic is still there")
	}
	for _, path := range []string{"/_console/topics/doomed/delete", "/_console/topics/doomed/purge"} {
		if rec = req(t, c, "POST", path, nil); rec.Code != 404 {
			t.Errorf("%s: %d", path, rec.Code)
		}
	}
	if body := req(t, c, "GET", "/_console/topics/doomed", nil); body.Code != 404 || !strings.Contains(body.Body.String(), "doomed") {
		t.Errorf("a deleted topic's page: %d", body.Code)
	}
}
