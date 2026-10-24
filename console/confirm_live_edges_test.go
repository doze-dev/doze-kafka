package console

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func confirmOf(t *testing.T, c *Console, path string) (int, map[string]any) {
	t.Helper()
	rec := req(t, c, "GET", "/_console/confirm/"+path, nil)
	var out map[string]any
	if rec.Code == 200 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s: %v\n%s", path, err, rec.Body)
		}
		if rec.Header().Get("Cache-Control") != "no-store" || !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
			t.Errorf("%s: headers %v", path, rec.Header())
		}
	}
	return rec.Code, out
}

func TestTopicFactsMeasureWhatDeletingTouches(t *testing.T) {
	dir := t.TempDir()
	src := &fakeTraffic{events: []TrafficEvent{{ID: "p1", Kind: "produce", API: "Produce", Topics: []string{"orders"}, ClientID: "writer-b"}, {ID: "p2", Kind: "produce", API: "Produce", Topics: []string{"orders"}, ClientID: "writer-a"}}}
	c, addr, _ := newRegistryConsole(t, func(o *Options) { o.Traffic = src; o.Facts.KafkaMajor = 4; o.Facts.AutoCreateTopics = true })
	c.facts.DataDir = dir
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "orders", 2, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "orders", 0, 5, vals("a"), time.Time{})
	bulkProduce(t, cl, "orders", 1, 3, vals("b"), time.Time{})
	deleteBefore(t, cl, "orders", 0, 2)
	commitFor(t, addr, "z-behind", "orders", map[int32]int64{0: 3, 1: 1})
	commitFor(t, addr, "a-ahead", "orders", map[int32]int64{0: 5, 1: 3})
	commitFor(t, addr, "m-behind", "orders", map[int32]int64{0: 4})
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-key", userV1)
	register(t, c, "orders-other", userV1)

	f := c.topicFacts(ctx, "orders", 2)
	if f.Name != "orders" || f.Partitions != 2 || f.Records != 3+3 || f.DataDir != dir || !f.AutoKnown || !f.AutoCreate {
		t.Errorf("%+v", f)
	}
	if len(f.Counts) != 2 || f.Counts[0] != 3 || f.Counts[1] != 3 || len(f.Ends) != 2 || f.Ends[0] != 5 || f.Ends[1] != 3 {
		t.Errorf("counts %v ends %v", f.Counts, f.Ends)
	}
	if strings.Join(f.Groups, ",") != "a-ahead,m-behind,z-behind" || strings.Join(f.Behind, ",") != "m-behind,z-behind" || len(f.Lags) != 2 || f.Lags[0] != 1 || f.Lags[1] != 4 || f.Unread != 5 {
		t.Errorf("groups %v behind %v lags %v unread %d", f.Groups, f.Behind, f.Lags, f.Unread)
	}
	if strings.Join(f.Subjects, ",") != "orders-key,orders-value" {
		t.Errorf("subjects %v", f.Subjects)
	}
	if strings.Join(f.Writers, ",") != "writer-b,writer-a" {
		t.Errorf("writers %v", f.Writers)
	}
	// With the data directory told: the segments' base offsets say whether bytes on disk are the records'.
	if f.DiskOK || !f.DiskExact {
		t.Errorf("disk %v exact %v (no files under %s)", f.DiskOK, f.DiskExact, dir)
	}
}

func TestConfirmLiveSentences(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "orders", 2, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "orders", 0, 4, vals("a"), time.Time{})
	commitFor(t, addr, "g", "orders", map[int32]int64{0: 1})
	register(t, c, "s-value", userV1)
	register(t, c, "s-value", userV2)

	code, got := confirmOf(t, c, "topic.purge?name=orders")
	if code != 200 || got["title"] != "Purge topic orders" || got["verb"] != "Purge 4 records" || !strings.HasPrefix(got["text"].(string), "Purge topic orders. Deletes every record in all 2 partitions") {
		t.Errorf("purge: %d %v", code, got)
	}
	if rows := got["rows"].([]any); len(rows) < 3 {
		t.Errorf("rows %v", rows)
	}
	code, got = confirmOf(t, c, "topic.partitions?name=orders&add=3")
	if code != 200 || got["verb"] != "Add 3 partitions" || !strings.Contains(got["text"].(string), "which has 2 partitions now, 5 after?") {
		t.Errorf("add: %d %v", code, got)
	}
	for _, add := range []string{"0", "-1", "x", ""} {
		_, got = confirmOf(t, c, "topic.partitions?name=orders&add="+add)
		if got["verb"] != "Add partitions" || !strings.Contains(got["text"].(string), "Add the number of partitions entered") {
			t.Errorf("add=%q: %v", add, got)
		}
	}
	// "add" belongs to the partitions confirm only.
	if _, got = confirmOf(t, c, "topic.delete?name=orders&add=3"); got["verb"] != "Delete topic" {
		t.Errorf("%v", got)
	}
	if code, _ = confirmOf(t, c, "topic.delete?name=nobody"); code != 404 {
		t.Errorf("a topic that is not there: %d", code)
	}
	code, got = confirmOf(t, c, "group.delete?group=g")
	if code != 200 || !strings.Contains(got["text"].(string), `Delete group "g" and its 1 committed offset on 1 topic?`) {
		t.Errorf("group: %d %v", code, got)
	}
	if _, got = confirmOf(t, c, "group.reset.latest?group=g"); !strings.Contains(got["text"].(string), "This moves 1 offset of its 1") {
		t.Errorf("reset: %v", got)
	}
	if code, _ = confirmOf(t, c, "group.delete?group=nobody"); code != 404 {
		t.Errorf("a missing group: %d", code)
	}
	code, got = confirmOf(t, c, "group.offset.delete?group=g&topic=orders&partition=0")
	if code != 200 || !strings.Contains(got["text"].(string), "orders partition 0 (it is at 1)") {
		t.Errorf("offset: %d %v", code, got)
	}
	for q, want := range map[string]int{"group.offset.delete?group=g&topic=orders&partition=1": 404, "group.offset.delete?group=g&topic=orders&partition=x": 400, "group.offset.delete?group=g&topic=other&partition=0": 404} {
		if code, _ = confirmOf(t, c, q); code != want {
			t.Errorf("%s: %d, want %d", q, code, want)
		}
	}
	code, got = confirmOf(t, c, "schema.version.delete?subject=s-value&version=1")
	if code != 200 || !strings.Contains(got["text"].(string), `Delete version 1 of "s-value"? It is one of 2; the other 1 stay and version 2 becomes the latest.`) {
		t.Errorf("version: %d %v", code, got)
	}
	if _, got = confirmOf(t, c, "schema.delete?subject=s-value"); !strings.Contains(got["text"].(string), "with all 2 versions") {
		t.Errorf("subject: %v", got)
	}
	if code, _ = confirmOf(t, c, "schema.delete?subject=nobody"); code != 404 {
		t.Errorf("a missing subject: %d", code)
	}
	if code, _ = confirmOf(t, c, "nothing.at.all"); code != 404 {
		t.Errorf("an unknown confirm: %d", code)
	}
}
