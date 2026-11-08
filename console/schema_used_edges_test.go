package console

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// frame is a value in the registry's wire format: magic byte 0, the schema id as four
// bytes, then a payload.
func framedBytes(id int) string {
	return fmt.Sprintf("\x00%c%c%c%c-payload", byte(id>>24), byte(id>>16), byte(id>>8), byte(id))
}

// The Used by tab counts, per id the subject has, the messages carrying it and says where
// to open them; ids the registry does not have are Sc-17's undecodable messages, the first
// and the last of each.
func TestUsedByCountsEachIdAndListsTheMessagesNothingCanDecode(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	ctx := context.Background()
	topicWith(t, c, "orders", 1, 0)
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-value", userV2)
	cl := rawClient(t, addr)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// offset 0: id 1 (the old version, once); 1 and 2: id 2; 3 and 4: id 77, which no one registered.
	for i, id := range []int{1, 2, 2, 77, 77} {
		bulkProduce(t, cl, "orders", 0, 1, func(int) string { return framedBytes(id) }, t0.Add(time.Duration(i)*time.Second))
	}

	data := map[string]any{}
	c.usedBy(ctx, "orders-value", []int{1, 2}, data)
	rows, _ := data["UsedRows"].([]usedRow)
	if len(rows) != 2 || rows[0].ID != 2 || rows[1].ID != 1 {
		t.Fatalf("rows, newest id first: %+v", rows)
	}
	if rows[0].Count != 2 || rows[0].Old || rows[0].Share != 40 || rows[0].Open != "newest on this id" || !strings.Contains(rows[0].OpenURL, "offset=2") {
		t.Errorf("the latest id: %+v", rows[0])
	}
	// One message on an old id is that one message, not a range of offsets.
	if rows[1].Count != 1 || !rows[1].Old || rows[1].Open != "first on this id" || rows[1].Version != 1 || !strings.Contains(rows[1].OpenURL, "offset=0") {
		t.Errorf("the old id: %+v", rows[1])
	}
	if old, _ := data["OldRow"].(usedRow); old.ID != 1 {
		t.Errorf("why the old id is still used: %+v", data["OldRow"])
	}
	und, _ := data["Undecoded"].([]undecodedRow)
	if len(und) != 2 || und[0].Offset != 3 || und[1].Offset != 4 || und[0].ID != 77 || und[1].ID != 77 {
		t.Errorf("undecoded, the first and the last: %+v", und)
	}
	if data["View"] != "schema_undecoded" || data["UsedTopic"] != "orders" || data["UnknownCount"] != 2 || data["Decoded"] != 3 || data["Framed"] != 5 {
		t.Errorf("view %v topic %v unknown %v decoded %v framed %v", data["View"], data["UsedTopic"], data["UnknownCount"], data["Decoded"], data["Framed"])
	}

	// A subject whose topic holds nothing of its: no view of its own, and the topic it is named for
	// is still the one the tab points at; a version the registry lacks leaves no id to look for.
	data = map[string]any{}
	c.usedBy(ctx, "empty-value", []int{99}, data)
	if _, set := data["View"]; set {
		t.Errorf("no undecodable message, yet the view is %v", data["View"])
	}
	if ids, _ := data["IDList"].([]int); len(ids) != 0 {
		t.Errorf("ids %v", ids)
	}
	if data["UsedTopic"] != "empty" {
		t.Errorf("the topic the subject is named for: %v", data["UsedTopic"])
	}
	// A subject named for no topic has none to point at.
	data = map[string]any{}
	c.usedBy(ctx, "com.acme.Order", nil, data)
	if data["UsedTopic"] != "" {
		t.Errorf("a record-name subject: %v", data["UsedTopic"])
	}
	// And one whose ids are all decodable raises no Sc-17 view.
	data = map[string]any{}
	c.usedBy(ctx, "orders-value", []int{1}, data)
	if rows, _ := data["UsedRows"].([]usedRow); len(rows) != 1 || rows[0].ID != 1 || rows[0].Old {
		t.Errorf("id 1 alone: %+v", data["UsedRows"])
	}
}

// Sc-17's row reads the key of the message it points at, and points at one that is no longer
// there without one.
func TestUndecodedRowReadsTheKeyOfTheMessageItPointsAt(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	topicWith(t, c, "orders", 1, 0)
	cl := rawClient(t, addr)
	bulkProduce(t, cl, "orders", 0, 1, func(int) string { return framedBytes(5) }, time.Time{})
	ctx := context.Background()
	at := tpOffset{tpKey: tpKey{"orders", 0}, Offset: 0, Set: true}
	if row := c.undecodedAt(ctx, at, 5); row.Topic != "orders" || row.Offset != 0 || row.ID != 5 || !strings.Contains(row.URL, "offset=0") {
		t.Errorf("%+v", row)
	}
	gone := c.undecodedAt(ctx, tpOffset{tpKey: tpKey{"orders", 0}, Offset: 50, Set: true}, 5)
	if gone.Key != "" || gone.Offset != 50 {
		t.Errorf("a message past the end: %+v", gone)
	}
}

// Try it asks for a schema id as a number above 0; 0 is not one, and is not looked up.
func TestOpenIDRefusesZeroAndNegativeIds(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	topicWith(t, c, "orders", 1, 0)
	register(t, c, "orders-value", userV1)
	for _, id := range []string{"0", "-3", "x", ""} {
		rec := req(t, c, "GET", "/_console/schemas/orders-value/open-id?id="+id, nil)
		if rec.Code != 400 || !strings.Contains(rec.Body.String(), "a schema id is a number") {
			t.Errorf("id %q: %d %s", id, rec.Code, rec.Body)
		}
	}
}

// The topics a subject is looked for on: the one it is named for and those named after it
// (not another that merely starts the same); a subject named for no topic looks at the
// first eight, and never at the broker's own.
func TestUsedTopicsAreTheOneTheSubjectIsNamedForAndThoseNamedAfterIt(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	ctx := context.Background()
	for _, n := range []string{"orders", "orders.dlq", "orders-retry", "ordersx", "other"} {
		if err := c.be.CreateTopic(ctx, n, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	join := func(subject string) string { return strings.Join(c.usedTopics(ctx, subject), ",") }
	if got := join("orders-value"); got != "orders,orders-retry,orders.dlq" {
		t.Errorf("a topic's value subject: %s", got)
	}
	if got := join("com.acme.Order"); got != "orders,orders-retry,orders.dlq,ordersx,other" {
		t.Errorf("a record-name subject, every topic: %s", got)
	}
	for i := 0; i < 6; i++ {
		if err := c.be.CreateTopic(ctx, fmt.Sprintf("a%d", i), 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := join("com.acme.Order"); got != "a0,a1,a2,a3,a4,a5,orders,orders-retry" {
		t.Errorf("eleven topics, the first eight: %s", got)
	}
	if got := join("orders-value"); got != "orders,orders-retry,orders.dlq" {
		t.Errorf("a topic's value subject among eleven: %s", got)
	}
}
