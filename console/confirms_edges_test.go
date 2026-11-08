package console

import (
	"strings"
	"testing"
)

func TestGroupConfirmsCountWhatAResetMoves(t *testing.T) {
	rows := []groupLagRow{
		{Topic: "a", Committed: 5, Earliest: 0, Latest: 10},  // moves to either.
		{Topic: "a", Committed: 0, Earliest: 0, Latest: 10},  // already at the earliest.
		{Topic: "b", Committed: 10, Earliest: 0, Latest: 10}, // already at the latest.
		{Topic: "b", Committed: 3, Earliest: 3, Latest: 3},   // at both.
		{Topic: "gone", Committed: 9, Deleted: true},         // a reset skips it.
	}
	v := groupConfirms("g", rows, 0)
	if len(v) != 3 {
		t.Fatalf("%d confirms", len(v))
	}
	del := v["group.delete"].Text
	if !strings.Contains(del, `This removes the group and its 5 committed offsets on a, b and gone.`) {
		t.Errorf("delete: %s", del)
	}
	e := v["group.reset.earliest"].Text
	if !strings.Contains(e, "This moves 2 offsets of its 5") || !strings.Contains(e, "counted over all 3 topics") {
		t.Errorf("earliest: %s", e)
	}
	l := v["group.reset.latest"].Text
	if !strings.Contains(l, "This moves 2 offsets of its 5") || !strings.Contains(l, "counted over all 3 topics") {
		t.Errorf("latest: %s", l)
	}
	one := groupConfirms("g", rows[:1], 0)
	if txt := one["group.reset.earliest"].Text; !strings.Contains(txt, "This moves 1 offset of its 1") || strings.Contains(txt, "counted over") {
		t.Errorf("one row: %s", txt)
	}
	if txt := groupConfirms("g", nil, 0)["group.delete"].Text; !strings.Contains(txt, "no committed offsets") {
		t.Errorf("no rows: %s", txt)
	}
	if v["group.reset.earliest"].Title != "Reset to earliest" || v["group.reset.latest"].Verb != "Reset offsets" {
		t.Errorf("%+v", v["group.reset.earliest"])
	}
}

func TestSubjectConfirmsKeepAVersionToBeLatest(t *testing.T) {
	// Sc-12: a version is soft-deleted, and says what becomes latest.
	v := subjectConfirms("s", []int{1, 2, 3}, 2)
	if txt := v["schema.version.delete"].Text; !strings.Contains(txt, "Soft delete version 2 of s.") || !strings.Contains(txt, "The latest stays v3.") || !strings.Contains(txt, "(v4), not v2") {
		t.Errorf("middle: %s", txt)
	}
	if txt := subjectConfirms("s", []int{1, 2, 3}, 3)["schema.version.delete"].Text; !strings.Contains(txt, "Latest becomes v2.") {
		t.Errorf("newest: %s", txt)
	}
	if txt := subjectConfirms("s", []int{1, 2, 3}, 1)["schema.version.delete"].Text; !strings.Contains(txt, "The latest stays v3.") {
		t.Errorf("oldest: %s", txt)
	}
	if txt := subjectConfirms("s", []int{7}, 7)["schema.version.delete"].Text; !strings.Contains(txt, "the subject's last version") {
		t.Errorf("the last one: %s", txt)
	}
	// The subject is deleted for good, behind its name.
	if txt := v["schema.delete"].Text; !strings.Contains(txt, "Permanently delete subject s.") || !strings.Contains(txt, "All 3 versions") || !strings.Contains(txt, "Type s to confirm.") {
		t.Errorf("subject: %s", txt)
	}
	if txt := subjectConfirms("s", []int{4}, 4)["schema.delete"].Text; !strings.Contains(txt, "Its one version and its schema text") {
		t.Errorf("one version: %s", txt)
	}
	// Measured: the ids, the next id, the messages that carry them.
	f := confirmFacts{Name: "s", Versions: 3, IDs: []int{1, 4, 7}, SchemaID: 7, NextID: 11, Scanned: true, CarryTopic: "t", Carry: 8378, CarryAll: 14452}
	m := subjectConfirmsWith(f, []int{1, 2, 3}, 3)
	if txt := m["schema.delete"].Text; !strings.Contains(txt, "Ids 1, 4 and 7 are never handed out again") || !strings.Contains(txt, "A new registration gets id 11 or later.") || !strings.Contains(txt, "The 14,452 messages on t that carry ids 1, 4 and 7") {
		t.Errorf("measured subject: %s", txt)
	}
	if txt := m["schema.version.delete"].Text; !strings.Contains(txt, "8,378 messages on t carry id 7.") || !strings.Contains(txt, "/schemas/ids/7") {
		t.Errorf("measured version: %s", txt)
	}
	f.Carry = 1
	if txt := subjectConfirmsWith(f, []int{1, 2, 3}, 3)["schema.version.delete"].Text; !strings.Contains(txt, "1 message on t carries id 7.") {
		t.Errorf("one message: %s", txt)
	}
	f.IDs, f.CarryAll = []int{5}, 0
	if txt := subjectConfirmsWith(f, []int{1}, 1)["schema.delete"].Text; !strings.Contains(txt, "Id 5 is never handed out again") || !strings.Contains(txt, "None of the newest messages on t carries id 5.") {
		t.Errorf("one id, none carried: %s", txt)
	}
}

func TestConfirmURLAndLookup(t *testing.T) {
	if got := confirmURLFor("/p", "topic.purge", "name", "a b"); got != "/p/confirm/topic.purge?name=a+b" {
		t.Errorf("%q", got)
	}
	if got := confirmURLFor("/p", "x", "a", 1, "b", true, "odd"); got != "/p/confirm/x?a=1&b=true" {
		t.Errorf("%q", got)
	}
	if got := confirmURLFor("", "x"); got != "/confirm/x?" {
		t.Errorf("%q", got)
	}
	if confirmByID("topic.purge").ID != "topic.purge" {
		t.Error("lookup")
	}
	defer func() {
		if r := recover(); r == nil || !strings.Contains(r.(string), "no confirm nope") {
			t.Errorf("a confirm that does not exist: %v", r)
		}
	}()
	confirmByID("nope")
}

func TestOffsetDeleteConfirm(t *testing.T) {
	v := offsetDeleteConfirm("g", "orders", 2, 17, 3)
	for _, want := range []string{"Remove the offset of orders partition 2", "read up to 17 on this one partition", "The other 2 stay", "Nothing in the topic changes", "Allowed while the group is Empty"} {
		if !strings.Contains(v.Text, want) {
			t.Errorf("lacks %q: %s", want, v.Text)
		}
	}
	if v.Say.Call != "OffsetDelete" {
		t.Errorf("call %q", v.Say.Call)
	}
	if last := offsetDeleteConfirm("g", "orders", 2, 17, 1).Text; !strings.Contains(last, "last offset") {
		t.Errorf("the last offset: %s", last)
	}
}
