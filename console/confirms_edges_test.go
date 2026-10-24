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
	v := groupConfirms("g", rows)
	if len(v) != 3 {
		t.Fatalf("%d confirms", len(v))
	}
	del := v["group.delete"].Text
	if !strings.Contains(del, `Delete group "g" and its 5 committed offsets on 3 topics?`) {
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
	one := groupConfirms("g", rows[:1])
	if txt := one["group.reset.earliest"].Text; !strings.Contains(txt, "This moves 1 offset of its 1") || strings.Contains(txt, "counted over") {
		t.Errorf("one row: %s", txt)
	}
	if txt := groupConfirms("g", nil)["group.delete"].Text; !strings.Contains(txt, "no committed offsets") {
		t.Errorf("no rows: %s", txt)
	}
	if v["group.reset.earliest"].Title != "Reset to earliest" || v["group.reset.latest"].Verb != "Reset offsets" {
		t.Errorf("%+v", v["group.reset.earliest"])
	}
}

func TestSubjectConfirmsKeepAVersionToBeLatest(t *testing.T) {
	v := subjectConfirms("s", []int{1, 2, 3}, 2)
	if txt := v["schema.version.delete"].Text; !strings.Contains(txt, `Delete version 2 of "s"? It is one of 3; the other 2 stay and version 3 becomes the latest.`) {
		t.Errorf("middle: %s", txt)
	}
	if txt := subjectConfirms("s", []int{1, 2, 3}, 3)["schema.version.delete"].Text; !strings.Contains(txt, "version 2 becomes the latest") {
		t.Errorf("newest: %s", txt)
	}
	if txt := subjectConfirms("s", []int{1, 2, 3}, 1)["schema.version.delete"].Text; !strings.Contains(txt, "version 3 becomes the latest") {
		t.Errorf("oldest: %s", txt)
	}
	if txt := v["schema.delete"].Text; !strings.Contains(txt, `Delete subject "s" with all 3 versions?`) {
		t.Errorf("subject: %s", txt)
	}
	if txt := subjectConfirms("s", []int{4}, 4)["schema.delete"].Text; !strings.Contains(txt, "with all 1 version?") {
		t.Errorf("one version: %s", txt)
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
	v := offsetDeleteConfirm("g", "orders", 2, 17)
	if !strings.Contains(v.Text, `Remove the committed offset of "g" for orders partition 2 (it is at 17)?`) {
		t.Errorf("%s", v.Text)
	}
}
