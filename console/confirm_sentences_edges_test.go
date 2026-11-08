package console

import (
	"strings"
	"testing"
)

func saySegs(segs []confirmSeg) string {
	var b strings.Builder
	for _, s := range segs {
		b.WriteString(s.T)
	}
	return b.String()
}

// parts is a confirm's parts by head, so a test reads the one it means.
func sayParts(t *testing.T, v confirmView) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, p := range v.Say.Parts {
		out[p.Head] = saySegs(p.Body)
	}
	return out
}

func TestTheGroupDeleteConfirmCountsUnreadRecordsOnlyWhenThereAreSome(t *testing.T) {
	behind := []groupLagRow{{Topic: "a", Committed: 5, Earliest: 0, Latest: 10, Lag: 5}}
	if txt := groupConfirms("g", behind, 0)["group.delete"].Text; !strings.Contains(txt, "5 records it had not read stay in the topic.") {
		t.Errorf("a group behind: %s", txt)
	}
	caughtUp := []groupLagRow{{Topic: "a", Committed: 10, Earliest: 0, Latest: 10, Lag: 0}}
	if txt := groupConfirms("g", caughtUp, 0)["group.delete"].Text; strings.Contains(txt, "not read") {
		t.Errorf("a group with nothing unread still says it left records: %s", txt)
	}
}

func TestTheGroupDeleteConfirmSaysWhetherTheGroupHasMembers(t *testing.T) {
	rows := []groupLagRow{{Topic: "a", Committed: 5, Earliest: 0, Latest: 10, Lag: 5}}
	if txt := groupConfirms("g", rows, 0)["group.delete"].Text; !strings.Contains(txt, "This group is Empty, so it can be deleted.") || strings.Contains(txt, "now, so Kafka refuses") {
		t.Errorf("no members: %s", txt)
	}
	txt := groupConfirms("g", rows, 3)["group.delete"].Text
	if !strings.Contains(txt, "This group has 3 members now, so Kafka refuses the call with") || strings.Contains(txt, "is Empty") {
		t.Errorf("three members: %s", txt)
	}
}

func TestADeleteForGoodNamesNewIdsOnlyWhenIdsGoAndTheNextIdIsKnown(t *testing.T) {
	// Every id the subject uses is another subject's too: nothing is spent, so no
	// "new registration" sentence, and the messages still decode.
	shared := confirmFacts{Name: "s", Versions: 2, IDs: []int{1, 2}, NextID: 9, Shared: map[int][]string{1: {"other"}, 2: {"other"}}}
	v := subjectConfirmsWith(shared, []int{1, 2}, 2)["schema.delete"]
	parts := sayParts(t, v)
	if got := parts["Ids stay"]; strings.Contains(got, "A new registration") || strings.Contains(got, "never handed out") || !strings.Contains(got, "Id 1 stays: other uses it too") {
		t.Errorf("all ids shared: %q (parts %v)", got, parts)
	}
	if _, ok := parts["Ids stay reserved"]; ok {
		t.Errorf("all ids shared, but the head says they are reserved: %v", parts)
	}
	if got := parts["Messages are not touched"]; !strings.Contains(got, "every id this subject uses is another subject's too") || strings.Contains(got, "no longer decode") {
		t.Errorf("all ids shared, messages: %q", got)
	}

	// One id goes, one stays; the next id is not known (0), so none is promised.
	mixed := confirmFacts{Name: "s", Versions: 2, IDs: []int{1, 2}, Shared: map[int][]string{1: {"other"}}}
	parts = sayParts(t, subjectConfirmsWith(mixed, []int{1, 2}, 2)["schema.delete"])
	if got := parts["Ids stay reserved"]; !strings.Contains(got, "Id 2 is never handed out again") || strings.Contains(got, "A new registration") || !strings.Contains(got, "Id 1 stays") {
		t.Errorf("one id goes, next id unknown: %v", parts)
	}
	if got := parts["Messages are not touched"]; !strings.HasSuffix(got, "The console can no longer decode them and shows them raw (error 40403, schema not found).") {
		t.Errorf("one id goes, messages: %q", got)
	}
	mixed.NextID = 9
	parts = sayParts(t, subjectConfirmsWith(mixed, []int{1, 2}, 2)["schema.delete"])
	if got := parts["Ids stay reserved"]; !strings.Contains(got, "A new registration gets id 9 or later.") {
		t.Errorf("one id goes, next id 9: %v", parts)
	}
}

func TestAVersionDeleteNamesCarryingMessagesOnlyWhenSomeCarryTheId(t *testing.T) {
	f := confirmFacts{Name: "s", Versions: 3, IDs: []int{1, 2, 3}, SchemaID: 3, Scanned: true, CarryTopic: "t", Carry: 0}
	if n := subjectConfirmsWith(f, []int{1, 2, 3}, 3)["schema.version.delete"].Say.Notes; len(n) != 0 {
		t.Errorf("a scan that found no message with the id still warns: %+v", n)
	}
	f.Carry = 4
	n := subjectConfirmsWith(f, []int{1, 2, 3}, 3)["schema.version.delete"].Say.Notes
	if len(n) != 1 || !n[0].Warn || !strings.Contains(saySegs(n[0].Body), "4 messages on t carry id 3.") {
		t.Errorf("four messages carry it: %+v", n)
	}
}

func TestASubjectWithNoVersionsStillGetsItsConfirms(t *testing.T) {
	// No panic, and no "next version" invented from nothing.
	v := subjectConfirms("s", nil, 0)
	if v["schema.delete"].Title == "" || v["schema.version.delete"].Title == "" {
		t.Errorf("%+v", v)
	}
	if got := v["schema.version.delete"].Text; !strings.Contains(got, "(v0), not v0") {
		t.Errorf("a subject with no versions: %s", got)
	}
}

func TestWhatADeleteForGoodRemovesCountsTheSoftDeletedVersions(t *testing.T) {
	for _, c := range []struct {
		versions, soft int
		shared         bool
		want, not      string
	}{
		{3, 1, false, "All 3 versions, the soft-deleted one included, and their schema text", ""},
		{3, 2, false, "All 3 versions, the 2 soft-deleted ones included, and their schema text", ""},
		{3, 0, false, "All 3 versions and their schema text", "soft-deleted"},
		// Every version soft-deleted: "included" would be saying all of them are the exception.
		{3, 3, false, "All 3 versions and their schema text", "soft-deleted"},
		{2, 2, true, "All 2 versions and the schema text no other subject holds", "soft-deleted"},
		{1, 1, false, "Its one version and its schema text", "soft-deleted"},
	} {
		got := subjectGoes(c.versions, c.soft, c.shared)
		if !strings.Contains(got, c.want) || (c.not != "" && strings.Contains(got, c.not)) {
			t.Errorf("subjectGoes(%d, %d, %v) = %q, want %q", c.versions, c.soft, c.shared, got, c.want)
		}
	}
}
