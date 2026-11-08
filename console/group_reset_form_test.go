package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// text is a sentence's segments as one run of text.
func pageText(segs []confirmSeg) string {
	var b strings.Builder
	for _, s := range segs {
		b.WriteString(s.T)
	}
	return b.String()
}

func resetFormReq(form url.Values) *http.Request {
	r := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

// The Specific offsets form posts one box per partition, "off:<topic>:<partition>".
// An empty box is a partition left alone; 0 is an offset; a negative or a word is
// refused with the way to write one.
func TestResetFormReadsOneBoxPerPartitionAndLeavesEmptyOnesAlone(t *testing.T) {
	spec, err := resetSpecOf(resetFormReq(url.Values{
		"to":         {"offsets"},
		"off:rt:0":   {"5"},
		"off:rt:1":   {""},      // an empty box.
		"off:rt:2":   {"  "},    // a blank one.
		"off:a:0":    {" 0 "},   // a one-letter topic, and 0 is an offset.
		"off:rt:x":   {"3"},     // not a partition.
		"off:7":      {"9"},     // no topic, no partition: not a box.
		"off:a:b:12": {"4"},     // the partition is after the last colon.
		"other":      {"x"},     // not a box.
		"offsets":    {"1"},     // not a box either.
		"off:rt:-1":  {"3"},     // a partition number below 0 parses and is the plan's to refuse.
		"off:t:2":    {"12345"}, // a larger offset.
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := map[TopicPartition]int64{
		{"rt", 0}: 5, {"a", 0}: 0, {"a:b", 12}: 4, {"rt", -1}: 3, {"t", 2}: 12345,
	}
	if len(spec.Offsets) != len(want) {
		t.Errorf("offsets %v, want %v", spec.Offsets, want)
	}
	for k, v := range want {
		if got, ok := spec.Offsets[k]; !ok || got != v {
			t.Errorf("%v: %d (%v), want %d", k, got, ok, v)
		}
	}
	for _, bad := range []string{"-1", "abc", "1.5"} {
		_, err := resetSpecOf(resetFormReq(url.Values{"to": {"offsets"}, "off:rt:0": {bad}}))
		if err == nil || !strings.Contains(err.Error(), "is not an offset") {
			t.Errorf("%q: %v", bad, err)
		}
	}
	// A box name the form never sends, with no value at all, is passed over.
	r := resetFormReq(url.Values{"to": {"offsets"}})
	if err := r.ParseForm(); err != nil {
		t.Fatal(err)
	}
	r.Form["off:rt:0"] = nil
	r.Form["off:rt:1"] = []string{"2"}
	spec, err = resetSpecOf(r)
	if err != nil || len(spec.Offsets) != 1 || spec.Offsets[TopicPartition{"rt", 1}] != 2 {
		t.Errorf("a box with no value: %v %v", spec.Offsets, err)
	}
}

// The preview adds the plan up: the lag after is the log end less where it lands,
// and a group committed exactly at the end is not ahead of it.
func TestPreviewAddsThePlanUp(t *testing.T) {
	rows := []ResetRow{
		{Topic: "a", Partition: 0, Committed: 7, Earliest: 0, Latest: 10, After: 4, Moves: true},
		{Topic: "a", Partition: 1, Committed: 10, Earliest: 0, Latest: 10, After: 10, Moves: true}, // at the end exactly.
		{Topic: "a", Partition: 2, Committed: 12, Earliest: 0, Latest: 10, After: 10, Moves: true}, // past the end.
		{Topic: "a", Partition: 3, Committed: 2, Earliest: 0, Latest: 6, After: 2, Moves: false},
	}
	p := previewOf(rows, ResetSpec{To: "offsets"})
	if p.Rows[0].LagAfter != 6 || p.Rows[1].LagAfter != 0 || p.Rows[2].LagAfter != 0 || p.Rows[3].LagAfter != 4 {
		t.Errorf("lag after: %+v", p.Rows)
	}
	if p.Rows[0].Ahead || p.Rows[1].Ahead || !p.Rows[2].Ahead || p.Rows[3].Ahead {
		t.Errorf("ahead: %+v", p.Rows)
	}
	if p.Rows[1].LagNow != 0 || p.Rows[2].LagNow != 0 || p.Rows[0].LagNow != 3 {
		t.Errorf("lag now: %+v", p.Rows)
	}
	if p.LagNow != 3+0+0+4 || p.LagAfter != 6+0+0+4 || p.Moves != 3 || p.Count != "3 partitions" || p.Multi || !p.Specific {
		t.Errorf("totals: %+v", p)
	}
	if !p.Rows[0].Changed || p.Rows[1].Changed || p.Rows[3].Changed {
		t.Errorf("changed: %+v", p.Rows)
	}
}

// The sentence under the table says what the reset does to the lag, in the mode's own
// words, and says "as now" only where it is.
func TestPreviewSentenceSaysWhatTheLagBecomes(t *testing.T) {
	say := func(to string, now, after int64, moves int) string {
		return pageText(previewSentence(&resetPreview{LagNow: now, LagAfter: after, Moves: moves}, ResetSpec{To: to, At: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}))
	}
	for _, c := range []struct {
		to          string
		now, after  int64
		moves       int
		want        string
		wantAbsent  string
		description string
	}{
		{"earliest", 3, 8, 1, "Going to earliest makes the group re-read 8 records instead of 3.", "", "more to read"},
		{"earliest", 5, 5, 1, "Going to earliest leaves 5 records to read, as now.", "makes the group re-read", "the same"},
		{"earliest", 5, 2, 1, "Going to earliest leaves 2 records to read, where now it is 5.", "as now", "fewer"},
		{"latest", 4, 0, 1, "Going to latest skips the 4 records the group has not read", "already at the end", "some unread"},
		{"latest", 0, 0, 1, "The group is already at the end of every partition it has an offset on.", "skips the", "none unread"},
		{"offsets", 3, 1, 0, "Type an offset in a row to move it.", "With these offsets", "none moved"},
		{"offsets", 3, 1, 2, "With these offsets the group has 1 records to read instead of 3.", "Type an offset", "some moved"},
		{"timestamp", 3, 1, 2, "Going to 2026-01-02 03:04:05 UTC leaves 1 records to read instead of 3.", "", "a time"},
	} {
		got := say(c.to, c.now, c.after, c.moves)
		if !strings.Contains(got, c.want) || (c.wantAbsent != "" && strings.Contains(got, c.wantAbsent)) {
			t.Errorf("%s (%s): %q, want %q without %q", c.to, c.description, got, c.want, c.wantAbsent)
		}
	}
}

// The way out of a refused reset is told from who the members are: each client once,
// whether it is one app on several members, where they connect from, and which
// protocol the group runs.
func TestRefusedStepsNameTheClientsAndWhereTheyConnectFrom(t *testing.T) {
	ms := []memberView{
		{ClientID: "billing", Addr: "10.0.0.1:5000", Host: "ignored"},
		{ClientID: "billing", Host: "host-b"},
		{ClientID: "", Addr: "", Host: ""}, // says nothing about itself.
	}
	s := stepsOf(ms, groupFactsView{Session: "45s"}, "848")
	if s.Count != 3 || s.Session != "45s" || !s.Consumer {
		t.Errorf("%+v", s)
	}
	if len(s.Clients) != 1 || s.Clients[0] != "billing" || !s.SameApp {
		t.Errorf("clients %v same app %v", s.Clients, s.SameApp)
	}
	if len(s.Addrs) != 2 || s.Addrs[0] != "10.0.0.1:5000" || s.Addrs[1] != "host-b" {
		t.Errorf("addrs %v", s.Addrs)
	}
	if s := stepsOf(ms[:1], groupFactsView{}, "classic"); s.Consumer || s.SameApp || len(s.Clients) != 1 {
		t.Errorf("one member of the classic protocol: %+v", s)
	}
	if s := stepsOf([]memberView{{ClientID: "a"}, {ClientID: "b"}}, groupFactsView{}, ""); s.SameApp || len(s.Clients) != 2 {
		t.Errorf("two apps: %+v", s)
	}
	if s := stepsOf([]memberView{{}, {}}, groupFactsView{}, ""); s.SameApp || len(s.Clients) != 0 || len(s.Addrs) != 0 {
		t.Errorf("two members that say nothing: %+v", s)
	}
}

// The Offsets tab's form starts on earliest, with an "all topics" scope only where there
// are several topics and a preview only where the group has offsets.
func TestResetFormScopesFollowTheGroupsTopics(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "one", 1, 3)
	topicWith(t, c, "two", 1, 3)
	commitFor(t, addr, "g2", "one", map[int32]int64{0: 1})
	commitFor(t, addr, "g2", "two", map[int32]int64{0: 2})
	commitFor(t, addr, "g1", "one", map[int32]int64{0: 1})
	r := httptest.NewRequest("GET", "/", nil)

	f := c.resetFormOf(r, "g2", []groupLagRow{{Topic: "one"}, {Topic: "two"}, {Topic: "gone", Deleted: true}})
	if len(f.Scopes) != 3 || f.Scopes[0].Topic != "" || f.Scopes[0].Label != "all topics (2 partitions)" || f.Scopes[0].Partitions != 2 ||
		f.Scopes[1].Topic != "one" || f.Scopes[2].Topic != "two" || f.Topic != "" {
		t.Errorf("two topics: %+v", f)
	}
	if f.Preview == nil || len(f.Preview.Rows) != 2 {
		t.Errorf("two topics' preview: %+v", f.Preview)
	}

	f = c.resetFormOf(r, "g1", []groupLagRow{{Topic: "one"}})
	if len(f.Scopes) != 1 || f.Scopes[0].Topic != "one" || f.Scopes[0].Label != "one (1 partition)" || f.Topic != "one" {
		t.Errorf("one topic: %+v", f)
	}
	if f.Preview == nil || len(f.Preview.Rows) != 1 {
		t.Errorf("one topic's preview: %+v", f.Preview)
	}

	f = c.resetFormOf(r, "nobody", nil)
	if len(f.Scopes) != 0 || f.Preview != nil || f.PreviewErr != "" || f.To != "earliest" {
		t.Errorf("no topics: %+v", f)
	}
}

// The Apply button's confirm counts what the form describes: how many offsets move, over
// how many topics (all of them, unless one is chosen), and the time a time-reset goes to.
func TestResetConfirmCountsWhatTheFormDescribes(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "ta", 1, 6)
	topicWith(t, c, "tb", 1, 6)
	commitFor(t, addr, "cg", "ta", map[int32]int64{0: 2})
	commitFor(t, addr, "cg", "tb", map[int32]int64{0: 3})
	say := func(q string) string {
		t.Helper()
		rec := req(t, c, "GET", "/_console/confirm/group.reset?group=cg&"+q, nil)
		var v struct{ Text string }
		if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil || v.Text == "" {
			t.Fatalf("%s: %d %s", q, rec.Code, rec.Body)
		}
		return v.Text
	}
	if got := say("to=earliest"); !strings.Contains(got, "This moves 2 offsets of its 2") || !strings.Contains(got, "counted over all 2 topics") {
		t.Errorf("all topics: %s", got)
	}
	if got := say("to=earliest&topic=ta"); !strings.Contains(got, "This moves 1 offset of its 1") || strings.Contains(got, "counted over") {
		t.Errorf("one topic: %s", got)
	}
	want := utcStamp(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if got := say("to=timestamp&at=2026-01-01T00:00:00Z"); !strings.Contains(got, "first record at or after "+want+"?") {
		t.Errorf("a time: %s (want %s)", got, want)
	}
}
