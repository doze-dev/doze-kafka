package console

import (
	"reflect"
	"testing"
	"time"
)

// "Did you mean": names that contain the typo or are a couple of edits away, closest
// first and then alphabetical, three at most, each with its link.
func TestCloseNamesAreOrderedClosestThenAlphabetical(t *testing.T) {
	c := &Console{}
	link := func(n string) string { return "/t/" + n }
	names := []string{"orders-b", "orders", "orders-a", "ordars", "payments", "ordres", "orders-c"}
	got := c.closeNames(names, "orders", link)
	var gotNames []string
	for _, s := range got {
		gotNames = append(gotNames, s.Name)
		if s.URL != link(s.Name) {
			t.Errorf("%s links to %q", s.Name, s.URL)
		}
	}
	// "orders" itself is not a suggestion; the three that contain it come before the
	// typos (score 0 before score 1), alphabetical within a score, and only three.
	if want := []string{"orders-a", "orders-b", "orders-c"}; !reflect.DeepEqual(gotNames, want) {
		t.Errorf("closeNames = %v, want %v", gotNames, want)
	}
	typos := c.closeNames([]string{"ordres", "ordars", "payments", "orders"}, "orders", link)
	var tn []string
	for _, s := range typos {
		tn = append(tn, s.Name)
	}
	if want := []string{"ordars", "ordres"}; !reflect.DeepEqual(tn, want) {
		t.Errorf("typos = %v, want %v (two edits from one another, alphabetical; payments is too far)", tn, want)
	}
	// Equal scores, given in reverse: the sort must move them.
	rev := c.closeNames([]string{"xb-orders", "xa-orders"}, "orders", link)
	if len(rev) != 2 || rev[0].Name != "xa-orders" {
		t.Errorf("not alphabetical: %+v", rev)
	}
}

// retention.ms is written in the biggest unit that holds it exactly.
func TestRetentionIsWrittenInItsBiggestWholeUnit(t *testing.T) {
	for in, want := range map[string]string{
		"604800000": "7 d", "86400000": "1 d", "43200000": "12 h", "3600000": "1 h",
		"5400000": "90 m", "60000": "1 m", "30000": "30 s", "1000": "1 s", "1500": "1500 ms", "250": "250 ms",
		"-1": "no limit", " 60000 ": "1 m", "soon": "",
	} {
		if got := retentionText(in); got != want {
			t.Errorf("retentionText(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAgoIsWrittenInMinutesHoursAndDays(t *testing.T) {
	for d, want := range map[time.Duration]string{
		0: "just now", 59 * time.Second: "just now", time.Minute: "1m ago", 59 * time.Minute: "59m ago",
		time.Hour: "1h ago", 23 * time.Hour: "23h ago", 24 * time.Hour: "1d ago", 72 * time.Hour: "3d ago",
	} {
		if got := agoText(d); got != want {
			t.Errorf("agoText(%v) = %q, want %q", d, got, want)
		}
	}
}

// A row's Target is what the request is about: topics (and how many more), else its
// group, else its transaction, and nothing for a fetch that came by a group.
func TestARowsTargetIsWhatTheRequestNames(t *testing.T) {
	c := &Console{}
	for _, tc := range []struct {
		name string
		e    TrafficEvent
		want string
	}{
		{"topics", TrafficEvent{Topics: []string{"a", "b"}}, "a, b"},
		{"topics and more", TrafficEvent{Topics: []string{"a"}, TopicsMore: 3}, "a and 3 more"},
		{"group", TrafficEvent{Group: "g"}, "g"},
		{"transaction", TrafficEvent{TxnID: "t-1"}, "tx t-1"},
		{"via a group", TrafficEvent{ViaGroup: "g"}, ""},
	} {
		if got := c.trafficRow(tc.e, trafficView{}).Target; got != tc.want {
			t.Errorf("%s: Target = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// "5 topics" on a Metadata row is five names, and the title says which: a filter by
// a topic that lists a Metadata row has to show why it matched.
func TestAMetadataRowsCountIsTitledByItsNames(t *testing.T) {
	c := &Console{}
	r := c.trafficRow(TrafficEvent{API: "Metadata", Topics: []string{"a", "b", "c"}, TopicsMore: 2}, trafficView{})
	if r.Target != "5 topics" || r.TargetTitle != "a, b, c and 2 more" {
		t.Errorf("Target %q, title %q", r.Target, r.TargetTitle)
	}
}
