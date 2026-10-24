package console

import (
	"strings"
	"testing"
)

func TestHumanBytes(t *testing.T) {
	for n, want := range map[int]string{
		0: "0 B", 1: "1 B", 1023: "1023 B", 1024: "1 KB", 1025: "1 KB", 1536: "1.5 KB", 2150: "2.1 KB", 1 << 20: "1 MB", 3 << 20: "3 MB",
		1 << 30: "1 GB", 1 << 40: "1 TB", 1 << 50: "1024 TB",
	} {
		if got := humanBytes(n); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestPluralHelpers(t *testing.T) {
	for in, want := range map[string]string{"fetch": "fetches", "push": "pushes", "class": "classes", "box": "boxes", "quiz": "quizes", "heartbeat": "heartbeats", "": "s"} {
		if got := pluralWord(in); got != want {
			t.Errorf("pluralWord(%q) = %q, want %q", in, got, want)
		}
	}
	for n, want := range map[int]string{0: "s", 1: "", 2: "s", -1: "s"} {
		if got := pluralS(n); got != want {
			t.Errorf("pluralS(%d) = %q", n, got)
		}
		if got := plural(n); got != want {
			t.Errorf("plural(%d) = %q", n, got)
		}
	}
	if nounCount(1, "topic", "topics") != "1 topic" || nounCount(0, "topic", "topics") != "0 topics" || nounCount(2, "topic", "topics") != "2 topics" {
		t.Error("nounCount")
	}
	if count64(1, "record", "records") != "1 record" || count64(1000, "record", "records") != "1000 records" {
		t.Error("count64")
	}
	if numCount(1, "record", "records") != "1 record" || numCount(14429, "record", "records") != "14,429 records" || numCount(0, "record", "records") != "0 records" {
		t.Error("numCount")
	}
}

func TestNameListsInConfirms(t *testing.T) {
	for in, want := range map[string]string{
		"": "", "a": "a", "a,b": "a, b", "a,b,c": "a, b, c", "a,b,c,d": "a, b, c and 1 more", "a,b,c,d,e,f": "a, b, c and 3 more",
	} {
		var names []string
		if in != "" {
			names = strings.Split(in, ",")
		}
		if got := nameList(names); got != want {
			t.Errorf("nameList(%q) = %q, want %q", in, got, want)
		}
	}
	text := func(names ...string) string { return segsText(monoNames(names)) }
	for in, want := range map[string]string{
		"a": "a", "a,b": "a and b", "a,b,c": "a, b and c", "a,b,c,d": "a, b, c and 1 more", "a,b,c,d,e": "a, b, c and 2 more",
	} {
		if got := text(strings.Split(in, ",")...); got != want {
			t.Errorf("monoNames(%q) = %q, want %q", in, got, want)
		}
	}
	if len(monoNames(nil)) != 0 {
		t.Error("monoNames of nothing")
	}
}

func TestScopeNoteOnlyForSeveralTopics(t *testing.T) {
	if got := scopeNote(confirmFacts{Topics: 1}); got != "" {
		t.Errorf("one topic: %q", got)
	}
	if got := scopeNote(confirmFacts{Topics: 0}); got != "" {
		t.Errorf("no topics: %q", got)
	}
	if got := scopeNote(confirmFacts{Topics: 2}); !strings.Contains(got, "counted over all 2 topics") {
		t.Errorf("two topics: %q", got)
	}
	del := func(f confirmFacts) string { return confirmByID("group.delete").Say(f).Text() }
	if got := del(confirmFacts{Name: "g"}); !strings.Contains(got, "no committed offsets") {
		t.Errorf("%s", got)
	}
	if got := del(confirmFacts{Name: "g", Offsets: 1, Topics: 1}); !strings.Contains(got, "its 1 committed offset on 1 topic?") {
		t.Errorf("%s", got)
	}
	if got := del(confirmFacts{Name: "g", Offsets: 5, Topics: 2}); !strings.Contains(got, "its 5 committed offsets on 2 topics?") {
		t.Errorf("%s", got)
	}
}

func TestTemplateHelperFunctions(t *testing.T) {
	f := templateFuncs("/p", func(string, string) string { return "" })
	dict := f["dict"].(func(...any) map[string]any)
	if m := dict("a", 1, "b", 2); len(m) != 2 || m["a"] != 1 || m["b"] != 2 {
		t.Errorf("dict: %v", m)
	}
	if m := dict("a", 1, "dangling"); len(m) != 1 || m["a"] != 1 {
		t.Errorf("dict with an odd tail: %v", m)
	}
	if m := dict(7, 1, "b", 2); len(m) != 1 || m["b"] != 2 {
		t.Errorf("dict with a key that is not a name: %v", m)
	}
	if m := dict(); len(m) != 0 {
		t.Errorf("empty dict: %v", m)
	}
	if got := f["sub"].(func(int, int) int)(7, 3); got != 4 {
		t.Errorf("sub: %d", got)
	}
	if got := f["add"].(func(int, int) int)(7, 3); got != 10 {
		t.Errorf("add: %d", got)
	}
	nu := f["newSchemaURL"].(func(string) string)
	if nu("") != "/p/new/schema" || nu("a b&c") != "/p/new/schema?subject=a+b%26c" {
		t.Errorf("newSchemaURL: %q %q", nu(""), nu("a b&c"))
	}
	needs := f["needsLook"].(func(string, int64) bool)
	if needs("orders.dlq", 0) || !needs("orders.dlq", 1) || needs("orders", 5) {
		t.Error("needsLook")
	}
	if f["protoBadge"].(func(string) string)("848") != "proto-848" || f["protoBadge"].(func(string) string)("classic") != "proto-classic" {
		t.Error("protoBadge")
	}
	if f["protoLabel"].(func(string) string)("848") != "KIP-848" || f["protoLabel"].(func(string) string)("classic") != "classic" {
		t.Error("protoLabel")
	}
	sb := f["stateBadge"].(func(string) string)
	for in, want := range map[string]string{"Stable": "state-stable", "Empty": "state-empty", "Dead": "state-dead", "PreparingRebalance": "state-rebal"} {
		if sb(in) != want {
			t.Errorf("stateBadge(%q) = %q", in, sb(in))
		}
	}
	if got := f["memberKey"].(func(string, string) string)("g 1", "m/2"); got == "" || strings.ContainsAny(got, " /") {
		t.Errorf("memberKey %q", got)
	}
	if got := f["inspectHref"].(func(string, string) string)("txn", "a b"); got != "/p/inspect/txn?key=a+b" {
		t.Errorf("inspectHref %q", got)
	}
}

func TestIDSummary(t *testing.T) {
	for _, tc := range []struct {
		ids    []int
		count  int
		from   int
		to     int
		unused []int
		more   int
	}{
		{nil, 0, 0, 0, nil, 0},
		{[]int{4}, 1, 4, 4, nil, 0},
		{[]int{1, 2, 3}, 3, 1, 3, nil, 0},
		{[]int{1, 2, 3, 4, 5, 8}, 6, 1, 8, []int{6, 7}, 0},
		{[]int{1, 5}, 2, 1, 5, []int{2, 3, 4}, 0},
		{[]int{1, 6}, 2, 1, 6, []int{2, 3, 4}, 1},
		{[]int{1, 12}, 2, 1, 12, []int{2, 3, 4}, 7},
	} {
		s := idSummary(tc.ids)
		if s.Count != tc.count || s.From != tc.from || s.To != tc.to || s.More != tc.more || len(s.Unused) != len(tc.unused) {
			t.Errorf("%v: %+v", tc.ids, s)
			continue
		}
		for i, u := range tc.unused {
			if s.Unused[i] != u {
				t.Errorf("%v: unused %v", tc.ids, s.Unused)
			}
		}
	}
}
