package console

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	legendRowRe  = regexp.MustCompile(`<div class="r(?: on)?"><span class="m [^"]*">([^<]*)</span><span class="note">[^<]*</span><span class="m mut ra">([^<]*)</span></div>`)
	historyRowRe = regexp.MustCompile(`<div class="r(?: on)?"><span class="m ra">([^<]*)</span><span class="m el">([^<]*)</span><span class="note">([^<]*)</span></div>`)
)

// legendOf is the "Null is not empty" card of a record's page as "label=offset" per row.
func legendOf(page string) []string {
	var out []string
	for _, m := range legendRowRe.FindAllStringSubmatch(between(page, `tp-legend"`, `</div></div>`)+"</div></div>", -1) {
		out = append(out, m[1]+"="+m[2])
	}
	return out
}

// historyOf is the "What compaction will do" card of a record's page, one
// "offset value: note" per row.
func historyOf(page string) []string {
	var out []string
	for _, m := range historyRowRe.FindAllStringSubmatch(between(page, `tp-history"`, `inspector-acts`), -1) {
		out = append(out, fmt.Sprintf("%s %s: %s", m[1], m[2], m[3]))
	}
	return out
}

// bigCompactedTopic is a compacted topic of 3102 records, none of them yet cleaned (they
// are all in the one open segment), where the key "hot" is written at offset 200, 1800 and
// 3099 and deleted at offset 1500 and 3100.
func bigCompactedTopic(t *testing.T) *Console {
	t.Helper()
	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"big"}, "partitions": {"1"}, "cleanup.policy": {"compact"}})
	recs := make([]*kgo.Record, 3102)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: "big", Partition: 0, Key: []byte(fmt.Sprintf("f%d", i)), Value: []byte(fmt.Sprintf("v%d", i))}
	}
	for off, val := range map[int]string{200: "a", 1500: "", 1800: "b", 3099: "c", 3100: ""} {
		recs[off].Key = []byte("hot")
		recs[off].Value = nil
		if val != "" {
			recs[off].Value = []byte(val)
		}
	}
	if err := rawClient(t, addr).ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return c
}

// A deleted key's history is the records of the key up to the one inspected, no more:
// the records written after it are not part of "what will compaction do with this key".
func TestAKeysHistoryEndsAtTheRecordInspected(t *testing.T) {
	c := bigCompactedTopic(t)
	page := message(t, c, "big", 1500)
	want := []string{
		"200 a: goes once cleaned",
		"1,500 tombstone: kept until its segment is cleaned and it is older than delete.retention.ms (1d), then goes",
	}
	if got := historyOf(page); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("history of hot at 1500:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if r := readable(page); !strings.Contains(r, "Read 1501 records up to this one.") {
		t.Errorf("the history says how many records it read:\n%s", between(r, "What compaction will do", "Records of this key"))
	}
	// The card around it reads 100 records either side of this one, and says how many.
	if r := readable(page); !strings.Contains(r, "Offsets are the nearest of the 201 records read around this one.") {
		t.Errorf("the legend says how many records it read:\n%s", between(r, "Null is not empty", "What compaction"))
	}
}

// The history reads back at most 3000 records from the one inspected, and the record
// inspected is the last of them.
func TestAKeysHistoryReadsBackAtMost3000Records(t *testing.T) {
	c := bigCompactedTopic(t)
	page := message(t, c, "big", 3100)
	want := []string{
		"200 a: goes once cleaned",
		"1,500 tombstone: an earlier delete of this key",
		"1,800 b: goes once cleaned",
		"3,099 c: goes once cleaned",
		"3,100 tombstone: kept until its segment is cleaned and it is older than delete.retention.ms (1d), then goes",
	}
	if got := historyOf(page); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("history of hot at 3100:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if r := readable(page); !strings.Contains(r, "Read 3000 records up to this one.") {
		t.Errorf("the history says how many records it read:\n%s", between(r, "What compaction will do", "Records of this key"))
	}
}

// A tombstone that is the first record of its partition still has a history: itself.
func TestATombstoneAtTheStartOfAPartitionHasAHistory(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"first"}, "partitions": {"1"}, "cleanup.policy": {"compact"}})
	sendForm(t, c, "first", url.Values{"partition": {"0"}, "key": {"k"}, "tombstone": {"1"}})
	sendForm(t, c, "first", url.Values{"partition": {"0"}, "key": {"k"}, "value": {"back"}})
	page := message(t, c, "first", 0)
	if got := historyOf(page); len(got) != 1 || !strings.HasPrefix(got[0], "0 tombstone: kept until") {
		t.Errorf("history of the first record: %q", got)
	}
	if r := readable(page); !strings.Contains(r, "Read 1 records up to this one.") {
		t.Errorf("the history reads the one record:\n%s", between(r, "What compaction will do", "Records of this key"))
	}
}

// With a few records the history reads each once: the next read starts after the last
// record of the one before.
func TestAKeysHistoryReadsEachRecordOnce(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"levels"}, "partitions": {"1"}, "cleanup.policy": {"compact"}})
	sendForm(t, c, "levels", url.Values{"partition": {"0"}, "key": {"SKU-1"}, "value": {"onHand 42"}})
	sendForm(t, c, "levels", url.Values{"partition": {"0"}, "key": {"SKU-2"}, "value": {"onHand 7"}})
	sendForm(t, c, "levels", url.Values{"partition": {"0"}, "key": {"SKU-1"}, "tombstone": {"1"}})
	page := message(t, c, "levels", 2)
	if got := historyOf(page); len(got) != 2 || !strings.HasPrefix(got[0], "0 onHand 42: goes once cleaned") || !strings.HasPrefix(got[1], "2 tombstone: kept until") {
		t.Errorf("history of SKU-1: %q", got)
	}
	if r := readable(page); !strings.Contains(r, "Read 3 records up to this one.") {
		t.Errorf("the history read %s", between(r, "What compaction will do", "Records of this key"))
	}
}

// Null and empty are two kinds of value; each row says where the nearest other record
// of its kind is, and a tie goes to the earlier offset.
func TestTheLegendSaysWhereTheNearestOfEachKindIs(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"kinds"}, "partitions": {"1"}})
	sendForm(t, c, "kinds", url.Values{"partition": {"0"}, "key": {"k0"}, "tombstone": {"1"}})
	sendForm(t, c, "kinds", url.Values{"partition": {"0"}, "key": {"k1"}, "value": {}})
	sendForm(t, c, "kinds", url.Values{"partition": {"0"}, "key": {"k2"}, "value": {"x"}})
	sendForm(t, c, "kinds", url.Values{"partition": {"0"}, "key": {"k3"}, "value": {}})
	for _, tc := range []struct {
		offset int
		want   string
	}{
		{0, "null=this record empty=offset 1"},
		{1, "null=offset 0 empty=this record"},
		{3, "null=offset 0 empty=this record"},
	} {
		if got := strings.Join(legendOf(message(t, c, "kinds", tc.offset)), " "); got != tc.want {
			t.Errorf("the legend at offset %d: %q, want %q", tc.offset, got, tc.want)
		}
	}
	// Two records of the other kind, one each side and as near: the earlier is named.
	sendForm(t, c, "kinds", url.Values{"partition": {"0"}, "key": {"k4"}, "tombstone": {"1"}})
	sendForm(t, c, "kinds", url.Values{"partition": {"0"}, "key": {"k5"}, "value": {}})
	sendForm(t, c, "kinds", url.Values{"partition": {"0"}, "key": {"k6"}, "tombstone": {"1"}})
	if got := strings.Join(legendOf(message(t, c, "kinds", 5)), " "); got != "null=offset 4 empty=this record" {
		t.Errorf("a tie between offsets 4 and 6: %q", got)
	}
}

// The window the legend reads starts where the partition does: a partition whose start
// has been trimmed is read from its start, not from offsets that are no longer there.
func TestTheLegendReadsAPartitionFromItsStart(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"trim"}, "partitions": {"1"}})
	recs := make([]*kgo.Record, 10)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: "trim", Partition: 0, Key: []byte("k"), Value: []byte(fmt.Sprintf("v%d", i))}
	}
	recs[6].Value = nil
	cl := rawClient(t, addr)
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	deleteBefore(t, cl, "trim", 0, 5)
	r := readable(message(t, c, "trim", 6))
	if !strings.Contains(r, "Offsets are the nearest of the 5 records read around this one.") {
		t.Errorf("the legend of a trimmed partition:\n%s", between(r, "Null is not empty", "Send again"))
	}
}

// nearest is the closest record of its kind, either side, never the record itself; of two
// as close, the earlier.
func TestNearestIsTheClosestRecordOfItsKind(t *testing.T) {
	all := func(Message) bool { return true }
	at := func(offs ...int64) []Message {
		var ms []Message
		for _, o := range offs {
			ms = append(ms, Message{Offset: o})
		}
		return ms
	}
	for _, tc := range []struct {
		name  string
		msgs  []Message
		at    int64
		is    func(Message) bool
		want  int64
		found bool
	}{
		{"the far one comes first and the near one after", at(0, 5), 4, all, 5, true},
		{"the near one is before and the far one after", at(9, 14), 10, all, 9, true},
		{"a tie goes to the earlier", at(3, 5), 4, all, 3, true},
		{"never the record itself", at(4), 4, all, 0, false},
		{"only those that are of the kind", at(1, 2, 8), 3, func(m Message) bool { return m.Offset != 2 }, 1, true},
		{"nothing", nil, 3, all, 0, false},
	} {
		if got, ok := nearest(tc.msgs, tc.at, tc.is); got != tc.want || ok != tc.found {
			t.Errorf("%s: nearest = %d, %v; want %d, %v", tc.name, got, ok, tc.want, tc.found)
		}
	}
}
