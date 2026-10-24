package console

import (
	"strings"
	"testing"
)

func rowText(s confirmSay, label string) string {
	for _, r := range s.Rows {
		if r.Label == label {
			return segsText(r.Value)
		}
	}
	return "<no " + label + " row>"
}

func TestTopicPurgeSentence(t *testing.T) {
	base := confirmFacts{Name: "orders", Partitions: 3, Records: 1500, Counts: []int64{1000, 400, 100}, Ends: []int64{1000, 440, 150}}
	s := topicPurgeSay(base)
	if s.Title != "Purge topic orders" || s.Verb != "Purge 1,500 records" {
		t.Errorf("%q / %q", s.Title, s.Verb)
	}
	if got := segsText(s.Body); got != "Deletes every record in all 3 partitions, up to the high watermark: 1,000 + 400 + 100 = 1,500 records. This is DeleteRecords on each partition." {
		t.Errorf("body: %q", got)
	}
	if got := rowText(s, "New log start"); got != "1,000 / 440 / 150" {
		t.Errorf("log start %q", got)
	}
	one := confirmFacts{Name: "t", Partitions: 1, Records: 1}
	if s = topicPurgeSay(one); s.Verb != "Purge 1 record" || !strings.HasPrefix(segsText(s.Body), "Deletes every record in its 1 partition, up to the high watermark: 1 record.") {
		t.Errorf("one: %q / %q", s.Verb, segsText(s.Body))
	}
	// More partitions than are named one by one are described in totals.
	many := confirmFacts{Name: "t", Partitions: 9, Records: 90}
	for i := 0; i < 9; i++ {
		many.Counts, many.Ends = append(many.Counts, 10), append(many.Ends, 10)
	}
	s = topicPurgeSay(many)
	if got := segsText(s.Body); !strings.Contains(got, "in all 9 partitions, up to the high watermark: 90 records.") || strings.Contains(got, " + ") {
		t.Errorf("nine partitions: %q", got)
	}
	if got := rowText(s, "New log start"); got != "each partition's log end" {
		t.Errorf("%q", got)
	}
	eight := many
	eight.Partitions, eight.Counts, eight.Ends = 8, many.Counts[:8], many.Ends[:8]
	if got := segsText(topicPurgeSay(eight).Body); !strings.Contains(got, "10 + 10 + 10 + 10 + 10 + 10 + 10 + 10 = 90 records") {
		t.Errorf("eight partitions are listed: %q", got)
	}
	short := base
	short.Counts = short.Counts[:2]
	if got := segsText(topicPurgeSay(short).Body); strings.Contains(got, " + ") {
		t.Errorf("counts that do not match the partitions: %q", got)
	}
	// Disk.
	disk := base
	disk.DiskOK, disk.DiskExact, disk.Disk = true, true, topicDisk{LogBytes: 5 * 1024}
	if got := segsText(topicPurgeSay(disk).Body); !strings.Contains(got, "1,500 records, about 5 KiB. This is") {
		t.Errorf("exact: %q", got)
	}
	disk.DiskExact = false
	if got := segsText(topicPurgeSay(disk).Body); !strings.Contains(got, "; the segments that hold them take 5 KiB on disk.") {
		t.Errorf("not exact: %q", got)
	}
	// What is kept.
	if got := rowText(topicPurgeSay(base), "Kept"); got != "the topic and its id, 3 partitions, config and ACLs" {
		t.Errorf("kept: %q", got)
	}
	withOne := base
	withOne.Subjects = []string{"orders-value"}
	if got := rowText(topicPurgeSay(withOne), "Kept"); got != "the topic and its id, 3 partitions, config, ACLs, and the schema subject" {
		t.Errorf("kept with a subject: %q", got)
	}
	withTwo := base
	withTwo.Subjects = []string{"orders-value", "orders-key"}
	if got := rowText(topicPurgeSay(withTwo), "Kept"); !strings.HasSuffix(got, "and the schema subjects") {
		t.Errorf("kept with two: %q", got)
	}
	if got := rowText(topicPurgeSay(base), "Groups"); got != "no group has committed offsets on it" {
		t.Errorf("groups: %q", got)
	}
	grouped := base
	grouped.Groups = []string{"g"}
	if got := rowText(topicPurgeSay(grouped), "Groups"); !strings.HasPrefix(got, "keep their committed offsets") {
		t.Errorf("groups: %q", got)
	}
	// Who is left behind.
	if got := rowText(topicPurgeSay(base), "Readers behind"); got != "none: no group has committed offsets on it" {
		t.Errorf("%q", got)
	}
	if got := rowText(topicPurgeSay(grouped), "Readers behind"); got != "none: every group had read to the end" {
		t.Errorf("%q", got)
	}
	behind := grouped
	behind.Behind, behind.Lags = []string{"g1"}, []int64{1200}
	if got := rowText(topicPurgeSay(behind), "Readers behind"); got != "g1 (lag 1,200) is moved to the new start on its next fetch, with OFFSET_OUT_OF_RANGE if it has no reset policy" {
		t.Errorf("one behind: %q", got)
	}
	behind.Behind, behind.Lags = []string{"g1", "g2"}, []int64{3, 4}
	if got := rowText(topicPurgeSay(behind), "Readers behind"); got != "g1 (lag 3) and g2 (lag 4) are moved to the new start on their next fetch, with OFFSET_OUT_OF_RANGE if they have no reset policy" {
		t.Errorf("two behind: %q", got)
	}
	behind.Behind, behind.Lags = []string{"a", "b", "c"}, []int64{1, 2, 3}
	if got := rowText(topicPurgeSay(behind), "Readers behind"); got != "a (lag 1), b (lag 2) and c (lag 3) are moved to the new start on their next fetch, with OFFSET_OUT_OF_RANGE if they have no reset policy" {
		t.Errorf("three behind: %q", got)
	}
	behind.Behind, behind.Lags = []string{"a", "b", "c", "d", "e"}, []int64{1, 2, 3, 4, 5}
	if got := rowText(topicPurgeSay(behind), "Readers behind"); !strings.HasPrefix(got, "a (lag 1), b (lag 2), c (lag 3) and 2 more are moved") {
		t.Errorf("five behind: %q", got)
	}
}

func TestTopicDeleteSentence(t *testing.T) {
	f := confirmFacts{Name: "orders", Partitions: 2, Records: 14429, DataDir: "/data dir"}
	s := topicDeleteSay(f)
	if s.Title != "Delete topic orders" || s.Verb != "Delete topic" || !strings.HasPrefix(segsText(s.Body), "The topic is removed.") {
		t.Errorf("%+v", s)
	}
	if got := rowText(s, "Data"); got != "all 14,429 records and the 2 partition logs under /data dir are deleted" {
		t.Errorf("data: %q", got)
	}
	f.DataDir, f.Records, f.Partitions = "", 1, 1
	if got := rowText(topicDeleteSay(f), "Data"); got != "all 1 record and its 1 partition log are deleted" {
		t.Errorf("no data dir: %q", got)
	}
	if got := rowText(topicDeleteSay(f), "Offsets"); got != "no group has committed offsets on it" {
		t.Errorf("offsets: %q", got)
	}
	f.Groups = []string{"a", "b"}
	if got := rowText(topicDeleteSay(f), "Offsets"); got != "committed offsets of a and b for this topic are deleted with it" {
		t.Errorf("offsets: %q", got)
	}
	if got := rowText(topicDeleteSay(f), "Schema subject"); got != "<no Schema subject row>" {
		t.Errorf("a subject row with no subjects: %q", got)
	}
	f.Subjects = []string{"orders-value"}
	if got := rowText(topicDeleteSay(f), "Schema subject"); got != "orders-value is kept in the registry; delete it from Schemas if you want it gone" {
		t.Errorf("one subject: %q", got)
	}
	f.Subjects = []string{"orders-key", "orders-value"}
	if got := rowText(topicDeleteSay(f), "Schema subjects"); got != "orders-key and orders-value are kept in the registry; delete them from Schemas if you want them gone" {
		t.Errorf("two subjects: %q", got)
	}
	if got := rowText(topicDeleteSay(f), "Still running"); !strings.HasPrefix(got, "<no") {
		t.Errorf("writers row without writers: %q", got)
	}
	f.Writers = []string{"w"}
	if got := rowText(topicDeleteSay(f), "Still running"); got != "w will get UNKNOWN_TOPIC_OR_PARTITION, or recreate the topic if auto-create is on" {
		t.Errorf("auto-create unknown: %q", got)
	}
	f.AutoKnown, f.AutoCreate = true, true
	if got := rowText(topicDeleteSay(f), "Still running"); got != "w will get UNKNOWN_TOPIC_OR_PARTITION, or recreate the topic: auto-create is on" {
		t.Errorf("auto-create on: %q", got)
	}
	f.AutoCreate = false
	if got := rowText(topicDeleteSay(f), "Still running"); got != "w will get UNKNOWN_TOPIC_OR_PARTITION: auto-create is off" {
		t.Errorf("auto-create off: %q", got)
	}
}

func TestPartitionsConfirmSentence(t *testing.T) {
	spec := confirmByID("topic.partitions")
	s := spec.Say(confirmFacts{Name: "t", Partitions: 1, Add: 1})
	if s.Verb != "Add 1 partition" || !strings.HasPrefix(segsText(s.Body), `Add 1 partition to "t", which has 1 partition now, 2 after? They cannot be removed afterwards`) {
		t.Errorf("%q / %q", s.Verb, segsText(s.Body))
	}
	s = spec.Say(confirmFacts{Name: "t", Partitions: 4})
	if s.Verb != "Add partitions" || !strings.HasPrefix(segsText(s.Body), `Add the number of partitions entered to "t", which has 4 partitions now?`) {
		t.Errorf("%q / %q", s.Verb, segsText(s.Body))
	}
}
