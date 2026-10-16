package console

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// confirmJSON is what the dialog is given when its button is pressed.
type confirmJSON struct {
	Title string        `json:"title"`
	Verb  string        `json:"verb"`
	Text  string        `json:"text"`
	Body  []confirmSeg  `json:"body"`
	Rows  []confirmLine `json:"rows"`
}

func askConfirm(t *testing.T, c *Console, path string) confirmJSON {
	t.Helper()
	rec := req(t, c, "GET", path, nil)
	if rec.Code != 200 {
		t.Fatalf("%s: %d %s", path, rec.Code, rec.Body)
	}
	var got confirmJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func segText(s []confirmSeg) string {
	var b strings.Builder
	for _, x := range s {
		b.WriteString(x.T)
	}
	return b.String()
}

func rowsOf(c confirmJSON) map[string]string {
	out := map[string]string{}
	for _, r := range c.Rows {
		out[r.Label] = segText(r.Value)
	}
	return out
}

// Tp-24 draws the purge and the delete of a topic as a title that names it, a
// paragraph of what happens, a table of what goes and what stays, and a button that
// names the verb (and, for a purge, how many records). Each value is measured, and
// what each says happens is then made to happen and checked.
func TestTopicConfirmsAreTp24(t *testing.T) {
	src := sampleTraffic() // "billing" produced to "orders".
	c, addr, _ := newRegistryConsole(t, func(o *Options) { o.Traffic = src; o.Facts.KafkaMajor = 4; o.Facts.AutoCreateTopics = false })
	ctx := context.Background()
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders"}, "partitions": {"3"}})
	for p, n := range []int{5, 4, 3} {
		produceTo(t, c, "orders", p, n)
	}
	register(t, c, "orders-value", userV1)
	commitFor(t, addr, "g-behind", "orders", map[int32]int64{0: 1, 1: 4, 2: 3}) // 4 records unread.
	commitFor(t, addr, "g-done", "orders", map[int32]int64{0: 5, 1: 4, 2: 3})

	p := askConfirm(t, c, "/_console/confirm/topic.purge?name=orders")
	if p.Title != "Purge topic orders" || p.Verb != "Purge 12 records" {
		t.Errorf("title, verb = %q, %q", p.Title, p.Verb)
	}
	body := segText(p.Body)
	if !strings.HasPrefix(body, "Deletes every record in all 3 partitions, up to the high watermark: 5 + 4 + 3 = 12 records, about ") ||
		!strings.HasSuffix(body, ". This is DeleteRecords on each partition.") {
		t.Errorf("purge body: %q", body)
	}
	rows := rowsOf(p)
	for label, want := range map[string]string{
		"Kept":           "the topic and its id, 3 partitions, config, ACLs, and the schema subject",
		"Groups":         "keep their committed offsets; a group that had read everything carries on with the next record",
		"New log start":  "5 / 4 / 3",
		"Readers behind": "g-behind (lag 4) is moved to the new start on its next fetch, with OFFSET_OUT_OF_RANGE if it has no reset policy",
	} {
		if rows[label] != want {
			t.Errorf("purge %s = %q, want %q", label, rows[label], want)
		}
	}
	if len(p.Rows) != 4 {
		t.Errorf("purge has %d rows, want Tp-24's 4", len(p.Rows))
	}

	d := askConfirm(t, c, "/_console/confirm/topic.delete?name=orders")
	if d.Title != "Delete topic orders" || d.Verb != "Delete topic" {
		t.Errorf("delete title, verb = %q, %q", d.Title, d.Verb)
	}
	if got := segText(d.Body); got != "The topic is removed. A new topic with the same name would get a new id and start at offset 0." {
		t.Errorf("delete body: %q", got)
	}
	drows := rowsOf(d)
	wantData := "all 12 records and the 3 partition logs under " + c.facts.DataDir + " are deleted"
	for label, want := range map[string]string{
		"Data":           wantData,
		"Offsets":        "committed offsets of g-behind and g-done for this topic are deleted with it",
		"Schema subject": "orders-value is kept in the registry; delete it from Schemas if you want it gone",
		"Still running":  "billing will get UNKNOWN_TOPIC_OR_PARTITION: auto-create is off",
	} {
		if drows[label] != want {
			t.Errorf("delete %s = %q, want %q", label, drows[label], want)
		}
	}

	// What the purge said: every record goes, each partition starts at its old end,
	// the groups keep their offsets, and a fetch from g-behind's committed offset is
	// answered OFFSET_OUT_OF_RANGE (which is what moves a consumer by its reset policy).
	if rec := req(t, c, "POST", "/_console/topics/orders/purge", nil); rec.Code != 303 {
		t.Fatalf("purge: %d", rec.Code)
	}
	b, err := c.be.PartitionBounds(ctx, "orders", 3)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{5, 4, 3} {
		if b[i].Earliest != want || b[i].Count() != 0 {
			t.Errorf("p%d starts at %d with %d records, want %d and 0", i, b[i].Earliest, b[i].Count(), want)
		}
	}
	if got := offsetsOf(t, c, "g-behind")["orders/0"]; got != 1 {
		t.Errorf("g-behind's offset moved to %d; the confirm said groups keep theirs", got)
	}
	if code := fetchCode(t, addr, "orders", 0, 1); code != kerr.OffsetOutOfRange.Code {
		t.Errorf("a fetch from the old offset answered %d, want OFFSET_OUT_OF_RANGE", code)
	}

	// What the delete said: a new topic of the same name has another id and starts at 0.
	before, _, err := c.be.TopicShape(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if rec := req(t, c, "POST", "/_console/topics/orders/delete", nil); rec.Code != 303 {
		t.Fatalf("delete: %d", rec.Code)
	}
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders"}, "partitions": {"1"}})
	after, _, err := c.be.TopicShape(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	nb, err := c.be.PartitionBounds(ctx, "orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	if after == before || nb[0].Earliest != 0 || nb[0].Latest != 0 {
		t.Errorf("recreated: id %v (was %v), offsets %d-%d; the confirm said a new id from offset 0", after, before, nb[0].Earliest, nb[0].Latest)
	}
	if _, err := c.reg.Versions(ctx, "orders-value"); err != nil {
		t.Errorf("the subject the confirm said is kept is gone: %v", err)
	}
}

// Purge's sentence when no group is behind, and the add-partitions confirm with the
// number entered (it was "the number of partitions entered").
func TestPurgeWithNobodyBehindAndAddPartitionsSaysTheNumber(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"solo"}, "partitions": {"1"}})
	produceTo(t, c, "solo", 0, 1)
	p := askConfirm(t, c, "/_console/confirm/topic.purge?name=solo")
	rows := rowsOf(p)
	if got := segText(p.Body); !strings.HasPrefix(got, "Deletes every record in its 1 partition, up to the high watermark: 1 record") {
		t.Errorf("body: %q", got)
	}
	if rows["Readers behind"] != "none: no group has committed offsets on it" || rows["Groups"] != "no group has committed offsets on it" {
		t.Errorf("rows: %v", rows)
	}
	if p.Verb != "Purge 1 record" {
		t.Errorf("verb %q", p.Verb)
	}
	a := askConfirm(t, c, "/_console/confirm/topic.partitions?name=solo&add=2")
	if !strings.Contains(a.Text, "Add 2 partitions to") || !strings.Contains(a.Text, "1 partition now, 3 after") {
		t.Errorf("add partitions: %q", a.Text)
	}
}

// fetchCode is the error code of a fetch of one partition from an offset.
func fetchCode(t *testing.T, addr, topic string, partition int32, offset int64) int16 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	f := kmsg.NewPtrFetchRequest()
	f.Version = 12
	f.MaxWaitMillis, f.MinBytes, f.MaxBytes = 10, 1, 1<<20
	md := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr(topic)
	md.Topics = []kmsg.MetadataRequestTopic{mt}
	mresp, err := md.RequestWith(context.Background(), cl)
	if err != nil || len(mresp.Topics) != 1 {
		t.Fatalf("metadata: %v", err)
	}
	rt := kmsg.NewFetchRequestTopic()
	rt.Topic, rt.TopicID = topic, mresp.Topics[0].TopicID
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes, rp.LogStartOffset = partition, offset, 1<<20, -1
	rp.CurrentLeaderEpoch = -1
	rt.Partitions = []kmsg.FetchRequestTopicPartition{rp}
	f.Topics = []kmsg.FetchRequestTopic{rt}
	resp, err := f.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatal(err)
	}
	return resp.Topics[0].Partitions[0].ErrorCode
}
