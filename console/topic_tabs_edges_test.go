package console

import (
	"context"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func addParts(query string, now int32) addPartsView {
	var c *Console
	return c.addPartsView(httptest.NewRequest("GET", "/x?"+query, nil), now, nil)
}

func intsFrom(from, to int) []int {
	var out []int
	for i := from; i < to; i++ {
		out = append(out, i)
	}
	return out
}

// The Add partitions form starts one above what the topic has, lists the partitions that
// exist and the ones that would be made, and takes a total only between 1 and 99999.
func TestTheAddPartitionsFormStartsOneAboveAndListsWhatWouldBeMade(t *testing.T) {
	v := addParts("", 3)
	if v.Open || v.Now != 3 || v.Total != 4 {
		t.Errorf("default: open %v now %d total %d, want false 3 4", v.Open, v.Now, v.Total)
	}
	if !reflect.DeepEqual(v.Fewer, []int{0, 1, 2}) || !reflect.DeepEqual(v.More, []int{3}) {
		t.Errorf("default: existing %v new %v", v.Fewer, v.More)
	}
	if v := addParts("add=1", 3); !v.Open {
		t.Error("add=1 does not open the form")
	}
	if v := addParts("add=0", 3); v.Open {
		t.Error("add=0 opens the form")
	}
	for _, tc := range []struct {
		total string
		want  int
	}{
		{"0", 4}, {"-5", 4}, {"x", 4}, {"1", 1}, {"6", 6}, {"99999", 99999}, {"100000", 4}, {"100001", 4},
	} {
		if v := addParts("total="+tc.total, 3); v.Total != tc.want {
			t.Errorf("total=%s: %d, want %d", tc.total, v.Total, tc.want)
		}
	}
	if v := addParts("total=6", 3); !reflect.DeepEqual(v.More, []int{3, 4, 5}) {
		t.Errorf("a total of 6 over 3 would make %v", v.More)
	}
	if v := addParts("total=3", 3); len(v.More) != 0 {
		t.Errorf("a total equal to what exists would make %v", v.More)
	}
	if v := addParts("total=1000", 3); !reflect.DeepEqual(v.More, intsFrom(3, 67)) {
		t.Errorf("a big total lists %d partitions, want the first 64 (3 to 66)", len(v.More))
	}
}

// A config's value as the page says it, read from the key asked for and not another.
func TestAConfigIsShownTheWayThePageSaysItFromTheKeyAskedFor(t *testing.T) {
	entries := []ConfigEntry{{Name: "flush.ms", Value: "5"}, {Name: "retention.ms", Value: "604800000"}, {Name: "retention.bytes", Value: "1073741824"}}
	for key, want := range map[string]string{"retention.ms": "7d", "retention.bytes": "1 GB", "flush.ms": "5", "segment.ms": ""} {
		if got := configShown(entries, key); got != want {
			t.Errorf("configShown(%s) = %q, want %q", key, got, want)
		}
	}
}

func openWrite(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, partition int32) {
	t.Helper()
	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: partition, Value: []byte("open")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// The transactions open on a topic come back by partition, and within one partition by where
// they start.
func TestOpenTransactionsAreListedByPartitionThenByWhereTheyStart(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"ot"}, "partitions": {"2"}})
	// Partition 0 holds three plain records, so a transaction opened there starts at 3 or
	// later, after the one open on partition 1 starts at 0.
	bulkProduce(t, rawClient(t, addr), "ot", 0, 3, func(i int) string { return "plain" }, time.Time{})

	a := txnProducer(t, addr, "ot-a")
	openWrite(t, ctx, a, "ot", 1)
	b := txnProducer(t, addr, "ot-b")
	openWrite(t, ctx, b, "ot", 0)
	d := txnProducer(t, addr, "ot-d")
	openWrite(t, ctx, d, "ot", 0)

	got, err := c.be.TopicOpenTxns(ctx, "ot", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("%d open transactions, want 3: %+v", len(got), got)
	}
	type at struct {
		p     int32
		txn   string
		start int64
	}
	var seen []at
	for _, o := range got {
		seen = append(seen, at{o.Partition, o.TxnID, o.Start})
	}
	if got[0].Partition != 0 || got[1].Partition != 0 || got[2].Partition != 1 {
		t.Errorf("not in partition order: %+v", seen)
	}
	if got[0].Start >= got[1].Start {
		t.Errorf("within partition 0 the transaction that starts first is not first: %+v", seen)
	}
	if got[2].Start != 0 || got[0].Start < 3 {
		t.Errorf("starts are not where the records were written: %+v", seen)
	}
	if got[0].TxnID != "ot-b" || got[1].TxnID != "ot-d" || got[2].TxnID != "ot-a" {
		t.Errorf("transactional ids %+v, want ot-b then ot-d on partition 0, ot-a on 1", seen)
	}
}
