package console

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A transactional producer against the console's broker, on partition 0 of topic.
func txnProducer(t *testing.T, addr, id string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.TransactionalID(id), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.DialTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func txnWrite(t *testing.T, ctx context.Context, cl *kgo.Client, topic, value string) {
	t.Helper()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte(value)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// The tape tells records a read-committed consumer would see from those it would not
// (aborted ones, and ones whose transaction is still open), draws the markers as what
// they are, and can show the committed view alone.
func TestTapeShowsTransactions(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"tx"}, "partitions": {"1"}})

	p := txnProducer(t, addr, "tape")
	for _, step := range []struct {
		value string
		ok    bool
	}{{"aborted-record", false}, {"kept-record", true}} {
		if err := p.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		txnWrite(t, ctx, p, "tx", step.value)
		if err := p.EndTransaction(ctx, kgo.TransactionEndTry(step.ok)); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	txnWrite(t, ctx, p, "tx", "open-record")

	page := req(t, c, "GET", "/_console/topics/tx", nil).Body.String()
	row := func(body, value string) string {
		i := strings.Index(body, value)
		if i < 0 {
			return ""
		}
		start := strings.LastIndex(body[:i], "<tr ")
		return body[start:i]
	}
	if r := row(page, "aborted-record"); !strings.Contains(r, "txn-aborted") || !strings.Contains(r, "txnbadge-aborted") {
		t.Errorf("the aborted record is not marked aborted:\n%s", r)
	}
	if r := row(page, "kept-record"); strings.Contains(r, "txn-") || strings.Contains(r, "txnbadge") {
		t.Errorf("the committed record is marked as a transaction's:\n%s", r)
	}
	if r := row(page, "open-record"); !strings.Contains(r, "txn-open") || !strings.Contains(r, "txnbadge-open") {
		t.Errorf("the open transaction's record is not marked open:\n%s", r)
	}
	if n := strings.Count(page, `class="txnmark"`); n != 2 {
		t.Errorf("%d marker rows, want 2 (one abort, one commit)", n)
	}
	if !strings.Contains(page, "committed=1") {
		t.Error("no way to show only the committed records")
	}

	only := req(t, c, "GET", "/_console/topics/tx?committed=1", nil).Body.String()
	if !strings.Contains(only, "kept-record") {
		t.Error("the committed view lost the committed record")
	}
	for _, gone := range []string{"aborted-record", "open-record", `class="txnmark"`} {
		if strings.Contains(only, gone) {
			t.Errorf("the committed-only view still shows %q", gone)
		}
	}
	if !strings.Contains(only, "show all") {
		t.Error("the committed-only view has no way back")
	}
	if !strings.Contains(only, "tape?partition=0&amp;committed=1") {
		t.Error("the live refresh of the committed-only view would drop the filter")
	}

	// The Cluster page lists the transaction while it is open, and not after.
	cluster := req(t, c, "GET", "/_console/cluster?view=transactions", nil).Body.String()
	for _, want := range []string{"tape", "Ongoing", "tx-0"} {
		if !strings.Contains(cluster, want) {
			t.Errorf("the Cluster page does not show the open transaction (missing %q)", want)
		}
	}
	if err := p.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	cluster = req(t, c, "GET", "/_console/cluster?view=transactions", nil).Body.String()
	if !strings.Contains(cluster, "No transaction is open") {
		t.Error("the Cluster page still lists a transaction that ended")
	}
	after := req(t, c, "GET", "/_console/topics/tx", nil).Body.String()
	if r := row(after, "open-record"); strings.Contains(r, "txn-open") {
		t.Errorf("a record stays marked open after its transaction committed:\n%s", r)
	}
}

// A window whose records end in a marker does not wait for records that are not there.
func TestAWindowEndingInAMarkerIsPrompt(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"m"}, "partitions": {"1"}})
	p := txnProducer(t, addr, "prompt")
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	txnWrite(t, ctx, p, "m", "only-record")
	if err := p.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	page := req(t, c, "GET", "/_console/topics/m", nil)
	if page.Code != 200 || !strings.Contains(page.Body.String(), "only-record") {
		t.Fatalf("status %d", page.Code)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("the page took %s: the window waited out its timeout for records that do not exist", d)
	}
}
