package console

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestUpTextBoundaries(t *testing.T) {
	for s, want := range map[int64]string{0: "0 s", 59: "59 s", 60: "1 m 0 s", 3599: "59 m 59 s", 3600: "1 h 0 m", 7500: "2 h 5 m", 172799: "47 h 59 m", 172800: "2 d 0 h", 90000 + 172800: "3 d 1 h"} {
		if got := upText(time.Duration(s) * time.Second); got != want {
			t.Errorf("upText(%ds) = %q, want %q", s, got, want)
		}
	}
}

func TestRetentionTextUnits(t *testing.T) {
	for in, want := range map[string]string{
		"": "", "x": "", "-1": "no time limit", "0": "0 ms", "1": "1 ms", "999": "999 ms", "1000": "1 s", "90000": "90 s", "60000": "1 m",
		"5400000": "90 m", "3600000": "1 h", "7200000": "2 h", "86400000": "1 d", "604800000": "7 d", "90000000": "25 h", " 1000 ": "1 s",
	} {
		if got := retentionText(in); got != want {
			t.Errorf("retentionText(%q) = %q, want %q", in, got, want)
		}
	}
}

// A topic named like a dead-letter queue is flagged only while it holds something.
func TestDeadLetterTopicsAreFlaggedOnlyWhenTheyHoldRecords(t *testing.T) {
	c := newTestConsole(t)
	rows := c.topicRows(context.Background(), []TopicSummary{
		{Name: "orders.dlq", Partitions: 1, Msgs: 0}, {Name: "payments.dlq", Partitions: 1, Msgs: 3}, {Name: "orders", Partitions: 1, Msgs: 9},
	}, nil, false)
	got := map[string][2]bool{}
	for _, r := range rows {
		got[r.Name] = [2]bool{r.DeadLetter, r.NeedsALook}
	}
	if got["orders.dlq"] != [2]bool{false, false} || got["payments.dlq"] != [2]bool{true, true} || got["orders"] != [2]bool{false, false} {
		t.Errorf("%v", got)
	}
}

func TestTopicRowsRetentionAndReaders(t *testing.T) {
	c := newTestConsole(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "keep", 1, map[string]string{"cleanup.policy": "compact"}); err != nil {
		t.Fatal(err)
	}
	if err := c.be.CreateTopic(ctx, "both", 1, map[string]string{"cleanup.policy": "compact,delete", "retention.ms": "86400000"}); err != nil {
		t.Fatal(err)
	}
	if err := c.be.CreateTopic(ctx, "plain", 1, map[string]string{"retention.ms": "3600000"}); err != nil {
		t.Fatal(err)
	}
	topics := []TopicSummary{{Name: "both", Partitions: 1}, {Name: "keep", Partitions: 1}, {Name: "plain", Partitions: 1}}
	groups := []groupRow{{GroupSummary: GroupSummary{ID: "g1"}, Topics: []string{"plain"}}, {GroupSummary: GroupSummary{ID: "g2"}, Topics: []string{"plain", "keep"}}}
	rows := c.topicRows(ctx, topics, groups, true)
	by := map[string]topicRow{}
	for _, r := range rows {
		by[r.Name] = r
	}
	if r := by["keep"]; !r.Compact || r.Retention != "kept per key" || r.ReadByCount != 1 {
		t.Errorf("keep: %+v", r)
	}
	if r := by["both"]; !r.Compact || r.Retention != "1 d" || r.ReadByCount != 0 {
		t.Errorf("both: %+v", r)
	}
	if r := by["plain"]; r.Compact || r.Retention != "1 h" || strings.Join(r.ReadBy, ",") != "g1,g2" || r.ReadByCount != 2 {
		t.Errorf("plain: %+v", r)
	}
}

// The Cluster page: the sentence, what the broker declares, which view is on, the
// counts beside each view, and the partitions that compact.
func TestClusterPage(t *testing.T) {
	started := time.Now().Add(-2*time.Hour - 5*time.Minute)
	c, addr := newTestConsoleAddr(t, func(o *Options) {
		o.Facts.KafkaMajor, o.Facts.Started, o.Facts.Version = 4, started, "v9.9.9"
	})
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "cmp", 3, map[string]string{"cleanup.policy": "compact"}); err != nil {
		t.Fatal(err)
	}
	if err := c.be.CreateTopic(ctx, "plain", 2, nil); err != nil {
		t.Fatal(err)
	}
	page := req(t, c, "GET", "/_console/cluster", nil).Body.String()
	sentence := between(page, `id="cluster-sentence"`, "</div>")
	for _, want := range []string{"has been up", "2 h 5 m", "serving", "Kafka 4.0"} {
		if !strings.Contains(sentence, want) {
			t.Errorf("the sentence lacks %q:\n%s", want, sentence)
		}
	}
	if !regexp.MustCompile(`>2</span> topics in <span class="m b">5</span> partitions`).MatchString(sentence) {
		t.Errorf("the sentence does not count two topics in five partitions:\n%s", sentence)
	}
	if !regexp.MustCompile(`Compacted partitions</span><span class="m ra b">3</span>`).MatchString(page) {
		t.Errorf("compacted partitions are not 3:\n%s", between(page, "Compacted", "</a>"))
	}
	if !strings.Contains(page, "v9.9.9 · declares Kafka 4.0") {
		t.Error("the identity card does not say what the broker declares")
	}
	for view, wantLabel := range map[string]string{"": "Overview", "config": "Configuration", "apis": "Supported APIs", "transactions": "Transactions", "bogus": "Overview"} {
		body := req(t, c, "GET", "/_console/cluster?view="+view, nil).Body.String()
		rail := between(body, `aria-label="Cluster pages"`, "</aside>")
		on := regexp.MustCompile(`<a class="li on" href="[^"]*" aria-current="page">([A-Za-z ]+)<`).FindAllStringSubmatch(rail, -1)
		if len(on) != 1 || on[0][1] != wantLabel {
			t.Errorf("view %q: highlighted %v, want only %s", view, on, wantLabel)
		}
		if view != "" && view != "bogus" && !strings.Contains(rail, "/cluster?view="+view) {
			t.Errorf("view %q: no link to it", view)
		}
	}

	// An open transaction is counted beside Transactions.
	p, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.TransactionalID("rail"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := p.ProduceSync(ctx, &kgo.Record{Topic: "plain", Partition: 0, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	rail := between(req(t, c, "GET", "/_console/cluster", nil).Body.String(), `aria-label="Cluster pages"`, "</aside>")
	if !strings.Contains(rail, `<span class="m warn">1 open</span>`) {
		t.Errorf("the rail does not say one transaction is open:\n%s", rail)
	}
}

// A console not told the Kafka major or a start time says nothing about either.
func TestClusterPageWithoutFacts(t *testing.T) {
	c := newTestConsole(t)
	page := req(t, c, "GET", "/_console/cluster", nil).Body.String()
	sentence := between(page, `id="cluster-sentence"`, "</div>")
	if strings.Contains(sentence, "has been up") || strings.Contains(sentence, "serving") {
		t.Errorf("the sentence claims facts nobody gave:\n%s", sentence)
	}
}
