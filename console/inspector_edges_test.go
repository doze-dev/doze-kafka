package console

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestInspectMemberAndTransaction(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "my-topic", 2, nil); err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup("mg"), kgo.ConsumeTopics("my-topic"), kgo.ClientID("member-client"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	go func() {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		cl.PollFetches(pctx)
	}()
	var member string
	deadline := time.Now().Add(8 * time.Second)
	for member == "" {
		if d, err := c.be.DescribeGroup(ctx, "mg"); err == nil && len(d.Members) == 1 && len(d.Members[0].Assigned["my-topic"]) == 2 {
			member = d.Members[0].ID
		} else if time.Now().After(deadline) {
			t.Fatal("the member never showed")
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	get := func(kind, key string) string {
		rec := req(t, c, "GET", "/_console/inspect/"+kind+"?key="+url.QueryEscape(key), nil)
		if rec.Code != 200 {
			t.Fatalf("%s %s: %d %s", kind, key, rec.Code, rec.Body)
		}
		return readable(rec.Body.String())
	}
	body := get("member", joinKey("mg", member))
	for _, want := range []string{"member-client", "my-topic", "0–1", "Stable", "classic"} {
		if !strings.Contains(body, want) {
			t.Errorf("member page lacks %q:\n%s", want, body)
		}
	}
	if gone := get("member", joinKey("mg", "someone-else")); !strings.Contains(gone, "no longer in the group") {
		t.Errorf("a member who left:\n%s", gone)
	}
	if rec := req(t, c, "GET", "/_console/inspect/member?key=nocolon", nil); rec.Code != 400 {
		t.Errorf("a malformed member key: %d", rec.Code)
	}
	if rec := req(t, c, "GET", "/_console/inspect/member?key="+url.QueryEscape(joinKey("nogroup", "m")), nil); rec.Code != 404 {
		t.Errorf("a member of a group that is not there: %d", rec.Code)
	}

	// A transaction in progress, with its partitions linking to their topics.
	p := txnProducer(t, addr, "txn-1")
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := p.ProduceSync(ctx, &kgo.Record{Topic: "my-topic", Partition: 1, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	raw := req(t, c, "GET", "/_console/inspect/transaction?key=txn-1", nil).Body.String()
	if !strings.Contains(raw, `/_console/topics/my-topic`) || !strings.Contains(readable(raw), "my-topic-1") || !strings.Contains(readable(raw), "Ongoing") {
		t.Errorf("transaction page:\n%s", readable(raw))
	}
	if gone := get("transaction", "txn-gone"); !strings.Contains(gone, "This transaction has ended") {
		t.Errorf("an ended transaction:\n%s", gone)
	}
	if rec := req(t, c, "GET", "/_console/inspect/transaction?key=", nil); rec.Code != 400 {
		t.Errorf("no key: %d", rec.Code)
	}
	if rec := req(t, c, "GET", "/_console/inspect/nothing?key=x", nil); rec.Code != 404 {
		t.Errorf("an unknown kind: %d", rec.Code)
	}
}

func TestInspectSubjectAndEvent(t *testing.T) {
	src := &fakeTraffic{groups: map[string][]TrafficGroupEvent{"g": {{Seq: 7, Kind: "joined", Member: "m-1", Generation: 3, Detail: "joined"}}}}
	c, _, _ := newRegistryConsole(t, func(o *Options) { o.Traffic = src })
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-value", userV2)
	body := readable(req(t, c, "GET", "/_console/inspect/subject?key=orders-value", nil).Body.String())
	for _, want := range []string{"orders-value", "Avro", "BACKWARD"} {
		if !strings.Contains(body, want) {
			t.Errorf("subject inspector lacks %q:\n%s", want, body)
		}
	}
	if rec := req(t, c, "GET", "/_console/inspect/subject?key=nobody", nil); rec.Code != 404 {
		t.Errorf("a missing subject: %d", rec.Code)
	}
	if rec := req(t, c, "GET", "/_console/inspect/subject?key=", nil); rec.Code != 400 {
		t.Errorf("no subject: %d", rec.Code)
	}
	ev := readable(req(t, c, "GET", "/_console/inspect/event?key="+url.QueryEscape(joinKey("g", "7")), nil).Body.String())
	if !strings.Contains(ev, "g") || strings.Contains(ev, "no longer") {
		t.Errorf("event:\n%s", ev)
	}
	for _, key := range []string{"g", joinKey("g", "x")} {
		if rec := req(t, c, "GET", "/_console/inspect/event?key="+url.QueryEscape(key), nil); rec.Code != 400 {
			t.Errorf("event key %q: %d", key, rec.Code)
		}
	}
}
