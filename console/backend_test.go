package console

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestCountsAreCorrectAndCheap(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	for _, n := range []string{"a", "b", "c"} {
		req(t, c, "POST", "/_console/new/topic", url.Values{"name": {n}, "partitions": {"4"}})
	}
	// Two groups known by offsets alone.
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for _, g := range []string{"g1", "g2"} {
		oc := kmsg.NewPtrOffsetCommitRequest()
		oc.Group, oc.Generation = g, -1
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = "a"
		rt.Partitions = []kmsg.OffsetCommitRequestTopicPartition{kmsg.NewOffsetCommitRequestTopicPartition()}
		oc.Topics = append(oc.Topics, rt)
		if _, err := oc.RequestWith(context.Background(), cl); err != nil {
			t.Fatal(err)
		}
	}

	topics, groups, err := c.be.Counts(context.Background())
	if err != nil || topics != 3 || groups != 2 {
		t.Fatalf("Counts = %d topics, %d groups, %v; want 3 and 2", topics, groups, err)
	}
}

// The group list batches its describes: the member counts must still be right for
// both protocols, and every group must appear exactly once.
func TestListGroupsCountsMembersAcrossProtocols(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"2"}})

	// franz-go v1.21 puts the KIP-848 protocol behind a beta opt-in: a context
	// value plus a sticky balancer, or the client quietly uses classic.
	var betaKey any = "opt_in_kafka_next_gen_balancer_beta"
	kip848 := []kgo.Opt{
		kgo.WithContext(context.WithValue(context.Background(), betaKey, true)),
		kgo.GroupProtocol("consumer"),
		kgo.Balancers(kgo.StickyBalancer()),
	}

	var wg sync.WaitGroup
	join := func(group string, extra ...kgo.Opt) {
		cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(addr), kgo.ConsumerGroup(group), kgo.ConsumeTopics("t")}, extra...)...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cl.Close)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			cl.PollFetches(ctx) // joins and holds the group while it polls.
		}()
	}
	join("classic-group")
	join("classic-group")
	join("kip848-group", kip848...)
	defer wg.Wait()

	var got []GroupSummary
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		if got, err = c.be.ListGroups(context.Background()); err != nil {
			t.Fatal(err)
		}
		byID := map[string]int{}
		for _, g := range got {
			byID[g.ID] = g.Members
		}
		if byID["classic-group"] == 2 && byID["kip848-group"] == 1 {
			if len(got) != 2 {
				t.Fatalf("expected exactly two groups, got %+v", got)
			}
			protocols := map[string]string{}
			for _, g := range got {
				protocols[g.ID] = g.Protocol
			}
			if protocols["classic-group"] != "classic" || protocols["kip848-group"] != "848" {
				t.Fatalf("protocols = %v, want classic and 848", protocols)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("member counts never converged: %+v", got)
}

// A console whose broker has gone away must answer, not hang: every request is
// bounded, so a dead connection becomes an error page rather than a spinner.
func TestRequestsAreBoundedWhenTheBrokerIsUnreachable(t *testing.T) {
	old := requestTimeout
	requestTimeout = 400 * time.Millisecond
	defer func() { requestTimeout = old }()

	c, err := New(Options{Addr: "192.0.2.1:9092"}) // TEST-NET-1: packets go nowhere, so a dial hangs rather than being refused.
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	start := time.Now()
	rec := req(t, c, "GET", "/_console/topics", nil)
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("a page for an unreachable broker took %v; it must give up at the request timeout", took)
	}
	if rec.Code == http.StatusOK {
		t.Fatalf("expected an error for an unreachable broker, got 200")
	}
}
