package main

import (
	"context"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func consoleGet(t *testing.T, base, path string) string {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s = %d: %.200s", path, resp.StatusCode, b)
	}
	return string(b)
}

// The whole path, in the real binary: an app talks to the broker, and the console's
// Traffic page shows what it asked, who it was, and what its group did about it.
func TestTrafficShowsAnAppsRequestsAndItsGroupsTimeline(t *testing.T) {
	out, _ := start(t)
	waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "doze-kafka is up") })
	kafka := regexp.MustCompile(`kafka     (\S+)`).FindStringSubmatch(out.String())[1]
	console := regexp.MustCompile(`console   (http://\S+)`).FindStringSubmatch(out.String())[1]

	cl, err := kgo.NewClient(kgo.SeedBrokers(kafka), kgo.ClientID("orders-app"),
		kgo.ConsumerGroup("orders-workers"), kgo.ConsumeTopics("orders"),
		kgo.AllowAutoTopicCreation(), kgo.SessionTimeout(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("hello")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	// Consume until the group has a generation, so the coordinator has things to say.
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		fetches := cl.PollFetches(ctx)
		if fetches.NumRecords() > 0 {
			break
		}
	}

	page := consoleGet(t, console, "/_console/traffic?client=orders-app")
	for _, want := range []string{"orders-app", "Produce", "orders", "JoinGroup", "orders-workers", "Connected clients"} {
		if !strings.Contains(page, want) {
			t.Errorf("the traffic page does not show %q", want)
		}
	}

	// The console's own polling is not in its own traffic.
	consoleGet(t, console, "/_console/topics")
	consoleGet(t, console, "/_console/groups")
	own := consoleGet(t, console, "/_console/traffic?client=doze-kafka-console")
	if strings.Contains(own, `id="rq-`) {
		t.Errorf("the console's own requests are listed:\n%s", own)
	}

	// The group's timeline names the join and the generation it produced.
	group := consoleGet(t, console, "/_console/groups/orders-workers?tab=timeline")
	for _, want := range []string{`id="group-timeline"`, ">joined<", "new generation"} {
		if !strings.Contains(group, want) {
			t.Errorf("the group page's timeline does not show %q", want)
		}
	}
}
