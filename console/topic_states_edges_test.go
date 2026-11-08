package console

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The cleaner card says which partitions it speaks for: all of them, or the one asked
// for on a topic that has more than one.
func TestTheCleanerCardNamesTheScopeItSpeaksFor(t *testing.T) {
	c := &Console{}
	for _, tc := range []struct {
		partition, partitions int32
		want                  string
	}{
		{allPartitions, 3, "all partitions"},
		{1, 3, "partition 1"},
		{0, 1, "all partitions"},
	} {
		if got := c.cleanerCard("t", tc.partition, tc.partitions).Scope; got != tc.want {
			t.Errorf("partition %d of %d: scope %q, want %q", tc.partition, tc.partitions, got, tc.want)
		}
	}
}

// The cleaner is asked about the partitions the topic has, and no further.
func TestTheCleanerIsAskedOnlyAboutThePartitionsTheTopicHas(t *testing.T) {
	c := &Console{logs: fakeLogs{
		"t/0": {CleanedTo: 5}, "t/1": {CleanedTo: 1234}, "t/2": {CleanedTo: 99},
	}}
	got := c.cleanerCard("t", allPartitions, 2)
	if got.CleanedTo != "p0 5 · p1 1,234" {
		t.Errorf("cleaned to %q, want the two partitions the topic has", got.CleanedTo)
	}
}

// Offsets are written with thousands separators, at every size.
func TestOffsetsAreGroupedInThousands(t *testing.T) {
	for in, want := range map[int64]string{0: "0", 12: "12", 999: "999", 1000: "1,000", 123456: "123,456", 1234567: "1,234,567", 100000: "100,000"} {
		if got := groupedInt(in); got != want {
			t.Errorf("groupedInt(%d) = %q, want %q", in, got, want)
		}
	}
}

// A partition whose last stable offset is its end has no transaction holding it back.
func TestAPartitionWithNothingHeldBackHasNoTransactionCard(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "plainx", 2, 3)
	ctx := context.Background()
	for _, p := range []int32{0, 1} {
		if v := c.txnHold(ctx, "plainx", p, 2); v != nil {
			t.Errorf("partition %d, with nothing in flight, is held back: %+v", p, v)
		}
	}
}

// The command to produce from a client asks for the client's properties only when the
// listener needs a login.
func TestTheConsoleProducerCommandAsksForPropertiesOnlyWithALogin(t *testing.T) {
	c := newTestConsole(t)
	if cmd := c.consoleProducer("orders"); strings.Contains(cmd, "--producer.config") || !strings.HasSuffix(cmd, "--topic orders") {
		t.Errorf("an open listener: %q", cmd)
	}
	c.facts.SASLMechanisms = []string{"PLAIN"}
	if cmd := c.consoleProducer("orders"); !strings.HasSuffix(cmd, "--producer.config client.properties") {
		t.Errorf("a listener with a login: %q", cmd)
	}
}

// The cards on the Messages tab: the empty one only when there are bounds and all are
// empty, and says whether the broker needs a login; the log-moved one only for a
// partition that exists and has lost its start.
func TestMessageStatesDecideWhichCardsToShow(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "ms", 2, 3)
	ctx := context.Background()
	detail, err := c.be.DescribeTopic(ctx, "ms")
	if err != nil {
		t.Fatal(err)
	}
	empty := []PartitionBound{{Partition: 0}, {Partition: 1}}
	states := func(partition int32, bounds []PartitionBound) map[string]any {
		data := map[string]any{}
		if bounds != nil {
			data["Bounds"] = bounds
		}
		c.messageStates(ctx, "ms", detail, partition, data)
		return data
	}

	if _, ok := states(allPartitions, nil)["Empty"]; ok {
		t.Error("a page with no bounds says the topic is empty")
	}
	e, ok := states(allPartitions, empty)["Empty"].(map[string]any)
	if !ok || e["SASL"] != false {
		t.Fatalf("empty bounds on an open listener: %+v", e)
	}
	c.facts.SASLMechanisms = []string{"PLAIN"}
	if e, _ := states(allPartitions, empty)["Empty"].(map[string]any); e["SASL"] != true {
		t.Errorf("empty bounds on a listener with a login: %+v", e)
	}

	still := []PartitionBound{{Partition: 0, Earliest: 0, Latest: 3}, {Partition: 1, Earliest: 0, Latest: 0}}
	if _, ok := states(0, still)["LogMoved"]; ok {
		t.Error("a log that still starts at 0 is said to have moved")
	}
	moved := []PartitionBound{{Partition: 0, Earliest: 2, Latest: 3}, {Partition: 1}}
	if m, ok := states(0, moved)["LogMoved"].(map[string]any); !ok || m["Start"] != int64(2) {
		t.Errorf("a log that starts at 2: %+v", m)
	}
	// A partition past the ones the bounds describe has no log to have moved.
	if _, ok := states(2, moved)["LogMoved"]; ok {
		t.Error("a partition the topic does not have is said to have moved")
	}
}

// The groups behind a log's start are the ones that committed below it, and each says
// by how much.
func TestGroupsBehindTheLogStartAreThoseCommittedBelowIt(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "bs", 1, 5)
	commitAt(t, addr, "ga", "bs", 1)
	commitAt(t, addr, "gb", "bs", 3)
	commitAt(t, addr, "gc", "bs", 4)
	rows := c.behindStart(context.Background(), "bs", 0, 3)
	if len(rows) != 1 || rows[0].Group != "ga" || rows[0].Behind != 2 || rows[0].Start != 3 || rows[0].Committed != 1 {
		t.Errorf("behind a start of 3: %+v, want only ga, behind by 2", rows)
	}
}

// The count of arrivals since a pause counts only partitions that grew.
func TestNoRecordsSincePausedAreCountedAsNone(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "still", 2, 2)
	got := page(t, c, "/_console/topics/still/new?ends=0:2,1:0")
	if strings.TrimSpace(got) != `{"n":0,"parts":{}}` {
		t.Errorf("nothing arrived, counted %s", got)
	}
}

// A refused client is named by what it called itself, or by its address when it did
// not; the access check asks about its host, not its port.
func TestTheRefusedClientIsNamedAndCheckedByItsHost(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "den7", 1, 1)
	c.traffic = &fakeTraffic{events: []TrafficEvent{{ID: "r1", Time: time.Now(), Remote: "10.1.2.3:5555", ClientID: "etl", Principal: "User:etl",
		API: "Produce", Version: 9, Topics: []string{"den7"}, Error: "TOPIC_AUTHORIZATION_FAILED", Class: "denied", Count: 1}}}
	v := c.topicDenied(context.Background(), "den7")
	if v == nil || v.Client != "etl" || !strings.Contains(v.CheckURL, "host=10.1.2.3&") {
		t.Fatalf("the card = %+v", v)
	}
	c.traffic = &fakeTraffic{events: []TrafficEvent{{ID: "r2", Time: time.Now(), Remote: "10.1.2.3:5555",
		API: "Fetch", Version: 9, Topics: []string{"den7"}, Error: "TOPIC_AUTHORIZATION_FAILED", Class: "denied", Count: 1}}}
	if v := c.topicDenied(context.Background(), "den7"); v == nil || v.Client != "10.1.2.3:5555" || v.Operation != "read" {
		t.Errorf("a client with no name: %+v", v)
	}
}

// The not-found page lists the refused requests that named the topic: not the console's
// own look, not another error, one line to a client and call, and no more than five.
func TestTheNotFoundPageListsOnlyTheRequestsThatAskedForTheTopic(t *testing.T) {
	c := newTestConsole(t)
	ev := func(id, client, api, errName string) TrafficEvent {
		return TrafficEvent{ID: id, Time: time.Now(), ClientID: client, API: api, Topics: []string{"ghost"}, Error: errName, Class: "refused", Count: 1}
	}
	const unknown = "UNKNOWN_TOPIC_OR_PARTITION"
	c.traffic = &fakeTraffic{events: []TrafficEvent{
		ev("e1", ClientID, "Metadata", unknown),                         // the console's own look.
		ev("e2", "other-md", "Metadata", unknown),                       // another client's.
		ev("e3", ClientID, "Produce", unknown),                          // the console's client, but not looking.
		ev("e4", "denied-one", "Produce", "TOPIC_AUTHORIZATION_FAILED"), // another error.
		ev("e5", "other-md", "Metadata", unknown),                       // the same client and call again.
	}}
	var page errorPage
	c.topicNotFound(context.Background(), "ghost", &page)
	got := map[string]bool{}
	for _, w := range page.Why {
		got[w.Client+"/"+w.API] = true
	}
	if len(page.Why) != 2 || !got["other-md/Metadata"] || !got[ClientID+"/Produce"] {
		t.Errorf("the requests listed: %+v", page.Why)
	}

	var many []TrafficEvent
	for _, n := range []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7"} {
		many = append(many, ev(n, n, "Produce", unknown))
	}
	c.traffic = &fakeTraffic{events: many}
	page = errorPage{}
	c.topicNotFound(context.Background(), "ghost", &page)
	if len(page.Why) != 5 {
		t.Errorf("%d requests listed, want the first five", len(page.Why))
	}
}
