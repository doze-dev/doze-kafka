package console

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The word above an inspector's title is the kind's, unless the selection gives its own.
func TestTheInspectorCapWordIsTheKindsUnlessTheSelectionGivesItsOwn(t *testing.T) {
	c := &Console{}
	plain := c.inspectData(inspectorView{Kind: KindMember, Key: "k"})
	if plain["InspKindLabel"] != inspectorKindLabels[KindMember] || plain["InspKindLabel"] == "" {
		t.Errorf("label = %v, want the kind's %q", plain["InspKindLabel"], inspectorKindLabels[KindMember])
	}
	own := c.inspectData(inspectorView{Kind: KindMember, Key: "k", Cap: "Folded row"})
	if own["InspKindLabel"] != "Folded row" {
		t.Errorf("label = %v, want the selection's own", own["InspKindLabel"])
	}
}

// A classic member the broker's snapshot does not describe is "not held", not "set on the
// group"; its lag is the lag on the partitions it owns; its requests are found by its client id.
func TestAClassicMembersInspectorSaysWhatItOwnsAndWhereItsRequestsAre(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "mt", 1, 5)
	commitFor(t, addr, "lg", "mt", map[int32]int64{0: 2})
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("lag-client"), kgo.ConsumerGroup("lg"), kgo.ConsumeTopics("mt"), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()
	go func() {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		cl.PollFetches(pctx)
	}()
	var member string
	for deadline := time.Now().Add(8 * time.Second); member == ""; time.Sleep(50 * time.Millisecond) {
		if d, err := c.be.DescribeGroup(ctx, "lg"); err == nil && len(d.Members) == 1 && len(d.Members[0].Assigned["mt"]) == 1 {
			member = d.Members[0].ID
		} else if time.Now().After(deadline) {
			t.Fatal("the member never showed")
		}
	}
	rec := req(t, c, "GET", "/_console/inspect/member?key="+url.QueryEscape(joinKey("lg", member)), nil)
	if rec.Code != 200 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	raw := rec.Body.String()
	body := readable(raw)
	for _, want := range []string{"lag on this member: 3 of 3", "Its requests"} {
		if !strings.Contains(body, want) {
			t.Errorf("the member's inspector lacks %q:\n%s", want, body)
		}
	}
	if !strings.Contains(raw, "/_console/traffic?client=lag-client") {
		t.Errorf("the requests link does not name the client:\n%s", raw)
	}
	if strings.Contains(body, "set on the group, not by the member") || strings.Contains(body, "the consumer protocol has none") {
		t.Errorf("a classic member is described as a consumer-protocol one:\n%s", body)
	}
}

// Tp-13: the headers row of the Send again card says how many headers and how many bytes, or none.
func TestSendAgainCountsTheHeadersItCopies(t *testing.T) {
	c := newTestConsole(t)
	ctx := context.Background()
	with := Message{Offset: 1, Value: []byte("v"), Headers: []MessageHeader{{Key: "ab", Value: []byte("cde")}, {Key: "f", Value: []byte("g")}}}
	rv := c.resendView(ctx, "nowhere", 0, with, with)
	if got := rv.Rows[2]; got.Label != "Headers" || got.What != "2 · 7 B" || !got.Same {
		t.Errorf("headers row = %+v, want %q", got, "2 · 7 B")
	}
	bare := Message{Offset: 1, Value: []byte("v")}
	rv = c.resendView(ctx, "nowhere", 0, bare, bare)
	if got := rv.Rows[2]; got.What != "none" {
		t.Errorf("headers row = %+v, want none", got)
	}
}
