package console

import (
	"strings"
	"testing"
)

// Tp-06: who wrote a record comes from the traffic ledger where the recorder holds the
// request, and is said not to be known otherwise; it is never a guess.
func TestMessageInspectorWrittenByIsTheLedgersOrNotKnown(t *testing.T) {
	src := &fakeTraffic{events: []TrafficEvent{{
		ID: "p1", Kind: "produce", API: "Produce", Topics: []string{"orders"}, ClientID: "checkout-api", TxnID: "checkout-1",
		Facts: TrafficFacts{Partitions: []TrafficPart{{Topic: "orders", Partition: 0, Offset: 1, HasOffset: true, Records: 2}}},
	}}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "orders", 1, 4)

	body := func(off string) string {
		return req(t, c, "GET", "/_console/topics/orders/message?partition=0&offset="+off, nil).Body.String()
	}
	for _, off := range []string{"1", "2"} {
		if b := body(off); !strings.Contains(b, "checkout-api") || !strings.Contains(b, "tx checkout-1") {
			t.Errorf("offset %s: the writer is the request that wrote it:\n%s", off, between(b, "Written by", "</span></span>"))
		}
	}
	for _, off := range []string{"0", "3"} {
		b := body(off)
		if strings.Contains(b, "checkout-api") || !strings.Contains(b, "not known") {
			t.Errorf("offset %s: no request is held for it, so the page says it is not known:\n%s", off, between(b, "Written by", "</span></span>"))
		}
	}
	// The fields the board names.
	b := body("1")
	for _, want := range []string{"Timestamp", `data-style="datetimems"`, "CreateTime", "Sizes", "value 2 B", "headers none", "Follow", "Copy ▾"} {
		if want == "Follow" {
			continue // no trace header on this record
		}
		if !strings.Contains(b, want) {
			t.Errorf("the inspector lacks %q:\n%s", want, b)
		}
	}
	if !strings.Contains(b, "carries no trace id") {
		t.Errorf("a record without a trace id says there is nothing to follow")
	}
}

func TestMessageInspectorWithoutARecorderSaysSo(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "orders", 1, 1)
	b := req(t, c, "GET", "/_console/topics/orders/message?partition=0&offset=0", nil).Body.String()
	if !strings.Contains(b, "not known") || !strings.Contains(b, "the traffic recorder is off") {
		t.Errorf("without a recorder the writer is not known, and says why:\n%s", between(b, "Written by", "</span></span>"))
	}
}

// R8: the console-producer command is offered only where the tool can say the record exactly.
func TestProducerCommandSaysOnlyWhatTheToolCanSay(t *testing.T) {
	view := func(b []byte) bytesView { return newBytesView(b, b == nil) }
	ok := producerCommand("127.0.0.1:9092", "orders", view([]byte("k1")), view([]byte(`{"a":1}`)), []MessageHeader{{Key: "source", Value: []byte("api")}})
	want := `printf 'source:api\tk1\t{"a":1}\n' | kafka-console-producer.sh --bootstrap-server '127.0.0.1:9092' --topic 'orders' --property parse.key=true --property parse.headers=true`
	if ok.Cmd != want {
		t.Errorf("command:\n got %s\nwant %s", ok.Cmd, want)
	}
	if nk := producerCommand("a:1", "t", view(nil), view([]byte("v")), nil); !strings.Contains(nk.Cmd, "printf 'v\\n'") || strings.Contains(nk.Cmd, "parse.key") {
		t.Errorf("a null key is a line of the value alone: %q", nk.Cmd)
	}
	if q := producerCommand("a:1", "t", view([]byte("k")), view([]byte("it's 100%")), nil); !strings.Contains(q.Cmd, `'k\tit'\''s 100%%\n'`) {
		t.Errorf("quotes and percent signs are escaped: %q", q.Cmd)
	}
	for name, p := range map[string]producerView{
		"tombstone":    producerCommand("a:1", "t", view([]byte("k")), view(nil), nil),
		"empty value":  producerCommand("a:1", "t", view([]byte("k")), view([]byte{}), nil),
		"empty key":    producerCommand("a:1", "t", view([]byte{}), view([]byte("v")), nil),
		"binary value": producerCommand("a:1", "t", view([]byte("k")), view([]byte{0, 1, 2, 0xff}), nil),
		"tab in value": producerCommand("a:1", "t", view([]byte("k")), view([]byte("a\tb")), nil),
		"newline":      producerCommand("a:1", "t", view([]byte("k")), view([]byte("a\nb")), nil),
		"comma header": producerCommand("a:1", "t", view([]byte("k")), view([]byte("v")), []MessageHeader{{Key: "h", Value: []byte("a,b")}}),
		"null header":  producerCommand("a:1", "t", view([]byte("k")), view([]byte("v")), []MessageHeader{{Key: "h"}}),
	} {
		if p.Cmd != "" || p.Why == "" {
			t.Errorf("%s: inexpressible, so no command and a reason; got %+v", name, p)
		}
	}
}
