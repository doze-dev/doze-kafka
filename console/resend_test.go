package console

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func commitAt(t *testing.T, addr, group, topic string, offset int64) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	oc := kmsg.NewPtrOffsetCommitRequest()
	oc.Group, oc.Generation = group, -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition, rp.Offset = 0, offset
	rt.Partitions = append(rt.Partitions, rp)
	oc.Topics = append(oc.Topics, rt)
	if _, err := oc.RequestWith(context.Background(), cl); err != nil {
		t.Fatal(err)
	}
}

// Tp-13: Send again answers with the original and the copy side by side, the copy read back
// from the broker; what the copy shares is compared, not claimed; and the groups that have
// read the original are named, with what they will do.
func TestSendAgainSetsTheCopyBesideTheOriginal(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders"}, "partitions": {"1"}})
	sendForm(t, c, "orders", url.Values{"partition": {"0"}, "key": {"HRB-1"}, "value": {`{"a":1}`}, "headers": {`[{"k":"traceparent","v":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},{"k":"source","v":"checkout-api"}]`}})
	sendForm(t, c, "orders", url.Values{"partition": {"0"}, "key": {"HRB-2"}, "value": {`{"a":2}`}})
	commitAt(t, addr, "g-validator", "orders", 1) // read offset 0.
	commitAt(t, addr, "g-audit", "orders", 0)     // has not.

	rec := req(t, c, "POST", "/_console/topics/orders/resend", url.Values{"partition": {"0"}, "offset": {"0"}})
	if rec.Code != 200 {
		t.Fatalf("resend: %d %s", rec.Code, rec.Body.String())
	}
	b := rec.Body.String()
	r := readable(b)
	for _, want := range []string{
		"Original and copy, side by side", "Offset0", "2", // the original at 0, the copy at 2.
		"HRB-1 · 5 B", "equal", "7 B of JSON", "2 · ", "Headers carried over", "so it carries the same trace id",
		"Byte for byte, except when", "no producer id or sequence",
		"g-validator has committed past offset 0, so it has read the original and will read the copy too.",
		"g-audit has not reached the original yet, so it will read both.",
		"Sent again.", "An exact copy of p0 offset 0 was written to p0 at offset 2.",
	} {
		if !strings.Contains(r, want) {
			t.Errorf("the answer lacks %q:\n%s", want, r)
		}
	}
	if strings.Contains(r, "differs") {
		t.Errorf("a copy of the stored bytes differs from the original:\n%s", r)
	}
	if !strings.Contains(b, `id="tape-and-table" hx-swap-oob="outerHTML"`) || !strings.Contains(b, `id="resend-note" hx-swap-oob="true"`) {
		t.Errorf("the tape and the note are carried out of band:\n%s", b[:min(len(b), 600)])
	}
	// The copy is the stored original, byte for byte, and a plain batch.
	orig, cp := fetchRaw(t, c, "orders", 0), fetchRaw(t, c, "orders", 2)
	if !identicalBytes(orig.Key, cp.Key) || !identicalBytes(orig.Value, cp.Value) || !sameHeaders(orig.Headers, cp.Headers) || len(cp.Headers) != 2 {
		t.Errorf("copy %+v != original %+v", cp, orig)
	}
}
