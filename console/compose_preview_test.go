package console

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"
)

func preview(t *testing.T, c *Console, topic string, form url.Values) composePreview {
	t.Helper()
	rec := req(t, c, "POST", "/_console/topics/"+topic+"/compose/preview", form)
	if rec.Code != 200 {
		t.Fatalf("preview: %d %s", rec.Code, rec.Body.String())
	}
	var p composePreview
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("preview is not JSON: %v\n%s", err, rec.Body.String())
	}
	return p
}

// Tp-10: "What will be sent" is what the send would do, from the real partitioner and the
// real encoding of the batch, and asking writes nothing.
func TestComposePreviewSaysWhereAndHowMuch(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders"}, "partitions": {"3"}})
	p := preview(t, c, "orders", url.Values{"key": {"HRB-2026-04812"}, "value": {`{"orderId":"HRB-2026-04812"}`}, "partition": {"auto"}, "acks": {"1"},
		"headers": {`[{"k":"traceparent","v":"00-4bf9"},{"k":"source","v":"console"}]`}})
	want := c.be.keyPartition("orders", []byte("HRB-2026-04812"), 3)
	if p.Partition != "p"+itoa(int(want))+", by key" {
		t.Errorf("partition = %q, the partitioner says p%d", p.Partition, want)
	}
	if p.Key != "14 B" || p.Value != "28 B" || p.Headers != "2 · 31 B" {
		t.Errorf("sizes = key %q value %q headers %q", p.Key, p.Value, p.Headers)
	}
	size := batchSize([]byte("HRB-2026-04812"), []byte(`{"orderId":"HRB-2026-04812"}`), []MessageHeader{{"traceparent", []byte("00-4bf9")}, {"source", []byte("console")}})
	if p.Batch != "1 record, "+groupDigits(int64(size))+" B" {
		t.Errorf("batch = %q, want %d B", p.Batch, size)
	}
	if !strings.Contains(p.PartNote, "murmur2 of the key bytes modulo 3") || !strings.Contains(p.PartNote, "HRB-2026-04812 lands on p") {
		t.Errorf("the note under Partition: %q", p.PartNote)
	}
	// Asking where a key goes does not move the round robin a keyless send uses.
	before := preview(t, c, "orders", url.Values{"value": {"x"}})
	for i := 0; i < 3; i++ {
		preview(t, c, "orders", url.Values{"key": {"k"}, "value": {"x"}})
	}
	if after := preview(t, c, "orders", url.Values{"value": {"x"}}); after.Partition != before.Partition || before.Partition != "in turn" {
		t.Errorf("a keyless preview = %q then %q", before.Partition, after.Partition)
	}
	if n := req(t, c, "GET", "/_console/topics/orders", nil).Body.String(); strings.Contains(n, "msg-0") {
		t.Error("a preview wrote a record")
	}
	// The form's own complaints are said as they would be on Send.
	if p := preview(t, c, "orders", url.Values{"key": {"%%%"}, "key_enc": {"base64"}, "value": {"x"}}); !strings.Contains(p.Error, "not valid base64") {
		t.Errorf("a bad base64 key: %+v", p)
	}
	// An empty key is a key (TOP-12): it is hashed, and called empty, not none.
	if p := preview(t, c, "orders", url.Values{"key_empty": {"1"}, "value": {"x"}}); p.Key != "empty" || !strings.HasSuffix(p.Partition, ", by key") {
		t.Errorf("an empty key: %+v", p)
	}
	if p := preview(t, c, "orders", url.Values{"tombstone": {"1"}}); p.Value != "null, a tombstone" {
		t.Errorf("a tombstone: %+v", p)
	}
}

// Tp-11: a value written as JSON for a subject is checked against its latest schema as it
// is typed, and the preview is the bytes the send would write, with the schema id.
func TestComposePreviewEncodesWithTheSubject(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders"}, "partitions": {"2"}})
	p := preview(t, c, "orders", url.Values{"key": {"k"}, "value": {`{"id": 7}`}, "schema_subject": {"orders-value"}})
	s := p.Schema
	if s == nil || !s.OK || s.ID != 1 || s.Version != 1 || s.Type != "Avro" {
		t.Fatalf("the preview: %+v", s)
	}
	if !strings.HasPrefix(s.Message, "Valid against orders-value v1: id long") {
		t.Errorf("the message: %q", s.Message)
	}
	if s.Bytes != "00 00 00 01" || s.Size != 6 || s.Body != 1 || !strings.Contains(s.Hex, "00 00 00 00 01 0e") {
		t.Errorf("the bytes: %+v", s)
	}
	if p.Value != "6 B" {
		t.Errorf("the value weighs what it is on disk: %q", p.Value)
	}
	// A value that does not fit is said not to, with the registry's words, and nothing is encoded.
	bad := preview(t, c, "orders", url.Values{"value": {`{"id": "seven"}`}, "schema_subject": {"orders-value"}}).Schema
	if bad == nil || bad.OK || !strings.Contains(bad.Message, "does not fit orders-value version 1") {
		t.Errorf("a value that does not fit: %+v", bad)
	}
	// What Send does with it is the same encoding: the record on disk is the previewed bytes.
	sendForm(t, c, "orders", url.Values{"partition": {"0"}, "key": {"k"}, "value": {`{"id": 7}`}, "schema_subject": {"orders-value"}})
	if b := message(t, c, "orders", 0); !strings.Contains(b, "00000000  00 00 00 00 01 0e") {
		t.Errorf("the stored record is not the previewed bytes:\n%s", readable(b))
	}
}
