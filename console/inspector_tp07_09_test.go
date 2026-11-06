package console

import (
	"encoding/base64"
	"net/url"
	"strings"
	"testing"
)

func sendForm(t *testing.T, c *Console, topic string, form url.Values) {
	t.Helper()
	if rec := req(t, c, "POST", "/_console/topics/"+topic+"/produce", form); rec.Code != 200 {
		t.Fatalf("produce %v: %d %s", form, rec.Code, rec.Body.String())
	}
}

func message(t *testing.T, c *Console, topic string, offset int) string {
	t.Helper()
	rec := req(t, c, "GET", "/_console/topics/"+topic+"/message?partition=0&offset="+itoa(offset), nil)
	if rec.Code != 200 {
		t.Fatalf("message %d: %d %s", offset, rec.Code, rec.Body.String())
	}
	return rec.Body.String()
}

// Tp-07: a value that is not text is shown as bytes and says why: its first byte is not
// the registry's marker. Null and empty keys are told apart, with where the other kind sits.
func TestInspectorBinaryValueSaysWhyItIsBytes(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"dispatch"}, "partitions": {"1"}})
	bin := base64.StdEncoding.EncodeToString([]byte("\x0a\x08DP-00417\x10\x02\x1a\x05BAY-3"))
	sendForm(t, c, "dispatch", url.Values{"partition": {"0"}, "key": {"DP-00417"}, "value": {bin}, "value_enc": {"base64"}})
	sendForm(t, c, "dispatch", url.Values{"partition": {"0"}, "key_empty": {"1"}, "value": {bin}, "value_enc": {"base64"}})

	b := message(t, c, "dispatch", 0)
	for _, want := range []string{"binary", "Not readable as text, so shown as bytes", "The first byte is 0x0a, not the registry's 0x00 marker", "Base64 view:", bin, "Null is not empty", "offset 1", "this record: 8 bytes"} {
		if !strings.Contains(b, want) {
			t.Errorf("the binary record lacks %q:\n%s", want, readable(b))
		}
	}
	// The empty key is told from a null one, and its own row is the record's.
	b = message(t, c, "dispatch", 1)
	if !strings.Contains(b, `&#34;&#34; (empty)`) || !strings.Contains(b, "this record") {
		t.Errorf("the empty-key record:\n%s", readable(b))
	}
}

// Tp-08, TOP-38: a record framed with an id the registry has never seen says so, rather
// than looking like any other binary; a record that is not framed says it is not.
func TestInspectorFramedWithAnUnknownIDSaysSo(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders"}, "partitions": {"1"}})
	framed := base64.StdEncoding.EncodeToString([]byte("\x00\x00\x00\x00\x09\x1cHRB-2026"))
	sendForm(t, c, "orders", url.Values{"partition": {"0"}, "key": {"k"}, "value": {framed}, "value_enc": {"base64"}})
	b := readable(message(t, c, "orders", 0))
	if !strings.Contains(b, "Schema id 9 is not registered") {
		t.Errorf("an unknown id is named:\n%s", b)
	}
	if strings.Contains(b, "not the registry's 0x00 marker") {
		t.Errorf("a framed value is not said to lack the marker:\n%s", b)
	}
}

// Tp-08: a value written with a registered schema is shown decoded, with the schema id and
// subject, and Raw gives the bytes with the framing explained.
func TestInspectorShowsADecodedValueAndItsFraming(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders"}, "partitions": {"1"}})
	sendForm(t, c, "orders", url.Values{"partition": {"0"}, "key": {"k"}, "value": {`{"id": 7}`}, "schema_subject": {"orders-value"}})
	b := message(t, c, "orders", 0)
	r := readable(b)
	for _, want := range []string{"Value, decoded", "Avro", "schema 1", "orders-value v1", "Decoded by the registry at", "Raw, 6 B", "magic byte, always zero", "00 00 00 01", "schema id 1, four bytes, big-endian", "after the 5-byte framing"} {
		if !strings.Contains(r, want) {
			t.Errorf("the decoded record lacks %q:\n%s", want, r)
		}
	}
	if !strings.Contains(b, ">Decoded</button>") || !strings.Contains(b, ">Raw</button>") {
		t.Errorf("a decoded value is toggled Decoded and Raw:\n%s", b)
	}
}

// Tp-09: a null value is a tombstone, not an empty value; on a compacted topic it says what
// the cleaner will do with the key, and a topic that is not compacted says nothing is deleted.
func TestInspectorTombstoneNullIsNotEmpty(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"levels"}, "partitions": {"1"}, "cleanup.policy": {"compact"}})
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"plain"}, "partitions": {"1"}})
	for _, topic := range []string{"levels", "plain"} {
		sendForm(t, c, topic, url.Values{"partition": {"0"}, "key": {"SKU-1"}, "value": {"onHand 42"}})
		sendForm(t, c, topic, url.Values{"partition": {"0"}, "key": {"SKU-2"}, "value": {}}) // empty value
		sendForm(t, c, topic, url.Values{"partition": {"0"}, "key": {"SKU-1"}, "tombstone": {"1"}})
	}
	b := message(t, c, "levels", 2)
	r := readable(b)
	for _, want := range []string{"null, a tombstone", `It says "this key is deleted".`, "Null is not empty", "offset 1", "What compaction will do with SKU-1", "goes once cleaned", "delete.retention.ms", "Records of this key that compaction has already removed"} {
		if !strings.Contains(r, want) {
			t.Errorf("the tombstone lacks %q:\n%s", want, r)
		}
	}
	p := readable(message(t, c, "plain", 2))
	if !strings.Contains(p, "This topic is not compacted, so nothing is deleted by it") || strings.Contains(p, "What compaction will do") {
		t.Errorf("a tombstone on a delete topic:\n%s", p)
	}
	if e := readable(message(t, c, "levels", 1)); !strings.Contains(e, "zero bytes, a record and not a delete") {
		t.Errorf("an empty value is not a delete:\n%s", e)
	}
}
