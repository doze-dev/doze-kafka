package console

import (
	"bytes"
	"context"
	"encoding/base64"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
)

func produceForm(t *testing.T, c *Console, topic string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	form.Set("partition", "0")
	form.Set("acks", "1")
	return req(t, c, "POST", "/_console/topics/"+topic+"/produce", form)
}

func fetchOne(t *testing.T, c *Console, topic string, offset int64) Message {
	t.Helper()
	msgs, err := c.be.FetchWindow(context.Background(), topic, 0, offset, 1)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("FetchWindow(%d) = %d msgs, %v", offset, len(msgs), err)
	}
	return msgs[0]
}

func newTopicWithConsole(t *testing.T) *Console {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"m"}, "partitions": {"3"}})
	return c
}

func TestPartitionPickerMatchesKafka(t *testing.T) {
	c := newTestConsole(t)
	// Golden values from Apache Kafka's own UtilsTest: murmur2 of these keys,
	// made positive, modulo the partition count. A console record with key K must
	// land where a Java (or franz-go) producer would put K.
	for _, tc := range []struct {
		key        string
		partitions int32
		want       int32
	}{
		{"21", 10, 0},     // murmur2 = -973932308
		{"foobar", 10, 6}, // -790332482
		{"abc", 10, 7},    // 479470107
		{"21", 7, int32((uint32(0xffffffff&(-973932308)) & 0x7fffffff) % 7)},
	} {
		if got := c.be.PickPartition("any-topic", []byte(tc.key), tc.partitions); got != tc.want {
			t.Errorf("PickPartition(%q, %d) = %d, want %d (Kafka's murmur2)", tc.key, tc.partitions, got, tc.want)
		}
	}
	// The same key is stable, and a keyless record round-robins.
	if a, b := c.be.PickPartition("t", []byte("k"), 5), c.be.PickPartition("t", []byte("k"), 5); a != b {
		t.Error("a key must always map to the same partition")
	}
	seen := map[int32]bool{}
	for range 6 {
		seen[c.be.PickPartition("rr", nil, 3)] = true
	}
	if len(seen) != 3 {
		t.Errorf("keyless records should spread over all partitions, got %v", seen)
	}
}

func TestProduceCarriesBinaryNullAndEmptyExactly(t *testing.T) {
	c := newTopicWithConsole(t)
	bin := []byte{0x00, 0xff, 0xfe, 0x10, 0x80, 0x7f}
	key := []byte{0xde, 0xad, 0xbe, 0xef}

	// 0: binary key + binary value + a binary header and a text header.
	rec := produceForm(t, c, "m", url.Values{
		"key": {base64.StdEncoding.EncodeToString(key)}, "key_enc": {"base64"},
		"value": {base64.StdEncoding.EncodeToString(bin)}, "value_enc": {"base64"},
		"headers": {`[{"k":"trace","v":"abc"},{"k":"blob","v":"//4A","b64":true}]`},
	})
	if rec.Code != 200 {
		t.Fatalf("binary produce: %d %s", rec.Code, rec.Body)
	}
	// 1: a tombstone with a key. 2: an empty (not null) value, no key.
	produceForm(t, c, "m", url.Values{"key": {"gone"}, "tombstone": {"1"}})
	produceForm(t, c, "m", url.Values{"value": {""}})

	m0 := fetchOne(t, c, "m", 0)
	if !bytes.Equal(m0.Key, key) || !bytes.Equal(m0.Value, bin) {
		t.Fatalf("binary key/value not byte-exact: key %x value %x", m0.Key, m0.Value)
	}
	hdr := map[string][]byte{}
	for _, h := range m0.Headers {
		hdr[h.Key] = h.Value
	}
	if string(hdr["trace"]) != "abc" || !bytes.Equal(hdr["blob"], []byte{0xff, 0xfe, 0x00}) {
		t.Fatalf("headers: %q", hdr)
	}
	if m1 := fetchOne(t, c, "m", 1); m1.Value != nil || string(m1.Key) != "gone" {
		t.Fatalf("a tombstone must have a NULL value, got %#v", m1.Value)
	}
	m2 := fetchOne(t, c, "m", 2)
	if m2.Value == nil || len(m2.Value) != 0 || m2.Key != nil {
		t.Fatalf("an empty value must be empty (not null) with a null key, got key %#v value %#v", m2.Key, m2.Value)
	}

	// The table tells them apart.
	tape := req(t, c, "GET", "/_console/topics/m", nil).Body.String()
	for _, want := range []string{"∅ null", `&#34;&#34;`, "⟨binary · 6 B⟩ 00 ff fe 10 80 7f"} {
		if !strings.Contains(tape, want) {
			t.Errorf("the table is missing %q", want)
		}
	}
	if strings.Contains(tape, "�") {
		t.Error("binary bytes were rendered as replacement characters")
	}
}

func inspectorFor(t *testing.T, c *Console, offset string) string {
	t.Helper()
	rec := req(t, c, "GET", "/_console/topics/m/message?partition=0&offset="+offset, nil)
	if rec.Code != 200 {
		t.Fatalf("inspector %s: %d %s", offset, rec.Code, rec.Body)
	}
	return rec.Body.String()
}

func TestInspectorShowsBinaryReadablyAndOffersTheBytes(t *testing.T) {
	c := newTopicWithConsole(t)
	bin := bytes.Repeat([]byte{0x00, 0xff, 0x41, 0x42}, 8)
	produceForm(t, c, "m", url.Values{"value": {base64.StdEncoding.EncodeToString(bin)}, "value_enc": {"base64"}})

	d := inspectorFor(t, c, "0")
	for _, want := range []string{"32 B", "binary", "Hex", "Base64", "00000000  00 ff 41 42", "|..AB..AB", base64.StdEncoding.EncodeToString(bin), "Download value"} {
		if !strings.Contains(d, want) {
			t.Errorf("the inspector for a binary value is missing %q", want)
		}
	}
	if strings.Contains(d, "�") {
		t.Error("the inspector shows replacement characters")
	}

	// Download is the exact bytes, as an attachment, not sniffed as HTML.
	rec := req(t, c, "GET", "/_console/topics/m/message/raw?partition=0&offset=0&field=value", nil)
	if rec.Code != 200 || !bytes.Equal(rec.Body.Bytes(), bin) {
		t.Fatalf("raw download: %d, %d bytes", rec.Code, rec.Body.Len())
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment;") || !strings.Contains(cd, "m-0-0.value.bin") {
		t.Errorf("Content-Disposition = %q", cd)
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Error("a raw download must not be sniffed")
	}
}

func TestInspectorSaysTombstoneAndEmpty(t *testing.T) {
	c := newTopicWithConsole(t)
	produceForm(t, c, "m", url.Values{"key": {"k"}, "tombstone": {"1"}})
	produceForm(t, c, "m", url.Values{"key": {"k"}, "value": {""}})
	if d := inspectorFor(t, c, "0"); !strings.Contains(d, "null — a tombstone") || !strings.Contains(d, "has a null value") {
		t.Error("a tombstone should be called one")
	}
	if d := inspectorFor(t, c, "1"); !strings.Contains(d, ">empty<") && !strings.Contains(d, "has an empty value") {
		t.Error("an empty value should be called empty, not null")
	}
}

var dataAttr = regexp.MustCompile(`data-(key|key-enc|value|value-enc|headers|partition|tombstone)="([^"]*)"`)

// "Send again" must reproduce the record, not a rendering of it: a binary value
// goes back as base64 with the switch on, headers (binary ones included) ride
// along, and the result is byte-identical.
func TestSendAgainReproducesARecordByteForByte(t *testing.T) {
	c := newTopicWithConsole(t)
	bin := []byte{0x00, 0xff, 0xfe, 0x10, 0x80}
	produceForm(t, c, "m", url.Values{
		"key": {"order-7"}, "value": {base64.StdEncoding.EncodeToString(bin)}, "value_enc": {"base64"},
		"headers": {`[{"k":"trace","v":"abc"},{"k":"blob","v":"//4A","b64":true}]`},
	})

	// Read the inspector's resend attributes exactly as the browser would...
	attrs := map[string]string{}
	for _, m := range dataAttr.FindAllStringSubmatch(inspectorFor(t, c, "0"), -1) {
		attrs[m[1]] = html.UnescapeString(m[2])
	}
	if attrs["value-enc"] != "base64" || attrs["key-enc"] != "text" || attrs["key"] != "order-7" {
		t.Fatalf("resend attributes: %v", attrs)
	}
	// ...and submit them as the composer would.
	rec := produceForm(t, c, "m", url.Values{
		"key": {attrs["key"]}, "key_enc": {attrs["key-enc"]},
		"value": {attrs["value"]}, "value_enc": {attrs["value-enc"]},
		"headers": {attrs["headers"]},
	})
	if rec.Code != 200 {
		t.Fatalf("resend: %d %s", rec.Code, rec.Body)
	}

	a, b := fetchOne(t, c, "m", 0), fetchOne(t, c, "m", 1)
	if !bytes.Equal(a.Key, b.Key) || !bytes.Equal(a.Value, b.Value) || len(a.Headers) != len(b.Headers) {
		t.Fatalf("the resent record differs:\n  original %x / %x / %v\n  resent   %x / %x / %v", a.Key, a.Value, a.Headers, b.Key, b.Value, b.Headers)
	}
	for i := range a.Headers {
		if a.Headers[i].Key != b.Headers[i].Key || !bytes.Equal(a.Headers[i].Value, b.Headers[i].Value) {
			t.Errorf("header %d differs: %v vs %v", i, a.Headers[i], b.Headers[i])
		}
	}

	// A tombstone resends as a tombstone.
	produceForm(t, c, "m", url.Values{"key": {"k"}, "tombstone": {"1"}})
	tomb := map[string]string{}
	for _, m := range dataAttr.FindAllStringSubmatch(inspectorFor(t, c, "2"), -1) {
		tomb[m[1]] = html.UnescapeString(m[2])
	}
	if tomb["tombstone"] != "1" {
		t.Errorf("a tombstone's resend should say so: %v", tomb)
	}
}

// Every problem with the form is reported; none is quietly coerced.
func TestProduceFormRejectsWhatItCannotHonour(t *testing.T) {
	c := newTopicWithConsole(t)
	for name, tc := range map[string]struct {
		form url.Values
		want string
	}{
		"bad base64 value": {url.Values{"value": {"not base64!!"}, "value_enc": {"base64"}, "partition": {"0"}}, "not valid base64"},
		"bad base64 key":   {url.Values{"key": {"@@"}, "key_enc": {"base64"}, "partition": {"0"}}, "key is not valid base64"},
		"bad header json":  {url.Values{"headers": {"{nope"}, "partition": {"0"}}, "headers are not valid"},
		"header w/o key":   {url.Values{"headers": {`[{"k":"","v":"x"}]`}, "partition": {"0"}}, "no key"},
		"partition text":   {url.Values{"partition": {"zero"}}, `"zero" is not a partition number`},
		"partition range":  {url.Values{"partition": {"9"}}, "partition 9 does not exist; this topic has 3"},
		"bad acks":         {url.Values{"partition": {"0"}, "acks": {"maybe"}}, "acks must be"},
	} {
		rec := req(t, c, "POST", "/_console/topics/m/produce", tc.form)
		body := html.UnescapeString(rec.Body.String())
		if rec.Code != http.StatusBadRequest || !strings.Contains(body, tc.want) {
			t.Errorf("%s: %d %q, want 400 mentioning %q", name, rec.Code, body, tc.want)
		}
		if strings.Contains(body, "invalid input") {
			t.Errorf("%s: the internal sentinel leaked into the message: %q", name, body)
		}
	}
	// And nothing was written by any of them.
	if msgs, _ := c.be.FetchWindow(context.Background(), "m", 0, 0, 5); len(msgs) != 0 {
		t.Errorf("%d records were written by rejected submissions", len(msgs))
	}
}

func TestTickHeightIsPayloadSizeOnALogScale(t *testing.T) {
	prev := 0
	for _, size := range []int{0, 1, 10, 100, 1000, 10_000, 100_000, 1 << 20, 1 << 30} {
		h := tickHeight(size)
		if h < prev || h < 6 || h > 40 {
			t.Errorf("tickHeight(%d) = %d (previous %d): must be monotonic within 6..40", size, h, prev)
		}
		prev = h
	}
	if tickHeight(0) != 6 || tickHeight(1<<30) != 40 {
		t.Errorf("bounds: empty=%d huge=%d", tickHeight(0), tickHeight(1<<30))
	}
	if tickHeight(100) == tickHeight(100_000) {
		t.Error("a 100 B and a 100 KB record must not look the same")
	}
}
