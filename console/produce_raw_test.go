package console

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Tp-12, TOP-10: a refused produce is shown as the broker answered it: the code, Kafka's
// own sentence for the code, and the broker's ErrorMessage where it sent one (real Kafka
// sends none for MESSAGE_TOO_LARGE, recorded: R14), and never a sentence the console wrote
// as if it were the broker's.
func TestProduceRefusedIsShownAsTheBrokerAnsweredIt(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"limited"}, "partitions": {"1"}, "max.message.bytes": {"1024"}})
	rec := req(t, c, "POST", "/_console/topics/limited/produce", url.Values{"partition": {"0"}, "value": {strings.Repeat("x", 2400)}, "acks": {"1"}})
	if rec.Code < 400 {
		t.Fatalf("an oversize record was accepted: %d", rec.Code)
	}
	b := rec.Body.String()
	for _, want := range []string{"MESSAGE_TOO_LARGE", "larger than the max message size the server will accept"} {
		if !strings.Contains(b, want) {
			t.Errorf("the refusal lacks %q:\n%s", want, b)
		}
	}
	// The sizes are the console's own facts (what it sent, what the topic's config says),
	// said to be that, not pasted into the broker's sentence.
	sent := groupDigits(int64(batchSize(nil, []byte(strings.Repeat("x", 2400)), nil)))
	for _, want := range []string{"batch the console sent is " + sent + " B", "max.message.bytes is 1,024"} {
		if !strings.Contains(b, want) {
			t.Errorf("the refusal lacks the fact %q:\n%s", want, b)
		}
	}
}

// TOP-16: with acks 0 the broker answers nothing and closes the connection on a refused
// batch (recorded: Q9). The console says the record was not delivered; it used to toast
// "Sent".
func TestProduceAcksZeroRefusedSaysNotDelivered(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"limited"}, "partitions": {"1"}, "max.message.bytes": {"1024"}})
	rec := req(t, c, "POST", "/_console/topics/limited/produce", url.Values{"partition": {"0"}, "value": {strings.Repeat("x", 2400)}, "acks": {"0"}})
	if rec.Code < 400 {
		t.Fatalf("acks 0 oversize answered %d, want a refusal: %q", rec.Code, rec.Header().Get("Hx-Trigger"))
	}
	if b := rec.Body.String(); !strings.Contains(b, "Not delivered") {
		t.Errorf("the page says it was not delivered:\n%s", b)
	}
	// And an accepted acks 0 is still sent, said without an offset.
	rec = req(t, c, "POST", "/_console/topics/limited/produce", url.Values{"partition": {"0"}, "value": {"small"}, "acks": {"0"}})
	if rec.Code != 200 || !strings.Contains(rec.Header().Get("Hx-Trigger"), "no confirmation requested") {
		t.Errorf("a small acks 0 record: %d %q", rec.Code, rec.Header().Get("Hx-Trigger"))
	}
}

// TOP-11: another client stored a 3 MiB record; Send again used to fail at the console's
// own 1 MiB batch cap. The raw produce has no cap of its own: the broker decides.
func TestResendOfARecordLargerThanTheConsolesBatchCap(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"big"}, "partitions": {"1"}})
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ProducerBatchMaxBytes(8<<20), kgo.BrokerMaxWriteBytes(16<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: "big", Partition: 0, Value: make([]byte, 3<<20)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	rec := req(t, c, "POST", "/_console/topics/big/resend", url.Values{"partition": {"0"}, "offset": {"0"}})
	if rec.Code != 200 {
		t.Fatalf("resend of a 3 MiB record: %d %s", rec.Code, rec.Body.String()[:min(400, rec.Body.Len())])
	}
}

// Tp-12: a keyless record on a compacted topic is refused, and the page says what the broker
// answered.
func TestProduceKeylessOnACompactedTopicShowsTheBrokersAnswer(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"dispatch"}, "partitions": {"2"}, "cleanup.policy": {"compact"}})
	rec := req(t, c, "POST", "/_console/topics/dispatch/produce", url.Values{"partition": {"1"}, "value": {"x"}})
	t.Log(rec.Code, readable(rec.Body.String()))
	if rec.Code < 400 || !strings.Contains(rec.Body.String(), "INVALID_RECORD") {
		t.Fatalf("keyless on a compacted topic: %d %s", rec.Code, rec.Body.String())
	}
	// An empty key is a key, and is accepted (TOP-13).
	if rec := req(t, c, "POST", "/_console/topics/dispatch/produce", url.Values{"partition": {"1"}, "key_empty": {"1"}, "value": {"x"}}); rec.Code != 200 {
		t.Fatalf("an empty key on a compacted topic: %d %s", rec.Code, rec.Body.String())
	}
}

// The batch is the one Kafka's record format describes: its length and CRC are right, a small
// one goes as it is, and a large, compressible one goes gzipped (TOP-11).
func TestEncodedBatchIsWellFormed(t *testing.T) {
	small := encodeBatch(time.UnixMilli(1600000000000), []byte("k"), []byte("v"), []MessageHeader{{Key: "h", Value: []byte("x")}})
	if got := int(binary.BigEndian.Uint32(small[8:])) + 12; got != len(small) {
		t.Errorf("length field says %d, the batch is %d", got, len(small))
	}
	if crc32.Checksum(small[21:], crc32c) != binary.BigEndian.Uint32(small[17:]) {
		t.Error("the CRC does not cover attributes to the end")
	}
	if small[16] != 2 || binary.BigEndian.Uint32(small[57:]) != 1 || binary.BigEndian.Uint64(small[27:]) != 1600000000000 {
		t.Errorf("header: magic %d records %d first timestamp %d", small[16], binary.BigEndian.Uint32(small[57:]), binary.BigEndian.Uint64(small[27:]))
	}
	if string(compressed(small)) != string(small) {
		t.Error("a small batch is sent as it is")
	}
	big := compressed(encodeBatch(time.Now(), nil, make([]byte, 3<<20), nil))
	if binary.BigEndian.Uint16(big[21:])&7 != 1 || len(big) > 64<<10 {
		t.Errorf("a 3 MiB batch of zeros: codec %d, %d bytes", binary.BigEndian.Uint16(big[21:])&7, len(big))
	}
	if got := int(binary.BigEndian.Uint32(big[8:])) + 12; got != len(big) || crc32.Checksum(big[21:], crc32c) != binary.BigEndian.Uint32(big[17:]) {
		t.Errorf("a gzipped batch's length %d (is %d) or CRC is wrong", got, len(big))
	}
}
