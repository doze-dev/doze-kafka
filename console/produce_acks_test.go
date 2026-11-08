package console

import (
	"bytes"
	"compress/gzip"
	"context"
	"math/rand"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// random is n bytes that gzip cannot make smaller.
func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(7)).Read(b)
	return b
}

// A batch is gzipped only when it is over gzipAbove, and only when that makes it smaller.
func TestABatchIsGzippedOnlyWhenLargeAndOnlyWhenItShrinks(t *testing.T) {
	// A batch of exactly the threshold goes as it is, though zeros would shrink it.
	var exact []byte
	for n := gzipAbove - 200; n < gzipAbove; n++ {
		if b := encodeBatch(time.Unix(0, 0), nil, make([]byte, n), nil); len(b) == gzipAbove {
			exact = b
			break
		}
	}
	if exact == nil {
		t.Fatal("no value makes a batch of exactly gzipAbove bytes")
	}
	if !bytes.Equal(compressed(exact), exact) {
		t.Error("a batch of exactly gzipAbove bytes was compressed")
	}
	over := encodeBatch(time.Unix(0, 0), nil, make([]byte, gzipAbove+1), nil)
	if got := compressed(over); bytes.Equal(got, over) || len(got) >= len(over) {
		t.Errorf("a batch over gzipAbove that shrinks was sent as it is (%d bytes)", len(got))
	}
	// Random bytes grow under gzip, so the batch is sent as it is, however large.
	noise := encodeBatch(time.Unix(0, 0), nil, randomBytes(gzipAbove+100), nil)
	if !bytes.Equal(compressed(noise), noise) {
		t.Error("a batch that gzip would make larger was gzipped")
	}
	// And so is one that gzip would leave the same size: noise with a run of zeros at its end,
	// the run found by trying lengths until the gzip stream is exactly as long as what it holds.
	rnd := randomBytes(gzipAbove)
	for z := 0; z < 400; z++ {
		batch := encodeBatch(time.Unix(0, 0), nil, append(append([]byte{}, rnd...), make([]byte, z)...), nil)
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		w.Write(batch[batchHeaderLen:])
		w.Close()
		if buf.Len() == len(batch)-batchHeaderLen {
			if !bytes.Equal(compressed(batch), batch) {
				t.Errorf("a batch that gzip leaves the same size was gzipped (%d zeros)", z)
			}
			return
		}
	}
	t.Log("no run of zeros makes gzip exactly as long as its input with this Go's compress/flate; the equal-size case is not held")
}

// An acks 0 send is answered by what became of it, not by the broker. A request the broker
// takes and does nothing with (it names no partition) neither moves the log end nor closes
// the connection, so after the wait it is "no confirmation", with no offset.
func TestAcksZeroThatChangesNothingIsNoConfirmationAfterTheWait(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "quiet", 1, 1)
	r := kmsg.NewPtrProduceRequest()
	r.Acks, r.TimeoutMillis = 0, 30000
	start := time.Now()
	res, err := c.be.produceUnconfirmed(context.Background(), c.be.produceNone, r, "quiet", 0, 100)
	if err != nil {
		t.Fatalf("a request that changed nothing was answered with an error: %v", err)
	}
	if res.Confirmed || res.Offset != -1 || res.Partition != 0 {
		t.Errorf("%+v", res)
	}
	if took := time.Since(start); took < ackZeroWait-100*time.Millisecond {
		t.Errorf("answered after %v, before the %v the console waits for a sign", took, ackZeroWait)
	}
}

// With acks 0 the record is seen to arrive (the log end moves) and the console says so at
// once, without waiting out the time it gives a refusal to show itself; a large record has
// not arrived by the time the console first looks, and that is not a refusal.
func TestAcksZeroThatArrivesIsAnsweredAtOnceAndIsNotAReportedRefusal(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "arrives", 1, 0)
	ctx := context.Background()
	for _, size := range []int{10, 900 << 10} {
		start := time.Now()
		res, err := c.be.Produce(ctx, "arrives", 0, nil, randomBytes(size), nil, "0")
		if err != nil {
			t.Fatalf("%d bytes: %v", size, err)
		}
		if res.Confirmed || res.Offset != -1 {
			t.Errorf("%d bytes: %+v", size, res)
		}
		if took := time.Since(start); took >= ackZeroWait {
			t.Errorf("%d bytes: answered after %v; the record had arrived", size, took)
		}
	}
	if _, latest, err := c.be.PartitionOffsets(ctx, "arrives", 0); err != nil || latest != 2 {
		t.Errorf("log end %d (%v), want both records", latest, err)
	}
	if rec := req(t, c, "POST", "/_console/topics/arrives/produce", url.Values{"partition": {"0"}, "value": {"x"}, "acks": {"0"}}); rec.Code != 200 || strings.Contains(rec.Body.String(), "Not delivered") {
		t.Errorf("%d %s", rec.Code, rec.Body)
	}
}
