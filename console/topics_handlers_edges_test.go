package console

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/schemaregistry"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The Partitions tab gives a size only to a partition the data directory listed; one
// the bounds name beyond the listing is left without.
func TestAPartitionBeyondTheDiskListingShowsNoSize(t *testing.T) {
	c, _ := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "pb", 1, nil); err != nil {
		t.Fatal(err)
	}
	rows, tot := c.partitionRows(ctx, "pb", []PartitionBound{{Partition: 1}})
	if len(rows) != 1 || rows[0].HasDisk || rows[0].Size != "" || tot.Segments != 0 {
		t.Errorf("a partition the listing does not hold: %+v %+v", rows, tot)
	}
	rows, _ = c.partitionRows(ctx, "pb", []PartitionBound{{Partition: 0}})
	if len(rows) != 1 || !rows[0].HasDisk {
		t.Errorf("a partition the listing holds: %+v", rows)
	}
}

// A topic of several partitions opens on the merged view, but an address that names an
// offset to start from (even 0) is a window of one partition.
func TestAnAddressThatNamesWhereToStartIsNotTheMergedView(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "two", 2, 3)
	const merged = "newest first across partitions"
	if body := readable(req(t, c, "GET", "/_console/topics/two", nil).Body.String()); !strings.Contains(body, merged) {
		t.Fatalf("the plain address is the merged view:\n%s", body)
	}
	for _, q := range []string{"?from=0", "?from=1"} {
		if body := readable(req(t, c, "GET", "/_console/topics/two"+q, nil).Body.String()); strings.Contains(body, merged) {
			t.Errorf("%s shows the merged view", q)
		}
	}
}

// The inspector of a record framed by a registry says which registry decoded it and
// how large the body after the five framing bytes is.
func TestTheInspectorNamesTheRegistryAndTheBodyAfterTheFraming(t *testing.T) {
	for _, builtIn := range []bool{false, true} {
		c, addr, ts := newRegistryConsole(t, func(o *Options) { o.Facts.RegistryBuiltIn = builtIn })
		register(t, c, "events-value", userV1)
		topicWith(t, c, "events", 1, 0)
		codec, err := schemaregistry.NewCodec("AVRO", userV1, nil)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := codec.Encode(1, `{"id":4242}`) // 2 bytes of body after 5 of framing.
		if err != nil {
			t.Fatal(err)
		}
		cl := rawClient(t, addr)
		if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: "events", Value: encoded}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		page := readable(req(t, c, "GET", "/_console/topics/events/message?partition=0&offset=0", nil).Body.String())
		where := "the registry at " + strings.TrimPrefix(ts.URL, "http://")
		if builtIn {
			where = "the built-in registry at " + strings.TrimPrefix(ts.URL, "http://")
		}
		if !strings.Contains(page, "Decoded by "+where+".") {
			t.Errorf("built in %v: the registry is not named %q:\n%s", builtIn, where, page)
		}
		if want := "the rest, " + humanBytes(len(encoded)-5) + ", is the"; !strings.Contains(page, want) || len(encoded) != 7 {
			t.Errorf("built in %v: body size (%d bytes in all) not given as %q:\n%s", builtIn, len(encoded), want, page)
		}
	}
}

// Sending a record names the partition and offset it went to, and the view stays the
// merged one only when it was the merged one that sent it.
func TestSendingFromTheMergedViewStaysMergedAndFromAPartitionStaysThere(t *testing.T) {
	const merged = "newest first across partitions"
	toastOf := func(msg string) string { return `{"toast":` + strconv.QuoteToASCII(msg) + `}` }
	send := func(t *testing.T, c *Console, topic string, form url.Values) (toast, body string) {
		t.Helper()
		form.Set("acks", "1")
		form.Set("value", "v")
		rec := req(t, c, "POST", "/_console/topics/"+topic+"/produce", form)
		if rec.Code != 200 {
			t.Fatalf("produce: %d %s", rec.Code, rec.Body)
		}
		return rec.Header().Get("HX-Trigger"), readable(rec.Body.String())
	}

	c := newTestConsole(t)
	topicWith(t, c, "two", 2, 0)
	toast, body := send(t, c, "two", url.Values{"partition": {"1"}, "partition_ctx": {"all"}})
	if toast != toastOf("Sent to partition 1 · offset 0") {
		t.Errorf("from the merged view: %s", toast)
	}
	if !strings.Contains(body, merged) {
		t.Errorf("sending from the merged view leaves it:\n%s", body)
	}

	toast, body = send(t, c, "two", url.Values{"partition": {"1"}})
	if toast != toastOf("Sent to partition 1 · offset 1") {
		t.Errorf("from no view: %s", toast)
	}
	if strings.Contains(body, merged) {
		t.Errorf("a send that names no view is shown merged:\n%s", body)
	}

	toast, body = send(t, c, "two", url.Values{"partition": {"1"}, "partition_ctx": {"0"}})
	if toast != toastOf("Sent to partition 1 · offset 2 (you are viewing partition 0)") || strings.Contains(body, merged) {
		t.Errorf("from partition 0 to partition 1: %s\n%s", toast, body)
	}

	// A topic of one partition has no merged view to stay in.
	one := newTestConsole(t)
	topicWith(t, one, "solo", 1, 0)
	toast, body = send(t, one, "solo", url.Values{"partition": {"0"}, "partition_ctx": {"all"}})
	if toast != toastOf("Sent to partition 0 · offset 0") || strings.Contains(body, merged) {
		t.Errorf("one partition: %s\n%s", toast, body)
	}
}
