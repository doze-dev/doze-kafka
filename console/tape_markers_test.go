package console

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// TOP-14: an offset with no record is a transaction marker only if a control batch
// holds it. A gap that compaction left is not a marker, and was drawn as one.
func TestOnlyControlBatchesAreMarkers(t *testing.T) {
	have := map[int64]bool{0: true, 1: true, 4: true, 5: true}
	controls := []offsetSpan{{First: 2, Last: 2}}
	got := markersIn(0, 6, have, controls)
	if !got[2] || got[3] || len(got) != 1 {
		t.Fatalf("markers: %v, want only offset 2 (offset 3 is a gap)", got)
	}
	if len(markersIn(0, 6, have, nil)) != 0 {
		t.Error("with no control batch every gap was taken for a marker")
	}
}

func TestControlSpansReadFromBatches(t *testing.T) {
	batch := func(base int64, lastDelta int32, attrs int16) []byte {
		b := make([]byte, 61)
		put := func(off int, v uint64, n int) {
			for i := 0; i < n; i++ {
				b[off+i] = byte(v >> (8 * (n - 1 - i)))
			}
		}
		put(0, uint64(base), 8)
		put(8, uint64(len(b)-12), 4)
		put(21, uint64(uint16(attrs)), 2)
		put(23, uint64(uint32(lastDelta)), 4)
		return b
	}
	raw := append(append(batch(10, 0, 0), batch(11, 0, 0x20)...), batch(12, 3, 0)...)
	got := controlSpans(raw)
	if len(got) != 1 || got[0] != (offsetSpan{First: 11, Last: 11}) {
		t.Fatalf("spans: %v", got)
	}
}

// TOP-12: Java's and franz-go's partitioners hash any key that is not null, an empty
// one included; only a record with no key at all is spread round robin.
func TestAnEmptyKeyIsHashedLikeAnyKey(t *testing.T) {
	c := newTestConsole(t)
	a, b := c.be.PickPartition("ek", []byte{}, 5), c.be.PickPartition("ek", []byte{}, 5)
	if a != b {
		t.Errorf("an empty key went to p%d, then p%d: it was spread like a record with no key", a, b)
	}
	if got := c.be.PickPartition("ek", nil, 5); got == a && c.be.PickPartition("ek", nil, 5) == got {
		t.Log("no key repeats a partition only by chance")
	}
}

// TOP-13: the composer can send an empty key (not null), and a copy of a record that
// has one keeps it.
func TestTheComposerCanSendAnEmptyKey(t *testing.T) {
	parse := func(kv ...string) produceInput {
		v := url.Values{}
		for i := 0; i < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}
		r := httptest.NewRequest("POST", "/x", strings.NewReader(v.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		in, err := parseProduceForm(r, 1)
		if err != nil {
			t.Fatal(err)
		}
		return in
	}
	if in := parse("value", "v"); in.Key != nil {
		t.Errorf("no key field is a null key, got %q", in.Key)
	}
	if in := parse("value", "v", "key_empty", "1"); in.Key == nil || len(in.Key) != 0 {
		t.Errorf("key_empty must give an empty, non-null key, got %#v", in.Key)
	}
	if in := parse("value", "v", "key", "k", "key_empty", "1"); string(in.Key) != "k" {
		t.Errorf("a key that is typed wins over the box: %q", in.Key)
	}
}

// Tp-05: with no partition named, a topic of several partitions shows the newest
// records of all of them, merged, newest first, and says how many and from where.
func TestTheTapeDefaultsToEveryPartitionMergedNewestFirst(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"merged"}, "partitions": {"3"}})
	for i, p := range []int{1, 0, 2, 1} {
		rec := req(t, c, "POST", "/_console/topics/merged/produce", url.Values{"partition": {itoa(p)}, "key": {"k" + itoa(i)}, "value": {"v" + itoa(i)}})
		if rec.Code != 200 {
			t.Fatalf("produce %d: %d %s", i, rec.Code, rec.Body)
		}
	}
	page := req(t, c, "GET", "/_console/topics/merged", nil).Body.String()
	for _, want := range []string{"Messages · all partitions", "4 shown, newest first across partitions", "log end p0 1 · p1 2 · p2 1", `id="hit-1-1"`, `id="hit-0-0"`, `id="hit-2-0"`, `id="hit-1-0"`} {
		if !strings.Contains(page, want) {
			t.Errorf("the page lacks %q", want)
		}
	}
	// Within a partition the newer offset is first (the order across partitions is the
	// records' timestamps, which a test cannot choose).
	if strings.Index(page, `id="hit-1-1"`) > strings.Index(page, `id="hit-1-0"`) {
		t.Error("partition 1's offset 0 came before its offset 1")
	}
	// A partition named is that partition alone, as before.
	one := req(t, c, "GET", "/_console/topics/merged?partition=1", nil).Body.String()
	if strings.Contains(one, "Messages · all partitions") || !strings.Contains(one, `id="msg-1"`) {
		t.Error("a named partition did not show that partition's own tape")
	}
}
