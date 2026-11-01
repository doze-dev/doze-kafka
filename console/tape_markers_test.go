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
