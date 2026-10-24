package protocol

import (
	"reflect"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

func written(v any, items, depth int) string {
	var b strings.Builder
	writeValue(&b, reflect.ValueOf(v), items, depth)
	return b.String()
}

func TestWriteValueBoundsLists(t *testing.T) {
	ints := []int{1, 2, 3}
	for _, c := range []struct {
		name         string
		v            any
		items, depth int
		want         string
	}{
		{"all of them", ints, 3, 4, `[1, 2, 3]`},
		{"more than shown", ints, 2, 4, `[1, 2, "… 1 more"]`},
		{"none shown", ints, 0, 4, `["… 3 more"]`},
		{"at the depth", ints, 8, 0, `"3 items"`},
		{"one level in", [][]int{{1, 2}, {3}}, 8, 1, `["2 items", "1 items"]`},
		{"two levels in", [][]int{{1, 2}, {3}}, 8, 2, `[[1, 2], [3]]`},
		{"bytes", []byte("abc"), 8, 4, `"3 bytes"`},
		{"an id", [16]byte{0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, 8, 4, `"deadbeef-0102-0304-0506-0708090a0b0c"`},
		{"a short array", [3]byte{1, 2, 255}, 8, 4, `"0102ff"`},
		{"a float", 1.2345678, 8, 4, `1.2345678`},
		{"a negative float", -0.5, 8, 4, `-0.5`},
		{"a nil pointer", (*int)(nil), 8, 4, `null`},
	} {
		if got := written(c.v, c.items, c.depth); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

type inner struct{ B int }
type outer struct{ A inner }

func TestWriteValueBoundsStructs(t *testing.T) {
	if got := written(outer{}, 8, 2); got != `{"a": {"b": 0}}` {
		t.Errorf("deep enough: %s", got)
	}
	if got := written(outer{}, 8, 1); got != `{"a": "…"}` {
		t.Errorf("one level short: %s", got)
	}
	if got := written(outer{}, 8, 0); got != `"…"` {
		t.Errorf("no depth: %s", got)
	}
}

type setting struct{ Name, Value string }
type keyed struct{ Key, Value string }
type keyedInt struct {
	Key   int
	Value string
}

func TestWriteValueHidesTheValueOfASecret(t *testing.T) {
	for _, c := range []struct {
		name string
		v    any
		want string
	}{
		{"by name", setting{"sasl.jaas.config", "hunter2"}, `{"name": "sasl.jaas.config", "value": "(hidden)"}`},
		{"by key", keyed{"ssl.keystore.password", "hunter2"}, `{"key": "ssl.keystore.password", "value": "(hidden)"}`},
		{"a name that is not one", setting{"retention.ms", "100"}, `{"name": "retention.ms", "value": "100"}`},
		{"a key that is not one", keyed{"retention.ms", "100"}, `{"key": "retention.ms", "value": "100"}`},
		{"a key that is not text", keyedInt{7, "x"}, `{"key": 7, "value": "x"}`},
	} {
		if got := written(c.v, 8, 4); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

func TestJSONStringKeepsAHundredAndTwentyEightCharacters(t *testing.T) {
	r := func(n int) string { return strings.Repeat("é", n) }
	if got := written(r(128), 1, 1); got != `"`+r(128)+`"` {
		t.Errorf("128 characters were cut: %s", got)
	}
	if got := written(r(129), 1, 1); got != `"`+r(128)+`…"` {
		t.Errorf("129 characters: %s", got)
	}
}

// The decoded text is held under a kilobyte, and one of exactly that is kept whole.
func TestDecodedFieldsOfExactlyTheLimitAreNotNarrowed(t *testing.T) {
	build := func(last int) *kmsg.MetadataRequest {
		q := kmsg.NewPtrMetadataRequest()
		for i := 0; i < 6; i++ {
			tp := kmsg.NewMetadataRequestTopic()
			s := strings.Repeat("a", 128)
			tp.Topic = &s
			q.Topics = append(q.Topics, tp)
		}
		tp := kmsg.NewMetadataRequestTopic()
		s := strings.Repeat("b", last)
		tp.Topic = &s
		q.Topics = append(q.Topics, tp)
		return q
	}
	found := false
	for last := 1; last <= 128; last++ {
		q := build(last)
		var b strings.Builder
		writeValue(&b, reflect.ValueOf(q), 8, 8)
		if b.Len() != maxDecodedBytes {
			continue
		}
		found = true
		if got := decodedFields(q); got != b.String() {
			t.Errorf("a text of exactly %d bytes was narrowed to %d", maxDecodedBytes, len(got))
		}
	}
	if !found {
		t.Fatal("no request makes a text of exactly the limit")
	}
}
