package broker

import (
	"reflect"
	"testing"
)

// The consumer-protocol decoders are exact about lengths: input that fits to the byte
// is read, input one byte short is refused.

func TestDecodeConsumerSubscriptionAtTheEdgesOfItsLength(t *testing.T) {
	for _, tc := range []struct {
		what string
		in   []byte
		want []string
		ok   bool
	}{
		{"no topics, nothing after the count", []byte{0, 0, 0, 0, 0, 0}, nil, true},
		{"five bytes", []byte{0, 0, 0, 0, 0}, nil, false},
		{"one topic with an empty name, nothing after it", []byte{0, 0, 0, 0, 0, 1, 0, 0}, []string{""}, true},
		{"one topic named by one byte, nothing after it", []byte{0, 0, 0, 0, 0, 1, 0, 1, 'a'}, []string{"a"}, true},
		{"one topic whose name is a byte short", []byte{0, 0, 0, 0, 0, 1, 0, 2, 'a'}, nil, false},
		{"a negative count", []byte{0, 0, 0xff, 0xff, 0xff, 0xff}, nil, false},
		{"a count with no room for its topics", []byte{0, 0, 0, 0, 0, 2, 0, 0}, nil, false},
	} {
		got, ok := decodeConsumerSubscription(tc.in)
		if ok != tc.ok || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %q %v, want %q %v", tc.what, got, ok, tc.want, tc.ok)
		}
	}
}

func TestDecodeClassicAssignmentAtTheEdgesOfItsLength(t *testing.T) {
	// A topic that holds no partitions, and nothing after it: read, as nothing.
	got := decodeClassicAssignment([]byte{0, 0, 0, 0, 0, 1, 0, 1, 'a', 0, 0, 0, 0})
	if got == nil || len(got) != 0 {
		t.Errorf("a topic with no partitions: %#v, want an empty assignment", got)
	}
	// The same one byte short of its partition count.
	if got := decodeClassicAssignment([]byte{0, 0, 0, 0, 0, 1, 0, 1, 'a', 0, 0, 0}); got != nil {
		t.Errorf("a partition count cut short: %v, want none", got)
	}
	// One partition, exactly there.
	if got := decodeClassicAssignment([]byte{0, 0, 0, 0, 0, 1, 0, 1, 'a', 0, 0, 0, 1, 0, 0, 0, 7}); !reflect.DeepEqual(got, map[string][]int32{"a": {7}}) {
		t.Errorf("one partition: %v", got)
	}
	// No topics at all.
	if got := decodeClassicAssignment([]byte{0, 0, 0, 0, 0, 0}); got == nil || len(got) != 0 {
		t.Errorf("no topics: %#v, want an empty assignment", got)
	}
}

// A version 1 subscription whose user data claims more bytes than there are is not
// read; it used to be sliced past its end.
func TestDecodeClassicSubscriptionRefusesUserDataLongerThanTheRest(t *testing.T) {
	head := []byte{0, 1, 0, 0, 0, 1, 0, 1, 'a'} // version 1, one topic "a".
	// Four bytes of length, then two bytes: the data claims three.
	in := append(append([]byte{}, head...), 0, 0, 0, 3, 'x', 'y')
	if sub, ok := decodeClassicSubscription(in); ok {
		t.Errorf("user data longer than the rest was read as %+v", sub)
	}
}
