package broker

import (
	"reflect"
	"testing"
)

// What a classic member is told and what it uploads are bytes of the consumer protocol,
// and the group leader's upload comes from a client: round trips and refusals.
func TestClassicAssignmentBytesRoundTrip(t *testing.T) {
	in := map[string][]int32{"orders": {2, 0, 1}, "payments": {5}}
	got := decodeClassicAssignment(encodeClassicAssignment(in))
	want := map[string][]int32{"orders": {0, 1, 2}, "payments": {5}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round trip: %v, want %v", got, want)
	}
	if got := decodeClassicAssignment(encodeClassicAssignment(nil)); len(got) != 0 {
		t.Errorf("an empty assignment decoded to %v", got)
	}
	// What a Java client sends for "mp" partitions 0 and 1 (recorded).
	java := []byte{0, 0, 0, 0, 0, 1, 0, 2, 'm', 'p', 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff}
	if got := decodeClassicAssignment(java); !reflect.DeepEqual(got, map[string][]int32{"mp": {0, 1}}) {
		t.Errorf("a client's bytes decoded to %v", got)
	}
}

func TestClassicAssignmentBytesRefuseLiesAboutLengths(t *testing.T) {
	for name, b := range map[string][]byte{
		"empty":                         nil,
		"short":                         {0, 0, 0},
		"a topic count that cannot fit": {0, 0, 0x7f, 0xff, 0xff, 0xff},
		"a name past the end":           {0, 0, 0, 0, 0, 1, 0xff, 0xff, 'a'},
		"partitions past the end":       {0, 0, 0, 0, 0, 1, 0, 1, 'a', 0x7f, 0xff, 0xff, 0xff},
	} {
		if got := decodeClassicAssignment(b); got != nil && len(got) != 0 {
			t.Errorf("%s: decoded to %v", name, got)
		}
	}
}

// The subscription is read from the first protocol offered that can be read, so a
// member that offers one this broker cannot decode is not given the topics of another
// by accident.
func TestSeatSubscription(t *testing.T) {
	good := subscriptionBytes("a", "b")
	topics, name, ok := seatSubscription([]groupProtocolEntry{{"cooperative-sticky", []byte{1}}, {"range", good}})
	if !ok || name != "range" || !reflect.DeepEqual(topics, []string{"a", "b"}) {
		t.Errorf("%v %q %v", topics, name, ok)
	}
	if _, _, ok := seatSubscription([]groupProtocolEntry{{"range", []byte{9}}}); ok {
		t.Error("an unreadable subscription was accepted")
	}
}

// Both byte formats parse what a client sent: no panic, and nothing is read past the
// end.
func FuzzDecodeConsumerProtocol(f *testing.F) {
	f.Add(subscriptionBytes("a", "b"))
	f.Add(encodeClassicAssignment(map[string][]int32{"t": {0, 1}}))
	f.Add([]byte{0, 0, 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, b []byte) {
		decodeConsumerSubscription(b)
		got := decodeClassicAssignment(b)
		// What decodes encodes and decodes to the same.
		if len(got) > 0 {
			again := decodeClassicAssignment(encodeClassicAssignment(got))
			if len(again) != len(got) {
				t.Fatalf("%v became %v", got, again)
			}
		}
	})
}
