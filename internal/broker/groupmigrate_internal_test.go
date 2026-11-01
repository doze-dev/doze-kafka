package broker

import (
	"reflect"
	"testing"
)

// What a classic member is told and what it uploads are bytes of the consumer protocol,
// and the group leader's upload comes from a client: round trips and refusals.
func TestClassicAssignmentBytesRoundTrip(t *testing.T) {
	in := map[string][]int32{"orders": {2, 0, 1}, "payments": {5}}
	got := decodeClassicAssignment(encodeClassicAssignment(0, in))
	want := map[string][]int32{"orders": {0, 1, 2}, "payments": {5}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round trip: %v, want %v", got, want)
	}
	if got := decodeClassicAssignment(encodeClassicAssignment(0, nil)); len(got) != 0 {
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
	sub, name, ok := seatSubscription([]groupProtocolEntry{{"cooperative-sticky", []byte{1}}, {"range", good}})
	if !ok || name != "range" || !reflect.DeepEqual(sub.topics, []string{"a", "b"}) || sub.version != 0 || sub.owned != nil {
		t.Errorf("%+v %q %v", sub, name, ok)
	}
	if _, _, ok := seatSubscription([]groupProtocolEntry{{"range", []byte{9}}}); ok {
		t.Error("an unreadable subscription was accepted")
	}
}

// From version 1 a subscription says which partitions the member still owns, after its
// user data and before fields this broker does not read. One whose owned partitions
// cannot be read is not read at all: it must not be taken to own nothing.
func TestDecodeClassicSubscriptionOwnedPartitions(t *testing.T) {
	for version := uint16(1); version <= 3; version++ {
		sub, ok := decodeClassicSubscription(owningSubscription(version, "a", 2, 0))
		if !ok || sub.version != int16(version) || !reflect.DeepEqual(sub.topics, []string{"a"}) || !reflect.DeepEqual(sub.owned, map[string][]int32{"a": {2, 0}}) {
			t.Errorf("version %d: %+v %v", version, sub, ok)
		}
		if sub, ok := decodeClassicSubscription(owningSubscription(version, "a")); !ok || sub.owned == nil || len(sub.owned) != 0 {
			t.Errorf("version %d owning nothing: %+v %v", version, sub, ok)
		}
	}
	// With user data in front of the owned partitions.
	withData := owningSubscription(1, "a", 1)
	at := 2 + 4 + 2 + 1 // version, topic count, the one topic.
	withData = append(append(append([]byte{}, withData[:at]...), 0, 0, 0, 3, 'x', 'y', 'z'), withData[at+4:]...)
	if sub, ok := decodeClassicSubscription(withData); !ok || !reflect.DeepEqual(sub.owned, map[string][]int32{"a": {1}}) {
		t.Errorf("with user data: %+v %v", sub, ok)
	}
	// Cut short anywhere after the topics, it is not a subscription.
	whole := owningSubscription(1, "a", 0, 1)
	for cut := at; cut < len(whole); cut++ {
		if sub, ok := decodeClassicSubscription(whole[:cut]); ok {
			t.Errorf("cut at %d of %d it was read as %+v", cut, len(whole), sub)
		}
	}
}

// Both byte formats parse what a client sent: no panic, and nothing is read past the
// end.
func FuzzDecodeConsumerProtocol(f *testing.F) {
	f.Add(subscriptionBytes("a", "b"))
	f.Add(encodeClassicAssignment(0, map[string][]int32{"t": {0, 1}}))
	f.Add([]byte{0, 0, 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, b []byte) {
		decodeConsumerSubscription(b)
		decodeClassicSubscription(b)
		got := decodeClassicAssignment(b)
		// What decodes encodes and decodes to the same.
		if len(got) > 0 {
			again := decodeClassicAssignment(encodeClassicAssignment(0, got))
			if len(again) != len(got) {
				t.Fatalf("%v became %v", got, again)
			}
		}
	})
}
