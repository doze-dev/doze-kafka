package meta

import (
	"encoding/binary"
	"os"
	"testing"
	"time"
)

// v2 builds a livenessV2 value by hand: flags, empty since, session ms, epoch,
// two length-prefixed strings, and optionally the assignment epoch.
func v2(flags byte, since, sessMs uint64, epoch uint32, proto, assignor string, asnEpoch *uint32) []byte {
	b := []byte{livenessV2, flags}
	b = binary.BigEndian.AppendUint64(b, since)
	b = binary.BigEndian.AppendUint64(b, sessMs)
	b = binary.BigEndian.AppendUint32(b, epoch)
	b = binary.BigEndian.AppendUint16(b, uint16(len(proto)))
	b = append(b, proto...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(assignor)))
	b = append(b, assignor...)
	if asnEpoch != nil {
		b = binary.BigEndian.AppendUint32(b, *asnEpoch)
	}
	return b
}

func TestDecodeGroupLivenessLegacyForms(t *testing.T) {
	be := func(n uint64) []byte { return binary.BigEndian.AppendUint64(nil, n) }
	if st, ok := decodeGroupLiveness(be(1234)); !ok || st != (GroupLiveness{EmptySince: 1234}) {
		t.Errorf("8 bytes: %+v %v", st, ok)
	}
	c := append([]byte{liveClassic}, be(45000)...)
	if st, ok := decodeGroupLiveness(c); !ok || st != (GroupLiveness{Live: true, Classic: true, Session: 45 * time.Second}) {
		t.Errorf("classic: %+v %v", st, ok)
	}
	n := append([]byte{liveConsumer}, be(6000)...)
	if st, ok := decodeGroupLiveness(n); !ok || st != (GroupLiveness{Live: true, Session: 6 * time.Second}) {
		t.Errorf("consumer: %+v %v", st, ok)
	}
	x := append([]byte{'x'}, be(6000)...)
	if _, ok := decodeGroupLiveness(x); ok {
		t.Error("an unknown 9-byte type was accepted")
	}
	if _, ok := decodeGroupLiveness(be(1)[:5]); ok {
		t.Error("a 5-byte value was accepted")
	}
}

func TestDecodeGroupLivenessV2(t *testing.T) {
	ae := uint32(7)
	st, ok := decodeGroupLiveness(v2(flagLive|flagClassic|flagConsumer, 99, 45000, 3, "consumer", "range", &ae))
	want := GroupLiveness{Live: true, Classic: true, Consumer: true, EmptySince: 99, Session: 45 * time.Second,
		Epoch: 3, ProtocolType: "consumer", Assignor: "range", AssignmentEpoch: 7}
	if !ok || st != want {
		t.Fatalf("got %+v %v, want %+v", st, ok, want)
	}
	// Without the trailing assignment epoch it is read as at Epoch; strings that
	// end exactly at the value's end, and empty ones, are whole.
	for _, c := range []struct{ proto, asn string }{{"consumer", "range"}, {"", ""}, {"p", ""}, {"", "a"}} {
		st, ok := decodeGroupLiveness(v2(0, 0, 0, 5, c.proto, c.asn, nil))
		if !ok || st.ProtocolType != c.proto || st.Assignor != c.asn || st.AssignmentEpoch != 5 || st.Epoch != 5 {
			t.Errorf("%q/%q without assignment epoch: %+v %v", c.proto, c.asn, st, ok)
		}
	}
	// A string longer than what is left is refused, not sliced past the end.
	bad := v2(0, 0, 0, 1, "", "", nil)
	bad = append(bad[:len(bad)-2], 0, 10, 'a', 'b', 'c')
	if _, ok := decodeGroupLiveness(bad); ok {
		t.Error("a truncated string was accepted")
	}
	// Two stray bytes after the strings are neither form.
	if _, ok := decodeGroupLiveness(append(v2(0, 0, 0, 1, "", "", nil), 1, 2)); ok {
		t.Error("trailing bytes accepted")
	}
}

// The durability hook hears each commit with the database path.
func TestDurabilityHookHearsCommits(t *testing.T) {
	var events []string
	restore := SetDurabilityHookForTest(func(ev, path string) { events = append(events, ev+" "+path) })
	path := t.TempDir() + "/meta.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	events = nil
	if _, err := s.CreateTopic("t", 1, nil); err != nil {
		t.Fatal(err)
	}
	restore()
	if len(events) != 1 || events[0] != "commit "+path {
		t.Fatalf("events = %v", events)
	}
	// A nil hook is no hook.
	restore = SetDurabilityHookForTest(nil)
	defer restore()
	if _, err := s.CreateTopic("u", 1, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSyncDirectory(t *testing.T) {
	if err := syncDirectory(t.TempDir()); err != nil {
		t.Errorf("existing directory: %v", err)
	}
	if err := syncDirectory(t.TempDir() + "/absent"); !os.IsNotExist(err) {
		t.Errorf("absent directory: %v, want not-exist", err)
	}
}
