package log

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func sampleRecords() []Record {
	return []Record{
		{TimestampDelta: 0, OffsetDelta: 0, Key: []byte("k1"), Value: []byte("v1"), Headers: []RecordHeader{{"trace", []byte("abc")}, {"nil-header", nil}}},
		{TimestampDelta: 5, OffsetDelta: 1, Key: nil, Value: []byte("no key")},
		{TimestampDelta: 7, OffsetDelta: 2, Key: []byte("tomb"), Value: nil}, // a tombstone.
		{TimestampDelta: 9, OffsetDelta: 4, Key: []byte{}, Value: []byte{}},  // present and empty is not null.
		{TimestampDelta: -3, OffsetDelta: 5, Key: bytes.Repeat([]byte{0xff}, 300), Value: bytes.Repeat([]byte("x"), 70000)},
	}
}

// What goes in comes out: every field, null versus empty, headers, for every codec.
func TestRecordsRoundTripThroughEveryCodec(t *testing.T) {
	recs := sampleRecords()
	head := makeBatch(1, nil)
	binary.BigEndian.PutUint64(head[batchProducerIDOff:], 77)
	binary.BigEndian.PutUint32(head[batchLastOffsetDeltaOff:], 5)
	for name, codec := range map[string]int{"none": codecNone, "gzip": codecGzip, "snappy": codecSnappy, "lz4": codecLZ4, "zstd": codecZstd} {
		t.Run(name, func(t *testing.T) {
			b, err := rebuildBatch(head, recs, codec)
			if err != nil {
				t.Fatal(err)
			}
			if !crcOK(b) {
				t.Error("the rebuilt batch has a wrong checksum")
			}
			// The header says where the batch is and who wrote it, and is unchanged.
			if binary.BigEndian.Uint64(b[batchProducerIDOff:]) != 77 || binary.BigEndian.Uint32(b[batchLastOffsetDeltaOff:]) != 5 {
				t.Error("producer id or last offset delta changed")
			}
			if got := binary.BigEndian.Uint32(b[batchRecordCountOff:]); got != uint32(len(recs)) {
				t.Errorf("record count %d, want %d", got, len(recs))
			}
			got, err := DecodeRecords(b)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, recs) {
				t.Errorf("records changed:\n got  %+v\n want %+v", got, recs)
			}
			if got[1].Key != nil || got[2].Value != nil || got[3].Key == nil || got[3].Value == nil {
				t.Error("null and empty were confused")
			}
		})
	}
}

func TestDecodeRecordsRefusesLies(t *testing.T) {
	good, _ := rebuildBatch(makeBatch(1, nil), sampleRecords()[:2], codecNone)
	for name, mutate := range map[string]func([]byte) []byte{
		"a record count the body cannot hold": func(b []byte) []byte { binary.BigEndian.PutUint32(b[batchRecordCountOff:], 1<<30); return b },
		"a negative count":                    func(b []byte) []byte { binary.BigEndian.PutUint32(b[batchRecordCountOff:], 0xFFFFFFFE); return b },
		"cut short":                           func(b []byte) []byte { return b[:len(b)-3] },
		"a header only":                       func(b []byte) []byte { return b[:batchFullHeaderBytes] },
		"too short for a header":              func(b []byte) []byte { return b[:20] },
	} {
		if _, err := DecodeRecords(mutate(append([]byte(nil), good...))); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}

// DecodeRecords parses bytes a client sent, so it must never panic or allocate by what a
// length field claims, whatever it is given.
func FuzzDecodeRecords(f *testing.F) {
	for _, codec := range []int{codecNone, codecGzip, codecSnappy} {
		b, _ := rebuildBatch(makeBatch(1, nil), sampleRecords(), codec)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		recs, err := DecodeRecords(b)
		if err != nil {
			return
		}
		// Whatever decodes encodes and decodes to the same.
		again, err := DecodeRecords(mustRebuild(t, b, recs))
		if err != nil || !reflect.DeepEqual(again, recs) {
			t.Fatalf("a decoded batch did not survive re-encoding: %v", err)
		}
	})
}

func mustRebuild(t *testing.T, orig []byte, recs []Record) []byte {
	t.Helper()
	b, err := rebuildBatch(orig, recs, codecNone)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
