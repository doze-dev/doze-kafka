package log

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// bombBatch is a v2 batch with one keyed record whose value is `inflated` zero bytes,
// its records section compressed with codec (gzip or zstd; zstd with the given declared
// window). The batch weighs a few tens of KB; what it inflates to is the point.
func bombBatch(t testing.TB, codec int, window int, inflated int) []byte {
	t.Helper()
	body := EncodeRecords([]Record{{Key: []byte("k"), Value: make([]byte, inflated)}})
	var packed []byte
	switch codec {
	case codecZstd:
		enc, err := zstd.NewWriter(nil, zstd.WithWindowSize(window), zstd.WithEncoderConcurrency(1))
		if err != nil {
			t.Fatal(err)
		}
		packed = enc.EncodeAll(body, nil)
	default:
		var err error
		if packed, err = compress(codec, body); err != nil {
			t.Fatal(err)
		}
	}
	b := makeBatch(1, packed)
	binary.BigEndian.PutUint16(b[batchAttributesOff:], uint16(codec))
	sealCRCOf(b)
	return b
}

// A 200 MiB record inside a 30 KB batch, offered to a compacted topic. The old check
// inflated it whole (200 MiB held, then copied again by the record decoder) before it
// looked at a key; Kafka's own recorded answer for a batch that weighs little is that it
// is accepted, so the bound is on what this broker will hold, and holding is the
// streamed reader's: the memory is a window and a buffer, not the payload.
func TestRequireKeysDoesNotHoldWhatABatchInflatesTo(t *testing.T) {
	bomb := bombBatch(t, codecZstd, 64<<20, 200<<20)
	if len(bomb) > 64<<10 {
		t.Fatalf("the test's bomb is %d bytes", len(bomb))
	}
	var err error
	got := allocated(func() { err = RequireKeys(bomb) })
	if err == nil {
		t.Fatal("a batch that inflates to 200 MiB was accepted")
	}
	if got > 24<<20 {
		t.Errorf("refusing it allocated %d bytes; the payload it claims is 200 MiB", got)
	}
}

// What real Kafka accepts keeps working: a keyed batch that inflates well past the
// topic's max.message.bytes (recorded: 50 KB inflating from a few hundred bytes under a
// 2000 byte limit, and 20 MiB under the default), on a compacted topic, in every codec.
func TestRequireKeysAcceptsALegitimateCompressedBatch(t *testing.T) {
	for name, b := range map[string][]byte{
		"gzip":      bombBatch(t, codecGzip, 0, 8<<20),
		"zstd":      bombBatch(t, codecZstd, 2<<20, 8<<20),
		"zstd 8MiB": bombBatch(t, codecZstd, 8<<20, 8<<20),
	} {
		if err := RequireKeys(b); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	// And a null key is still refused, compressed or not.
	nb, err := rebuildBatch(makeBatch(1, nil), sampleRecords(), codecGzip)
	if err != nil {
		t.Fatal(err)
	}
	if err := RequireKeys(nb); err == nil {
		t.Error("a null key in a gzip batch was accepted")
	}
}

// A bomb that declares an ordinary window and simply inflates: the stream stops at the
// ceiling, with what was held being a buffer and the window, and says it is a size.
func TestCheckRecordsStopsAtTheCeilingForEveryCodecAndTheTopicLimit(t *testing.T) {
	zbomb := bombBatch(t, codecZstd, 2<<20, 100<<20)
	gbomb := bombBatch(t, codecGzip, 0, 100<<20)
	for name, b := range map[string][]byte{"zstd": zbomb, "gzip": gbomb} {
		var err error
		got := allocated(func() { err = CheckRecords(b, RecordCheck{RequireKeys: true}) })
		if !errors.Is(err, ErrInflatedTooLarge) {
			t.Errorf("%s: %v, want ErrInflatedTooLarge", name, err)
		}
		if got > 24<<20 {
			t.Errorf("%s: allocated %d bytes for a batch that inflates to 100 MiB", name, got)
		}
	}
	// With the topic's own limit it is the topic's size error, and it is checked on a
	// batch that has keys and nothing else to look for.
	small := bombBatch(t, codecGzip, 0, 50000)
	if err := CheckRecords(small, RecordCheck{MaxStored: 2000}); !errors.Is(err, ErrStoredTooLarge) {
		t.Errorf("50 KB inflated under a 2000 byte topic: %v, want ErrStoredTooLarge", err)
	}
	if err := CheckRecords(bombBatch(t, codecGzip, 0, 1000), RecordCheck{MaxStored: 2000}); err != nil {
		t.Errorf("1 KB inflated under a 2000 byte topic: %v", err)
	}
	// An uncompressed batch is its own stored size: the produce check's business (CheckProduce), not this.
	if err := CheckRecords(makeBatch(1, EncodeRecords(sampleRecords()[:1])), RecordCheck{MaxStored: 10}); err != nil {
		t.Errorf("an uncompressed batch: %v", err)
	}
}

// CheckRecords reads the batches of a Produce request: whatever it is given, no panic,
// and nothing the stream holds is the payload.
func FuzzCheckRecords(f *testing.F) {
	for _, codec := range []int{codecNone, codecGzip, codecSnappy, codecLZ4, codecZstd} {
		b, _ := rebuildBatch(makeBatch(1, nil), sampleRecords(), codec)
		f.Add(b, true, int64(0))
		f.Add(b, false, int64(2000))
	}
	// The bomb of the produce path: a 64 MiB window over a 16 MiB record, in a few KB.
	enc, _ := zstd.NewWriter(nil, zstd.WithWindowSize(64<<20), zstd.WithEncoderConcurrency(1))
	body := EncodeRecords([]Record{{Key: []byte("k"), Value: make([]byte, 16<<20)}})
	bomb := makeBatch(1, enc.EncodeAll(body, nil))
	binary.BigEndian.PutUint16(bomb[batchAttributesOff:], codecZstd)
	sealCRCOf(bomb)
	f.Add(bomb, true, int64(0))
	f.Fuzz(func(t *testing.T, b []byte, keys bool, maxStored int64) {
		if len(b) > 1<<20 {
			return
		}
		_ = CheckRecords(b, RecordCheck{RequireKeys: keys, MaxStored: maxStored})
	})
}
