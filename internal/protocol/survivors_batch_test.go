package protocol

import (
	"encoding/binary"
	"testing"
)

// rawBatch is the header of a record batch, length bytes long after its length field,
// claiming records records, padded or cut to total bytes.
func rawBatch(length int32, records int32, total int) []byte {
	b := make([]byte, max(total, 61))
	binary.BigEndian.PutUint32(b[8:], uint32(length))
	binary.BigEndian.PutUint32(b[57:], uint32(records))
	return b[:total]
}

func TestBatchCountsAtTheSmallestBatch(t *testing.T) {
	for _, c := range []struct {
		name         string
		in           []byte
		batches, rec int
	}{
		{"the smallest whole batch", rawBatch(49, 3, 61), 1, 3},
		{"a length one short of a header", rawBatch(48, 3, 61), 0, 0},
		{"a length one past what is there", rawBatch(50, 3, 61), 0, 0},
		{"one byte less than a header", rawBatch(49, 3, 60), 0, 0},
		{"two, the second exact", append(rawBatch(49, 2, 61), rawBatch(52, 5, 64)...), 2, 7},
		{"one then a cut one", append(rawBatch(49, 2, 61), rawBatch(60, 5, 64)...), 1, 2},
		{"nothing", nil, 0, 0},
	} {
		b, r := batchCounts(c.in)
		if b != c.batches || r != c.rec {
			t.Errorf("%s: %d batches %d records, want %d and %d", c.name, b, r, c.batches, c.rec)
		}
	}
}
