package log

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func testRules() ProduceRules {
	return ProduceRules{
		Version: 9, MaxBatchBytes: DefaultMaxMessageBytes, Target: TargetProducer,
		NowMs: time.Now().UnixMilli(), BeforeMaxMs: math.MaxInt64, AfterMaxMs: DefaultTimestampAfterMaxMs,
		TopicPartition: "t-0",
	}
}

// withBody is a batch of count records whose records section is body, sealed, in codec.
func withBody(codec int, count int32, body []byte) []byte {
	b := makeBatch(count, body)
	binary.BigEndian.PutUint16(b[batchAttributesOff:], uint16(codec))
	sealCRCOf(b)
	return b
}

var produceCodecs = map[string]int{"gzip": codecGzip, "snappy": codecSnappy, "lz4": codecLZ4, "zstd": codecZstd}

// On a topic that keeps the producer's codec (the default), a batch is read through
// its codec before it is stored, as Kafka reads it (recorded: produce-validation): a
// section that is not that codec, or is cut short, is UNKNOWN_SERVER_ERROR; a header
// that claims more or fewer records than there are is INVALID_RECORD. It used to be
// stored, and every reader that looked inside it failed later.
func TestCheckProduceReadsEveryCodec(t *testing.T) {
	for name, codec := range produceCodecs {
		recs := []Record{rec(0, "k0", "a value"), rec(1, "k1", "a value"), rec(2, "k2", "a value")}
		packed, err := compress(codec, EncodeRecords(recs))
		if err != nil {
			t.Fatal(err)
		}
		if _, r := CheckProduce(withBody(codec, 3, packed), testRules()); r != nil {
			t.Errorf("%s: a valid batch was refused: %v", name, r)
		}
		for what, tc := range map[string]struct {
			count int32
			body  []byte
			want  RefusalKind
		}{
			"garbage":       {1, []byte("this is not a compressed records section at all"), RefuseUnreadable},
			"cut in half":   {3, packed[:len(packed)/2], RefuseUnreadable},
			"claims 5 of 3": {5, packed, RefuseInvalidRecord},
			"claims 1 of 3": {1, packed, RefuseInvalidRecord},
		} {
			_, r := CheckProduce(withBody(codec, tc.count, tc.body), testRules())
			if r == nil {
				t.Errorf("%s, %s: accepted", name, what)
				continue
			}
			if r.Kind != tc.want {
				t.Errorf("%s, %s: refusal %d (%v), want %d", name, what, r.Kind, r, tc.want)
			}
		}
	}
}

// A batch that inflates past MaxInflatedBytes is refused in every codec, by a stream
// that never holds the payload: nothing this broker acknowledges can be one its own
// readers (DecodeRecords holds a batch's records) then fail on.
func TestCheckProduceBoundsInflationInEveryCodec(t *testing.T) {
	for name, codec := range produceCodecs {
		window := 0
		if codec == codecZstd {
			window = 4 << 20
		}
		bomb := bombBatch(t, codec, window, MaxInflatedBytes+(1<<20))
		rules := testRules()
		rules.MaxBatchBytes = math.MaxInt32 // the size as it arrives is not what is tested
		var r *Refusal
		got := allocated(func() { _, r = CheckProduce(bomb, rules) })
		if r == nil || r.Kind != RefuseTooLarge {
			t.Errorf("%s: a batch inflating past %d bytes: %v, want MESSAGE_TOO_LARGE", name, MaxInflatedBytes, r)
		}
		if got > 24<<20 {
			t.Errorf("%s: refusing it allocated %d bytes", name, got)
		}
		under := bombBatch(t, codec, window, 8<<20)
		if _, r := CheckProduce(under, rules); r != nil {
			t.Errorf("%s: a batch inflating to 8 MiB: %v", name, r)
		}
	}
}

// A partition that already holds a batch nothing can read (stored by a version that
// did not read records before storing them) still opens after a restart, serves the
// batches around it, answers timestamp lookups, and is compacted around it.
func TestUnreadableBatchFromAnEarlierVersionSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	p := smallPartition(t, dir)
	bad := withBody(codecGzip, 1, []byte("this is not a compressed records section at all"))
	if _, r := CheckProduce(bad, testRules()); r == nil {
		t.Fatal("the batch the test stores as an older version would have is accepted by produce now")
	}
	mustAppend(t, p, bad) // first, so that every lookup has to get past it.
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "1"}))
	sealSome(t, p, 4)
	mustAppend(t, p, kvBatch(codecNone, kv{"a", "2"}))
	end := p.LogEndOffset()
	p.close()

	p = smallPartition(t, dir)
	defer p.close()
	if got := p.LogEndOffset(); got != end {
		t.Fatalf("after a restart the log ends at %d, want %d", got, end)
	}
	if _, _, found, err := p.OffsetForTimestamp(0); err != nil || !found {
		t.Errorf("a timestamp lookup across the unreadable batch: found %v, %v", found, err)
	}
	if _, _, _, err := p.OffsetOfMaxTimestamp(); err != nil {
		t.Errorf("the max-timestamp lookup: %v", err)
	}
	res := compact(t, p, eager)
	if res.Unreadable == 0 {
		t.Errorf("compaction did not report the batch it could not read: %+v", res)
	}
	data, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "this is not a compressed records section") {
		t.Error("the unreadable batch was dropped by compaction; it is kept as it is")
	}
}

// What reading every record on the produce path costs, per codec, against what the
// produce path did before it (the header and checksum only, ValidateBatches): a batch
// of 100 records of 100 bytes, as a producer with linger sends one.
func BenchmarkCheckProduce(b *testing.B) {
	recs := make([]Record, 100)
	for i := range recs {
		recs[i] = rec(i, fmt.Sprintf("key-%03d", i), strings.Repeat(fmt.Sprintf("value %d ", i), 14)[:100])
	}
	body := EncodeRecords(recs)
	for _, name := range []string{"plain", "gzip", "snappy", "lz4", "zstd"} {
		codec := codecNone
		if name != "plain" {
			codec = produceCodecs[name]
		}
		packed, err := compress(codec, body)
		if err != nil {
			b.Fatal(err)
		}
		batch := withBody(codec, int32(len(recs)), packed)
		rules := testRules()
		b.Run(name+"/before", func(b *testing.B) {
			for range b.N {
				if err := ValidateBatches(batch); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(name+"/after", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, r := CheckProduce(batch, rules); r != nil {
					b.Fatal(r)
				}
			}
		})
	}
}
