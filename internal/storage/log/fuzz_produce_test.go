package log

import (
	"math"
	"testing"
)

// CheckProduce reads every Produce payload a client sends before anything is stored:
// the batch's shape, then every record through its codec (scanRecords, and for gzip
// the member reader). Whatever the bytes, it must not panic, and a batch it accepts
// is one whole version 2 batch that ParseBatch reads.
func FuzzCheckProduce(f *testing.F) {
	recs := []Record{rec(0, "k0", "a value"), rec(1, "k1", "another value"), rec(2, "", "no key")}
	for _, codec := range []int{codecNone, codecGzip, codecSnappy, codecLZ4, codecZstd} {
		body := EncodeRecords(recs)
		if codec != codecNone {
			packed, err := compress(codec, body)
			if err != nil {
				f.Fatal(err)
			}
			body = packed
		}
		b := withBody(codec, int32(len(recs)), body)
		f.Add(b, int16(9), false, int8(TargetProducer))
		f.Add(b, int16(3), true, int8(codecNone))
		f.Add(b[:len(b)-3], int16(9), false, int8(codecGzip))
		f.Add(append(append([]byte(nil), b...), b...), int16(9), false, int8(TargetProducer))
	}
	f.Add([]byte{}, int16(9), false, int8(TargetProducer))
	f.Fuzz(func(t *testing.T, payload []byte, version int16, keys bool, target int8) {
		if len(payload) > 1<<20 {
			return
		}
		rules := ProduceRules{
			Version: version, MaxBatchBytes: DefaultMaxMessageBytes, RequireKeys: keys,
			Target: int(target) % 5, NowMs: 1_700_000_000_000, BeforeMaxMs: math.MaxInt64,
			AfterMaxMs: DefaultTimestampAfterMaxMs, TopicPartition: "t-0",
		}
		if target < 0 {
			rules.Target = TargetProducer
		}
		acc, refusal := CheckProduce(payload, rules)
		if refusal != nil {
			return
		}
		if _, ok := ParseBatch(acc.Batch); !ok {
			t.Fatalf("accepted a batch ParseBatch refuses (%d bytes)", len(acc.Batch))
		}
	})
}
