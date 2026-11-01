package broker_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"hash/crc32"
	"runtime"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/storage/log"
)

// zeroBatch is a v2 batch of one keyed record whose value is n zero bytes, compressed
// with codec (1 gzip, 4 zstd at the given declared window). It is the shape of a
// decompression bomb: tens of KB on the wire, n bytes once inflated.
func zeroBatch(t *testing.T, codec int, window, n int) []byte {
	t.Helper()
	body := log.EncodeRecords([]log.Record{{Key: []byte("k"), Value: make([]byte, n)}})
	var packed []byte
	switch codec {
	case 4:
		enc, err := zstd.NewWriter(nil, zstd.WithWindowSize(window), zstd.WithEncoderConcurrency(1))
		if err != nil {
			t.Fatal(err)
		}
		packed = enc.EncodeAll(body, nil)
	default:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		w.Write(body)
		w.Close()
		packed = buf.Bytes()
	}
	const header = 61
	b := make([]byte, header+len(packed))
	binary.BigEndian.PutUint32(b[8:], uint32(len(b)-12)) // batch length
	b[16] = 2                                            // magic
	binary.BigEndian.PutUint16(b[21:], uint16(codec))    // attributes
	binary.BigEndian.PutUint32(b[23:], 0)                // last offset delta
	binary.BigEndian.PutUint64(b[27:], 1000)             // first timestamp
	binary.BigEndian.PutUint64(b[35:], 1000)             // max timestamp
	binary.BigEndian.PutUint64(b[43:], ^uint64(0))       // producer id -1
	binary.BigEndian.PutUint16(b[51:], 0xffff)           // producer epoch -1
	binary.BigEndian.PutUint32(b[53:], 0xffffffff)       // base sequence -1
	binary.BigEndian.PutUint32(b[57:], 1)                // record count
	copy(b[header:], packed)
	binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], crc32.MakeTable(crc32.Castagnoli)))
	return b
}

func produceRaw(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, records []byte) int16 {
	t.Helper()
	req := kmsg.NewPtrProduceRequest()
	req.Acks, req.TimeoutMillis = -1, 10000
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Records = records
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	return resp.Topics[0].Partitions[0].ErrorCode
}

// What the produce path holds for a compressed batch is a window, not the payload, and
// what a topic is held to is what real Kafka holds it to (recorded:
// conformance/recordings/produce-decompressed-size.json): the size as it arrived, and for
// a topic that stores uncompressed, the size it inflates to, both MESSAGE_TOO_LARGE. A
// batch that inflates past what this broker will hold is refused the same way, without
// the broker having held it.
func TestAProduceBombIsRefusedWithoutInflatingIt(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	createCompacted(t, ctx, admin, "keyed", "compact", nil)
	createCompacted(t, ctx, admin, "plain", "delete", map[string]string{"compression.type": "uncompressed", "max.message.bytes": "2000", "segment.bytes": "1048576"})
	createCompacted(t, ctx, admin, "ordinary", "delete", map[string]string{"segment.bytes": "1048576"})
	cl := manualProducer(t, addr)

	// 200 MiB claimed by a few tens of KB, under a window of 64 MiB.
	bomb := zeroBatch(t, 4, 64<<20, 200<<20)
	if len(bomb) > 64<<10 {
		t.Fatalf("the bomb is %d bytes", len(bomb))
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	code := produceRaw(t, ctx, cl, "keyed", bomb)
	runtime.ReadMemStats(&after)
	if code != kerr.MessageTooLarge.Code {
		t.Errorf("a bomb to a compacted topic: error %d (%s), want MESSAGE_TOO_LARGE", code, kerr.ErrorForCode(code))
	}
	// The test's own bomb and the client are in this process; the broker alone used to
	// take 200 MiB (and again for the decoded records).
	if got := after.TotalAlloc - before.TotalAlloc; got > 48<<20 {
		t.Errorf("refusing the bomb allocated %d bytes in this process, want far under the 200 MiB it claimed", got)
	}

	// A batch Kafka accepts is accepted: 8 MiB inflated from a few KB.
	if code := produceRaw(t, ctx, cl, "keyed", zeroBatch(t, 1, 0, 8<<20)); code != 0 {
		t.Errorf("a keyed gzip batch inflating to 8 MiB on a compacted topic: error %d", code)
	}
	if code := produceRaw(t, ctx, cl, "keyed", zeroBatch(t, 4, 8<<20, 8<<20)); code != 0 {
		t.Errorf("a keyed zstd batch (8 MiB window) inflating to 8 MiB on a compacted topic: error %d", code)
	}

	// A topic that stores uncompressed is held to max.message.bytes as it would be stored
	// (Kafka: 10 for the 50 KB one, accepted at 1 KB).
	if code := produceRaw(t, ctx, cl, "plain", zeroBatch(t, 1, 0, 50000)); code != kerr.MessageTooLarge.Code {
		t.Errorf("50 KB inflated on an uncompressed topic with max.message.bytes 2000: error %d, want MESSAGE_TOO_LARGE", code)
	}
	if code := produceRaw(t, ctx, cl, "plain", zeroBatch(t, 1, 0, 1000)); code != 0 {
		t.Errorf("1 KB inflated on the same topic: error %d", code)
	}

	// Every other topic is held to the size as it arrived, and the bomb is below it: as on
	// Kafka (recorded: 20 MiB accepted on a default topic) it is stored, and a lookup by
	// time that has to read it is bounded the same way.
	if code := produceRaw(t, ctx, cl, "ordinary", zeroBatch(t, 4, 2<<20, 20<<20)); code != 0 {
		t.Errorf("20 MiB inflated on an ordinary topic: error %d", code)
	}
}
