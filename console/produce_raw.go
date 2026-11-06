package console

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The composer sends a raw Produce request, not a franz-go record. A client library
// batches, compresses and caps what it sends, and drops the ErrorMessage a broker
// answers with (TOP-10); the composer's job is to put exactly the bytes asked for in
// front of the broker and show exactly what it said. One record, one uncompressed
// batch, no producer id: the same bytes "Send again" and the composer both write.

const batchHeaderLen = 61 // base offset to record count, in a magic 2 batch.

var crc32c = crc32.MakeTable(crc32.Castagnoli)

// encodeBatch is one uncompressed, non-idempotent, non-transactional record batch of
// one record.
func encodeBatch(ts time.Time, key, value []byte, headers []MessageHeader) []byte {
	rec := kmsg.Record{TimestampDelta64: 0, OffsetDelta: 0, Key: key, Value: value}
	for _, h := range headers {
		rec.Headers = append(rec.Headers, kmsg.Header{Key: h.Key, Value: h.Value})
	}
	body := rec.AppendTo(nil)
	rec.Length = int32(len(body) - kbin.VarintLen(0)) // AppendTo wrote the Length it was given (0).
	body = rec.AppendTo(nil)

	b := make([]byte, batchHeaderLen, batchHeaderLen+len(body))
	ms := ts.UnixMilli()
	binary.BigEndian.PutUint64(b[0:], 0)                           // base offset: the broker sets it.
	binary.BigEndian.PutUint32(b[8:], uint32(len(b)-12+len(body))) // length of what follows this field.
	binary.BigEndian.PutUint32(b[12:], 0xFFFFFFFF)                 // partition leader epoch: -1.
	b[16] = 2                                                      // magic.
	binary.BigEndian.PutUint16(b[21:], 0)                          // attributes: no codec, CreateTime, not transactional.
	binary.BigEndian.PutUint32(b[23:], 0)                          // last offset delta.
	binary.BigEndian.PutUint64(b[27:], uint64(ms))                 // first timestamp.
	binary.BigEndian.PutUint64(b[35:], uint64(ms))                 // max timestamp.
	binary.BigEndian.PutUint64(b[43:], ^uint64(0))                 // producer id: -1.
	binary.BigEndian.PutUint16(b[51:], 0xFFFF)                     // producer epoch: -1.
	binary.BigEndian.PutUint32(b[53:], 0xFFFFFFFF)                 // base sequence: -1.
	binary.BigEndian.PutUint32(b[57:], 1)                          // record count.
	b = append(b, body...)
	binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], crc32c))
	return b
}

// gzipAbove is the batch size past which the console compresses what it sends. A topic's
// limit is held to the batch as it arrived, so a record that is stored large because it
// arrived compressed (Kafka accepts such a batch however far it inflates, recorded Q10a)
// can only be sent again compressed. Under it the bytes go as they are.
const gzipAbove = 512 << 10

// compressed returns batch as a gzip batch when it is large enough to need it and the
// codec makes it smaller; otherwise batch itself.
func compressed(batch []byte) []byte {
	if len(batch) <= gzipAbove {
		return batch
	}
	var z bytes.Buffer
	w := gzip.NewWriter(&z)
	w.Write(batch[batchHeaderLen:])
	w.Close()
	if z.Len() >= len(batch)-batchHeaderLen {
		return batch
	}
	out := append(append(make([]byte, 0, batchHeaderLen+z.Len()), batch[:batchHeaderLen]...), z.Bytes()...)
	binary.BigEndian.PutUint16(out[21:], 1) // attributes: gzip.
	binary.BigEndian.PutUint32(out[8:], uint32(len(out)-12))
	binary.BigEndian.PutUint32(out[17:], crc32.Checksum(out[21:], crc32c))
	return out
}

// batchSize is what one record weighs on the wire as a batch of one, before any codec:
// what "What will be sent" says.
func batchSize(key, value []byte, headers []MessageHeader) int {
	return len(encodeBatch(time.Time{}, key, value, headers))
}

// disconnects counts the connections the broker has closed on a client. With acks 0 a
// refused batch is answered by closing the connection and nothing else (recorded, Q9).
type disconnects struct{ n atomic.Int64 }

func (d *disconnects) OnBrokerDisconnect(kgo.BrokerMetadata, net.Conn) { d.n.Add(1) }

// ackZeroWait is how long the console looks for the outcome of an acks 0 send: the
// record arriving (the log end moves) or the connection being closed.
const ackZeroWait = 2 * time.Second

// Produce sends one record as a raw Produce request on the client for acks.
func (b *backend) Produce(ctx context.Context, topic string, partition int32, key, value []byte, headers []MessageHeader, acks string) (ProduceResult, error) {
	cl, wire, confirmed := b.produceLeader, int16(1), true
	switch acks {
	case "0":
		cl, wire, confirmed = b.produceNone, 0, false
	case "all":
		cl, wire = b.produceAll, -1
	}
	batch := compressed(encodeBatch(time.Now(), key, value, headers))

	req := kmsg.NewPtrProduceRequest()
	req.Acks, req.TimeoutMillis = wire, 30000
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition, rp.Records = partition, batch
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	if !confirmed {
		return b.produceUnconfirmed(ctx, cl, req, topic, partition, len(batch))
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return ProduceResult{}, err
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		return ProduceResult{}, fmt.Errorf("unexpected Produce response shape")
	}
	p := resp.Topics[0].Partitions[0]
	if e := kerr.ErrorForCode(p.ErrorCode); e != nil {
		ke := e.(*kerr.Error)
		facts := ""
		if ke.Code == kerr.MessageTooLarge.Code {
			facts = b.refusalFacts(ctx, topic, len(batch))
		}
		return ProduceResult{}, &kafkaRefusal{Err: ke, Msg: derefString(p.ErrorMessage), Facts: facts}
	}
	return ProduceResult{Partition: partition, Offset: p.BaseOffset, Confirmed: true}, nil
}

// produceUnconfirmed sends with acks 0 and then looks for what became of it. The broker
// sends no answer either way, so the outcome is read from the log end (it moved: the
// record is there) and from the connection (the broker closed it: refused).
func (b *backend) produceUnconfirmed(ctx context.Context, cl *kgo.Client, req *kmsg.ProduceRequest, topic string, partition int32, batch int) (ProduceResult, error) {
	_, before, _ := b.PartitionOffsets(ctx, topic, partition)
	closedBefore := b.closed.n.Load()
	if _, err := cl.Request(ctx, req); err != nil {
		return ProduceResult{}, err
	}
	deadline := time.Now().Add(ackZeroWait)
	for time.Now().Before(deadline) {
		if _, after, err := b.PartitionOffsets(ctx, topic, partition); err == nil && after > before {
			return ProduceResult{Partition: partition, Offset: -1, Confirmed: false}, nil
		}
		if b.closed.n.Load() > closedBefore {
			return ProduceResult{}, &notDelivered{Facts: b.refusalFacts(ctx, topic, batch)}
		}
		select {
		case <-ctx.Done():
			return ProduceResult{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return ProduceResult{Partition: partition, Offset: -1, Confirmed: false}, nil
}

// notDelivered is the answer to an acks 0 send the broker closed the connection on.
type notDelivered struct{ Facts string }

func (e *notDelivered) Error() string { return "not delivered: the broker closed the connection" }

// refusalFacts says what the console itself knows about a send, for a refusal to
// carry beside the broker's words: the size of the batch against the topic's limit.
// It is the console's fact, kept apart from the broker's sentence.
func (b *backend) refusalFacts(ctx context.Context, topic string, batch int) string {
	s := fmt.Sprintf("The batch the console sent is %s B", groupDigits(int64(batch)))
	req := kmsg.NewPtrDescribeConfigsRequest()
	r := kmsg.NewDescribeConfigsRequestResource()
	r.ResourceType, r.ResourceName, r.ConfigNames = kmsg.ConfigResourceTypeTopic, topic, []string{"max.message.bytes"}
	req.Resources = append(req.Resources, r)
	if resp, err := req.RequestWith(ctx, b.cl); err == nil && len(resp.Resources) == 1 && resp.Resources[0].ErrorCode == 0 {
		for _, c := range resp.Resources[0].Configs {
			if n, err := strconv.ParseInt(derefString(c.Value), 10, 64); err == nil && c.Name == "max.message.bytes" {
				s += " and this topic's max.message.bytes is " + groupDigits(n)
			}
		}
	}
	return s + "."
}
