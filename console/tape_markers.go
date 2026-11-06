package console

import (
	"context"
	"encoding/binary"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// offsetSpan is the offsets [First, Last] a batch covers.
type offsetSpan struct{ First, Last int64 }

// markersIn is the offsets of [from, to) that hold no record and lie in a control batch.
func markersIn(from, to int64, have map[int64]bool, controls []offsetSpan) map[int64]bool {
	out := map[int64]bool{}
	for _, c := range controls {
		for off := max(c.First, from); off <= c.Last && off < to; off++ {
			if !have[off] {
				out[off] = true
			}
		}
	}
	return out
}

// controlSpans reads the control batches (transaction commit and abort markers) out of
// a Fetch response's record bytes: the batch header carries the baseOffset (0), the
// attributes (21) whose bit 5 marks a control batch, and the lastOffsetDelta (23).
func controlSpans(raw []byte) []offsetSpan {
	var out []offsetSpan
	for len(raw) >= 61 {
		length := int(binary.BigEndian.Uint32(raw[8:12]))
		if length < 49 || 12+length > len(raw) {
			break // a batch cut off at the end of the response.
		}
		if binary.BigEndian.Uint16(raw[21:23])&0x20 != 0 {
			base := int64(binary.BigEndian.Uint64(raw[0:8]))
			out = append(out, offsetSpan{First: base, Last: base + int64(int32(binary.BigEndian.Uint32(raw[23:27])))})
		}
		raw = raw[12+length:]
	}
	return out
}

// ControlSpans reads the window [from, to) of a partition as stored (uncommitted) and
// returns the control batches in it.
func (b *backend) ControlSpans(ctx context.Context, topic string, partition int32, from, to int64) ([]offsetSpan, error) {
	req := kmsg.NewPtrFetchRequest()
	req.MaxBytes, req.MinBytes, req.MaxWaitMillis, req.IsolationLevel = 4<<20, 0, 0, 0
	mreq := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = &topic
	mreq.Topics = []kmsg.MetadataRequestTopic{mt}
	mresp, err := mreq.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if len(mresp.Topics) != 1 || mresp.Topics[0].ErrorCode != 0 {
		return nil, errNotFound
	}
	rt := kmsg.NewFetchRequestTopic()
	rt.Topic, rt.TopicID = topic, mresp.Topics[0].TopicID
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes = partition, from, 4<<20
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = []kmsg.FetchRequestTopic{rt}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	var out []offsetSpan
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if e := kerr.ErrorForCode(p.ErrorCode); e != nil {
				return nil, e
			}
			out = append(out, controlSpans(p.RecordBatches)...)
		}
	}
	return out, nil
}
