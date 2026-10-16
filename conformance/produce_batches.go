package conformance

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// What the produce scenarios send is built here byte for byte: a version-2 record
// batch with any field set to anything, its records section written by hand where a
// case needs it wrong, and compressed with any of the four codecs.

// The codecs of a batch's attributes.
const (
	codecGzip   = 1
	codecSnappy = 2
	codecLZ4    = 3
	codecZstd   = 4
)

var codecNames = map[int]string{codecGzip: "gzip", codecSnappy: "snappy", codecLZ4: "lz4", codecZstd: "zstd"}

// rawBatch is one record batch, field by field. newBatch fills in what an honest
// producer writes; a case then changes what it is about.
type rawBatch struct {
	Codec     int    // attributes bits 0 to 2.
	Attrs     uint16 // the other attribute bits, as they go on the wire.
	Body      []byte // the records section, before compression.
	Wire      []byte // when set, what follows the header as it is (Body and Codec's compressor are not used).
	Count     int32
	LastDelta int32
	Base      int64
	Magic     byte
	PID       int64
	Epoch     int16
	Seq       int32
	First     int64 // the timestamp the records' deltas are relative to.
	Max       int64
	LengthAdd int32 // added to the batch's length field.
	BadCRC    bool
}

// newBatch is a batch of n records whose records section is body, as a producer with
// no producer id writes it, at timestamp 1000.
func newBatch(n int, body []byte) rawBatch {
	return rawBatch{Body: body, Count: int32(n), LastDelta: int32(n - 1), Magic: 2, PID: -1, Epoch: -1, Seq: -1, First: 1000, Max: 1000}
}

// oneRecord is the records section of the batch every case starts from: key "k",
// value "v", no headers.
func oneRecord() []byte { return aRecord(0, 0, sp("k"), sp("v")) }

func sp(s string) *string { return &s }

// aRecord is one record: timestamp and offset deltas, a key and a value (nil is null).
func aRecord(tsDelta, offsetDelta int64, key, value *string) []byte {
	var r []byte
	r = append(r, 0) // attributes
	r = binary.AppendVarint(r, tsDelta)
	r = binary.AppendVarint(r, offsetDelta)
	for _, f := range []*string{key, value} {
		if f == nil {
			r = binary.AppendVarint(r, -1)
			continue
		}
		r = binary.AppendVarint(r, int64(len(*f)))
		r = append(r, *f...)
	}
	r = binary.AppendVarint(r, 0) // headers
	return append(binary.AppendVarint(nil, int64(len(r))), r...)
}

// nRecords is n records with keys k0, k1, ... and the value given.
func nRecords(n int, value string) []byte {
	var out []byte
	for i := range n {
		out = append(out, aRecord(0, int64(i), sp(fmt.Sprintf("k%d", i)), &value)...)
	}
	return out
}

// compress packs a records section as a producer's client would.
func compress(codec int, body []byte) []byte {
	var b bytes.Buffer
	switch codec {
	case 0:
		return body
	case codecGzip:
		w := gzip.NewWriter(&b)
		w.Write(body)
		w.Close()
	case codecSnappy:
		// The Java client's framing (snappy-java): a 16 byte header, then blocks.
		b.Write([]byte{0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0, 0, 0, 0, 1, 0, 0, 0, 1})
		for len(body) > 0 {
			n := min(len(body), 32<<10)
			block := snappy.Encode(nil, body[:n])
			b.Write(binary.BigEndian.AppendUint32(nil, uint32(len(block))))
			b.Write(block)
			body = body[n:]
		}
	case codecLZ4:
		w := lz4.NewWriter(&b)
		w.Write(body)
		w.Close()
	case codecZstd:
		w, _ := zstd.NewWriter(&b)
		w.Write(body)
		w.Close()
	default:
		panic(fmt.Sprintf("no compressor for codec %d", codec))
	}
	return b.Bytes()
}

// zstdFrame is a zstd frame written by hand: one raw (stored) block holding content,
// in a frame that declares a window of 2^windowLog bytes. No real compressor declares
// more than its level needs; this is how a batch says anything it likes.
func zstdFrame(windowLog int, content []byte) []byte {
	out := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, byte(windowLog-10) << 3}
	head := uint32(len(content))<<3 | 1 // a raw block, and the last.
	out = append(out, byte(head), byte(head>>8), byte(head>>16))
	return append(out, content...)
}

func (b rawBatch) bytes() []byte {
	body := b.Wire
	if body == nil {
		body = compress(b.Codec, b.Body)
	}
	var tail []byte // everything the CRC covers: attributes onward.
	tail = binary.BigEndian.AppendUint16(tail, uint16(b.Codec)|b.Attrs)
	tail = binary.BigEndian.AppendUint32(tail, uint32(b.LastDelta))
	tail = binary.BigEndian.AppendUint64(tail, uint64(b.First))
	tail = binary.BigEndian.AppendUint64(tail, uint64(b.Max))
	tail = binary.BigEndian.AppendUint64(tail, uint64(b.PID))
	tail = binary.BigEndian.AppendUint16(tail, uint16(b.Epoch))
	tail = binary.BigEndian.AppendUint32(tail, uint32(b.Seq))
	tail = binary.BigEndian.AppendUint32(tail, uint32(b.Count))
	tail = append(tail, body...)

	var out []byte
	out = binary.BigEndian.AppendUint64(out, uint64(b.Base))
	out = binary.BigEndian.AppendUint32(out, uint32(int32(4+1+4+len(tail))+b.LengthAdd))
	out = binary.BigEndian.AppendUint32(out, 0xffffffff) // partition leader epoch -1
	out = append(out, b.Magic)
	crc := crc32.Checksum(tail, crc32.MakeTable(crc32.Castagnoli))
	if b.BadCRC {
		crc ^= 0xdeadbeef
	}
	out = binary.BigEndian.AppendUint32(out, crc)
	return append(out, tail...)
}

// noiseBytes is n bytes no codec can shrink, the same every run.
func noiseBytes(n int) []byte {
	b := make([]byte, n)
	x := uint32(2463534242)
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}

// produceRaw sends records to partition 0 of topic at version, acks=all, and records
// the answer under label. A topic only just created has no leader for a moment
// (NOT_LEADER_OR_FOLLOWER); the request is sent again until the answer is about the
// batch. txnID, when not empty, is the request's transactional id.
func (e *Env) produceRaw(label, topic string, version int16, acks int16, txnID string, recs []byte) *kmsg.ProduceResponse {
	r := kmsg.NewPtrProduceRequest()
	r.Acks, r.TimeoutMillis = acks, 5000
	if txnID != "" {
		r.TransactionID = &txnID
	}
	t := kmsg.NewProduceRequestTopic()
	t.Topic = topic
	p := kmsg.NewProduceRequestTopicPartition()
	p.Records = recs
	t.Partitions = append(t.Partitions, p)
	r.Topics = append(r.Topics, t)
	resp := e.DoUntil(label, r, version, func(resp kmsg.Response) bool {
		for _, t := range resp.(*kmsg.ProduceResponse).Topics {
			for _, p := range t.Partitions {
				if p.ErrorCode == 6 {
					return false
				}
			}
		}
		return true
	})
	e.steadyProduceAnswer()
	pr, _ := resp.(*kmsg.ProduceResponse)
	return pr
}

// steadyProduceAnswer replaces, in the step just recorded, the two things a Produce
// answer holds that are the wall clock's: the time a LogAppendTime topic stamped the
// batch with, and the bounds in the text of a timestamp refusal ("should be within
// [now - before, now + after]").
func (e *Env) steadyProduceAnswer() {
	if len(e.steps) == 0 {
		return
	}
	resp, _ := e.steps[len(e.steps)-1].Response.(map[string]any)
	topics, _ := resp["Topics"].([]any)
	for _, t := range topics {
		tm, _ := t.(map[string]any)
		parts, _ := tm["Partitions"].([]any)
		for _, p := range parts {
			pm, _ := p.(map[string]any)
			// A year of milliseconds or more is a clock; less is the timestamp a
			// script put in a batch (a duplicate is answered with its batch's).
			if n, ok := pm["LogAppendTime"].(json.Number); ok {
				if v, err := n.Int64(); err == nil && v > 31_536_000_000 {
					pm["LogAppendTime"] = "<broker-time>"
				}
			}
			recs, _ := pm["ErrorRecords"].([]any)
			for _, r := range recs {
				rm, _ := r.(map[string]any)
				if msg, ok := rm["ErrorMessage"].(string); ok {
					if i := strings.Index(msg, "The timestamp should be within"); i >= 0 {
						rm["ErrorMessage"] = msg[:i] + "The timestamp should be within <bounds>"
					}
				}
			}
		}
	}
}

// readRecord is one record as a consumer reads it.
type readRecord struct {
	tsDelta, offsetDelta int64
	key, value           *string
}

// readBatch is what a consumer's client does with a fetched batch: inflate the
// records section and take the records apart, each by its own length. It is the
// harness's own reader on purpose (it does not ask the broker under test how to
// read), and it has no limits a client would not have.
func readBatch(b []byte) ([]readRecord, error) {
	codec := int(binary.BigEndian.Uint16(b[21:]) & 7)
	count := int(int32(binary.BigEndian.Uint32(b[57:])))
	body := b[61:]
	switch codec {
	case 0:
	case codecGzip:
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		if body, err = io.ReadAll(zr); err != nil {
			return nil, err
		}
	case codecSnappy:
		if bytes.HasPrefix(body, []byte{0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0}) && len(body) >= 16 {
			var out []byte
			for rest := body[16:]; len(rest) > 0; {
				if len(rest) < 4 || len(rest)-4 < int(binary.BigEndian.Uint32(rest)) {
					return nil, errors.New("a snappy block is cut short")
				}
				n := int(binary.BigEndian.Uint32(rest))
				block, err := snappy.Decode(nil, rest[4:4+n])
				if err != nil {
					return nil, err
				}
				out, rest = append(out, block...), rest[4+n:]
			}
			body = out
		} else {
			out, err := snappy.Decode(nil, body)
			if err != nil {
				return nil, err
			}
			body = out
		}
	case codecLZ4:
		out, err := io.ReadAll(lz4.NewReader(bytes.NewReader(body)))
		if err != nil {
			return nil, err
		}
		body = out
	case codecZstd:
		d, err := zstd.NewReader(bytes.NewReader(body), zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(1<<31))
		if err != nil {
			return nil, err
		}
		defer d.Close()
		if body, err = io.ReadAll(d); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("codec %d", codec)
	}
	varint := func(p *[]byte) (int64, error) {
		v, n := binary.Varint(*p)
		if n <= 0 {
			return 0, errors.New("a number is cut short")
		}
		*p = (*p)[n:]
		return v, nil
	}
	field := func(p *[]byte) (*string, error) {
		n, err := varint(p)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		if n > int64(len(*p)) {
			return nil, errors.New("a field runs past its record")
		}
		s := string((*p)[:n])
		*p = (*p)[n:]
		return &s, nil
	}
	var out []readRecord
	for i := 0; i < count; i++ {
		size, err := varint(&body)
		if err != nil {
			return out, fmt.Errorf("record %d: %w", i, err)
		}
		if size < 1 || size > int64(len(body)) {
			return out, fmt.Errorf("record %d says it is %d bytes and %d are left", i, size, len(body))
		}
		r := body[1:size] // after the attributes byte.
		body = body[size:]
		var rr readRecord
		if rr.tsDelta, err = varint(&r); err == nil {
			rr.offsetDelta, err = varint(&r)
		}
		if err == nil {
			rr.key, err = field(&r)
		}
		if err == nil {
			rr.value, err = field(&r)
		}
		if err != nil {
			return out, fmt.Errorf("record %d: %w", i, err)
		}
		out = append(out, rr)
	}
	if len(body) != 0 {
		return out, fmt.Errorf("%d bytes after the %d records the batch says it has", len(body), count)
	}
	return out, nil
}

// storedRecords reads everything in partition 0 of topic and records, as a step of
// its own, what a consumer would get: each batch's base offset and each record's
// offset, timestamp, key and value. It is what "the batch was stored" and "the batch
// was not stored" mean, and it is the same whether the broker kept the producer's
// bytes or rewrote them. brokerTime says the topic stamps records with its own
// clock, which is then not compared.
func (e *Env) storedRecords(label, topic string, brokerTime bool) {
	e.fail2(label)
	type rec struct {
		Offset    int64
		Timestamp any
		Key       *string
		Value     *string
	}
	type batch struct {
		Base    int64
		Records []rec  `json:",omitempty"`
		Error   string `json:",omitempty"`
	}
	var batches []batch
	next := int64(0)
	for attempt := 0; attempt < 200; attempt++ {
		r := kmsg.NewPtrFetchRequest()
		r.ReplicaID, r.MaxWaitMillis, r.MinBytes, r.MaxBytes, r.SessionEpoch = -1, 200, 1, 64<<20, -1
		t := kmsg.NewFetchRequestTopic()
		t.Topic = topic
		p := kmsg.NewFetchRequestTopicPartition()
		p.FetchOffset, p.PartitionMaxBytes, p.CurrentLeaderEpoch, p.LogStartOffset = next, 64<<20, -1, -1
		t.Partitions = append(t.Partitions, p)
		r.Topics = append(r.Topics, t)
		resp, err := e.w.roundTrip(r, 11)
		if err != nil {
			e.fail("%s: fetch: %v", label, err)
			return
		}
		fr := resp.(*kmsg.FetchResponse)
		if len(fr.Topics) != 1 || len(fr.Topics[0].Partitions) != 1 {
			e.fail("%s: fetch answered %d topics", label, len(fr.Topics))
			return
		}
		fp := fr.Topics[0].Partitions[0]
		if fp.ErrorCode != 0 {
			e.fail("%s: fetch error %d", label, fp.ErrorCode)
			return
		}
		raw := fp.RecordBatches
		for len(raw) >= 61 {
			size := 12 + int(int32(binary.BigEndian.Uint32(raw[8:])))
			if size < 61 || size > len(raw) {
				break // the last batch of a fetch may be cut short.
			}
			b := raw[:size]
			raw = raw[size:]
			base := int64(binary.BigEndian.Uint64(b[0:]))
			last := base + int64(int32(binary.BigEndian.Uint32(b[23:])))
			if last < next {
				continue
			}
			next = last + 1
			attrs := binary.BigEndian.Uint16(b[21:])
			if attrs&0x20 != 0 {
				continue // a control batch.
			}
			first, max := int64(binary.BigEndian.Uint64(b[27:])), int64(binary.BigEndian.Uint64(b[35:]))
			out := batch{Base: base}
			decoded, err := readBatch(b)
			if err != nil {
				out.Error = "the batch cannot be read: " + err.Error()
			}
			for _, d := range decoded {
				r := rec{Offset: base + d.offsetDelta, Timestamp: first + d.tsDelta, Key: d.key}
				if attrs&0x08 != 0 {
					r.Timestamp = max
				}
				if brokerTime {
					r.Timestamp = "<broker-time>"
				}
				if d.value != nil {
					v := *d.value
					if len(v) > 40 {
						v = fmt.Sprintf("%d bytes", len(v))
					}
					r.Value = &v
				}
				out.Records = append(out.Records, r)
			}
			batches = append(batches, out)
		}
		if next >= fp.HighWatermark {
			break
		}
	}
	raw, _ := json.Marshal(map[string]any{"Batches": batches, "End": next})
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var tree any
	dec.Decode(&tree)
	e.steps = append(e.steps, Step{Label: label, API: "(stored records)", Request: map[string]any{"Topic": topic}, Response: tree})
}

// crc32IEEE is the checksum of the message formats before version 2.
func crc32IEEE(b []byte) uint32 { return crc32.ChecksumIEEE(b) }
