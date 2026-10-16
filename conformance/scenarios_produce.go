package conformance

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/golang/snappy"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// What a real Apache Kafka does with a record batch that is not what an honest
// producer writes: which it stores, which it refuses and with which code, on each kind
// of topic. The kinds matter because Kafka looks inside a batch differently on each:
//
//	pe     no topic configuration (compression.type=producer, delete, CreateTime)
//	pec    cleanup.policy=compact
//	peu    compression.type=uncompressed
//	pez    compression.type=zstd
//	pelat  message.timestamp.type=LogAppendTime
//
// Each topic's script ends by reading the partition back, so "accepted" is shown to
// mean these records at these offsets, and "refused" that nothing was kept.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "produce-validation", Run: produceValidation, Known: knownProduceValidation()},
		Scenario{Name: "produce-inflated-size", Run: produceInflatedSize, Known: knownProduceInflated()},
	)
}

func produceValidation(e *Env) {
	e.Do("create", createTopics(false,
		newTopic("pe", 1, 1),
		newTopic("pec", 1, 1, "cleanup.policy", "compact"),
		newTopic("peu", 1, 1, "compression.type", "uncompressed"),
		newTopic("pez", 1, 1, "compression.type", "zstd"),
		newTopic("pelat", 1, 1, "message.timestamp.type", "LogAppendTime"),
		newTopic("pebefore", 1, 1, "message.timestamp.before.max.ms", "1000"),
		newTopic("peafter", 1, 1, "message.timestamp.after.max.ms", "9223372036854775807"),
	), 7)
	one := oneRecord()
	send := func(label, topic string, b rawBatch) { e.produceRaw(label, topic, 9, -1, "", b.bytes()) }
	sendBytes := func(label, topic string, version int16, recs []byte) {
		e.produceRaw(label, topic, version, -1, "", recs)
	}

	// Every version this broker offers: a good batch, one whose checksum is wrong, and
	// one whose records section says gzip and is not.
	for v := int16(3); v <= 9; v++ {
		sendBytes(fmt.Sprintf("v%d-ok", v), "pe", v, newBatch(1, one).bytes())
		bad := newBatch(1, one)
		bad.BadCRC = true
		sendBytes(fmt.Sprintf("v%d-bad-crc", v), "pe", v, bad.bytes())
		garbage := newBatch(1, nil)
		garbage.Codec, garbage.Wire = codecGzip, []byte("this is not a compressed records section at all")
		sendBytes(fmt.Sprintf("v%d-garbage-gzip", v), "pe", v, garbage.bytes())
	}
	// zstd came with Produce v7; a codec number nothing has; two batches where one is allowed.
	for _, v := range []int16{3, 6, 7} {
		z := newBatch(1, one)
		z.Codec = codecZstd
		sendBytes(fmt.Sprintf("v%d-zstd", v), "pe", v, z.bytes())
	}
	unknown := newBatch(1, nil)
	unknown.Codec, unknown.Wire = 5, one
	sendBytes("v3-unknown-codec-5", "pe", 3, unknown.bytes())
	sendBytes("v3-two-batches", "pe", 3, append(newBatch(1, one).bytes(), newBatch(1, one).bytes()...))

	for _, topic := range []string{"pe", "pec", "peu", "pez"} {
		produceMatrix(e, topic)
	}

	// --- one batch each, on the plain topic ---
	two := append(newBatch(1, one).bytes(), newBatch(1, one).bytes()...)
	sendBytes("two-batches-in-one-partition", "pe", 9, two)

	// The formats before version 2: a magic 1 message set, and a magic byte of 0, 1
	// or 3 on what is otherwise a version 2 batch.
	legacy := legacyMessage()
	sendBytes("legacy-magic-1-v3", "pe", 3, legacy)
	sendBytes("legacy-magic-1-v9", "pe", 9, legacy)
	for _, magic := range []byte{0, 1, 3} {
		b := newBatch(1, one)
		b.Magic = magic
		send(fmt.Sprintf("magic-%d-in-v2-layout", magic), "pe", b)
	}

	// Attribute bits.
	bit := func(label, topic string, attrs uint16) {
		b := newBatch(1, one)
		b.Attrs = attrs
		send(label, topic, b)
	}
	bit("control-bit-from-a-client", "pe", 0x20)
	bit("transactional-bit-no-producer-id", "pe", 0x10)
	txn := newBatch(1, one)
	txn.Attrs = 0x10
	e.produceRaw("transactional-bit-and-a-transactional-id-no-producer-id", "pe", 9, -1, "txn-nobody-began", txn.bytes())
	bit("timestamp-type-bit-set-by-client", "pe", 0x08)
	bit("timestamp-type-bit-set-by-client-logappend-topic", "pelat", 0x08)
	bit("delete-horizon-bit", "pe", 0x40)
	bit("unused-attribute-bit", "pe", 0x80)

	// The header's own numbers.
	for _, base := range []int64{1000, -5} {
		b := newBatch(1, one)
		b.Base = base
		send(fmt.Sprintf("base-offset-%d", base), "pe", b)
	}
	longer := newBatch(1, one)
	longer.LengthAdd = 10
	send("batch-length-10-more-than-sent", "pe", longer)
	shorter := newBatch(1, one)
	shorter.LengthAdd = -3
	send("batch-length-3-less-than-sent", "pe", shorter)

	// Bytes after the batch: too few to be the start of another, enough to be one
	// that says it is tiny, and enough to be one that says it is longer than what is
	// there.
	good := newBatch(1, one).bytes()
	sendBytes("batch-then-5-bytes", "pe", 9, append(bytes.Clone(good), 1, 2, 3, 4, 5))
	sendBytes("batch-then-20-zero-bytes", "pe", 9, append(bytes.Clone(good), make([]byte, 20)...))
	claimsMore := make([]byte, 30)
	binary.BigEndian.PutUint32(claimsMore[8:], 5000)
	claimsMore[16] = 2
	sendBytes("batch-then-30-bytes-claiming-5000", "pe", 9, append(bytes.Clone(good), claimsMore...))
	sendBytes("records-empty", "pe", 9, []byte{})
	sendBytes("records-7-bytes", "pe", 9, []byte{1, 2, 3, 4, 5, 6, 7})
	sendBytes("records-20-zero-bytes", "pe", 9, make([]byte, 20))
	sendBytes("records-header-only-61-bytes", "pe", 9, good[:61])

	// Timestamps. CreateTime holds a record to the broker's clock, within
	// message.timestamp.before.max.ms and message.timestamp.after.max.ms.
	stamp := func(label, topic string, first, max int64) {
		b := newBatch(1, one)
		b.First, b.Max = first, max
		send(label, topic, b)
	}
	const year2100 = 4102444800000
	stamp("timestamp-year-2100", "pe", year2100, year2100)
	stamp("timestamp-year-2100-logappend-topic", "pelat", year2100, year2100)
	stamp("timestamp-year-2100-after-max-unbounded", "peafter", year2100, year2100)
	stamp("timestamp-1970-before-max-one-second", "pebefore", 1000, 1000)
	stamp("timestamp-minus-1-before-max-one-second", "pebefore", -1, -1)
	stamp("timestamp-minus-5", "pe", -5, -5)
	stamp("timestamp-minus-1", "pe", -1, -1)
	stamp("max-timestamp-below-first", "pe", 5000, 1000)
	stamp("max-timestamp-year-2100-records-in-1970", "pe", 1000, year2100)
	// One record of three out of range: the answer names it.
	var mixed []byte
	mixed = append(mixed, aRecord(0, 0, sp("k0"), sp("v"))...)
	mixed = append(mixed, aRecord(year2100, 1, sp("k1"), sp("v"))...)
	mixed = append(mixed, aRecord(0, 2, sp("k2"), sp("v"))...)
	send("timestamp-second-of-three-in-year-2100", "pe", newBatch(3, mixed))
	lat := newBatch(1, one)
	lat.Attrs, lat.First, lat.Max = 0x08, -1, -1
	send("timestamp-type-bit-and-no-timestamp", "pe", lat)

	// Producer ids nobody was given.
	pid := func(label string, id int64, epoch int16, seq int32) {
		b := newBatch(1, one)
		b.PID, b.Epoch, b.Seq = id, epoch, seq
		send(label, "pe", b)
	}
	pid("unknown-pid-seq-5", 987654, 5, 5)
	pid("unknown-pid-seq-9-gap", 987654, 5, 9)
	pid("unknown-pid-seq-5-again", 987654, 5, 5)
	pid("unknown-pid-seq-0-lower-epoch", 987654, 4, 0)
	pid("pid-set-epoch-minus-1-seq-minus-1", 987655, -1, -1)
	pid("pid-set-seq-minus-1", 987656, 0, -1)
	pid("pid-minus-1-seq-7", -1, -1, 7)
	pid("pid-minus-1-epoch-3", -1, 3, -1)

	e.produceRaw("acks-2", "pe", 9, 2, "", good)
	e.produceRaw("acks-minus-2", "pe", 9, -2, "", good)

	// The size of a batch as it arrives, against the broker's default for a topic
	// that sets none (message.max.bytes, 1048588): the last byte that fits, the first
	// that does not, and a compressed batch that weighs more than the limit.
	sendBytes("default-limit-exactly-1048588", "pe", 9, batchOfSize(1048588, 0))
	sendBytes("default-limit-1048589", "pe", 9, batchOfSize(1048589, 0))
	sendBytes("default-limit-gzip-of-noise-1048700", "pe", 9, batchOfSize(1048700, codecGzip))
	sendBytes("after-the-large-batches", "pe", 9, good)

	e.storedRecords("stored-pe", "pe", false)
	e.storedRecords("stored-pec", "pec", false)
	e.storedRecords("stored-peu", "peu", false)
	e.storedRecords("stored-pez", "pez", false)
	e.storedRecords("stored-pelat", "pelat", true)
	e.storedRecords("stored-pebefore", "pebefore", false)
	e.storedRecords("stored-peafter", "peafter", false)

	for _, topic := range []string{"pe", "pec", "peu", "pez"} {
		produceStricter(e, topic)
	}
}

// produceMatrix is the cases that depend on how a topic makes the broker look inside
// a batch, run on one topic.
func produceMatrix(e *Env, topic string) {
	one := oneRecord()
	send := func(label string, b rawBatch) { e.produceRaw(label+"-"+topic, topic, 9, -1, "", b.bytes()) }
	e.produceRaw("warm-"+topic, topic, 9, -1, "", newBatch(1, one).bytes())

	// Each codec: a real batch, a records section that is not that codec at all, and
	// a real one cut in half.
	many := nRecords(40, "a value that repeats, so that it compresses")
	for _, codec := range []int{codecGzip, codecSnappy, codecLZ4, codecZstd} {
		name := codecNames[codec]
		valid := newBatch(40, many)
		valid.Codec = codec
		send("valid-"+name, valid)
		garbage := newBatch(1, nil)
		garbage.Codec, garbage.Wire = codec, []byte("this is not a compressed records section at all")
		send("garbage-"+name, garbage)
		cut := newBatch(40, nil)
		cut.Codec = codec
		whole := compress(codec, many)
		cut.Wire = whole[:len(whole)/2]
		send("truncated-"+name, cut)
	}
	raw := newBatch(40, nil)
	raw.Codec, raw.Wire = codecSnappy, snappy.Encode(nil, many) // librdkafka's snappy: one block, no framing.
	send("valid-snappy-rawblock", raw)
	afterStream := newBatch(40, nil)
	afterStream.Codec, afterStream.Wire = codecGzip, append(compress(codecGzip, many), []byte("bytes after the gzip stream has ended")...)
	send("gzip-then-bytes-after-the-stream", afterStream)
	for _, codec := range []int{5, 6, 7} {
		b := newBatch(1, nil)
		b.Codec, b.Wire = codec, one
		send(fmt.Sprintf("unknown-codec-%d", codec), b)
	}

	// The record count against what is there, plain and compressed.
	for _, codec := range []int{0, codecGzip} {
		suffix := ""
		if codec != 0 {
			suffix = "-" + codecNames[codec]
		}
		five := newBatch(5, one)
		five.Codec = codec
		send("count-declared-5-actual-1"+suffix, five)
		oneOfThree := newBatch(1, nRecords(3, "v"))
		oneOfThree.Codec = codec
		send("count-declared-1-actual-3"+suffix, oneOfThree)

		// A record that says it is longer than what is left, and one that says it is
		// shorter than it is. (Compressed, they are in produceStricter.)
		if codec == 0 {
			send("record-length-past-end", lengthPastEnd(codec))
			send("record-length-2-short", lengthShort(codec))
		}

		keyless := newBatch(1, aRecord(0, 0, nil, sp("v")))
		keyless.Codec = codec
		send("keyless"+suffix, keyless)

		// Offsets inside the batch that are not 0, 1, 2.
		var same, backwards []byte
		for i := range 3 {
			same = append(same, aRecord(0, 0, sp(fmt.Sprintf("k%d", i)), sp("v"))...)
			backwards = append(backwards, aRecord(0, int64(2-i), sp(fmt.Sprintf("k%d", i)), sp("v"))...)
		}
		zeros := newBatch(3, same)
		zeros.Codec = codec
		send("offset-deltas-all-zero"+suffix, zeros)
		down := newBatch(3, backwards)
		down.Codec = codec
		send("offset-deltas-descending"+suffix, down)
	}
	empty := newBatch(0, nil)
	empty.LastDelta = 0
	send("count-zero-empty", empty)
	delta := newBatch(1, one)
	delta.LastDelta = 7
	send("last-offset-delta-7-one-record", delta)
	trailing := newBatch(1, append(bytes.Clone(one), []byte("garbage after the record")...))
	trailing.Codec = codecGzip
	send("gzip-trailing-garbage-record", trailing)
	send("tombstone", newBatch(1, aRecord(0, 0, sp("k"), nil)))

	// Fields of a record that cannot be.
	field := func(label string, tail []byte) {
		r := append([]byte{0, 0, 0}, tail...) // attributes, timestamp delta, offset delta.
		send(label, newBatch(1, append(binary.AppendVarint(nil, int64(len(r))), r...)))
	}
	field("key-length-minus-2", []byte{0x03, 0x02, 'v', 0x00})        // key length -2, value "v", no headers.
	field("header-count-minus-1", []byte{0x02, 'k', 0x02, 'v', 0x01}) // key, value, -1 headers.
	field("header-count-3-none-there", []byte{0x02, 'k', 0x02, 'v', 0x06})
	field("header-key-length-minus-1", []byte{0x02, 'k', 0x02, 'v', 0x02, 0x01, 0x02, 'x'}) // one header, null key.

	// The window a zstd frame declares: what every compressor uses, and what none
	// does and Kafka's decoder will not take. (Between the two is in produceStricter.)
	for _, w := range []struct {
		name string
		log  int
	}{{"1m", 20}, {"8m", 23}, {"256m", 28}, {"2g", 31}} {
		b := newBatch(1, nil)
		b.Codec, b.Wire = codecZstd, zstdFrame(w.log, one)
		send("zstd-window-"+w.name, b)
	}
}

// lengthPastEnd is a batch of one record that says it is 40 bytes longer than it is;
// lengthShort, one that says it is 2 bytes shorter.
func lengthPastEnd(codec int) rawBatch {
	one := oneRecord()
	b := newBatch(1, append(binary.AppendVarint(nil, int64(len(one)-1+40)), one[1:]...))
	b.Codec = codec
	return b
}

func lengthShort(codec int) rawBatch {
	one := oneRecord()
	b := newBatch(1, append(binary.AppendVarint(nil, int64(len(one)-1-2)), one[1:]...))
	b.Codec = codec
	return b
}

// produceStricter is the batches Apache Kafka stores and this broker refuses, run
// last on each topic so that nothing before them is moved by the difference:
//
//   - a compressed batch whose record lies about its own length. Where the topic
//     stores the batch as it came, Kafka reads it with an iterator that skips fields
//     and never checks the length, and keeps it; the read-back that follows shows a
//     consumer cannot read what it kept. (Where the topic's codec differs, Kafka
//     reads the records in full and refuses, as it refuses the same batch uncompressed.)
//   - a zstd frame declaring a window of 16 or 128 MiB, which a decoder allocates
//     before it has read a byte.
func produceStricter(e *Env, topic string) {
	send := func(label string, b rawBatch) { e.produceRaw(label+"-"+topic, topic, 9, -1, "", b.bytes()) }
	send("record-length-past-end-gzip", lengthPastEnd(codecGzip))
	send("record-length-2-short-gzip", lengthShort(codecGzip))
	for _, w := range []struct {
		name string
		log  int
	}{{"16m", 24}, {"128m", 27}} {
		b := newBatch(1, nil)
		b.Codec, b.Wire = codecZstd, zstdFrame(w.log, oneRecord())
		send("zstd-window-"+w.name, b)
	}
	e.storedRecords("stored-after-the-stricter-cases-"+topic, topic, false)
}

// legacyMessage is one message of the format before record batches (magic 1), as a
// message set: offset, size, then crc, magic, attributes, timestamp, key, value.
func legacyMessage() []byte {
	var m []byte
	m = append(m, 1, 0) // magic, attributes
	m = binary.BigEndian.AppendUint64(m, 1000)
	m = binary.BigEndian.AppendUint32(m, 0xffffffff) // null key
	m = binary.BigEndian.AppendUint32(m, 1)
	m = append(m, 'v')
	var out []byte
	out = binary.BigEndian.AppendUint64(out, 0)
	out = binary.BigEndian.AppendUint32(out, uint32(4+len(m)))
	out = binary.BigEndian.AppendUint32(out, crc32IEEE(m))
	return append(out, m...)
}

// batchOfSize is a one-record batch that is exactly size bytes on the wire: zeros
// when it is not compressed, bytes no codec shrinks when it is.
func batchOfSize(size, codec int) []byte {
	for n := size - 200; ; n++ {
		value := string(make([]byte, n))
		if codec != 0 {
			value = string(noiseBytes(n))
		}
		b := newBatch(1, aRecord(0, 0, sp("k"), &value))
		b.Codec = codec
		out := b.bytes()
		if len(out) == size {
			return out
		}
		if len(out) > size {
			if codec == 0 {
				panic(fmt.Sprintf("no batch of exactly %d bytes", size))
			}
			return out // a compressor's output moves by more than a byte at a time.
		}
	}
}

// The limitations sentences for the three batches Kafka stores and this broker refuses.
const (
	limRecordLength = "a compressed record whose length disagrees with its own fields is INVALID_RECORD"
	limInflate      = "a batch that inflates past 64 MiB is"
	limZstdWindow   = "window past 8 MiB is MESSAGE_TOO_LARGE"
)

func knownProduceValidation() []Known {
	var k []Known
	for _, topic := range []string{"pe", "pec", "peu", "pez"} {
		if topic == "pe" || topic == "pec" {
			// Where the topic keeps the producer's codec; elsewhere Kafka reads the
			// records in full and refuses as this does.
			k = append(k, Known{Step: "record-length-past-end-gzip-" + topic, Limitation: limRecordLength},
				Known{Step: "record-length-2-short-gzip-" + topic, Limitation: limRecordLength})
		}
		k = append(k, Known{Step: "zstd-window-16m-" + topic, Limitation: limZstdWindow},
			Known{Step: "zstd-window-128m-" + topic, Limitation: limZstdWindow},
			Known{Step: "stored-after-the-stricter-cases-" + topic, Limitation: limZstdWindow})
	}
	return k
}

// produceInflatedSize is how large a compressed batch may inflate on each kind of
// topic: 63, 65 and 70 one-mebibyte records that weigh a few tens of kilobytes, and
// one record of 70 MiB. (Q10 recorded this to 20 MiB.) It also has the refusal Q10c
// lacked: a batch that is small as it arrives and over the topic's limit once the
// broker has compressed it again with the topic's codec.
func produceInflatedSize(e *Env) {
	e.Do("create", createTopics(false,
		newTopic("pb", 1, 1),
		newTopic("pbc", 1, 1, "cleanup.policy", "compact"),
		newTopic("pbu", 1, 1, "compression.type", "uncompressed"),
		newTopic("pbz", 1, 1, "compression.type", "zstd"),
		newTopic("pblat", 1, 1, "message.timestamp.type", "LogAppendTime"),
		newTopic("pbgz", 1, 1, "compression.type", "gzip", "max.message.bytes", "2000"),
		newTopic("pblz", 1, 1, "compression.type", "lz4", "max.message.bytes", "2000"),
	), 7)
	one := oneRecord()
	topics := []string{"pb", "pbc", "pbu", "pbz", "pblat"}
	for _, topic := range topics {
		e.produceRaw("warm-"+topic, topic, 9, -1, "", newBatch(1, one).bytes())
	}
	mib := string(make([]byte, 1<<20))
	big := func(n, codec int) []byte {
		b := newBatch(n, nRecords(n, mib))
		b.Codec = codec
		return b.bytes()
	}
	end := func(label, topic string) {
		r := kmsg.NewPtrListOffsetsRequest()
		r.ReplicaID = -1
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Timestamp, rp.CurrentLeaderEpoch = -1, -1
		rt.Partitions = append(rt.Partitions, rp)
		r.Topics = append(r.Topics, rt)
		e.Do(label, r, 7)
	}
	// Under this broker's own ceiling first, and where the log ends then; what is
	// past the ceiling comes after, so a difference there does not move the rest.
	for _, codec := range []int{codecGzip, codecZstd} {
		payload := big(63, codec)
		for _, topic := range topics {
			e.produceRaw(fmt.Sprintf("%s-inflates-63m-%s", codecNames[codec], topic), topic, 9, -1, "", payload)
		}
	}
	for _, topic := range topics {
		end("end-after-63m-"+topic, topic)
	}

	// The topic's codec is not the batch's: Kafka compresses the records again and
	// holds what it then has to the limit. Zeros that zstd packs into a few dozen
	// bytes are a few thousand as gzip or lz4; under the limit they stay accepted.
	zeros := func(n int) []byte {
		v := string(make([]byte, n))
		b := newBatch(1, aRecord(0, 0, sp("k"), &v))
		b.Codec = codecZstd
		return b.bytes()
	}
	e.produceRaw("zstd-of-50k-recompressed-as-gzip-under-2000", "pbgz", 9, -1, "", zeros(50_000))
	e.produceRaw("zstd-of-8m-recompressed-as-gzip-over-2000", "pbgz", 9, -1, "", zeros(8<<20))
	e.produceRaw("zstd-of-50k-recompressed-as-lz4-under-2000", "pblz", 9, -1, "", zeros(50_000))
	e.produceRaw("zstd-of-8m-recompressed-as-lz4-over-2000", "pblz", 9, -1, "", zeros(8<<20))
	// And a batch that is not compressed, on a topic that compresses: what counts is
	// the size it arrived at, before anything else.
	plain := string(make([]byte, 3000))
	e.produceRaw("plain-3000-zeros-on-a-gzip-topic-with-limit-2000", "pbgz", 9, -1, "", newBatch(1, aRecord(0, 0, sp("k"), &plain)).bytes())
	end("end-pbgz", "pbgz")
	end("end-pblz", "pblz")

	for _, n := range []int{65, 70} {
		for _, codec := range []int{codecGzip, codecZstd} {
			payload := big(n, codec)
			for _, topic := range topics {
				e.produceRaw(fmt.Sprintf("%s-inflates-%dm-%s", codecNames[codec], n, topic), topic, 9, -1, "", payload)
			}
		}
	}
	huge := string(make([]byte, 70<<20))
	single := newBatch(1, aRecord(0, 0, sp("k"), &huge))
	single.Codec = codecGzip
	payload := single.bytes()
	for _, topic := range topics {
		e.produceRaw("gzip-one-record-70m-"+topic, topic, 9, -1, "", payload)
	}
}

func knownProduceInflated() []Known {
	var k []Known
	for _, topic := range []string{"pb", "pbc", "pbz", "pblat"} {
		for _, step := range []string{"gzip-inflates-65m-", "gzip-inflates-70m-", "zstd-inflates-65m-", "zstd-inflates-70m-", "gzip-one-record-70m-"} {
			k = append(k, Known{Step: step + topic, Limitation: limInflate})
		}
	}
	return k
}
