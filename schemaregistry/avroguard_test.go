package schemaregistry

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
)

// zz is an Avro long: a zig-zag varint.
func zz(v int64) []byte {
	return binary.AppendUvarint(nil, uint64(v<<1)^uint64(v>>63))
}

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// wire is a record value in the registry's framing around an Avro body.
func wire(body []byte) []byte { return append([]byte{0, 0, 0, 0, 1}, body...) }

const (
	recOf   = `{"type":"record","name":"R","fields":[{"name":"f","type":%s}]}`
	treeRec = `{"type":"record","name":"Tree","fields":[{"name":"next","type":["null","Tree"]}]}`
)

// decodeBounded runs Decode and fails the test if it takes long or allocates much:
// the point of the guard is that a few hostile bytes cost what a few bytes should.
func decodeBounded(t *testing.T, c *Codec, value []byte) (string, error) {
	t.Helper()
	type result struct {
		out string
		err error
	}
	done := make(chan result, 1)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	go func() {
		out, err := c.Decode(value)
		done <- result{out, err}
	}()
	select {
	case r := <-done:
		runtime.ReadMemStats(&after)
		if grew := after.TotalAlloc - before.TotalAlloc; grew > 48<<20 {
			t.Errorf("decoding %d bytes allocated %d MiB", len(value), grew>>20)
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Errorf("decoding %d bytes took %s", len(value), took)
		}
		return r.out, r.err
	case <-time.After(20 * time.Second):
		t.Fatalf("decoding %d bytes is still running after 20s (a goroutine is left spinning)", len(value))
		return "", nil
	}
}

// Bytes that are not what the schema says are refused before the Avro decoder is
// given them, in the time and memory a few bytes are worth. Each case is an input
// one of the three advisories against the decoder describes (GO-2026-5046, 5047,
// 5048), or one found while reading it for them: the per-array limit it does enforce
// is per array, so arrays inside an array multiplied it.
func TestHostileAvroIsRefusedBeforeTheDecoderSeesIt(t *testing.T) {
	const million = 1 << 20
	var manyArrays [][]byte // 64 arrays of a million nulls each: 256 bytes that ask for a gigabyte.
	manyArrays = append(manyArrays, zz(64))
	for range 64 {
		manyArrays = append(manyArrays, zz(million), zz(0))
	}
	manyArrays = append(manyArrays, zz(0))

	for _, tc := range []struct {
		name, schema string
		body         []byte
	}{
		{"an array block of 2^63-1 elements, then nothing", fmt.Sprintf(recOf, `{"type":"array","items":"long"}`), zz(math.MaxInt64)},
		{"a map block of 2^63-1 entries, then nothing", fmt.Sprintf(recOf, `{"type":"map","values":"long"}`), zz(math.MaxInt64)},
		{"a map of nulls with 2^62 entries", fmt.Sprintf(recOf, `{"type":"map","values":"null"}`), zz(1 << 62)},
		{"an array of 2^40 nulls, which weigh nothing", fmt.Sprintf(recOf, `{"type":"array","items":"null"}`), cat(zz(1<<40), zz(0))},
		{"an array of 2^40 empty records", fmt.Sprintf(recOf, `{"type":"array","items":{"type":"record","name":"E","fields":[]}}`), cat(zz(1<<40), zz(0))},
		{"one element past the limit, over two blocks", fmt.Sprintf(recOf, `{"type":"array","items":"null"}`), cat(zz(million), zz(1), zz(0))},
		{"64 arrays of a million nulls inside an array", fmt.Sprintf(recOf, `{"type":"array","items":{"type":"array","items":"null"}}`), cat(manyArrays...)},
		{"an array block of -2^63 elements", fmt.Sprintf(recOf, `{"type":"array","items":"long"}`), cat(zz(math.MinInt64), zz(0), zz(0))},
		{"a map block of -2^63 entries", fmt.Sprintf(recOf, `{"type":"map","values":"long"}`), cat(zz(math.MinInt64), zz(0), zz(0))},
		{"a block whose byte size is negative", fmt.Sprintf(recOf, `{"type":"array","items":"long"}`), cat(zz(-1), zz(-5), zz(7), zz(0))},
		{"a string longer than what is left", fmt.Sprintf(recOf, `"string"`), cat(zz(8<<20), []byte("ab"))},
		{"bytes of negative length", fmt.Sprintf(recOf, `"bytes"`), zz(-3)},
		{"bytes of 2^62", fmt.Sprintf(recOf, `"bytes"`), cat(zz(1<<62), []byte("ab"))},
		{"a fixed of 512 MiB with two bytes sent", fmt.Sprintf(recOf, `{"type":"fixed","name":"F","size":536870912}`), []byte("ab")},
		{"a union branch that is not there", fmt.Sprintf(recOf, `["null","string"]`), zz(2)},
		{"a negative union branch", fmt.Sprintf(recOf, `["null","string"]`), zz(-1)},
		{"a number of eleven bytes", fmt.Sprintf(recOf, `"long"`), bytes.Repeat([]byte{0xff}, 11)},
		{"a number that is not finished", fmt.Sprintf(recOf, `"long"`), []byte{0x80, 0x80}},
		{"a value 200,000 records deep", treeRec, cat(bytes.Repeat(zz(1), 200000), zz(0))},
		{"a map key longer than what is left", fmt.Sprintf(recOf, `{"type":"map","values":"long"}`), cat(zz(1), zz(99), []byte("k"))},
		{"nothing at all", fmt.Sprintf(recOf, `"long"`), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := mustCodec(t, TypeAvro, tc.schema)
			if _, err := decodeBounded(t, c, wire(tc.body)); err == nil {
				t.Fatal("decoded")
			} else if !errors.Is(err, errAvroValue) {
				t.Errorf("refused by the decoder, not before it: %v", err)
			}
		})
	}
}

// What a real encoder writes passes: every kind of schema, both ways of writing a
// block (a count, or a negative count and a byte size), the limit itself, and a
// recursion as deep as is allowed.
func TestTheGuardLetsThroughWhatAnEncoderWrites(t *testing.T) {
	for _, tc := range []struct{ name, schema, doc string }{
		{"every primitive", `{"type":"record","name":"P","fields":[
			{"name":"b","type":"boolean"},{"name":"i","type":"int"},{"name":"l","type":"long"},
			{"name":"f","type":"float"},{"name":"d","type":"double"},{"name":"s","type":"string"},{"name":"y","type":"bytes"}]}`,
			`{"b":true,"i":-7,"l":9007199254740993,"f":1.5,"d":-2.25,"s":"héllo","y":"abc"}`},
		{"arrays, maps, unions, an enum, a fixed", `{"type":"record","name":"C","fields":[
			{"name":"tags","type":{"type":"array","items":"string"}},
			{"name":"grid","type":{"type":"array","items":{"type":"array","items":"long"}}},
			{"name":"attrs","type":{"type":"map","values":["null","long"]}},
			{"name":"kind","type":{"type":"enum","name":"K","symbols":["A","B","C"]}},
			{"name":"id","type":{"type":"fixed","name":"Id","size":4}},
			{"name":"opt","type":["null",{"type":"record","name":"In","fields":[{"name":"x","type":"int"}]}]}]}`,
			`{"tags":["a","bb",""],"grid":[[1,2,3],[],[4]],"attrs":{"a":null,"b":5,"":6},"kind":"C","id":"abcd","opt":{"x":3}}`},
		{"empty containers", `{"type":"record","name":"E","fields":[
			{"name":"a","type":{"type":"array","items":"long"}},{"name":"m","type":{"type":"map","values":"long"}}]}`, `{"a":[],"m":{}}`},
		{"a recursive record", treeRec, `{"next":{"next":{"next":null}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := mustCodec(t, TypeAvro, tc.schema)
			value, err := c.Encode(1, tc.doc)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if err := checkAvro(c.p.(*avroSchema).schema, value[5:]); err != nil {
				t.Fatalf("the guard refused what the encoder wrote: %v", err)
			}
			if _, err := decodeBounded(t, c, value); err != nil {
				t.Fatalf("decode: %v", err)
			}
		})
	}

	longs := mustCodec(t, TypeAvro, fmt.Sprintf(recOf, `{"type":"array","items":"long"}`))
	// The other block form: a negative count, then the block's size in bytes.
	out, err := decodeBounded(t, longs, wire(cat(zz(-2), zz(2), zz(7), zz(8), zz(1), zz(9), zz(0))))
	if err != nil || out != `{"f":[7,8,9]}` {
		t.Errorf("a block written as a negative count and a size: %q, %v", out, err)
	}
	// Exactly the limit, in two blocks: allowed. (A million nulls is what it is.)
	nulls := mustCodec(t, TypeAvro, fmt.Sprintf(recOf, `{"type":"array","items":"null"}`))
	if err := checkAvro(nulls.p.(*avroSchema).schema, cat(zz(maxAvroItems-5), zz(5), zz(0))); err != nil {
		t.Errorf("an array of exactly the limit: %v", err)
	}
	// As deep as is allowed, and one deeper.
	tree := mustCodec(t, TypeAvro, treeRec)
	levels := func(n int) []byte { return cat(bytes.Repeat(zz(1), n), zz(0)) }
	// Each level is a record in a union: two steps down.
	if _, err := decodeBounded(t, tree, wire(levels(maxAvroDepth/2-1))); err != nil {
		t.Errorf("%d records deep: %v", maxAvroDepth/2-1, err)
	}
	if _, err := decodeBounded(t, tree, wire(levels(maxAvroDepth/2+1))); !errors.Is(err, errAvroValue) {
		t.Errorf("%d records deep: %v, want it refused", maxAvroDepth/2+1, err)
	}
	// Bytes after the value are not the value's.
	if out, err := decodeBounded(t, longs, wire(cat(zz(1), zz(4), zz(0), []byte("trailing")))); err != nil || out != `{"f":[4]}` {
		t.Errorf("bytes after the value: %q, %v", out, err)
	}
}

// The guard and the decoder agree about values the library's own encoder writes:
// random values of schemas with every container, encoded by hamba/avro itself.
func TestTheGuardAcceptsEverythingTheLibraryEncodes(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"G","fields":[
		{"name":"a","type":{"type":"array","items":{"type":"map","values":{"type":"array","items":["null","long","string"]}}}},
		{"name":"s","type":"string"},{"name":"b","type":"bytes"},{"name":"d","type":"double"}]}`)
	rng := rand.New(rand.NewSource(11))
	text := func() string { return strings.Repeat("x", rng.Intn(40)) }
	for i := range 300 {
		var a []any
		for range rng.Intn(5) {
			m := map[string]any{}
			for range rng.Intn(4) {
				var inner []any
				for range rng.Intn(6) {
					switch rng.Intn(3) {
					case 0:
						inner = append(inner, nil)
					case 1:
						inner = append(inner, map[string]any{"long": rng.Int63() - 1<<62})
					default:
						inner = append(inner, map[string]any{"string": text()})
					}
				}
				m[text()] = inner
			}
			a = append(a, m)
		}
		value := map[string]any{"a": a, "s": text(), "b": []byte(text()), "d": rng.NormFloat64()}
		body, err := avro.Marshal(schema, value)
		if err != nil {
			t.Fatalf("value %d: marshal: %v", i, err)
		}
		if err := checkAvro(schema, body); err != nil {
			t.Fatalf("value %d: the guard refused what the library encoded (%x): %v", i, body, err)
		}
		w := &avroWalk{b: body}
		if err := w.value(schema, 0); err != nil || len(w.b) != 0 {
			t.Fatalf("value %d: the walk ended %d bytes before the value did (%v)", i, len(w.b), err)
		}
	}
}

// Random bytes against schemas with every container never cost more than bytes
// should, whether the guard or the decoder is what refuses them.
func TestRandomBytesNeverCostMoreThanTheyWeigh(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	var codecs []*Codec
	for _, s := range fuzzAvroSchemas {
		codecs = append(codecs, mustCodec(t, TypeAvro, s))
	}
	start := time.Now()
	for range 4000 {
		body := make([]byte, rng.Intn(24))
		rng.Read(body)
		for _, c := range codecs {
			c.Decode(wire(body))
		}
	}
	if took := time.Since(start); took > 20*time.Second {
		t.Errorf("4000 short random values against %d schemas took %s", len(codecs), took)
	}
}
