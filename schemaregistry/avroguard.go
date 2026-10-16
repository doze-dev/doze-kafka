package schemaregistry

import (
	"errors"
	"fmt"
	"math"

	"github.com/hamba/avro/v2"
)

// A record value is whatever a producer wrote, and the Avro decoder this package uses
// (hamba/avro, which is archived) can be made to spin, to allocate without limit and
// to panic by bytes that are not what the schema says: GO-2026-5046 (a block that
// claims up to 2^63 elements is looped over), GO-2026-5047 (lengths and counts that
// overflow) and GO-2026-5048 (maps that grow without a bound). No release of it fixes
// them. So the decoder is never shown such bytes: checkAvro walks the value against
// its schema first, reading nothing but lengths, and refuses anything the decoder
// would have to trust.
//
// What it guarantees of a value it accepts:
//
//   - every length and count is non-negative, fits the bytes that are left where it
//     must (a string, bytes, a fixed), and no count is the one number whose negation
//     overflows;
//   - the arrays and maps of the whole value hold no more than maxAvroItems elements
//     between them, however they are nested and however many blocks they come in (an
//     element can weigh nothing, an array of nulls, so the payload's size bounds
//     nothing: 1,000 such arrays of a million each fit in 4 KB);
//   - nothing is nested more than maxAvroDepth deep (a recursive schema is as deep
//     as its data says, and the decoder recurses with it; a stack that outgrows its
//     limit takes the process down, and no recover catches that);
//   - the value ends where the schema says, inside the bytes there are.
//
// A value the walk accepts costs the decoder time and memory in proportion to the
// payload plus maxAvroItems, and none of the three advisories' inputs reach it.
const (
	maxAvroItems = 1 << 20
	maxAvroDepth = 1024
)

var errAvroValue = errors.New("the payload does not match the schema")

type avroWalk struct {
	b     []byte
	items int // array elements and map entries so far, in the whole value.
}

func (w *avroWalk) fail(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errAvroValue, fmt.Sprintf(format, args...))
}

// long reads one zig-zag varint, the encoding of every Avro int, long, length and count.
func (w *avroWalk) long() (int64, error) {
	var v uint64
	for i := 0; i < len(w.b); i++ {
		c := w.b[i]
		if i == 9 && c > 1 {
			return 0, w.fail("a number is longer than 64 bits")
		}
		v |= uint64(c&0x7f) << (7 * i)
		if c&0x80 == 0 {
			w.b = w.b[i+1:]
			return int64(v>>1) ^ -int64(v&1), nil
		}
	}
	return 0, w.fail("it ends inside a number")
}

func (w *avroWalk) skip(n int64, what string) error {
	if n < 0 || n > int64(len(w.b)) {
		return w.fail("%s of %d bytes with %d left", what, n, len(w.b))
	}
	w.b = w.b[n:]
	return nil
}

// blocks walks the blocks of an array or a map, calling item once per element.
func (w *avroWalk) blocks(what string, item func() error) error {
	for {
		n, err := w.long()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		if n < 0 {
			if n == math.MinInt64 {
				return w.fail("%s block of -2^63 elements", what)
			}
			n = -n
			// The block's size in bytes follows a negative count. It is a hint for
			// skipping, which nothing here does; only that it is a size is checked.
			size, err := w.long()
			if err != nil {
				return err
			}
			if size < 0 {
				return w.fail("%s block of %d bytes", what, size)
			}
		}
		if n > int64(maxAvroItems-w.items) {
			return w.fail("more than %d array and map elements in one value", maxAvroItems)
		}
		w.items += int(n)
		for range n {
			if err := item(); err != nil {
				return err
			}
		}
	}
}

func (w *avroWalk) value(s avro.Schema, depth int) error {
	if depth > maxAvroDepth {
		return w.fail("nested more than %d deep", maxAvroDepth)
	}
	switch s.Type() {
	case avro.Ref:
		return w.value(s.(*avro.RefSchema).Schema(), depth)
	case avro.Null:
		return nil
	case avro.Boolean:
		return w.skip(1, "a boolean")
	case avro.Int, avro.Long, avro.Enum:
		_, err := w.long()
		return err
	case avro.Float:
		return w.skip(4, "a float")
	case avro.Double:
		return w.skip(8, "a double")
	case avro.String, avro.Bytes:
		n, err := w.long()
		if err != nil {
			return err
		}
		return w.skip(n, "a "+string(s.Type()))
	case avro.Fixed:
		return w.skip(int64(s.(*avro.FixedSchema).Size()), "a fixed")
	case avro.Record, avro.Error:
		for _, f := range s.(*avro.RecordSchema).Fields() {
			if err := w.value(f.Type(), depth+1); err != nil {
				return err
			}
		}
		return nil
	case avro.Array:
		items := s.(*avro.ArraySchema).Items()
		return w.blocks("an array", func() error { return w.value(items, depth+1) })
	case avro.Map:
		values := s.(*avro.MapSchema).Values()
		return w.blocks("a map", func() error {
			n, err := w.long()
			if err != nil {
				return err
			}
			if err := w.skip(n, "a map key"); err != nil {
				return err
			}
			return w.value(values, depth+1)
		})
	case avro.Union:
		types := s.(*avro.UnionSchema).Types()
		i, err := w.long()
		if err != nil {
			return err
		}
		if i < 0 || i >= int64(len(types)) {
			return w.fail("union branch %d of %d", i, len(types))
		}
		return w.value(types[i], depth+1)
	}
	return w.fail("a schema of type %s", s.Type())
}

// checkAvro reports whether body is one value of schema that the decoder can be
// given: see the guarantees above. Bytes after the value are not its business (the
// decoder ignores them too).
func checkAvro(schema avro.Schema, body []byte) error {
	w := &avroWalk{b: body}
	return w.value(schema, 0)
}
