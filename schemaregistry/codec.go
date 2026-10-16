package schemaregistry

// Reading and writing the bytes a Confluent serializer puts in a Kafka record value:
//
//	byte 0        magic, 0
//	bytes 1-4     the schema's id, big-endian
//	Protobuf only the message index: a list of varints naming which message in the
//	              schema (a single 0 means the first message)
//	the rest      the payload: Avro binary, Protobuf binary, or JSON text
//
// The console uses this to show a payload as the JSON it means and to produce one from
// JSON a person typed.

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// WireID reports the schema id a value in Confluent's wire format names.
//
//fuzz:parser FuzzDecode
func WireID(value []byte) (id int, ok bool) {
	if len(value) < 5 || value[0] != 0 {
		return 0, false
	}
	return int(binary.BigEndian.Uint32(value[1:5])), true
}

// Ref is a schema a codec's schema depends on.
type Ref struct{ Name, Type, Text string }

// Codec reads and writes payloads for one schema.
type Codec struct {
	p parsedSchema
}

// NewCodec prepares a schema (and the schemas it references, dependencies first) for
// decoding and encoding.
func NewCodec(typ, text string, refs []Ref) (*Codec, error) {
	rr := make([]resolvedRef, len(refs))
	for i, r := range refs {
		rr[i] = resolvedRef{Reference: Reference{Name: r.Name}, Type: r.Type, Text: r.Text}
	}
	p, err := parseSchema(typ, text, rr)
	if err != nil {
		return nil, err
	}
	return &Codec{p: p}, nil
}

// Type is AVRO, JSON or PROTOBUF.
func (c *Codec) Type() string { return c.p.Type() }

// avroLimits bounds what a decoder may allocate for one string and one array. A
// record's bytes name their own lengths, so without a bound a few bytes ask for
// gigabytes. It is not the whole bound (the limit on an array is per array, and there
// is none on a map): checkAvro, which every value passes first, is.
var avroLimits = avro.Config{MaxByteSliceSize: 8 << 20, MaxSliceAllocSize: maxAvroItems}.Freeze()

// Decode turns a whole record value (header included) into JSON text.
//
//fuzz:parser FuzzDecode
func (c *Codec) Decode(value []byte) (out string, err error) {
	// The bytes are whatever a producer wrote; a decoder that trips over them must say
	// so, not take the caller down with it.
	defer func() {
		if r := recover(); r != nil {
			out, err = "", fmt.Errorf("the payload does not match the schema (%v)", r)
		}
	}()
	if _, ok := WireID(value); !ok {
		return "", errors.New("not in the registry's wire format")
	}
	body := value[5:]
	switch p := c.p.(type) {
	case *avroSchema:
		// Before the decoder sees a byte: it trusts counts and lengths that these
		// bytes, which anyone may have produced, are free to lie about (avroguard.go).
		if err := checkAvro(p.schema, body); err != nil {
			return "", err
		}
		var v any
		if err := avroLimits.Unmarshal(p.schema, body, &v); err != nil {
			return "", err
		}
		j, err := json.Marshal(avroToJSON(v))
		return string(j), err
	case *jsonSchema:
		var buf bytes.Buffer
		if err := json.Compact(&buf, body); err != nil {
			return "", err
		}
		return buf.String(), nil
	case *protoSchema:
		msgs := p.file.Messages()
		idx, rest, err := readMessageIndex(body)
		if err != nil {
			return "", err
		}
		md, err := messageAt(msgs, idx)
		if err != nil {
			return "", err
		}
		m := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(rest, m); err != nil {
			return "", err
		}
		j, err := protojson.MarshalOptions{EmitUnpopulated: false}.Marshal(m)
		if err != nil {
			return "", err
		}
		var buf bytes.Buffer
		json.Compact(&buf, j)
		return buf.String(), nil
	}
	return "", errors.New("unsupported schema type")
}

// Encode turns JSON text into a record value for schema id.
//
//fuzz:parser FuzzEncode
func (c *Codec) Encode(id int, doc string) ([]byte, error) {
	head := make([]byte, 5)
	binary.BigEndian.PutUint32(head[1:], uint32(id))
	switch p := c.p.(type) {
	case *avroSchema:
		v, err := parseJSONValue(doc)
		if err != nil {
			return nil, err
		}
		native, err := jsonToAvro(p.schema, v)
		if err != nil {
			return nil, err
		}
		body, err := avro.Marshal(p.schema, native)
		if err != nil {
			return nil, err
		}
		return append(head, body...), nil
	case *jsonSchema:
		if p.compiled != nil {
			inst, err := jsonschema.UnmarshalJSON(strings.NewReader(doc))
			if err != nil {
				return nil, err
			}
			if err := p.compiled.Validate(inst); err != nil {
				return nil, err
			}
		}
		var buf bytes.Buffer
		if err := json.Compact(&buf, []byte(doc)); err != nil {
			return nil, err
		}
		return append(head, buf.Bytes()...), nil
	case *protoSchema:
		msgs := p.file.Messages()
		if msgs.Len() == 0 {
			return nil, errors.New("the schema declares no message")
		}
		m := dynamicpb.NewMessage(msgs.Get(0))
		if err := (protojson.UnmarshalOptions{}).Unmarshal([]byte(doc), m); err != nil {
			return nil, err
		}
		body, err := proto.Marshal(m)
		if err != nil {
			return nil, err
		}
		head = append(head, 0) // the first message: Confluent writes a single zero for index [0].
		return append(head, body...), nil
	}
	return nil, errors.New("unsupported schema type")
}

// readMessageIndex reads Protobuf's message-index list: a zigzag-varint count, then
// that many zigzag varints. A count of zero stands for [0].
//
//fuzz:parser FuzzDecode
func readMessageIndex(b []byte) (idx []int, rest []byte, err error) {
	n, k := binary.Varint(b)
	if k <= 0 {
		return nil, nil, errors.New("bad message index")
	}
	b = b[k:]
	if n == 0 {
		return []int{0}, b, nil
	}
	if n < 0 || n > 64 {
		return nil, nil, errors.New("bad message index")
	}
	for i := int64(0); i < n; i++ {
		v, k := binary.Varint(b)
		if k <= 0 || v < 0 {
			return nil, nil, errors.New("bad message index")
		}
		idx = append(idx, int(v))
		b = b[k:]
	}
	return idx, b, nil
}

func messageAt(msgs protoreflect.MessageDescriptors, idx []int) (protoreflect.MessageDescriptor, error) {
	var md protoreflect.MessageDescriptor
	for depth, i := range idx {
		if i < 0 || i >= msgs.Len() {
			return nil, fmt.Errorf("message index %v is outside the schema", idx)
		}
		md = msgs.Get(i)
		if depth < len(idx)-1 {
			msgs = md.Messages()
		}
	}
	return md, nil
}

//fuzz:parser FuzzEncode
func parseJSONValue(doc string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(doc))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("unexpected content after the JSON value")
	}
	return v, nil
}

// avroToJSON makes what Avro decoded fit encoding/json: raw bytes read as text,
// fixed as text, decimals as numbers in text.
func avroToJSON(v any) any {
	switch t := v.(type) {
	case []byte:
		return bytesToString(t)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = avroToJSON(x)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = avroToJSON(x)
		}
		return out
	case *big.Rat:
		return t.FloatString(10)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case time.Duration:
		return t.String()
	case float32:
		if math.IsNaN(float64(t)) || math.IsInf(float64(t), 0) {
			return fmt.Sprint(t)
		}
		return t
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return fmt.Sprint(t)
		}
		return t
	}
	rv := reflect.ValueOf(v)
	if rv.IsValid() && rv.Kind() == reflect.Array && rv.Type().Elem().Kind() == reflect.Uint8 {
		b := make([]byte, rv.Len())
		reflect.Copy(reflect.ValueOf(b), rv)
		return bytesToString(b)
	}
	return v
}

// bytesToString is how Avro's JSON encoding writes bytes: each byte as the character
// with that code.
func bytesToString(b []byte) string {
	r := make([]rune, len(b))
	for i, c := range b {
		r[i] = rune(c)
	}
	return string(r)
}

func stringToBytes(s string) ([]byte, error) {
	out := make([]byte, 0, len(s))
	for _, r := range s {
		if r > 255 {
			// Not Avro's byte-per-character form; accept base64, which is how most
			// people write bytes.
			if b, err := base64.StdEncoding.DecodeString(s); err == nil {
				return b, nil
			}
			return nil, fmt.Errorf("%q is not bytes: characters must be 0-255 (or the value base64)", s)
		}
		out = append(out, byte(r))
	}
	return out, nil
}

// jsonToAvro converts a decoded JSON value to the Go value hamba/avro writes for the
// schema, choosing a union branch by trying them in order.
func jsonToAvro(s avro.Schema, v any) (any, error) {
	switch sc := s.(type) {
	case *avro.RefSchema:
		return jsonToAvro(sc.Schema(), v)
	case *avro.UnionSchema:
		if v == nil {
			if sc.Nullable() {
				return nil, nil
			}
			return nil, errors.New("null is not one of the union's branches")
		}
		var errs []string
		for _, br := range sc.Types() {
			if br.Type() == avro.Null {
				continue
			}
			native, err := jsonToAvro(br, v)
			if err == nil {
				if _, merr := avro.Marshal(br, native); merr == nil {
					return map[string]any{unionBranchName(br): native}, nil
				} else {
					err = merr
				}
			}
			errs = append(errs, err.Error())
		}
		return nil, fmt.Errorf("no branch of the union fits: %s", strings.Join(errs, "; "))
	case *avro.RecordSchema:
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("record %s needs a JSON object", sc.FullName())
		}
		out := make(map[string]any, len(sc.Fields()))
		for _, f := range sc.Fields() {
			fv, present := m[f.Name()]
			if !present {
				if f.HasDefault() {
					out[f.Name()] = f.Default()
					continue
				}
				return nil, fmt.Errorf("field %q is missing and has no default", f.Name())
			}
			native, err := jsonToAvro(f.Type(), fv)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", f.Name(), err)
			}
			out[f.Name()] = native
		}
		return out, nil
	case *avro.ArraySchema:
		arr, ok := v.([]any)
		if !ok {
			return nil, errors.New("an array needs a JSON array")
		}
		out := make([]any, len(arr))
		for i, x := range arr {
			native, err := jsonToAvro(sc.Items(), x)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
			out[i] = native
		}
		return out, nil
	case *avro.MapSchema:
		m, ok := v.(map[string]any)
		if !ok {
			return nil, errors.New("a map needs a JSON object")
		}
		out := make(map[string]any, len(m))
		for k, x := range m {
			native, err := jsonToAvro(sc.Values(), x)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
			out[k] = native
		}
		return out, nil
	case *avro.EnumSchema:
		str, ok := v.(string)
		if !ok {
			return nil, errors.New("an enum needs a string")
		}
		return str, nil
	case *avro.FixedSchema:
		str, ok := v.(string)
		if !ok {
			return nil, errors.New("fixed needs a string")
		}
		b, err := stringToBytes(str)
		if err != nil {
			return nil, err
		}
		if len(b) != sc.Size() {
			return nil, fmt.Errorf("fixed %s needs %d bytes, got %d", sc.FullName(), sc.Size(), len(b))
		}
		arr := reflect.New(reflect.ArrayOf(sc.Size(), reflect.TypeFor[byte]())).Elem()
		reflect.Copy(arr, reflect.ValueOf(b))
		return arr.Interface(), nil
	case *avro.PrimitiveSchema:
		return primitiveToAvro(sc, v)
	}
	return nil, errors.New("unsupported Avro schema")
}

func unionBranchName(s avro.Schema) string {
	if ref, ok := s.(*avro.RefSchema); ok {
		s = ref.Schema()
	}
	if n, ok := s.(avro.NamedSchema); ok {
		return n.FullName()
	}
	return string(s.Type())
}

func primitiveToAvro(sc *avro.PrimitiveSchema, v any) (any, error) {
	logical := ""
	if l := sc.Logical(); l != nil {
		logical = string(l.Type())
	}
	num := func() (json.Number, bool) { n, ok := v.(json.Number); return n, ok }
	switch sc.Type() {
	case avro.Null:
		if v != nil {
			return nil, errors.New("expected null")
		}
		return nil, nil
	case avro.Boolean:
		b, ok := v.(bool)
		if !ok {
			return nil, errors.New("expected true or false")
		}
		return b, nil
	case avro.String:
		str, ok := v.(string)
		if !ok {
			return nil, errors.New("expected a string")
		}
		return str, nil
	case avro.Bytes:
		if logical == "decimal" {
			if n, ok := num(); ok {
				r, ok := new(big.Rat).SetString(n.String())
				if !ok {
					return nil, errors.New("not a decimal")
				}
				return r, nil
			}
			if str, ok := v.(string); ok {
				r, ok := new(big.Rat).SetString(str)
				if !ok {
					return nil, errors.New("not a decimal")
				}
				return r, nil
			}
			return nil, errors.New("expected a decimal number")
		}
		str, ok := v.(string)
		if !ok {
			return nil, errors.New("expected a string of bytes")
		}
		return stringToBytes(str)
	case avro.Int, avro.Long:
		switch logical {
		case "timestamp-millis", "timestamp-micros", "local-timestamp-millis", "local-timestamp-micros", "date":
			if str, ok := v.(string); ok {
				for _, layout := range []string{time.RFC3339Nano, "2006-01-02"} {
					if t, err := time.Parse(layout, str); err == nil {
						return t, nil
					}
				}
				return nil, fmt.Errorf("%q is not a timestamp or date", str)
			}
		}
		n, ok := num()
		if !ok {
			return nil, errors.New("expected a whole number")
		}
		i, err := n.Int64()
		if err != nil {
			return nil, errors.New("expected a whole number")
		}
		switch logical {
		case "timestamp-millis", "local-timestamp-millis":
			return time.UnixMilli(i).UTC(), nil
		case "timestamp-micros", "local-timestamp-micros":
			return time.UnixMicro(i).UTC(), nil
		case "date":
			return time.Unix(i*86400, 0).UTC(), nil
		case "time-millis":
			return time.Duration(i) * time.Millisecond, nil
		case "time-micros":
			return time.Duration(i) * time.Microsecond, nil
		}
		if sc.Type() == avro.Int {
			if i < math.MinInt32 || i > math.MaxInt32 {
				return nil, errors.New("too large for an int")
			}
			return int(i), nil
		}
		return i, nil
	case avro.Float:
		n, ok := num()
		if !ok {
			return nil, errors.New("expected a number")
		}
		f, err := n.Float64()
		return float32(f), err
	case avro.Double:
		n, ok := num()
		if !ok {
			return nil, errors.New("expected a number")
		}
		return n.Float64()
	}
	return nil, fmt.Errorf("unsupported type %s", sc.Type())
}
