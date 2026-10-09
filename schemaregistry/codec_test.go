package schemaregistry

import (
	"encoding/hex"
	"strings"
	"testing"
)

func hexBytes(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustCodec(t *testing.T, typ, schema string) *Codec {
	t.Helper()
	c, err := NewCodec(typ, schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// These are the bytes Confluent's own Java console producers wrote to a topic (the
// registry was this one): the codec must read what the real serializers write.
func TestDecodesWhatConfluentsSerializersWrite(t *testing.T) {
	for _, tc := range []struct {
		name, typ, schema, value, want string
	}{
		{"avro", TypeAvro, `{"type":"record","name":"U","fields":[{"name":"id","type":"long"}]}`, "000000000102", `{"id":1}`},
		{"json schema", TypeJSON, `{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}}}`,
			"00000000027b226e616d65223a22616e6e222c22616765223a33307d", `{"name":"ann","age":30}`},
		{"protobuf", TypeProtobuf, `syntax = "proto3"; message P { int64 id = 1; string name = 2; }`, "00000000030008071203626f62", `{"id":"7","name":"bob"}`},
	} {
		got, err := mustCodec(t, tc.typ, tc.schema).Decode(hexBytes(t, tc.value))
		if err != nil || got != tc.want {
			t.Errorf("%s: %q, %v; want %q", tc.name, got, err, tc.want)
		}
		if id, ok := WireID(hexBytes(t, tc.value)); !ok || id < 1 {
			t.Errorf("%s: WireID = %d, %v", tc.name, id, ok)
		}
	}
}

// What the codec writes it reads back, for every kind of Avro value, and it writes the
// same bytes Avro's own library would.
func TestAvroRoundTrip(t *testing.T) {
	schema := `{"type":"record","name":"Order","namespace":"shop","fields":[
	  {"name":"id","type":"long"},
	  {"name":"note","type":["null","string"],"default":null},
	  {"name":"qty","type":["null","int","string"]},
	  {"name":"tags","type":{"type":"array","items":"string"}},
	  {"name":"attrs","type":{"type":"map","values":"double"}},
	  {"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}},
	  {"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}},
	  {"name":"day","type":{"type":"int","logicalType":"date"}},
	  {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
	  {"name":"addr","type":{"type":"record","name":"Addr","fields":[{"name":"city","type":"string"}]}},
	  {"name":"alt","type":["null","Addr"],"default":null},
	  {"name":"key","type":{"type":"fixed","name":"Key","size":2}}
	]}`
	c := mustCodec(t, TypeAvro, schema)
	in := `{"id":42,"note":"hi","qty":7,"tags":["a","b"],"attrs":{"x":1.5},"status":"PAID","at":1700000000123,"day":"2024-03-01","price":"12.50","addr":{"city":"Pune"},"alt":{"city":"Goa"},"key":"\u0001\u0002"}`
	enc, err := c.Encode(9, in)
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := WireID(enc); !ok || id != 9 {
		t.Fatalf("header id = %d %v", id, ok)
	}
	out, err := c.Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"id":42`, `"note":"hi"`, `"qty":7`, `"tags":["a","b"]`, `"status":"PAID"`, `"city":"Pune"`, `"city":"Goa"`, `"at":"2023-11-14T22:13:20.123Z"`, `"day":"2024-03-01`, `"price":"12.5000000000"`} {
		if !strings.Contains(out, want) {
			t.Errorf("decoded %s\nmissing %s", out, want)
		}
	}
	// A string where the union offers one; a missing field with no default is named.
	if _, err := c.Encode(9, `{"id":1,"qty":"many","tags":[],"attrs":{},"status":"NEW","at":0,"day":"1970-01-01","price":"0","addr":{"city":"x"},"key":"ab"}`); err != nil {
		t.Errorf("union string branch: %v", err)
	}
	_, err = c.Encode(9, `{"id":1}`)
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Errorf("a missing field: %v, want an error naming it", err)
	}
	_, err = c.Encode(9, `{"id":"nope"}`)
	if err == nil {
		t.Error("a string for a long was accepted")
	}
}

func TestProtobufRoundTripAndNestedMessageIndex(t *testing.T) {
	schema := `syntax = "proto3"; message A { int32 x = 1; } message B { string s = 1; repeated int32 n = 2; }`
	c := mustCodec(t, TypeProtobuf, schema)
	enc, err := c.Encode(5, `{"x":3}`)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(enc[:6]) != "000000000500" {
		t.Errorf("header = %x, want magic, id 5 and the single zero that means message 0", enc[:6])
	}
	if out, err := c.Decode(enc); err != nil || out != `{"x":3}` {
		t.Errorf("round trip = %q, %v", out, err)
	}
	// Message 1 (B), as the Java serializer writes it: count 1 (zigzag 2), index 1 (zigzag 2).
	b := append([]byte{0, 0, 0, 0, 5, 2, 2}, 0x0a, 0x02, 'h', 'i', 0x12, 0x02, 0x01, 0x02)
	if out, err := c.Decode(b); err != nil || out != `{"s":"hi","n":[1,2]}` {
		t.Errorf("message 1 = %q, %v", out, err)
	}
	// An index outside the schema is an error, not a panic.
	if _, err := c.Decode([]byte{0, 0, 0, 0, 5, 2, 8}); err == nil {
		t.Error("an index outside the schema was accepted")
	}
	if _, err := c.Encode(5, `{"nope":1}`); err == nil {
		t.Error("an unknown field was accepted")
	}
}

func TestJSONSchemaEncodeValidates(t *testing.T) {
	c := mustCodec(t, TypeJSON, `{"type":"object","properties":{"age":{"type":"integer"}},"required":["age"]}`)
	if enc, err := c.Encode(2, `{ "age": 3 }`); err != nil || string(enc[5:]) != `{"age":3}` {
		t.Errorf("valid document: %q, %v", enc, err)
	}
	if _, err := c.Encode(2, `{"age":"x"}`); err == nil {
		t.Error("a document the schema rejects was encoded")
	}
	if _, err := c.Encode(2, `{`); err == nil {
		t.Error("broken JSON was encoded")
	}
}

func TestDecodeRefusesWhatIsNotWireFormat(t *testing.T) {
	c := mustCodec(t, TypeAvro, `"string"`)
	for _, b := range [][]byte{nil, {0}, {1, 0, 0, 0, 1, 2}, []byte("plain text")} {
		if _, err := c.Decode(b); err == nil {
			t.Errorf("Decode(%q) did not fail", b)
		}
		if _, ok := WireID(b); ok && len(b) < 5 {
			t.Errorf("WireID(%q) says ok", b)
		}
	}
	// Truncated Avro is an error, not a panic.
	if _, err := mustCodec(t, TypeAvro, `{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}`).Decode([]byte{0, 0, 0, 0, 1, 0xfe, 0x01}); err == nil {
		t.Error("truncated Avro decoded")
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(hexString("000000000102"), []byte{0, 0, 0, 0, 1, 2})
	f.Fuzz(func(t *testing.T, _ string, value []byte) {
		for _, c := range fuzzCodecs {
			c.Decode(value)
		}
	})
}

func hexString(s string) string { return s }

var fuzzCodecs = func() []*Codec {
	var out []*Codec
	for _, x := range []struct{ typ, s string }{
		{TypeAvro, `{"type":"record","name":"R","fields":[{"name":"a","type":["null","string"]},{"name":"b","type":{"type":"array","items":"long"}}]}`},
		{TypeProtobuf, `syntax = "proto3"; message A { int32 x = 1; repeated string s = 2; } message B { A a = 1; }`},
		{TypeJSON, `{"type":"object"}`},
	} {
		if c, err := NewCodec(x.typ, x.s, nil); err == nil {
			out = append(out, c)
		}
	}
	return out
}()
