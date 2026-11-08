package console

import "testing"

// The boards' own printings (Sc-05, Sc-08).
func TestPrintSchemaIsTheBoards(t *testing.T) {
	avro := `{"type":"record","name":"Order","namespace":"harbour.checkout","fields":[{"name":"orderId","type":"string"},{"name":"lines","type":"int"},{"name":"total","type":"double"},{"name":"currency","type":"string","default":"EUR"},{"name":"coupon","type":["null","string"],"default":null}]}`
	want := `{"type": "record", "name": "Order", "namespace": "harbour.checkout", "fields": [
  {"name": "orderId",  "type": "string"},
  {"name": "lines",    "type": "int"},
  {"name": "total",    "type": "double"},
  {"name": "currency", "type": "string", "default": "EUR"},
  {"name": "coupon",   "type": ["null", "string"], "default": null}
]}`
	if got, ok := printSchema("AVRO", avro); !ok || got != want {
		t.Errorf("avro:\n%s\nwant\n%s", got, want)
	}
	js := `{"$schema":"http://json-schema.org/draft-07/schema#","title":"PointsEvent","type":"object","properties":{"memberId":{"type":"string"},"points":{"type":"integer","minimum":0},"reason":{"type":"string"},"orderId":{"type":"string"}},"required":["memberId","points"],"additionalProperties":false}`
	want = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "PointsEvent",
  "type": "object",
  "properties": {
    "memberId": {"type": "string"},
    "points":   {"type": "integer", "minimum": 0},
    "reason":   {"type": "string"},
    "orderId":  {"type": "string"}
  },
  "required": ["memberId", "points"],
  "additionalProperties": false
}`
	if got, ok := printSchema("JSON", js); !ok || got != want {
		t.Errorf("json schema:\n%s\nwant\n%s", got, want)
	}
	// Strings keep their own commas and colons; other shapes are not printed this way.
	if got := spacedJSON([]byte(`{"a":"x, y: z\"q","b":[1,2]}`)); got != `{"a": "x, y: z\"q", "b": [1, 2]}` {
		t.Errorf("spaced: %s", got)
	}
	for _, c := range []struct{ typ, text string }{{"AVRO", `"string"`}, {"AVRO", `{"type":"enum","name":"E","symbols":["A"]}`}, {"JSON", `{"type":"string"}`}, {"AVRO", `{nope`}, {"AVRO", `{"type":"record","fields":{}}`}} {
		if _, ok := printSchema(c.typ, c.text); ok {
			t.Errorf("%s %s should not print as a record", c.typ, c.text)
		}
	}
	// A field that is not an object is kept as it is.
	if got, ok := printSchema("AVRO", `{"type":"record","name":"R","fields":["odd",{"name":"a","type":"int"}]}`); !ok || got != "{\"type\": \"record\", \"name\": \"R\", \"fields\": [\n  \"odd\",\n  {\"name\": \"a\", \"type\": \"int\"}\n]}" {
		t.Errorf("odd field:\n%s", got)
	}
}
