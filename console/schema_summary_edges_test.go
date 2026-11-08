package console

import (
	"testing"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

func summaryField(t *testing.T, s schemaShape, name string) schemaField {
	t.Helper()
	for _, f := range s.Fields {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("no field %s in %+v", name, s.Fields)
	return schemaField{}
}

func TestAnAvroFieldIsOptionalWhenItsUnionHasNull(t *testing.T) {
	s := avroShape(`{"type":"record","name":"R","fields":[
		{"name":"a","type":["null","string"]},
		{"name":"b","type":["string","int"]},
		{"name":"c","type":["string","null"]},
		{"name":"d","type":["null"]},
		{"name":"e","type":"string"}]}`)
	for name, want := range map[string]bool{"a": true, "b": false, "c": true, "d": true, "e": false} {
		if got := summaryField(t, s, name).Optional; got != want {
			t.Errorf("field %s optional = %v, want %v", name, got, want)
		}
	}
}

func TestAProtobufFieldKeepsItsRepeatedAndOptionalMarks(t *testing.T) {
	s := protoShape("syntax = \"proto3\";\nmessage M {\n  repeated string tags = 1;\n  optional int32 n = 2;\n  string s = 3;\n  .pkg.Line line = 4;\n}\n")
	if !s.ok || s.Record != "M" || len(s.Fields) != 4 {
		t.Fatalf("%+v", s)
	}
	for name, want := range map[string]struct {
		typ string
		opt bool
	}{"tags": {"repeated string", false}, "n": {"int32", true}, "s": {"string", false}, "line": {"pkg.Line", false}} {
		if f := summaryField(t, s, name); f.Type != want.typ || f.Optional != want.opt {
			t.Errorf("field %s = %q optional %v, want %q optional %v", name, f.Type, f.Optional, want.typ, want.opt)
		}
	}
	// Text with no message in it is not a shape.
	if protoShape("syntax = \"proto3\";\npackage p;\n").ok || protoShape("").ok {
		t.Error("a .proto with no message was read as a shape")
	}
	if !protoShape("message A {\n}\n").ok {
		t.Error("a .proto with one empty message was not read")
	}
	// A stray closing brace does not push the nesting below the top.
	if s := protoShape("}\nmessage A {\n  string x = 1;\n}\n"); len(s.Messages) != 1 || len(s.Fields) != 1 {
		t.Errorf("a stray brace: %+v", s)
	}
}

func TestAJSONSchemaPropertyKeepsItsRefAndItsDefault(t *testing.T) {
	s := jsonSchemaShape(`{"type":"object","properties":{
		"a":{"$ref":"#/$defs/Money"},
		"b":{"type":"string","$ref":"#/$defs/Ignored"},
		"c":{"type":"integer","default":0},
		"d":{"type":"string"},
		"e":{"type":["string","null"],"default":"x"}},"required":["a"]}`)
	for name, want := range map[string]struct {
		typ, def string
		has      bool
	}{
		"a": {"#/$defs/Money", "", false},
		"b": {"string", "", false},
		"c": {"integer", "0", true},
		"d": {"string", "", false},
		"e": {"string or null", "x", true},
	} {
		if f := summaryField(t, s, name); f.Type != want.typ || f.Default != want.def || f.HasDefault != want.has {
			t.Errorf("property %s = %+v, want type %q default %q (%v)", name, f, want.typ, want.def, want.has)
		}
	}
}

func TestTheChangeSummaryNamesWhatItSetAndWhatItRequired(t *testing.T) {
	avro := func(s string) schemaShape { return avroShape(s) }
	got := changeText(changeSummary("AVRO",
		avro(`{"type":"record","name":"R","fields":[{"name":"currency","type":"string","default":"EUR"}]}`),
		avro(`{"type":"record","name":"R","fields":[{"name":"currency","type":"string","default":"USD"}]}`)))
	if got != "set the default of currency to USD" {
		t.Errorf("a changed default: %q", got)
	}
	js := func(s string) schemaShape { return jsonSchemaShape(s) }
	got = changeText(changeSummary(schemaregistry.TypeJSON,
		js(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`),
		js(`{"type":"object","properties":{"id":{"type":"string"},"tier":{"type":"integer"}},"required":["id","tier"]}`)))
	if got != "added required tier (integer)" {
		t.Errorf("a required property added: %q", got)
	}
}
