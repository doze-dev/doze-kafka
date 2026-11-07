package console

import (
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

func changeText(ps []changePart) string {
	var out []string
	for _, p := range ps {
		out = append(out, p.Before+p.Name+p.After)
	}
	return strings.Join(out, "; ")
}

// The boards' own versions, said as the boards say them.
func TestChangeSummariesAreTheBoards(t *testing.T) {
	const (
		ordersV1   = `{"type":"record","name":"Order","namespace":"harbour.checkout","fields":[{"name":"orderId","type":"string"},{"name":"lines","type":"int"},{"name":"total","type":"double"}]}`
		ordersV2   = `{"type":"record","name":"Order","namespace":"harbour.checkout","fields":[{"name":"orderId","type":"string"},{"name":"lines","type":"int"},{"name":"total","type":"double"},{"name":"currency","type":"string","default":"EUR"}]}`
		ordersV3   = `{"type":"record","name":"Order","namespace":"harbour.checkout","fields":[{"name":"orderId","type":"string"},{"name":"lines","type":"int"},{"name":"total","type":"double"},{"name":"currency","type":"string","default":"EUR"},{"name":"coupon","type":["null","string"],"default":null}]}`
		ordersV4   = `{"type":"record","name":"Order","namespace":"harbour.checkout","fields":[{"name":"orderId","type":"string"},{"name":"lines","type":"long"},{"name":"currency","type":"string"},{"name":"coupon","type":["null","string"],"default":null},{"name":"channel","type":"string"}]}`
		dispatchV1 = "syntax = \"proto3\";\npackage harbour.depot;\n\nmessage Dispatch {\n  string order_id = 1;\n  string depot = 2;\n  int32 parcels = 3;\n}\n"
		dispatchV2 = "syntax = \"proto3\";\npackage harbour.depot;\n\nmessage Dispatch {\n  string order_id = 1;\n  string depot = 2;\n  int32 parcels = 3;\n  string carrier = 4;\n  message Inner {\n    string x = 1;\n  }\n}\nmessage Other {\n  int64 y = 1;\n}\n"
		points     = `{"$schema":"http://json-schema.org/draft-07/schema#","title":"PointsEvent","type":"object","properties":{"memberId":{"type":"string"},"points":{"type":"integer","minimum":0},"reason":{"type":"string"},"orderId":{"type":"string"}},"required":["memberId","points"],"additionalProperties":false}`
		pointsV2   = `{"title":"PointsEvent","type":"object","properties":{"memberId":{"type":"string"},"points":{"type":"integer","minimum":0},"reason":{"type":"string"},"orderId":{"type":"string"},"tier":{"type":["string","null"]}},"required":["memberId","points","reason"]}`
	)
	avro := func(s string) schemaShape { return shapeOf(schemaVersion{Schema: s}) }
	proto := func(s string) schemaShape {
		return shapeOf(schemaVersion{Schema: s, SchemaType: schemaregistry.TypeProtobuf})
	}
	js := func(s string) schemaShape {
		return shapeOf(schemaVersion{Schema: s, SchemaType: schemaregistry.TypeJSON})
	}

	for _, c := range []struct{ got, want string }{
		{firstVersionSummary(avro(ordersV1)), "first version: orderId, lines, total"},
		{changeText(changeSummary("AVRO", avro(ordersV1), avro(ordersV2))), "added currency with default EUR"},
		{changeText(changeSummary("AVRO", avro(ordersV2), avro(ordersV3))), "added optional coupon (null or string, default null)"},
		{changeText(changeSummary("AVRO", avro(ordersV3), avro(ordersV4))), "changed lines from int to long; dropped the default of currency; added channel, no default; removed total"},
		{firstVersionSummary(proto(dispatchV1)), "first version: order_id, depot, parcels"},
		{changeText(changeSummary("PROTOBUF", proto(dispatchV1), proto(dispatchV2))), "added carrier = 4"},
		{changeText(changeSummary("PROTOBUF", proto(dispatchV2), proto(dispatchV1))), "removed carrier = 4"},
		{firstVersionSummary(js(points)), "first version: memberId, points, reason, orderId"},
		{changeText(changeSummary("JSON", js(points), js(pointsV2))), "made reason required; added optional tier (string or null)"},
		{shapeLine(avro(`{"type":"record","name":"AuditEvent","fields":[{"name":"actor","type":"string"},{"name":"action","type":"string"},{"name":"at","type":"long"}]}`)), "AuditEvent: actor string, action string, at long"},
		{firstVersionSummary(avro(`{"type":"record","name":"W","fields":[{"name":"a","type":"int"},{"name":"b","type":"int"},{"name":"c","type":"int"},{"name":"d","type":"int"},{"name":"e","type":"int"},{"name":"f","type":"int"}]}`)), "first version: a, b, c, d and 2 more"},
		{firstVersionSummary(avro(`"string"`)), "first version"},
		{avroTypeWords([]byte(`{"type":"array","items":{"type":"map","values":"long"}}`)), "array of map of long"},
		{avroTypeWords([]byte(`{"type":"int","logicalType":"date"}`)), "date"},
		{avroTypeWords([]byte(`{"type":"record","name":"Line","fields":[]}`)), "Line"},
	} {
		if c.got != c.want {
			t.Errorf("got  %q\nwant %q", c.got, c.want)
		}
	}

	// Protobuf: the message types, and the first one's fields only.
	p := proto(dispatchV2)
	if strings.Join(p.Messages, ",") != "Dispatch,Other" || len(p.Fields) != 4 || p.Record != "Dispatch" {
		t.Errorf("proto shape %+v", p)
	}
	// JSON Schema: required of how many, closed, the draft.
	j := js(points)
	if j.Required != 2 || len(j.Fields) != 4 || !j.Closed || j.Draft != "draft-07" || j.Fields[1].Type != "integer, min 0" {
		t.Errorf("json shape %+v", j)
	}
	// A schema that cannot be read changes nothing it can name.
	if changeSummary("AVRO", avro(`"string"`), avro(ordersV1)) != nil || changeSummary("JSON", js(`{}`), js(points)) != nil {
		t.Error("an unreadable schema was summarised")
	}
	if shapeLine(avro(`"string"`)) != "" {
		t.Error("an unreadable schema has no line")
	}
}
