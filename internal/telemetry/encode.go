package telemetry

import (
	"math"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

// Encode writes a payload as an OTLP ExportMetricsServiceRequest: the inverse of Decode,
// for tests and for the browser suite's seed, which both have to play a client that
// pushes telemetry. A Gauge or Sum point's Value is written as a double; a Histogram's as
// a count and a sum (Value x Count).
func Encode(p Payload) []byte {
	var scope []byte
	for _, m := range p.Metrics {
		var points []byte
		for _, pt := range m.Points {
			var dp []byte
			if m.Kind == Histogram {
				dp = protowire.AppendTag(dp, 3, protowire.Fixed64Type)
				dp = protowire.AppendFixed64(dp, uint64(pt.Time.UnixNano()))
				dp = protowire.AppendTag(dp, 4, protowire.Fixed64Type)
				dp = protowire.AppendFixed64(dp, pt.Count)
				dp = protowire.AppendTag(dp, 5, protowire.Fixed64Type)
				dp = protowire.AppendFixed64(dp, math.Float64bits(pt.Value*float64(pt.Count)))
				dp = appendAttrs(dp, 9, pt.Attrs)
			} else {
				dp = protowire.AppendTag(dp, 3, protowire.Fixed64Type)
				dp = protowire.AppendFixed64(dp, uint64(pt.Time.UnixNano()))
				dp = protowire.AppendTag(dp, 4, protowire.Fixed64Type)
				dp = protowire.AppendFixed64(dp, math.Float64bits(pt.Value))
				dp = appendAttrs(dp, 7, pt.Attrs)
			}
			points = protowire.AppendTag(points, 1, protowire.BytesType)
			points = protowire.AppendBytes(points, dp)
		}
		var data []byte
		var field protowire.Number
		switch m.Kind {
		case Sum:
			field, data = 7, points
		case Histogram:
			field, data = 9, points
		default:
			field, data = 5, points
		}
		var metric []byte
		metric = protowire.AppendTag(metric, 1, protowire.BytesType)
		metric = protowire.AppendString(metric, m.Name)
		if m.Unit != "" {
			metric = protowire.AppendTag(metric, 3, protowire.BytesType)
			metric = protowire.AppendString(metric, m.Unit)
		}
		metric = protowire.AppendTag(metric, field, protowire.BytesType)
		metric = protowire.AppendBytes(metric, data)
		scope = protowire.AppendTag(scope, 2, protowire.BytesType)
		scope = protowire.AppendBytes(scope, metric)
	}
	var res []byte
	for k, v := range p.Resource {
		res = protowire.AppendTag(res, 1, protowire.BytesType)
		res = protowire.AppendBytes(res, keyValue(k, v))
	}
	var rm []byte
	rm = protowire.AppendTag(rm, 1, protowire.BytesType)
	rm = protowire.AppendBytes(rm, res)
	rm = protowire.AppendTag(rm, 2, protowire.BytesType)
	rm = protowire.AppendBytes(rm, scope)
	var out []byte
	out = protowire.AppendTag(out, 1, protowire.BytesType)
	return protowire.AppendBytes(out, rm)
}

func appendAttrs(b []byte, field protowire.Number, attrs string) []byte {
	if attrs == "" {
		return b
	}
	for _, kv := range strings.Split(attrs, ",") {
		k, v, _ := strings.Cut(kv, "=")
		b = protowire.AppendTag(b, field, protowire.BytesType)
		b = protowire.AppendBytes(b, keyValue(k, v))
	}
	return b
}

// keyValue is KeyValue{key, AnyValue{string_value}}.
func keyValue(k, v string) []byte {
	var any []byte
	any = protowire.AppendTag(any, 1, protowire.BytesType)
	any = protowire.AppendString(any, v)
	var kv []byte
	kv = protowire.AppendTag(kv, 1, protowire.BytesType)
	kv = protowire.AppendString(kv, k)
	kv = protowire.AppendTag(kv, 2, protowire.BytesType)
	return protowire.AppendBytes(kv, any)
}
