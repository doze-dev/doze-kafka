// Package telemetry receives what clients say about themselves.
//
// KIP-714 lets a Kafka client push its own metrics to the broker it is connected to,
// as an OpenTelemetry metrics payload (OTLP, protobuf, optionally compressed). The broker
// hands out a subscription, the client pushes on the interval it was given, and this
// package decodes the payload and keeps the last few minutes of each client instance's
// series in memory, for the console to draw. Nothing is written to disk and nothing is
// exported anywhere: these are metrics a client reports about itself (request latency,
// poll idle ratio, connection counts), not traces, and the console labels them so.
//
// OTLP is decoded by hand against the wire format rather than through generated code:
// only a handful of fields of ExportMetricsServiceRequest matter here, and the schema's
// generated package is a large dependency for them.
package telemetry

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// Kind is the shape of a metric.
type Kind string

const (
	Gauge     Kind = "gauge"
	Sum       Kind = "sum"
	Histogram Kind = "histogram"
)

// Point is one data point of a metric: a value at a moment, for one set of attributes.
type Point struct {
	// Attrs are the point's attributes as sorted "key=value" pairs joined by commas
	// ("node.id=1,topic=orders"), or "" for none. Two points with the same metric name
	// and Attrs belong to one series.
	Attrs string
	Time  time.Time
	// Value is the gauge or sum's value, or a histogram's mean (sum / count).
	Value float64
	// Count is a histogram's sample count.
	Count uint64
}

// Metric is one metric of a payload.
type Metric struct {
	Name   string
	Unit   string
	Kind   Kind
	Points []Point
}

// Payload is what one push said.
type Payload struct {
	// Resource are the attributes the client describes itself with (client_rack,
	// client_id, ... whatever it chose to send).
	Resource map[string]string
	Metrics  []Metric
}

// ErrMalformed is returned for a payload that is not valid OTLP metrics.
var ErrMalformed = errors.New("telemetry: not a valid OTLP metrics payload")

// Decode reads an ExportMetricsServiceRequest.
func Decode(b []byte) (Payload, error) {
	p := Payload{Resource: map[string]string{}}
	err := eachField(b, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) error {
		if num != 1 || typ != protowire.BytesType { // resource_metrics
			return nil
		}
		return decodeResourceMetrics(v, &p)
	})
	if err != nil {
		return Payload{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return p, nil
}

// eachField walks the fields of one message, handing each to fn: its number, its wire
// type, its bytes (for the length-delimited ones) and its value (for varint and the
// fixed-width ones).
func eachField(b []byte, fn func(num protowire.Number, typ protowire.Type, bytes []byte, val uint64) error) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		var (
			bytes []byte
			val   uint64
		)
		switch typ {
		case protowire.VarintType:
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return protowire.ParseError(m)
			}
			val, n = v, m
		case protowire.Fixed64Type:
			v, m := protowire.ConsumeFixed64(b)
			if m < 0 {
				return protowire.ParseError(m)
			}
			val, n = v, m
		case protowire.Fixed32Type:
			v, m := protowire.ConsumeFixed32(b)
			if m < 0 {
				return protowire.ParseError(m)
			}
			val, n = uint64(v), m
		case protowire.BytesType:
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return protowire.ParseError(m)
			}
			bytes, n = v, m
		default: // groups are not used by OTLP.
			return fmt.Errorf("field %d has wire type %d", num, typ)
		}
		b = b[n:]
		if err := fn(num, typ, bytes, val); err != nil {
			return err
		}
	}
	return nil
}

// ResourceMetrics { Resource resource = 1; repeated ScopeMetrics scope_metrics = 2; }
func decodeResourceMetrics(b []byte, out *Payload) error {
	return eachField(b, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) error {
		if typ != protowire.BytesType {
			return nil
		}
		switch num {
		case 1:
			return eachField(v, func(num protowire.Number, typ protowire.Type, kv []byte, _ uint64) error {
				if num == 1 && typ == protowire.BytesType { // Resource.attributes
					if k, val, ok := decodeKeyValue(kv); ok {
						out.Resource[k] = val
					}
				}
				return nil
			})
		case 2:
			return decodeScopeMetrics(v, out)
		}
		return nil
	})
}

// ScopeMetrics { InstrumentationScope scope = 1; repeated Metric metrics = 2; }
func decodeScopeMetrics(b []byte, out *Payload) error {
	return eachField(b, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) error {
		if num == 2 && typ == protowire.BytesType {
			m, err := decodeMetric(v)
			if err != nil {
				return err
			}
			if m.Name != "" {
				out.Metrics = append(out.Metrics, m)
			}
		}
		return nil
	})
}

// Metric { string name = 1; string description = 2; string unit = 3;
// oneof data { Gauge gauge = 5; Sum sum = 7; Histogram histogram = 9; ... } }
func decodeMetric(b []byte) (Metric, error) {
	var m Metric
	err := eachField(b, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) error {
		if typ != protowire.BytesType {
			return nil
		}
		switch num {
		case 1:
			m.Name = string(v)
		case 3:
			m.Unit = string(v)
		case 5: // Gauge { repeated NumberDataPoint data_points = 1; }
			m.Kind = Gauge
			return eachField(v, func(n protowire.Number, t protowire.Type, dp []byte, _ uint64) error {
				if n == 1 && t == protowire.BytesType {
					m.Points = append(m.Points, decodeNumberPoint(dp))
				}
				return nil
			})
		case 7: // Sum { repeated NumberDataPoint data_points = 1; temporality = 2; is_monotonic = 3; }
			m.Kind = Sum
			return eachField(v, func(n protowire.Number, t protowire.Type, dp []byte, _ uint64) error {
				if n == 1 && t == protowire.BytesType {
					m.Points = append(m.Points, decodeNumberPoint(dp))
				}
				return nil
			})
		case 9: // Histogram { repeated HistogramDataPoint data_points = 1; temporality = 2; }
			m.Kind = Histogram
			return eachField(v, func(n protowire.Number, t protowire.Type, dp []byte, _ uint64) error {
				if n == 1 && t == protowire.BytesType {
					m.Points = append(m.Points, decodeHistogramPoint(dp))
				}
				return nil
			})
		}
		return nil
	})
	return m, err
}

// NumberDataPoint { repeated KeyValue attributes = 7; fixed64 time_unix_nano = 3;
// oneof value { double as_double = 4; sfixed64 as_int = 6; } }
func decodeNumberPoint(b []byte) Point {
	var p Point
	var attrs []string
	_ = eachField(b, func(num protowire.Number, typ protowire.Type, v []byte, val uint64) error {
		switch {
		case num == 3 && typ == protowire.Fixed64Type:
			p.Time = time.Unix(0, int64(val))
		case num == 4 && typ == protowire.Fixed64Type:
			p.Value = math.Float64frombits(val)
		case num == 6 && typ == protowire.Fixed64Type:
			p.Value = float64(int64(val))
		case num == 7 && typ == protowire.BytesType:
			if k, s, ok := decodeKeyValue(v); ok {
				attrs = append(attrs, k+"="+s)
			}
		}
		return nil
	})
	p.Attrs = joinAttrs(attrs)
	return p
}

// HistogramDataPoint { repeated KeyValue attributes = 9; fixed64 time_unix_nano = 3;
// fixed64 count = 4; optional double sum = 5; }
func decodeHistogramPoint(b []byte) Point {
	var p Point
	var attrs []string
	var sum float64
	_ = eachField(b, func(num protowire.Number, typ protowire.Type, v []byte, val uint64) error {
		switch {
		case num == 3 && typ == protowire.Fixed64Type:
			p.Time = time.Unix(0, int64(val))
		case num == 4 && typ == protowire.Fixed64Type:
			p.Count = val
		case num == 5 && typ == protowire.Fixed64Type:
			sum = math.Float64frombits(val)
		case num == 9 && typ == protowire.BytesType:
			if k, s, ok := decodeKeyValue(v); ok {
				attrs = append(attrs, k+"="+s)
			}
		}
		return nil
	})
	if p.Count > 0 {
		p.Value = sum / float64(p.Count)
	}
	p.Attrs = joinAttrs(attrs)
	return p
}

func joinAttrs(a []string) string {
	sort.Strings(a)
	return strings.Join(a, ",")
}

// KeyValue { string key = 1; AnyValue value = 2; }
// AnyValue { oneof value { string string_value = 1; bool bool_value = 2; int64 int_value = 3;
// double double_value = 4; ... } }
func decodeKeyValue(b []byte) (key, val string, ok bool) {
	_ = eachField(b, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) error {
		switch {
		case num == 1 && typ == protowire.BytesType:
			key = string(v)
		case num == 2 && typ == protowire.BytesType:
			_ = eachField(v, func(n protowire.Number, t protowire.Type, s []byte, x uint64) error {
				switch {
				case n == 1 && t == protowire.BytesType:
					val = string(s)
				case n == 2 && t == protowire.VarintType:
					val = strconv.FormatBool(x != 0)
				case n == 3 && t == protowire.VarintType:
					val = strconv.FormatInt(int64(x), 10)
				case n == 4 && t == protowire.Fixed64Type:
					val = strconv.FormatFloat(math.Float64frombits(x), 'g', -1, 64)
				}
				return nil
			})
		}
		return nil
	})
	return key, val, key != ""
}
