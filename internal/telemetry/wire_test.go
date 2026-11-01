package telemetry

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

func bytesField(num protowire.Number, b []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, num, protowire.BytesType), b)
}

func cat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func fixed64(num protowire.Number, v uint64) []byte {
	return protowire.AppendFixed64(protowire.AppendTag(nil, num, protowire.Fixed64Type), v)
}

func varint(num protowire.Number, v uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(nil, num, protowire.VarintType), v)
}

func kv(key string, value []byte) []byte {
	return cat(bytesField(1, []byte(key)), bytesField(2, value))
}

// A payload using what Encode does not write: a sum, a histogram, every kind of
// attribute value, and a resource.
func TestDecodeReadsSumsHistogramsAndEveryAttributeKind(t *testing.T) {
	attrs := cat(
		bytesField(7, kv("s", bytesField(1, []byte("text")))),
		bytesField(7, kv("b", cat(varint(2, 1)))),
		bytesField(7, kv("i", cat(varint(3, 42)))),
		bytesField(7, kv("d", fixed64(4, math.Float64bits(1.5)))),
	)
	sumPoint := cat(fixed64(3, uint64(t0.UnixNano())), fixed64(4, math.Float64bits(10)), attrs)
	sum := bytesField(7, bytesField(1, sumPoint))
	histPoint := cat(fixed64(3, uint64(t0.UnixNano())), fixed64(4, 4), fixed64(5, math.Float64bits(10)),
		bytesField(9, kv("k", bytesField(1, []byte("v")))))
	hist := bytesField(9, bytesField(1, histPoint))
	emptyHist := bytesField(9, bytesField(1, fixed64(3, uint64(t0.UnixNano()))))
	metric := func(name string, data []byte) []byte {
		return bytesField(2, cat(bytesField(1, []byte(name)), bytesField(3, []byte("ms")), data))
	}
	res := bytesField(1, bytesField(1, kv("service.name", bytesField(1, []byte("billing")))))
	scope := bytesField(2, cat(metric("a.sum", sum), metric("a.hist", hist), metric("a.empty", emptyHist), bytesField(2, bytesField(3, []byte("no name")))))
	// A Fixed32 field and a varint where bytes are expected are skipped, not trusted.
	noise := cat(protowire.AppendTag(nil, 9, protowire.Fixed32Type), protowire.AppendFixed32(nil, 7), varint(1, 3))
	p, err := Decode(bytesField(1, cat(res, scope, noise)))
	if err != nil {
		t.Fatal(err)
	}
	if p.Resource["service.name"] != "billing" || len(p.Metrics) != 3 {
		t.Fatalf("payload = %+v", p)
	}
	byName := map[string]Metric{}
	for _, m := range p.Metrics {
		byName[m.Name] = m
	}
	if m := byName["a.sum"]; m.Kind != Sum || m.Unit != "ms" || m.Points[0].Value != 10 || m.Points[0].Attrs != "b=true,d=1.5,i=42,s=text" {
		t.Errorf("sum = %+v", m)
	}
	if m := byName["a.hist"]; m.Kind != Histogram || m.Points[0].Value != 2.5 || m.Points[0].Count != 4 || m.Points[0].Attrs != "k=v" {
		t.Errorf("histogram = %+v", m)
	}
	if m := byName["a.empty"]; m.Points[0].Value != 0 {
		t.Errorf("a histogram with no count = %+v", m)
	}
}

// Every way a message can be cut or wrong at the wire level is an ErrMalformed, at every
// depth the decoder goes to.
func TestDecodeRefusesWhatIsCutOrWrongAtEveryDepth(t *testing.T) {
	cutKV := bytesField(7, kv("k", bytesField(1, []byte("v"))))
	for name, in := range map[string][]byte{
		"a tag cut short":            {0x80},
		"a varint cut short":         {0x08, 0x80},
		"a fixed64 cut short":        cat(protowire.AppendTag(nil, 1, protowire.Fixed64Type), []byte{1, 2}),
		"a fixed32 cut short":        cat(protowire.AppendTag(nil, 1, protowire.Fixed32Type), []byte{1}),
		"bytes longer than the rest": cat(protowire.AppendTag(nil, 1, protowire.BytesType), []byte{9, 1}),
		"a group":                    protowire.AppendTag(nil, 1, protowire.StartGroupType),
		"a resource attribute cut":   bytesField(1, bytesField(1, bytesField(1, cat(protowire.AppendTag(nil, 1, protowire.BytesType), []byte{9})))),
		"a scope cut":                bytesField(1, bytesField(2, []byte{0x80})),
		"a metric cut":               bytesField(1, bytesField(2, bytesField(2, []byte{0x80}))),
		"a gauge cut":                bytesField(1, bytesField(2, bytesField(2, bytesField(5, []byte{0x80})))),
		"a sum cut":                  bytesField(1, bytesField(2, bytesField(2, bytesField(7, []byte{0x80})))),
		"a histogram cut":            bytesField(1, bytesField(2, bytesField(2, bytesField(9, []byte{0x80})))),
		"a number point cut":         bytesField(1, bytesField(2, bytesField(2, bytesField(5, bytesField(1, []byte{0x80}))))),
		"a histogram point cut":      bytesField(1, bytesField(2, bytesField(2, bytesField(9, bytesField(1, []byte{0x80}))))),
		"a point attribute cut":      bytesField(1, bytesField(2, bytesField(2, bytesField(5, bytesField(1, bytesField(7, cat(bytesField(1, []byte("k")), []byte{0x12, 9}))))))),
		"a histogram attribute cut":  bytesField(1, bytesField(2, bytesField(2, bytesField(9, bytesField(1, bytesField(9, cat(bytesField(1, []byte("k")), []byte{0x12, 9}))))))),
		"an any-value cut":           bytesField(1, bytesField(1, bytesField(1, kv("k", []byte{0x08, 0x80})))),
		"a cut attribute value":      bytesField(1, bytesField(2, bytesField(2, bytesField(5, bytesField(1, cutKV[:len(cutKV)-1]))))),
	} {
		if _, err := Decode(in); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: error %v, want ErrMalformed", name, err)
		}
	}
}

func TestSeriesLastAndPushInterval(t *testing.T) {
	if (Series{}).Last() != 0 {
		t.Error("an empty series has a last value")
	}
	s := NewStore()
	s.SetPushInterval(time.Second)
	if s.interval != time.Second {
		t.Error("the interval was not set")
	}
}

func TestClipTextKeepsWholeCharacters(t *testing.T) {
	got := clipText(strings.Repeat("é", 10), 5) // 5 is in the middle of the third character.
	if got != "éé" {
		t.Errorf("clipped to %q, want two whole characters", got)
	}
	if clipText("short", 10) != "short" {
		t.Error("a short string was cut")
	}
}

// The truncation corpus: a valid payload cut at every byte is a seed, so the fuzzer
// starts from the boundaries (TRF-24).
func TestEveryTruncationOfAValidPayloadIsHandled(t *testing.T) {
	full := Encode(samplePayload())
	for n := 0; n <= len(full); n++ {
		p, err := Decode(full[:n])
		if err != nil && !errors.Is(err, ErrMalformed) {
			t.Fatalf("cut at %d: %v", n, err)
		}
		_ = p
	}
}
