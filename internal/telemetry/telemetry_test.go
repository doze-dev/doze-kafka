package telemetry

import (
	"errors"
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

var t0 = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

func samplePayload() Payload {
	return Payload{
		Resource: map[string]string{"client_rack": "r1", "service.name": "billing"},
		Metrics: []Metric{
			{Name: "org.apache.kafka.producer.node.request.latency.avg", Unit: "ms", Kind: Gauge, Points: []Point{
				{Attrs: "node_id=1", Time: t0, Value: 12.5},
				{Attrs: "node_id=2", Time: t0, Value: 3},
			}},
			{Name: "org.apache.kafka.producer.connection.creation.total", Kind: Sum, Points: []Point{{Time: t0, Value: 7}}},
			{Name: "org.apache.kafka.consumer.coordinator.commit.latency", Unit: "ms", Kind: Histogram, Points: []Point{{Time: t0, Value: 4, Count: 10}}},
		},
	}
}

func TestDecodeReadsWhatEncodeWrites(t *testing.T) {
	want := samplePayload()
	got, err := Decode(Encode(want))
	if err != nil {
		t.Fatal(err)
	}
	if got.Resource["client_rack"] != "r1" || got.Resource["service.name"] != "billing" {
		t.Errorf("resource = %v", got.Resource)
	}
	if len(got.Metrics) != 3 {
		t.Fatalf("%d metrics, want 3: %+v", len(got.Metrics), got.Metrics)
	}
	lat := got.Metrics[0]
	if lat.Name != want.Metrics[0].Name || lat.Unit != "ms" || lat.Kind != Gauge || len(lat.Points) != 2 {
		t.Fatalf("latency = %+v", lat)
	}
	if lat.Points[0].Attrs != "node_id=1" || lat.Points[0].Value != 12.5 || !lat.Points[0].Time.Equal(t0) {
		t.Errorf("first latency point = %+v", lat.Points[0])
	}
	if sum := got.Metrics[1]; sum.Kind != Sum || sum.Points[0].Value != 7 {
		t.Errorf("sum = %+v", sum)
	}
	h := got.Metrics[2]
	if h.Kind != Histogram || h.Points[0].Count != 10 || math.Abs(h.Points[0].Value-4) > 1e-9 {
		t.Errorf("histogram = %+v, want count 10 and mean 4", h)
	}
}

// Attributes are order-independent: the same set written two ways is one series.
func TestAttributesAreSorted(t *testing.T) {
	p := Payload{Metrics: []Metric{{Name: "m", Kind: Gauge, Points: []Point{{Attrs: "topic=orders,node_id=1", Time: t0, Value: 1}}}}}
	got, err := Decode(Encode(p))
	if err != nil {
		t.Fatal(err)
	}
	if a := got.Metrics[0].Points[0].Attrs; a != "node_id=1,topic=orders" {
		t.Errorf("attrs = %q", a)
	}
}

// An integer data point is a gauge too (sfixed64, field 6), and an integer attribute
// reads as its number.
func TestIntegerPointsAndAttributes(t *testing.T) {
	var dp []byte
	dp = protowire.AppendTag(dp, 3, protowire.Fixed64Type)
	dp = protowire.AppendFixed64(dp, uint64(t0.UnixNano()))
	dp = protowire.AppendTag(dp, 6, protowire.Fixed64Type)
	minus42 := int64(-42)
	dp = protowire.AppendFixed64(dp, uint64(minus42))
	var intVal []byte
	intVal = protowire.AppendTag(intVal, 3, protowire.VarintType)
	intVal = protowire.AppendVarint(intVal, 5)
	var kv []byte
	kv = protowire.AppendTag(kv, 1, protowire.BytesType)
	kv = protowire.AppendString(kv, "partition")
	kv = protowire.AppendTag(kv, 2, protowire.BytesType)
	kv = protowire.AppendBytes(kv, intVal)
	dp = protowire.AppendTag(dp, 7, protowire.BytesType)
	dp = protowire.AppendBytes(dp, kv)
	pt, err := decodeNumberPoint(dp)
	if err != nil {
		t.Fatal(err)
	}
	if pt.Value != -42 || pt.Attrs != "partition=5" || !pt.Time.Equal(t0) {
		t.Errorf("point = %+v", pt)
	}
}

func TestMalformedPayloadsAreRefusedNotTrusted(t *testing.T) {
	good := Encode(samplePayload())
	for name, in := range map[string][]byte{
		"truncated":              good[:len(good)-5],
		"a length that overruns": {0x0a, 0xff, 0x7f, 1, 2, 3},
		"a bad tag":              {0x00},
		"a group":                {0x0b, 0x0c},
	} {
		if _, err := Decode(in); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
	// Nothing at all is a valid, empty payload.
	if p, err := Decode(nil); err != nil || len(p.Metrics) != 0 {
		t.Errorf("empty = %+v %v", p, err)
	}
	// Unknown fields are skipped, not refused: a newer client may send more.
	extra := protowire.AppendTag(nil, 99, protowire.VarintType)
	extra = protowire.AppendVarint(extra, 1)
	if _, err := Decode(append(extra, good...)); err != nil {
		t.Errorf("an unknown field broke decoding: %v", err)
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(Encode(samplePayload()))
	f.Add([]byte{})
	f.Add([]byte{0x0a, 0x00})
	full := Encode(samplePayload())
	for n := 1; n < len(full); n += 3 {
		f.Add(full[:n]) // the truncation corpus: every cut is a boundary the decoder has to refuse or read.
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = Decode(b) // must not panic or hang, whatever the bytes.
	})
}

// ---- the store ----

func id(n byte) [16]byte { return [16]byte{n} }

func TestStoreKeepsASeriesPerMetricAndAttributesAndBoundsIt(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	who := Who{ClientID: "billing", Principal: "User:ANONYMOUS", Host: "/127.0.0.1"}
	for i := 0; i < samplesKept+30; i++ {
		now = t0.Add(time.Duration(i) * 5 * time.Second)
		s.Push(id(1), who, Payload{Metrics: []Metric{{Name: "m", Kind: Gauge, Points: []Point{
			{Attrs: "node_id=1", Time: now, Value: float64(i)}, {Attrs: "node_id=2", Time: now, Value: 100},
		}}}}, false)
	}
	got := s.Instances()
	if len(got) != 1 || got[0].ClientID != "billing" || got[0].Pushes != samplesKept+30 {
		t.Fatalf("instances = %+v", got)
	}
	if len(got[0].Series) != 2 {
		t.Fatalf("%d series, want 2 (one per node)", len(got[0].Series))
	}
	one := got[0].Series[0]
	if one.Attrs != "node_id=1" || len(one.Samples) != samplesKept || one.Last() != float64(samplesKept+29) || one.Samples[0].V != 30 {
		t.Errorf("series = %d samples, first %v last %v; want the newest %d", len(one.Samples), one.Samples[0].V, one.Last(), samplesKept)
	}
}

func TestStoreForgetsTheSilentAndTheTerminated(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	s.Push(id(1), Who{ClientID: "quiet"}, Payload{}, false)
	s.Push(id(2), Who{ClientID: "leaving"}, Payload{}, true)
	s.Push(id(3), Who{ClientID: "busy"}, Payload{}, false)
	now = t0.Add(2 * time.Minute)
	s.Push(id(3), Who{}, Payload{}, false)
	var names []string
	for _, in := range s.Instances() {
		names = append(names, in.ClientID)
	}
	if len(names) != 2 {
		t.Fatalf("after two minutes: %v, want quiet and busy (leaving said it was terminating)", names)
	}
	now = t0.Add(20 * time.Minute)
	if got := s.Instances(); len(got) != 0 {
		t.Errorf("after twenty minutes: %d instances, want none", len(got))
	}
}

func TestStoreIsBoundedAndNewestFirst(t *testing.T) {
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	for i := 0; i < maxInstances+10; i++ {
		now = t0.Add(time.Duration(i) * time.Second)
		s.Touch([16]byte{byte(i), byte(i >> 8)}, Who{ClientID: "c"})
	}
	got := s.Instances()
	if len(got) != maxInstances {
		t.Fatalf("%d instances, want %d", len(got), maxInstances)
	}
	if !got[0].Last.After(got[len(got)-1].Last) {
		t.Error("not most recent first")
	}
	if s.Known([16]byte{0, 0}) || !s.Known([16]byte{byte(maxInstances + 9), byte((maxInstances + 9) >> 8)}) {
		t.Error("the oldest was not the one dropped")
	}
}

func TestNewIDIsAVersion4UUID(t *testing.T) {
	a, b := NewID(), NewID()
	if a == b {
		t.Error("two ids are the same")
	}
	if a[6]>>4 != 4 || a[8]>>6 != 2 {
		t.Errorf("not a v4 UUID: % x", a)
	}
}

func TestShortName(t *testing.T) {
	if got := ShortName("org.apache.kafka.producer.node.request.latency.avg"); got != "producer.node.request.latency.avg" {
		t.Error(got)
	}
	if got := ShortName("custom.metric"); got != "custom.metric" {
		t.Error(got)
	}
}
