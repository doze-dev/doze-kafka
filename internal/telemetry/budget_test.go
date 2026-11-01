package telemetry

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func heapNow() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// The store used to hold up to 300 MB (200 instances x 400 series x 120 samples x 32
// bytes) and copy all of it on every poll (TRF-6). Filled to every cap it now holds
// under 32 MB, estimated and measured.
func TestStoreWorstCaseIsUnderBudget(t *testing.T) {
	if got := WorstCaseBytes(); got > 32<<20 {
		t.Errorf("the estimate of the worst case is %d bytes, want at most 32 MiB", got)
	}
	s := NewStore()
	now := t0
	s.now = func() time.Time { return now }
	before := heapNow()
	for i := 0; i < maxInstances; i++ {
		var pts []Point
		for j := 0; j < maxSeriesEach; j++ {
			pts = append(pts, Point{Attrs: fmt.Sprintf("node_id=%d,%s", j, string(make([]byte, 0))), Time: now})
		}
		for k := 0; k < samplesKept+5; k++ {
			now = t0.Add(time.Duration(k) * 5 * time.Second)
			for j := range pts {
				pts[j].Time, pts[j].Value = now, float64(k)
			}
			s.Push(id2(i), Who{ClientID: "c"}, Payload{Metrics: []Metric{{
				Name: "org.apache.kafka.producer.node.request.latency.avg", Unit: "ms", Kind: Gauge, Points: pts}}}, false)
		}
	}
	got := int64(heapNow()) - int64(before)
	if got > 32<<20 {
		t.Errorf("a full store holds %d bytes, want at most 32 MiB", got)
	}
	if n := len(s.Summaries()); n != maxInstances {
		t.Fatalf("%d instances, want %d", n, maxInstances)
	}
	for _, in := range s.Summaries() {
		if in.Series != nil || in.Metrics != maxSeriesEach {
			t.Fatalf("a summary carries %d series and says %d metrics", len(in.Series), in.Metrics)
		}
	}
}

func id2(i int) [16]byte { return [16]byte{byte(i), byte(i >> 8), 1} }

func TestInstanceReadsOneWithItsSeries(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0 }
	s.Push(id(1), Who{ClientID: "a", Conn: 7}, Payload{Metrics: []Metric{{Name: "m", Kind: Gauge, Points: []Point{{Time: t0, Value: 3}}}}}, false)
	s.Push(id(2), Who{ClientID: "b", Conn: 8}, Payload{}, false)
	in, ok := s.Instance(id(1))
	if !ok || len(in.Series) != 1 || in.Series[0].Last() != 3 || in.Conn != 7 {
		t.Fatalf("instance 1 = %+v %v", in, ok)
	}
	if _, ok := s.Instance(id(9)); ok {
		t.Error("an instance nobody knows was found")
	}
	if in, ok := s.ForConn(8); !ok || in.ClientID != "b" || in.Series != nil {
		t.Errorf("for conn 8 = %+v %v, want b without series", in, ok)
	}
	if _, ok := s.ForConn(99); ok {
		t.Error("a connection with no instance has one")
	}
}
