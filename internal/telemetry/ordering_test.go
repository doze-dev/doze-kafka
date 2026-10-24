package telemetry

import (
	"testing"
	"time"
)

// A point earlier than the series' newest is placed at the newest, which is read from the
// series' last sample after more than one has been kept.
func TestAPointEarlierThanTheNewestIsPlacedAtTheNewest(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0.Add(time.Minute) }
	for _, off := range []int{10, 20, 15} {
		s.Push(id(1), Who{}, gauge("m", t0.Add(time.Duration(off)*time.Second), float64(off)), false)
	}
	sm := s.Instances()[0].Series[0].Samples
	if len(sm) != 3 || !sm[2].T.Equal(t0.Add(20*time.Second)) {
		t.Errorf("samples = %+v, want the third at +20s", sm)
	}
}
