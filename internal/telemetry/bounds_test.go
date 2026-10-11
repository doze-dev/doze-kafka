package telemetry

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// A client chooses its resource attributes, and the store merged every push's into the
// instance and never trimmed: 200 pushes of one new 1 KiB key and a 100 KiB value
// left 20 MB in one instance (TRF-4).
func TestResourceIsBounded(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0 }
	for i := 0; i < 200; i++ {
		s.Push(id(1), Who{ClientID: "noisy"}, Payload{Resource: map[string]string{
			fmt.Sprintf("k%03d-%s", i, strings.Repeat("k", 1024)): strings.Repeat("v", 100<<10),
		}}, false)
	}
	in := s.Instances()[0]
	if len(in.Resource) > maxResourceKeys {
		t.Errorf("%d resource keys held, want at most %d", len(in.Resource), maxResourceKeys)
	}
	total := 0
	for k, v := range in.Resource {
		if len(k) > maxResourceKey || len(v) > maxResourceValue {
			t.Errorf("a key of %d bytes and a value of %d bytes were kept (limits %d and %d)", len(k), len(v), maxResourceKey, maxResourceValue)
		}
		total += len(k) + len(v)
	}
	if total > 64<<10 {
		t.Errorf("the resource holds %d bytes, want under 64 KiB", total)
	}
	// A key it already has is still updated (a client's version changes), and a fresh
	// key does not push an old one out.
	s.Push(id(2), Who{}, Payload{Resource: map[string]string{"service.name": "a"}}, false)
	for i := 0; i < maxResourceKeys*2; i++ {
		s.Push(id(2), Who{}, Payload{Resource: map[string]string{fmt.Sprintf("other%d", i): "x"}}, false)
	}
	s.Push(id(2), Who{}, Payload{Resource: map[string]string{"service.name": "b"}}, false)
	for _, in := range s.Instances() {
		if in.ID == id(2) && in.Resource["service.name"] != "b" {
			t.Errorf("service.name = %q after the keys were full, want the latest value of a key it already has", in.Resource["service.name"])
		}
	}
}

// A series' name and attributes are the client's too.
func TestSeriesNamesAndAttributesAreClipped(t *testing.T) {
	s := NewStore()
	s.now = func() time.Time { return t0 }
	long := strings.Repeat("n", 100<<10)
	s.Push(id(1), Who{ClientID: "c"}, Payload{Metrics: []Metric{{Name: long, Unit: long, Kind: Gauge, Points: []Point{{Attrs: long, Time: t0, Value: 1}}}}}, false)
	ser := s.Instances()[0].Series[0]
	if len(ser.Name) > maxNameBytes || len(ser.Attrs) > maxNameBytes || len(ser.Unit) > 32 {
		t.Errorf("name %d, attrs %d, unit %d bytes kept", len(ser.Name), len(ser.Attrs), len(ser.Unit))
	}
}

// What a connection says about itself is the client's too.
func TestWhoIsClipped(t *testing.T) {
	s := NewStore()
	long := strings.Repeat("w", 32<<10)
	s.Touch(id(1), Who{ClientID: long, Software: long, Principal: long, Host: long})
	in := s.Instances()[0]
	if len(in.ClientID) > 128 || len(in.Software) > 128 || len(in.Principal) > 256 || len(in.Host) > 256 {
		t.Errorf("kept %d %d %d %d bytes", len(in.ClientID), len(in.Software), len(in.Principal), len(in.Host))
	}
}

// The decoder builds the resource map per push, and the push may be 512 KiB of
// attributes: it keeps what the store would keep.
func TestDecodeBoundsTheResource(t *testing.T) {
	res := map[string]string{}
	for i := 0; i < 500; i++ {
		res[fmt.Sprintf("key%03d", i)] = strings.Repeat("v", 2000)
	}
	p, err := Decode(Encode(Payload{Resource: res}))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Resource) > maxResourceKeys {
		t.Errorf("decoded %d resource keys, want at most %d", len(p.Resource), maxResourceKeys)
	}
	for _, v := range p.Resource {
		if len(v) > maxResourceValue {
			t.Errorf("a value of %d bytes decoded", len(v))
		}
	}
}
