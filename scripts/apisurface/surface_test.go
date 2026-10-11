package main

import (
	"reflect"
	"testing"
)

func TestSurfaceListsExportedDeclarationsOnly(t *testing.T) {
	got, err := Surface("testdata/p")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"const Max",
		"field Thing.Name string",
		"field Thing.Opt *bool",
		"field Thing embeds net.Conn",
		"func New(n string, opts ...int) (*Thing, error)",
		"iface Doer.Do(x int) error",
		"method (*Thing) Close() error",
		"type Alias = map[string]int",
		"type Doer interface",
		"type Thing struct",
		"var Default Thing",
	}
	// "field Thing embeds" sorts among the others; compare as sorted sets.
	if !reflect.DeepEqual(sorted(got), sorted(want)) {
		t.Errorf("surface:\n got %q\nwant %q", got, want)
	}
}

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}
