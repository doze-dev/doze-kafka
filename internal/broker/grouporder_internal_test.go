package broker

import (
	"slices"
	"testing"
	"time"
)

// The orders Kafka answered in (conformance/recordings/describe-groups-order.json).
func TestInKafkaGroupOrderIsKafkas(t *testing.T) {
	cases := []struct{ in, want []string }{
		{[]string{"dgo-a", "dgo-b", "dgo-c", "dgo-d", "dgo-x", "dgo-y"}, []string{"dgo-x", "dgo-y", "dgo-d", "dgo-b", "dgo-c", "dgo-a"}},
		{[]string{"dgo-c", "dgo-x", "dgo-a", "dgo-d", "dgo-y", "dgo-b"}, []string{"dgo-x", "dgo-y", "dgo-d", "dgo-b", "dgo-c", "dgo-a"}},
		{[]string{"dgo-b", "dgo-a", "dgo-b"}, []string{"dgo-b", "dgo-b", "dgo-a"}},
		{[]string{"dgo-c", "", "dgo-x", "dgo-a"}, []string{"", "dgo-x", "dgo-c", "dgo-a"}},
		{[]string{"dgo-n3", "dgo-y"}, []string{"dgo-y", "dgo-n3"}}, // one bucket: the later partition first.
		{[]string{"dgo-y", "dgo-n3"}, []string{"dgo-n3", "dgo-y"}},
		{[]string{"dgo-p8", "dgo-x", "dgo-p44"}, []string{"dgo-p8", "dgo-x", "dgo-p44"}}, // one partition: as asked.
		{
			[]string{"dgo-p0", "dgo-p1", "dgo-p2", "dgo-p3", "dgo-p4", "dgo-p5", "dgo-p6", "dgo-p7", "dgo-p8", "dgo-p9",
				"dgo-p10", "dgo-p11", "dgo-p12", "dgo-p13", "dgo-p14", "dgo-p15", "dgo-p16", "dgo-p17", "dgo-p18", "dgo-p19"},
			[]string{"dgo-p2", "dgo-p13", "dgo-p1", "dgo-p14", "dgo-p4", "dgo-p11", "dgo-p3", "dgo-p12", "dgo-p6", "dgo-p5",
				"dgo-p10", "dgo-p8", "dgo-p7", "dgo-p19", "dgo-p17", "dgo-p18", "dgo-p0", "dgo-p15", "dgo-p16", "dgo-p9"},
		},
	}
	for _, c := range cases {
		var got []string
		within(t, 5*time.Second, "inKafkaGroupOrder", func() { got = inKafkaGroupOrder(c.in) })
		if !slices.Equal(got, c.want) {
			t.Errorf("%v: got %v, want %v", c.in, got, c.want)
		}
	}
	if p := offsetsPartitionFor("dgo-a"); p != 36 {
		t.Errorf("partition of dgo-a = %d, want 36", p)
	}
}
