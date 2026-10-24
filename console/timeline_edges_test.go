package console

import (
	"testing"
)

func TestTimelineKinds(t *testing.T) {
	for kind, want := range map[string][2]string{
		"joined": {"joined", "ok"}, "left": {"left", "dim"}, "expired": {"session expired", "bad"}, "dropped": {"dropped", "bad"},
		"rebalance": {"rebalance", "warn"}, "generation": {"new generation", "ok"}, "assigned": {"assigned", "ok"}, "epoch": {"new epoch", "ok"},
		"something-else": {"something-else", "dim"}, "": {"", "dim"},
	} {
		if l, tone := timelineKind(kind); l != want[0] || tone != want[1] {
			t.Errorf("timelineKind(%q) = %q, %q", kind, l, tone)
		}
	}
}

func TestGroupTimelineIsNewestFirst(t *testing.T) {
	long := "consumer-1-0123456789abcdef-4242"
	src := &fakeTraffic{groups: map[string][]TrafficGroupEvent{"g": {
		{Seq: 1, Kind: "joined", Member: "m1"}, {Seq: 2, Kind: "rebalance"}, {Seq: 3, Kind: "expired", Member: long},
	}}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	rows := c.groupTimeline("g")
	if len(rows) != 3 || rows[0].Seq != 3 || rows[1].Seq != 2 || rows[2].Seq != 1 {
		t.Fatalf("%+v", rows)
	}
	if rows[0].Label != "session expired" || rows[0].Tone != "bad" || rows[0].Member != shortID(long) || rows[2].Member != "m1" || rows[1].Label != "rebalance" {
		t.Errorf("%+v", rows)
	}
	if got := c.groupTimeline("none"); len(got) != 0 {
		t.Errorf("%+v", got)
	}
	if got := newTestConsole(t).groupTimeline("g"); got != nil {
		t.Errorf("without a recorder: %+v", got)
	}
}

func TestShortIDAndPartRanges(t *testing.T) {
	for in, want := range map[string]string{"short": "short", "12345678901234": "12345678901234", "123456789012345": "123456…012345", "": ""} {
		if got := shortID(in); got != want {
			t.Errorf("shortID(%q) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string][]int32{"": nil, "0": {0}, "0–2": {0, 1, 2}, "0–2, 5, 7–8": {8, 0, 5, 2, 1, 7}, "3, 5": {5, 3}, "1–2": {2, 1}} {
		if got := partRanges(want); got != in {
			t.Errorf("partRanges(%v) = %q, want %q", want, got, in)
		}
	}
	in := []int32{3, 1}
	partRanges(in)
	if in[0] != 3 {
		t.Error("partRanges sorted its argument")
	}
}
