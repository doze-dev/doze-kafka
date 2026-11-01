package console

import (
	"strings"
	"testing"
	"time"
)

// How a recorded request reads in the table: what it is about, who sent it, how it went.
func TestTrafficRowTargetsAndDetails(t *testing.T) {
	c := newTestConsole(t)
	t0 := time.Date(2026, 10, 10, 12, 30, 0, 0, time.UTC)
	type want struct{ target, title, detail string }
	for name, tc := range map[string]struct {
		e    TrafficEvent
		v    trafficView
		want want
	}{
		"a commit on the main view names its group": {
			TrafficEvent{API: "OffsetCommit", Group: "g", Topics: []string{"t"}, Parts: "p0–p2"}, trafficView{}, want{"g", "g · t", "t p0–p2"},
		},
		"a commit beside a topic filter names the topic": {
			TrafficEvent{API: "OffsetCommit", Group: "g", Topics: []string{"t"}, Parts: "p0–p2"}, trafficView{TrafficFilter: TrafficFilter{Topic: "t"}}, want{"t", "", "p0–p2 · g"},
		},
		"a commit with no partitions beside a topic": {
			TrafficEvent{API: "OffsetCommit", Group: "g", Topics: []string{"t"}}, trafficView{TrafficFilter: TrafficFilter{Topic: "t"}}, want{"t", "", "g"},
		},
		"a heartbeat names its group": {
			TrafficEvent{API: "Heartbeat", Group: "g"}, trafficView{}, want{"g", "", ""},
		},
		"a group request with topics, on the main view, says them on hover": {
			TrafficEvent{API: "JoinGroup", Group: "g", Topics: []string{"a", "b"}}, trafficView{}, want{"g", "g · a, b", ""},
		},
		"a produce names its topics and the ones not kept": {
			TrafficEvent{API: "Produce", Topics: []string{"a", "b"}, TopicsMore: 2, Detail: "4 partitions"}, trafficView{}, want{"a, b and 2 more", "", "4 partitions"},
		},
		"one topic":              {TrafficEvent{API: "Fetch", Topics: []string{"a"}}, trafficView{}, want{"a", "", ""}},
		"a group without a name": {TrafficEvent{API: "FindCoordinator", Group: ""}, trafficView{}, want{"", "", ""}},
		"a group on another API": {TrafficEvent{API: "DescribeGroups", Group: "g"}, trafficView{}, want{"g", "", ""}},
		"a transaction":          {TrafficEvent{API: "InitProducerId", TxnID: "tx-1"}, trafficView{}, want{"tx tx-1", "", ""}},
		"metadata of one topic":  {TrafficEvent{API: "Metadata", Topics: []string{"a"}, Detail: "1 topic"}, trafficView{}, want{"a", "", "1 topic"}},
		"metadata of one topic and more not kept": {
			TrafficEvent{API: "Metadata", Topics: []string{"a"}, TopicsMore: 1, Detail: "2 topics"}, trafficView{}, want{"2 topics", "a and 1 more", "2 topics"},
		},
		"metadata of three": {TrafficEvent{API: "Metadata", Topics: []string{"a", "b", "c"}, Detail: "3 topics"}, trafficView{}, want{"3 topics", "a, b, c", "3 topics"}},
		"metadata of all":   {TrafficEvent{API: "Metadata", Detail: "9 topics"}, trafficView{}, want{"all topics", "", "9 topics"}},
		"metadata beside a topic filter": {
			TrafficEvent{API: "Metadata", Topics: []string{"a", "b"}, Detail: "2 topics"}, trafficView{TrafficFilter: TrafficFilter{Topic: "a"}}, want{"a, b", "", "2 topics"},
		},
	} {
		row := c.trafficRow(tc.e, tc.v)
		if row.Target != tc.want.target || row.TargetTitle != tc.want.title || row.DetailText != tc.want.detail {
			t.Errorf("%s: target %q title %q detail %q; want %q %q %q", name, row.Target, row.TargetTitle, row.DetailText, tc.want.target, tc.want.title, tc.want.detail)
		}
	}

	// Who sent it, how big, how long.
	row := c.trafficRow(TrafficEvent{ClientID: "svc", Remote: "10.0.0.1:9", ReqBytes: 526, RespBytes: 2048, Duration: 12 * time.Millisecond, ID: "r7", Count: 1}, trafficView{})
	if row.Who != "svc" || row.WhoQ != "svc" || row.Sizes != "526 B / 2.0 KiB" || row.Dur != "12 ms" || row.DurExact != "took 12.0 ms" || row.OpenKey != "r7" || row.Folded || row.Interval != "" {
		t.Errorf("%+v", row)
	}
	row = c.trafficRow(TrafficEvent{Remote: "10.0.0.1:9"}, trafficView{})
	if row.Who != "10.0.0.1:9" || row.WhoQ != "10.0.0.1%3A9" {
		t.Errorf("by address: %q %q", row.Who, row.WhoQ)
	}

	// A repeated request is a rhythm, if it is the kind that repeats on a timer.
	hb := c.trafficRow(TrafficEvent{API: "Heartbeat", Group: "g", Count: 4, First: t0, Time: t0.Add(9 * time.Second), Detail: "ignored"}, trafficView{})
	if !hb.Folded || hb.Interval != "every 3 s" || !hb.Rhythm || hb.DetailText != "every 3 s, since 12:30:00 UTC" {
		t.Errorf("heartbeat: %+v", hb)
	}
	pr := c.trafficRow(TrafficEvent{API: "Produce", Topics: []string{"t"}, Count: 4, First: t0, Time: t0.Add(9 * time.Second), Detail: "1 partition"}, trafficView{})
	if !pr.Folded || pr.Interval != "every 3 s" || pr.Rhythm || pr.DetailText != "1 partition" {
		t.Errorf("a repeated produce is not a rhythm: %+v", pr)
	}
	two := c.trafficRow(TrafficEvent{API: "Heartbeat", Group: "g", Count: 2, First: t0, Time: t0.Add(1500 * time.Millisecond)}, trafficView{})
	if two.Interval != "every 1.5 s" {
		t.Errorf("two: %q", two.Interval)
	}
	if one := c.trafficRow(TrafficEvent{API: "Heartbeat", Group: "g", Count: 1, First: t0, Time: t0.Add(time.Hour)}, trafficView{}); one.Folded || one.Rhythm || one.Interval != "" {
		t.Errorf("one request: %+v", one)
	}

	// How it went.
	for class, tc := range map[string]struct{ tone, title, row string }{
		"expected":        {"deny", "Part of the normal flow, not an error: the client does this next", ""},
		"refused":         {"deny", "Refused: real Kafka would say no too", "errrow refused"},
		"denied":          {"deny", "Denied: an ACL or a login decided", "errrow denied"},
		"not-implemented": {"warn", "Not implemented by this broker", "errrow not-implemented"},
		"failed":          {"bad", "Failed: the broker could not do it", "errrow failed"},
		"":                {"bad", "The response carried this error", "errrow "},
	} {
		r := c.trafficRow(TrafficEvent{Error: "SOME_ERROR", Class: class}, trafficView{})
		wantTone := tc.tone
		if class == "" {
			wantTone = "ok"
		}
		if r.Result != "SOME_ERROR" || r.Tone != wantTone || r.ResultTitle != tc.title || r.RowClass != tc.row {
			t.Errorf("class %q: %q %q %q %q", class, r.Result, r.Tone, r.ResultTitle, r.RowClass)
		}
	}
	if r := c.trafficRow(TrafficEvent{}, trafficView{}); r.Result != "ok" || r.Tone != "ok" || r.ResultTitle != "" || r.RowClass != "" {
		t.Errorf("a request that went well: %+v", r)
	}
}

// What a Metadata row says about topic auto-creation depends on what the broker declares.
func TestMetadataDetailAndAutoCreate(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Facts.KafkaMajor = 4; o.Facts.AutoCreateTopics = true })
	if got := c.metadataDetail(TrafficEvent{Detail: "3 topics"}); got != "3 topics" {
		t.Errorf("a count alone: %q", got)
	}
	if got := c.metadataDetail(TrafficEvent{Detail: "1 topic", AutoCreate: true}); got != "auto-create on" {
		t.Errorf("a count and auto-create: %q", got)
	}
	if got := c.metadataDetail(TrafficEvent{Detail: "answered 2", AutoCreate: true}); got != "answered 2 · auto-create on" {
		t.Errorf("an answer and auto-create: %q", got)
	}
	if got := c.metadataDetail(TrafficEvent{Detail: "answered 2"}); got != "answered 2" {
		t.Errorf("an answer: %q", got)
	}
	c.facts.AutoCreateTopics = false
	if got := c.metadataDetail(TrafficEvent{Detail: "answered 2", AutoCreate: true}); got != "answered 2 · asks to auto-create; off here" {
		t.Errorf("auto-create off: %q", got)
	}
	c.facts.KafkaMajor = 0
	if got := c.metadataDetail(TrafficEvent{Detail: "answered 2", AutoCreate: true}); got != "answered 2" {
		t.Errorf("a broker that did not say what it declares: %q", got)
	}
	if got := c.metadataDetail(TrafficEvent{Detail: "2 topics", AutoCreate: true}); got != "2 topics" {
		t.Errorf("a count with nothing to add: %q", got)
	}
}

func TestRingTextSaysHowFarBackTheRecorderReaches(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	short, long := c.ringText(now)
	if short != "from recent requests; none are kept yet" || long == "" {
		t.Errorf("empty: %q | %q", short, long)
	}
	src.events = []TrafficEvent{{ID: "r1", Seq: 1, First: now.Add(-3 * time.Second), Time: now, Count: 1, API: "Fetch"}}
	if short, long = c.ringText(now); short != "from the last 3s of requests" || !strings.Contains(long, "the last 2,000 requests the broker keeps in memory") || !strings.Contains(long, "reach back 3s") {
		t.Errorf("3 s: %q | %q", short, long)
	}
	src.events[0].First = now.Add(-time.Second)
	if short, _ = c.ringText(now); short != "from the last 2s of requests" {
		t.Errorf("younger than 2 s: %q", short)
	}
	src.events[0].First = now.Add(-2 * time.Second)
	if short, _ = c.ringText(now); short != "from the last 2s of requests" {
		t.Errorf("2 s: %q", short)
	}
	src.events[0].First = now.Add(-5 * time.Minute)
	if short, _ = c.ringText(now); short != "from the last 5m 00s of requests" {
		t.Errorf("5 min: %q", short)
	}
}
