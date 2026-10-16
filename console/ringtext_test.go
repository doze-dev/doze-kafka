package console

import (
	"strings"
	"testing"
	"time"
)

// "Read by nobody" is not true of a consumer that assigns partitions itself (it has
// no group), and "written by" is what the recorder's ring happened to keep: both
// pages say what they are counted from.
func TestTopicsSayWhatReadByAndWrittenByAreCountedFrom(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{events: []TrafficEvent{
		{Seq: 2, First: now.Add(-3 * time.Second), Time: now.Add(-3 * time.Second), Count: 1, ClientID: "billing", Remote: "r", API: "Produce", Topics: []string{"orders"}},
		{Seq: 1, First: now.Add(-125 * time.Second), Time: now.Add(-125 * time.Second), Count: 1, ClientID: "audit", Remote: "r2", API: "Fetch", Topics: []string{"orders"}},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "orders", 1, 2)

	home := req(t, c, "GET", "/_console/topics", nil).Body.String()
	if strings.Contains(home, ">nobody<") {
		t.Error("the Topics table still says a topic is read by nobody")
	}
	for _, want := range []string{"no group has committed", "written by 1 client", "from the last 2m 05s of requests"} {
		if !strings.Contains(home, want) {
			t.Errorf("the Topics page does not say %q", want)
		}
	}

	page := req(t, c, "GET", "/_console/topics/orders", nil).Body.String()
	note := between(page, `id="neighbours-note"`, "</div>")
	for _, want := range []string{"Written by is from the last 2m 05s of requests", "a consumer that assigns partitions itself has no group"} {
		if !strings.Contains(note, want) {
			t.Errorf("the topic's neighbours note does not say %q:\n%s", want, note)
		}
	}
}
