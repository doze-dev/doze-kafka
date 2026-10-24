package console

import (
	"fmt"
	"testing"
	"time"
)

func TestDockRowNameDropsOnlyAVersion(t *testing.T) {
	for in, want := range map[string]string{
		"Produce v9": "Produce", "Fetch v13": "Fetch", "Produce v0": "Produce", "Produce v": "Produce v", "Produce v1x": "Produce v1x",
		"Produce": "Produce", "": "", "a v1 v22": "a v1", "a vx v2": "a vx", " v5": "", "Metadata v12": "Metadata",
	} {
		if got := (dockRow{DockEvent: DockEvent{Request: in}}).Name(); got != want {
			t.Errorf("Name(%q) = %q, want %q", in, got, want)
		}
	}
}

// What a dock row says of a request: who sent it, what it was about, and how it went.
func TestDockEventsNameTheClientAndTheTarget(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "r3", Seq: 3, Time: now, API: "Heartbeat", Version: 4, Group: "workers", ClientID: "svc", Count: 7, Duration: 800 * time.Microsecond},
		{ID: "r2", Seq: 2, Time: now, API: "Produce", Version: 9, Topics: []string{"a", "b"}, Remote: "10.1.1.1:5000", Detail: "2 partitions", Error: "X", Duration: 12 * time.Millisecond},
		{ID: "r1", Seq: 1, Time: now, API: "Metadata", Version: 12, ClientID: "svc"},
	}}
	got, next := trafficDock{src}.DockEvents(DockFilter{Group: "workers"}, 8, "")
	if next != "" || len(got) != 1 {
		t.Fatalf("%+v %q", got, next)
	}
	if e := got[0]; e.Client != "svc" || e.Target != "workers" || e.Request != "Heartbeat v4" || e.Count != 7 || e.Took != "0.8 ms" && e.Took != "1 ms" || e.Source != "kafka" || e.Kind != KindRequest {
		t.Errorf("%+v", e)
	}
	got, _ = trafficDock{src}.DockEvents(DockFilter{Topic: "a"}, 8, "")
	if len(got) != 1 || got[0].Client != "10.1.1.1:5000" || got[0].Target != "a, b" || got[0].Detail != "2 partitions" || got[0].Error != "X" {
		t.Errorf("%+v", got)
	}
	// A page of rows is as long as asked, and says where the next starts.
	many := &fakeTraffic{}
	for i := 12; i >= 1; i-- {
		many.events = append(many.events, TrafficEvent{ID: fmt.Sprintf("r%d", i), Seq: uint64(i), Time: now, API: "Fetch", Topics: []string{"t"}, ClientID: "c"})
	}
	page, next := trafficDock{many}.DockEvents(DockFilter{Topic: "t"}, 5, "")
	if len(page) != 5 || next != page[4].ID {
		t.Errorf("%d rows, next %q", len(page), next)
	}
	page, next = trafficDock{many}.DockEvents(DockFilter{Topic: "t"}, 12, "")
	if len(page) != 12 || next != "" {
		t.Errorf("exactly the rows there are: %d rows, next %q", len(page), next)
	}
	page, next = trafficDock{many}.DockEvents(DockFilter{Topic: "t"}, 5, page[10].ID)
	if len(page) != 1 || next != "" {
		t.Errorf("after the second-to-last: %d rows, next %q", len(page), next)
	}
	if page, _ = (trafficDock{many}).DockEvents(DockFilter{Topic: "t"}, 5, "gone"); len(page) != 0 {
		t.Errorf("after a row that is no longer held: %d rows", len(page))
	}
	if page, _ = (trafficDock{many}).DockEvents(DockFilter{Subject: "s"}, 5, ""); len(page) != 0 {
		t.Errorf("a subject's dock: %d rows", len(page))
	}
}
