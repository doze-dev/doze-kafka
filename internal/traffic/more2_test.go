package traffic

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

func TestTheRecorderNotesWhoAskedToReportAndKeepsFacts(t *testing.T) {
	r := New(10, nil)
	r.ConnOpened(1, "a")
	q := kmsg.NewPtrProduceRequest()
	q.Acks = 1
	r.Request(obs(1, "app", kmsg.GetTelemetrySubscriptions, 0))
	r.Request(obs(1, "app", kmsg.Produce, time.Second, func(o *protocol.Observation) { o.Req = q }))
	if cs := r.Clients(); len(cs) != 1 || !cs[0].AskedTelemetry {
		t.Errorf("clients = %+v", cs)
	}
	var f protocol.Facts
	for _, e := range r.Events(Filter{API: "produce"}, 0) {
		f = e.Facts
	}
	if !f.HasAcks || f.Acks != 1 || f.Decoded == "" {
		t.Errorf("facts = %+v", f)
	}
}

func TestOrderingTiesAndNamesFallBackToTheAddress(t *testing.T) {
	r := New(20, nil)
	r.ConnOpened(5, "x")
	r.ConnOpened(2, "y")
	// Two rows that began at the same instant: the later one first.
	r.Request(obs(5, "", kmsg.Fetch, 0, func(o *protocol.Observation) {
		o.Remote = "10.0.0.5:1"
		o.ErrorCode, o.Class = 3, protocol.ClassRefused
	}))
	r.Request(obs(2, "", kmsg.Fetch, 0, func(o *protocol.Observation) {
		o.Remote = "10.0.0.2:1"
		o.ErrorCode, o.Class = 3, protocol.ClassRefused
	}))
	evs := r.Events(Filter{}, 0)
	if len(evs) != 2 || evs[0].Conn != 2 {
		t.Errorf("order = %v %v", evs[0].Conn, evs[1].Conn)
	}
	cs := r.Clients()
	if len(cs) != 2 || cs[0].Conn != 2 || cs[1].Conn != 5 {
		t.Errorf("clients = %+v", cs)
	}
	rf := r.Refusals()
	if len(rf) != 1 || len(rf[0].Clients) != 2 || rf[0].Clients[0].Client != "x" {
		t.Errorf("refusals = %+v", rf)
	}
}

func TestARebalanceWithNoCauseFieldUsesItsDetail(t *testing.T) {
	r := New(10, nil)
	r.GroupEvent("g", "rebalance", "m", 1, "a member joined", 1, "")
	r.GroupEvent("g", "generation", "m", 2, "1 member", 1, "")
	s := r.Story("g")
	if len(s.Rebalances) != 1 || s.Rebalances[0].Cause != "a member joined" {
		t.Errorf("story = %+v", s.Rebalances)
	}
}
