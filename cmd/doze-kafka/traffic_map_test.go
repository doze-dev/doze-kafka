package main

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/console"
	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/telemetry"
	"github.com/doze-dev/doze-kafka/internal/traffic"
)

// The console's types are the recorder's, field for field: nothing is lost on the way
// from the one to the other.
func TestTheConsoleSeesWhatTheRecorderHolds(t *testing.T) {
	rec := traffic.New(100, nil)
	start := time.Now()
	rec.ConnOpened(1, "127.0.0.1:50000")
	prod := kmsg.NewPtrProduceRequest()
	prod.Acks = -1
	tp := kmsg.NewProduceRequestTopic()
	tp.Topic = "orders"
	pp := kmsg.NewProduceRequestTopicPartition()
	pp.Partition = 2
	tp.Partitions = append(tp.Partitions, pp)
	prod.Topics = append(prod.Topics, tp)
	rec.Request(protocol.Observation{
		Conn: 1, Remote: "127.0.0.1:50000", Principal: "User:ANONYMOUS", ClientID: "billing", API: kmsg.Produce, Version: 9,
		Start: start, Duration: time.Millisecond, ReqBytes: 100, RespBytes: 20, Correlation: 41, Req: prod,
		Summary: protocol.Summary{Topics: []string{"orders"}, TxnID: "tx-1", Kind: "produce", Detail: "p2", ErrorCode: 3, Class: protocol.ClassRefused},
	})
	for i := 0; i < 3; i++ {
		rec.Request(protocol.Observation{Conn: 1, ClientID: "billing", API: kmsg.Heartbeat, Version: 4, Start: start.Add(time.Duration(i+1) * time.Second),
			Summary: protocol.Summary{Group: "g", Idle: true, Kind: "groups"}})
	}
	rec.GroupEvent("g", "rebalance", "m", 1, "a member joined", 1, "a member joined")
	rec.GroupEvent("g", "generation", "m", 2, "1 member", 1, "")

	src := trafficFor(rec)
	evs := src.Events(console.TrafficFilter{}, 0)
	if len(evs) != 2 {
		t.Fatalf("%d rows", len(evs))
	}
	var produce, beat console.TrafficEvent
	for _, e := range evs {
		if e.API == "Produce" {
			produce = e
		} else {
			beat = e
		}
	}
	if produce.ID == "" || produce.TxnID != "tx-1" || produce.Class != "refused" || produce.Error != "UNKNOWN_TOPIC_OR_PARTITION" ||
		produce.Correlation != 41 || !produce.Facts.HasAcks || produce.Facts.Acks != -1 || len(produce.Facts.Partitions) != 1 || produce.Facts.Partitions[0].Partition != 2 ||
		produce.Facts.Decoded == "" {
		t.Errorf("produce row = %+v", produce)
	}
	if beat.Count != 3 || len(beat.Ticks) != 3 || beat.Group != "g" {
		t.Errorf("heartbeat row = %+v", beat)
	}
	if got, ok := src.Row(produce.ID); !ok || got.ID != produce.ID {
		t.Error("a row is not found by its id")
	}
	if n, _ := src.NewSince(console.TrafficFilter{}, start.Add(-time.Hour)); n != 4 {
		t.Errorf("new since = %d, want 4", n)
	}
	if w := src.Window(time.Minute); w.Requests != 4 || w.Errors != 1 {
		t.Errorf("window = %+v", w)
	}
	rs := src.Refusals()
	if len(rs) != 1 || rs[0].Name != "UNKNOWN_TOPIC_OR_PARTITION" || len(rs[0].Clients) != 1 || rs[0].Clients[0].Client != "billing" {
		t.Errorf("refusals = %+v", rs)
	}
	cs := src.Clients()
	if len(cs) != 1 || cs[0].TxnID != "tx-1" || !cs[0].Produces || cs[0].Req5m != 4 || len(cs[0].Groups) != 1 {
		t.Errorf("clients = %+v", cs)
	}
	if ge := src.GroupEvents("g", 0); len(ge) != 2 || ge[0].Cause != "a member joined" || ge[1].Members != 1 {
		t.Errorf("group events = %+v", ge)
	}
	st := src.Story("g")
	if st.Protocol != "classic" || len(st.Generations) != 1 || st.Generations[0].Gen != 2 {
		t.Errorf("story = %+v", st)
	}
	if src.Capacity() != 100 || src.Since().IsZero() {
		t.Errorf("capacity %d since %v", src.Capacity(), src.Since())
	}
}

func TestClientMetricsAreMappedWithTheirConnection(t *testing.T) {
	store := telemetry.NewStore()
	id := [16]byte{1, 2, 3, 4}
	store.Touch(id, telemetry.Who{ClientID: "billing", Principal: "User:a", Conn: 7})
	store.Push(id, telemetry.Who{ClientID: "billing", Principal: "User:a", Conn: 7}, telemetry.Payload{
		Resource: map[string]string{"k": "v"},
		Metrics:  []telemetry.Metric{{Name: "org.apache.kafka.producer.x", Kind: telemetry.Gauge, Points: []telemetry.Point{{Time: time.Now(), Value: 2}}}},
	}, false)
	src := clientMetricsFor(store)
	all := src.Instances()
	if len(all) != 1 || all[0].Conn != 7 || len(all[0].Series) != 1 || all[0].Series[0].Samples[0].V != 2 || all[0].Resource["k"] != "v" {
		t.Fatalf("instances = %+v", all)
	}
	sum := src.(interface {
		Summaries() []console.ClientInstance
	}).Summaries()
	if len(sum) != 1 || len(sum[0].Series) != 0 || sum[0].Pushes != 1 || sum[0].Metrics != 1 {
		t.Errorf("summaries = %+v", sum)
	}
	by := src.(interface {
		ByID(string) (console.ClientInstance, bool)
	})
	full, ok := by.ByID(all[0].ID)
	if !ok || len(full.Series) != 1 {
		t.Errorf("by id: %+v %v", full, ok)
	}
	if _, ok := by.ByID("not-a-uuid"); ok {
		t.Error("a bad id found an instance")
	}
	if _, ok := by.ByID("00000000-0000-0000-0000-000000000000"); ok {
		t.Error("an unknown id found an instance")
	}
}
