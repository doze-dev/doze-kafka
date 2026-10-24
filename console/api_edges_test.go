package console

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRailCountsAndHeartbeat(t *testing.T) {
	src := &fakeTraffic{clients: []TrafficClient{{Conn: 1, ClientID: "a"}, {Conn: 2, ClientID: "a"}, {Conn: 3, Remote: "r:3"}}}
	c, addr, _ := newRegistryConsole(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "t1", 1, 0)
	topicWith(t, c, "t2", 1, 0)
	commitFor(t, addr, "g", "t1", map[int32]int64{0: 0})
	register(t, c, "t1-value", userV1)
	rec := req(t, c, "GET", "/_console/api/counts", nil)
	var got map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if rec.Code != 200 || rec.Header().Get("Cache-Control") != "no-store" || got["topics"] != 2 || got["groups"] != 1 || got["schemas"] != 1 || got["traffic"] != 2 || got["broker"] != 1 {
		t.Errorf("%d %v", rec.Code, got)
	}
	plain := newTestConsole(t)
	rec = req(t, plain, "GET", "/_console/api/counts", nil)
	got = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["schemas"]; ok {
		t.Errorf("a count of schemas without a registry: %v", got)
	}
	if _, ok := got["traffic"]; ok {
		t.Errorf("a count of clients without a recorder: %v", got)
	}
	if got["broker"] != 1 || got["topics"] != 0 {
		t.Errorf("%v", got)
	}
	// A broker that does not answer: no counts, and the heartbeat says so.
	dead, err := New(Options{Addr: "192.0.2.1:9092"})
	if err != nil {
		t.Fatal(err)
	}
	defer dead.Close()
	start := time.Now()
	rec = req(t, dead, "GET", "/_console/api/counts", nil)
	got = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["broker"] != 0 {
		t.Errorf("an unreachable broker: %v", got)
	}
	if _, ok := got["topics"]; ok {
		t.Errorf("a count of topics from a broker that did not answer: %v", got)
	}
	if time.Since(start) > 4*time.Second {
		t.Errorf("the heartbeat waited %v; it is bounded at the rail's deadline", time.Since(start))
	}
}

func TestListPollNumbers(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	cl := rawClient(t, addr)
	topicWith(t, c, "orders", 1, 0)
	topicWith(t, c, "orders.dlq", 1, 0)
	topicWith(t, c, "empty.dlq", 1, 0)
	bulkProduce(t, cl, "orders", 0, 1234, vals("m"), time.Time{})
	bulkProduce(t, cl, "orders.dlq", 0, 3, vals("m"), time.Time{})
	commitFor(t, addr, "behind", "orders", map[int32]int64{0: 1000})
	commitFor(t, addr, "caught", "orders", map[int32]int64{0: 1234})
	get := func(section string) (int, []listNumber) {
		rec := req(t, c, "GET", "/_console/api/list/"+section, nil)
		var out []listNumber
		if rec.Code == 200 {
			if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
		}
		return rec.Code, out
	}
	code, topics := get("topics")
	want := map[string]listNumber{"empty.dlq": {"empty.dlq", "0", "mut", ""}, "orders": {"orders", "1,234", "mut", ""}, "orders.dlq": {"orders.dlq", "3", "warn", ""}}
	if code != 200 || len(topics) != 3 {
		t.Fatalf("%d %v", code, topics)
	}
	for _, n := range topics {
		if n != want[n.Name] {
			t.Errorf("%+v, want %+v", n, want[n.Name])
		}
	}
	code, groups := get("groups")
	byName := map[string]listNumber{}
	for _, n := range groups {
		byName[n.Name] = n
	}
	if code != 200 || byName["behind"] != (listNumber{"behind", "234", "warn", "off"}) || byName["caught"] != (listNumber{"caught", "0", "mut", "off"}) {
		t.Errorf("%d %v", code, groups)
	}
	if code, _ = get("schemas"); code != 404 {
		t.Errorf("an unknown list: %d", code)
	}
	for state, want := range map[string]string{"Stable": "", "Empty": "off", "Dead": "bad", "PreparingRebalance": "warn", "CompletingRebalance": "warn", "": "warn"} {
		if got := groupDot(state); got != want {
			t.Errorf("groupDot(%q) = %q", state, got)
		}
	}
}
