package traffic

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// A request is described (its partitions paired with their answers, its fields written
// out, its topic ids looked up) before the recorder's lock is taken or with it let go:
// every connection takes that lock after every request, and describing is the slow
// part. The hooks here try the lock; it must be free.
func TestARequestIsDescribedOutsideTheLock(t *testing.T) {
	var r *Recorder
	free := func(what string) {
		t.Helper()
		if !r.mu.TryLock() {
			t.Errorf("%s ran with the recorder's lock held", what)
			return
		}
		r.mu.Unlock()
	}
	resolved := 0
	r = New(50, func([16]byte) string {
		resolved++
		free("the topic id lookup")
		return "orders"
	})
	described := 0
	r.describe = func(q kmsg.Request, resp kmsg.Response, resolve func([16]byte) string) protocol.Facts {
		described++
		free("the description")
		return protocol.DescribeFactsNamed(q, resp, resolve)
	}

	produce := kmsg.NewPtrProduceRequest()
	r.Request(obs(1, "app", kmsg.Produce, 0, func(o *protocol.Observation) { o.Req = produce }))
	if described != 1 {
		t.Fatalf("a produce was described %d times, want once", described)
	}

	// The first heartbeat of a run becomes a row and is described; the ones that fold
	// into it are not described at all.
	beat := kmsg.NewPtrHeartbeatRequest()
	for i := range 5 {
		r.Request(obs(1, "app", kmsg.Heartbeat, time.Duration(i+1)*time.Second, func(o *protocol.Observation) {
			o.Req, o.Idle, o.Group = beat, true, "g"
		}))
	}
	if described != 2 {
		t.Errorf("five heartbeats that fold into one row were described %d times, want once", described-1)
	}
	rows := r.Events(Filter{API: "Heartbeat"}, 0)
	if len(rows) != 1 || rows[0].Count != 5 || rows[0].Facts.Decoded == "" {
		t.Fatalf("heartbeat rows = %+v", rows)
	}

	fetch := kmsg.NewPtrFetchRequest()
	fetch.SetVersion(13)
	ft := kmsg.NewFetchRequestTopic()
	ft.TopicID = [16]byte{9}
	ft.Partitions = append(ft.Partitions, kmsg.NewFetchRequestTopicPartition())
	fetch.Topics = append(fetch.Topics, ft)
	r.Request(obs(1, "app", kmsg.Fetch, 10*time.Second, func(o *protocol.Observation) {
		o.Req, o.TopicIDs, o.Kind = fetch, [][16]byte{{9}}, "fetch"
	}))
	if resolved == 0 {
		t.Error("the topic id was never looked up")
	}
	if got := r.Events(Filter{Topic: "orders"}, 0); len(got) != 1 || got[0].Facts.Partitions[0].Topic != "orders" {
		t.Errorf("the fetch by id = %+v", got)
	}
}

// While one connection's request is being described, every other connection's goes
// through: the lock is not held across it.
func TestASlowDescriptionHoldsNobodyUp(t *testing.T) {
	r := New(50, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	slow := kmsg.NewPtrProduceRequest()
	r.describe = func(q kmsg.Request, resp kmsg.Response, resolve func([16]byte) string) protocol.Facts {
		if q == kmsg.Request(slow) {
			close(entered)
			<-release
		}
		return protocol.DescribeFactsNamed(q, resp, resolve)
	}
	done := make(chan struct{})
	go func() {
		r.Request(obs(1, "slow", kmsg.Produce, 0, func(o *protocol.Observation) { o.Req = slow }))
		close(done)
	}()
	<-entered
	other := make(chan int)
	go func() {
		r.Request(obs(2, "quick", kmsg.Metadata, time.Second, func(o *protocol.Observation) { o.Req = kmsg.NewPtrMetadataRequest() }))
		other <- len(r.Events(Filter{}, 0)) + len(r.Clients())
	}()
	select {
	case n := <-other:
		if n != 2 { // the quick row and its client: the slow one has not reached the recorder yet.
			t.Errorf("rows and clients seen while the slow one was described = %d, want 2", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("another connection's request waited for the slow description")
	}
	close(release)
	<-done
	if got := r.Events(Filter{}, 0); len(got) != 2 {
		t.Errorf("%d rows, want 2", len(got))
	}
}

// A page asks for the newest 200 rows. The recorder copies those and no others,
// whatever it holds: it used to copy every matching row, ticks and all, under the lock
// every connection waits on, and cut the copy to 200 afterwards.
func TestAQueryCopiesOnlyTheRowsItReturns(t *testing.T) {
	r := fullRecorder()
	alloc := func(limit int) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for range 20 {
			if got := r.Query(Query{Limit: limit}); len(got.Rows) != limit || got.Total != 2000 {
				t.Fatalf("limit %d: %d rows of %d", limit, len(got.Rows), got.Total)
			}
		}
		runtime.ReadMemStats(&after)
		return (after.TotalAlloc - before.TotalAlloc) / 20
	}
	one, page := alloc(1), alloc(200)
	// A row is some hundreds of bytes; 200 of them, with the ticks of the few that fold,
	// are far under what 2,000 are.
	if one > 16<<10 {
		t.Errorf("a query for one row allocates %d bytes: it is copying rows it does not return", one)
	}
	if page > 400<<10 {
		t.Errorf("a query for 200 rows allocates %d bytes: it is copying rows it does not return", page)
	}
}

// The newest rows by when they began are the ones returned, in that order, although
// the recorder keeps its rows by latest activity and a limit cuts the list.
func TestALimitedQueryReturnsTheNewestByStart(t *testing.T) {
	r := New(100, nil)
	// A long poll that began first and ended last sits at the front of the activity
	// list; by start it is the oldest.
	starts := []time.Duration{5, 9, 2, 7, 1, 8, 3}
	for i, s := range starts {
		r.Request(obs(1, "app", kmsg.Produce, s*time.Second, func(o *protocol.Observation) { o.Topics = []string{fmt.Sprint("t", i)} }))
	}
	got := r.Query(Query{Limit: 3})
	if got.Total != len(starts) || len(got.Rows) != 3 {
		t.Fatalf("%d rows of %d", len(got.Rows), got.Total)
	}
	for i, want := range []time.Duration{9, 8, 7} {
		if !got.Rows[i].Time.Equal(t0.Add(want * time.Second)) {
			t.Errorf("row %d began %v, want %v", i, got.Rows[i].Time.Sub(t0), want*time.Second)
		}
	}
	if !got.Newest.Equal(t0.Add(9*time.Second)) || !got.Oldest.Equal(t0.Add(time.Second)) {
		t.Errorf("newest %v, oldest %v", got.Newest.Sub(t0), got.Oldest.Sub(t0))
	}
	all := r.Query(Query{})
	for i := range got.Rows {
		if all.Rows[i].ID != got.Rows[i].ID {
			t.Errorf("row %d of the limited query is %s, of the whole %s", i, got.Rows[i].ID, all.Rows[i].ID)
		}
	}
}

// What a query counts: the rows that match, the requests they stand for, and the
// requests of everything held, without a second pass by the caller.
func TestAQueryCountsRequestsAsWellAsRows(t *testing.T) {
	r := New(100, nil)
	for i := range 6 {
		r.Request(obs(1, "app", kmsg.Heartbeat, time.Duration(i)*time.Second, func(o *protocol.Observation) { o.Idle, o.Group = true, "g" }))
	}
	r.Request(obs(1, "app", kmsg.Produce, 7*time.Second, func(o *protocol.Observation) { o.Topics = []string{"orders"} }))
	r.Request(obs(1, "app", kmsg.Produce, 8*time.Second, func(o *protocol.Observation) { o.Topics = []string{"orders"} }))
	got := r.Query(Query{Filter: Filter{Group: "g"}, Limit: 10})
	if got.Total != 1 || got.Requests != 6 || got.Held != 8 {
		t.Errorf("rows %d, requests %d, held %d; want 1, 6, 8", got.Total, got.Requests, got.Held)
	}
	// Rows pushed out take their requests with them.
	small := New(2, nil)
	for i := range 5 {
		small.Request(obs(1, "app", kmsg.Produce, time.Duration(i)*time.Second, func(o *protocol.Observation) { o.Topics = []string{fmt.Sprint(i)} }))
	}
	if got := small.Query(Query{}); got.Held != 2 || got.Total != 2 {
		t.Errorf("held %d of %d rows, want 2 of 2", got.Held, got.Total)
	}
}
