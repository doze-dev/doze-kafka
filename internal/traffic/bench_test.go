package traffic

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// fullRecorder holds 2,000 rows of the mix a broker sees: produces to forty topics from
// ten connections, with partitions listed, and forty folded heartbeat rows with all
// their ticks.
func fullRecorder() *Recorder {
	r := New(2000, nil)
	q := kmsg.NewPtrProduceRequest()
	tp := kmsg.NewProduceRequestTopic()
	tp.Topic = "orders"
	for p := range 4 {
		pp := kmsg.NewProduceRequestTopicPartition()
		pp.Partition = int32(p)
		tp.Partitions = append(tp.Partitions, pp)
	}
	q.Topics = append(q.Topics, tp)
	for i := range 2000 {
		conn := uint64(i%10 + 1)
		at := time.Duration(i) * time.Millisecond
		if i%50 == 0 {
			for b := range 70 {
				r.Request(obs(conn, "app", kmsg.Heartbeat, at+time.Duration(b), func(o *protocol.Observation) {
					o.Idle, o.Group = true, fmt.Sprint("g", i/50)
				}))
			}
			continue
		}
		r.Request(obs(conn, fmt.Sprintf("app-%d", conn), kmsg.Produce, at, func(o *protocol.Observation) {
			o.Topics, o.Req, o.Kind = []string{fmt.Sprintf("topic-%d", i%40)}, q, "produce"
		}))
	}
	return r
}

// BenchmarkQuery is what a page's poll asks of the recorder.
func BenchmarkQuery(b *testing.B) {
	r := fullRecorder()
	for _, c := range []struct {
		name  string
		f     Filter
		limit int
	}{
		{"newest200", Filter{}, 200},
		{"one", Filter{}, 1},
		{"everything", Filter{}, 0},
		{"topic200", Filter{Topic: "topic-7"}, 200},
		{"errors-none", Filter{ErrorsOnly: true}, 200},
		{"client-text", Filter{Client: "app-3"}, 200},
		{"group-folded", Filter{Group: "g3"}, 200},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				r.Events(c.f, c.limit)
			}
		})
	}
}

// BenchmarkRequest is what every connection pays after every request: a produce with
// its partitions (a new row each time) and a heartbeat (which folds).
func BenchmarkRequest(b *testing.B) {
	q := kmsg.NewPtrProduceRequest()
	tp := kmsg.NewProduceRequestTopic()
	tp.Topic = "orders"
	for p := range 16 {
		pp := kmsg.NewProduceRequestTopicPartition()
		pp.Partition = int32(p)
		tp.Partitions = append(tp.Partitions, pp)
	}
	q.Topics = append(q.Topics, tp)
	b.Run("produce", func(b *testing.B) {
		r := New(2000, nil)
		o := obs(1, "app", kmsg.Produce, 0, func(o *protocol.Observation) { o.Topics, o.Req = []string{"orders"}, q })
		b.ReportAllocs()
		for b.Loop() {
			r.Request(o)
		}
	})
	b.Run("heartbeat", func(b *testing.B) {
		r := New(2000, nil)
		o := obs(1, "app", kmsg.Heartbeat, 0, func(o *protocol.Observation) {
			o.Idle, o.Group, o.Req = true, "g", kmsg.NewPtrHeartbeatRequest()
		})
		b.ReportAllocs()
		for b.Loop() {
			r.Request(o)
		}
	})
	// Many connections at once: what they wait for is each other, so this is the time
	// the lock is held.
	b.Run("produce-parallel", func(b *testing.B) {
		r := New(2000, nil)
		var next atomic.Uint64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			id := next.Add(1)
			o := obs(1000+id, "app", kmsg.Produce, 0, func(o *protocol.Observation) {
				o.Topics, o.Req = []string{"orders"}, q
			})
			for pb.Next() {
				r.Request(o)
			}
		})
	})
}
