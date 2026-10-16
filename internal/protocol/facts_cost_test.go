package protocol

import (
	"fmt"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// bigFetch is a fetch of n partitions answered in full, spread over the given number of
// topics.
func bigFetch(n, topics int) (*kmsg.FetchRequest, *kmsg.FetchResponse) {
	q, r := kmsg.NewPtrFetchRequest(), kmsg.NewPtrFetchResponse()
	q.SetVersion(12)
	per := n / topics
	for ti := range topics {
		tp, rt := kmsg.NewFetchRequestTopic(), kmsg.NewFetchResponseTopic()
		tp.Topic = "topic-" + fmt.Sprint(ti)
		rt.Topic = tp.Topic
		for p := range per {
			pp, rp := kmsg.NewFetchRequestTopicPartition(), kmsg.NewFetchResponseTopicPartition()
			pp.Partition, rp.Partition = int32(p), int32(p)
			tp.Partitions = append(tp.Partitions, pp)
			rt.Partitions = append(rt.Partitions, rp)
		}
		q.Topics, r.Topics = append(q.Topics, tp), append(r.Topics, rt)
	}
	return q, r
}

// The cost of describing a request is bounded by what is kept of it, not by what it
// named: sixteen partitions are kept, so four thousand cost about what sixteen do. It
// was quadratic (48 ms at 4,000 against 10 µs at 16), on every connection's path.
func TestDescribingARequestCostsWhatIsKeptNotWhatItNamed(t *testing.T) {
	cost := func(q *kmsg.FetchRequest, r *kmsg.FetchResponse) time.Duration {
		best := time.Duration(1 << 62)
		for range 9 { // the least of several: the machine has other work.
			began := time.Now()
			for range 200 {
				DescribeFacts(q, r)
			}
			if d := time.Since(began); d < best {
				best = d
			}
		}
		return best
	}
	small := cost(bigFetch(MaxFactPartitions, 1))
	for name, topics := range map[string]int{"one topic": 1, "a partition a topic": 4000} {
		q, r := bigFetch(4000, topics)
		if f := DescribeFacts(q, r); len(f.Partitions) != MaxFactPartitions || f.PartitionsMore != 4000-MaxFactPartitions {
			t.Fatalf("%s: %d kept, %d more", name, len(f.Partitions), f.PartitionsMore)
		}
		if big := cost(q, r); big > 4*small {
			t.Errorf("4,000 partitions (%s) cost %v for 200, %d for 16 cost %v: more than four times", name, big, MaxFactPartitions, small)
		}
	}
}

func BenchmarkDescribeFacts(b *testing.B) {
	for _, n := range []int{1, 16, 100, 1000, 4000} {
		q, r := bigFetch(n, 1)
		b.Run(fmt.Sprintf("partitions=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				DescribeFacts(q, r)
			}
		})
	}
}
