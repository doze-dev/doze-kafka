package console

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// What the Partitions, Config and Readers tabs (Tp-16, Tp-18, Tp-21 to Tp-23) hold that
// the tape does not: who leads each partition, the transactions open on it, and
// what the broker reports for every config key.

// partitionLead is who leads a partition and how many replicas it has in sync.
type partitionLead struct {
	Leader   int32
	Replicas int
	InSync   int
}

// PartitionLeads asks Metadata for each partition's leader.
func (b *backend) PartitionLeads(ctx context.Context, topic string) (map[int32]partitionLead, error) {
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = &topic
	req.Topics = []kmsg.MetadataRequestTopic{t}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	out := map[int32]partitionLead{}
	for _, mt := range resp.Topics {
		for _, p := range mt.Partitions {
			out[p.Partition] = partitionLead{Leader: p.Leader, Replicas: len(p.Replicas), InSync: len(p.ISR)}
		}
	}
	return out, nil
}

// openTxn is a transaction that holds a partition's last stable offset back: the
// producer the broker lists as having one open there (DescribeProducers), and, when
// the transaction coordinator names it, its transactional id.
type openTxn struct {
	Partition  int32
	ProducerID int64
	Epoch      int16
	Start      int64 // the first offset of the transaction on this partition.
	TxnID      string
	Began      time.Time
	Touches    []string // every topic-partition the transaction has written to.
}

// TopicOpenTxns lists, for each partition of a topic, the producers with a transaction
// open on it (DescribeProducers, whose CurrentTxnStartOffset is -1 when there is none),
// named by the transactional ids the coordinator holds.
func (b *backend) TopicOpenTxns(ctx context.Context, topic string, partitions int32) ([]openTxn, error) {
	req := kmsg.NewPtrDescribeProducersRequest()
	rt := kmsg.NewDescribeProducersRequestTopic()
	rt.Topic = topic
	for p := int32(0); p < partitions; p++ {
		rt.Partitions = append(rt.Partitions, p)
	}
	req.Topics = []kmsg.DescribeProducersRequestTopic{rt}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	var out []openTxn
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				continue
			}
			for _, a := range p.ActiveProducers {
				if a.CurrentTxnStartOffset >= 0 {
					out = append(out, openTxn{Partition: p.Partition, ProducerID: a.ProducerID, Epoch: int16(a.ProducerEpoch), Start: a.CurrentTxnStartOffset})
				}
			}
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	byPID := map[int64]TxnRow{}
	for _, r := range b.openTransactions(ctx) {
		byPID[r.ProducerID] = r
	}
	for i := range out {
		if r, ok := byPID[out[i].ProducerID]; ok {
			out[i].TxnID, out[i].Began, out[i].Touches = r.ID, r.Began, r.Partitions
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Partition != out[j].Partition {
			return out[i].Partition < out[j].Partition
		}
		return out[i].Start < out[j].Start
	})
	return out, nil
}

// configShown is a config's value as the page says it ("7d", "128 MB"), and "" when the
// broker did not report the key.
func configShown(entries []ConfigEntry, key string) string {
	for _, e := range entries {
		if e.Name != key {
			continue
		}
		for _, ts := range topicSettings {
			if ts.Key == key {
				return ts.show(e.Value)
			}
		}
		return e.Value
	}
	return ""
}

// addPartsView is the Add partitions form (Tp-17): the total asked for, what it makes
// of the partitions, and who is told.
type addPartsView struct {
	Open    bool
	Now     int
	Total   int
	Fewer   []int // the partitions that exist.
	More    []int // the ones that would be created.
	Readers []readerRow
}

func (c *Console) addPartsView(r *http.Request, now int32, readers []readerRow) addPartsView {
	q := r.URL.Query()
	v := addPartsView{Open: q.Get("add") == "1", Now: int(now), Total: int(now) + 1, Readers: readers}
	if n, err := strconv.Atoi(q.Get("total")); err == nil && n > 0 && n < 100000 {
		v.Total = n
	}
	for p := 0; p < v.Now; p++ {
		v.Fewer = append(v.Fewer, p)
	}
	for p := v.Now; p < v.Total && p < v.Now+64; p++ {
		v.More = append(v.More, p)
	}
	return v
}
