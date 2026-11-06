package console

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/doze-dev/doze-kafka/schemaregistry"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The schema scan (SCH-25): which schema ids a topic's record values carry, how many of
// each, and where each was first and last seen. It reads records, so it is bounded as
// Trace is (it uses Trace's reader, readRanges): the newest records of each partition up
// to a total, and a time; what it leaves unread it says. A topic whose first records
// are none of them framed for the registry is given up on early: it is not a topic of
// registry-encoded values, and reading it all would say nothing more.

var (
	schemaScanMaxRecords = 50_000
	schemaScanMaxTime    = 5 * time.Second
)

// schemaScanProbe is how many records without a schema id it takes to give up.
const schemaScanProbe = 20

// idScan is what a topic's record values say about schema ids.
type idScan struct {
	Topic      string
	Scanned    int     // records read
	NotFramed  int     // values that are not registry-framed (plain text, other binary)
	Tombstones int     // null values
	IDs        []idUse // by id
	Unread     []string
	TimedOut   bool
	GaveUp     bool // the first schemaScanProbe records carried no id: not read further
}

// idUse is one schema id in a topic.
type idUse struct {
	ID          int
	Count       int
	First, Last tpOffset
	FirstAt     time.Time
	LastAt      time.Time
}

func (s idScan) idUse(id int) *idUse {
	for i := range s.IDs {
		if s.IDs[i].ID == id {
			return &s.IDs[i]
		}
	}
	return nil
}

// scanSchemaIDs reads the newest records of each of a topic's partitions, within the
// scan's budget, and counts the schema ids their values carry.
func (c *Console) scanSchemaIDs(ctx context.Context, topic string) (idScan, error) {
	s := idScan{Topic: topic}
	d, err := c.be.DescribeTopic(ctx, topic)
	if err != nil {
		return s, err
	}
	quota := int64(max(1, schemaScanMaxRecords/max(1, int(d.Partitions))))
	var ranges []partRange
	for p := int32(0); p < d.Partitions; p++ {
		earliest, latest, err := c.be.PartitionOffsets(ctx, topic, p)
		if err != nil {
			return s, err
		}
		from := max(earliest, latest-quota)
		if from > earliest {
			s.Unread = append(s.Unread, fmt.Sprintf("%s partition %d: the oldest %s of %s records", topic, p, groupDigits(from-earliest), groupDigits(latest-earliest)))
		}
		if latest > from {
			ranges = append(ranges, partRange{tpKey: tpKey{topic, p}, From: from, To: latest})
		}
	}
	uses := map[int]*idUse{}
	framed := 0
	timedOut, unread, err := c.readRanges(ctx, ranges, schemaScanMaxTime, func(r *kgo.Record) bool {
		s.Scanned++
		at := tpOffset{tpKey: tpKey{r.Topic, r.Partition}, Offset: r.Offset, Set: true}
		switch id, ok := schemaregistry.WireID(r.Value); {
		case r.Value == nil:
			s.Tombstones++
		case !ok:
			s.NotFramed++
		default:
			framed++
			// First and last by timestamp; on a tie the one read later is later (a
			// later offset of the same partition).
			u := uses[id]
			if u == nil {
				u = &idUse{ID: id, First: at, FirstAt: r.Timestamp, Last: at, LastAt: r.Timestamp}
				uses[id] = u
			}
			u.Count++
			if r.Timestamp.Before(u.FirstAt) {
				u.First, u.FirstAt = at, r.Timestamp
			}
			if !r.Timestamp.Before(u.LastAt) {
				u.Last, u.LastAt = at, r.Timestamp
			}
		}
		if framed == 0 && s.NotFramed >= schemaScanProbe {
			s.GaveUp = true
			return false
		}
		return s.Scanned < schemaScanMaxRecords
	}, nil)
	if err != nil {
		return s, err
	}
	s.TimedOut = timedOut
	s.Unread = append(s.Unread, unread...)
	sort.Strings(s.Unread)
	for _, u := range uses {
		s.IDs = append(s.IDs, *u)
	}
	sort.Slice(s.IDs, func(i, j int) bool { return s.IDs[i].ID < s.IDs[j].ID })
	return s, nil
}
