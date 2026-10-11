package console

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Searching a topic's records.
//
// Kafka has no index over what records contain, so a search is a scan: the
// console reads records and looks at each one. It reads the newest first, a
// bounded number of them, and says how far it got, so "no matches" is never
// mistaken for "not in the topic". One partition can be searched further back a
// step at a time; all partitions together are searched over their newest records.

const (
	searchBudget     = 5000 // records read per search, across the partitions searched.
	searchChunk      = 1000 // records fetched at a time.
	searchMaxHits    = 100  // matches shown.
	searchMaxParts   = 50   // partitions one search covers.
	searchMinPerPart = 100  // the least a partition gets of the budget.
)

// searchView is what a search found, ready to show.
type searchView struct {
	Query    string
	All      bool // every partition, not just the one on screen.
	Rows     []msgView
	Note     string // what was read: which offsets, how many records, how many matched.
	OlderURL string // the same search continued further back, if there is more.
	ClearURL string
}

// recordMatches reports whether a record's key, value or headers contain needle
// (already lower-cased). The empty needle matches everything.
func recordMatches(m Message, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	if bytes.Contains(bytes.ToLower(m.Key), needle) || bytes.Contains(bytes.ToLower(m.Value), needle) {
		return true
	}
	for _, h := range m.Headers {
		if bytes.Contains(bytes.ToLower([]byte(h.Key)), needle) || bytes.Contains(bytes.ToLower(h.Value), needle) {
			return true
		}
	}
	return false
}

// scanPartition reads up to budget records of one partition ending just before
// `before` (-1 = the end of the log), newest chunk first, and returns the matches
// with the range it read: [from, to).
func (c *Console) scanPartition(ctx context.Context, topic string, partition int32, needle []byte, before int64, budget int) (hits []Message, from, to, earliest int64, err error) {
	earliest, latest, err := c.be.PartitionOffsets(ctx, topic, partition)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	to = latest
	if before >= 0 && before < latest {
		to = max(before, earliest)
	}
	from = to
	for from > earliest && int(to-from) < budget && len(hits) < searchMaxHits {
		n := min(int64(searchChunk), from-earliest, int64(budget)-(to-from))
		msgs, err := c.be.FetchWindow(ctx, topic, partition, from-n, int(n))
		if err != nil {
			return nil, 0, 0, 0, err
		}
		from -= n
		for i := len(msgs) - 1; i >= 0; i-- { // newest first.
			if recordMatches(msgs[i], needle) {
				hits = append(hits, msgs[i])
			}
		}
	}
	return hits, from, to, earliest, nil
}

// searchTopic runs a search over one partition or all of them.
func (c *Console) searchTopic(ctx context.Context, topic string, partition, partitionCount int32, q string, all bool, before int64) (*searchView, error) {
	needle := []byte(strings.ToLower(q))
	base := c.topicURL(topic) + "?partition=" + strconv.Itoa(int(partition))
	sv := &searchView{Query: q, All: all, ClearURL: base}

	var hits []Message
	var read int64
	what := "matches"
	if q == "" {
		what = "records"
	}
	if !all {
		found, from, to, earliest, err := c.scanPartition(ctx, topic, partition, needle, before, searchBudget)
		if err != nil {
			return nil, err
		}
		hits, read = found, to-from
		full := len(hits) >= searchMaxHits
		if full {
			hits = hits[:searchMaxHits]
		}
		switch {
		case read == 0:
			sv.Note = fmt.Sprintf("Partition %d has no records in that range.", partition)
		default:
			sv.Note = fmt.Sprintf("Read offsets %d–%d of partition %d (%d record%s): %d %s.", from, to-1, partition, read, plural(int(read)), len(hits), what)
		}
		if from > earliest || full {
			sv.OlderURL = base + "&q=" + url.QueryEscape(q) + "&before=" + strconv.FormatInt(from, 10)
			if full {
				// Stopped at the limit: continue from just below the oldest match shown.
				sv.OlderURL = base + "&q=" + url.QueryEscape(q) + "&before=" + strconv.FormatInt(hits[len(hits)-1].Offset, 10)
			}
		}
	} else {
		parts := min(partitionCount, searchMaxParts)
		per := max(searchBudget/int(parts), searchMinPerPart)
		partial := false
		for p := int32(0); p < parts; p++ {
			found, from, to, earliest, err := c.scanPartition(ctx, topic, p, needle, -1, per)
			if err != nil {
				return nil, err
			}
			hits = append(hits, found...)
			read += to - from
			partial = partial || from > earliest
		}
		sort.SliceStable(hits, func(i, j int) bool {
			if !hits[i].Timestamp.Equal(hits[j].Timestamp) {
				return hits[i].Timestamp.After(hits[j].Timestamp)
			}
			if hits[i].Partition != hits[j].Partition {
				return hits[i].Partition < hits[j].Partition
			}
			return hits[i].Offset > hits[j].Offset
		})
		sv.Note = fmt.Sprintf("Read %d record%s across %d partition%s: %d %s, newest first.", read, plural(int(read)), parts, plural(int(parts)), len(hits), what)
		if partial {
			sv.Note += fmt.Sprintf(" Only the newest %d of each partition were read; search one partition to go further back.", per)
		}
		if partitionCount > parts {
			sv.Note += fmt.Sprintf(" The topic has %d partitions; the first %d were searched.", partitionCount, parts)
		}
	}
	if len(hits) > searchMaxHits {
		hits = hits[:searchMaxHits]
		sv.Note += fmt.Sprintf(" Showing the newest %d.", searchMaxHits)
	}
	sv.Rows = make([]msgView, len(hits))
	for i, m := range hits {
		sv.Rows[i] = c.msgView(ctx, topic, m)
	}
	return sv, nil
}

// newMsgView dresses a record for a table row.
func newMsgView(m Message) msgView {
	return msgView{
		Offset: m.Offset, Partition: m.Partition, Timestamp: m.Timestamp, TimeStr: m.Timestamp.Format("15:04:05"),
		TimeFull:   m.Timestamp.Format(time.RFC3339),
		Key:        previewOnly(m.Key), // the table shows one line; the inspector does the rest.
		Value:      previewOnly(m.Value),
		Headers:    m.Headers,
		TickHeight: tickHeight(len(m.Value)),
	}
}

// searchParams reads a search from the query string: the text, whether it covers
// every partition, and where to continue from (-1 = the end of the log).
func searchParams(r *http.Request) (q string, all bool, before int64, asked bool) {
	q = strings.TrimSpace(r.URL.Query().Get("q"))
	all = r.URL.Query().Get("all") == "1"
	before = -1
	if n, err := strconv.ParseInt(r.URL.Query().Get("before"), 10, 64); err == nil && n >= 0 {
		before = n
	}
	return q, all, before, q != "" || all
}
