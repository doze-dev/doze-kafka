package console

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Tp-07 and Tp-09: "Null is not empty". A null key or value and an empty one are two
// different things on the wire, and a person looking at a record needs to see which one
// it is and where the other kind sits. The offsets are the nearest of the records the
// page read around this one, and the page says that; it does not scan the topic.

// legendWindow is how many offsets either side of a record the legend reads.
const legendWindow = 100

// historyBudget is the most records read for one key's history (Tp-09).
const historyBudget = 3000

// topicConfigValues is the value the broker reports for each named config of a topic.
func (b *backend) topicConfigValues(ctx context.Context, topic string, names ...string) map[string]string {
	out := map[string]string{}
	req := kmsg.NewPtrDescribeConfigsRequest()
	r := kmsg.NewDescribeConfigsRequestResource()
	r.ResourceType, r.ResourceName, r.ConfigNames = kmsg.ConfigResourceTypeTopic, topic, names
	req.Resources = append(req.Resources, r)
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil || len(resp.Resources) != 1 || resp.Resources[0].ErrorCode != 0 {
		return out
	}
	for _, c := range resp.Resources[0].Configs {
		if c.Value != nil {
			out[c.Name] = *c.Value
		}
	}
	return out
}

// legendRow is one row of the "Null is not empty" card.
type legendRow struct {
	Label  string // null, "" (empty), or the record's own bytes.
	Class  string // "i mut" for the two that are not text.
	Note   string
	Offset string // "offset 699", "this record", "never" or "none near".
	This   bool
	Deny   bool
}

// legendView is the card and what it was read from.
type legendView struct {
	Cap  string // "Null is not empty".
	Rows []legendRow
	Note string
}

// nearest is the offset in msgs closest to at (not at itself) whose record satisfies is.
func nearest(msgs []Message, at int64, is func(Message) bool) (int64, bool) {
	best, found := int64(0), false
	for _, m := range msgs {
		if m.Offset == at || !is(m) {
			continue
		}
		d, bd := abs64(m.Offset-at), abs64(best-at)
		if !found || d < bd {
			best, found = m.Offset, true
		}
	}
	return best, found
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// recordLegend builds the card for a record: about its key when the key is null or empty
// or the value is binary, about its value when the value is null or empty. nil when the
// record has nothing to tell apart.
func (c *Console) recordLegend(ctx context.Context, topic string, partition int32, m Message, key, val bytesView, compacted bool) *legendView {
	about := ""
	switch {
	case val.Null || val.Empty:
		about = "value"
	case val.Binary || key.Null || key.Empty:
		about = "key"
	default:
		return nil
	}
	from := max(m.Offset-legendWindow, 0)
	if earliest, _, err := c.be.PartitionOffsets(ctx, topic, partition); err == nil {
		from = max(from, earliest)
	}
	win, _ := c.be.FetchWindow(ctx, topic, partition, from, 2*legendWindow+1)
	at := func(null bool, field func(Message) []byte) string {
		off, ok := nearest(win, m.Offset, func(x Message) bool {
			b := field(x)
			if null {
				return b == nil
			}
			return b != nil && len(b) == 0
		})
		if !ok {
			return "none near"
		}
		return "offset " + groupDigits(off)
	}
	get := func(f func(Message) []byte) func(Message) []byte { return f }
	keyOf, valOf := get(func(x Message) []byte { return x.Key }), get(func(x Message) []byte { return x.Value })
	lv := &legendView{Cap: "Null is not empty", Note: fmt.Sprintf("Offsets are the nearest of the %d records read around this one.", len(win))}
	if about == "value" {
		null := legendRow{Label: "null", Class: "b", Note: "no value at all: a tombstone. Deletes the key when a compacted topic is cleaned.", Offset: at(true, valOf), Deny: true}
		empty := legendRow{Label: "empty", Class: "b i", Note: "zero bytes. A normal record, kept as the key's latest value.", Offset: at(false, valOf)}
		if val.Null {
			null.Offset, null.This = "this record", true
		} else {
			empty.Offset, empty.This = "this record", true
		}
		lv.Rows = []legendRow{null, empty}
		return lv
	}
	null := legendRow{Label: "null", Class: "i mut", Note: "no key at all.", Offset: at(true, keyOf)}
	if compacted {
		null.Note += " Cannot exist on a compacted topic: it refuses a record with no key (INVALID_RECORD)."
		null.Offset = "never"
	}
	empty := legendRow{Label: `"" (empty)`, Class: "i mut", Note: "a key of zero bytes, hashed like any key.", Offset: at(false, keyOf)}
	rows := []legendRow{null, empty}
	switch {
	case key.Null:
		rows[0].Offset, rows[0].This = "this record", true
	case key.Empty:
		rows[1].Offset, rows[1].This = "this record", true
	default:
		label := key.Preview
		if !key.Binary {
			label = key.Text
		}
		rows = append(rows, legendRow{Label: label, Class: "", Note: fmt.Sprintf("this record: %d bytes", key.Size), Offset: "offset " + groupDigits(m.Offset), This: true})
	}
	lv.Rows = rows
	return lv
}

// historyRow is one record of a key, in the order they were written.
type historyRow struct {
	Offset string
	Value  string
	Note   string
	Deny   bool
}

// keyHistory is "what compaction will do with this key" (Tp-09): the records of the key
// that are still on disk, oldest first, and what the cleaner does with each. Records it
// has already removed are offsets that are not there, and cannot be listed.
type keyHistory struct {
	Key       string
	Rows      []historyRow
	Read      int    // records read to build it.
	Retention string // delete.retention.ms, as the broker reports it.
}

func (c *Console) keyHistory(ctx context.Context, topic string, partition int32, m Message, key bytesView) *keyHistory {
	earliest, _, err := c.be.PartitionOffsets(ctx, topic, partition)
	if err != nil {
		return nil
	}
	from := max(earliest, m.Offset-historyBudget+1)
	var all []Message
	for next := from; next <= m.Offset && len(all) < historyBudget; {
		chunk, err := c.be.FetchWindow(ctx, topic, partition, next, min(1000, int(m.Offset-next)+1))
		if err != nil || len(chunk) == 0 {
			break
		}
		all = append(all, chunk...)
		next = chunk[len(chunk)-1].Offset + 1
	}
	cfg := c.be.topicConfigValues(ctx, topic, "delete.retention.ms")
	h := &keyHistory{Key: key.Text, Read: len(all), Retention: showDuration(cfg["delete.retention.ms"])}
	if cfg["delete.retention.ms"] == "" {
		h.Retention = ""
	}
	for _, x := range all {
		if string(x.Key) != string(m.Key) || x.Key == nil {
			continue
		}
		r := historyRow{Offset: groupDigits(x.Offset)}
		switch {
		case x.Offset == m.Offset && x.Value == nil:
			r.Value, r.Deny = "tombstone", true
			r.Note = "kept until its segment is cleaned and it is older than delete.retention.ms"
			if h.Retention != "" {
				r.Note = "kept until its segment is cleaned and it is older than delete.retention.ms (" + h.Retention + "), then goes"
			}
		case x.Value == nil:
			r.Value, r.Note = "tombstone", "an earlier delete of this key"
		default:
			r.Value = oneLine(string(x.Value), 40)
			if x.Offset < m.Offset {
				r.Note = "goes once cleaned"
			}
		}
		h.Rows = append(h.Rows, r)
	}
	return h
}

func (h *keyHistory) Summary() string {
	return strings.TrimSpace(fmt.Sprintf("Read %d records up to this one.", h.Read))
}
