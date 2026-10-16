package console

// The overviews: what a section's home page and its list say about every resource at
// once (Tp-Home, Gr-Home, Sc-Home). They are read from the broker with the requests a
// Kafka admin client makes (Metadata, DescribeConfigs, OffsetFetch, ListOffsets) and
// from the registry; a column whose data the broker cannot give is not drawn.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TopicConfigs reads two settings of every named topic in one DescribeConfigs:
// retention.ms and cleanup.policy, as the broker reports them (its own value, or the
// default in force). A topic the broker would not describe is left out.
func (b *backend) TopicConfigs(ctx context.Context, names []string) map[string]map[string]string {
	out := map[string]map[string]string{}
	if len(names) == 0 {
		return out
	}
	req := kmsg.NewPtrDescribeConfigsRequest()
	for _, n := range names {
		r := kmsg.NewDescribeConfigsRequestResource()
		r.ResourceType = kmsg.ConfigResourceTypeTopic
		r.ResourceName = n
		r.ConfigNames = []string{"retention.ms", "cleanup.policy"}
		req.Resources = append(req.Resources, r)
	}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return out
	}
	for _, res := range resp.Resources {
		if res.ErrorCode != 0 {
			continue
		}
		m := map[string]string{}
		for _, c := range res.Configs {
			if c.Value != nil {
				m[c.Name] = *c.Value
			}
		}
		out[res.ResourceName] = m
	}
	return out
}

// topicRow is one topic as the Topics home and its list show it.
type topicRow struct {
	TopicSummary
	URL         string
	Retention   string // "7 d", "kept per key" for a compacted topic, "no time limit".
	Compact     bool
	ReadBy      []string
	Subject     string // the value subject (or key subject) the registry has for it.
	SubjectURL  string
	DeadLetter  bool // a dead-letter topic that has records in it.
	NeedsALook  bool
	ReadByCount int
	// Size is what the topic takes on disk, when the console knows the data directory
	// (HasSize).
	Size    int64
	HasSize bool
}

// isDeadLetterName says whether a topic is named like a dead-letter queue. It is a
// naming convention (dlq, dead-letter, deadletter, dead.letter, .dead), and the page
// says "named like", not "is": nothing in Kafka marks a topic as one.
func isDeadLetterName(name string) bool {
	n := strings.ToLower(name)
	for _, s := range []string{"dlq", "dead-letter", "deadletter", "dead_letter", "dead.letter", ".dead", "-dead", "_dead"} {
		if strings.HasSuffix(n, s) || strings.Contains(n, s+".") || strings.Contains(n, s+"-") || strings.Contains(n, s+"_") {
			return true
		}
	}
	return false
}

// retentionText is retention.ms as a person reads it: 7 d, 12 h, 90 m, no time limit
// (-1 is no limit by time; retention.bytes may still limit the topic by size).
func retentionText(ms string) string {
	n, err := strconv.ParseInt(strings.TrimSpace(ms), 10, 64)
	if err != nil {
		return ""
	}
	if n < 0 {
		return "no time limit"
	}
	d := time.Duration(n) * time.Millisecond
	switch {
	case d%(24*time.Hour) == 0 && d >= 24*time.Hour:
		return fmt.Sprintf("%d d", d/(24*time.Hour))
	case d%time.Hour == 0 && d >= time.Hour:
		return fmt.Sprintf("%d h", d/time.Hour)
	case d%time.Minute == 0 && d >= time.Minute:
		return fmt.Sprintf("%d m", d/time.Minute)
	case d >= time.Second && d%time.Second == 0:
		return fmt.Sprintf("%d s", d/time.Second)
	}
	return fmt.Sprintf("%d ms", n)
}

// topicRows is every topic with what the home page shows beside it. groups may be
// nil, in which case no topic says who reads it.
func (c *Console) topicRows(ctx context.Context, topics []TopicSummary, groups []groupRow, withConfig bool) []topicRow {
	var cfgs map[string]map[string]string
	if withConfig {
		names := make([]string, len(topics))
		for i, t := range topics {
			names[i] = t.Name
		}
		cfgs = c.be.TopicConfigs(ctx, names)
	}
	subjects := map[string]bool{}
	if c.reg != nil {
		for _, s := range c.reg.SubjectNames(ctx) {
			subjects[s] = true
		}
	}
	readers := map[string][]string{}
	for _, g := range groups {
		for _, t := range g.Topics {
			readers[t] = append(readers[t], g.ID)
		}
	}
	rows := make([]topicRow, 0, len(topics))
	for _, t := range topics {
		r := topicRow{TopicSummary: t, URL: c.topicURL(t.Name)}
		cfg := cfgs[t.Name]
		if strings.Contains(cfg["cleanup.policy"], "compact") {
			r.Compact = true
		}
		switch {
		case r.Compact && !strings.Contains(cfg["cleanup.policy"], "delete"):
			r.Retention = "kept per key"
		default:
			r.Retention = retentionText(cfg["retention.ms"])
		}
		r.ReadBy = readers[t.Name]
		r.ReadByCount = len(r.ReadBy)
		for _, suffix := range []string{"-value", "-key"} {
			if subjects[t.Name+suffix] {
				r.Subject = t.Name + suffix
				r.SubjectURL = c.schemaURL(r.Subject)
				break
			}
		}
		if d, ok := c.topicDisk(t.Name, int(t.Partitions)); ok {
			r.Size, r.HasSize = d.Bytes, true
		}
		r.DeadLetter = isDeadLetterName(t.Name) && t.Msgs > 0
		r.NeedsALook = r.DeadLetter
		rows = append(rows, r)
	}
	return rows
}

// groupRow is one consumer group as the Groups home and its list show it.
type groupRow struct {
	GroupSummary
	URL        string
	Topics     []string // the topics it has committed offsets on, that still exist.
	Lag        int64    // summed over those topics.
	HasOffsets bool
	Generation int32 // from what the coordinator did lately; 0 when it has not said.
	Behind     bool
	Bad        bool
}

// groupRows reads every group's committed offsets and sums its lag against the log
// ends. It asks per group (OffsetFetch has no batch for "every group's offsets" that
// the console can use), so it is for the home page and the list, not for a poll.
func (c *Console) groupRows(ctx context.Context, groups []GroupSummary) []groupRow {
	type tbounds = []PartitionBound
	bounds := map[string]tbounds{}
	boundsOf := func(topic string) tbounds {
		if b, ok := bounds[topic]; ok {
			return b
		}
		var out tbounds
		if n, err := c.be.PartitionCount(ctx, topic); err == nil {
			out, _ = c.be.PartitionBounds(ctx, topic, n)
		}
		bounds[topic] = out
		return out
	}
	rows := make([]groupRow, 0, len(groups))
	for _, g := range groups {
		r := groupRow{GroupSummary: g, URL: c.groupURL(g.ID)}
		offs, err := c.be.GroupOffsets(ctx, g.ID)
		if err == nil {
			seen := map[string]bool{}
			for _, o := range offs {
				b := boundsOf(o.Topic)
				if b == nil || int(o.Partition) >= len(b) {
					continue // the topic is gone: a leftover offset, not lag.
				}
				r.HasOffsets = true
				if !seen[o.Topic] {
					seen[o.Topic] = true
					r.Topics = append(r.Topics, o.Topic)
				}
				r.Lag += max(b[o.Partition].Latest-o.Committed, 0)
			}
			sort.Strings(r.Topics)
		}
		r.Behind = r.Lag > 0
		if c.traffic != nil {
			evs := c.traffic.GroupEvents(g.ID, 0)
			for i := len(evs) - 1; i >= 0; i-- {
				if evs[i].Generation > 0 {
					r.Generation = evs[i].Generation
					break
				}
			}
		}
		rows = append(rows, r)
	}
	return rows
}

// num writes a count with thousands separators, as the boards do (14,407).
func num(v any) string {
	switch n := v.(type) {
	case int:
		return groupDigits(int64(n))
	case int32:
		return groupDigits(int64(n))
	case int64:
		return groupDigits(n)
	case uint64:
		return groupDigits(int64(n))
	case int16:
		return groupDigits(int64(n))
	}
	return fmt.Sprint(v)
}
