package console

import (
	"fmt"
	"hash/fnv"
	"net/http"
	"sort"
	"strings"
)

func (c *Console) groups(w http.ResponseWriter, r *http.Request) {
	list, err := c.be.ListGroups(r.Context())
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.render(w, r, "groups_home", map[string]any{"List": list, "Page": "groups"})
}

// groupLagRow is one topic-partition's committed-vs-latest offset, dressed
// up for the table: lag is computed here, not by the broker (there's no
// server-side lag field — see GroupOffsets's doc comment).
type groupLagRow struct {
	Topic      string
	TopicURL   string
	Partition  int32
	Owner      string // which member holds the partition now, if any.
	Committed  int64
	Latest     int64
	Lag        int64
	LagPercent int
	Deleted    bool // the topic no longer exists; its offset is a leftover.
	Ahead      bool // committed is beyond the log end (the topic was purged or recreated).
	AheadBy    int64
	Trimmed    bool // committed is before the oldest retained offset: records were lost to retention.
}

// memberView is a group member, ready to show.
type memberView struct {
	ID       string
	ClientID string
	Host     string
	Label    string // the name a person would recognise: the client.id, else a short member id.
	Assigned []assignView
}

type assignView struct {
	Topic    string
	TopicURL string
	Parts    string
}

// partRanges writes partitions compactly: [0 1 2 5 7 8] -> "0–2, 5, 7–8". The
// console used to print the Go slice, "[0 1 2 5 7 8]".
func partRanges(ps []int32) string {
	if len(ps) == 0 {
		return ""
	}
	s := append([]int32(nil), ps...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	var out []string
	start, prev := s[0], s[0]
	flush := func() {
		if start == prev {
			out = append(out, fmt.Sprint(start))
		} else {
			out = append(out, fmt.Sprintf("%d–%d", start, prev))
		}
	}
	for _, p := range s[1:] {
		if p == prev+1 {
			prev = p
			continue
		}
		flush()
		start, prev = p, p
	}
	flush()
	return strings.Join(out, ", ")
}

func shortID(id string) string {
	if len(id) > 14 {
		return id[:6] + "…" + id[len(id)-6:]
	}
	return id
}

// groupData gathers everything the group page shows. The page and its live poll
// both use it, so they can never disagree about what a group looks like.
func (c *Console) groupData(r *http.Request, id string) (map[string]any, error) {
	ctx := r.Context()
	detail, err := c.be.DescribeGroup(ctx, id)
	if err != nil {
		return nil, err
	}
	offsets, err := c.be.GroupOffsets(ctx, id)
	if err != nil {
		return nil, err
	}

	owner := map[string]string{} // "topic/partition" -> member label.
	members := make([]memberView, 0, len(detail.Members))
	for _, m := range detail.Members {
		mv := memberView{ID: m.ID, ClientID: m.ClientID, Host: m.Host, Label: m.ClientID}
		if mv.Label == "" {
			mv.Label = shortID(m.ID)
		}
		topics := make([]string, 0, len(m.Assigned))
		for t := range m.Assigned {
			topics = append(topics, t)
		}
		sort.Strings(topics)
		for _, t := range topics {
			mv.Assigned = append(mv.Assigned, assignView{Topic: t, TopicURL: c.topicURL(t), Parts: partRanges(m.Assigned[t])})
			for _, p := range m.Assigned[t] {
				owner[fmt.Sprintf("%s/%d", t, p)] = mv.Label
			}
		}
		members = append(members, mv)
	}

	rows := make([]groupLagRow, 0, len(offsets))
	var totalLag int64
	var anyAhead bool
	bounds := map[string][]PartitionBound{}
	for _, o := range offsets {
		row := groupLagRow{Topic: o.Topic, TopicURL: c.topicURL(o.Topic), Partition: o.Partition, Committed: o.Committed,
			Owner: owner[fmt.Sprintf("%s/%d", o.Topic, o.Partition)]}
		if _, ok := bounds[o.Topic]; !ok {
			if n, err := c.be.PartitionCount(ctx, o.Topic); err == nil {
				bounds[o.Topic], _ = c.be.PartitionBounds(ctx, o.Topic, n)
			} else {
				bounds[o.Topic] = nil
			}
		}
		b := bounds[o.Topic]
		if b == nil || int(o.Partition) >= len(b) {
			// The offset outlived its topic. Say so; dropping the row hid a group's
			// leftover state from the one place that could explain it.
			row.Deleted = true
			rows = append(rows, row)
			continue
		}
		row.Latest = b[o.Partition].Latest
		switch {
		case o.Committed > row.Latest:
			row.Ahead, row.AheadBy = true, o.Committed-row.Latest
			anyAhead = true
		default:
			row.Lag = row.Latest - o.Committed
		}
		row.Trimmed = o.Committed < b[o.Partition].Earliest
		totalLag += row.Lag
		if row.Latest > 0 {
			row.LagPercent = int(row.Lag * 100 / row.Latest)
			if row.Lag > 0 && row.LagPercent < 6 {
				row.LagPercent = 6 // keep a sliver visible for "behind but barely"
			}
		}
		rows = append(rows, row)
	}

	h := fnv.New64a()
	fmt.Fprint(h, detail.State, "|")
	for _, m := range members {
		fmt.Fprint(h, m.ID, m.ClientID, m.Host, m.Assigned, ";")
	}
	for _, o := range rows {
		// Everything a row shows: a record trimmed by retention, a topic deleted,
		// or a partition changing hands must refresh the page, not answer 204.
		fmt.Fprint(h, o.Topic, o.Partition, o.Committed, o.Latest, o.Trimmed, o.Deleted, o.Ahead, o.Owner, ",")
	}

	return map[string]any{
		"Group": detail, "Members": members, "Offsets": rows, "TotalLag": totalLag, "AnyAhead": anyAhead,
		"CanAct":    detail.State == "Empty" && len(detail.Members) == 0,
		"NoCommits": len(offsets) == 0,
		"Hash":      fmt.Sprintf("%x", h.Sum64()),
	}, nil
}

func (c *Console) groupDetail(w http.ResponseWriter, r *http.Request) {
	id := groupFromSegment(r.PathValue("id"))
	data, err := c.groupData(r, id)
	if err != nil {
		c.failResource(w, r, err, "group", id)
		return
	}
	list, _ := c.be.ListGroups(r.Context())
	data["List"], data["Page"], data["Title"] = list, "groups", id
	c.render(w, r, "group_detail", data)
}

// groupLive is the live-polled partial behind the group page: members, offsets and
// lag. Lag is the number someone watches a group for, and used to need a refresh.
func (c *Console) groupLive(w http.ResponseWriter, r *http.Request) {
	data, err := c.groupData(r, groupFromSegment(r.PathValue("id")))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if liveUnchanged(w, r, data["Hash"].(string)) {
		return
	}
	c.partial(w, "group_live", data)
}

// groupReset moves an inactive group's committed offsets to the earliest or latest
// position and re-renders the page's live region.
func (c *Console) groupReset(w http.ResponseWriter, r *http.Request) {
	id, to := groupFromSegment(r.PathValue("id")), r.FormValue("to")
	n, err := c.be.ResetGroupOffsets(r.Context(), id, to)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	toast(w, fmt.Sprintf("Reset %d offset%s to %s", n, plural(n), to))
	data, err := c.groupData(r, id)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.partial(w, "group_live", data)
}

func (c *Console) groupDelete(w http.ResponseWriter, r *http.Request) {
	id := groupFromSegment(r.PathValue("id"))
	if err := c.be.DeleteGroup(r.Context(), id); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.prefix+"/groups", "Group deleted")
}

// topicGroups is the lazily loaded "who consumes this topic" strip on a topic page.
func (c *Console) topicGroups(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	consumers, err := c.be.TopicConsumers(r.Context(), name)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	type row struct {
		TopicConsumer
		URL string
	}
	rows := make([]row, len(consumers))
	for i, tc := range consumers {
		rows[i] = row{tc, c.groupURL(tc.Group)}
	}
	c.partial(w, "topic_groups", map[string]any{"Consumers": rows})
}
