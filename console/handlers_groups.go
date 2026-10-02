package console

import "net/http"

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
	Partition  int32
	Committed  int64
	Latest     int64
	Lag        int64
	LagPercent int
}

func (c *Console) groupDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()

	list, _ := c.be.ListGroups(ctx)
	detail, err := c.be.DescribeGroup(ctx, id)
	if err != nil {
		c.failResource(w, r, err, "group", id)
		return
	}
	offsets, err := c.be.GroupOffsets(ctx, id)
	if err != nil {
		c.fail(w, r, err)
		return
	}

	rows := make([]groupLagRow, 0, len(offsets))
	var totalLag int64
	for _, o := range offsets {
		_, latest, err := c.be.PartitionOffsets(ctx, o.Topic, o.Partition)
		if err != nil {
			continue // topic may have been deleted since the offset was committed
		}
		lag := max(latest-o.Committed, 0)
		totalLag += lag
		pct := 0
		if latest > 0 {
			pct = int(lag * 100 / latest)
			if lag > 0 && pct < 6 {
				pct = 6 // keep a sliver visible for "behind but barely"
			}
		}
		rows = append(rows, groupLagRow{Topic: o.Topic, Partition: o.Partition, Committed: o.Committed, Latest: latest, Lag: lag, LagPercent: pct})
	}

	c.render(w, r, "group_detail", map[string]any{
		"List": list, "Group": detail, "Offsets": rows, "TotalLag": totalLag, "Page": "groups", "Title": id,
	})
}
