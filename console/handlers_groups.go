package console

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// groups is the Groups home (Gr-Home): every group, how far behind each is, and which
// need a look.
func (c *Console) groups(w http.ResponseWriter, r *http.Request) {
	data, err := c.groupsHomeData(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	data["LiveHash"], _ = c.liveHash("groups_live", data)
	if len(data["Groups"].([]groupRow)) == 0 {
		data["View"] = "groups_none"
	}
	c.render(w, r, "groups_home", data)
}

// groupsLive is Gr-Home's summary, table and callouts, polled every 3 s as the board
// says ("refreshes every 3 s").
func (c *Console) groupsLive(w http.ResponseWriter, r *http.Request) {
	data, err := c.groupsHomeData(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.livePartial(w, r, "groups_live", data)
}

func (c *Console) groupsHomeData(r *http.Request) (map[string]any, error) {
	list, err := c.be.ListGroups(r.Context())
	if err != nil {
		return nil, err
	}
	rows := c.groupRows(r.Context(), list)
	var withMembers, members int
	var lag int64
	var need []groupRow
	for _, g := range rows {
		members += g.Members
		if g.Members > 0 {
			withMembers++
		}
		lag += g.Lag
		if g.Members == 0 && g.Lag > 0 {
			need = append(need, g)
		}
	}
	// Gr-01's commands name this broker and, when it has one, a topic that exists.
	sample := "<topic>"
	if tps, err := c.be.ListTopics(r.Context()); err == nil {
		for _, t := range tps {
			if !strings.HasPrefix(t.Name, "__") {
				sample = t.Name
				break
			}
		}
	}
	return map[string]any{"Groups": rows, "Page": "groups", "Title": "Groups", "Bootstrap": c.be.displayAddr(), "SampleTopic": sample,
		"WithMembers": withMembers, "MemberCount": members, "TotalLag": lag, "NeedALook": need}, nil
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
	Earliest   int64
	Latest     int64
	Lag        int64
	LagPercent int
	Deleted    bool // the topic no longer exists; its offset is a leftover.
	Ahead      bool // committed is beyond the log end (the topic was purged or recreated).
	AheadBy    int64
	Trimmed    bool // committed is before the oldest retained offset: records were lost to retention.
}

// tryRow is the partition the Try it form starts on: the first one the group has an
// offset for, or none (the fields then start empty).
func tryRow(rows []groupLagRow) groupLagRow {
	for _, r := range rows {
		if !r.Deleted {
			return r
		}
	}
	return groupLagRow{}
}

// memberView is a group member, ready to show.
type memberView struct {
	ID       string
	ClientID string
	Host     string
	Label    string // the name a person would recognise: the client.id, else a short member id.
	Assigned []assignView
	// From the coordinator's snapshot (GRP-22); empty where the broker holds nothing:
	// a consumer-protocol (KIP-848) member has no protocols, session timeout or leader.
	Key          string // the inspector's key for it (?open=member:<key>).
	Snap         bool
	Addr         string // host:port of its connection.
	Leader       bool
	Software     string
	Principal    string
	Protocols    []string
	Session      string // "45 s", or "" when the member has none.
	Rebalance    string
	Joined       time.Time
	Heartbeat    time.Time
	HeartbeatAgo string // "2 s ago".
	HeartbeatOK  bool   // inside its session timeout.
}

// secondsText writes a timeout the way the boards do: "45 s".
func secondsText(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	if d%time.Second == 0 {
		return fmt.Sprintf("%d s", int(d/time.Second))
	}
	return fmt.Sprintf("%.1f s", d.Seconds())
}

// ageText is how long ago, to the second under a minute: "2 s ago".
func ageText(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	d := time.Since(t)
	switch {
	case d < time.Second:
		return "0 s ago"
	case d < time.Minute:
		return fmt.Sprintf("%d s ago", int(d/time.Second))
	}
	return agoText(d)
}

// snapshotMember fills a member from the coordinator's memory, if it holds it.
func (mv *memberView) snapshotMember(s MemberSnapshot) {
	mv.Snap = true
	mv.Addr, mv.Leader, mv.Software, mv.Principal = s.Addr, s.Leader, s.Software, s.Principal
	mv.Protocols = s.Protocols
	mv.Session, mv.Rebalance = secondsText(s.SessionTimeout), secondsText(s.RebalanceTimeout)
	mv.Joined, mv.Heartbeat = s.Joined, s.LastHeartbeat
	mv.HeartbeatAgo = ageText(s.LastHeartbeat)
	mv.HeartbeatOK = s.SessionTimeout > 0 && !s.LastHeartbeat.IsZero() && time.Since(s.LastHeartbeat) < s.SessionTimeout
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

	tab := r.FormValue("tab")
	switch tab {
	case "members", "timeline", "offsets":
	default:
		tab = "lag"
	}
	insp := c.inspectGroup(id)
	snap := map[string]MemberSnapshot{}
	if insp.State == InspectOK {
		for _, ms := range insp.Group.Members {
			snap[ms.ID] = ms
		}
	}
	owner := map[string]string{} // "topic/partition" -> member id.
	members := make([]memberView, 0, len(detail.Members))
	for _, m := range detail.Members {
		mv := memberView{ID: m.ID, ClientID: m.ClientID, Host: m.Host, Label: m.ClientID}
		if mv.Label == "" {
			mv.Label = shortID(m.ID)
		}
		mv.Key = joinKey(id, m.ID)
		if ms, ok := snap[m.ID]; ok {
			mv.snapshotMember(ms)
		}
		topics := make([]string, 0, len(m.Assigned))
		for t := range m.Assigned {
			topics = append(topics, t)
		}
		sort.Strings(topics)
		for _, t := range topics {
			mv.Assigned = append(mv.Assigned, assignView{Topic: t, TopicURL: c.topicURL(t), Parts: partRanges(m.Assigned[t])})
			for _, p := range m.Assigned[t] {
				owner[fmt.Sprintf("%s/%d", t, p)] = mv.ID
			}
		}
		members = append(members, mv)
	}

	rows := make([]groupLagRow, 0, len(offsets))
	var totalLag, totalCommitted, totalLogEnd int64
	var anyAhead bool
	var largest *groupLagRow
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
		row.Earliest, row.Latest = b[o.Partition].Earliest, b[o.Partition].Latest
		switch {
		case o.Committed > row.Latest:
			row.Ahead, row.AheadBy = true, o.Committed-row.Latest
			anyAhead = true
		default:
			row.Lag = row.Latest - o.Committed
		}
		row.Trimmed = o.Committed < b[o.Partition].Earliest
		totalLag += row.Lag
		totalCommitted += row.Committed
		totalLogEnd += row.Latest
		if row.Lag > 0 && (largest == nil || row.Lag > largest.Lag) {
			cp := row
			largest = &cp
		}
		if row.Latest > 0 {
			row.LagPercent = int(row.Lag * 100 / row.Latest)
			if row.Lag > 0 && row.LagPercent < 6 {
				row.LagPercent = 6 // keep a sliver visible for "behind but barely"
			}
		}
		rows = append(rows, row)
	}

	now := time.Now()
	show := r.FormValue("show")
	if _, ok := timelineKinds[show]; !ok {
		show = "all"
	}
	var tlv timelineView
	if c.traffic != nil {
		tlv = buildTimeline(c.traffic.GroupEvents(id, 0), show, now, c.groupURL(id))
	}
	timeline := tlv.Rows
	rebalance := rebalanceOf(insp, now)
	var noCommits []noCommitRow
	if len(offsets) == 0 && len(members) > 0 {
		for _, m := range members {
			for _, a := range m.Assigned {
				if _, ok := bounds[a.Topic]; !ok {
					if n, err := c.be.PartitionCount(ctx, a.Topic); err == nil {
						bounds[a.Topic], _ = c.be.PartitionBounds(ctx, a.Topic, n)
					}
				}
			}
		}
		noCommits = c.noCommitRows(detail.Members, members, bounds)
	}
	leftTopics, leftN := leftoverTopics(rows)
	var held *heldView
	var expiry *expiryView
	var denied *deniedView
	if tab == "lag" {
		held = heldBackOf(c, ctx, rows, now)
		expiry = c.expiryOf(id, len(rows) > 0, now)
		denied = c.deniedOf(ctx, id, now)
	}

	h := fnv.New64a()
	fmt.Fprint(h, detail.State, "|")
	for _, m := range members {
		fmt.Fprint(h, m.ID, m.ClientID, m.Host, m.Addr, m.Assigned, m.HeartbeatAgo, ";")
	}
	for _, o := range rows {
		// Everything a row shows: a record trimmed by retention, a topic deleted,
		// or a partition changing hands must refresh the page, not answer 204.
		fmt.Fprint(h, o.Topic, o.Partition, o.Committed, o.Latest, o.Trimmed, o.Deleted, o.Ahead, o.Owner, ",")
	}

	for _, e := range timeline {
		fmt.Fprint(h, e.Seq, ";")
	}
	fmt.Fprint(h, "|", show, len(noCommits))
	if held != nil {
		fmt.Fprint(h, "|held", held.Total, len(held.Txns))
	}
	if expiry != nil {
		fmt.Fprint(h, "|exp", expiry.In, expiry.Used, expiry.None, expiry.Forever)
	}
	if denied != nil {
		fmt.Fprint(h, "|den", denied.Count, denied.At.UnixNano())
	}
	if rebalance != nil {
		fmt.Fprint(h, "|rb", rebalance.Waiting, rebalance.Left)
		for _, rr := range rebalance.Rows {
			fmt.Fprint(h, rr.ID, rr.Rejoined, rr.HeartbeatAgo)
		}
	}
	handoffs := handoffsOf(insp)
	for _, ho := range handoffs {
		fmt.Fprint(h, "|ho", ho.ID, ho.State, ho.Epoch, ho.Target, ho.Current)
	}

	// The generation is what the coordinator holds now (the snapshot); only a console
	// that cannot see the coordinator falls back to the last one its recorder saw.
	var generation int32
	genLabel := "generation"
	if insp.State == InspectOK {
		generation = insp.Group.Generation
		if generation == 0 && insp.Group.Epoch > 0 {
			generation, genLabel = insp.Group.Epoch, "epoch"
		}
	} else if c.traffic != nil {
		evs := c.traffic.GroupEvents(id, 0)
		for i := len(evs) - 1; i >= 0; i-- {
			if evs[i].Generation > 0 {
				generation = evs[i].Generation
				break
			}
		}
	}
	topics := offsetTopics(rows)
	g := NewGraph()
	g.AddGroupReads(id, topics)
	clients := make([]string, 0, len(members))
	for _, m := range members {
		clients = append(clients, m.ClientID)
	}
	g.AddGroupMembers(id, clients)
	if c.reg != nil {
		g.AddSubjects(c.reg.SubjectNames(r.Context()), topics)
	}
	if c.traffic != nil {
		g.AddTraffic(c.traffic.Events(TrafficFilter{Group: id}, 0))
	}
	fmt.Fprint(h, "|", tab, generation)

	tl := func(key, label, count string) tabLink {
		u := c.groupURL(id)
		if key != "lag" {
			u += "?tab=" + key
		}
		return tabLink{Key: key, Label: label, URL: u, On: tab == key, Count: count}
	}
	tabs := []tabLink{tl("lag", "Lag", ""), tl("members", "Members", strconv.Itoa(len(members))), tl("timeline", "Timeline", ""), tl("offsets", "Offsets", "")}

	canAct := detail.State == "Empty" && len(detail.Members) == 0
	facts := groupFacts(insp, members)
	out := map[string]any{
		"Timeline": timeline, "HasTimeline": c.traffic != nil, "TL": tlv, "Rebalance": rebalance, "NoCommitRows": noCommits,
		"Ahead": aheadOf(rows), "Held": held, "Expiry": expiry, "Denied": denied, "LeftTopics": leftTopics, "LeftN": leftN, "Handoffs": handoffs, "ClassicFacts": classicOf(insp), "Group": detail, "Members": members, "Offsets": rows, "TotalLag": totalLag, "AnyAhead": anyAhead,
		"TotalCommitted": totalCommitted, "TotalLogEnd": totalLogEnd, "Generation": generation, "GenLabel": genLabel,
		"Facts":     facts,
		"Topics":    topics,
		"Try":       tryRow(rows),
		"Confirm":   groupConfirms(id, rows, len(members)),
		"CanAct":    canAct,
		"NoCommits": len(offsets) == 0,
		"Hash":      fmt.Sprintf("%x", h.Sum64()),
		"Tab":       tab, "Tabs": tabs, "View": groupView(tab),
		"Neighbours": c.neighbourClauses(g, NodeGroup, id),
	}
	if largest != nil {
		out["Largest"] = largest
	}
	if tab == "offsets" {
		// Gr-06: the reset form and its preview; Gr-07: with members, the refusal and the way out.
		switch {
		case len(members) > 0:
			out["Refusal"] = refusalFor(&groupNotEmpty{Group: id, Members: len(members)})
			out["Steps"] = stepsOf(members, facts, detail.Protocol)
		case canAct && len(rows) > 0:
			out["Reset"] = c.resetFormOf(r, id, rows)
		}
	}
	return out, nil
}

func (c *Console) groupDetail(w http.ResponseWriter, r *http.Request) {
	id := groupFromSegment(r.PathValue("id"))
	data, err := c.groupData(r, id)
	if err != nil {
		c.failResource(w, r, err, "group", id)
		return
	}
	list, _ := c.be.ListGroups(r.Context())
	data["Groups"], data["Page"], data["Title"] = c.groupRows(r.Context(), list), "groups", id
	// The group page has no pause control of its own, so its dock carries one, and
	// says so to its poll.
	maps.Copy(data, c.dockPage(dockView{DockFilter: DockFilter{Group: id}, Toggle: true}))
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

// groupReset moves an inactive group's committed offsets: to the earliest or latest
// position, to a time (the first record at or after it) or to the offsets typed in the
// rows; all of them, or those on one topic. A group with members is refused (Gr-07).
func (c *Console) groupReset(w http.ResponseWriter, r *http.Request) {
	id, topic := groupFromSegment(r.PathValue("id")), r.FormValue("topic")
	spec, err := resetSpecOf(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	n, err := c.be.ApplyReset(r.Context(), id, spec)
	var gne *groupNotEmpty
	if errors.As(err, &gne) {
		c.groupRefused(w, r, id, err)
		return
	}
	if err != nil {
		c.fail(w, r, err)
		return
	}
	what := spec.To
	switch spec.To {
	case "timestamp":
		what = utcStamp(spec.At)
	case "offsets":
		what = "the offsets entered"
	}
	msg := fmt.Sprintf("Reset %d offset%s to %s", n, plural(n), what)
	if topic != "" {
		msg += " on " + topic
	}
	c.groupChanged(w, r, id, msg)
}

// groupChanged answers a change to a group's offsets: a toast, and the live region.
func (c *Console) groupChanged(w http.ResponseWriter, r *http.Request, id, msg string) {
	toast(w, msg)
	data, err := c.groupData(r, id)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.partial(w, "group_live", data)
}

// groupOffsetSet writes one partition's committed offset: to a number, or to the
// partition's earliest or latest position.
func (c *Console) groupOffsetSet(w http.ResponseWriter, r *http.Request) {
	id, topic, to := groupFromSegment(r.PathValue("id")), r.FormValue("topic"), r.FormValue("to")
	partition, offset, err := offsetCoordinates(r, to == "")
	if err != nil {
		c.fail(w, r, err)
		return
	}
	got, err := c.be.SetGroupOffset(r.Context(), id, topic, partition, to, offset)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.groupChanged(w, r, id, fmt.Sprintf("%s · partition %d is now at offset %d", topic, partition, got))
}

// groupOffsetDelete removes one partition's committed offset.
func (c *Console) groupOffsetDelete(w http.ResponseWriter, r *http.Request) {
	id, topic := groupFromSegment(r.PathValue("id")), r.FormValue("topic")
	partition, _, err := offsetCoordinates(r, false)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if err := c.be.DeleteGroupOffset(r.Context(), id, topic, partition); err != nil {
		c.fail(w, r, err)
		return
	}
	// The group's last offset may have been the only thing keeping it listed.
	if !c.groupListed(r, id) {
		c.redirect(w, r, c.prefix+"/groups", "Removed the group's last committed offset")
		return
	}
	c.groupChanged(w, r, id, fmt.Sprintf("Removed the committed offset for %s · partition %d", topic, partition))
}

// groupListed reports whether the group still appears in the broker's list.
func (c *Console) groupListed(r *http.Request, id string) bool {
	list, err := c.be.ListGroups(r.Context())
	if err != nil {
		return true // cannot tell; stay on the page.
	}
	for _, g := range list {
		if g.ID == id {
			return true
		}
	}
	return false
}

// offsetCoordinates reads the topic partition (and, when wanted, the offset) a
// form names. Both must be whole numbers, 0 or more: nothing is coerced.
func offsetCoordinates(r *http.Request, wantOffset bool) (int32, int64, error) {
	if r.FormValue("topic") == "" {
		return 0, 0, badInput("no topic was named")
	}
	p, err := strconv.ParseInt(strings.TrimSpace(r.FormValue("partition")), 10, 32)
	if err != nil || p < 0 {
		return 0, 0, badInput("%q is not a partition number", r.FormValue("partition"))
	}
	if !wantOffset {
		return int32(p), 0, nil
	}
	raw := strings.TrimSpace(r.FormValue("offset"))
	o, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || o < 0 {
		return 0, 0, badInput("%q is not an offset; type a whole number, 0 or more", raw)
	}
	return int32(p), o, nil
}

func (c *Console) groupDelete(w http.ResponseWriter, r *http.Request) {
	id := groupFromSegment(r.PathValue("id"))
	if err := c.be.DeleteGroup(r.Context(), id); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.prefix+"/groups", "Group deleted")
}

// consumerRow is one group reading a topic, with the link to it.
type consumerRow struct {
	TopicConsumer
	URL string
}

// topicConsumers is who consumes a topic, for the strip on its page. It is part of
// the page rather than fetched after it: arriving late, it pushed everything below
// it down a line on every visit. A failure here costs the strip, not the page.
func (c *Console) topicConsumers(ctx context.Context, topic string) []consumerRow {
	consumers, err := c.be.TopicConsumers(ctx, topic)
	if err != nil {
		return nil
	}
	rows := make([]consumerRow, len(consumers))
	for i, tc := range consumers {
		rows[i] = consumerRow{tc, c.groupURL(tc.Group)}
	}
	return rows
}

// offsetTopics is the topics a group has offsets on that still exist, in order:
// the choices for "reset only this topic".
func offsetTopics(rows []groupLagRow) []string {
	var out []string
	for _, r := range rows {
		if !r.Deleted && (len(out) == 0 || out[len(out)-1] != r.Topic) {
			out = append(out, r.Topic)
		}
	}
	return out
}

// groupFacts is what the Members tab's sentence says beyond the member count: the
// leader and the timeouts, from the coordinator's memory. A field the broker does not
// hold for the group is not here (a consumer-protocol group has no leader and no
// per-member timeouts) and the sentence says so.
type groupFactsView struct {
	Leader      string
	Session     string
	Rebalance   string
	NoLeader    bool // a consumer-protocol group, or a classic one with no leader yet.
	Unavailable bool // this console cannot see the coordinator.
}

func groupFacts(insp GroupInspection, members []memberView) groupFactsView {
	switch insp.State {
	case InspectUnavailable:
		return groupFactsView{Unavailable: true}
	case InspectNone:
		return groupFactsView{NoLeader: true}
	}
	f := groupFactsView{NoLeader: true}
	var sess, reb []time.Duration
	for _, ms := range insp.Group.Members {
		if ms.Leader {
			f.Leader, f.NoLeader = ms.ID, false
		}
		if ms.SessionTimeout > 0 {
			sess = append(sess, ms.SessionTimeout)
		}
		if ms.RebalanceTimeout > 0 {
			reb = append(reb, ms.RebalanceTimeout)
		}
	}
	f.Session, f.Rebalance = spanOf(sess), spanOf(reb)
	return f
}

// spanOf is "45 s" when the members agree and "10 s to 45 s" when they do not.
func spanOf(ds []time.Duration) string {
	if len(ds) == 0 {
		return ""
	}
	lo, hi := ds[0], ds[0]
	for _, d := range ds {
		lo, hi = min(lo, d), max(hi, d)
	}
	if lo == hi {
		return secondsText(lo)
	}
	return secondsText(lo) + " to " + secondsText(hi)
}

// groupView names the page's kind for its keys strip: the Lag and Members tabs have
// the strips Gr-02 and Gr-03 draw; the others keep the group's own.
func groupView(tab string) string {
	switch tab {
	case "lag":
		return "group_lag"
	case "members":
		return "group_members"
	}
	return "group"
}
