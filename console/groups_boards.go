package console

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

// The functional group boards: Gr-04 and Gr-17 (the timeline and the pattern in it),
// Gr-05 (a rebalance under way), Gr-09 (no commits), Gr-10 (ahead of the log end),
// Gr-11 (a deleted topic's offsets), Gr-12 and Gr-13 (what each protocol holds). Each
// says only what the broker holds; where it holds nothing the page says that.

// groupEventsHeld is how many events the broker keeps of one group (traffic.MaxGroupEvents).
// The ring is per group (GRP-24): a noisy group does not push out a quiet group's story.
const groupEventsHeld = 200

// timelineKinds are the kinds each filter chip keeps.
var timelineKinds = map[string][]string{
	"rebalances": {"rebalance", "generation", "epoch", "assigned", "downgraded"},
	"members":    {"joined", "left", "expired", "dropped"},
	"problems":   {"expired", "dropped"},
}

type reasonCount struct {
	Reason string
	N      int
}

// flapView is the pattern Gr-17 names: the group rebalancing over and over.
type flapView struct {
	Every   string // "30 s": the mean gap between rebalances in the last ten minutes.
	Count   int
	Window  string
	Gaps    []string // gaps between session expiries, "30.1 s".
	Steady  bool
	Expired int
}

type timelineView struct {
	Rows       []timelineRow
	Chips      []tabLink
	Show       string
	Rebalances int // in the last hour.
	Held       int
	Full       bool // the ring is full: older events were pushed out.
	Since      time.Time
	Reasons    []reasonCount
	Flap       *flapView
	Problems   int
}

func isRebalanceBegin(kind string) bool { return kind == "rebalance" || kind == "epoch" }

// buildTimeline works out the Timeline tab from a group's events, oldest first, at the
// moment now. The window ("the last hour") and the flapping rule are over the events
// the broker still holds, and the page says so.
func buildTimeline(events []TrafficGroupEvent, show string, now time.Time, base string) timelineView {
	v := timelineView{Show: show, Held: len(events), Full: len(events) >= groupEventsHeld}
	if len(events) > 0 {
		v.Since = events[0].Time
	}
	hour, ten := now.Add(-time.Hour), now.Add(-10*time.Minute)
	reasons := map[string]int{}
	var begins, expiries []time.Time
	expiredClient := map[string]bool{}
	keep := map[string]bool{}
	for _, k := range timelineKinds[show] {
		keep[k] = true
	}
	var rows []timelineRow
	for _, e := range events {
		label, tone := timelineKind(e.Kind)
		if e.Kind == "joined" {
			// A client that lost a member to an expiry and joins again is rejoining.
			if cl := detailClient(e.Detail); cl != "" && expiredClient[cl] {
				e.Detail += " · rejoined with a new member id"
			}
		}
		if e.Kind == "expired" {
			if cl := detailClient(e.Detail); cl != "" {
				expiredClient[cl] = true
			}
			if e.Time.After(ten) {
				expiries = append(expiries, e.Time)
			}
			v.Problems++
		}
		if isRebalanceBegin(e.Kind) {
			if e.Time.After(hour) {
				v.Rebalances++
				if e.Cause != "" {
					reasons[e.Cause]++
				}
			}
			if e.Time.After(ten) {
				begins = append(begins, e.Time)
			}
		}
		if show == "all" || keep[e.Kind] {
			rows = append(rows, timelineRow{TrafficGroupEvent: e, Label: label, Tone: tone, Member: shortID(e.Member)})
		}
	}
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 { // newest first.
		rows[i], rows[j] = rows[j], rows[i]
	}
	v.Rows = rows
	for r, n := range reasons {
		v.Reasons = append(v.Reasons, reasonCount{r, n})
	}
	sort.Slice(v.Reasons, func(i, j int) bool {
		if v.Reasons[i].N != v.Reasons[j].N {
			return v.Reasons[i].N > v.Reasons[j].N
		}
		return v.Reasons[i].Reason < v.Reasons[j].Reason
	})
	if len(begins) >= 4 {
		mean := begins[len(begins)-1].Sub(begins[0]) / time.Duration(len(begins)-1)
		f := &flapView{Every: spanShort(mean), Count: len(begins), Window: "10 minutes", Expired: len(expiries)}
		if len(expiries) >= 3 {
			var gaps []time.Duration
			lo, hi := time.Duration(1<<62), time.Duration(0)
			for i := 1; i < len(expiries); i++ {
				g := expiries[i].Sub(expiries[i-1])
				gaps = append(gaps, g)
				lo, hi = min(lo, g), max(hi, g)
				f.Gaps = append(f.Gaps, fmt.Sprintf("%.1f s", g.Seconds()))
			}
			var sum time.Duration
			for _, g := range gaps {
				sum += g
			}
			f.Steady = hi-lo <= max(time.Second, sum/time.Duration(len(gaps))/10)
		}
		v.Flap = f
	}
	chip := func(key, label string) tabLink {
		u := base + "?tab=timeline"
		if key != "all" {
			u += "&show=" + key
		}
		return tabLink{Key: key, Label: label, URL: u, On: show == key}
	}
	v.Chips = []tabLink{chip("all", "all"), chip("rebalances", "rebalances"), chip("members", "members"), chip("problems", "problems")}
	return v
}

// detailClient is the client a coordinator's event names: "client billing sent no ..." -> "billing".
func detailClient(detail string) string {
	if f := strings.Fields(detail); len(f) >= 2 && f[0] == "client" {
		return f[1]
	}
	return ""
}

// spanShort is a gap in whole seconds under two minutes ("30 s"), else minutes.
func spanShort(d time.Duration) string {
	if d < 2*time.Minute {
		return fmt.Sprintf("%d s", int((d+500*time.Millisecond)/time.Second))
	}
	return fmt.Sprintf("%d min", int(d.Minutes()))
}

// rejoinRow is a classic member during a rebalance: has it sent the JoinGroup the
// group is waiting for?
type rejoinRow struct {
	ID, Client, Addr string
	Rejoined         bool
	At               time.Time
	HeartbeatAgo     string
}

// rebalanceView is Gr-05: a classic group in PreparingRebalance, from the coordinator's memory.
type rebalanceView struct {
	Next     int32
	Waiting  int
	Total    int
	Left, Of string
	Holding  bool
	Rows     []rejoinRow
	Late     bool // the deadline has passed and the group has not moved on yet.
}

func rebalanceOf(insp GroupInspection, now time.Time) *rebalanceView {
	if insp.State != InspectOK || insp.Group.State != "PreparingRebalance" || insp.Group.Kind != "classic" {
		return nil
	}
	g := insp.Group
	v := &rebalanceView{Next: g.Generation + 1, Total: len(g.Members), Holding: g.Holding, Of: secondsText(g.RebalanceTimeout)}
	if !g.RebalanceDeadline.IsZero() {
		left := g.RebalanceDeadline.Sub(now)
		v.Late = left <= 0
		v.Left = secondsText(max(left, 0).Round(time.Second))
		if v.Left == "" {
			v.Left = "0 s"
		}
		if g.Holding {
			v.Of = ""
		}
	}
	for _, m := range g.Members {
		if !m.Rejoined {
			v.Waiting++
		}
		v.Rows = append(v.Rows, rejoinRow{ID: m.ID, Client: m.ClientID, Addr: m.Addr, Rejoined: m.Rejoined, At: m.RejoinedAt, HeartbeatAgo: ageText(m.LastHeartbeat)})
	}
	return v
}

// noCommitRow is Gr-09: a partition a member owns that the group has not committed to.
type noCommitRow struct {
	Topic, TopicURL string
	Partition       int32
	Owner           string
	LogEnd          int64
	HasEnd          bool
}

// aheadView is Gr-10: how many partitions the group has committed past the log end,
// and what resetting would do. All of it is arithmetic over the bounds.
type aheadView struct {
	N, Of    int
	Exist    int64 // records that exist on those partitions (log end minus log start).
	Earliest bool
}

func aheadOf(rows []groupLagRow) *aheadView {
	v := &aheadView{}
	for _, r := range rows {
		if r.Deleted {
			continue
		}
		v.Of++
		if r.Ahead {
			v.N++
			v.Exist += r.Latest - r.Earliest
		}
	}
	if v.N == 0 {
		return nil
	}
	return v
}

// leftoverTopics are the topics that no longer exist that the group still has offsets on (Gr-11).
func leftoverTopics(rows []groupLagRow) (topics []string, offsets int) {
	seen := map[string]bool{}
	for _, r := range rows {
		if !r.Deleted {
			continue
		}
		offsets++
		if !seen[r.Topic] {
			seen[r.Topic] = true
			topics = append(topics, r.Topic)
		}
	}
	return topics, offsets
}

// partsText writes a snapshot assignment the way the boards do: "orders:0,1,2".
func partsText(ps []TopicParts) string {
	var out []string
	for _, p := range ps {
		name := p.Topic
		if name == "" {
			name = "(deleted topic " + shortID(p.TopicID) + ")"
		}
		var nums []string
		for _, n := range p.Partitions {
			nums = append(nums, fmt.Sprint(n))
		}
		out = append(out, name+":"+strings.Join(nums, ","))
	}
	return strings.Join(out, " ")
}

// partSet is the "topic/partition" keys of an assignment.
func partSet(ps []TopicParts) map[string]bool {
	out := map[string]bool{}
	for _, p := range ps {
		for _, n := range p.Partitions {
			out[fmt.Sprintf("%s:%d", p.Topic+"/"+p.TopicID, n)] = true
		}
	}
	return out
}

// partsMinus is the partitions in a that are not in b, as text.
func partsMinus(a, b []TopicParts) string {
	have := partSet(b)
	var out []TopicParts
	for _, p := range a {
		var left []int32
		for _, n := range p.Partitions {
			if !have[fmt.Sprintf("%s:%d", p.Topic+"/"+p.TopicID, n)] {
				left = append(left, n)
			}
		}
		if len(left) > 0 {
			out = append(out, TopicParts{Topic: p.Topic, TopicID: p.TopicID, Partitions: left})
		}
	}
	return partsList(out)
}

// partsList is just the partition numbers: "2" or "0,1".
func partsList(ps []TopicParts) string {
	var nums []string
	for _, p := range ps {
		for _, n := range p.Partitions {
			nums = append(nums, fmt.Sprint(n))
		}
	}
	return strings.Join(nums, ",")
}

// handoffRow is one consumer-protocol member (Gr-12): what the coordinator wants it to
// own (target) and what it holds (current), which is its own record, not the member's report.
type handoffRow struct {
	ID, Client  string
	State       string
	Epoch       string // "12", or "12 → 13" while it has not caught up.
	Target      string
	Current     string
	Releasing   string // partitions it was told to give up and has not.
	Waiting     string // partitions it is to get that are not its yet.
	Reconciling bool
}

func handoffsOf(insp GroupInspection) []handoffRow {
	if insp.State != InspectOK || insp.Group.Kind != "consumer" {
		return nil
	}
	var out []handoffRow
	for _, m := range insp.Group.Members {
		if m.State == "" {
			continue
		}
		r := handoffRow{ID: m.ID, Client: m.ClientID, State: m.State, Epoch: fmt.Sprint(m.Epoch), Reconciling: m.State == "RECONCILING",
			Target: partsText(m.Target), Current: partsText(m.Current), Releasing: partsList(m.Revoking), Waiting: partsMinus(m.Target, m.Current)}
		if m.PrevEpoch > 0 && m.PrevEpoch != m.Epoch {
			r.Epoch = fmt.Sprintf("%d → %d", m.PrevEpoch, m.Epoch)
		}
		if insp.Group.TargetEpoch > m.Epoch {
			r.Epoch = fmt.Sprintf("%d → %d", m.Epoch, insp.Group.TargetEpoch)
		}
		out = append(out, r)
	}
	return out
}

// classicFacts is the Gr-13 "The group" card: what a classic group's coordinator holds.
type classicFacts struct {
	Generation   int32
	ProtocolType string
	Protocol     string
	Offered      string // the protocols the members offered, when more than the one chosen.
	Leader       string
	Session      string
	Rebalance    string
}

func classicOf(insp GroupInspection) *classicFacts {
	if insp.State != InspectOK || insp.Group.Kind != "classic" || len(insp.Group.Members) == 0 {
		return nil
	}
	g := insp.Group
	f := &classicFacts{Generation: g.Generation, ProtocolType: g.ProtocolType, Protocol: g.ProtocolName, Leader: g.Leader}
	var sess, reb []time.Duration
	offered := map[string]bool{}
	var order []string
	for _, m := range g.Members {
		if m.SessionTimeout > 0 {
			sess = append(sess, m.SessionTimeout)
		}
		if m.RebalanceTimeout > 0 {
			reb = append(reb, m.RebalanceTimeout)
		}
		for _, p := range m.Protocols {
			if !offered[p] {
				offered[p] = true
				order = append(order, p)
			}
		}
	}
	f.Session, f.Rebalance = spanOf(sess), spanOf(reb)
	if len(order) > 1 {
		f.Offered = strings.Join(order, ", ")
	}
	return f
}

// noCommitRows lists the partitions the members own, with the log end, for a group that has
// not committed (Gr-09). It never prints a lag.
func (c *Console) noCommitRows(desc []GroupMember, views []memberView, bounds map[string][]PartitionBound) []noCommitRow {
	var out []noCommitRow
	for i, m := range desc {
		label := views[i].Label
		topics := make([]string, 0, len(m.Assigned))
		for t := range m.Assigned {
			topics = append(topics, t)
		}
		sort.Strings(topics)
		for _, t := range topics {
			ps := append([]int32(nil), m.Assigned[t]...)
			sort.Slice(ps, func(a, b int) bool { return ps[a] < ps[b] })
			for _, p := range ps {
				row := noCommitRow{Topic: t, TopicURL: c.topicURL(t), Partition: p, Owner: label}
				if b := bounds[t]; int(p) < len(b) {
					row.LogEnd, row.HasEnd = b[p].Latest, true
				}
				out = append(out, row)
			}
		}
	}
	sort.SliceStable(out, func(a, b int) bool {
		if out[a].Topic != out[b].Topic {
			return out[a].Topic < out[b].Topic
		}
		return out[a].Partition < out[b].Partition
	})
	return out
}

// ---- Gr-18: the offsets retention (GRP-19) ---------------------------------------

// OffsetExpiry is when a group's committed offsets expire, as the broker's own sweep
// judges them (broker.OffsetExpiry). Retention 0 says offsets are kept for ever.
type OffsetExpiry struct {
	Retention  time.Duration
	EmptySince time.Time
	Offsets    []OffsetExpiryAt
	Interval   time.Duration
}

// OffsetExpiryAt is one offset's expiry.
type OffsetExpiryAt struct {
	Topic     string
	Partition int32
	At        time.Time
}

// OffsetExpirer is what a GroupInspector may also be: it knows the retention.
type OffsetExpirer interface {
	OffsetExpiry(group string) (OffsetExpiry, bool)
}

// expiryView is the Gr-18 callout: only what the broker computes.
type expiryView struct {
	Forever    bool // retention is 0: offsets are never expired.
	None       bool // retention is set but none of this group's offsets expire now.
	Retention  string
	At         time.Time // the earliest expiry.
	In         string
	EmptySince time.Time
	Used, Of   string // "3.0 d" of "7.0 d", for a group counted from when it became empty.
	Pct        int
	Interval   string
	Count      int
}

func (c *Console) expiryOf(id string, hasOffsets bool, now time.Time) *expiryView {
	x, ok := c.inspector.(OffsetExpirer)
	if !ok || !hasOffsets {
		return nil
	}
	e, known := x.OffsetExpiry(id)
	if !known {
		return nil // the broker behind this console cannot say; the page says nothing about retention.
	}
	if e.Retention <= 0 {
		return &expiryView{Forever: true}
	}
	v := &expiryView{Retention: unitSpan(e.Retention, e.Retention), Interval: spanShort(e.Interval)}
	if len(e.Offsets) == 0 {
		v.None = true
		return v
	}
	v.Count = len(e.Offsets)
	v.At = e.Offsets[0].At
	for _, o := range e.Offsets {
		if o.At.Before(v.At) {
			v.At = o.At
		}
	}
	v.In = untilText(v.At.Sub(now))
	if !e.EmptySince.IsZero() {
		v.EmptySince = e.EmptySince
		used := min(max(now.Sub(e.EmptySince), 0), e.Retention)
		v.Used, v.Of = unitSpan(used, e.Retention), unitSpan(e.Retention, e.Retention)
		v.Pct = int(used * 100 / e.Retention)
	}
	return v
}

// unitSpan writes d in the unit that suits the whole span: days for a span of two
// days or more, then hours, minutes, seconds.
func unitSpan(d, span time.Duration) string {
	switch {
	case span >= 48*time.Hour:
		return fmt.Sprintf("%.1f d", d.Hours()/24)
	case span >= 2*time.Hour:
		return fmt.Sprintf("%.1f h", d.Hours())
	case span >= 2*time.Minute:
		return fmt.Sprintf("%.1f min", d.Minutes())
	}
	return fmt.Sprintf("%.1f s", d.Seconds())
}

// untilText is how long to a moment: "in 4 days", "in 3 h", "in 12 min", "in 40 s", "now".
func untilText(d time.Duration) string {
	switch {
	case d <= 0:
		return "now (at the next check)"
	case d < 2*time.Minute:
		return fmt.Sprintf("in %d s", int(d.Seconds()))
	case d < 2*time.Hour:
		return fmt.Sprintf("in %d min", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("in %d h", int(d.Hours()))
	}
	return fmt.Sprintf("in %d days", int(d.Hours()/24))
}

// ---- Gr-14: records held back by an open transaction (GRP-18) ---------------------

type heldRow struct {
	Topic, TopicURL string
	Partition       int32
	LogEnd, Stable  int64
	Held            int64
	Behind          int64 // lag if measured to the last stable offset.
}

type heldTxn struct {
	ID         string
	ProducerID int64
	Epoch      int16
	Began      time.Time
	OpenFor    string
	Partitions []string
}

type heldView struct {
	Rows  []heldRow
	Total int64
	Txns  []heldTxn
}

// heldBackOf lists the partitions the group has offsets on whose last stable offset is
// below the log end, and the open transactions on them. The console cannot tell whether
// the group reads with read_committed (Fetch carries no group id), so it does not claim
// to: it says what each isolation level would see.
func heldBackOf(c *Console, ctx context.Context, rows []groupLagRow, now time.Time) *heldView {
	stable := map[string][]int64{}
	v := &heldView{}
	for _, r := range rows {
		if r.Deleted {
			continue
		}
		if _, ok := stable[r.Topic]; !ok {
			n, err := c.be.PartitionCount(ctx, r.Topic)
			if err != nil {
				stable[r.Topic] = nil
				continue
			}
			stable[r.Topic], _ = c.be.StableEnds(ctx, r.Topic, n)
		}
		s := stable[r.Topic]
		if int(r.Partition) >= len(s) || s[r.Partition] >= r.Latest {
			continue
		}
		h := heldRow{Topic: r.Topic, TopicURL: r.TopicURL, Partition: r.Partition, LogEnd: r.Latest, Stable: s[r.Partition], Held: r.Latest - s[r.Partition]}
		h.Behind = max(h.Stable-r.Committed, 0)
		v.Rows = append(v.Rows, h)
		v.Total += h.Held
	}
	if len(v.Rows) == 0 {
		return nil
	}
	want := map[string]bool{}
	for _, h := range v.Rows {
		want[fmt.Sprintf("%s-%d", h.Topic, h.Partition)] = true
	}
	for _, t := range c.be.openTransactions(ctx) {
		var mine []string
		for _, p := range t.Partitions {
			if want[p] {
				mine = append(mine, p)
			}
		}
		if len(mine) == 0 {
			continue
		}
		ht := heldTxn{ID: t.ID, ProducerID: t.ProducerID, Epoch: t.Epoch, Began: t.Began, Partitions: mine}
		if !t.Began.IsZero() {
			ht.OpenFor = spanText(now.Sub(t.Began))
		}
		v.Txns = append(v.Txns, ht)
	}
	return v
}

// ---- Gr-16: refused (GRP-21) -------------------------------------------------------

// groupAPINeeds is the ACL operation on a group each API needs, as the broker checks
// it (canGroup in each handler). A DENY matches one operation exactly.
var groupAPINeeds = map[string]string{
	"JoinGroup": "Read", "SyncGroup": "Read", "Heartbeat": "Read", "LeaveGroup": "Read", "OffsetCommit": "Read",
	"ConsumerGroupHeartbeat": "Read", "TxnOffsetCommit": "Read", "AddOffsetsToTxn": "Read",
	"OffsetFetch": "Describe", "FindCoordinator": "Describe", "DescribeGroups": "Describe", "ConsumerGroupDescribe": "Describe", "ListGroups": "Describe",
	"DeleteGroups": "Delete", "OffsetDelete": "Delete",
}

type deniedView struct {
	Error, API, Needs string
	Version           int16
	At, First         time.Time
	Client, Principal string
	Count, Clients    int
	Over              string
	CheckURL          string
	ACLs              []ACLRow
	Unknown           bool // an API this table does not know: no operation is claimed.
}

func (c *Console) deniedOf(ctx context.Context, id string, now time.Time) *deniedView {
	if c.traffic == nil {
		return nil
	}
	clients := map[string]bool{}
	var last *TrafficEvent
	var first time.Time
	n, over, _ := c.deniedWindow(now, func(e TrafficEvent) bool {
		if e.Group != id && e.ViaGroup != id {
			return false
		}
		key := e.ClientID + "|" + e.Principal
		clients[key] = true
		cp := e
		if last == nil {
			last = &cp
		}
		if first.IsZero() || e.First.Before(first) {
			first = e.First
		}
		return true
	})
	if last == nil {
		return nil
	}
	v := &deniedView{Error: last.Error, API: last.API, Version: last.Version, At: last.Time, First: first, Client: last.ClientID, Principal: last.Principal,
		Count: n, Clients: len(clients), Over: over}
	if v.Error == "" {
		v.Error = "GROUP_AUTHORIZATION_FAILED"
	}
	v.Needs = groupAPINeeds[last.API]
	if v.Needs == "" {
		v.Unknown = true
	}
	if v.Principal != "" && v.Needs != "" {
		v.CheckURL = c.prefix + "/access/check?" + url.Values{"principal": {v.Principal}, "operation": {v.Needs}, "resource": {"group"}, "name": {id}}.Encode()
	}
	if acls, err := c.be.ListACLs(ctx); err == nil {
		for _, a := range acls {
			if a.Resource != "group" || !aclNames(a, id) {
				continue
			}
			if a.Principal == v.Principal || a.Principal == "User:*" {
				v.ACLs = append(v.ACLs, a)
			}
		}
	}
	return v
}

// aclNames says whether an ACL on a group names this group: literally, by "*", or by prefix.
func aclNames(a ACLRow, group string) bool {
	switch strings.ToLower(a.Pattern) {
	case "prefixed":
		return strings.HasPrefix(group, a.Name)
	}
	return a.Name == group || a.Name == "*"
}
