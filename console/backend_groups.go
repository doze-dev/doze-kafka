package console

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// GroupSummary is one row in the consumer groups list.
type GroupSummary struct {
	ID       string
	Protocol string // "classic" or "848" (KIP-848, broker-driven)
	State    string
	Members  int
}

// GroupMember is one member of a group, with its assigned partitions.
type GroupMember struct {
	ID       string
	ClientID string             // the client.id the member connected with.
	Host     string             // where it connected from.
	Assigned map[string][]int32 // topic -> partitions
}

// GroupDetail is a group's identity plus its members.
type GroupDetail struct {
	ID       string
	Protocol string
	State    string
	Assignor string // KIP-848 only ("uniform"/"range"); empty for classic
	Members  []GroupMember
}

// GroupOffset is one committed offset a group holds.
type GroupOffset struct {
	Topic     string
	Partition int32
	Committed int64
}

func normalizeProtocol(groupType string) string {
	if groupType == "classic" {
		return "classic"
	}
	return "848" // "consumer" (KIP-848) — doze-kafka's only other group type
}

// ListGroups returns every group, sorted by ID.
func (b *backend) ListGroups(ctx context.Context) ([]GroupSummary, error) {
	req := kmsg.NewPtrListGroupsRequest()
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(resp.ErrorCode)
	}
	out := make([]GroupSummary, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		out = append(out, GroupSummary{ID: g.Group, Protocol: normalizeProtocol(g.GroupType), State: g.GroupState})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	// Member counts need a describe, which ListGroups does not carry. One batched
	// request per protocol covers every group. This used to describe each group on
	// its own, and each of those describes first re-listed all groups to learn the
	// protocol: 1 + 2N round trips for N groups, which grew with every group a
	// test suite ever created and ran on every poll of the rail.
	var classic, consumer []string
	for _, g := range out {
		if g.Protocol == "classic" {
			classic = append(classic, g.ID)
		} else {
			consumer = append(consumer, g.ID)
		}
	}
	members := map[string]int{}
	if len(classic) > 0 {
		dreq := kmsg.NewPtrDescribeGroupsRequest()
		dreq.Groups = classic
		if dresp, err := dreq.RequestWith(ctx, b.cl); err == nil {
			for _, g := range dresp.Groups {
				members[g.Group] = len(g.Members)
			}
		}
	}
	if len(consumer) > 0 {
		creq := kmsg.NewPtrConsumerGroupDescribeRequest()
		creq.Groups = consumer
		if cresp, err := creq.RequestWith(ctx, b.cl); err == nil {
			for _, g := range cresp.Groups {
				members[g.Group] = len(g.Members)
			}
		}
	}
	for i := range out {
		out[i].Members = members[out[i].ID]
	}
	return out, nil
}

// Counts returns how many topics and groups exist, for the rail's live counts.
// It asks for names only: the rail polls this every few seconds from every open
// tab, and it used to run the full topic listing (offsets for every partition)
// and the full group listing (a describe per group) just to take len().
func (b *backend) Counts(ctx context.Context) (topics, groups int, err error) {
	mreq := kmsg.NewPtrMetadataRequest()
	mreq.Topics = nil // all topics: names only, no ListOffsets, no DescribeConfigs.
	mresp, err := mreq.RequestWith(ctx, b.cl)
	if err != nil {
		return 0, 0, err
	}
	for _, t := range mresp.Topics {
		if !t.IsInternal && t.Topic != nil {
			topics++
		}
	}
	lresp, err := kmsg.NewPtrListGroupsRequest().RequestWith(ctx, b.cl)
	if err != nil {
		return topics, 0, err
	}
	return topics, len(lresp.Groups), nil
}

// DescribeGroup returns a group's members and their assigned partitions,
// decoding whichever protocol the group actually uses.
func (b *backend) DescribeGroup(ctx context.Context, id string) (*GroupDetail, error) {
	lreq := kmsg.NewPtrListGroupsRequest()
	lresp, err := lreq.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	protocol := ""
	for _, g := range lresp.Groups {
		if g.Group == id {
			protocol = normalizeProtocol(g.GroupType)
			break
		}
	}
	if protocol == "" {
		return nil, fmt.Errorf("%w: group %q", errNotFound, id)
	}
	if protocol == "848" {
		return b.describeGroup848(ctx, id)
	}
	return b.describeGroupClassic(ctx, id)
}

func (b *backend) describeGroupClassic(ctx context.Context, id string) (*GroupDetail, error) {
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{id}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if len(resp.Groups) == 0 {
		return nil, fmt.Errorf("%w: group %q", errNotFound, id)
	}
	g := resp.Groups[0]
	if g.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(g.ErrorCode)
	}
	members := make([]GroupMember, len(g.Members))
	for i, m := range g.Members {
		assigned := map[string][]int32{}
		var a kmsg.ConsumerMemberAssignment
		if len(m.MemberAssignment) > 0 && a.ReadFrom(m.MemberAssignment) == nil {
			for _, t := range a.Topics {
				assigned[t.Topic] = t.Partitions
			}
		}
		members[i] = GroupMember{ID: m.MemberID, ClientID: m.ClientID, Host: cleanHost(m.ClientHost), Assigned: assigned}
	}
	return &GroupDetail{ID: id, Protocol: "classic", State: g.State, Members: members}, nil
}

func (b *backend) describeGroup848(ctx context.Context, id string) (*GroupDetail, error) {
	req := kmsg.NewPtrConsumerGroupDescribeRequest()
	req.Groups = []string{id}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if len(resp.Groups) == 0 {
		return nil, fmt.Errorf("%w: group %q", errNotFound, id)
	}
	g := resp.Groups[0]
	if g.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(g.ErrorCode)
	}
	members := make([]GroupMember, len(g.Members))
	for i, m := range g.Members {
		assigned := map[string][]int32{}
		for _, tp := range m.Assignment.TopicPartitions {
			assigned[tp.Topic] = tp.Partitions
		}
		members[i] = GroupMember{ID: m.MemberID, ClientID: m.ClientID, Host: cleanHost(m.ClientHost), Assigned: assigned}
	}
	return &GroupDetail{ID: id, Protocol: "848", State: g.State, Assignor: g.AssignorName, Members: members}, nil
}

// GroupOffsets returns every committed offset a group holds, across all
// topics — the same shape kafka-consumer-groups.sh reads to compute lag.
func (b *backend) GroupOffsets(ctx context.Context, id string) ([]GroupOffset, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = id
	req.Topics = nil // null = every topic the group has committed offsets for
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(resp.ErrorCode)
	}
	var out []GroupOffset
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 || p.Offset < 0 {
				continue // no committed offset for this partition
			}
			out = append(out, GroupOffset{Topic: t.Topic, Partition: p.Partition, Committed: p.Offset})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out, nil
}

// cleanHost drops the leading slash Kafka puts on a client host ("/127.0.0.1").
func cleanHost(h string) string { return strings.TrimPrefix(h, "/") }

// OffsetTarget is one committed offset to write.
type OffsetTarget struct {
	Topic     string
	Partition int32
	Offset    int64
}

// groupNotEmpty is the refusal of a change to the offsets of a group that has
// members: they would commit over the change, or be fenced by it.
//
// claim: groups.md G5
type groupNotEmpty struct {
	Group   string
	Members int
}

func (e *groupNotEmpty) Error() string {
	return fmt.Sprintf("the group has %d active member%s; stop its consumers before changing its offsets", e.Members, plural(e.Members))
}
func (e *groupNotEmpty) Is(target error) bool { return target == errBadInput }

// requireIdle refuses to change the offsets of a group that has members.
func (b *backend) requireIdle(ctx context.Context, id string) error {
	d, err := b.DescribeGroup(ctx, id)
	if err != nil {
		return err
	}
	if len(d.Members) > 0 {
		return &groupNotEmpty{Group: id, Members: len(d.Members)}
	}
	return nil
}

// TopicPartition names one partition of one topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// ResetSpec is what a reset asks for. To is "earliest", "latest", "timestamp" (the
// first record at or after At, per partition) or "offsets" (the named partitions to
// the given offsets; the others stay). Topic limits it to one topic; empty is every
// topic the group has offsets on.
type ResetSpec struct {
	To      string
	Topic   string
	At      time.Time
	Offsets map[TopicPartition]int64
}

// ResetRow is one partition of a reset: where the group stands, where it would stand,
// and the bounds both are read against.
type ResetRow struct {
	Topic                       string
	Partition                   int32
	Committed, Earliest, Latest int64
	After                       int64
	Moves                       bool   // the reset writes this partition.
	Note                        string // what the row needs said (no record at or after the time).
}

// PlanReset works out a reset from what the broker holds and sends nothing: it is
// the preview, and ApplyReset writes what it returns. Partitions are those the group
// has committed offsets on, on topics that still exist.
func (b *backend) PlanReset(ctx context.Context, id string, spec ResetSpec) ([]ResetRow, error) {
	switch spec.To {
	case "earliest", "latest", "timestamp", "offsets":
	default:
		return nil, badInput("reset to earliest, latest, a timestamp or specific offsets, not %q", spec.To)
	}
	offsets, err := b.GroupOffsets(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(offsets) == 0 {
		return nil, badInput("the group has no committed offsets to reset")
	}
	bounds := map[string][]PartitionBound{}
	var rows []ResetRow
	onTopic := 0
	for _, o := range offsets {
		if spec.Topic != "" && o.Topic != spec.Topic {
			continue
		}
		onTopic++
		if _, ok := bounds[o.Topic]; !ok {
			parts, err := b.PartitionCount(ctx, o.Topic)
			if err != nil {
				bounds[o.Topic] = nil
				continue // a deleted topic's offset is left alone.
			}
			if bounds[o.Topic], err = b.PartitionBounds(ctx, o.Topic, parts); err != nil {
				return nil, err
			}
		}
		if o.Partition < 0 || int(o.Partition) >= len(bounds[o.Topic]) {
			continue
		}
		bd := bounds[o.Topic][o.Partition]
		row := ResetRow{Topic: o.Topic, Partition: o.Partition, Committed: o.Committed, Earliest: bd.Earliest, Latest: bd.Latest, Moves: true}
		switch spec.To {
		case "earliest":
			row.After = bd.Earliest
		case "latest":
			row.After = bd.Latest
		case "timestamp":
			off, found, err := b.OffsetAtTime(ctx, o.Topic, o.Partition, spec.At.UnixMilli())
			if err != nil {
				return nil, err
			}
			row.After = off
			if !found {
				row.After, row.Note = bd.Latest, "no record at or after that time: the end of the log"
			}
		case "offsets":
			want, named := spec.Offsets[TopicPartition{o.Topic, o.Partition}]
			row.After, row.Moves = o.Committed, named && want != o.Committed
			if named {
				if err := offsetInRange(want, o.Topic, o.Partition, bd.Earliest, bd.Latest); err != nil {
					return nil, err
				}
				row.After = want
			}
		}
		rows = append(rows, row)
	}
	switch {
	case spec.Topic != "" && onTopic == 0:
		return nil, badInput("the group has no committed offsets on topic %q", spec.Topic)
	case len(rows) == 0:
		return nil, badInput("none of those offsets are on topics that still exist")
	}
	return rows, nil
}

// offsetInRange is the console's rule for an offset it is asked to commit (GRP-15):
// from the first retained record to the end of the log. It is the console's own: an
// OffsetCommit is accepted by Kafka at any offset (recorded, offsetcommit-generations
// #commit-offset-past-end), and OFFSET_OUT_OF_RANGE is what the group's next Fetch
// would answer (claims/api.md V7), so the rule is said without a Kafka name.
func offsetInRange(offset int64, topic string, partition int32, earliest, latest int64) error {
	if offset >= earliest && offset <= latest {
		return nil
	}
	return &ruledError{
		msg: fmt.Sprintf("Offset %d is outside partition %d of %s.", offset, partition, topic),
		rule: fmt.Sprintf("A committed offset must lie between the first retained record (%d) and the end of the log (%d, where there is nothing left to read). "+
			"Kafka would accept any number in a commit, and the group's next Fetch would then be answered OFFSET_OUT_OF_RANGE, so the console checks the range before it sends.",
			earliest, latest),
	}
}

// ApplyReset writes a reset: the plan's partitions that move, as the console (generation
// -1), after checking that no member is there to commit over it. It reports how many
// offsets it wrote.
func (b *backend) ApplyReset(ctx context.Context, id string, spec ResetSpec) (int, error) {
	if spec.To != "earliest" && spec.To != "latest" && spec.To != "timestamp" && spec.To != "offsets" {
		return 0, badInput("reset to earliest, latest, a timestamp or specific offsets, not %q", spec.To)
	}
	if err := b.requireIdle(ctx, id); err != nil {
		return 0, err
	}
	rows, err := b.PlanReset(ctx, id, spec)
	if err != nil {
		return 0, err
	}
	var targets []OffsetTarget
	for _, r := range rows {
		if r.Moves {
			targets = append(targets, OffsetTarget{r.Topic, r.Partition, r.After})
		}
	}
	if len(targets) == 0 {
		return 0, badInput("no offset was changed: every partition is already where you asked")
	}
	return b.commitOffsets(ctx, id, targets)
}

// ResetGroupOffsets moves an inactive group's committed offsets to the earliest or
// the latest position: all of them, or only those on one topic when topic is set.
// It reports how many were moved.
func (b *backend) ResetGroupOffsets(ctx context.Context, id, to, topic string) (int, error) {
	if to != "earliest" && to != "latest" {
		return 0, fmt.Errorf("%w: reset to earliest or latest, not %q", errBadInput, to)
	}
	return b.ApplyReset(ctx, id, ResetSpec{To: to, Topic: topic})
}

// SetGroupOffset writes one committed offset of an inactive group. to is
// "earliest", "latest", or empty for the given offset, which must be a position
// the partition has: from its first retained record to its end.
func (b *backend) SetGroupOffset(ctx context.Context, id, topic string, partition int32, to string, offset int64) (int64, error) {
	if err := b.requireIdle(ctx, id); err != nil {
		return 0, err
	}
	parts, err := b.PartitionCount(ctx, topic)
	if err != nil {
		return 0, err
	}
	if partition < 0 || partition >= parts {
		return 0, badInput("topic %q has no partition %d", topic, partition)
	}
	earliest, latest, err := b.PartitionOffsets(ctx, topic, partition)
	if err != nil {
		return 0, err
	}
	switch to {
	case "earliest":
		offset = earliest
	case "latest":
		offset = latest
	case "":
		if err := offsetInRange(offset, topic, partition, earliest, latest); err != nil {
			return 0, err
		}
	default:
		return 0, badInput("set an offset to earliest, latest or a number, not %q", to)
	}
	if _, err := b.commitOffsets(ctx, id, []OffsetTarget{{topic, partition, offset}}); err != nil {
		return 0, err
	}
	return offset, nil
}

// commitOffsets writes offsets for a group as no member (generation -1), and
// reports how many the broker accepted.
func (b *backend) commitOffsets(ctx context.Context, id string, targets []OffsetTarget) (int, error) {
	byTopic := map[string][]kmsg.OffsetCommitRequestTopicPartition{}
	var topics []string
	for _, t := range targets {
		if _, ok := byTopic[t.Topic]; !ok {
			topics = append(topics, t.Topic)
		}
		rp := kmsg.NewOffsetCommitRequestTopicPartition()
		rp.Partition, rp.Offset = t.Partition, t.Offset
		byTopic[t.Topic] = append(byTopic[t.Topic], rp)
	}
	sort.Strings(topics)
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group, req.Generation = id, -1
	for _, t := range topics {
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic, rt.Partitions = t, byTopic[t]
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return 0, err
	}
	// The broker decides each partition on its own. It also refuses the whole
	// request if a consumer joined after the check above (a commit that names no
	// member is not accepted by a group that has members), which is the real guard.
	failed, moved := 0, 0
	var firstErr error
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				failed++
				if firstErr == nil {
					firstErr = kerr.ErrorForCode(p.ErrorCode)
				}
				continue
			}
			moved++
		}
	}
	switch {
	case failed == 0:
		return moved, nil
	case errors.Is(firstErr, kerr.UnknownMemberID), errors.Is(firstErr, kerr.IllegalGeneration), errors.Is(firstErr, kerr.RebalanceInProgress):
		return moved, badInput("a consumer joined the group while its offsets were being changed; stop its consumers and try again")
	case moved > 0:
		return moved, fmt.Errorf("%d of %d offsets were written; the rest failed: %w", moved, moved+failed, firstErr)
	default:
		return 0, firstErr
	}
}

// DeleteGroupOffset removes one committed offset of an inactive group. The group
// then has no position on that partition, and a consumer starts there from its
// auto.offset.reset policy.
func (b *backend) DeleteGroupOffset(ctx context.Context, id, topic string, partition int32) error {
	req := kmsg.NewPtrOffsetDeleteRequest()
	req.Group = id
	rt := kmsg.NewOffsetDeleteRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewOffsetDeleteRequestTopicPartition()
	rp.Partition = partition
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return err
	}
	if resp.ErrorCode != 0 {
		return kerr.ErrorForCode(resp.ErrorCode)
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			switch err := kerr.ErrorForCode(p.ErrorCode); {
			case err == nil:
			case errors.Is(err, kerr.GroupSubscribedToTopic):
				return badInput("the group has active members; stop its consumers before removing its offsets")
			default:
				return err
			}
		}
	}
	return nil
}

// DeleteGroup removes an inactive group and its committed offsets.
func (b *backend) DeleteGroup(ctx context.Context, id string) error {
	req := kmsg.NewPtrDeleteGroupsRequest()
	req.Groups = []string{id}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return err
	}
	for _, g := range resp.Groups {
		if g.ErrorCode == 68 { // NON_EMPTY_GROUP
			return badInput("the group still has active members; stop its consumers first")
		}
		if g.ErrorCode != 0 {
			return kerr.ErrorForCode(g.ErrorCode)
		}
	}
	return nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// TopicConsumer is a group that has committed offsets on a topic, with its lag there.
type TopicConsumer struct {
	Group    string
	Protocol string
	State    string
	Members  int
	Lag      int64
}

// TopicConsumers lists the groups consuming a topic (those with committed offsets
// on it) and how far behind each is. It asks per group, so it is for a lazily
// loaded panel, not something to run on every poll.
func (b *backend) TopicConsumers(ctx context.Context, topic string) ([]TopicConsumer, error) {
	groups, err := b.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	parts, err := b.PartitionCount(ctx, topic)
	if err != nil {
		return nil, err
	}
	bounds, err := b.PartitionBounds(ctx, topic, parts)
	if err != nil {
		return nil, err
	}
	var out []TopicConsumer
	for _, g := range groups {
		offs, err := b.GroupOffsets(ctx, g.ID)
		if err != nil {
			continue
		}
		var on bool
		var lag int64
		for _, o := range offs {
			if o.Topic != topic || o.Partition < 0 || int(o.Partition) >= len(bounds) {
				continue
			}
			on = true
			lag += max(bounds[o.Partition].Latest-o.Committed, 0)
		}
		if on {
			out = append(out, TopicConsumer{Group: g.ID, Protocol: g.Protocol, State: g.State, Members: g.Members, Lag: lag})
		}
	}
	return out, nil
}
