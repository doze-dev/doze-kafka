package broker_test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// A KIP-848 member, modelled the way a real client behaves.
//
// member848 keeps what the member really owns (owned) apart from what the broker
// has told it (pending). A real client applies a new assignment asynchronously
// (revoke callback, commit, assign callback) and only then reports its owned
// partitions; between heartbeats it sends Topics=nil. Tests that apply an
// assignment the instant it arrives, and report on every heartbeat, cannot see
// the bugs that live in that gap.

type ownedPart struct {
	id [16]byte
	p  int32
}

type member848 struct {
	cl      *kgo.Client
	group   string
	id      string
	epoch   int32
	joined  bool
	owned   map[ownedPart]bool
	pending []map[ownedPart]bool // targets received, oldest first, not yet applied.
	dirty   bool                 // owned changed since last reported.
	subs    []string
	inst    *string
	assign  string
	ver     int16
}

func new848(t *testing.T, addr, group, id string) *member848 {
	t.Helper()
	cl := newClient(t, addr)
	t.Cleanup(cl.Close)
	return &member848{cl: cl, group: group, id: id, owned: map[ownedPart]bool{}, assign: "uniform", ver: -1}
}

func (c *member848) ownedTopics() []kmsg.ConsumerGroupHeartbeatRequestTopic {
	by := map[[16]byte][]int32{}
	for k := range c.owned {
		by[k.id] = append(by[k.id], k.p)
	}
	out := []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	for id, ps := range by {
		slices.Sort(ps)
		rt := kmsg.NewConsumerGroupHeartbeatRequestTopic()
		rt.TopicID, rt.Partitions = id, ps
		out = append(out, rt)
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].TopicID[:]) < string(out[j].TopicID[:]) })
	return out
}

// raw sends a request built by mut on top of the member's identity.
func (c *member848) raw(t *testing.T, ctx context.Context, mut func(r *kmsg.ConsumerGroupHeartbeatRequest)) *kmsg.ConsumerGroupHeartbeatResponse {
	t.Helper()
	r := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	r.Group, r.MemberID, r.MemberEpoch = c.group, c.id, c.epoch
	r.RebalanceTimeoutMillis = 60_000
	if c.assign != "" {
		r.ServerAssignor = kmsg.StringPtr(c.assign)
	}
	r.InstanceID = c.inst
	if mut != nil {
		mut(r)
	}
	if c.ver >= 0 {
		r.SetVersion(c.ver)
	}
	resp, err := r.RequestWith(ctx, c.cl)
	if err != nil {
		t.Fatalf("ConsumerGroupHeartbeat: %v", err)
	}
	return resp
}

func (c *member848) absorb(resp *kmsg.ConsumerGroupHeartbeatResponse) {
	if resp.ErrorCode != 0 {
		return
	}
	if resp.MemberID != nil {
		c.id = *resp.MemberID
	}
	c.epoch = resp.MemberEpoch
	c.joined = true
	if resp.Assignment != nil {
		tgt := map[ownedPart]bool{}
		for _, at := range resp.Assignment.Topics {
			for _, p := range at.Partitions {
				tgt[ownedPart{at.TopicID, p}] = true
			}
		}
		c.pending = append(c.pending, tgt)
	}
}

// join: epoch 0, subscription, empty owned list (what the Java client sends).
func (c *member848) join(t *testing.T, ctx context.Context, subs []string) *kmsg.ConsumerGroupHeartbeatResponse {
	t.Helper()
	c.epoch, c.owned, c.pending, c.dirty, c.subs = 0, map[ownedPart]bool{}, nil, false, subs
	resp := c.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) {
		r.SubscribedTopicNames = subs
		r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	})
	c.absorb(resp)
	return resp
}

// hb: a regular heartbeat. Topics is sent only when owned changed since the last
// report (dirty), or always if force.
func (c *member848) hb(t *testing.T, ctx context.Context, force bool) *kmsg.ConsumerGroupHeartbeatResponse {
	t.Helper()
	report := c.dirty || force
	resp := c.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) {
		if report {
			r.Topics = c.ownedTopics()
		}
	})
	if resp.ErrorCode == 0 && report {
		c.dirty = false
	}
	c.absorb(resp)
	return resp
}

// reconcile applies the oldest pending target (all=false) or all of them.
func (c *member848) reconcile(all bool) {
	// A member reconciles to the LATEST target it has been told, however late:
	// a response always reaches it before its next heartbeat goes out, so an
	// older target it never got round to is superseded, not applied afterwards.
	if len(c.pending) > 0 {
		c.owned = c.pending[len(c.pending)-1]
		c.pending = nil
		c.dirty = true
	}
}

func (c *member848) leave(t *testing.T, ctx context.Context) *kmsg.ConsumerGroupHeartbeatResponse {
	t.Helper()
	resp := c.raw(t, ctx, func(r *kmsg.ConsumerGroupHeartbeatRequest) { r.MemberEpoch = -1 })
	c.joined, c.epoch, c.owned, c.pending, c.dirty = false, 0, map[ownedPart]bool{}, nil, false
	return resp
}

func (c *member848) ownedOf(id [16]byte) []int32 {
	var out []int32
	for k := range c.owned {
		if k.id == id {
			out = append(out, k.p)
		}
	}
	slices.Sort(out)
	return out
}

func doubleOwned(ms ...*member848) string {
	seen := map[ownedPart]string{}
	for _, m := range ms {
		for k := range m.owned {
			if o, ok := seen[k]; ok {
				return fmt.Sprintf("partition %d owned by both %s and %s", k.p, o, m.id)
			}
			seen[k] = m.id
		}
	}
	return ""
}

// broker848 is a broker whose consumer groups compute new targets at every heartbeat
// (the assignment interval at 0, which Kafka allows): the tests that use it are of how
// members reconcile, one heartbeat at a time. broker848Interval keeps the interval.
func broker848(t *testing.T, session time.Duration) string {
	t.Helper()
	return broker848Interval(t, session, 0)
}

func broker848Interval(t *testing.T, session, interval time.Duration) string {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Version = 4
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.MetaDB = filepath.Join(dir, "meta.db")
	if session > 0 {
		cfg.ConsumerSessionTimeout = session
	}
	cfg.ConsumerAssignmentInterval = interval
	addr, stop := boot(t, cfg)
	t.Cleanup(stop)
	return addr
}

func describe848(t *testing.T, ctx context.Context, cl *kgo.Client, group string) kmsg.ConsumerGroupDescribeResponseGroup {
	t.Helper()
	req := kmsg.NewPtrConsumerGroupDescribeRequest()
	req.Groups = []string{group}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Groups) != 1 {
		t.Fatalf("describe: %v %+v", err, resp)
	}
	return resp.Groups[0]
}

func deleteTopic848(t *testing.T, ctx context.Context, cl *kgo.Client, name string) {
	t.Helper()
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{name}
	dt := kmsg.NewDeleteTopicsRequestTopic()
	dt.Topic = kmsg.StringPtr(name)
	req.Topics = append(req.Topics, dt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("delete topic: %v %+v", err, resp)
	}
}

// Randomized model test: five members doing random joins, heartbeats, late
// reconciliations, leaves, resubscriptions and rejoins against three topics, with
// two invariants checked throughout. No partition is ever held by two members;
// and once everyone behaves, every wanted partition is held by exactly one.
//
// lag=false: every member applies an assignment the moment it arrives and reports
// what it owns on every heartbeat. lag=true: members apply the latest assignment
// they were told some time later, and report only after applying. The second is
// what real clients do, and is where double ownership was found.
func run848Model(t *testing.T, seed int64, steps int, lag bool, topicChurn bool) (violations int, first string) {
	return run848ModelEvery(t, seed, steps, lag, topicChurn, 0)
}

// run848ModelEvery is run848Model with the groups' assignment interval at interval: the
// targets then trail the epoch, and the members settle once it has passed.
func run848ModelEvery(t *testing.T, seed int64, steps int, lag bool, topicChurn bool, interval time.Duration) (violations int, first string) {
	addr := broker848Interval(t, 0, interval)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	defer admin.Close()
	rng := rand.New(rand.NewSource(seed))
	names := []string{"pa", "pb", "pc"}
	parts := map[string]int32{"pa": 1, "pb": 4, "pc": 7}
	exists := map[string]bool{}
	for _, n := range names {
		mustCreateTopic(t, ctx, admin, n, parts[n])
		exists[n] = true
	}
	group := fmt.Sprintf("prop-%d-%v-%v", seed, lag, topicChurn)
	const N = 5
	ms := make([]*member848, N)
	for i := range ms {
		ms[i] = new848(t, addr, group, fmt.Sprintf("m%d", i))
		if i%2 == 1 {
			ms[i].assign = "" // only some members name an assignor
		}
	}
	randSubs := func() []string {
		var s []string
		for _, n := range names {
			if rng.Intn(3) > 0 {
				s = append(s, n)
			}
		}
		if len(s) == 0 {
			s = []string{names[rng.Intn(len(names))]}
		}
		return s
	}
	check := func(step int, what string) {
		if d := doubleOwned(ms...); d != "" {
			violations++
			if first == "" {
				first = fmt.Sprintf("step %d after %s: %s", step, what, d)
			}
		}
	}
	apply := func(m *member848) {
		if !lag {
			m.reconcile(true)
		}
	}
	handle := func(m *member848, resp *kmsg.ConsumerGroupHeartbeatResponse) {
		if resp.ErrorCode != 0 { // fenced: drop everything, rejoin later.
			m.joined, m.epoch, m.owned, m.pending, m.dirty = false, 0, map[ownedPart]bool{}, nil, false
		}
	}
	for step := 0; step < steps; step++ {
		m := ms[rng.Intn(N)]
		var what string
		switch r := rng.Intn(100); {
		case !m.joined:
			if r < 50 {
				what = m.id + " join"
				handle(m, m.join(t, ctx, randSubs()))
				apply(m)
			}
		case r < 55:
			what = m.id + " hb"
			handle(m, m.hb(t, ctx, !lag))
			apply(m)
		case r < 75:
			what = m.id + " reconcile"
			m.reconcile(false)
		case r < 82:
			what = m.id + " leave"
			m.leave(t, ctx)
		case r < 88:
			what = m.id + " resubscribe"
			subs := randSubs()
			resp := m.raw(t, ctx, func(q *kmsg.ConsumerGroupHeartbeatRequest) { q.SubscribedTopicNames = subs })
			m.absorb(resp)
			handle(m, resp)
			apply(m)
		case r < 91:
			what = m.id + " rejoin at epoch 0"
			handle(m, m.join(t, ctx, randSubs()))
			apply(m)
		case r < 94 && topicChurn:
			n := names[rng.Intn(len(names))]
			if exists[n] {
				what = "delete " + n
				deleteTopic848(t, ctx, admin, n)
				exists[n] = false
			} else {
				what = "create " + n
				mustCreateTopic(t, ctx, admin, n, parts[n])
				exists[n] = true
			}
		default: // silence
		}
		if what != "" {
			check(step, what)
		}
	}
	// Settle: every topic exists, every joined member behaves perfectly.
	for _, n := range names {
		if !exists[n] {
			mustCreateTopic(t, ctx, admin, n, parts[n])
		}
	}
	ids := map[string][16]byte{}
	for _, n := range names {
		ids[n] = topicIDOf(t, ctx, admin, n)
	}
	time.Sleep(interval + 100*time.Millisecond) // the targets are computed at the next heartbeat.
	for round := 0; round < 12; round++ {
		for _, m := range ms {
			if !m.joined {
				continue
			}
			m.reconcile(true)
			handle(m, m.hb(t, ctx, true))
			m.reconcile(true)
			check(steps+round, m.id+" settle hb")
		}
	}
	// Coverage: every partition of every topic with a live subscriber is owned exactly once.
	want := map[ownedPart]bool{}
	anyJoined := false
	for _, m := range ms {
		if !m.joined {
			continue
		}
		anyJoined = true
		for _, s := range m.subs {
			for p := int32(0); p < parts[s]; p++ {
				want[ownedPart{ids[s], p}] = true
			}
		}
	}
	got := map[ownedPart]bool{}
	for _, m := range ms {
		for k := range m.owned {
			got[k] = true
		}
	}
	var missing, extra []string
	rev := map[[16]byte]string{}
	for n, id := range ids {
		rev[id] = n
	}
	for k := range want {
		if !got[k] {
			missing = append(missing, fmt.Sprintf("%s-%d", rev[k.id], k.p))
		}
	}
	for k := range got {
		if !want[k] {
			extra = append(extra, fmt.Sprintf("%s-%d", rev[k.id], k.p))
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if anyJoined && (len(missing) > 0 || len(extra) > 0) {
		g := describe848(t, ctx, admin, group)
		t.Errorf("seed %d lag=%v churn=%v: after settling, unowned partitions %v, stale/foreign owned %v; group state %s", seed, lag, topicChurn, missing, extra, g.State)
	}
	return violations, first
}

func TestGroupKIP848_Model_InstantClient(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 4} {
		v, first := run848Model(t, seed, 3000, false, false)
		t.Logf("seed %d instant client: %d double-ownership violations %s", seed, v, first)
		if v > 0 {
			t.Errorf("seed %d: %d violations; first: %s", seed, v, first)
		}
	}
}

func TestGroupKIP848_Model_LaggingClient(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 4} {
		v, first := run848Model(t, seed, 3000, true, false)
		t.Logf("seed %d lagging client: %d double-ownership violations; first: %s", seed, v, first)
		if v > 0 {
			t.Errorf("seed %d: %d violations; first: %s", seed, v, first)
		}
	}
}

// The same with Kafka's assignment interval (1 s): the targets of most steps are
// computed late, by a heartbeat, after members have come and gone in between.
func TestGroupKIP848_Model_KafkasAssignmentInterval(t *testing.T) {
	for _, seed := range []int64{1, 2} {
		v, first := run848ModelEvery(t, seed, 3000, true, false, time.Second)
		if v > 0 {
			t.Errorf("seed %d: %d violations; first: %s", seed, v, first)
		}
	}
}

func TestGroupKIP848_Model_TopicChurn(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		v, first := run848Model(t, seed, 3000, false, true)
		t.Logf("seed %d instant client + topic delete/create: %d violations %s", seed, v, first)
		if v > 0 {
			t.Errorf("seed %d: %d violations; first: %s", seed, v, first)
		}
	}
}
