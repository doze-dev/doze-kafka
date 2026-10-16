package broker

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// The offsets retention (GRP-19 / D7), driven with a clock the test owns. The rules are
// the ones recorded from a real Kafka in conformance/recordings/offsets-retention.json;
// what is checked here is that the broker applies them, and what a restart keeps.

type expiryRig struct {
	b   *Broker
	m   meta.Store
	exp *offsetExpiry
	now time.Time
}

const testRetention = 10 * time.Minute

func newExpiryRig(t *testing.T, dir string, now time.Time, topics ...string) *expiryRig {
	t.Helper()
	return newExpiryRigRetaining(t, dir, now, testRetention, topics...)
}

// newExpiryRigRetaining is newExpiryRig with a retention other than the test's usual one.
func newExpiryRigRetaining(t *testing.T, dir string, now time.Time, retention time.Duration, topics ...string) *expiryRig {
	t.Helper()
	r := &expiryRig{now: now}
	m, err := meta.OpenWithClock(filepath.Join(dir, "meta.db"), func() time.Time { return r.now })
	if err != nil {
		t.Fatal(err)
	}
	logs, err := log.OpenManager(filepath.Join(dir, "data"), log.ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.OffsetsRetention = retention
	b, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), logs, m)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Stop(); logs.Close(); m.Close() })
	exp := b.expiry
	exp.stopAndFlush() // the sweep is run by hand, on this clock.
	r.b, r.m, r.exp = b, m, exp
	exp.now, exp.started = func() time.Time { return r.now }, now
	for _, name := range topics {
		if _, ok, _ := m.Topic(name); !ok {
			if _, err := m.CreateTopic(name, 1, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	return r
}

func (r *expiryRig) commit(t *testing.T, group, topic string, offset int64) {
	t.Helper()
	if err := r.m.CommitOffset(group, topic, 0, offset, ""); err != nil {
		t.Fatal(err)
	}
}

func (r *expiryRig) has(group, topic string) bool {
	_, ok, _ := r.m.FetchOffset(group, topic, 0)
	return ok
}

// advance moves the rig's clock, which the store stamps commits with and the sweep reads.
func (r *expiryRig) advance(d time.Duration) { r.now = r.now.Add(d) }

func (r *expiryRig) join(t *testing.T, group, topic string) (member string, gen int32) {
	t.Helper()
	noDelay(t)
	req := joinReq(group, "")
	req.Protocols[0].Metadata = subscriptionBytes(topic)
	res, err := r.b.groups.join(context.Background(), req, clientRef{})
	if err != nil || res.errorCode != 0 {
		t.Fatalf("join: %v %+v", err, res)
	}
	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.MemberID, sync.Generation = group, res.memberID, res.generation
	a := kmsg.NewSyncGroupRequestGroupAssignment()
	a.MemberID, a.MemberAssignment = res.memberID, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	sync.GroupAssignment = append(sync.GroupAssignment, a)
	if sr, err := r.b.groups.sync(context.Background(), sync); err != nil || sr.errorCode != 0 {
		t.Fatalf("sync: %v %+v", err, sr)
	}
	return res.memberID, res.generation
}

func (r *expiryRig) leave(t *testing.T, group, member string) {
	t.Helper()
	lr := kmsg.NewPtrLeaveGroupRequest()
	lr.Group = group
	m := kmsg.NewLeaveGroupRequestMember()
	m.MemberID = member
	lr.Members = append(lr.Members, m)
	r.b.groups.leave(lr)
}

func subscriptionBytes(topics ...string) []byte {
	b := binary.BigEndian.AppendUint16(nil, 0)
	b = binary.BigEndian.AppendUint32(b, uint32(len(topics)))
	for _, t := range topics {
		b = binary.BigEndian.AppendUint16(b, uint16(len(t)))
		b = append(b, t...)
	}
	return binary.BigEndian.AppendUint32(b, 0xffffffff)
}

// A group nobody ever joined: each offset is kept for the retention from its own commit.
func TestOffsetRetentionStandaloneCountsFromTheCommit(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	r.commit(t, "solo", "a", 1)

	r.advance(testRetention - time.Second)
	r.exp.sweep()
	if !r.has("solo", "a") {
		t.Fatal("expired before the retention")
	}
	r.advance(2 * time.Second)
	r.exp.sweep()
	if r.has("solo", "a") {
		t.Fatal("kept past the retention")
	}
}

// A group that had members is kept from the moment it emptied, however late its last
// commit: a commit into the empty group does not extend it (recorded: g-recommit).
func TestOffsetRetentionEmptyGroupCountsFromEmptiness(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	member, _ := r.join(t, "g", "a")
	r.commit(t, "g", "a", 5)
	r.leave(t, "g", member) // empty now (the rig's clock).

	r.advance(testRetention / 2)
	r.commit(t, "g", "a", 6) // a commit into the empty group.

	r.advance(testRetention/2 + time.Second)
	r.exp.sweep()
	if r.has("g", "a") {
		t.Fatal("an offset committed into an empty group extended its retention")
	}
	// And the group is forgotten with its offsets.
	for _, g := range r.b.listAllGroups() {
		if g.id == "g" {
			t.Errorf("an emptied group with no offsets is still listed: %+v", g)
		}
	}
}

// A group with members keeps what it reads and loses what it does not (recorded:
// g-active, ret-b).
func TestOffsetRetentionKeepsWhatALiveGroupIsSubscribedTo(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a", "b")
	r.join(t, "g", "a")
	r.commit(t, "g", "a", 1)
	r.commit(t, "g", "b", 2)

	r.advance(3 * testRetention)
	r.exp.sweep()
	if !r.has("g", "a") {
		t.Error("an offset on a topic the live group is subscribed to expired")
	}
	if r.has("g", "b") {
		t.Error("an offset on a topic the live group is not subscribed to was kept")
	}
}

// A group in the middle of a rebalance is not touched.
func TestOffsetRetentionDoesNotLookAtAGroupInARebalance(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a", "b")
	r.join(t, "g", "a")
	r.commit(t, "g", "b", 2)

	r.b.groups.mu.Lock()
	r.b.groups.groups["g"].state = statePreparingRebalance
	r.b.groups.mu.Unlock()

	r.advance(3 * testRetention)
	r.exp.sweep()
	if !r.has("g", "b") {
		t.Error("an offset was expired from a group that is rebalancing")
	}
}

// When a group became empty is kept across a restart, as the members are not: the
// clock does not start again.
func TestOffsetRetentionRemembersWhenAGroupEmptiedAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	start := time.Now()
	r := newExpiryRig(t, dir, start, "a")
	member, _ := r.join(t, "g", "a")
	r.commit(t, "g", "a", 5)
	r.leave(t, "g", member)
	r.exp.flush()
	r.b.Stop()
	r.m.Close()

	// A new broker over the same files, a retention and a bit later.
	r2 := newExpiryRig(t, dir, start.Add(testRetention+time.Minute), "a")
	r2.exp.sweep()
	if r2.has("g", "a") {
		t.Fatal("a restart gave the group a new retention")
	}
}

// A retention of zero keeps offsets for ever, whatever the group is.
func TestOffsetRetentionOffKeepsOffsetsForEver(t *testing.T) {
	r := newExpiryRigRetaining(t, t.TempDir(), time.Now(), 0, "a", "b")
	r.commit(t, "solo", "a", 1)
	member, _ := r.join(t, "emptied", "a")
	r.commit(t, "emptied", "a", 2)
	r.leave(t, "emptied", member)
	r.join(t, "live", "a")
	r.commit(t, "live", "b", 3) // a topic it does not read.

	r.advance(1000 * 24 * time.Hour)
	r.exp.sweep()
	for group, topic := range map[string]string{"solo": "a", "emptied": "a", "live": "b"} {
		if !r.has(group, topic) {
			t.Errorf("with no retention the offset of %s on %s expired", group, topic)
		}
	}
}

// Which groups have members is recorded with the retention off too: turned on later, it
// must not take a group that was live all along for one nobody joined.
func TestOffsetRetentionTurnedOnLaterKnowsTheGroupsThatWereLive(t *testing.T) {
	dir := t.TempDir()
	r := newExpiryRigRetaining(t, dir, time.Now(), 0, "a")
	r.join(t, "g", "a")
	r.commit(t, "g", "a", 5)
	r.advance(testRetention + time.Minute)
	r.m.Close() // the broker dies.

	r2 := newExpiryRig(t, dir, r.now.Add(time.Second), "a")
	r2.exp.sweep()
	if !r2.has("g", "a") {
		t.Fatal("turning the retention on expired the offset of a group that was live with it off")
	}
}

// A consumer-protocol group's members read what they subscribe to, by name and by
// pattern, and nothing else is kept for them.
func TestOffsetRetentionKeepsWhatAConsumerProtocolGroupReads(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a", "b", "c")
	req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req.Group, req.MemberID, req.RebalanceTimeoutMillis = "g", "n1", 60_000
	req.SubscribedTopicNames = []string{"a"}
	req.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	if res := r.b.cg848.heartbeat(req, clientRef{}); res.errorCode != 0 {
		t.Fatalf("join: %+v", res)
	}
	re := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	re.SetVersion(1)
	re.Group, re.MemberID, re.RebalanceTimeoutMillis = "g", "n2", 60_000
	pattern := "^b$"
	re.SubscribedTopicRegex = &pattern
	re.SubscribedTopicNames = []string{}
	re.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	if res := r.b.cg848.heartbeat(re, clientRef{}); res.errorCode != 0 {
		t.Fatalf("join with a pattern: %+v", res)
	}
	for _, topic := range []string{"a", "b", "c"} {
		r.commit(t, "g", topic, 1)
	}
	r.advance(3 * testRetention)
	r.exp.sweep()
	if !r.has("g", "a") || !r.has("g", "b") {
		t.Errorf("an offset on a subscribed topic expired (a: %v, b: %v)", r.has("g", "a"), r.has("g", "b"))
	}
	if r.has("g", "c") {
		t.Error("an offset on a topic nobody reads was kept")
	}
}

// The sweep runs on its own, on the real clock, and what it learned about empty groups
// is on disk once the broker is stopped.
func TestOffsetRetentionSweepsOnItsOwn(t *testing.T) {
	shortSessions(t)
	dir := t.TempDir()
	m, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	logs, err := log.OpenManager(filepath.Join(dir, "data"), log.ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	if _, err := m.CreateTopic("a", 1, nil); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.OffsetsRetention = 400 * time.Millisecond
	b, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), logs, m)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.CommitOffset("solo", "a", 0, 1, ""); err != nil {
		t.Fatal(err)
	}
	r := &expiryRig{b: b, m: m}
	member, _ := r.join(t, "g", "a")
	r.leave(t, "g", member) // its emptiness is recorded.

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok, _ := m.FetchOffset("solo", "a", 0); !ok {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, ok, _ := m.FetchOffset("solo", "a", 0); ok {
		t.Error("the sweep did not remove an offset past its retention")
	}
	b.Stop() // writes what is only in memory.
	known, err := m.GroupLiveness()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := known["solo"]; ok {
		t.Errorf("an entry for a group that never had members: %v", known)
	}
	if st, ok := known["g"]; ok {
		t.Errorf("an entry for a group with no members and no offsets, which a sweep drops: %+v", st)
	}
}
