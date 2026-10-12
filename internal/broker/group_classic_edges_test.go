package broker_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/twmb/franz-go/pkg/kerr"
)

// These tests drive the classic coordinator with raw requests so they can assert
// exact error codes, ordering and timing — the things a real client's retry
// logic papers over, and where the bugs were.

const (
	codeNone               = int16(0)
	testSessionMs          = 30_000
	testRebalanceMsDefault = 30_000
)

var (
	codeIllegalGeneration = kerr.IllegalGeneration.Code
	codeUnknownMember     = kerr.UnknownMemberID.Code
	codeRebalanceInProg   = kerr.RebalanceInProgress.Code
	codeMemberIDRequired  = kerr.MemberIDRequired.Code
)

func joinRequest(group, member string, sessionMs, rebalanceMs int32) *kmsg.JoinGroupRequest {
	r := kmsg.NewPtrJoinGroupRequest()
	r.Group, r.MemberID = group, member
	r.SessionTimeoutMillis, r.RebalanceTimeoutMillis = sessionMs, rebalanceMs
	r.ProtocolType = "consumer"
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name = "range"
	p.Metadata = []byte("subscription-of-" + member)
	r.Protocols = append(r.Protocols, p)
	return r
}

// acquireID does the first half of the KIP-394 handshake: an empty member id is
// answered MEMBER_ID_REQUIRED with the id to join under.
func acquireID(t *testing.T, ctx context.Context, cl *kgo.Client, group string) string {
	t.Helper()
	resp, err := joinRequest(group, "", testSessionMs, testRebalanceMsDefault).RequestWith(ctx, cl)
	if err != nil || resp.ErrorCode != codeMemberIDRequired || resp.MemberID == "" {
		t.Fatalf("acquire member id: %v %+v", err, resp)
	}
	return resp.MemberID
}

type joinOutcome struct {
	resp *kmsg.JoinGroupResponse
	err  error
	took time.Duration
}

// joinAsync sends a JoinGroup that may park until the rebalance completes.
func joinAsync(ctx context.Context, cl *kgo.Client, group, member string, sessionMs, rebalanceMs int32) <-chan joinOutcome {
	out := make(chan joinOutcome, 1)
	start := time.Now()
	go func() {
		resp, err := joinRequest(group, member, sessionMs, rebalanceMs).RequestWith(ctx, cl)
		out <- joinOutcome{resp, err, time.Since(start)}
	}()
	return out
}

func await[T any](t *testing.T, ch <-chan T, within time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(within):
		t.Fatalf("%s: no answer within %v", what, within)
		panic("unreachable")
	}
}

func mustJoin(t *testing.T, o joinOutcome, what string) *kmsg.JoinGroupResponse {
	t.Helper()
	if o.err != nil || o.resp.ErrorCode != codeNone {
		t.Fatalf("%s: join failed: %v %+v", what, o.err, o.resp)
	}
	return o.resp
}

type syncOutcome struct {
	resp *kmsg.SyncGroupResponse
	err  error
	took time.Duration
}

func syncAsync(ctx context.Context, cl *kgo.Client, group, member string, gen int32, assignments map[string]string) <-chan syncOutcome {
	out := make(chan syncOutcome, 1)
	start := time.Now()
	go func() {
		r := kmsg.NewPtrSyncGroupRequest()
		r.Group, r.MemberID, r.Generation = group, member, gen
		r.ProtocolType, r.Protocol = kmsg.StringPtr("consumer"), kmsg.StringPtr("range")
		for id, a := range assignments {
			ga := kmsg.NewSyncGroupRequestGroupAssignment()
			ga.MemberID, ga.MemberAssignment = id, []byte(a)
			r.GroupAssignment = append(r.GroupAssignment, ga)
		}
		resp, err := r.RequestWith(ctx, cl)
		out <- syncOutcome{resp, err, time.Since(start)}
	}()
	return out
}

func heartbeatCode(t *testing.T, ctx context.Context, cl *kgo.Client, group, member string, gen int32) int16 {
	t.Helper()
	r := kmsg.NewPtrHeartbeatRequest()
	r.Group, r.MemberID, r.Generation = group, member, gen
	resp, err := r.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	return resp.ErrorCode
}

func leaveGroup(t *testing.T, ctx context.Context, cl *kgo.Client, group, member string) {
	t.Helper()
	r := kmsg.NewPtrLeaveGroupRequest()
	r.Group = group
	m := kmsg.NewLeaveGroupRequestMember()
	m.MemberID = member
	r.Members = append(r.Members, m)
	if _, err := r.RequestWith(ctx, cl); err != nil {
		t.Fatalf("leave: %v", err)
	}
}

func listedGroups(t *testing.T, ctx context.Context, cl *kgo.Client) map[string]string {
	t.Helper()
	resp, err := kmsg.NewPtrListGroupsRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	out := map[string]string{}
	for _, g := range resp.Groups {
		out[g.Group] = g.GroupState
	}
	return out
}

// stableGroupOfOne joins a single member and syncs it, returning its id and generation.
func stableGroupOfOne(t *testing.T, ctx context.Context, cl *kgo.Client, group string, sessionMs, rebalanceMs int32) (string, int32) {
	t.Helper()
	id := acquireID(t, ctx, cl, group)
	r := mustJoin(t, await(t, joinAsync(ctx, cl, group, id, sessionMs, rebalanceMs), 5*time.Second, "first join"), "first join")
	s := await(t, syncAsync(ctx, cl, group, id, r.Generation, map[string]string{id: "assign-" + id}), 5*time.Second, "first sync")
	if s.err != nil || s.resp.ErrorCode != codeNone {
		t.Fatalf("first sync: %v %+v", s.err, s.resp)
	}
	return id, r.Generation
}

// A follower parked in SyncGroup is waiting for an assignment the leader has not
// delivered. If a rebalance starts first it must be told so at once; it used to
// sit unanswered until the client's own request timeout.
func TestGroupClassic_ParkedFollowerSyncIsAnsweredWhenARebalanceStarts(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	A, B, C := newClient(t, addr), newClient(t, addr), newClient(t, addr)
	defer A.Close()
	defer B.Close()
	defer C.Close()

	const g = "parked-sync"
	ida, _ := stableGroupOfOne(t, ctx, A, g, testSessionMs, testRebalanceMsDefault)
	idb := acquireID(t, ctx, B, g)
	jB := joinAsync(ctx, B, g, idb, testSessionMs, testRebalanceMsDefault) // parks until A rejoins.
	time.Sleep(200 * time.Millisecond)                                     // a real member rejoins after learning of the rebalance, so B arrives first.
	jA := joinAsync(ctx, A, g, ida, testSessionMs, testRebalanceMsDefault)
	rA, rB := mustJoin(t, await(t, jA, 5*time.Second, "A rejoin"), "A rejoin"), mustJoin(t, await(t, jB, 5*time.Second, "B join"), "B join")
	if rA.LeaderID != ida || rA.Generation != rB.Generation {
		t.Fatalf("setup: leader %q gen %d/%d, want leader %q and one generation", rA.LeaderID, rA.Generation, rB.Generation, ida)
	}

	bSync := syncAsync(ctx, B, g, idb, rB.Generation, nil) // follower: parks, A (leader) has not synced.
	time.Sleep(300 * time.Millisecond)
	select {
	case o := <-bSync:
		t.Fatalf("the follower's sync returned before the leader synced: %+v", o)
	default:
	}

	idc := acquireID(t, ctx, C, g)
	joinAsync(ctx, C, g, idc, testSessionMs, testRebalanceMsDefault) // a new member forces a rebalance.

	o := await(t, bSync, 3*time.Second, "parked follower sync")
	if o.err != nil || o.resp.ErrorCode != codeRebalanceInProg {
		t.Fatalf("parked follower sync = %v %+v, want REBALANCE_IN_PROGRESS (%d)", o.err, o.resp, codeRebalanceInProg)
	}
}

// A member that is busy for a few seconds is not dead. The rebalance must wait
// for it for as long as it said it might take (max.poll.interval.ms), not a fixed
// 3 seconds after which it was evicted and had to start over.
func TestGroupClassic_SlowRejoinIsWaitedForNotEvicted(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	A, B := newClient(t, addr), newClient(t, addr)
	defer A.Close()
	defer B.Close()

	const g = "slow-rejoin"
	ida, _ := stableGroupOfOne(t, ctx, A, g, testSessionMs, testRebalanceMsDefault)
	idb := acquireID(t, ctx, B, g)
	jB := joinAsync(ctx, B, g, idb, testSessionMs, testRebalanceMsDefault)

	time.Sleep(3500 * time.Millisecond) // longer than the old fixed window.
	select {
	case o := <-jB:
		t.Fatalf("B's join completed after %v without A, who is still within its rebalance timeout: %+v %v", o.took, o.resp, o.err)
	default:
	}

	rA := mustJoin(t, await(t, joinAsync(ctx, A, g, ida, testSessionMs, testRebalanceMsDefault), 5*time.Second, "A rejoin"), "A rejoin (slow)")
	rB := mustJoin(t, await(t, jB, 5*time.Second, "B join"), "B join")
	if rA.Generation != rB.Generation {
		t.Fatalf("A and B ended in different generations: %d vs %d", rA.Generation, rB.Generation)
	}
	if hb := heartbeatCode(t, ctx, A, g, ida, rA.Generation); hb != codeNone {
		t.Fatalf("A heartbeat after rejoining = %d, want 0 (it was evicted)", hb)
	}
}

// The window is the members' own rebalance timeout, so a client that declares a
// short one is not made to wait longer.
func TestGroupClassic_RebalanceTimeoutIsTheMembersOwn(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	A, B := newClient(t, addr), newClient(t, addr)
	defer A.Close()
	defer B.Close()

	const g, short = "short-timeout", 1000
	ida, _ := stableGroupOfOne(t, ctx, A, g, testSessionMs, short)
	idb := acquireID(t, ctx, B, g)
	o := await(t, joinAsync(ctx, B, g, idb, testSessionMs, short), 6*time.Second, "B join")
	r := mustJoin(t, o, "B join")
	if o.took < 700*time.Millisecond || o.took > 2500*time.Millisecond {
		t.Errorf("B's join took %v; want about its 1s rebalance timeout (the old fixed window was 3s)", o.took)
	}
	if r.LeaderID != idb || len(r.Members) != 1 {
		t.Errorf("A never rejoined and should have been dropped: leader %q members %d", r.LeaderID, len(r.Members))
	}
	if hb := heartbeatCode(t, ctx, A, g, ida, r.Generation); hb != codeUnknownMember {
		t.Errorf("heartbeat from the dropped member = %d, want UNKNOWN_MEMBER_ID", hb)
	}
}

func TestGroupClassic_FencingCodes(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	A, B := newClient(t, addr), newClient(t, addr)
	defer A.Close()
	defer B.Close()

	const g = "fencing"
	ida, gen := stableGroupOfOne(t, ctx, A, g, testSessionMs, testRebalanceMsDefault)

	if hb := heartbeatCode(t, ctx, A, g, ida, gen); hb != codeNone {
		t.Errorf("heartbeat at the current generation = %d, want 0", hb)
	}
	if hb := heartbeatCode(t, ctx, A, g, ida, gen+7); hb != codeIllegalGeneration {
		t.Errorf("heartbeat at a stale generation = %d, want ILLEGAL_GENERATION", hb)
	}
	if hb := heartbeatCode(t, ctx, A, g, "nobody", gen); hb != codeUnknownMember {
		t.Errorf("heartbeat from an unknown member = %d, want UNKNOWN_MEMBER_ID", hb)
	}

	// OffsetCommit is fenced the same way.
	mustCreateTopic(t, ctx, A, "t", 1)
	commit := func(member string, generation int32) int16 {
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group, req.MemberID, req.Generation = g, member, generation
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = "t"
		rp := kmsg.NewOffsetCommitRequestTopicPartition()
		rp.Offset = 5
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, A)
		if err != nil {
			t.Fatal(err)
		}
		return resp.Topics[0].Partitions[0].ErrorCode
	}
	if c := commit(ida, gen+7); c != codeIllegalGeneration {
		t.Errorf("commit at a stale generation = %d, want ILLEGAL_GENERATION", c)
	}
	if c := commit("nobody", gen); c != codeUnknownMember {
		t.Errorf("commit from an unknown member = %d, want UNKNOWN_MEMBER_ID", c)
	}
	if c := commit(ida, gen); c != codeNone {
		t.Errorf("commit at the current generation = %d, want 0", c)
	}

	// While a rebalance is under way the member is told to rejoin.
	idb := acquireID(t, ctx, B, g)
	joinAsync(ctx, B, g, idb, testSessionMs, testRebalanceMsDefault) // parks: A has not rejoined.
	time.Sleep(200 * time.Millisecond)
	if hb := heartbeatCode(t, ctx, A, g, ida, gen); hb != codeRebalanceInProg {
		t.Errorf("heartbeat during a rebalance = %d, want REBALANCE_IN_PROGRESS", hb)
	}
}

// MEMBER_ID_REQUIRED is JoinGroup v4. A client older than that has no idea what
// to do with it, so it must be given an id and joined in one step.
func TestGroupClassic_MemberIDRequiredIsVersionAware(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	modern := newClient(t, addr)
	defer modern.Close()
	resp, err := joinRequest("modern", "", testSessionMs, testRebalanceMsDefault).RequestWith(ctx, modern)
	if err != nil || resp.ErrorCode != codeMemberIDRequired || resp.MemberID == "" {
		t.Fatalf("a v4+ join with no member id: %v %+v, want MEMBER_ID_REQUIRED and an id", err, resp)
	}

	old, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.MaxVersions(kversion.V2_1_0()), kgo.DialTimeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	jr := joinRequest("legacy", "", testSessionMs, testRebalanceMsDefault)
	resp, err = jr.RequestWith(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	if jr.GetVersion() >= 4 {
		t.Fatalf("test setup: the pinned client sent JoinGroup v%d", jr.GetVersion())
	}
	if resp.ErrorCode != codeNone || resp.MemberID == "" || resp.Generation < 1 {
		t.Fatalf("a v%d join with no member id: %+v, want an immediate join under a new id", jr.GetVersion(), resp)
	}
}

// If the leader's SyncGroup omits a member, that member must get an empty
// assignment for the new generation — not the previous one, which names
// partitions another member may now own.
func TestGroupClassic_AssignmentsDoNotCarryOverAGeneration(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	A, B := newClient(t, addr), newClient(t, addr)
	defer A.Close()
	defer B.Close()

	const g = "carry-over"
	ida, _ := stableGroupOfOne(t, ctx, A, g, testSessionMs, testRebalanceMsDefault)
	idb := acquireID(t, ctx, B, g)

	// Who starts the rebalance: a new member's first join does; after that only
	// the leader rejoining does (a follower rejoining with nothing changed is
	// simply told the current generation). The other one rejoins on hearing of it.
	rejoinBoth := func(first string) (int32, int32) {
		var jA, jB <-chan joinOutcome
		if first == idb {
			jB = joinAsync(ctx, B, g, idb, testSessionMs, testRebalanceMsDefault)
			time.Sleep(200 * time.Millisecond)
			jA = joinAsync(ctx, A, g, ida, testSessionMs, testRebalanceMsDefault)
		} else {
			jA = joinAsync(ctx, A, g, ida, testSessionMs, testRebalanceMsDefault)
			time.Sleep(200 * time.Millisecond)
			jB = joinAsync(ctx, B, g, idb, testSessionMs, testRebalanceMsDefault)
		}
		rA := mustJoin(t, await(t, jA, 5*time.Second, "A join"), "A join")
		rB := mustJoin(t, await(t, jB, 5*time.Second, "B join"), "B join")
		if rA.LeaderID != ida {
			t.Fatalf("leader = %q, want %q (leadership should be sticky)", rA.LeaderID, ida)
		}
		return rA.Generation, rB.Generation
	}

	gen2, _ := rejoinBoth(idb)
	bSync := syncAsync(ctx, B, g, idb, gen2, nil)
	time.Sleep(150 * time.Millisecond)
	await(t, syncAsync(ctx, A, g, ida, gen2, map[string]string{ida: "a-gen2", idb: "b-gen2"}), 5*time.Second, "leader sync")
	if o := await(t, bSync, 5*time.Second, "B sync gen2"); o.err != nil || string(o.resp.MemberAssignment) != "b-gen2" {
		t.Fatalf("B's gen-2 assignment = %+v %v, want b-gen2", o.resp, o.err)
	}

	gen3, _ := rejoinBoth(ida)
	bSync = syncAsync(ctx, B, g, idb, gen3, nil)
	time.Sleep(150 * time.Millisecond)
	await(t, syncAsync(ctx, A, g, ida, gen3, map[string]string{ida: "a-gen3"}), 5*time.Second, "leader sync") // B omitted.
	o := await(t, bSync, 5*time.Second, "B sync gen3")
	if o.err != nil || o.resp.ErrorCode != codeNone {
		t.Fatalf("B's gen-3 sync: %+v %v", o.resp, o.err)
	}
	if len(o.resp.MemberAssignment) != 0 {
		t.Fatalf("B was handed %q for gen 3, but the leader gave it nothing: the previous generation's assignment leaked", o.resp.MemberAssignment)
	}
}

// A JoinGroup left parked must be released when its member is replaced or removed.
func TestGroupClassic_ParkedJoinsAreReleased(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	A, B, B2 := newClient(t, addr), newClient(t, addr), newClient(t, addr)
	defer A.Close()
	defer B.Close()
	defer B2.Close()

	const g = "parked-join"
	stableGroupOfOne(t, ctx, A, g, testSessionMs, testRebalanceMsDefault)
	idb := acquireID(t, ctx, B, g)

	first := joinAsync(ctx, B, g, idb, testSessionMs, testRebalanceMsDefault) // parks.
	time.Sleep(200 * time.Millisecond)
	second := joinAsync(ctx, B2, g, idb, testSessionMs, testRebalanceMsDefault) // the client retried.
	if o := await(t, first, 3*time.Second, "superseded join"); o.err != nil || o.resp.ErrorCode != codeUnknownMember {
		t.Fatalf("the superseded join = %+v %v, want UNKNOWN_MEMBER_ID", o.resp, o.err)
	}

	time.Sleep(100 * time.Millisecond)
	leaveGroup(t, ctx, A, g, idb) // the member is removed while the retry is still parked.
	if o := await(t, second, 3*time.Second, "join of a removed member"); o.err != nil || o.resp.ErrorCode != codeUnknownMember {
		t.Fatalf("the join of a removed member = %+v %v, want UNKNOWN_MEMBER_ID", o.resp, o.err)
	}
}

// A group that has just emptied is listed Empty, whether or not it holds offsets, as on
// Kafka (recorded: mixed-protocol-rolling, down-list-after-leave). One with nothing
// committed is gone at the next sweep of the offsets retention (recorded:
// offsets-retention-recreate, g-none); one with offsets stays, because those are durable
// and a consumer will come back for them.
func TestGroupClassic_EmptyGroupsAreForgottenUnlessTheyHoldOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.MetaDB = filepath.Join(dir, "meta.db")
	cfg.OffsetsRetention = time.Minute // the sweep runs every 600 ms.
	addr, stop := boot(t, cfg)
	t.Cleanup(stop)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "t", 1)
	commitOffset(t, ctx, cl, "keeper", "t", 3)

	for _, g := range []string{"keeper", "ghost"} {
		id, _ := stableGroupOfOne(t, ctx, cl, g, testSessionMs, testRebalanceMsDefault)
		if got := listedGroups(t, ctx, cl); got[g] == "" {
			t.Fatalf("group %q should be listed while it has a member", g)
		}
		leaveGroup(t, ctx, cl, g, id)
	}

	got := listedGroups(t, ctx, cl)
	for deadline := time.Now().Add(10 * time.Second); got["ghost"] != "" && time.Now().Before(deadline); got = listedGroups(t, ctx, cl) {
		if got["ghost"] != "Empty" {
			t.Fatalf("an emptied group is listed as %q until it is dropped, want Empty", got["ghost"])
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got["ghost"] != "" {
		t.Errorf("an empty group with no offsets is still listed after several sweeps: %v", got)
	}
	if got["keeper"] != "Empty" {
		t.Errorf("a group with committed offsets should stay listed as Empty: %v", got)
	}
}

// A SyncGroup that is refused names no protocol type and no protocol, where one that
// succeeds echoes the request's (recorded: mixed-protocol-parked, ps-b-sync-parked and
// ps-a-leader-sync against pj-b-sync).
func TestGroupClassic_ARefusedSyncGroupNamesNoProtocol(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, addr)
	defer cl.Close()
	id, gen := stableGroupOfOne(t, ctx, cl, "g", testSessionMs, testRebalanceMsDefault)

	sync := func(member string) *kmsg.SyncGroupResponse {
		req := kmsg.NewPtrSyncGroupRequest()
		req.Group, req.MemberID, req.Generation = "g", member, gen
		ptype, protocol := "consumer", "range"
		req.ProtocolType, req.Protocol = &ptype, &protocol
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("SyncGroup: %v", err)
		}
		return resp
	}
	if ok := sync(id); ok.ErrorCode != 0 || ok.ProtocolType == nil || *ok.ProtocolType != "consumer" || ok.Protocol == nil || *ok.Protocol != "range" {
		t.Errorf("a SyncGroup that succeeds: code %d type %v protocol %v, want the request's echoed", ok.ErrorCode, ok.ProtocolType, ok.Protocol)
	}
	if refused := sync("nobody"); refused.ErrorCode != kerr.UnknownMemberID.Code || refused.ProtocolType != nil || refused.Protocol != nil {
		t.Errorf("a refused SyncGroup: code %d type %v protocol %v, want UNKNOWN_MEMBER_ID and neither", refused.ErrorCode, refused.ProtocolType, refused.Protocol)
	}
}
