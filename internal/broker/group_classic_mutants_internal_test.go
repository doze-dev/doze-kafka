package broker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// Tests of the classic coordinator's arithmetic and its edges, driven straight at the
// groupManager. A join that parks does so on a context with a deadline, so a coordinator
// that never answers fails the test instead of hanging it.

type cmJoined struct {
	res joinResult
	err error
}

func cmCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func cmJoinAsync(ctx context.Context, gm *groupManager, r *kmsg.JoinGroupRequest) <-chan cmJoined {
	ch := make(chan cmJoined, 1)
	go func() {
		res, err := gm.join(ctx, r, clientRef{})
		ch <- cmJoined{res, err}
	}()
	return ch
}

func cmWait(t *testing.T, ch <-chan cmJoined, what string) joinResult {
	t.Helper()
	select {
	case j := <-ch:
		if j.err != nil {
			t.Fatalf("%s: %v", what, j.err)
		}
		return j.res
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: no answer", what)
		panic("unreachable")
	}
}

func cmUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("never happened: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func cmLocked(gm *groupManager, f func()) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	f()
}

func cmReq(group, member string, instance *string, version int16) *kmsg.JoinGroupRequest {
	r := joinReq(group, member)
	r.SetVersion(version)
	r.InstanceID = instance
	return r
}

func cmSync(t *testing.T, gm *groupManager, group, member string, gen int32, members ...string) {
	t.Helper()
	sr := kmsg.NewPtrSyncGroupRequest()
	sr.Group, sr.MemberID, sr.Generation = group, member, gen
	for _, m := range members {
		a := kmsg.NewSyncGroupRequestGroupAssignment()
		a.MemberID, a.MemberAssignment = m, []byte("for-"+m)
		sr.GroupAssignment = append(sr.GroupAssignment, a)
	}
	if res, err := gm.sync(cmCtx(t), sr); err != nil || res.errorCode != 0 {
		t.Fatalf("sync: %v %+v", err, res)
	}
}

// cmStable makes a Stable group of the given members, joined one after another, the
// first its leader. instances[i] is member i's static instance id, or "" for none.
func cmStable(t *testing.T, gm *groupManager, group string, instances ...string) (ids []string, gen int32) {
	t.Helper()
	ctx := cmCtx(t)
	var inst []*string
	for _, in := range instances {
		if in == "" {
			inst = append(inst, nil)
			continue
		}
		in := in
		inst = append(inst, &in)
	}
	first := cmWait(t, cmJoinAsync(ctx, gm, cmReq(group, "", inst[0], 3)), "first join")
	ids = []string{first.memberID}
	gen = first.generation
	for i := 1; i < len(inst); i++ {
		next := cmJoinAsync(ctx, gm, cmReq(group, "", inst[i], 3))
		cmUntil(t, "the joiner is seated", func() bool {
			n := 0
			cmLocked(gm, func() { n = len(gm.groups[group].members) })
			return n == i+1
		})
		var rejoins []<-chan cmJoined
		for j, id := range ids {
			rejoins = append(rejoins, cmJoinAsync(ctx, gm, cmReq(group, id, inst[j], 3)))
		}
		res := cmWait(t, next, "the joiner's join")
		ids = append(ids, res.memberID)
		for j, ch := range rejoins {
			if r := cmWait(t, ch, "a rejoin"); r.errorCode != 0 || r.generation != res.generation {
				t.Fatalf("rejoin %d: %+v", j, r)
			}
		}
		gen = res.generation
	}
	cmSync(t, gm, group, ids[0], gen, ids...)
	return ids, gen
}

func sp2(s string) *string { return &s }

// A rebalance waits as long as the longest any member asked for; a member that asked
// for none is waited for as long as its session, and one with neither for the default
// (claim: groups R24c).
func TestClassicRebalanceTimeoutFallsBackToTheSessionAndThenTheDefault(t *testing.T) {
	if defaultSessionTimeout != 30*time.Second {
		t.Fatalf("the default session timeout is %v, want 30s", defaultSessionTimeout)
	}
	one := &group{members: map[string]*groupMember{"a": {rebalanceTimeout: 0, sessionTimeout: 7 * time.Second}}}
	if got := one.rebalanceTimeout(); got != 7*time.Second {
		t.Errorf("a member with no rebalance timeout and a 7 s session: %v, want 7s", got)
	}
	two := &group{members: map[string]*groupMember{
		"a": {rebalanceTimeout: 0, sessionTimeout: 0},
		"b": {rebalanceTimeout: 5 * time.Second},
	}}
	if got := two.rebalanceTimeout(); got != 30*time.Second {
		t.Errorf("a member with neither, beside one with 5 s: %v, want the default 30s", got)
	}
	if got := (&group{members: map[string]*groupMember{}}).rebalanceTimeout(); got != 30*time.Second {
		t.Errorf("a group with no members: %v, want 30s", got)
	}
	long := &group{members: map[string]*groupMember{"a": {rebalanceTimeout: 90 * time.Second, sessionTimeout: 7 * time.Second}, "b": {rebalanceTimeout: 10 * time.Second}}}
	if got := long.rebalanceTimeout(); got != 90*time.Second {
		t.Errorf("the longest of 90 s and 10 s: %v", got)
	}
}

func TestNewGroupManagerDelaysTheInitialRebalanceByTwoHundredMilliseconds(t *testing.T) {
	if got := newGroupManager(nil, nil).initialDelay; got != 200*time.Millisecond {
		t.Errorf("initial rebalance delay %v, want 200ms", got)
	}
}

// The protocol chosen for a group whose members share none is the first its first
// member offered; with none offered, none.
func TestClassicSelectProtocolWithNothingInCommon(t *testing.T) {
	g := &group{members: map[string]*groupMember{
		"a": {protocols: []groupProtocolEntry{{name: "x"}, {name: "z"}}},
		"b": {protocols: []groupProtocolEntry{{name: "y"}}},
	}}
	if got := g.selectProtocol(); got != "x" {
		t.Errorf("no protocol in common: %q, want the first member's first, x", got)
	}
	g = &group{members: map[string]*groupMember{"a": {}}}
	if got := g.selectProtocol(); got != "" {
		t.Errorf("a member offering nothing: %q, want none", got)
	}
}

// From JoinGroup v4 a dynamic member with no id is given one to come back with; an older
// client, and a static member, are joined in one step. The refusal has generation -1.
func TestClassicJoinGivesAnIDFromVersion4OnlyToADynamicMember(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	ctx := cmCtx(t)

	res, err := gm.join(ctx, cmReq("v4", "", nil, 4), clientRef{})
	if err != nil || res.errorCode != kerr.MemberIDRequired.Code || res.memberID == "" || res.generation != -1 {
		t.Fatalf("v4, no id: %v %+v, want MEMBER_ID_REQUIRED, an id, generation -1", err, res)
	}
	res, err = gm.join(ctx, cmReq("v4", "", sp2("inst"), 4), clientRef{})
	if err != nil || res.errorCode != 0 || !strings.HasPrefix(res.memberID, "inst-doze-") {
		t.Fatalf("v4, static: %v %+v, want a join under an id made of the instance", err, res)
	}
	res, err = gm.join(ctx, cmReq("v3", "", nil, 3), clientRef{})
	if err != nil || res.errorCode != 0 || res.generation != 1 {
		t.Fatalf("v3, no id: %v %+v, want a join at once", err, res)
	}

	// A refusal carries generation -1.
	bad := cmReq("v4", "", nil, 3)
	bad.SessionTimeoutMillis = 1
	if res, _ := gm.join(ctx, bad, clientRef{}); res.errorCode != kerr.InvalidSessionTimeout.Code || res.generation != -1 {
		t.Errorf("a session timeout out of bounds: %+v, want INVALID_SESSION_TIMEOUT and generation -1", res)
	}
}

func TestGenerateMemberIDAndPlural(t *testing.T) {
	if id := generateMemberID(nil); !strings.HasPrefix(id, "doze-") || strings.Contains(id, "--") {
		t.Errorf("no instance: %q", id)
	}
	if id := generateMemberID(sp2("")); !strings.HasPrefix(id, "doze-") {
		t.Errorf("an empty instance id: %q, want a plain id", id)
	}
	if id := generateMemberID(sp2("web-1")); !strings.HasPrefix(id, "web-1-doze-") {
		t.Errorf("instance web-1: %q", id)
	}
	if got := plural(1, "member"); got != "1 member" {
		t.Errorf("plural(1) = %q", got)
	}
	if got := plural(2, "member"); got != "2 members" {
		t.Errorf("plural(2) = %q", got)
	}
	if got := plural(0, "member"); got != "0 members" {
		t.Errorf("plural(0) = %q", got)
	}
}

// A commit that names no generation is allowed for a group that does not exist or has
// no members; generation 0 is a generation.
func TestClassicValidateCommitTellsNoGenerationFromGenerationZero(t *testing.T) {
	gm := newGroupManager(func(string) bool { return true }, nil)
	if code := gm.validateCommit("nobody", "", nil, -1); code != errNone {
		t.Errorf("no group, no generation: %d", code)
	}
	if code := gm.validateCommit("nobody", "m", nil, 0); code != kerr.IllegalGeneration.Code {
		t.Errorf("no group, generation 0: %d, want ILLEGAL_GENERATION", code)
	}
	gm.restoreEmpty("empty", "consumer")
	if code := gm.validateCommit("empty", "", nil, -1); code != errNone {
		t.Errorf("an empty group, no generation: %d", code)
	}
	if code := gm.validateCommit("empty", "m", nil, 0); code != kerr.UnknownMemberID.Code {
		t.Errorf("an empty group, generation 0 from a stranger: %d, want UNKNOWN_MEMBER_ID", code)
	}
}

// restoreEmpty puts back a group that is not there and leaves one that is.
func TestClassicRestoreEmptyOnlyCreates(t *testing.T) {
	gm := newGroupManager(nil, nil)
	gm.restoreEmpty("g", "consumer")
	g := gm.groups["g"]
	if g == nil || g.protocolType != "consumer" || g.state != stateEmpty {
		t.Fatalf("restored: %+v", g)
	}
	gm.restoreEmpty("g", "connect")
	if gm.groups["g"] != g || g.protocolType != "consumer" {
		t.Errorf("a group already there was changed: %+v", gm.groups["g"])
	}
}

// A member with no session timeout of its own is given the default one, not none.
func TestClassicSessionOfAMemberWithNoTimeoutIsTheDefault(t *testing.T) {
	gm := newGroupManager(nil, nil)
	g := gm.getOrCreate("g")
	m := &groupMember{id: "m"}
	g.members["m"] = m
	cmLocked(gm, func() { gm.resetSession(g, m) })
	time.Sleep(100 * time.Millisecond)
	cmLocked(gm, func() {
		if g.members["m"] == nil {
			t.Error("a member with no session timeout was evicted at once")
		}
		m.session.stop()
	})
}

// The initial rebalance delay, as Kafka does it (claim: groups R23a): a window of one
// delay; if a member the group did not have joined during it, another, cut to what the
// rebalance timeout leaves; it completes at the end of a window nobody new joined in.
func TestClassicInitialDelayWindowsAreCutToWhatTheRebalanceTimeoutLeaves(t *testing.T) {
	gm := newGroupManager(nil, nil)
	gm.initialDelay = time.Hour // the timer never fires; the test fires it.
	ctx := cmCtx(t)

	first := cmReq("w", "", nil, 3)
	first.RebalanceTimeoutMillis = int32((3*time.Hour + 30*time.Minute) / time.Millisecond)
	cmJoinAsync(ctx, gm, first)
	var g *group
	cmUntil(t, "the first window", func() bool {
		cmLocked(gm, func() { g = gm.groups["w"] })
		return g != nil && g.holding
	})
	cmLocked(gm, func() {
		if want := 2*time.Hour + 30*time.Minute; g.holdLeft != want {
			t.Errorf("after the first window, %v is left for further windows, want %v", g.holdLeft, want)
		}
	})

	// A new member joins during the window: another is waited, and what is left shrinks by one delay.
	cmJoinAsync(ctx, gm, cmReq("w", "", nil, 3))
	cmUntil(t, "the second member is seated", func() bool {
		n := 0
		cmLocked(gm, func() { n = len(g.members) })
		return n == 2
	})
	var gen uint64
	cmLocked(gm, func() {
		if !g.holdJoined {
			t.Error("a member the group did not have joined in the window, and it is not noted")
		}
		gen = g.join.gen
	})
	gm.joinExpired(g, gen)
	cmLocked(gm, func() {
		if g.state != statePreparingRebalance || !g.holding || !g.join.armed() {
			t.Errorf("after a window with a newcomer the rebalance should wait another one: state %v, holding %v, armed %v", g.state, g.holding, g.join.armed())
		}
		if want := 1*time.Hour + 30*time.Minute; g.holdLeft != want {
			t.Errorf("after the second window %v is left, want %v", g.holdLeft, want)
		}
		if g.holdJoined {
			t.Error("the new window starts with nobody new in it")
		}
	})
}

// When nothing is left for further windows the newcomer does not buy another one.
func TestClassicInitialDelayEndsWhenNothingIsLeftForAnotherWindow(t *testing.T) {
	gm := newGroupManager(nil, nil)
	gm.initialDelay = time.Hour
	ctx := cmCtx(t)

	first := cmReq("w", "", nil, 3)
	first.RebalanceTimeoutMillis = int32(time.Hour / time.Millisecond) // exactly one window.
	a := cmJoinAsync(ctx, gm, first)
	var g *group
	cmUntil(t, "the first window", func() bool {
		cmLocked(gm, func() { g = gm.groups["w"] })
		return g != nil && g.holding
	})
	cmLocked(gm, func() {
		if g.holdLeft != 0 {
			t.Errorf("a rebalance timeout of one window leaves %v, want nothing", g.holdLeft)
		}
	})
	b := cmJoinAsync(ctx, gm, cmReq("w", "", nil, 3))
	cmUntil(t, "the second member is seated", func() bool {
		n := 0
		cmLocked(gm, func() { n = len(g.members) })
		return n == 2
	})
	var gen uint64
	cmLocked(gm, func() { gen = g.join.gen })
	gm.joinExpired(g, gen)
	// Ended by that firing itself, not by another window's.
	cmLocked(gm, func() {
		if g.state != stateCompletingRebalance || g.join.armed() {
			t.Errorf("the rebalance should have ended at that firing: state %v, timer armed %v", g.state, g.join.armed())
		}
	})
	ra, rb := cmWait(t, a, "first member"), cmWait(t, b, "second member")
	if ra.errorCode != 0 || rb.errorCode != 0 || ra.generation != 1 || rb.generation != 1 {
		t.Errorf("the rebalance should have ended with both in generation 1: %+v %+v", ra, rb)
	}
	cmLocked(gm, func() {
		if g.state != stateCompletingRebalance {
			t.Errorf("state %v, want CompletingRebalance", g.state)
		}
	})
}

// Leaving a static member: by its member id and instance id together, or by either. A
// member id that is not the instance's is FENCED_INSTANCE_ID. The group then rebalances
// and no longer holds the instance.
func TestClassicLeaveOfAStaticMember(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	ids, gen := cmStable(t, gm, "s", "inst-a", "")
	g := gm.groups["s"]

	leave := func(member string, instance *string) leaveResult {
		lr := kmsg.NewPtrLeaveGroupRequest()
		lr.Group = "s"
		m := kmsg.NewLeaveGroupRequestMember()
		m.MemberID, m.InstanceID = member, instance
		lr.Members = append(lr.Members, m)
		res := gm.leave(lr)
		if len(res) != 1 {
			t.Fatalf("leave: %+v", res)
		}
		return res[0]
	}
	if r := leave("someone-else", sp2("inst-a")); r.errorCode != kerr.FencedInstanceID.Code {
		t.Errorf("the instance named with another member's id: code %d, want FENCED_INSTANCE_ID", r.errorCode)
	}
	if r := leave("", sp2("inst-unheld")); r.errorCode != kerr.UnknownMemberID.Code {
		t.Errorf("an instance nobody holds: code %d, want UNKNOWN_MEMBER_ID", r.errorCode)
	}
	cmLocked(gm, func() {
		if len(g.members) != 2 || g.state != stateStable {
			t.Fatalf("refused leaves changed the group: %d members, state %v", len(g.members), g.state)
		}
	})
	if r := leave(ids[0], sp2("inst-a")); r.errorCode != 0 || r.memberID != ids[0] {
		t.Errorf("the member's own id and instance: %+v, want success", r)
	}
	cmLocked(gm, func() {
		if _, held := g.static["inst-a"]; held {
			t.Error("the group still holds the instance of a member that left")
		}
		if g.state != statePreparingRebalance || g.generation != gen {
			t.Errorf("after the leave: state %v generation %d, want PreparingRebalance at %d", g.state, g.generation, gen)
		}
	})
	hb := kmsg.NewPtrHeartbeatRequest()
	hb.Group, hb.MemberID, hb.Generation = "s", ids[1], gen
	if code := gm.heartbeat(hb); code != kerr.RebalanceInProgress.Code {
		t.Errorf("the survivor's heartbeat: %d, want REBALANCE_IN_PROGRESS", code)
	}
}

// A leave by instance id alone names whoever holds it.
func TestClassicLeaveByInstanceIDAlone(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	ids, _ := cmStable(t, gm, "s", "", "inst-b")
	lr := kmsg.NewPtrLeaveGroupRequest()
	lr.Group = "s"
	m := kmsg.NewLeaveGroupRequestMember()
	m.InstanceID = sp2("inst-b")
	lr.Members = append(lr.Members, m)
	res := gm.leave(lr)
	if len(res) != 1 || res[0].errorCode != 0 || res[0].memberID != ids[1] {
		t.Fatalf("leave by instance: %+v, want success naming %s", res, ids[1])
	}
}

// A static member that is replaced by its restarted instance: the new member takes the
// old one's place, its session and rebalance timeouts are its own request's, and the
// leader (and the member list only a leader gets) follow it. A join the old one had
// parked is answered FENCED_INSTANCE_ID with generation -1.
func TestClassicReplacingAStaticLeaderKeepsTheGroupStable(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	ids, gen := cmStable(t, gm, "r", "inst-lead", "")
	g := gm.groups["r"]

	req := cmReq("r", "", sp2("inst-lead"), 3)
	req.SessionTimeoutMillis, req.RebalanceTimeoutMillis = 20_000, 25_000
	res, err := gm.join(cmCtx(t), req, clientRef{})
	if err != nil || res.errorCode != 0 || res.generation != gen || res.memberID == ids[0] {
		t.Fatalf("the restarted leader: %v %+v", err, res)
	}
	if res.leaderID != res.memberID || len(res.members) != 2 {
		t.Errorf("the leader's answer: leader %q, %d members listed, want itself and 2", res.leaderID, len(res.members))
	}
	cmLocked(gm, func() {
		m := g.members[res.memberID]
		if g.leader != res.memberID || g.members[ids[0]] != nil || m == nil {
			t.Fatalf("the group's leader is %q, old member present: %v", g.leader, g.members[ids[0]] != nil)
		}
		if m.sessionTimeout != 20*time.Second || m.rebalanceTimeout != 25*time.Second {
			t.Errorf("the replacement's timeouts: session %v rebalance %v, want 20s and 25s", m.sessionTimeout, m.rebalanceTimeout)
		}
		if g.state != stateStable {
			t.Errorf("state %v, want Stable", g.state)
		}
	})
}

func TestClassicReplacingAStaticFollowerLeavesTheLeaderAlone(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	ids, gen := cmStable(t, gm, "r", "", "inst-f")
	g := gm.groups["r"]

	res, err := gm.join(cmCtx(t), cmReq("r", "", sp2("inst-f"), 3), clientRef{})
	if err != nil || res.errorCode != 0 || res.generation != gen {
		t.Fatalf("the restarted follower: %v %+v", err, res)
	}
	if res.leaderID != ids[0] || len(res.members) != 0 {
		t.Errorf("the follower's answer: leader %q with %d members listed, want %q and none", res.leaderID, len(res.members), ids[0])
	}
	cmLocked(gm, func() {
		if g.leader != ids[0] {
			t.Errorf("the leader became %q", g.leader)
		}
	})
}

func TestClassicReplacingAStaticMemberAnswersTheJoinTheOldOneHadParked(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	ids, _ := cmStable(t, gm, "p", "inst-p", "", "")
	ctx := cmCtx(t)

	// The static member asks to rejoin; the others have not, so it is parked.
	old := cmJoinAsync(ctx, gm, cmReq("p", ids[0], sp2("inst-p"), 3))
	cmUntil(t, "the join is parked", func() bool {
		parked := false
		cmLocked(gm, func() { parked = gm.groups["p"].members[ids[0]].awaitingJoin })
		return parked
	})
	// Its restarted instance arrives with no member id.
	cmJoinAsync(ctx, gm, cmReq("p", "", sp2("inst-p"), 3))
	res := cmWait(t, old, "the replaced member's parked join")
	if res.errorCode != kerr.FencedInstanceID.Code || res.generation != -1 || res.memberID != ids[0] {
		t.Errorf("the parked join of the replaced member: %+v, want FENCED_INSTANCE_ID, generation -1, its own id", res)
	}
}

// A join that is superseded by a retry of the same member is never answered, as Kafka
// does not (claim: groups G4): the retry takes the member's place and is the one answered.
func TestClassicARetriedJoinLeavesTheOneItReplacesUnanswered(t *testing.T) {
	noDelay(t)
	gm := newGroupManager(nil, nil)
	ids, _ := cmStable(t, gm, "q", "", "")
	ctx := cmCtx(t)
	first := cmJoinAsync(ctx, gm, cmReq("q", ids[0], nil, 3))
	cmUntil(t, "the join is parked", func() bool {
		parked := false
		cmLocked(gm, func() { parked = gm.groups["q"].members[ids[0]].awaitingJoin })
		return parked
	})
	second := cmJoinAsync(ctx, gm, cmReq("q", ids[0], nil, 3))
	time.Sleep(100 * time.Millisecond)
	cmJoinAsync(ctx, gm, cmReq("q", ids[1], nil, 3)) // the other member completes the rebalance.
	if res := cmWait(t, second, "the retried join"); res.errorCode != 0 {
		t.Errorf("the retried join: %+v, want 0", res)
	}
	select {
	case j := <-first:
		t.Errorf("the superseded join was answered: %+v", j.res)
	case <-time.After(300 * time.Millisecond):
	}
}
