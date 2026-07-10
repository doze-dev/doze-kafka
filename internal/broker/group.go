package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// joinCollectTimeout bounds how long the coordinator waits for all known members
// to (re)join a rebalance before proceeding with whoever showed up. On localhost
// members rejoin in milliseconds, so this is only a backstop for a member that
// never comes back (its session timer normally evicts it first).
const joinCollectTimeout = 3 * time.Second

// defaultSessionTimeout is used when a client requests a non-positive one.
const defaultSessionTimeout = 30 * time.Second

// groupManager holds ephemeral, in-memory consumer-group state. Membership,
// generation, and assignments live only in memory and are rebuilt when clients
// rejoin after a restart; only committed offsets (in the metadata store) are
// durable. In the classic protocol the broker is a relay — the elected client
// leader computes assignments — so we shuttle opaque bytes and never assign
// partitions ourselves. See docs/consumer-groups.md.
//
// All state transitions happen under mu. Handlers block for a deferred JoinGroup
// or SyncGroup response on a per-request channel *after* releasing mu; completions
// and timer callbacks deliver on those channels via non-blocking buffered sends.
type groupManager struct {
	mu     sync.Mutex
	groups map[string]*group
}

func newGroupManager() *groupManager {
	return &groupManager{groups: make(map[string]*group)}
}

// groupState is the classic-protocol group lifecycle state.
type groupState int

const (
	stateEmpty groupState = iota
	statePreparingRebalance
	stateCompletingRebalance
	stateStable
)

// String renders the state as the name Kafka reports in Describe/ListGroups.
func (s groupState) String() string {
	switch s {
	case stateEmpty:
		return "Empty"
	case statePreparingRebalance:
		return "PreparingRebalance"
	case stateCompletingRebalance:
		return "CompletingRebalance"
	case stateStable:
		return "Stable"
	default:
		return "Unknown"
	}
}

type group struct {
	id          string
	state       groupState
	generation  int32
	leader      string
	protocol    string // selected assignor name.
	members     map[string]*groupMember
	pendingSync map[string]chan syncResult
	joinTimer   *time.Timer
}

type groupMember struct {
	id             string
	instanceID     *string
	protocols      []groupProtocolEntry // ordered by client preference.
	assignment     []byte               // from the leader's SyncGroup.
	rejoined       bool
	sessionTimeout time.Duration
	sessionTimer   *time.Timer
	joinResult     chan joinResult
}

type groupProtocolEntry struct {
	name     string
	metadata []byte
}

type joinResult struct {
	errorCode  int16
	generation int32
	protocol   string
	leaderID   string
	memberID   string
	members    []joinMemberInfo // populated only for the leader.
}

type joinMemberInfo struct {
	memberID   string
	instanceID *string
	metadata   []byte
}

type syncResult struct {
	errorCode  int16
	assignment []byte
}

type leaveResult struct {
	memberID  string
	errorCode int16
}

// join runs the classic JoinGroup relay. It blocks until the rebalance completes
// (all known members rejoined, or the collect timeout), returning this member's
// slice of the result. A first-contact member (empty id) gets MEMBER_ID_REQUIRED
// with a fresh id and must retry (KIP-394).
func (gm *groupManager) join(ctx context.Context, r *kmsg.JoinGroupRequest) (joinResult, error) {
	gm.mu.Lock()
	g := gm.getOrCreate(r.Group)

	if r.MemberID == "" {
		id := generateMemberID(r.InstanceID)
		gm.mu.Unlock()
		return joinResult{errorCode: int16(protocol.ErrMemberIDRequired), memberID: id}, nil
	}

	m := g.members[r.MemberID]
	if m == nil {
		m = &groupMember{id: r.MemberID}
		g.members[r.MemberID] = m
	}
	m.instanceID = r.InstanceID
	m.protocols = protocolEntries(r.Protocols)
	m.sessionTimeout = time.Duration(r.SessionTimeoutMillis) * time.Millisecond

	if g.state != statePreparingRebalance {
		g.beginRebalance()
	}
	m.rejoined = true
	m.joinResult = make(chan joinResult, 1)
	gm.resetSession(g, m)

	if g.allRejoined() {
		g.completeJoin()
	} else {
		gm.armJoinTimer(g)
	}

	ch := m.joinResult
	gm.mu.Unlock()

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return joinResult{}, ctx.Err()
	}
}

// sync distributes assignments. The leader uploads everyone's assignment; each
// member (leader included) receives its own slice. Followers block until the
// leader's SyncGroup arrives.
func (gm *groupManager) sync(ctx context.Context, r *kmsg.SyncGroupRequest) (syncResult, error) {
	gm.mu.Lock()
	g := gm.groups[r.Group]
	if g == nil {
		gm.mu.Unlock()
		return syncResult{errorCode: int16(protocol.ErrUnknownMemberID)}, nil
	}
	m := g.members[r.MemberID]
	if m == nil {
		gm.mu.Unlock()
		return syncResult{errorCode: int16(protocol.ErrUnknownMemberID)}, nil
	}
	if r.Generation != g.generation {
		gm.mu.Unlock()
		return syncResult{errorCode: int16(protocol.ErrIllegalGeneration)}, nil
	}
	if g.state == statePreparingRebalance {
		gm.mu.Unlock()
		return syncResult{errorCode: int16(protocol.ErrRebalanceInProgress)}, nil
	}
	gm.resetSession(g, m)

	if r.MemberID == g.leader {
		for _, a := range r.GroupAssignment {
			if target := g.members[a.MemberID]; target != nil {
				target.assignment = a.MemberAssignment
			}
		}
		g.state = stateStable
		for id, ch := range g.pendingSync {
			ch <- syncResult{assignment: g.members[id].assignment}
		}
		g.pendingSync = make(map[string]chan syncResult)
		assignment := m.assignment
		gm.mu.Unlock()
		return syncResult{assignment: assignment}, nil
	}

	// Follower: return immediately if the leader already synced, else wait.
	if g.state == stateStable {
		assignment := m.assignment
		gm.mu.Unlock()
		return syncResult{assignment: assignment}, nil
	}
	ch := make(chan syncResult, 1)
	g.pendingSync[r.MemberID] = ch
	gm.mu.Unlock()

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return syncResult{}, ctx.Err()
	}
}

// heartbeat refreshes a member's session and reports whether a rebalance is under
// way (so the member rejoins).
func (gm *groupManager) heartbeat(r *kmsg.HeartbeatRequest) int16 {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[r.Group]
	if g == nil {
		return int16(protocol.ErrUnknownMemberID)
	}
	m := g.members[r.MemberID]
	if m == nil {
		return int16(protocol.ErrUnknownMemberID)
	}
	if r.Generation != g.generation {
		return int16(protocol.ErrIllegalGeneration)
	}
	gm.resetSession(g, m)
	if g.state == statePreparingRebalance {
		return int16(protocol.ErrRebalanceInProgress)
	}
	return int16(protocol.ErrNone)
}

// leave removes the named members and triggers a rebalance for the rest.
func (gm *groupManager) leave(r *kmsg.LeaveGroupRequest) []leaveResult {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[r.Group]

	var results []leaveResult
	for _, id := range membersToLeave(r) {
		if g != nil && g.members[id] != nil {
			gm.removeMember(g, id)
			results = append(results, leaveResult{memberID: id, errorCode: int16(protocol.ErrNone)})
		} else {
			results = append(results, leaveResult{memberID: id, errorCode: int16(protocol.ErrUnknownMemberID)})
		}
	}
	if g != nil {
		gm.rebalanceAfterChange(g)
	}
	return results
}

// validateCommit fences a stale OffsetCommit. An unknown/empty group (a simple
// consumer) is allowed; a known group requires a live member at the current
// generation.
func (gm *groupManager) validateCommit(groupID, memberID string, generation int32) int16 {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	g := gm.groups[groupID]
	if g == nil || len(g.members) == 0 || generation < 0 {
		return int16(protocol.ErrNone)
	}
	if g.members[memberID] == nil {
		return int16(protocol.ErrUnknownMemberID)
	}
	if generation != g.generation {
		return int16(protocol.ErrIllegalGeneration)
	}
	return int16(protocol.ErrNone)
}

// --- internal state transitions (all called under gm.mu) ---

func (gm *groupManager) getOrCreate(id string) *group {
	g := gm.groups[id]
	if g == nil {
		g = &group{id: id, state: stateEmpty, members: make(map[string]*groupMember)}
		gm.groups[id] = g
	}
	return g
}

func (g *group) beginRebalance() {
	g.state = statePreparingRebalance
	for _, m := range g.members {
		m.rejoined = false
	}
}

func (g *group) allRejoined() bool {
	if len(g.members) == 0 {
		return false
	}
	for _, m := range g.members {
		if !m.rejoined {
			return false
		}
	}
	return true
}

// completeJoin closes the rebalance: drop members that never rejoined, bump the
// generation, elect a leader, pick a common protocol, and deliver every waiting
// JoinGroup response.
func (g *group) completeJoin() {
	if g.joinTimer != nil {
		g.joinTimer.Stop()
		g.joinTimer = nil
	}
	for id, m := range g.members {
		if !m.rejoined {
			if m.sessionTimer != nil {
				m.sessionTimer.Stop()
			}
			delete(g.members, id)
		}
	}
	if len(g.members) == 0 {
		g.state = stateEmpty
		g.leader = ""
		return
	}

	g.generation++
	g.protocol = g.selectProtocol()
	g.leader = g.electLeader()
	g.state = stateCompletingRebalance
	g.pendingSync = make(map[string]chan syncResult)

	leaderView := g.memberInfos(g.protocol)
	for id, m := range g.members {
		res := joinResult{
			generation: g.generation,
			protocol:   g.protocol,
			leaderID:   g.leader,
			memberID:   id,
		}
		if id == g.leader {
			res.members = leaderView
		}
		m.joinResult <- res
	}
}

func (gm *groupManager) armJoinTimer(g *group) {
	if g.joinTimer != nil {
		return
	}
	g.joinTimer = time.AfterFunc(joinCollectTimeout, func() {
		gm.mu.Lock()
		defer gm.mu.Unlock()
		if g.state == statePreparingRebalance {
			g.completeJoin()
		}
	})
}

func (gm *groupManager) resetSession(g *group, m *groupMember) {
	if m.sessionTimer != nil {
		m.sessionTimer.Stop()
	}
	timeout := m.sessionTimeout
	if timeout <= 0 {
		timeout = defaultSessionTimeout
	}
	id := m.id
	m.sessionTimer = time.AfterFunc(timeout, func() {
		gm.mu.Lock()
		defer gm.mu.Unlock()
		if g.members[id] == nil {
			return
		}
		gm.removeMember(g, id)
		gm.rebalanceAfterChange(g)
	})
}

func (gm *groupManager) removeMember(g *group, id string) {
	if m := g.members[id]; m != nil && m.sessionTimer != nil {
		m.sessionTimer.Stop()
	}
	delete(g.members, id)
	delete(g.pendingSync, id)
}

// rebalanceAfterChange nudges the group after a membership change (leave/expire):
// empty → Empty; otherwise start a rebalance so survivors rejoin, or finish an
// in-flight one if the departure made everyone-left rejoined.
func (gm *groupManager) rebalanceAfterChange(g *group) {
	if len(g.members) == 0 {
		g.state = stateEmpty
		g.leader = ""
		return
	}
	if g.state != statePreparingRebalance {
		g.beginRebalance()
	} else if g.allRejoined() {
		g.completeJoin()
	}
}

func (g *group) selectProtocol() string {
	ids := sortedMemberIDs(g)
	ref := g.members[ids[0]]
	for _, entry := range ref.protocols {
		if g.allSupport(entry.name) {
			return entry.name
		}
	}
	if len(ref.protocols) > 0 {
		return ref.protocols[0].name
	}
	return ""
}

func (g *group) allSupport(name string) bool {
	for _, m := range g.members {
		if !m.has(name) {
			return false
		}
	}
	return true
}

func (g *group) electLeader() string {
	if g.leader != "" {
		if _, ok := g.members[g.leader]; ok {
			return g.leader
		}
	}
	return sortedMemberIDs(g)[0]
}

func (g *group) memberInfos(protocolName string) []joinMemberInfo {
	ids := sortedMemberIDs(g)
	infos := make([]joinMemberInfo, 0, len(ids))
	for _, id := range ids {
		m := g.members[id]
		infos = append(infos, joinMemberInfo{
			memberID:   id,
			instanceID: m.instanceID,
			metadata:   m.metadataFor(protocolName),
		})
	}
	return infos
}

func (m *groupMember) metadataFor(name string) []byte {
	for _, e := range m.protocols {
		if e.name == name {
			return e.metadata
		}
	}
	return nil
}

func (m *groupMember) has(name string) bool {
	for _, e := range m.protocols {
		if e.name == name {
			return true
		}
	}
	return false
}

func sortedMemberIDs(g *group) []string {
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func protocolEntries(ps []kmsg.JoinGroupRequestProtocol) []groupProtocolEntry {
	out := make([]groupProtocolEntry, 0, len(ps))
	for _, p := range ps {
		out = append(out, groupProtocolEntry{name: p.Name, metadata: p.Metadata})
	}
	return out
}

func membersToLeave(r *kmsg.LeaveGroupRequest) []string {
	if len(r.Members) > 0 {
		ids := make([]string, 0, len(r.Members))
		for _, m := range r.Members {
			ids = append(ids, m.MemberID)
		}
		return ids
	}
	if r.MemberID != "" {
		return []string{r.MemberID}
	}
	return nil
}

func generateMemberID(instanceID *string) string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	id := "doze-" + hex.EncodeToString(buf[:])
	if instanceID != nil && *instanceID != "" {
		return *instanceID + "-" + id
	}
	return id
}
