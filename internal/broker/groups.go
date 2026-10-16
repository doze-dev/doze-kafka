package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
)

// This file holds the classic consumer-group request handlers — the thin wire
// layer that decodes each request, drives the in-memory groupManager (see
// group.go), and encodes the response. The coordinator state machine, timers, and
// deferred join/sync completion all live in group.go.

// invalidRequestMessage is the sentence Apache Kafka puts in an INVALID_REQUEST
// answer that carries no reason of its own.
const invalidRequestMessage = "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details."

// handleFindCoordinator returns this broker as the coordinator for any group (or
// transaction) key — single-node is always its own coordinator. It answers both
// the v0-v3 single-key form and the v4+ batched CoordinatorKeys form.
func (b *Broker) handleFindCoordinator(ctx context.Context, req *protocol.Request, fr *kmsg.FindCoordinatorRequest, resp *kmsg.FindCoordinatorResponse) error {
	host, port, node := b.cfg.AdvertisedHost, b.advertisedPort(), b.nodeID()

	// Finding a group's coordinator needs Describe on the group; a principal that may
	// not describe it is told so here rather than at its first JoinGroup.
	// A transaction's coordinator needs Describe on the transactional id.
	groupKey := fr.CoordinatorType == 0 // 0 is a group, 1 a transaction.
	denied := func(key string) bool {
		if groupKey {
			return !b.canGroup(req.Conn, key, opDescribe)
		}
		return fr.CoordinatorType == 1 && !b.canTxnID(req.Conn, key, opDescribe)
	}
	deniedCode := func() int16 {
		if groupKey {
			return kerr.GroupAuthorizationFailed.Code
		}
		return kerr.TransactionalIDAuthorizationFailed.Code
	}

	// Only a group (0) and a transaction (1) have a coordinator here. A share
	// group (2) needs v6, which is not offered; anything else is no type at all.
	// Kafka answers INVALID_REQUEST, and says nothing in the share-group case.
	badType := fr.CoordinatorType != 0 && fr.CoordinatorType != 1 && req.Header.APIVersion >= 1
	badMessage := func() *string {
		if fr.CoordinatorType == 2 {
			return nil
		}
		m := invalidRequestMessage
		return &m
	}

	// v0-v3 single-coordinator fields. A success reads "NONE" from v1 to v3, the
	// word Kafka puts there (v0 has no message).
	resp.NodeID = node
	resp.Host = host
	resp.Port = port
	if req.Header.APIVersion >= 1 && req.Header.APIVersion <= 3 {
		none := "NONE"
		resp.ErrorMessage = &none
	}
	switch {
	case badType:
		resp.ErrorCode = kerr.InvalidRequest.Code
		resp.ErrorMessage = badMessage()
		resp.NodeID, resp.Host, resp.Port = -1, "", -1
	case denied(fr.CoordinatorKey):
		resp.ErrorCode = deniedCode()
		resp.NodeID, resp.Host, resp.Port = -1, "", -1
	}

	// v4+ batched fields — one coordinator entry per requested key, all us.
	for _, key := range fr.CoordinatorKeys {
		c := kmsg.NewFindCoordinatorResponseCoordinator()
		c.Key = key
		c.NodeID = node
		c.Host = host
		c.Port = port
		empty := ""
		c.ErrorMessage = &empty
		switch {
		case badType:
			c.ErrorCode = kerr.InvalidRequest.Code
			if m := badMessage(); m != nil {
				c.ErrorMessage = m
			}
			c.NodeID, c.Host, c.Port = -1, "", -1
		case denied(key):
			c.ErrorCode = deniedCode()
			c.NodeID, c.Host, c.Port = -1, "", -1
		}
		resp.Coordinators = append(resp.Coordinators, c)
	}
	return nil
}

// handleJoinGroup runs the classic join relay. The first contact from a new member
// (empty member id) is answered with MEMBER_ID_REQUIRED and a fresh id (KIP-394);
// otherwise the request is parked until the rebalance window closes, then this
// member's slice of the result is returned. Only the elected leader receives every
// member's subscription metadata.
func (b *Broker) handleJoinGroup(ctx context.Context, req *protocol.Request, jr *kmsg.JoinGroupRequest, resp *kmsg.JoinGroupResponse) error {
	if !b.canGroup(req.Conn, jr.Group, opRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		resp.Generation = -1
		return nil
	}
	// An empty group id is refused before anything else is looked at, the session
	// timeout included (recorded: group-requests-refused, ej-*).
	if jr.Group == "" {
		resp.ErrorCode, resp.Generation = kerr.InvalidGroupID.Code, -1
		return nil
	}
	// A group running under KIP-848 takes a classic client in as one of its members
	// (the migration policy's default, groupmigrate.go); otherwise the classic
	// coordinator claims the id on join (groupClaims).
	who := clientRef{req.Header.ClientID, req.Conn.RemoteHost()}
	res, taken := b.cg848.classicJoin(jr, who)
	if !taken {
		var err error
		if res, err = b.groups.join(ctx, jr, who); err != nil {
			return err
		}
	}

	resp.ErrorCode = res.errorCode
	resp.Generation = res.generation
	resp.LeaderID = res.leaderID
	resp.MemberID = res.memberID
	if res.errorCode != errNone {
		resp.Generation = -1 // no generation on an errored join.
	}
	if res.protocol != "" {
		p := res.protocol
		resp.Protocol = &p
	}
	// v7+ echoes the request's protocol type, on a join that succeeded: Kafka's
	// MEMBER_ID_REQUIRED (and every other refusal) carries none.
	if jr.ProtocolType != "" && res.errorCode == errNone {
		pt := jr.ProtocolType
		resp.ProtocolType = &pt
	}
	for _, mi := range res.members {
		m := kmsg.NewJoinGroupResponseMember()
		m.MemberID = mi.memberID
		m.InstanceID = mi.instanceID
		m.ProtocolMetadata = mi.metadata
		resp.Members = append(resp.Members, m)
	}
	return nil
}

// handleSyncGroup distributes assignments: the leader uploads everyone's assignment
// bytes and each member (leader included) receives its own slice. Followers block
// until the leader's SyncGroup arrives.
func (b *Broker) handleSyncGroup(ctx context.Context, req *protocol.Request, sr *kmsg.SyncGroupRequest, resp *kmsg.SyncGroupResponse) error {
	if !b.canGroup(req.Conn, sr.Group, opRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return nil
	}
	if sr.Group == "" {
		resp.ErrorCode = kerr.InvalidGroupID.Code
		return nil
	}
	if assignment, code, taken := b.cg848.classicSync(sr); taken {
		resp.ErrorCode = code
		resp.MemberAssignment = assignment
		// A refusal names no protocol (recorded: mixed-protocol-parked, ps-b-sync-parked).
		if code == errNone {
			resp.ProtocolType = sr.ProtocolType
			resp.Protocol = sr.Protocol
		}
		return nil
	}
	res, err := b.groups.sync(ctx, sr)
	if err != nil {
		return err
	}

	resp.ErrorCode = res.errorCode
	resp.MemberAssignment = res.assignment
	if res.errorCode == errNone {
		resp.ProtocolType = sr.ProtocolType // v5+; nil echoes to nothing on older versions.
		resp.Protocol = sr.Protocol
	}
	return nil
}

// handleHeartbeat refreshes a member's session and returns REBALANCE_IN_PROGRESS
// while a rebalance is under way (so the member rejoins) or a fencing error for a
// stale generation / unknown member.
func (b *Broker) handleHeartbeat(ctx context.Context, req *protocol.Request, hr *kmsg.HeartbeatRequest, resp *kmsg.HeartbeatResponse) error {
	if !b.canGroup(req.Conn, hr.Group, opRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return nil
	}
	if hr.Group == "" {
		resp.ErrorCode = kerr.InvalidGroupID.Code
		return nil
	}
	if code, taken := b.cg848.classicHeartbeat(hr); taken {
		resp.ErrorCode = code
		return nil
	}
	resp.ErrorCode = b.groups.heartbeat(hr)
	return nil
}

// handleLeaveGroup removes the named members and triggers a rebalance for the rest.
// The top-level ErrorCode serves v0-v2 clients (one member per request); v3+ get a
// per-member result list.
func (b *Broker) handleLeaveGroup(ctx context.Context, req *protocol.Request, lr *kmsg.LeaveGroupRequest, resp *kmsg.LeaveGroupResponse) error {
	if !b.canGroup(req.Conn, lr.Group, opRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		for _, m := range lr.Members {
			rm := kmsg.NewLeaveGroupResponseMember()
			rm.MemberID, rm.InstanceID, rm.ErrorCode = m.MemberID, m.InstanceID, kerr.GroupAuthorizationFailed.Code
			resp.Members = append(resp.Members, rm)
		}
		return nil
	}
	if lr.Group == "" {
		resp.ErrorCode = kerr.InvalidGroupID.Code
		return nil
	}
	results, taken := b.cg848.classicLeave(lr)
	if !taken {
		results = b.groups.leave(lr)
	}

	// From v3 a request names several members and each has its own code; the
	// request's is 0 even when none of them, or no group, is there (recorded:
	// group-requests-refused, cm-leave-unknown-member, lv-unknown-group-v4).
	if len(results) > 0 && lr.GetVersion() < 3 {
		resp.ErrorCode = results[0].errorCode
	}
	for _, r := range results {
		m := kmsg.NewLeaveGroupResponseMember()
		m.MemberID = r.memberID
		m.InstanceID = r.instanceID
		m.ErrorCode = r.errorCode
		resp.Members = append(resp.Members, m)
	}
	return nil
}
