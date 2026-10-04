package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// This file holds the classic consumer-group request handlers — the thin wire
// layer that decodes each request, drives the in-memory groupManager (see
// group.go), and encodes the response. The coordinator state machine, timers, and
// deferred join/sync completion all live in group.go.

// handleFindCoordinator returns this broker as the coordinator for any group (or
// transaction) key — single-node is always its own coordinator. It answers both
// the v0-v3 single-key form and the v4+ batched CoordinatorKeys form.
func (b *Broker) handleFindCoordinator(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	fr := kreq.(*kmsg.FindCoordinatorRequest)

	resp := kmsg.NewPtrFindCoordinatorResponse()
	resp.SetVersion(req.Header.APIVersion)

	host, port, node := b.cfg.AdvertisedHost, b.advertisedPort(), b.nodeID()

	// v0-v3 single-coordinator fields.
	resp.NodeID = node
	resp.Host = host
	resp.Port = port

	// v4+ batched fields — one coordinator entry per requested key, all us.
	for _, key := range fr.CoordinatorKeys {
		c := kmsg.NewFindCoordinatorResponseCoordinator()
		c.Key = key
		c.NodeID = node
		c.Host = host
		c.Port = port
		resp.Coordinators = append(resp.Coordinators, c)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// handleJoinGroup runs the classic join relay. The first contact from a new member
// (empty member id) is answered with MEMBER_ID_REQUIRED and a fresh id (KIP-394);
// otherwise the request is parked until the rebalance window closes, then this
// member's slice of the result is returned. Only the elected leader receives every
// member's subscription metadata.
func (b *Broker) handleJoinGroup(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	jr := kreq.(*kmsg.JoinGroupRequest)

	// One group id, one protocol: a group already running under KIP-848 cannot
	// also be a classic group, or every partition would go to both.
	// (the coordinator claims the id on join; see groupClaims).
	res, err := b.groups.join(ctx, jr, clientRef{req.Header.ClientID, req.Conn.RemoteHost()})
	if err != nil {
		return nil, err
	}

	resp := kmsg.NewPtrJoinGroupResponse()
	resp.SetVersion(req.Header.APIVersion)
	resp.ErrorCode = res.errorCode
	resp.Generation = res.generation
	resp.LeaderID = res.leaderID
	resp.MemberID = res.memberID
	if res.errorCode != int16(protocol.ErrNone) {
		resp.Generation = -1 // no generation on an errored join.
	}
	if res.protocol != "" {
		p := res.protocol
		resp.Protocol = &p
	}
	if jr.ProtocolType != "" { // v7+ echoes the request's protocol type.
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
	return protocol.EncodeResponse(req.Header, resp), nil
}

// handleSyncGroup distributes assignments: the leader uploads everyone's assignment
// bytes and each member (leader included) receives its own slice. Followers block
// until the leader's SyncGroup arrives.
func (b *Broker) handleSyncGroup(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	sr := kreq.(*kmsg.SyncGroupRequest)

	res, err := b.groups.sync(ctx, sr)
	if err != nil {
		return nil, err
	}

	resp := kmsg.NewPtrSyncGroupResponse()
	resp.SetVersion(req.Header.APIVersion)
	resp.ErrorCode = res.errorCode
	resp.MemberAssignment = res.assignment
	resp.ProtocolType = sr.ProtocolType // v5+; nil echoes to nothing on older versions.
	resp.Protocol = sr.Protocol
	return protocol.EncodeResponse(req.Header, resp), nil
}

// handleHeartbeat refreshes a member's session and returns REBALANCE_IN_PROGRESS
// while a rebalance is under way (so the member rejoins) or a fencing error for a
// stale generation / unknown member.
func (b *Broker) handleHeartbeat(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	hr := kreq.(*kmsg.HeartbeatRequest)

	resp := kmsg.NewPtrHeartbeatResponse()
	resp.SetVersion(req.Header.APIVersion)
	resp.ErrorCode = b.groups.heartbeat(hr)
	return protocol.EncodeResponse(req.Header, resp), nil
}

// handleLeaveGroup removes the named members and triggers a rebalance for the rest.
// The top-level ErrorCode serves v0-v2 clients (one member per request); v3+ get a
// per-member result list.
func (b *Broker) handleLeaveGroup(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	lr := kreq.(*kmsg.LeaveGroupRequest)

	results := b.groups.leave(lr)

	resp := kmsg.NewPtrLeaveGroupResponse()
	resp.SetVersion(req.Header.APIVersion)
	if len(results) > 0 {
		resp.ErrorCode = results[0].errorCode
	}
	for _, r := range results {
		m := kmsg.NewLeaveGroupResponseMember()
		m.MemberID = r.memberID
		m.InstanceID = r.instanceID
		m.ErrorCode = r.errorCode
		resp.Members = append(resp.Members, m)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}
