package broker_test

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// These tests pin the fields of the group wire handlers that a refusal fills in
// by hand (-1 for "none", the empty host), and the version-dependent ones. They are
// doze's own contract, read off the handlers; the Kafka claims behind them are cited
// in groups.go and offsets.go.

// A coordinator that is refused, or asked for by a key type that has none, names no
// node: id -1, no host, port -1, in the single-key form and the batched one.
func TestWire_FindCoordinatorRefusalsNameNoNode(t *testing.T) {
	skipShort(t)
	addr := startAuthzBroker(t, "deny")

	mallory := rawAs(t, addr, "mallory") // allowed nothing.
	for _, v := range []int16{1, 3} {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.CoordinatorKey = "g"
		r := ask(t, mallory, req, v, kmsg.NewPtrFindCoordinatorResponse())
		if r.ErrorCode != kerr.GroupAuthorizationFailed.Code || r.NodeID != -1 || r.Host != "" || r.Port != -1 {
			t.Errorf("v%d denied: code %d node %d host %q port %d, want 30 / -1 / \"\" / -1", v, r.ErrorCode, r.NodeID, r.Host, r.Port)
		}
	}
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorKeys = []string{"g"}
	r := ask(t, mallory, req, 4, kmsg.NewPtrFindCoordinatorResponse())
	c := r.Coordinators[0]
	if c.ErrorCode != kerr.GroupAuthorizationFailed.Code || c.NodeID != -1 || c.Host != "" || c.Port != -1 {
		t.Errorf("v4 denied: %+v", c)
	}

	admin := rawAs(t, addr, "admin")
	// A key type that is neither a group nor a transaction.
	req = kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorType, req.CoordinatorKey = 9, "g"
	if r := ask(t, admin, req, 1, kmsg.NewPtrFindCoordinatorResponse()); r.ErrorCode != kerr.InvalidRequest.Code || r.NodeID != -1 || r.Host != "" || r.Port != -1 {
		t.Errorf("v1 type 9: code %d node %d host %q port %d", r.ErrorCode, r.NodeID, r.Host, r.Port)
	}
	req = kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorType, req.CoordinatorKeys = 9, []string{"g"}
	if c := ask(t, admin, req, 4, kmsg.NewPtrFindCoordinatorResponse()).Coordinators[0]; c.ErrorCode != kerr.InvalidRequest.Code || c.NodeID != -1 || c.Host != "" || c.Port != -1 {
		t.Errorf("v4 type 9: %+v", c)
	}

	// A success reads "NONE" from v1: the first version with a message.
	req = kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorKey = "g"
	if r := ask(t, admin, req, 1, kmsg.NewPtrFindCoordinatorResponse()); r.ErrorCode != 0 || r.ErrorMessage == nil || *r.ErrorMessage != "NONE" || r.NodeID < 0 || r.Port <= 0 {
		t.Errorf("v1 success: code %d message %v node %d port %d", r.ErrorCode, r.ErrorMessage, r.NodeID, r.Port)
	}
}

// A join that is refused answers generation -1, whatever the refusal was.
func TestWire_RefusedJoinsAnswerGenerationMinusOne(t *testing.T) {
	skipShort(t)
	addr := startAuthzBroker(t, "deny")
	join := func(c *rawConn, group, member string) *kmsg.JoinGroupResponse {
		t.Helper()
		req := kmsg.NewPtrJoinGroupRequest()
		req.Group, req.MemberID = group, member
		req.SessionTimeoutMillis, req.RebalanceTimeoutMillis, req.ProtocolType = 10_000, 10_000, "consumer"
		p := kmsg.NewJoinGroupRequestProtocol()
		p.Name = "range"
		req.Protocols = append(req.Protocols, p)
		return ask(t, c, req, 7, kmsg.NewPtrJoinGroupResponse())
	}
	admin := rawAs(t, addr, "admin")
	for _, tc := range []struct {
		what string
		c    *rawConn
		resp *kmsg.JoinGroupResponse
		code int16
	}{
		{"not allowed", rawAs(t, addr, "mallory"), nil, kerr.GroupAuthorizationFailed.Code},
		{"no group id", admin, join(admin, "", ""), kerr.InvalidGroupID.Code},
		{"a group id with a NUL", admin, join(admin, "g\x00", ""), kerr.InvalidGroupID.Code},
		{"a member id too long", admin, join(admin, "g", strings.Repeat("m", 5000)), kerr.InvalidRequest.Code},
	} {
		resp := tc.resp
		if resp == nil {
			resp = join(tc.c, "g", "")
		}
		if resp.ErrorCode != tc.code || resp.Generation != -1 {
			t.Errorf("%s: code %d generation %d, want %d and -1", tc.what, resp.ErrorCode, resp.Generation, tc.code)
		}
	}
}

// v7 echoes the protocol type on a join that succeeded and on no other: the
// MEMBER_ID_REQUIRED answer that comes first carries none.
func TestWire_JoinEchoesTheProtocolTypeOnlyOnSuccess(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	c := openRaw(t, addr)
	join := func(member string) *kmsg.JoinGroupResponse {
		t.Helper()
		req := kmsg.NewPtrJoinGroupRequest()
		req.Group, req.MemberID = "echo", member
		req.SessionTimeoutMillis, req.RebalanceTimeoutMillis, req.ProtocolType = 10_000, 10_000, "consumer"
		p := kmsg.NewJoinGroupRequestProtocol()
		p.Name = "range"
		req.Protocols = append(req.Protocols, p)
		return ask(t, c, req, 7, kmsg.NewPtrJoinGroupResponse())
	}
	first := join("")
	if first.ErrorCode != kerr.MemberIDRequired.Code || first.ProtocolType != nil || first.Generation != -1 {
		t.Fatalf("first join: code %d protocol type %v generation %d, want MEMBER_ID_REQUIRED, none, -1", first.ErrorCode, first.ProtocolType, first.Generation)
	}
	second := join(first.MemberID)
	if second.ErrorCode != 0 || second.ProtocolType == nil || *second.ProtocolType != "consumer" || second.Protocol == nil || *second.Protocol != "range" {
		t.Fatalf("second join: code %d protocol type %v protocol %v", second.ErrorCode, second.ProtocolType, second.Protocol)
	}
}

// Leave: v0 to v2 put the one member's code at the top; from v3 the top is 0 and
// each member carries its own.
func TestWire_LeaveGroupCodeIsAtTheTopOnlyBeforeV3(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	c := openRaw(t, addr)
	leave := func(v int16) *kmsg.LeaveGroupResponse {
		t.Helper()
		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group, req.MemberID = "nobody-here", "ghost" // v0-v2 name the member here
		m := kmsg.NewLeaveGroupRequestMember()
		m.MemberID = "ghost" // and v3+ here.
		req.Members = append(req.Members, m)
		return ask(t, c, req, v, kmsg.NewPtrLeaveGroupResponse())
	}
	for _, v := range []int16{1, 2} {
		if r := leave(v); r.ErrorCode != kerr.UnknownMemberID.Code {
			t.Errorf("v%d: top-level code %d, want UNKNOWN_MEMBER_ID", v, r.ErrorCode)
		}
	}
	// A v2 leave that names nobody has no result to take a code from.
	nobody := kmsg.NewPtrLeaveGroupRequest()
	nobody.Group = "nobody-here"
	if r := ask(t, c, nobody, 2, kmsg.NewPtrLeaveGroupResponse()); r.ErrorCode != 0 || len(r.Members) != 0 {
		t.Errorf("v2 naming nobody: code %d members %+v, want 0 and none", r.ErrorCode, r.Members)
	}
	r := leave(3)
	if r.ErrorCode != 0 || len(r.Members) != 1 || r.Members[0].ErrorCode != kerr.UnknownMemberID.Code {
		t.Errorf("v3: top-level code %d, members %+v, want 0 and one UNKNOWN_MEMBER_ID", r.ErrorCode, r.Members)
	}
}
