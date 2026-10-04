package broker_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// These tests exercise the session-timeout eviction path that every franz/Sarama
// group test skips: a member that dies WITHOUT a graceful LeaveGroup. They drive
// the group protocols at the wire level (raw kmsg over a franz transport) so they
// can join and then simply stop heartbeating.

// TestGroupSessionExpiry_Classic: a classic member joins with a short session
// timeout, then goes silent. The coordinator's session timer must evict it.
func TestGroupSessionExpiry_Classic(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	join := func(memberID string) *kmsg.JoinGroupResponse {
		req := kmsg.NewPtrJoinGroupRequest()
		req.Group = "expiry-classic"
		req.SessionTimeoutMillis = 1000
		req.RebalanceTimeoutMillis = 3000
		req.MemberID = memberID
		req.ProtocolType = "consumer"
		p := kmsg.NewJoinGroupRequestProtocol()
		p.Name = "range"
		p.Metadata = []byte{0, 1, 2, 3}
		req.Protocols = append(req.Protocols, p)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("join: %v", err)
		}
		return resp
	}

	// KIP-394 two-step: first join is answered MEMBER_ID_REQUIRED with a fresh id.
	r1 := join("")
	if r1.ErrorCode != int16(79) || r1.MemberID == "" { // MEMBER_ID_REQUIRED
		t.Fatalf("first join = code %d id %q, want 79 + id", r1.ErrorCode, r1.MemberID)
	}
	r2 := join(r1.MemberID)
	if r2.ErrorCode != 0 {
		t.Fatalf("second join errored: %d", r2.ErrorCode)
	}

	// Go silent. Within a couple of session timeouts the member must be evicted —
	// DescribeGroups should show the group with zero members.
	describe := func() *kmsg.DescribeGroupsResponseGroup {
		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{"expiry-classic"}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("describe: %v", err)
		}
		return &resp.Groups[0]
	}
	if len(describe().Members) != 1 {
		t.Fatalf("expected 1 member right after join, got %d", len(describe().Members))
	}

	deadline := time.Now().Add(8 * time.Second)
	for {
		if len(describe().Members) == 0 {
			return // evicted.
		}
		if time.Now().After(deadline) {
			t.Fatal("classic member was never evicted after its session timeout")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestGroupSessionExpiry_KIP848: a broker-driven (848) member joins against a broker
// with a short session timeout, then goes silent and must be evicted — the group
// disappears (ConsumerGroupDescribe -> GROUP_ID_NOT_FOUND).
func TestGroupSessionExpiry_KIP848(t *testing.T) {
	dir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg := config.Default()
	cfg.Version = 4
	cfg.ListenAddr = addr
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.MetaDB = filepath.Join(dir, "meta.db")
	cfg.ConsumerSessionTimeout = time.Second // short, for the test.
	defer runBroker(t, cfg)()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cl := newClient(t, addr)
	defer cl.Close()
	mustCreateTopic(t, ctx, cl, "expiry-topic", 1)

	req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req.Group = "expiry-848"
	req.MemberID = "member-848-1"
	req.MemberEpoch = 0
	req.RebalanceTimeoutMillis = 3000
	req.SubscribedTopicNames = []string{"expiry-topic"}
	assignor := "uniform"
	req.ServerAssignor = &assignor
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("consumer group heartbeat: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("join heartbeat errored: %d", resp.ErrorCode)
	}

	members := func() int {
		dreq := kmsg.NewPtrConsumerGroupDescribeRequest()
		dreq.Groups = []string{"expiry-848"}
		dresp, err := dreq.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("consumer group describe: %v", err)
		}
		if dresp.Groups[0].ErrorCode != 0 {
			t.Fatalf("describe: code %d", dresp.Groups[0].ErrorCode)
		}
		return len(dresp.Groups[0].Members)
	}
	if members() != 1 {
		t.Fatal("the group should have its member right after the join heartbeat")
	}

	// Go silent; the member must be evicted, leaving an Empty group.
	deadline := time.Now().Add(8 * time.Second)
	for {
		if members() == 0 {
			return // evicted.
		}
		if time.Now().After(deadline) {
			t.Fatal("KIP-848 member was never evicted after its session timeout")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
