package broker_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestHandshake_MetadataViaFranzGo is the Phase 1 acceptance gate: an unmodified
// franz-go client completes the ApiVersions handshake and a Metadata request
// against a fresh broker, and sees the single broker advertised.
func TestHandshake_MetadataViaFranzGo(t *testing.T) {
	addr := startBroker(t, 4)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DialTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// A metadata request implicitly drives the ApiVersions handshake first; if
	// that failed, this would error.
	resp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("metadata request: %v", err)
	}

	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(resp.Brokers))
	}
	got := resp.Brokers[0]
	if got.Host != "localhost" {
		t.Errorf("broker host = %q, want localhost", got.Host)
	}
	_, portStr, _ := net.SplitHostPort(addr)
	if fmt.Sprintf("%d", got.Port) != portStr {
		t.Errorf("broker port = %d, want %s", got.Port, portStr)
	}
	if resp.ControllerID != got.NodeID {
		t.Errorf("controller id = %d, want %d (self)", resp.ControllerID, got.NodeID)
	}
	if len(resp.Topics) != 0 {
		t.Errorf("expected 0 topics on a fresh broker, got %d", len(resp.Topics))
	}
}

// TestHandshake_ClassicProfileBoots checks a lower version profile also handshakes.
func TestHandshake_ClassicProfileBoots(t *testing.T) {
	addr := startBroker(t, 1)

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DialTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("metadata request (v1 profile): %v", err)
	}
	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(resp.Brokers))
	}
}
