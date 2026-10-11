package broker_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestTopics_CreateTopicPersistsAcrossRestart is the Phase 2 acceptance gate at
// the protocol level: create a topic via franz-go, restart the broker over the
// same data dir, and confirm the topic + its partitions survive.
func TestTopics_CreateTopicPersistsAcrossRestart(t *testing.T) {
	node := startNode(t)
	dir, addr := node.Dir, node.Addr

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newClient(t, addr)

	// Create "orders" with 3 partitions.
	creq := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = "orders"
	ct.NumPartitions = 3
	ct.ReplicationFactor = 1
	creq.Topics = append(creq.Topics, ct)

	cresp, err := creq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("create topics: %v", err)
	}
	if len(cresp.Topics) != 1 || cresp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create response: %+v", cresp.Topics)
	}

	// Metadata now lists it with 3 partitions.
	mresp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	topic, ok := findTopic(mresp, "orders")
	if !ok {
		t.Fatalf("orders not in metadata: %+v", mresp.Topics)
	}
	if len(topic.Partitions) != 3 {
		t.Fatalf("orders has %d partitions, want 3", len(topic.Partitions))
	}
	cl.Close()

	// Restart over the same dir; the topic must survive.
	node.Restart()

	cl2 := newClient(t, addr)
	defer cl2.Close()

	mresp2, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl2)
	if err != nil {
		t.Fatalf("metadata after restart: %v", err)
	}
	topic2, ok := findTopic(mresp2, "orders")
	if !ok {
		t.Fatalf("orders missing after restart: %+v", mresp2.Topics)
	}
	if len(topic2.Partitions) != 3 {
		t.Fatalf("orders has %d partitions after restart, want 3", len(topic2.Partitions))
	}

	// Partition directories are on disk.
	if _, err := os.Stat(filepath.Join(dir, "data", "orders-0")); err != nil {
		t.Fatalf("partition dir orders-0 missing: %v", err)
	}
}
