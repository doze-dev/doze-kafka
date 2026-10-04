package broker_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"

	"github.com/twmb/franz-go/pkg/kerr"
)

var (
	codeInvalidTopic      = kerr.InvalidTopicException.Code
	codeInvalidPartitions = kerr.InvalidPartitions.Code
)

func createTopicCode(t *testing.T, ctx context.Context, addr, name string, partitions int32, validateOnly bool) kmsg.CreateTopicsResponseTopic {
	t.Helper()
	cl := newClient(t, addr)
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.ValidateOnly = validateOnly
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = name
	ct.NumPartitions = partitions
	ct.ReplicationFactor = 1
	req.Topics = append(req.Topics, ct)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) != 1 {
		t.Fatalf("CreateTopics(%q): %v %+v", name, err, resp)
	}
	return resp.Topics[0]
}

// TestTopicNames_TraversalIsRefusedOnTheWire is the end-to-end regression for the
// directory-traversal hole: CreateTopics("../victim") used to succeed, writing
// into a sibling of the data dir, and DeleteTopics then removed that sibling —
// from any client that could reach the port.
func TestTopicNames_TraversalIsRefusedOnTheWire(t *testing.T) {
	root := t.TempDir()
	victim := filepath.Join(root, "victim-0")
	if err := os.MkdirAll(victim, 0o755); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(victim, "keep.txt")
	if err := os.WriteFile(keep, []byte("precious"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.DataDir = filepath.Join(root, "data")
	cfg.MetaDB = filepath.Join(root, "meta.db")
	addr, stop := boot(t, cfg)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, name := range []string{"../victim", "bad name!", "..", "x'+(window.__pwned=1)+'", "a/b", `a\b`} {
		if got := createTopicCode(t, ctx, addr, name, 1, false); got.ErrorCode != codeInvalidTopic {
			t.Errorf("CreateTopics(%q) code = %d, want INVALID_TOPIC_EXCEPTION (%d)", name, got.ErrorCode, codeInvalidTopic)
		}
		if got := createTopicCode(t, ctx, addr, name, 1, true); got.ErrorCode != codeInvalidTopic {
			t.Errorf("CreateTopics(validateOnly, %q) code = %d, want %d", name, got.ErrorCode, codeInvalidTopic)
		}
	}

	cl := newClient(t, addr)
	defer cl.Close()

	// Auto-creation through Metadata is the other way in.
	mreq := kmsg.NewPtrMetadataRequest()
	mreq.AllowAutoTopicCreation = true
	for _, name := range []string{"../victim", "bad name!"} {
		n := name
		mt := kmsg.NewMetadataRequestTopic()
		mt.Topic = &n
		mreq.Topics = append(mreq.Topics, mt)
	}
	mresp, err := mreq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, mt := range mresp.Topics {
		if mt.ErrorCode != codeInvalidTopic {
			t.Errorf("Metadata auto-create of %q: code %d, want %d", *mt.Topic, mt.ErrorCode, codeInvalidTopic)
		}
	}
	if topics := metadataTopics(t, ctx, cl); len(topics) != 0 {
		t.Errorf("invalid topics were registered: %v", topics)
	}

	// Deleting it is the destructive half.
	dreq := kmsg.NewPtrDeleteTopicsRequest()
	dreq.TopicNames = []string{"../victim"}
	dt := kmsg.NewDeleteTopicsRequestTopic()
	n := "../victim"
	dt.Topic = &n
	dreq.Topics = append(dreq.Topics, dt)
	if _, err := dreq.RequestWith(ctx, cl); err != nil {
		t.Fatalf("DeleteTopics: %v", err)
	}

	if b, err := os.ReadFile(keep); err != nil || string(b) != "precious" {
		t.Fatalf("a directory outside the data dir was modified: %v %q", err, b)
	}
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		switch e.Name() {
		case "victim-0", "data", "meta.db":
		default:
			t.Errorf("unexpected entry %q beside the data dir", e.Name())
		}
	}
}

func TestTopicNames_ValidNamesStillWork(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, name := range []string{"orders", "payments.events.v1", "a_b-c", ".hidden"} {
		if got := createTopicCode(t, ctx, addr, name, 2, false); got.ErrorCode != 0 {
			t.Errorf("CreateTopics(%q) code = %d, want 0", name, got.ErrorCode)
		}
	}
}

func TestTopicNames_PartitionCap(t *testing.T) {
	cfg := config.Default()
	cfg.ListenAddr = "127.0.0.1:0"
	dir := t.TempDir()
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.MetaDB = filepath.Join(dir, "meta.db")
	cfg.MaxPartitions = 5
	addr, stop := boot(t, cfg)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if got := createTopicCode(t, ctx, addr, "ok", 5, false); got.ErrorCode != 0 {
		t.Errorf("5 partitions at a cap of 5: code %d, want 0", got.ErrorCode)
	}
	got := createTopicCode(t, ctx, addr, "toomany", 6, false)
	if got.ErrorCode != codeInvalidPartitions {
		t.Errorf("6 partitions at a cap of 5: code %d, want INVALID_PARTITIONS (%d)", got.ErrorCode, codeInvalidPartitions)
	}
	if got.ErrorMessage == nil {
		t.Error("the refusal should say why (the limit and the flag that sets it)")
	}
}

// A topic is registered, then its directories are created; there is no
// transaction around the two. When the second fails the first must be undone, or
// the name stays registered with nothing behind it: every produce then fails and
// CreateTopics answers TOPIC_ALREADY_EXISTS forever.
func TestTopicNames_FailedCreateLeavesNothingBehind(t *testing.T) {
	cfg := config.Default()
	cfg.ListenAddr = "127.0.0.1:0"
	dir := t.TempDir()
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.MetaDB = filepath.Join(dir, "meta.db")
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A regular file where partition 0's directory has to go.
	if err := os.WriteFile(filepath.Join(cfg.DataDir, "blocked-0"), []byte("in the way"), 0o644); err != nil {
		t.Fatal(err)
	}
	addr, stop := boot(t, cfg)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if got := createTopicCode(t, ctx, addr, "blocked", 2, false); got.ErrorCode == 0 {
		t.Fatal("CreateTopics should fail when a partition directory cannot be created")
	}
	cl := newClient(t, addr)
	defer cl.Close()
	if topics := metadataTopics(t, ctx, cl); topics["blocked"] {
		t.Fatal("the failed topic is still registered")
	}
	// With the obstruction gone (the rollback removed it), the retry works instead
	// of being told the topic already exists.
	if got := createTopicCode(t, ctx, addr, "blocked", 2, false); got.ErrorCode != 0 {
		t.Fatalf("retry after a failed create: code %d, want 0", got.ErrorCode)
	}
}
