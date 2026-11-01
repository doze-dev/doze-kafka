package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/embed"
)

// bootWorld starts a broker and its registry in this process and returns where they
// are. Both stop with the test.
func bootWorld(t *testing.T) (addr, registryURL string) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.MetaDB = filepath.Join(dir, "meta.db")
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	srv, err := embed.NewFromConfig(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	bound, err := srv.Bind(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { srv.Serve(ctx); close(done) }()
	h, err := srv.SchemaRegistry()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	hs := httptest.NewServer(h)
	t.Cleanup(func() { hs.Close(); cancel(); <-done })
	return bound.String(), hs.URL
}

// dumpWorld reads back everything the harbour world is: every record of every
// harbour topic (key, value, headers, timestamp), the groups' committed offsets, the
// registry's subjects with their ids and the ACLs.
func dumpWorld(t *testing.T, addr, registryURL string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var out []string

	meta := kmsg.NewPtrMetadataRequest()
	mr, err := meta.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	type tp struct {
		topic string
		part  int32
	}
	var parts []tp
	for _, tt := range mr.Topics {
		if tt.Topic != nil && strings.HasPrefix(*tt.Topic, "harbour-") {
			for _, p := range tt.Partitions {
				parts = append(parts, tp{*tt.Topic, p.Partition})
			}
		}
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].topic != parts[j].topic {
			return parts[i].topic < parts[j].topic
		}
		return parts[i].part < parts[j].part
	})
	for _, p := range parts {
		off := map[string]map[int32]kgo.Offset{p.topic: {p.part: kgo.NewOffset().AtStart()}}
		rc, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumePartitions(off))
		if err != nil {
			t.Fatal(err)
		}
		end := endOffset(ctx, t, cl, p.topic, p.part)
		for seen := int64(0); ; {
			fs := rc.PollFetches(ctx)
			if fs.Err0() != nil && ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			fs.EachRecord(func(r *kgo.Record) {
				var hs []string
				for _, h := range r.Headers {
					hs = append(hs, h.Key+"="+string(h.Value))
				}
				out = append(out, fmt.Sprintf("rec %s/%d@%d key=%x val=%x ts=%d h=%v", r.Topic, r.Partition, r.Offset, r.Key, r.Value, r.Timestamp.UnixMilli(), hs))
				seen = r.Offset + 1
			})
			if seen >= end {
				break
			}
		}
		rc.Close()
	}

	for _, g := range []string{"harbour-pilots", "harbour-customs-desk", "harbour-billing", "harbour-dashboard"} {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = g
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		for _, tt := range resp.Topics {
			for _, p := range tt.Partitions {
				out = append(out, fmt.Sprintf("group %s %s/%d=%d", g, tt.Topic, p.Partition, p.Offset))
			}
		}
	}

	get := func(path string) string {
		resp, err := http.Get(registryURL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return fmt.Sprintf("%d %s", resp.StatusCode, b)
	}
	var subjects []string
	if err := json.Unmarshal([]byte(strings.TrimPrefix(get("/subjects?deleted=true"), "200 ")), &subjects); err != nil {
		t.Fatal(err)
	}
	sort.Strings(subjects)
	for _, s := range subjects {
		if !strings.HasPrefix(s, "harbour-") {
			continue
		}
		out = append(out, "subject "+s+" versions "+get("/subjects/"+s+"/versions?deleted=true"))
		out = append(out, "subject "+s+" mode "+get("/mode/"+s))
		var vs []int
		json.Unmarshal([]byte(strings.TrimPrefix(get("/subjects/"+s+"/versions?deleted=true"), "200 ")), &vs)
		for _, v := range vs {
			out = append(out, fmt.Sprintf("subject %s v%d %s", s, v, get(fmt.Sprintf("/subjects/%s/versions/%d?deleted=true", s, v))))
		}
	}

	acl := kmsg.NewPtrDescribeACLsRequest()
	acl.ResourceType, acl.ResourcePatternType, acl.Operation, acl.PermissionType = kmsg.ACLResourceTypeAny, kmsg.ACLResourcePatternTypeAny, kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny
	ar, err := acl.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range ar.Resources {
		for _, a := range r.ACLs {
			out = append(out, fmt.Sprintf("acl %d %s %d %s %s %d %d", r.ResourceType, r.ResourceName, r.ResourcePatternType, a.Principal, a.Host, a.Operation, a.PermissionType))
		}
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

func endOffset(ctx context.Context, t *testing.T, cl *kgo.Client, topic string, part int32) int64 {
	t.Helper()
	lo := kmsg.NewPtrListOffsetsRequest()
	lo.ReplicaID = -1
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = topic
	lp := kmsg.NewListOffsetsRequestTopicPartition()
	lp.Partition, lp.Timestamp = part, -1
	lt.Partitions = append(lt.Partitions, lp)
	lo.Topics = append(lo.Topics, lt)
	lr, err := lo.RequestWith(ctx, cl)
	if err != nil || len(lr.Topics) != 1 || len(lr.Topics[0].Partitions) != 1 {
		t.Fatalf("end offset of %s/%d: %v", topic, part, err)
	}
	return lr.Topics[0].Partitions[0].Offset
}

// The harbour world has the shape the boards state, is the same on every fresh
// broker, and running the seed again changes nothing.
func TestHarbourWorldIsDeterministicAndIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("boots two brokers")
	}
	a, ra := bootWorld(t)
	if err := run(a, ra); err != nil {
		t.Fatal(err)
	}
	first := dumpWorld(t, a, ra)

	if err := run(a, ra); err != nil {
		t.Fatalf("the second run: %v", err)
	}
	if again := dumpWorld(t, a, ra); again != first {
		t.Errorf("running the seed twice changed the world:\n--- first\n%s\n--- second\n%s", first, again)
	}

	b, rb := bootWorld(t)
	if err := run(b, rb); err != nil {
		t.Fatal(err)
	}
	if other := dumpWorld(t, b, rb); other != first {
		t.Errorf("two fresh brokers did not get the same world:\n--- a\n%s\n--- b\n%s", first, other)
	}

	for _, want := range []string{
		"rec harbour-arrivals/2@9", "subject harbour-alerts-value mode 200 {\"mode\":\"READONLY\"}",
		"group harbour-pilots harbour-arrivals/2=10", "subject harbour-retired-value versions 200 [1]",
	} {
		if !strings.Contains(first, want) {
			t.Errorf("the world lacks %q", want)
		}
	}
	// Seven topics, six ACLs, and one trace id on three topics.
	topics := map[string]bool{}
	acls, traced := 0, map[string]bool{}
	for _, line := range strings.Split(first, "\n") {
		if strings.HasPrefix(line, "rec ") {
			topics[strings.SplitN(strings.TrimPrefix(line, "rec "), "/", 2)[0]] = true
			if strings.Contains(line, harbourTrace) {
				traced[strings.SplitN(strings.TrimPrefix(line, "rec "), "/", 2)[0]] = true
			}
		}
		if strings.HasPrefix(line, "acl ") {
			acls++
		}
	}
	if len(topics) != 7 || acls != 6 || len(traced) != 3 {
		t.Errorf("topics %d (want 7), ACLs %d (want 6), topics carrying the trace %d (want 3)", len(topics), acls, len(traced))
	}
}
