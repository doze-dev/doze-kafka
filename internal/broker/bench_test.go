package broker

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// The benchmarks drive handlers directly, with no sockets and no client library,
// so what they measure is the broker's own work — chiefly the cost of the fsyncs
// and metadata transactions a request triggers. Run them with:
//
//	go test ./internal/broker/ -run '^$' -bench . -benchtime 200x

func benchBroker(b *testing.B, topic string, partitions int32) *Broker {
	b.Helper()
	dir := b.TempDir()
	metaStore, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { metaStore.Close() })
	logs, err := log.OpenManager(filepath.Join(dir, "data"), log.ManagerOptions{SegmentBytes: 128 << 20, IndexIntervalBytes: 4096})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { logs.Close() })
	cfg := config.Default()
	cfg.FsyncInterval = 0
	br, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), logs, metaStore)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := metaStore.CreateTopic(topic, partitions, nil); err != nil {
		b.Fatal(err)
	}
	if err := logs.EnsurePartitionDirs(topic, partitions); err != nil {
		b.Fatal(err)
	}
	return br
}

func call(b *testing.B, h func(context.Context, *protocol.Request) ([]byte, error), key protocol.APIKey, version int16, body []byte) {
	b.Helper()
	if _, err := h(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: key, APIVersion: version, CorrelationID: 1},
		Body:   body,
	}); err != nil {
		b.Fatal(err)
	}
}

func produceBody(acks int16, topic string, partition int32) []byte {
	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(8)
	req.Acks = acks
	req.TimeoutMillis = 1000
	pt := kmsg.NewProduceRequestTopic()
	pt.Topic = topic
	pp := kmsg.NewProduceRequestTopicPartition()
	pp.Partition = partition
	pp.Records = plainBatch()
	pt.Partitions = append(pt.Partitions, pp)
	req.Topics = append(req.Topics, pt)
	return req.AppendTo(nil)
}

// An OffsetCommit for every partition of a 20-partition topic: what a consumer
// doing commitSync on each poll sends.
func BenchmarkOffsetCommit20Partitions(b *testing.B) {
	br := benchBroker(b, "t", 20)
	req := kmsg.NewPtrOffsetCommitRequest()
	req.SetVersion(7) // v8+ is flexible; v7 keeps the body free of tagged fields.
	req.Group, req.Generation = "g", -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = "t"
	for p := int32(0); p < 20; p++ {
		rp := kmsg.NewOffsetCommitRequestTopicPartition()
		rp.Partition, rp.Offset = p, 1
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)
	body := req.AppendTo(nil)
	b.ResetTimer()
	for range b.N {
		call(b, typed(br.handleOffsetCommit), protocol.APIKey(kmsg.OffsetCommit), 7, body)
	}
}

func BenchmarkInitProducerID(b *testing.B) {
	br := benchBroker(b, "t", 1)
	req := kmsg.NewPtrInitProducerIDRequest()
	req.SetVersion(3)
	req.TransactionTimeoutMillis = 60000
	// v3 is flexible, and what a handler is given as the body of a flexible request
	// begins with the request header's tagged fields (none: one zero byte). Without it
	// the request did not decode and the benchmark failed before measuring anything.
	body := req.AppendTo([]byte{0})
	b.ResetTimer()
	for range b.N {
		call(b, typed(br.handleInitProducerID), protocol.APIKey(kmsg.InitProducerID), 3, body)
	}
}

func BenchmarkProduceAcksOne(b *testing.B) {
	br := benchBroker(b, "t", 1)
	body := produceBody(1, "t", 0)
	b.ResetTimer()
	for range b.N {
		call(b, br.handleProduce, protocol.APIKey(kmsg.Produce), 8, body)
	}
}

func BenchmarkProduceAcksAll(b *testing.B) {
	br := benchBroker(b, "t", 1)
	body := produceBody(-1, "t", 0)
	b.ResetTimer()
	for range b.N {
		call(b, br.handleProduce, protocol.APIKey(kmsg.Produce), 8, body)
	}
}

// Concurrent acks=all producers on one partition: the case group commit exists
// for. Without it each pays a full fsync in series.
func BenchmarkProduceAcksAllParallel8(b *testing.B) {
	br := benchBroker(b, "t", 1)
	body := produceBody(-1, "t", 0)
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			call(b, br.handleProduce, protocol.APIKey(kmsg.Produce), 8, body)
		}
	})
}

// Topic lookup is on every produce, fetch and commit.
func BenchmarkTopicLookup(b *testing.B) {
	br := benchBroker(b, "orders", 3)
	b.ResetTimer()
	for range b.N {
		if _, _, ok := br.resolveTopic("orders", [16]byte{}); !ok {
			b.Fatal("missing")
		}
	}
}
