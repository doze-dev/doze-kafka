//go:build faultinject

package broker_test

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/faultinject"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var enospc = &os.PathError{Op: "write", Path: "/data", Err: syscall.ENOSPC}

// BAD-4. A full disk is KAFKA_STORAGE_ERROR (retriable, "the broker's storage is
// the problem") from every request that writes, not UNKNOWN_SERVER_ERROR (which a
// client reads as a bug and will not retry). The write seam stands in for the
// disk, so every acks level and every API can be driven in one process.
func TestFullDiskProduceIsKafkaStorageError_FaultSeam(t *testing.T) {
	skipShort(t)
	n := startNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := kgo.NewClient(kgo.SeedBrokers(n.Addr))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	mustCreateTopic(t, ctx, admin, "full", 1)
	t.Cleanup(faultinject.Reset)

	faultinject.Set("log.write", faultinject.Fault{Err: enospc})
	for name, acks := range map[string]kgo.Acks{"acks=1": kgo.LeaderAck(), "acks=all": kgo.AllISRAcks()} {
		cl, err := kgo.NewClient(kgo.SeedBrokers(n.Addr), kgo.DisableIdempotentWrite(), kgo.RequiredAcks(acks), kgo.RecordRetries(0))
		if err != nil {
			t.Fatal(err)
		}
		err = cl.ProduceSync(ctx, &kgo.Record{Topic: "full", Value: []byte("v")}).FirstErr()
		cl.Close()
		if !errors.Is(err, kerr.KafkaStorageError) {
			t.Errorf("%s on a full disk: %v, want KAFKA_STORAGE_ERROR", name, err)
		}
	}
	// acks=0 has no response to carry a code. A broker that cannot take the data
	// says so by closing the connection, as it does for a refused batch; silence
	// loses the records unnoticed.
	if closed := rawAcksZeroProduceClosesTheConnection(t, n.Addr, "full"); !closed {
		t.Errorf("acks=0 on a full disk was accepted silently; the connection should be closed")
	}

	// The idempotent producer sees the same retriable code, and its retries
	// after the disk is freed neither fail nor duplicate.
	idem, err := kgo.NewClient(kgo.SeedBrokers(n.Addr), kgo.RecordRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	defer idem.Close()
	if err := idem.ProduceSync(ctx, &kgo.Record{Topic: "full", Value: []byte("i")}).FirstErr(); !errors.Is(err, kerr.KafkaStorageError) {
		t.Errorf("idempotent producer on a full disk: %v, want KAFKA_STORAGE_ERROR", err)
	}
	faultinject.Reset()

	// Metadata writes go through meta.db.
	faultinject.Set("meta.write", faultinject.Fault{Err: enospc})
	ct := kmsg.NewPtrCreateTopicsRequest()
	nt := kmsg.NewCreateTopicsRequestTopic()
	nt.Topic, nt.NumPartitions, nt.ReplicationFactor = "wont-fit", 1, 1
	ct.Topics = append(ct.Topics, nt)
	cresp, err := ct.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if code := cresp.Topics[0].ErrorCode; code != kerr.KafkaStorageError.Code {
		t.Errorf("CreateTopics on a full disk: code %d, want KAFKA_STORAGE_ERROR (%d)", code, kerr.KafkaStorageError.Code)
	}

	oc := kmsg.NewPtrOffsetCommitRequest()
	oc.Group, oc.Generation = "g", -1
	ot := kmsg.NewOffsetCommitRequestTopic()
	ot.Topic = "full"
	op := kmsg.NewOffsetCommitRequestTopicPartition()
	op.Partition, op.Offset = 0, 0
	ot.Partitions = append(ot.Partitions, op)
	oc.Topics = append(oc.Topics, ot)
	oresp, err := oc.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	// NOT_COORDINATOR, as Kafka answers when the append to its offsets log fails
	// (recorded: full-disk-offset-commit).
	if code := oresp.Topics[0].Partitions[0].ErrorCode; code != kerr.NotCoordinator.Code {
		t.Errorf("OffsetCommit on a full disk: code %d, want NOT_COORDINATOR", code)
	}

	cp := kmsg.NewPtrCreatePartitionsRequest()
	cpt := kmsg.NewCreatePartitionsRequestTopic()
	cpt.Topic, cpt.Count = "full", 3
	cp.Topics = append(cp.Topics, cpt)
	cpresp, err := cp.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if code := cpresp.Topics[0].ErrorCode; code != kerr.KafkaStorageError.Code {
		t.Errorf("CreatePartitions on a full disk: code %d, want KAFKA_STORAGE_ERROR", code)
	}

	ac := kmsg.NewPtrIncrementalAlterConfigsRequest()
	ar := kmsg.NewIncrementalAlterConfigsRequestResource()
	ar.ResourceType, ar.ResourceName = kmsg.ConfigResourceTypeTopic, "full"
	ao := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	v := "3600000"
	ao.Name, ao.Op, ao.Value = "retention.ms", kmsg.IncrementalAlterConfigOpSet, &v
	ar.Configs = append(ar.Configs, ao)
	ac.Resources = append(ac.Resources, ar)
	aresp, err := ac.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if code := aresp.Resources[0].ErrorCode; code != kerr.KafkaStorageError.Code {
		t.Errorf("IncrementalAlterConfigs on a full disk: code %d, want KAFKA_STORAGE_ERROR", code)
	}

	// Freed: the same requests succeed, and the refused topic was not left
	// half-registered.
	faultinject.Reset()
	cresp, err = ct.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if code := cresp.Topics[0].ErrorCode; code != 0 {
		t.Errorf("CreateTopics after the disk was freed: code %d, want 0 (a refused create must not leave the name behind)", code)
	}
	if err := idem.ProduceSync(ctx, &kgo.Record{Topic: "full", Value: []byte("i")}).FirstErr(); err != nil {
		t.Errorf("produce after the disk was freed: %v", err)
	}
}

// rawAcksZeroProduceClosesTheConnection sends one acks=0 Produce v7 on a bare
// connection and reports whether the broker closed it (a client library does not
// surface that for a fire-and-forget write).
func rawAcksZeroProduceClosesTheConnection(t *testing.T, addr, topic string) bool {
	t.Helper()
	rec := kmsg.Record{Length: 0, Key: []byte("k"), Value: []byte("v")}
	rec.Length = int32(len(rec.AppendTo(nil)) - 1) // one byte of varint length for a short record.
	recBytes := rec.AppendTo(nil)
	batch := kmsg.RecordBatch{Magic: 2, ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1, NumRecords: 1, Records: recBytes}
	raw := batch.AppendTo(nil)
	batch.Length = int32(len(raw) - 12)
	raw = batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
	raw = batch.AppendTo(nil)

	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(7)
	req.Acks, req.TimeoutMillis = 0, 1000
	pt := kmsg.NewProduceRequestTopic()
	pt.Topic = topic
	pp := kmsg.NewProduceRequestTopicPartition()
	pp.Records = raw
	pt.Partitions = append(pt.Partitions, pp)
	req.Topics = append(req.Topics, pt)
	body := req.AppendTo(nil)

	hdr := binary.BigEndian.AppendUint16(nil, 0) // Produce.
	hdr = binary.BigEndian.AppendUint16(hdr, 7)
	hdr = binary.BigEndian.AppendUint32(hdr, 1)
	hdr = binary.BigEndian.AppendUint16(hdr, 4)
	hdr = append(hdr, "test"...)
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(hdr)+len(body)))
	frame = append(append(frame, hdr...), body...)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write(frame); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = io.ReadAll(c) // returns at EOF if the broker closed; at the deadline otherwise.
	var ne net.Error
	return err == nil || !(errors.As(err, &ne) && ne.Timeout())
}
