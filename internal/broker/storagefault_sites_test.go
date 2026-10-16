//go:build faultinject

package broker_test

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/faultinject"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// BAD-4, site by site: each request whose write (or read) fails because of the
// disk answers the code Apache Kafka answers there (recorded: the full-disk-*
// scenarios), never UNKNOWN_SERVER_ERROR, which a client reads as a bug and does
// not retry. Each case arms one point of the fault seam for one request.
func TestEveryStorageFaultSiteAnswersARetriableCode_FaultSeam(t *testing.T) {
	skipShort(t)
	n := startNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	// No retries: each answer is the broker's first, not the client's last.
	cl, err := kgo.NewClient(kgo.SeedBrokers(n.Addr), kgo.RequestRetries(0), kgo.RetryTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	t.Cleanup(faultinject.Reset)
	mustCreateTopic(t, ctx, cl, "sites", 1)
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "sites", Value: []byte("v")}, &kgo.Record{Topic: "sites", Value: []byte("w")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	commit := func(group string) {
		t.Helper()
		oc := kmsg.NewPtrOffsetCommitRequest()
		oc.Group, oc.Generation = group, -1
		ot := kmsg.NewOffsetCommitRequestTopic()
		ot.Topic = "sites"
		op := kmsg.NewOffsetCommitRequestTopicPartition()
		ot.Partitions = append(ot.Partitions, op)
		oc.Topics = append(oc.Topics, ot)
		resp, err := oc.RequestWith(ctx, cl)
		if err != nil || resp.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("commit for %s: %v %+v", group, err, resp)
		}
	}
	md := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr("sites")
	md.Topics = append(md.Topics, mt)
	mresp, err := md.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	topicID := mresp.Topics[0].TopicID
	commit("g-delete")
	commit("g-offset-delete")

	// The transactional producer the transaction APIs are sent as.
	txid := "tx-sites"
	initTxn := func() *kmsg.InitProducerIDResponse {
		r := kmsg.NewPtrInitProducerIDRequest()
		r.TransactionalID, r.TransactionTimeoutMillis = &txid, 60000
		resp, err := r.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	ip := initTxn()
	if ip.ErrorCode != 0 {
		t.Fatalf("InitProducerId: %v", kerr.ErrorForCode(ip.ErrorCode))
	}
	pid, epoch := ip.ProducerID, ip.ProducerEpoch
	addPartitions := func() int16 {
		r := kmsg.NewPtrAddPartitionsToTxnRequest()
		r.TransactionalID, r.ProducerID, r.ProducerEpoch = txid, pid, epoch
		rt := kmsg.NewAddPartitionsToTxnRequestTopic()
		rt.Topic, rt.Partitions = "sites", []int32{0}
		r.Topics = append(r.Topics, rt)
		resp, err := r.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		return resp.Topics[0].Partitions[0].ErrorCode
	}
	addOffsets := func() int16 {
		r := kmsg.NewPtrAddOffsetsToTxnRequest()
		r.TransactionalID, r.ProducerID, r.ProducerEpoch, r.Group = txid, pid, epoch, "g-txn"
		resp, err := r.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		return resp.ErrorCode
	}
	txnOffsetCommit := func() int16 {
		r := kmsg.NewPtrTxnOffsetCommitRequest()
		r.TransactionalID, r.Group, r.ProducerID, r.ProducerEpoch, r.Generation = txid, "g-txn", pid, epoch, -1
		rt := kmsg.NewTxnOffsetCommitRequestTopic()
		rt.Topic = "sites"
		rp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
		rp.Offset, rp.LeaderEpoch = 1, -1
		rt.Partitions = append(rt.Partitions, rp)
		r.Topics = append(r.Topics, rt)
		resp, err := r.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		return resp.Topics[0].Partitions[0].ErrorCode
	}
	endTxn := func(commit bool) int16 {
		r := kmsg.NewPtrEndTxnRequest()
		r.TransactionalID, r.ProducerID, r.ProducerEpoch, r.Commit = txid, pid, epoch, commit
		resp, err := r.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		return resp.ErrorCode
	}

	enospc := faultinject.Fault{Err: &os.PathError{Op: "write", Path: "/data", Err: syscall.ENOSPC}}
	eio := faultinject.Fault{Err: &os.PathError{Op: "read", Path: "/data", Err: syscall.EIO}}
	cases := []struct {
		name  string
		point string
		fault faultinject.Fault
		send  func() int16
		want  int16
		ok    func() int16 // the same request once the fault is gone, when it can be sent again.
	}{
		{"DeleteRecords", faultinject.LogFileWrite, enospc, func() int16 {
			r := kmsg.NewPtrDeleteRecordsRequest()
			rt := kmsg.NewDeleteRecordsRequestTopic()
			rt.Topic = "sites"
			rp := kmsg.NewDeleteRecordsRequestTopicPartition()
			rp.Offset = 1
			rt.Partitions = append(rt.Partitions, rp)
			r.Topics = append(r.Topics, rt)
			r.TimeoutMillis = 5000
			resp, err := r.RequestWith(ctx, cl)
			if err != nil {
				t.Fatal(err)
			}
			return resp.Topics[0].Partitions[0].ErrorCode
		}, kerr.KafkaStorageError.Code, nil},
		{"DeleteGroups", faultinject.MetaWrite, enospc, func() int16 {
			r := kmsg.NewPtrDeleteGroupsRequest()
			r.Groups = []string{"g-delete"}
			resp, err := r.RequestWith(ctx, cl)
			if err != nil {
				t.Fatal(err)
			}
			return resp.Groups[0].ErrorCode
		}, kerr.KafkaStorageError.Code, nil},
		{"OffsetDelete", faultinject.MetaWrite, enospc, func() int16 {
			r := kmsg.NewPtrOffsetDeleteRequest()
			r.Group = "g-offset-delete"
			rt := kmsg.NewOffsetDeleteRequestTopic()
			rt.Topic = "sites"
			rt.Partitions = append(rt.Partitions, kmsg.NewOffsetDeleteRequestTopicPartition())
			r.Topics = append(r.Topics, rt)
			resp, err := r.RequestWith(ctx, cl)
			if err != nil {
				t.Fatal(err)
			}
			if resp.ErrorCode != 0 {
				return resp.ErrorCode
			}
			return resp.Topics[0].Partitions[0].ErrorCode
		}, kerr.KafkaStorageError.Code, nil},
		{"Fetch", faultinject.LogRead, eio, func() int16 {
			r := kmsg.NewPtrFetchRequest()
			r.ReplicaID, r.MaxWaitMillis, r.MaxBytes = -1, 100, 1<<20
			rt := kmsg.NewFetchRequestTopic()
			rt.Topic, rt.TopicID = "sites", topicID
			rp := kmsg.NewFetchRequestTopicPartition()
			rp.PartitionMaxBytes, rp.CurrentLeaderEpoch = 1<<20, -1
			rt.Partitions = append(rt.Partitions, rp)
			r.Topics = append(r.Topics, rt)
			resp, err := r.RequestWith(ctx, cl)
			if err != nil {
				t.Fatal(err)
			}
			return resp.Topics[0].Partitions[0].ErrorCode
		}, kerr.KafkaStorageError.Code, nil},
		{"InitProducerId (transactional)", faultinject.MetaWrite, enospc, func() int16 { return initTxn().ErrorCode }, kerr.NotCoordinator.Code, nil},
		{"AddPartitionsToTxn", faultinject.MetaWrite, enospc, addPartitions, kerr.NotCoordinator.Code, addPartitions},
		{"AddOffsetsToTxn", faultinject.MetaWrite, enospc, addOffsets, kerr.NotCoordinator.Code, addOffsets},
		{"TxnOffsetCommit", faultinject.MetaWrite, enospc, txnOffsetCommit, kerr.NotCoordinator.Code, txnOffsetCommit},
		{"EndTxn", faultinject.MetaWrite, enospc, func() int16 { return endTxn(false) }, kerr.NotCoordinator.Code, nil},
		{"Metadata (auto-create)", faultinject.MetaWrite, enospc, func() int16 {
			r := kmsg.NewPtrMetadataRequest()
			r.AllowAutoTopicCreation = true
			rt := kmsg.NewMetadataRequestTopic()
			rt.Topic = kmsg.StringPtr("made-by-metadata")
			r.Topics = append(r.Topics, rt)
			resp, err := r.RequestWith(ctx, cl)
			if err != nil {
				t.Fatal(err)
			}
			return resp.Topics[0].ErrorCode
		}, kerr.UnknownTopicOrPartition.Code, nil},
	}
	for _, c := range cases {
		clear := faultinject.Set(c.point, c.fault)
		got := c.send()
		clear()
		if got != c.want {
			t.Errorf("%s with %s failing: %s, want %s", c.name, c.point, name(got), name(c.want))
		}
		if c.ok != nil {
			if got := c.ok(); got != 0 {
				t.Errorf("%s once %s works again: %s, want success", c.name, c.point, name(got))
			}
		}
	}
}

func name(code int16) string {
	if code == 0 {
		return "NONE"
	}
	return kerr.ErrorForCode(code).(*kerr.Error).Message
}
