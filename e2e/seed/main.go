// Command seed puts known data into a running broker for the Playwright suite:
// the browser tests read it and never have to build it. Usage: seed host:port
//
//	seed-orders   3 partitions. Partition 0 holds 45 records: keyed JSON with headers,
//	              a tombstone, a record with no key, a binary value, a very long key.
//	seed-empty    1 partition, no records.
//	seed-txn      1 partition with one aborted transaction, one committed, and one left
//	              open (producer "seed-open"; its timeout is long, so it outlasts a run).
//	g-lag         committed at offset 10 of seed-orders/0: a lag to look at.
//	g-reset-*     one group per test that resets, sets or deletes, so tests that
//	              change a group never share one.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: seed host:port")
		os.Exit(2)
	}
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
}

func run(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return err
	}
	defer cl.Close()

	for name, parts := range map[string]int32{"seed-orders": 3, "seed-empty": 1, "seed-txn": 1} {
		req := kmsg.NewPtrCreateTopicsRequest()
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, parts, 1
		req.Topics = append(req.Topics, rt)
		if resp, err := req.RequestWith(ctx, cl); err != nil || resp.Topics[0].ErrorCode != 0 {
			return fmt.Errorf("create %s: %v %+v", name, err, resp)
		}
	}
	if err := seedTransactions(ctx, addr); err != nil {
		return err
	}
	produce := func(part int32, key, value []byte, h ...kgo.RecordHeader) error {
		return cl.ProduceSync(ctx, &kgo.Record{Topic: "seed-orders", Partition: part, Key: key, Value: value, Headers: h}).FirstErr()
	}
	for i := 0; i < 40; i++ {
		v := fmt.Sprintf(`{"orderId":"ord_%04d","customer":{"id":%d,"tier":"gold"},"total":%d.50,"items":["sku-%d","sku-%d"]}`, 8900+i, 100+i%7, 20+i*3, i, i+1)
		if err := produce(0, []byte(fmt.Sprintf("ord_%04d", 8900+i)), []byte(v),
			kgo.RecordHeader{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")}, kgo.RecordHeader{Key: "source", Value: []byte("checkout")}); err != nil {
			return err
		}
	}
	extras := []struct {
		key, value []byte
		h          []kgo.RecordHeader
	}{
		{nil, []byte("plain text, no key"), nil},
		{[]byte("tombstone-key"), nil, nil},
		{[]byte{0, 1, 2}, []byte{0xde, 0xad, 0xbe, 0xef, 0, 1, 2, 3, 0xff, 0xfe}, nil},
		{[]byte(strings.Repeat("unbroken-key-", 30)), []byte(`{"big":"` + strings.Repeat("x", 3000) + `"}`),
			[]kgo.RecordHeader{{Key: strings.Repeat("longheaderkey", 12), Value: []byte(strings.Repeat("v", 300))}}},
		{[]byte("last"), []byte("the newest record"), nil},
	}
	for _, e := range extras {
		if err := produce(0, e.key, e.value, e.h...); err != nil {
			return err
		}
	}
	for i := 0; i < 12; i++ { // partitions 1 and 2, so an all-partitions view has something to merge.
		if err := produce(int32(1+i%2), []byte(fmt.Sprintf("p-%d", i)), []byte(fmt.Sprintf(`{"n":%d,"tier":"gold"}`, i))); err != nil {
			return err
		}
	}

	for _, g := range []string{"g-lag", "g-reset-earliest", "g-reset-latest", "g-set-offset", "g-delete", "g-remove-offset"} {
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group, req.Generation = g, -1
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = "seed-orders"
		for p, off := range map[int32]int64{0: 10, 1: 2} {
			rp := kmsg.NewOffsetCommitRequestTopicPartition()
			rp.Partition, rp.Offset = p, off
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
		if resp, err := req.RequestWith(ctx, cl); err != nil || resp.Topics[0].Partitions[0].ErrorCode != 0 {
			return fmt.Errorf("commit for %s: %v %+v", g, err, resp)
		}
	}
	return nil
}

// seedTransactions writes seed-txn: an aborted transaction, a committed one and one that
// is left open, each from the same producer except the open one.
func seedTransactions(ctx context.Context, addr string) error {
	newTx := func(id string) (*kgo.Client, error) {
		return kgo.NewClient(kgo.SeedBrokers(addr), kgo.TransactionalID(id), kgo.TransactionTimeout(10*time.Minute), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	}
	write := func(cl *kgo.Client, v string) error {
		return cl.ProduceSync(ctx, &kgo.Record{Topic: "seed-txn", Partition: 0, Value: []byte(v)}).FirstErr()
	}
	p, err := newTx("seed-txn")
	if err != nil {
		return err
	}
	defer p.Close()
	for _, step := range []struct {
		value string
		ok    bool
	}{{"txn-aborted-record", false}, {"txn-committed-record", true}} {
		if err := p.BeginTransaction(); err != nil {
			return err
		}
		if err := write(p, step.value); err != nil {
			return err
		}
		if err := p.EndTransaction(ctx, kgo.TransactionEndTry(step.ok)); err != nil {
			return err
		}
	}
	open, err := newTx("seed-open")
	if err != nil {
		return err
	}
	defer open.Close()
	if err := open.BeginTransaction(); err != nil {
		return err
	}
	return write(open, "txn-open-record")
}
