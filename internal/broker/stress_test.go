package broker_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// A broker under everything at once: producers writing, consumers in a group
// coming and going, an admin creating, altering, listing and deleting topics.
// Run under -race this is what finds the shared state two requests touch from
// different connections. What it asserts besides "nothing panicked or deadlocked"
// is the one thing that must hold however the rest interleaves: every record that
// was acknowledged on a topic nobody deletes is read by the group.
func TestStress_ProducersConsumersAndAdminAtOnce(t *testing.T) {
	addr := startBroker(t, 4)
	d := 4 * time.Second
	if testing.Short() {
		d = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	admin := newClient(t, addr)
	defer admin.Close()
	perm := []string{"perm-a", "perm-b", "perm-c"}
	for _, topic := range perm {
		mustCreateTopic(t, ctx, admin, topic, 3)
	}

	run, stop := context.WithTimeout(ctx, d)
	defer stop()

	// ---- producers
	var (
		acked   sync.Map // "topic/value" -> struct{}: acknowledged by the broker.
		nAcked  atomic.Int64
		prodWG  sync.WaitGroup
		failure atomic.Value
	)
	for p := 0; p < 4; p++ {
		prodWG.Add(1)
		go func() {
			defer prodWG.Done()
			opts := []kgo.Opt{kgo.SeedBrokers(addr), kgo.DialTimeout(3 * time.Second)}
			if p%2 == 1 {
				opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks())) // fsync on every ack, as well.
			} else {
				opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
			}
			cl, err := kgo.NewClient(opts...)
			if err != nil {
				failure.Store(err)
				return
			}
			defer cl.Close()
			for seq := 0; run.Err() == nil; seq++ {
				topic := perm[seq%len(perm)]
				val := fmt.Sprintf("p%d-%d", p, seq)
				cl.Produce(ctx, &kgo.Record{Topic: topic, Value: []byte(val)}, func(r *kgo.Record, err error) {
					if err != nil {
						failure.Store(fmt.Errorf("produce to %s: %w", r.Topic, err))
						return
					}
					acked.Store(r.Topic+"/"+string(r.Value), struct{}{})
					nAcked.Add(1)
				})
				if seq%50 == 49 {
					cl.Flush(ctx)
				}
			}
			cl.Flush(ctx)
		}()
	}

	// ---- consumers: one group, members joining and leaving while it runs.
	var (
		seen    sync.Map
		consWG  sync.WaitGroup
		consume = func(id string, until context.Context) {
			defer consWG.Done()
			cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DialTimeout(3*time.Second),
				kgo.ConsumerGroup("stress"), kgo.ConsumeTopics(perm...), kgo.ClientID(id),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.SessionTimeout(6*time.Second),
				kgo.RebalanceTimeout(8*time.Second), kgo.AutoCommitInterval(100*time.Millisecond))
			if err != nil {
				failure.Store(err)
				return
			}
			defer cl.Close()
			for until.Err() == nil {
				pctx, pcancel := context.WithTimeout(until, 500*time.Millisecond)
				fs := cl.PollFetches(pctx)
				pcancel()
				fs.EachRecord(func(r *kgo.Record) { seen.Store(r.Topic+"/"+string(r.Value), struct{}{}) })
			}
			cl.CommitUncommittedOffsets(context.Background())
		}
	)
	allDone, finish := context.WithCancel(ctx)
	defer finish()
	for i := 0; i < 2; i++ {
		consWG.Add(1)
		go consume(fmt.Sprintf("steady-%d", i), allDone)
	}
	consWG.Add(1)
	go func() { // a member that leaves and is replaced, twice.
		defer consWG.Done()
		for round := 0; round < 2 && run.Err() == nil; round++ {
			short, c := context.WithTimeout(run, d/3)
			consWG.Add(1)
			consume(fmt.Sprintf("visitor-%d", round), short)
			c()
		}
	}()

	// ---- admin: churn topics, alter their config, list groups, describe, delete.
	var adminWG sync.WaitGroup
	adminWG.Add(1)
	go func() {
		defer adminWG.Done()
		cl := newClient(t, addr)
		defer cl.Close()
		for i := 0; run.Err() == nil; i++ {
			name := fmt.Sprintf("churn-%d", i)
			create := kmsg.NewPtrCreateTopicsRequest()
			rt := kmsg.NewCreateTopicsRequestTopic()
			rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, 2, 1
			create.Topics = append(create.Topics, rt)
			if _, err := create.RequestWith(ctx, cl); err != nil {
				failure.Store(fmt.Errorf("create %s: %w", name, err))
				return
			}
			cl.ProduceSync(ctx, &kgo.Record{Topic: name, Value: []byte("x")})
			// Alter while it is being produced to, and while the others are read.
			incrementalAlter(t, ctx, cl, kmsg.ConfigResourceTypeTopic, name, false, alterOp{"retention.ms", opSet, "60000"}, alterOp{"segment.bytes", opSet, "4096"})
			describe(t, ctx, cl, name)
			if _, err := kmsg.NewPtrListGroupsRequest().RequestWith(ctx, cl); err != nil {
				failure.Store(fmt.Errorf("list groups: %w", err))
				return
			}
			dg := kmsg.NewPtrDescribeGroupsRequest()
			dg.Groups = []string{"stress"}
			dg.RequestWith(ctx, cl)
			del := kmsg.NewPtrDeleteTopicsRequest()
			dt := kmsg.NewDeleteTopicsRequestTopic()
			dt.Topic = kmsg.StringPtr(name)
			del.Topics = append(del.Topics, dt)
			del.TopicNames = []string{name}
			if _, err := del.RequestWith(ctx, cl); err != nil {
				failure.Store(fmt.Errorf("delete %s: %w", name, err))
				return
			}
		}
	}()

	// A second admin changes the limits of a topic that is being produced to and
	// read, again and again: the log's segment size and retention move under open
	// partitions while their appends and the retention sweep run.
	adminWG.Add(1)
	go func() {
		defer adminWG.Done()
		cl := newClient(t, addr)
		defer cl.Close()
		for i := 0; run.Err() == nil; i++ {
			size := []string{"2048", "8192", "65536"}[i%3]
			// Retention generous enough that nothing the consumers still need is
			// deleted: this is about the machinery, not about trimming.
			incrementalAlter(t, ctx, cl, kmsg.ConfigResourceTypeTopic, "perm-a", false,
				alterOp{"segment.bytes", opSet, size}, alterOp{"retention.ms", opSet, "3600000"})
			if i%4 == 3 {
				incrementalAlter(t, ctx, cl, kmsg.ConfigResourceTypeTopic, "perm-a", false,
					alterOp{"segment.bytes", opDelete, ""}, alterOp{"retention.ms", opDelete, ""})
			}
			describe(t, ctx, cl, "perm-a")
		}
	}()

	prodWG.Wait()
	adminWG.Wait()
	if err, _ := failure.Load().(error); err != nil {
		t.Fatalf("a client failed: %v", err)
	}
	total := nAcked.Load()
	if total == 0 {
		t.Fatal("nothing was produced")
	}

	// Every acknowledged record is read, by whichever member had its partition.
	deadline := time.Now().Add(30 * time.Second)
	var missing []string
	for {
		missing = missing[:0]
		acked.Range(func(k, _ any) bool {
			if _, ok := seen.Load(k); !ok {
				missing = append(missing, k.(string))
			}
			return true
		})
		if len(missing) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	finish()
	consWG.Wait()
	if len(missing) > 0 {
		show := missing
		if len(show) > 5 {
			show = show[:5]
		}
		t.Errorf("%d of %d acknowledged records were never read by the group, e.g. %v", len(missing), total, show)
	}
	t.Logf("%d records acknowledged and read back through a group that was rebalancing", total)
}
