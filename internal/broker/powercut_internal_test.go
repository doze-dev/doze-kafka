package broker

// BAD-13 of plan/stages/bad-days.md: a power-cut model, which SIGKILL cannot be.
//
// The rig (durable_rig_internal_test.go) keeps, beside the live directory, what
// a power cut is entitled to leave: every file (segments, indexes, meta.db,
// log-start, the transaction mark, cleaner-offset, the clean-stop files) cut to
// its last fsync, every name created, renamed or removed since its directory's
// last fsync gone or back. This test drives a broker through every kind of
// acknowledged write there is and cuts the power after each one.

import (
	"fmt"
	"slices"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/powercut"
)

// promised is everything the broker has acknowledged so far: what a broker
// started on what a power cut leaves must still have.
type promised struct {
	records   map[topicPartition]map[int64]string // acks=all records: offset to payload.
	committed map[topicPartition][]string         // payloads of committed transactions.
	aborted   map[topicPartition][]string         // payloads of aborted ones.
	logStart  map[topicPartition]int64
	// logStartMay is a log start a DeleteRecords that was not acknowledged asked
	// for: a request that failed may have taken effect, as on any broker.
	logStartMay map[topicPartition]int64
	offsets     map[string]map[topicPartition]int64 // group to committed offset.
	topics      map[string]int32                    // topic to partition count.
	gone        []string                            // deleted topics.
	configs     map[string]map[string]string
}

func newPromised() *promised {
	return &promised{
		records: map[topicPartition]map[int64]string{}, committed: map[topicPartition][]string{},
		aborted: map[topicPartition][]string{}, logStart: map[topicPartition]int64{},
		logStartMay: map[topicPartition]int64{},
		offsets:     map[string]map[topicPartition]int64{}, topics: map[string]int32{},
		configs: map[string]map[string]string{},
	}
}

// check fails the test for anything acknowledged that the broker after the cut
// does not have.
func (w *promised) check(t *testing.T, after *rig, when string) {
	t.Helper()
	all, err := after.meta.ListTopics()
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]int32{}
	cfg := map[string]map[string]string{}
	for _, tp := range all {
		have[tp.Name], cfg[tp.Name] = tp.Partitions, tp.Config
	}
	for name, n := range w.topics {
		if have[name] != n {
			t.Fatalf("%s: topic %s has %d partitions after the cut, want %d", when, name, have[name], n)
		}
		for k, v := range w.configs[name] {
			if cfg[name][k] != v {
				t.Fatalf("%s: topic %s config %s is %q after the cut, want %q", when, name, k, cfg[name][k], v)
			}
		}
	}
	for _, name := range w.gone {
		if _, ok := have[name]; ok {
			t.Fatalf("%s: deleted topic %s is back after the cut", when, name)
		}
	}
	for tp, recs := range w.records {
		part, err := after.logs.ExistingPartition(tp.topic, tp.partition)
		if err != nil {
			t.Fatalf("%s: %s-%d: %v", when, tp.topic, tp.partition, err)
		}
		start := part.LogStartOffset()
		if may, ok := w.logStartMay[tp]; ok && start == may {
			w.logStart[tp] = may // what is below it was deleted by a request that may take effect.
			defer delete(w.logStart, tp)
		}
		if got, want := start, w.logStart[tp]; got != want {
			t.Fatalf("%s: %s-%d starts at %d after the cut, want %d", when, tp.topic, tp.partition, got, want)
		}
		batches := after.read(tp.topic, tp.partition, false)
		for off, payload := range recs {
			if off < w.logStart[tp] {
				continue
			}
			if got, ok := payloadAt(batches, off); !ok || got != payload {
				t.Fatalf("%s: %s-%d offset %d is %q (present %v) after the cut, want the acknowledged %q", when, tp.topic, tp.partition, off, got, ok, payload)
			}
		}
		visible := map[string]bool{}
		for _, s := range after.read(tp.topic, tp.partition, true) {
			if !s.control {
				visible[s.payload] = true
			}
		}
		for _, p := range w.committed[tp] {
			if !visible[p] {
				t.Fatalf("%s: %s-%d: the committed transaction's %q is not visible to read_committed after the cut", when, tp.topic, tp.partition, p)
			}
		}
		for _, p := range w.aborted[tp] {
			if visible[p] {
				t.Fatalf("%s: %s-%d: the aborted transaction's %q is visible to read_committed after the cut", when, tp.topic, tp.partition, p)
			}
		}
	}
	for group, offs := range w.offsets {
		for tp, want := range offs {
			got, ok, err := after.meta.FetchOffset(group, tp.topic, tp.partition)
			if err != nil || !ok || got != want {
				t.Fatalf("%s: group %s %s-%d committed offset %d (found %v, %v) after the cut, want %d", when, group, tp.topic, tp.partition, got, ok, err, want)
			}
		}
	}
}

func (r *rig) commitOffset(group, topic string, partition int32, offset int64) int16 {
	req := kmsg.NewPtrOffsetCommitRequest()
	req.SetVersion(2)
	req.Group, req.Generation = group, -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition, rp.Offset = partition, offset
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	return r.call(req).(*kmsg.OffsetCommitResponse).Topics[0].Partitions[0].ErrorCode
}

func (r *rig) deleteRecords(topic string, partition int32, before int64) int16 {
	req := kmsg.NewPtrDeleteRecordsRequest()
	req.SetVersion(1)
	rt := kmsg.NewDeleteRecordsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewDeleteRecordsRequestTopicPartition()
	rp.Partition, rp.Offset = partition, before
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	req.TimeoutMillis = 1000
	return r.call(req).(*kmsg.DeleteRecordsResponse).Topics[0].Partitions[0].ErrorCode
}

func (r *rig) deleteTopic(name string) int16 {
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.SetVersion(3)
	req.TopicNames, req.TimeoutMillis = []string{name}, 1000
	return r.call(req).(*kmsg.DeleteTopicsResponse).Topics[0].ErrorCode
}

func (r *rig) alterConfig(topic, key, value string) int16 {
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	req.SetVersion(0)
	res := kmsg.NewIncrementalAlterConfigsRequestResource()
	res.ResourceType, res.ResourceName = kmsg.ConfigResourceTypeTopic, topic
	c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	c.Name, c.Op, c.Value = key, kmsg.IncrementalAlterConfigOpSet, &value
	res.Configs = append(res.Configs, c)
	req.Resources = append(req.Resources, res)
	return r.call(req).(*kmsg.IncrementalAlterConfigsResponse).Resources[0].ErrorCode
}

// BAD-13. Every kind of acknowledged write, across several topics and partitions,
// with a kill -9 and restart in the middle; after each step the power is cut (a
// different mode each time, every mode at the end) and a broker started on what
// is left must have every acknowledged record at its offset, every committed
// transaction visible and no aborted one, every committed offset, every topic,
// partition count, config and log start, and no deleted topic.
func TestPowerCutKeepsEveryAcksAllRecord(t *testing.T) {
	r := newRig(t, 400) // a few batches to a segment: rolls throughout.
	w := newPromised()
	step := 0
	cut := func(when string) {
		t.Helper()
		step++
		mode := powercut.Modes[step%len(powercut.Modes)]
		w.check(t, r.cut(mode, int64(step)), fmt.Sprintf("step %d (%s, %s)", step, when, mode))
	}
	create := func(name string, n int32) {
		t.Helper()
		r.mustCreateTopic(name, n, nil)
		w.topics[name] = n
		for p := range n {
			w.records[topicPartition{name, p}] = map[int64]string{}
		}
		cut("create " + name)
	}
	var seq = map[topicPartition]int32{}
	produce := func(topic string, p int32, acks int16, idem bool) {
		t.Helper()
		tp := topicPartition{topic, p}
		payload := fmt.Sprintf("%s-%d-%d", topic, p, len(w.records[tp]))
		pid := int64(-1)
		if idem {
			pid = 900
		}
		resp := r.produce(topic, p, acks, nil, payloadBatch(pid, 0, seq[tp], 0, payload))
		if resp.ErrorCode != 0 {
			t.Fatalf("produce %s-%d: code %d", topic, p, resp.ErrorCode)
		}
		if idem {
			seq[tp]++
		}
		if acks == -1 {
			w.records[tp][resp.BaseOffset] = payload
			cut(fmt.Sprintf("acks=all to %s-%d", topic, p))
		}
	}

	create("a", 3)
	create("b", 2)
	for i := range 12 {
		produce("a", int32(i%3), []int16{-1, 1}[i%2], i%3 == 0)
		produce("b", int32(i%2), -1, i%4 == 0)
	}
	if code := r.commitOffset("g1", "a", 0, 3); code != 0 {
		t.Fatalf("commit: %d", code)
	}
	w.offsets["g1"] = map[topicPartition]int64{{"a", 0}: 3}
	cut("offset commit")
	if code := r.alterConfig("b", "retention.ms", "86400001"); code != 0 {
		t.Fatalf("alter config: %d", code)
	}
	w.configs["b"] = map[string]string{"retention.ms": "86400001"}
	cut("alter config")
	if code := r.deleteRecords("a", 0, 2); code != 0 {
		t.Fatalf("delete records: %d", code)
	}
	w.logStart[topicPartition{"a", 0}] = 2
	cut("delete records")

	// kill -9 and a restart in the middle: what follows runs in a process that
	// did not write what it found.
	r.restart()
	w.check(t, r, "after the restart, before the cut")
	cut("restart")
	for i := range 6 {
		produce("a", int32(i%3), -1, false)
		produce("b", int32(i%2), 1, false)
	}

	// A committed transaction over two topics with offsets, and an aborted one.
	x := r.beginTxn("tx-commit")
	if code := x.add("a", 1); code != 0 {
		t.Fatalf("add: %d", code)
	}
	if code := x.add("b", 0); code != 0 {
		t.Fatalf("add: %d", code)
	}
	for _, tp := range []topicPartition{{"a", 1}, {"b", 0}} {
		payload := "committed-" + tp.topic
		if p := x.send(tp.topic, tp.partition, -1, payload); p.ErrorCode != 0 {
			t.Fatalf("txn send: %d", p.ErrorCode)
		}
		w.committed[tp] = append(w.committed[tp], payload)
	}
	if code := x.addOffsets("g2"); code != 0 {
		t.Fatalf("add offsets: %d", code)
	}
	if code := x.commitOffset("g2", "a", 1, 5); code != 0 {
		t.Fatalf("txn offset commit: %d", code)
	}
	if code := x.end(true); code != 0 {
		t.Fatalf("commit: %d", code)
	}
	w.offsets["g2"] = map[topicPartition]int64{{"a", 1}: 5}
	cut("transaction committed")
	y := r.beginTxn("tx-abort")
	if code := y.add("a", 2); code != 0 {
		t.Fatalf("add: %d", code)
	}
	if p := y.send("a", 2, -1, "aborted-a"); p.ErrorCode != 0 {
		t.Fatalf("txn send: %d", p.ErrorCode)
	}
	if code := y.end(false); code != 0 {
		t.Fatalf("abort: %d", code)
	}
	w.aborted[topicPartition{"a", 2}] = []string{"aborted-a"}
	cut("transaction aborted")

	create("c", 1)
	produce("c", 0, -1, false)
	if code := r.deleteTopic("c"); code != 0 {
		t.Fatalf("delete topic: %d", code)
	}
	delete(w.topics, "c")
	delete(w.records, topicPartition{"c", 0})
	w.gone = append(w.gone, "c")
	cut("delete topic")
	create("d", 2)
	produce("d", 1, -1, true)
	if code := r.deleteRecords("b", 1, -1); code != 0 { // to the end: the partition empties.
		t.Fatalf("delete records to the end: %d", code)
	}
	b1, _ := r.logs.ExistingPartition("b", 1)
	w.logStart[topicPartition{"b", 1}] = b1.LogEndOffset()
	cut("delete records to the end")

	for _, mode := range powercut.Modes {
		w.check(t, r.cut(mode, 99), "at the end, "+string(mode))
	}
	if !slices.Contains(w.gone, "c") || len(w.records) != 7 {
		t.Fatalf("the workload did not run as written: %d partitions", len(w.records))
	}
}
