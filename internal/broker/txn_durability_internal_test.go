package broker

// What the end of a transaction promises about the disk (F2), and what it
// answers once the decision is made (F3). The rig keeps only what was fsynced.

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/powercut"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"

	"github.com/twmb/franz-go/pkg/kerr"
)

// rigTxn is one transactional producer on a rig.
type rigTxn struct {
	r     *rig
	id    string
	pid   int64
	epoch int16
	seq   map[topicPartition]int32
}

// beginTxn is InitProducerId for a transactional id; it fails the test on an error.
func (r *rig) beginTxn(id string) *rigTxn {
	r.t.Helper()
	x := &rigTxn{r: r, id: id, seq: map[topicPartition]int32{}}
	if code := x.init(); code != 0 {
		r.t.Fatalf("InitProducerId(%s): %v", id, kerr.ErrorForCode(code))
	}
	return x
}

func (x *rigTxn) init() int16 {
	req := kmsg.NewPtrInitProducerIDRequest()
	req.SetVersion(1)
	req.TransactionalID, req.TransactionTimeoutMillis = &x.id, 60000
	resp := x.r.call(req).(*kmsg.InitProducerIDResponse)
	if resp.ErrorCode == 0 {
		x.pid, x.epoch = resp.ProducerID, resp.ProducerEpoch
		x.seq = map[topicPartition]int32{}
	}
	return resp.ErrorCode
}

func (x *rigTxn) add(topic string, partitions ...int32) int16 {
	req := kmsg.NewPtrAddPartitionsToTxnRequest()
	req.SetVersion(1)
	req.TransactionalID, req.ProducerID, req.ProducerEpoch = x.id, x.pid, x.epoch
	rt := kmsg.NewAddPartitionsToTxnRequestTopic()
	rt.Topic, rt.Partitions = topic, partitions
	req.Topics = append(req.Topics, rt)
	return x.r.call(req).(*kmsg.AddPartitionsToTxnResponse).Topics[0].Partitions[0].ErrorCode
}

// send produces one transactional record and returns the answer.
func (x *rigTxn) send(topic string, partition int32, acks int16, payload string) kmsg.ProduceResponseTopicPartition {
	tp := topicPartition{topic, partition}
	p := x.r.produce(topic, partition, acks, &x.id, payloadBatch(x.pid, x.epoch, x.seq[tp], 0x10, payload))
	if p.ErrorCode == 0 {
		x.seq[tp]++
	}
	return p
}

func (x *rigTxn) end(commit bool) int16 {
	req := kmsg.NewPtrEndTxnRequest()
	req.SetVersion(1)
	req.TransactionalID, req.ProducerID, req.ProducerEpoch, req.Commit = x.id, x.pid, x.epoch, commit
	return x.r.call(req).(*kmsg.EndTxnResponse).ErrorCode
}

func (x *rigTxn) addOffsets(group string) int16 {
	req := kmsg.NewPtrAddOffsetsToTxnRequest()
	req.SetVersion(1)
	req.TransactionalID, req.ProducerID, req.ProducerEpoch, req.Group = x.id, x.pid, x.epoch, group
	return x.r.call(req).(*kmsg.AddOffsetsToTxnResponse).ErrorCode
}

func (x *rigTxn) commitOffset(group, topic string, partition int32, offset int64) int16 {
	req := kmsg.NewPtrTxnOffsetCommitRequest()
	req.SetVersion(2)
	req.TransactionalID, req.Group, req.ProducerID, req.ProducerEpoch, req.Generation = x.id, group, x.pid, x.epoch, -1
	rt := kmsg.NewTxnOffsetCommitRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
	rp.Partition, rp.Offset = partition, offset
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	return x.r.call(req).(*kmsg.TxnOffsetCommitResponse).Topics[0].Partitions[0].ErrorCode
}

// txnState is a transactional id's state as the store has it.
func (r *rig) txnState(id string) string {
	r.t.Helper()
	all, err := r.meta.ListTxns()
	if err != nil {
		r.t.Fatal(err)
	}
	for _, rec := range all {
		if rec.ID == id {
			return rec.State
		}
	}
	return ""
}

func (r *rig) hasPayload(topic string, partition int32, committed bool, payload string) bool {
	for _, s := range r.read(topic, partition, committed) {
		if !s.control && s.payload == payload {
			return true
		}
	}
	return false
}

func name(code int16) string {
	if code == 0 {
		return "NONE"
	}
	return kerr.ErrorForCode(code).(*kerr.Error).Message
}

// F2. EndTxn(commit) answered success means: every record of the transaction, in
// every partition, its markers, and the offsets it carried are on disk. Cut the
// power the moment it is answered, and a read-committed consumer of what is left
// sees all of it.
func TestACommittedTransactionIsWhollyOnDiskWhenEndTxnIsAnswered(t *testing.T) {
	r := newRig(t, 400)
	r.mustCreateTopic("a", 3, nil)
	r.mustCreateTopic("b", 1, nil)
	x := r.beginTxn("tx")
	if c := x.add("a", 1, 2); c != 0 {
		t.Fatalf("add a: %s", name(c))
	}
	if c := x.add("b", 0); c != 0 {
		t.Fatalf("add b: %s", name(c))
	}
	type sent struct {
		topic   string
		part    int32
		payload string
	}
	var all []sent
	for i, s := range []sent{{"a", 1, "a1-first"}, {"a", 2, "a2-first"}, {"b", 0, "b0-first"}, {"a", 1, "a1-second"}, {"b", 0, "b0-second"}} {
		acks := int16(-1)
		if i%2 == 1 {
			acks = 1 // a transaction's records are as durable as its commit, however they were sent.
		}
		if p := x.send(s.topic, s.part, acks, s.payload); p.ErrorCode != 0 {
			t.Fatalf("send %v: %s", s, name(p.ErrorCode))
		}
		all = append(all, s)
	}
	if c := x.addOffsets("g"); c != 0 {
		t.Fatalf("add offsets: %s", name(c))
	}
	if c := x.commitOffset("g", "a", 0, 41); c != 0 {
		t.Fatalf("txn offset commit: %s", name(c))
	}
	if c := x.end(true); c != 0 {
		t.Fatalf("EndTxn(commit): %s", name(c))
	}

	for _, mode := range powercut.Modes {
		after := r.cut(mode, 3)
		if got := after.txnState("tx"); got != txnCompleteCommit {
			t.Errorf("%s: the transaction's stored state after the cut is %q", mode, got)
		}
		for _, s := range all {
			if !after.hasPayload(s.topic, s.part, true, s.payload) {
				t.Errorf("COMMITTED AND LOST (power cut, %s): EndTxn(commit) was answered success; a read-committed consumer of %s-%d does not get %q (it gets %v)",
					mode, s.topic, s.part, s.payload, after.read(s.topic, s.part, true))
			}
		}
		if off, ok, err := after.meta.FetchOffset("g", "a", 0); err != nil || !ok || off != 41 {
			t.Errorf("%s: the offset committed with the transaction is %d (present=%v, err=%v), want 41", mode, off, ok, err)
		}
		after.shutdown()
	}
}

// F2, the other half: an aborted transaction, and one that was never ended, are
// not visible to a read-committed consumer after a power cut either. That rests
// on the abort marker being on disk and on the partition knowing, after the cut,
// that it holds transactions at all.
func TestAnAbortedOrUnfinishedTransactionIsNotVisibleAfterAPowerCut(t *testing.T) {
	r := newRig(t, 1<<20)
	r.mustCreateTopic("t", 2, nil)
	aborted := r.beginTxn("tx-aborted")
	aborted.add("t", 0)
	if p := aborted.send("t", 0, -1, "aborted"); p.ErrorCode != 0 {
		t.Fatalf("send: %s", name(p.ErrorCode))
	}
	if c := aborted.end(false); c != 0 {
		t.Fatalf("EndTxn(abort): %s", name(c))
	}
	// Committed after it, on the same partition: visible, and proof that the
	// consumer got past the aborted records rather than stopping before them.
	committed := r.beginTxn("tx-committed")
	committed.add("t", 0)
	committed.send("t", 0, -1, "committed")
	if c := committed.end(true); c != 0 {
		t.Fatalf("EndTxn(commit): %s", name(c))
	}
	open := r.beginTxn("tx-open")
	open.add("t", 1)
	if p := open.send("t", 1, -1, "never ended"); p.ErrorCode != 0 {
		t.Fatalf("send: %s", name(p.ErrorCode))
	}

	for _, mode := range powercut.Modes {
		after := r.cut(mode, 5)
		if after.hasPayload("t", 0, true, "aborted") {
			t.Errorf("%s: after a power cut a read-committed consumer is given the record of a transaction whose abort was acknowledged", mode)
		}
		if !after.hasPayload("t", 0, true, "committed") {
			t.Errorf("%s: the transaction committed after the aborted one is not visible: %v", mode, after.read("t", 0, true))
		}
		if after.hasPayload("t", 1, true, "never ended") {
			t.Errorf("%s: a transaction that was never ended is visible to a read-committed consumer after a power cut", mode)
		}
		if !after.hasPayload("t", 1, false, "never ended") {
			t.Errorf("%s: the acks=all record of the open transaction is gone", mode)
		}
		after.shutdown()
	}
}

func oneRecordTxn(t *testing.T, r *rig) *rigTxn {
	t.Helper()
	r.mustCreateTopic("t", 1, nil)
	x := r.beginTxn("tx")
	if c := x.add("t", 0); c != 0 {
		t.Fatalf("add: %s", name(c))
	}
	if p := x.send("t", 0, -1, "in the transaction"); p.ErrorCode != 0 {
		t.Fatalf("send: %s", name(p.ErrorCode))
	}
	return x
}

func mustBeCommittedOnDisk(t *testing.T, r *rig) {
	t.Helper()
	for _, mode := range powercut.Modes {
		after := r.cut(mode, 9)
		if !after.hasPayload("t", 0, true, "in the transaction") {
			t.Fatalf("COMMIT ACKNOWLEDGED, MARKER LOST (power cut, %s): stored state %q; the partition holds %v and a read-committed consumer gets %v",
				mode, after.txnState("tx"), after.read("t", 0, false), after.read("t", 0, true))
		}
		after.shutdown()
	}
}

// F2. The marker is appended and its flush is refused. The producer is told to
// ask again, and asks. The marker is in the log already, so the retry writes
// nothing, which is no reason to believe the marker is on disk.
func TestEndTxnRetryAfterAMarkerThatWasNotFlushedFlushesIt(t *testing.T) {
	r := newRig(t, 1<<20)
	x := oneRecordTxn(t, r)

	r.failSync.Store(true)
	if c := x.end(true); c != kerr.ConcurrentTransactions.Code {
		t.Fatalf("EndTxn with the marker's flush failing: %s, want CONCURRENT_TRANSACTIONS (the decision is stored; ask again)", name(c))
	}
	if got := r.txnState("tx"); got != txnPrepareCommit {
		t.Fatalf("stored state %q, want PrepareCommit", got)
	}
	r.failSync.Store(false)
	before := r.pc.FsyncsOf(".log")
	if c := x.end(true); c != 0 {
		t.Fatalf("EndTxn retry: %s", name(c))
	}
	if got := r.pc.FsyncsOf(".log") - before; got != 1 {
		t.Errorf("the retry was answered success after %d fsync(s) of the log, want 1", got)
	}
	if got := r.txnState("tx"); got != txnCompleteCommit {
		t.Fatalf("stored state %q", got)
	}
	mustBeCommittedOnDisk(t, r)
}

// F2 and BAD-3. The fsync of the marker really fails, which takes the partition
// offline: nothing of it is acknowledged again before a restart, and the end of a
// transaction is an acknowledgement. The retry is not answered success; the
// restart carries the decision out, with the marker flushed.
func TestEndTxnIsNotAcknowledgedOnAPartitionWhoseMarkerFsyncFailed(t *testing.T) {
	r := newRig(t, 1<<20)
	x := oneRecordTxn(t, r)

	failed := false
	restore := log.SetFsyncForTest(func(f *os.File) error {
		if !failed && strings.HasSuffix(f.Name(), ".log") {
			failed = true
			return syscall.EIO // the fsync of the marker fails, once.
		}
		return f.Sync()
	})
	defer restore()

	if c := x.end(true); c != kerr.ConcurrentTransactions.Code {
		t.Fatalf("EndTxn with the marker's fsync failing: %s", name(c))
	}
	fsyncs := r.pc.FsyncsOf(".log")
	if c := x.end(true); c != kerr.ConcurrentTransactions.Code {
		t.Fatalf("EndTxn retry on a partition that is offline: %s (stored state %q), want CONCURRENT_TRANSACTIONS: the commit was acknowledged on a partition whose flush failed", name(c), r.txnState("tx"))
	}
	if got := r.pc.FsyncsOf(".log") - fsyncs; got != 0 {
		t.Errorf("a failed fsync was retried %d time(s): the kernel may report success for pages it dropped", got)
	}
	if got := r.txnState("tx"); got != txnPrepareCommit {
		t.Fatalf("stored state %q, want PrepareCommit", got)
	}
	if p := r.produce("t", 0, -1, nil, payloadBatch(-1, -1, -1, 0, "after")); p.ErrorCode != kerr.KafkaStorageError.Code {
		t.Fatalf("produce to the offline partition: %s", name(p.ErrorCode))
	}

	r.restart() // recovery finishes what was decided.
	if got := r.txnState("tx"); got != txnCompleteCommit {
		t.Fatalf("after a restart the stored state is %q, want CompleteCommit", got)
	}
	mustBeCommittedOnDisk(t, r)
	if c := x.end(true); c != 0 {
		t.Fatalf("EndTxn after the restart: %s", name(c))
	}
}

// F2. The broker is killed between appending a commit marker and flushing it. The
// decision is in the store, so the restart carries it out; "the marker is already
// in the log" must not be taken for "the marker is on disk".
func TestARestartFinishesACommitOnlyWithItsMarkerOnDisk(t *testing.T) {
	r := newRig(t, 1<<20)
	x := oneRecordTxn(t, r)
	r.failSync.Store(true) // the process dies before the flush: modelled by a flush that does not happen.
	if c := x.end(true); c != kerr.ConcurrentTransactions.Code {
		t.Fatalf("setup: EndTxn = %s", name(c))
	}
	r.failSync.Store(false)
	before := r.pc.FsyncsOf(".log")
	r.restart()
	if got := r.txnState("tx"); got != txnCompleteCommit {
		t.Fatalf("after the restart the stored state is %q", got)
	}
	if got := r.pc.FsyncsOf(".log") - before; got != 1 {
		t.Errorf("the restart recorded the transaction complete after %d fsync(s) of the log, want 1", got)
	}
	mustBeCommittedOnDisk(t, r)
}

// A decision that could not be carried out is carried out when it can be, whether
// or not the producer asks again: until then the partition's last stable offset
// stands still and every read-committed consumer of it waits.
func TestADecidedTransactionIsFinishedWithoutBeingAskedAgain(t *testing.T) {
	r := newRig(t, 1<<20)
	x := oneRecordTxn(t, r)
	r.failSync.Store(true)
	if c := x.end(true); c != kerr.ConcurrentTransactions.Code {
		t.Fatalf("setup: EndTxn = %s", name(c))
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { r.b.RunTransactions(ctx); close(stopped) }()
	defer func() { cancel(); <-stopped }() // before the rig closes the stores under it.
	time.Sleep(2 * txnCheckEvery)
	if got := r.txnState("tx"); got != txnPrepareCommit {
		t.Fatalf("with the disk still failing the stored state is %q", got)
	}
	r.failSync.Store(false)
	deadline := time.Now().Add(10 * time.Second)
	for r.txnState("tx") != txnCompleteCommit {
		if time.Now().After(deadline) {
			t.Fatalf("the disk mended and the transaction is still %q", r.txnState("tx"))
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if !r.hasPayload("t", 0, true, "in the transaction") {
		t.Fatal("finished, and not visible to a read-committed consumer")
	}
}

// failingStore is a metadata store whose writes of transaction state (and of
// offsets) fail on demand, as a full disk makes them.
type failingStore struct {
	meta.Store
	fail *bool
}

var errStoreFull = &os.PathError{Op: "write", Path: "meta.db", Err: syscall.ENOSPC}

func (s failingStore) PutTxn(rec meta.TxnRecord) error {
	if *s.fail {
		return errStoreFull
	}
	return s.Store.PutTxn(rec)
}

func (s failingStore) CommitOffsets(group string, offsets []meta.OffsetCommit) error {
	if *s.fail {
		return errStoreFull
	}
	return s.Store.CommitOffsets(group, offsets)
}

func (s failingStore) ReserveProducerIDs(n int64) (int64, error) {
	if *s.fail {
		return 0, errStoreFull
	}
	return s.Store.ReserveProducerIDs(n)
}

// failStore makes the rig's broker use a store that fails while *fail is set.
func (r *rig) failStore() *bool {
	fail := new(bool)
	r.b.meta = failingStore{Store: r.b.meta, fail: fail}
	return fail
}

// F3. EndTxn(commit) cannot store its decision and says so. The application gives
// up on the commit and aborts. Nothing was decided, so the abort is the decision:
// it used to be answered success and the transaction committed.
func TestAnAbortAfterACommitThatWasNeverDecidedAborts(t *testing.T) {
	r := newRig(t, 1<<20)
	x := oneRecordTxn(t, r)
	fail := r.failStore()

	*fail = true
	if c := x.end(true); c != kerr.NotCoordinator.Code {
		t.Fatalf("EndTxn(commit) with the store failing: %s, want NOT_COORDINATOR", name(c))
	}
	if got := r.b.txns.get("tx").rec.State; got != txnOngoing {
		t.Fatalf("the decision could not be stored, and the coordinator's own state is %q: it runs ahead of the store (%q)", got, r.txnState("tx"))
	}
	*fail = false
	if c := x.end(false); c != 0 {
		t.Fatalf("EndTxn(abort): %s", name(c))
	}
	if got := r.txnState("tx"); got != txnCompleteAbort {
		t.Fatalf("the producer asked to ABORT and was answered success; the stored state is %q", got)
	}
	if r.hasPayload("t", 0, true, "in the transaction") {
		t.Fatal("the producer asked to ABORT and was answered success; a read-committed consumer is given the record")
	}
}

// F3. Once a decision is stored it stands. What an EndTxn that names the other
// one is answered is what Apache Kafka answers (recorded: endtxn-decisions,
// endtxn-while-preparing): CONCURRENT_TRANSACTIONS while the decision is still
// being carried out, INVALID_TXN_STATE once it has been; and a repeat of the same
// decision is CONCURRENT_TRANSACTIONS, then success.
func TestEndTxnAnswersTheOppositeDecisionAsKafkaDoes(t *testing.T) {
	for _, commit := range []bool{true, false} {
		decision := map[bool]string{true: "commit", false: "abort"}[commit]
		t.Run(decision, func(t *testing.T) {
			r := newRig(t, 1<<20)
			x := oneRecordTxn(t, r)
			prepare := map[bool]string{true: txnPrepareCommit, false: txnPrepareAbort}[commit]
			complete := map[bool]string{true: txnCompleteCommit, false: txnCompleteAbort}[commit]

			// The decision is stored and cannot be carried out.
			r.failSync.Store(true)
			if c := x.end(commit); c != kerr.ConcurrentTransactions.Code {
				t.Fatalf("EndTxn(%s) that cannot be carried out: %s", decision, name(c))
			}
			if got := r.txnState("tx"); got != prepare {
				t.Fatalf("stored state %q, want %s", got, prepare)
			}
			if c := x.end(!commit); c != kerr.ConcurrentTransactions.Code {
				t.Errorf("the other decision while %s: %s, want CONCURRENT_TRANSACTIONS", prepare, name(c))
			}
			if c := x.end(commit); c != kerr.ConcurrentTransactions.Code {
				t.Errorf("the same decision while %s: %s, want CONCURRENT_TRANSACTIONS", prepare, name(c))
			}
			if c := x.add("t", 0); c != kerr.ConcurrentTransactions.Code {
				t.Errorf("AddPartitionsToTxn while %s: %s, want CONCURRENT_TRANSACTIONS", prepare, name(c))
			}
			if got := r.txnState("tx"); got != prepare {
				t.Fatalf("stored state %q after the refusals, want %s still", got, prepare)
			}

			// The disk mends. The other decision arrives first: the stored one is
			// carried out, and the request is told it lost.
			r.failSync.Store(false)
			if c := x.end(!commit); c != kerr.InvalidTxnState.Code {
				t.Errorf("the other decision once the stored one can be carried out: %s, want INVALID_TXN_STATE", name(c))
			}
			if got := r.txnState("tx"); got != complete {
				t.Fatalf("stored state %q, want %s", got, complete)
			}
			if c := x.end(commit); c != 0 {
				t.Errorf("the same decision once %s: %s, want success", complete, name(c))
			}
			if c := x.end(!commit); c != kerr.InvalidTxnState.Code {
				t.Errorf("the other decision once %s: %s, want INVALID_TXN_STATE", complete, name(c))
			}
			if got := r.hasPayload("t", 0, true, "in the transaction"); got != commit {
				t.Errorf("the record is visible to a read-committed consumer: %v, want %v", got, commit)
			}
		})
	}
	// With nothing open, either decision is INVALID_TXN_STATE.
	r := newRig(t, 1<<20)
	x := r.beginTxn("tx")
	for _, commit := range []bool{true, false} {
		if c := x.end(commit); c != kerr.InvalidTxnState.Code {
			t.Errorf("EndTxn(commit=%v) with no transaction open: %s, want INVALID_TXN_STATE", commit, name(c))
		}
	}
}

// F3 and BAD-27. On every path of the coordinator that changes a transaction, a
// store write that fails leaves the coordinator's own record exactly as the store
// has it; and the same request then succeeds once the store takes writes again.
func TestACoordinatorWriteThatFailsLeavesItsStateAsTheStoreHasIt(t *testing.T) {
	type env struct {
		r *rig
		x *rigTxn
	}
	ops := []struct {
		name  string
		setup func(e env)               // brings the transaction to where the write is tried.
		do    func(e env) int16         // the write, failing and then again healed.
		then  func(t *testing.T, e env) // what must be true once it has succeeded.
	}{
		{"InitProducerId for a new id", nil,
			func(e env) int16 { y := &rigTxn{r: e.r, id: "tx-new"}; return y.init() },
			func(t *testing.T, e env) {
				if got := e.r.txnState("tx-new"); got != txnEmpty {
					t.Errorf("state %q", got)
				}
			}},
		{"InitProducerId again (the epoch is raised)", nil,
			func(e env) int16 { return e.x.init() }, nil},
		{"InitProducerId over an open transaction (the old producer is fenced)",
			func(e env) { e.x.add("t", 0); e.x.send("t", 0, -1, "open") },
			func(e env) int16 { return e.x.init() },
			func(t *testing.T, e env) {
				if e.r.hasPayload("t", 0, true, "open") {
					t.Error("the fenced producer's record is visible to a read-committed consumer")
				}
			}},
		{"AddPartitionsToTxn", nil, func(e env) int16 { return e.x.add("t", 0) }, nil},
		{"AddPartitionsToTxn, a second partition",
			func(e env) { e.x.add("t", 0) },
			func(e env) int16 { return e.x.add("t", 1) }, nil},
		{"AddOffsetsToTxn", nil, func(e env) int16 { return e.x.addOffsets("g") }, nil},
		{"TxnOffsetCommit",
			func(e env) { e.x.addOffsets("g") },
			func(e env) int16 { return e.x.commitOffset("g", "t", 0, 7) }, nil},
		{"EndTxn(commit)",
			func(e env) { e.x.add("t", 0); e.x.send("t", 0, -1, "v") },
			func(e env) int16 { return e.x.end(true) },
			func(t *testing.T, e env) {
				if !e.r.hasPayload("t", 0, true, "v") {
					t.Error("committed, and not visible")
				}
			}},
		{"EndTxn(abort)",
			func(e env) { e.x.add("t", 0); e.x.send("t", 0, -1, "v") },
			func(e env) int16 { return e.x.end(false) },
			func(t *testing.T, e env) {
				if e.r.hasPayload("t", 0, true, "v") {
					t.Error("aborted, and visible")
				}
			}},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			r := newRig(t, 1<<20)
			r.mustCreateTopic("t", 2, nil)
			e := env{r: r, x: r.beginTxn("tx")}
			if op.setup != nil {
				op.setup(e)
			}
			fail := r.failStore()
			agree := func(when string) {
				t.Helper()
				stored := map[string]meta.TxnRecord{}
				all, err := r.meta.ListTxns()
				if err != nil {
					t.Fatal(err)
				}
				for _, rec := range all {
					stored[rec.ID] = rec
				}
				for _, entry := range r.b.txns.all() {
					entry.mu.RLock()
					mem := entry.rec
					entry.mu.RUnlock()
					if mem.State == "" {
						continue // an id nobody owns yet: nothing is stored for it, and nothing is claimed.
					}
					if !reflect.DeepEqual(mem, stored[mem.ID]) {
						t.Fatalf("%s the coordinator's record and the store disagree:\n memory %+v\n store  %+v", when, mem, stored[mem.ID])
					}
				}
			}
			agree("before the write")
			pid, epoch := e.x.pid, e.x.epoch
			*fail = true
			if c := op.do(e); c != kerr.NotCoordinator.Code && c != kerr.ConcurrentTransactions.Code {
				t.Fatalf("with the store failing: %s, want NOT_COORDINATOR (or CONCURRENT_TRANSACTIONS once a decision is stored)", name(c))
			}
			agree("after a write that failed")
			if e.x.pid != pid || e.x.epoch != epoch {
				t.Fatalf("a failed request handed out producer %d epoch %d", e.x.pid, e.x.epoch)
			}
			*fail = false
			if c := op.do(e); c != 0 {
				t.Fatalf("once the store takes writes again: %s", name(c))
			}
			agree("after the write succeeded")
			if op.then != nil {
				op.then(t, e)
			}
		})
	}
}

// BAD-27. A transaction that times out is aborted by raising its producer's epoch
// first. If that cannot be stored, nothing has happened: the producer is not
// fenced by an epoch only the coordinator's memory has.
func TestFencingWithAFailingMetaWriteLeavesNoPhantomEpoch(t *testing.T) {
	r := newRig(t, 1<<20)
	x := oneRecordTxn(t, r)
	fail := r.failStore()
	e := r.b.txns.get("tx")

	*fail = true
	e.mu.Lock()
	code := r.b.abortFencing(e)
	e.mu.Unlock()
	if code != kerr.NotCoordinator.Code {
		t.Fatalf("abortFencing with the store failing: %s", name(code))
	}
	if e.rec.Epoch != x.epoch || e.rec.State != txnOngoing {
		t.Fatalf("after the failure the coordinator has epoch %d state %q; the store has epoch %d state %q", e.rec.Epoch, e.rec.State, x.epoch, r.txnState("tx"))
	}
	*fail = false
	// The old producer is not fenced: its next record is accepted.
	if p := x.send("t", 0, -1, "still the owner"); p.ErrorCode != 0 {
		t.Fatalf("the producer was fenced by an epoch that was never stored: %s", name(p.ErrorCode))
	}
	e.mu.Lock()
	code = r.b.abortFencing(e)
	e.mu.Unlock()
	if code != 0 {
		t.Fatalf("abortFencing once the store takes writes: %s", name(code))
	}
	if p := x.send("t", 0, -1, "fenced now"); p.ErrorCode == 0 {
		t.Fatal("the producer of an aborted transaction was not fenced")
	}
	if !errors.Is(errStoreFull, syscall.ENOSPC) {
		t.Fatal("the test's own error is not the one it says")
	}
}
