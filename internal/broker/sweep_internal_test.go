package broker

// The sweep: every operation the log makes on the file system during a workload
// (each write, truncation, creation, rename, removal, fsync and directory fsync)
// is made to fail, once, in a run of its own; and, in another run, the power is
// cut right after it. Two invariants after each: nothing acknowledged is lost,
// and the log is readable and self-consistent after a restart.

import (
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/powercut"
)

// sweepWorkload drives the rig through produces (plain, idempotent, rolling
// segments, two topics), an offset commit, a committed and an aborted
// transaction, DeleteRecords and a compaction-free roll, recording in w only
// what was acknowledged while record() says so.
func sweepWorkload(t *testing.T, r *rig, w *promised, record func() bool) {
	t.Helper()
	seq := map[topicPartition]int32{}
	produce := func(topic string, p int32, idem bool) {
		tp := topicPartition{topic, p}
		payload := fmt.Sprintf("%s-%d-%d-%d", topic, p, len(w.records[tp]), seq[tp])
		pid := int64(-1)
		if idem {
			pid = 77
		}
		resp := r.produce(topic, p, -1, nil, payloadBatch(pid, 0, seq[tp], 0, payload))
		if resp.ErrorCode == 0 {
			if idem {
				seq[tp]++
			}
			if record() {
				w.records[tp][resp.BaseOffset] = payload
			}
		}
	}
	for i := range 8 {
		produce("s", int32(i%2), i%2 == 0)
		produce("u", 0, false)
	}
	if r.commitOffset("g", "s", 0, 2) == 0 && record() {
		w.offsets["g"] = map[topicPartition]int64{{"s", 0}: 2}
	}
	x := r.beginTxnOrNil("tx-sweep-commit")
	if x != nil && x.add("s", 1) == 0 && x.add("u", 0) == 0 {
		ok := x.send("s", 1, -1, "sweep-committed-s").ErrorCode == 0
		ok = x.send("u", 0, -1, "sweep-committed-u").ErrorCode == 0 && ok
		if x.end(ok) == 0 && record() && ok {
			w.committed[topicPartition{"s", 1}] = append(w.committed[topicPartition{"s", 1}], "sweep-committed-s")
			w.committed[topicPartition{"u", 0}] = append(w.committed[topicPartition{"u", 0}], "sweep-committed-u")
		}
	}
	y := r.beginTxnOrNil("tx-sweep-abort")
	if y != nil && y.add("s", 0) == 0 && y.send("s", 0, -1, "sweep-aborted").ErrorCode == 0 {
		if y.end(false) == 0 && record() {
			w.aborted[topicPartition{"s", 0}] = []string{"sweep-aborted"}
		}
	}
	if r.deleteRecords("u", 0, 3) == 0 && record() {
		w.logStart[topicPartition{"u", 0}] = 3
	} else {
		w.logStartMay[topicPartition{"u", 0}] = 3
	}
	for i := range 4 {
		produce("s", int32(i%2), false)
	}
}

// beginTxnOrNil is beginTxn for a run in which InitProducerId may fail.
func (r *rig) beginTxnOrNil(id string) *rigTxn {
	x := &rigTxn{r: r, id: id, seq: map[topicPartition]int32{}}
	if x.init() != 0 {
		return nil
	}
	return x
}

func sweepSetup(t *testing.T) (*rig, *promised) {
	t.Helper()
	r := newRig(t, 300)
	w := newPromised()
	r.mustCreateTopic("s", 2, nil)
	r.mustCreateTopic("u", 1, nil)
	w.topics["s"], w.topics["u"] = 2, 1
	for _, tp := range []topicPartition{{"s", 0}, {"s", 1}, {"u", 0}} {
		w.records[tp] = map[int64]string{}
	}
	return r, w
}

func TestEveryFileOperationFailingOrCutLosesNothingAcknowledged(t *testing.T) {
	if testing.Short() {
		t.Skip("one broker per operation")
	}
	// How many operations the workload makes.
	r, w := sweepSetup(t)
	before := r.pc.Ops()
	sweepWorkload(t, r, w, func() bool { return true })
	total := r.pc.Ops() - before
	r.shutdown()
	if total < 50 {
		t.Fatalf("the workload made only %d file operations", total)
	}
	eio := &os.PathError{Op: "sweep", Path: "x", Err: syscall.EIO}
	for k := 1; k <= total; k++ {
		t.Run(fmt.Sprintf("fail-%03d", k), func(t *testing.T) {
			r, w := sweepSetup(t)
			r.pc.FailAt(k, eio)
			sweepWorkload(t, r, w, func() bool { return true })
			failed := r.pc.Failed()
			r.pc.FailAt(0, nil)
			// The live broker still serves what it acknowledged, and so does one
			// started after a kill -9 over the same directory.
			w.check(t, r, "live, after "+failed+" failed")
			r.restart()
			w.check(t, r, "after "+failed+" failed and a restart")
		})
		t.Run(fmt.Sprintf("cut-%03d", k), func(t *testing.T) {
			r, w := sweepSetup(t)
			r.pc.CutAfter(k)
			sweepWorkload(t, r, w, func() bool { return r.pc.Kept() == nil })
			kept := r.pc.Kept()
			if kept == nil {
				t.Skipf("operation %d did not complete", k)
			}
			mode := powercut.Modes[k%len(powercut.Modes)]
			w.check(t, r.cutOf(kept, mode, int64(k)), fmt.Sprintf("power cut after operation %d (%s)", k, mode))
		})
	}
}
