package broker_test

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// rawTxBatch is a one-record batch from a transactional producer, as a client writes it.
func rawTxBatch(pid int64, epoch int16, seq int32, value string) []byte {
	var put [binary.MaxVarintLen64]byte
	var rec []byte
	rec = append(rec, 0)
	rec = append(rec, put[:binary.PutVarint(put[:], 0)]...)  // timestamp delta.
	rec = append(rec, put[:binary.PutVarint(put[:], 0)]...)  // offset delta.
	rec = append(rec, put[:binary.PutVarint(put[:], -1)]...) // null key.
	rec = append(rec, put[:binary.PutVarint(put[:], int64(len(value)))]...)
	rec = append(rec, value...)
	rec = append(rec, put[:binary.PutVarint(put[:], 0)]...) // no headers.
	body := append(append([]byte(nil), put[:binary.PutVarint(put[:], int64(len(rec)))]...), rec...)

	b := make([]byte, 61+len(body))
	binary.BigEndian.PutUint32(b[8:], uint32(len(b)-12))
	b[16] = 2
	binary.BigEndian.PutUint16(b[21:], 0x10) // transactional.
	binary.BigEndian.PutUint64(b[27:], uint64(time.Now().UnixMilli()))
	binary.BigEndian.PutUint64(b[35:], uint64(time.Now().UnixMilli()))
	binary.BigEndian.PutUint64(b[43:], uint64(pid))
	binary.BigEndian.PutUint16(b[51:], uint16(epoch))
	binary.BigEndian.PutUint32(b[53:], uint32(seq))
	binary.BigEndian.PutUint32(b[57:], 1)
	copy(b[61:], body)
	binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], crc32.MakeTable(crc32.Castagnoli)))
	return b
}

type txnConn struct {
	t     *testing.T
	c     *rawConn
	id    string
	pid   int64
	epoch int16
}

func (x *txnConn) init(timeoutMs int32) int16 {
	req := kmsg.NewPtrInitProducerIDRequest()
	req.TransactionalID, req.TransactionTimeoutMillis = &x.id, timeoutMs
	resp := ask(x.t, x.c, req, 4, kmsg.NewPtrInitProducerIDResponse())
	x.pid, x.epoch = resp.ProducerID, resp.ProducerEpoch
	return resp.ErrorCode
}

func (x *txnConn) add(topic string, parts ...int32) []int16 {
	req := kmsg.NewPtrAddPartitionsToTxnRequest()
	req.TransactionalID, req.ProducerID, req.ProducerEpoch = x.id, x.pid, x.epoch
	rt := kmsg.NewAddPartitionsToTxnRequestTopic()
	rt.Topic, rt.Partitions = topic, parts
	req.Topics = append(req.Topics, rt)
	resp := ask(x.t, x.c, req, 3, kmsg.NewPtrAddPartitionsToTxnResponse())
	var codes []int16
	for _, p := range resp.Topics[0].Partitions {
		codes = append(codes, p.ErrorCode)
	}
	return codes
}

func (x *txnConn) produce(topic string, part int32, epoch int16, seq int32, value string) (code int16, offset int64) {
	req := kmsg.NewPtrProduceRequest()
	req.TransactionID, req.Acks, req.TimeoutMillis = &x.id, 1, 1000
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition, rp.Records = part, rawTxBatch(x.pid, epoch, seq, value)
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	r := ask(x.t, x.c, req, 9, kmsg.NewPtrProduceResponse()).Topics[0].Partitions[0]
	return r.ErrorCode, r.BaseOffset
}

func (x *txnConn) end(pid int64, epoch int16, commit bool) int16 {
	req := kmsg.NewPtrEndTxnRequest()
	req.TransactionalID, req.ProducerID, req.ProducerEpoch, req.Commit = x.id, pid, epoch, commit
	return ask(x.t, x.c, req, 3, kmsg.NewPtrEndTxnResponse()).ErrorCode
}

func (x *txnConn) state() string {
	req := kmsg.NewPtrDescribeTransactionsRequest()
	req.TransactionalIDs = []string{x.id}
	r := ask(x.t, x.c, req, 0, kmsg.NewPtrDescribeTransactionsResponse()).TransactionStates[0]
	if r.ErrorCode != 0 {
		return kerr.ErrorForCode(r.ErrorCode).Error()
	}
	return r.State
}

func code(c int16) string {
	if c == 0 {
		return "OK"
	}
	return kerr.ErrorForCode(c).Error()
}

func wantCode(t *testing.T, what string, got int16, want *kerr.Error) {
	t.Helper()
	if want == nil && got != 0 || want != nil && got != want.Code {
		w := "OK"
		if want != nil {
			w = want.Error()
		}
		t.Errorf("%s: %s, want %s", what, code(got), w)
	}
}

// The coordinator's rules, one request at a time: who owns an id, what may be added
// when, what a produce needs, and how ending a transaction behaves when repeated.
func TestTransactionCoordinatorRules(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	mustCreateTopic(t, ctx, admin, "r", 2)
	x := &txnConn{t: t, c: openRaw(t, addr), id: "rules"}

	wantCode(t, "a timeout above the maximum", x.init(16*60*1000), kerr.InvalidTransactionTimeout)
	wantCode(t, "a timeout of zero", x.init(0), kerr.InvalidTransactionTimeout)
	wantCode(t, "InitProducerId", x.init(30000), nil)
	if x.pid < 0 || x.epoch != 0 {
		t.Fatalf("first init gave producer %d epoch %d, want a producer id and epoch 0", x.pid, x.epoch)
	}

	// Producing before the partition is added is refused: the record would be in the log
	// with no marker ever coming for it.
	if c, _ := x.produce("r", 0, x.epoch, 0, "early"); c != kerr.InvalidTxnState.Code {
		t.Errorf("a transactional produce before AddPartitionsToTxn: %s, want INVALID_TXN_STATE", code(c))
	}
	// One bad partition refuses the lot and adds nothing.
	got := x.add("r", 0, 9)
	if got[0] != kerr.OperationNotAttempted.Code || got[1] != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("adding a good and an unknown partition: %s, %s", code(got[0]), code(got[1]))
	}
	if c, _ := x.produce("r", 0, x.epoch, 0, "still-early"); c != kerr.InvalidTxnState.Code {
		t.Errorf("produce after a refused add: %s", code(c))
	}
	// Who owns the id.
	realPID, realEpoch := x.pid, x.epoch
	x.pid = realPID + 1000
	if got := x.add("r", 0); got[0] != kerr.InvalidProducerIDMapping.Code {
		t.Errorf("AddPartitionsToTxn with another producer id: %s", code(got[0]))
	}
	x.pid, x.epoch = realPID, realEpoch+1
	if got := x.add("r", 0); got[0] != kerr.InvalidProducerEpoch.Code {
		t.Errorf("AddPartitionsToTxn with a newer epoch: %s", code(got[0]))
	}
	x.epoch = realEpoch

	if got := x.add("r", 0); got[0] != 0 {
		t.Fatalf("AddPartitionsToTxn: %s", code(got[0]))
	}
	if got := x.add("r", 0); got[0] != 0 {
		t.Errorf("adding a partition twice: %s", code(got[0]))
	}
	if c, off := x.produce("r", 0, x.epoch, 0, "one"); c != 0 || off != 0 {
		t.Errorf("a transactional produce to an added partition: %s at %d", code(c), off)
	}
	if c, _ := x.produce("r", 1, x.epoch, 0, "other-partition"); c != kerr.InvalidTxnState.Code {
		t.Errorf("a produce to a partition that was not added: %s", code(c))
	}
	if c, _ := x.produce("r", 0, x.epoch+1, 1, "bad-epoch"); c != kerr.InvalidProducerEpoch.Code {
		t.Errorf("a produce with the wrong epoch: %s", code(c))
	}

	// Tooling sees the transaction.
	if s := x.state(); s != "Ongoing" {
		t.Errorf("DescribeTransactions state = %q, want Ongoing", s)
	}
	lreq := kmsg.NewPtrListTransactionsRequest()
	lreq.StateFilters = []string{"Ongoing"}
	lr := ask(t, x.c, lreq, 0, kmsg.NewPtrListTransactionsResponse())
	if len(lr.TransactionStates) != 1 || lr.TransactionStates[0].TransactionalID != "rules" || lr.TransactionStates[0].ProducerID != realPID {
		t.Errorf("ListTransactions = %+v", lr.TransactionStates)
	}
	dreq := kmsg.NewPtrDescribeProducersRequest()
	dt := kmsg.NewDescribeProducersRequestTopic()
	dt.Topic, dt.Partitions = "r", []int32{0}
	dreq.Topics = append(dreq.Topics, dt)
	dp := ask(t, x.c, dreq, 0, kmsg.NewPtrDescribeProducersResponse()).Topics[0].Partitions[0]
	if len(dp.ActiveProducers) != 1 || dp.ActiveProducers[0].ProducerID != realPID || dp.ActiveProducers[0].CurrentTxnStartOffset != 0 {
		t.Errorf("DescribeProducers = %+v, want the producer with its transaction starting at 0", dp.ActiveProducers)
	}

	// Ending it.
	wantCode(t, "EndTxn from another producer", x.end(realPID+1000, realEpoch, true), kerr.InvalidProducerIDMapping)
	wantCode(t, "EndTxn commit", x.end(realPID, realEpoch, true), nil)
	wantCode(t, "EndTxn commit repeated (a retry)", x.end(realPID, realEpoch, true), nil)
	wantCode(t, "EndTxn abort after commit", x.end(realPID, realEpoch, false), kerr.InvalidTxnState)
	if s := x.state(); s != "CompleteCommit" {
		t.Errorf("state after commit = %q", s)
	}
	if lso := listOffsetIso(t, ctx, admin, "r", -1, 1); lso != 2 { // the record and the marker.
		t.Errorf("last stable offset after the commit = %d, want 2", lso)
	}
	// A new transaction on the same producer starts clean.
	if got := x.add("r", 1); got[0] != 0 {
		t.Errorf("a second transaction: %s", code(got[0]))
	}
	wantCode(t, "EndTxn abort of the second", x.end(realPID, realEpoch, false), nil)
	if s := x.state(); s != "CompleteAbort" {
		t.Errorf("state after abort = %q", s)
	}
	// Taking the id over: the epoch moves and the old one is fenced.
	oldEpoch := x.epoch
	wantCode(t, "InitProducerId again", x.init(30000), nil)
	if x.pid != realPID || x.epoch != oldEpoch+1 {
		t.Errorf("a second InitProducerId gave %d/%d, want the same producer at epoch %d", x.pid, x.epoch, oldEpoch+1)
	}
	if got := (&txnConn{t: t, c: x.c, id: "rules", pid: realPID, epoch: oldEpoch}).add("r", 0); got[0] != kerr.ProducerFenced.Code {
		t.Errorf("the previous epoch after taking the id over: %s, want PRODUCER_FENCED", code(got[0]))
	}
	wantCode(t, "EndTxn on a transactional id with nothing open", x.end(x.pid, x.epoch, true), kerr.InvalidTxnState)
}

// Offsets a transaction carries are applied with its commit and dropped with its abort;
// they need the group to have been named, and are checked against the group's member.
func TestTransactionOffsetsAreAppliedOnlyOnCommit(t *testing.T) {
	skipShort(t)
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	mustCreateTopic(t, ctx, admin, "src", 1)
	mustCreateTopic(t, ctx, admin, "dst", 1)
	x := &txnConn{t: t, c: openRaw(t, addr), id: "offsets"}
	wantCode(t, "init", x.init(30000), nil)

	commitOffsets := func(group string, offset int64) int16 {
		req := kmsg.NewPtrTxnOffsetCommitRequest()
		req.TransactionalID, req.Group, req.ProducerID, req.ProducerEpoch, req.Generation = x.id, group, x.pid, x.epoch, -1
		rt := kmsg.NewTxnOffsetCommitRequestTopic()
		rt.Topic = "src"
		rp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
		rp.Offset = offset
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return ask(t, x.c, req, 3, kmsg.NewPtrTxnOffsetCommitResponse()).Topics[0].Partitions[0].ErrorCode
	}
	addGroup := func(group string) int16 {
		req := kmsg.NewPtrAddOffsetsToTxnRequest()
		req.TransactionalID, req.ProducerID, req.ProducerEpoch, req.Group = x.id, x.pid, x.epoch, group
		return ask(t, x.c, req, 3, kmsg.NewPtrAddOffsetsToTxnResponse()).ErrorCode
	}

	wantCode(t, "TxnOffsetCommit with no open transaction", commitOffsets("g", 5), kerr.InvalidTxnState)
	wantCode(t, "AddOffsetsToTxn", addGroup("g"), nil)
	wantCode(t, "TxnOffsetCommit for a group that was not named", commitOffsets("other", 5), kerr.InvalidTxnState)
	wantCode(t, "TxnOffsetCommit", commitOffsets("g", 7), nil)
	if off := committedOffset(t, ctx, admin, "g", "src"); off != -1 {
		t.Errorf("an offset staged by an open transaction is already committed: %d", off)
	}
	wantCode(t, "abort", x.end(x.pid, x.epoch, false), nil)
	if off := committedOffset(t, ctx, admin, "g", "src"); off != -1 {
		t.Errorf("an aborted transaction's offset was applied: %d", off)
	}

	// Only the owner of the id may stage offsets.
	staleReq := kmsg.NewPtrTxnOffsetCommitRequest()
	staleReq.TransactionalID, staleReq.Group, staleReq.ProducerID, staleReq.ProducerEpoch, staleReq.Generation = x.id, "g", x.pid+1000, x.epoch, -1
	st := kmsg.NewTxnOffsetCommitRequestTopic()
	st.Topic = "src"
	sp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
	st.Partitions = append(st.Partitions, sp)
	staleReq.Topics = append(staleReq.Topics, st)
	wantCode(t, "TxnOffsetCommit from another producer", ask(t, x.c, staleReq, 3, kmsg.NewPtrTxnOffsetCommitResponse()).Topics[0].Partitions[0].ErrorCode, kerr.InvalidProducerIDMapping)

	wantCode(t, "AddOffsetsToTxn (second)", addGroup("g"), nil)
	wantCode(t, "TxnOffsetCommit (second)", commitOffsets("g", 9), nil)
	wantCode(t, "TxnOffsetCommit again, replacing", commitOffsets("g", 11), nil)
	wantCode(t, "commit", x.end(x.pid, x.epoch, true), nil)
	if off := committedOffset(t, ctx, admin, "g", "src"); off != 11 {
		t.Errorf("a committed transaction's offset = %d, want the last one staged, 11", off)
	}
}

// A stop in the middle of ending a transaction is finished on the next start: one whose
// commit was decided and stored is carried out (markers and offsets), and one still open
// belonged to a producer that is gone, so it is aborted and the id fenced.
func TestTransactionsLeftHalfDoneAreFinishedAfterARestart(t *testing.T) {
	skipShort(t)
	dir := t.TempDir()
	addr := "127.0.0.1:" + strconv.Itoa(freePort(t))
	stop := runBrokerAt(t, addr, dir, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	mustCreateTopic(t, ctx, admin, "rec", 1)

	open := func(id, value string, withOffsets bool) *txnConn {
		x := &txnConn{t: t, c: openRaw(t, addr), id: id}
		wantCode(t, id+" init", x.init(60000), nil)
		if got := x.add("rec", 0); got[0] != 0 {
			t.Fatalf("%s add: %s", id, code(got[0]))
		}
		if c, _ := x.produce("rec", 0, x.epoch, 0, value); c != 0 {
			t.Fatalf("%s produce: %s", id, code(c))
		}
		if withOffsets {
			areq := kmsg.NewPtrAddOffsetsToTxnRequest()
			areq.TransactionalID, areq.ProducerID, areq.ProducerEpoch, areq.Group = id, x.pid, x.epoch, "rg"
			wantCode(t, id+" add offsets", ask(t, x.c, areq, 3, kmsg.NewPtrAddOffsetsToTxnResponse()).ErrorCode, nil)
			treq := kmsg.NewPtrTxnOffsetCommitRequest()
			treq.TransactionalID, treq.Group, treq.ProducerID, treq.ProducerEpoch, treq.Generation = id, "rg", x.pid, x.epoch, -1
			rt := kmsg.NewTxnOffsetCommitRequestTopic()
			rt.Topic = "rec"
			rp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
			rp.Offset = 5
			rt.Partitions = append(rt.Partitions, rp)
			treq.Topics = append(treq.Topics, rt)
			wantCode(t, id+" stage offsets", ask(t, x.c, treq, 3, kmsg.NewPtrTxnOffsetCommitResponse()).Topics[0].Partitions[0].ErrorCode, nil)
		}
		return x
	}
	decided := open("decided", "the-decided-one", true)
	abandoned := open("abandoned", "the-abandoned-one", false)
	stop()

	// The stop came after "decided" chose to commit and before the markers were written.
	st, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	recs, _ := st.ListTxns()
	for _, r := range recs {
		if r.ID == "decided" {
			r.State = "PrepareCommit"
			if err := st.PutTxn(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	st.Close()

	stop = runBrokerAt(t, addr, dir, 4)
	defer stop()
	admin = newClient(t, addr)
	c := &txnConn{t: t, c: openRaw(t, addr)}
	c.id = "decided"
	if s := c.state(); s != "CompleteCommit" {
		t.Errorf("the decided transaction is %q after the restart, want CompleteCommit", s)
	}
	c.id = "abandoned"
	if s := c.state(); s != "CompleteAbort" {
		t.Errorf("the abandoned transaction is %q after the restart, want CompleteAbort", s)
	}
	if got := values(reader(t, addr, "rec", true), 2*time.Second); !eq(got, []string{"the-decided-one"}) {
		t.Errorf("read-committed saw %v, want only the transaction whose commit was decided", got)
	}
	if off := committedOffset(t, ctx, admin, "rg", "rec"); off != 5 {
		t.Errorf("the decided transaction's offset = %d, want 5", off)
	}
	// The abandoned producer is fenced: its id moved on.
	abandonedAfter := &txnConn{t: t, c: openRaw(t, addr), id: "abandoned", pid: abandoned.pid, epoch: abandoned.epoch}
	if got := abandonedAfter.add("rec", 0); got[0] != kerr.ProducerFenced.Code {
		t.Errorf("the abandoned producer after the restart: %s, want PRODUCER_FENCED", code(got[0]))
	}
	_ = decided
}
