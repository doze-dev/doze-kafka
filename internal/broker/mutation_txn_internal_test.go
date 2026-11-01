package broker

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// The transaction coordinator, driven through its handlers over a store that can be
// made to fail and a log whose flush can be made to fail.

// txnRig is a metadata store whose transaction writes, producer-id reservations and
// offset commits can be made to fail.
type txnRig struct {
	meta.Store
	putErr, reserveErr, commitErr error
}

func (r *txnRig) PutTxn(rec meta.TxnRecord) error {
	if r.putErr != nil {
		return r.putErr
	}
	return r.Store.PutTxn(rec)
}

func (r *txnRig) ReserveProducerIDs(n int64) (int64, error) {
	if r.reserveErr != nil {
		return 0, r.reserveErr
	}
	return r.Store.ReserveProducerIDs(n)
}

func (r *txnRig) CommitOffsets(group string, o []meta.OffsetCommit) error {
	if r.commitErr != nil {
		return r.commitErr
	}
	return r.Store.CommitOffsets(group, o)
}

var errStore = errors.New("the store is unavailable")

// newTxnRig is a broker with topic "t" (1 partition) over a rig; *failSync makes the
// log's flush fail.
func newTxnRig(t *testing.T) (b *Broker, rig *txnRig, failSync *bool, logs *log.Manager) {
	t.Helper()
	failSync = new(bool)
	b, logs = newTestBroker(t, failSync)
	rig = &txnRig{Store: b.meta}
	b.meta = rig
	return b, rig, failSync, logs
}

func initTxn(t *testing.T, b *Broker, id string, timeoutMs int32) *kmsg.InitProducerIDResponse {
	t.Helper()
	req := kmsg.NewPtrInitProducerIDRequest()
	req.TransactionalID, req.TransactionTimeoutMillis = &id, timeoutMs
	return callAPI(t, b.handleInitProducerID, req, 3)
}

func addPartitions(t *testing.T, b *Broker, id string, pid int64, epoch int16, topic string, parts ...int32) []kmsg.AddPartitionsToTxnResponseTopicPartition {
	t.Helper()
	req := kmsg.NewPtrAddPartitionsToTxnRequest()
	req.TransactionalID, req.ProducerID, req.ProducerEpoch = id, pid, epoch
	rt := kmsg.NewAddPartitionsToTxnRequestTopic()
	rt.Topic, rt.Partitions = topic, parts
	req.Topics = append(req.Topics, rt)
	return callAPI(t, b.handleAddPartitionsToTxn, req, 3).Topics[0].Partitions
}

func endTxnCall(t *testing.T, b *Broker, id string, pid int64, epoch int16, commit bool) int16 {
	t.Helper()
	req := kmsg.NewPtrEndTxnRequest()
	req.TransactionalID, req.ProducerID, req.ProducerEpoch, req.Commit = id, pid, epoch, commit
	return callAPI(t, b.handleEndTxn, req, 3).ErrorCode
}

// produceTxn sends one batch of a transaction.
func produceTxn(t *testing.T, b *Broker, txnID *string, topic string, partition int32, records []byte) kmsg.ProduceResponseTopicPartition {
	t.Helper()
	req := kmsg.NewPtrProduceRequest()
	req.Acks, req.TimeoutMillis, req.TransactionID = 1, 1000, txnID
	pt := kmsg.NewProduceRequestTopic()
	pt.Topic = topic
	pp := kmsg.NewProduceRequestTopicPartition()
	pp.Partition, pp.Records = partition, records
	pt.Partitions = append(pt.Partitions, pp)
	req.Topics = append(req.Topics, pt)
	req.SetVersion(8)
	out, err := b.handleProduce(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.Produce), APIVersion: 8, CorrelationID: 1},
		Body:   req.AppendTo(nil),
	})
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(8)
	if err := resp.ReadFrom(out[4:]); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp.Topics[0].Partitions[0]
}

func TestInitTxnProducerHoldsTheTimeoutToItsBounds(t *testing.T) {
	b, _, _, _ := newTxnRig(t)
	for i, tc := range []struct {
		ms int32
		ok bool
	}{{0, false}, {-1, false}, {1, true}, {maxTxnTimeoutMs, true}, {maxTxnTimeoutMs + 1, false}} {
		pid, epoch, code := b.initTxnProducer("tx-"+string(rune('a'+i)), tc.ms)
		if tc.ok && (code != 0 || pid < 0 || epoch != 0) {
			t.Errorf("timeout %d ms: pid %d epoch %d code %d, want a producer at epoch 0", tc.ms, pid, epoch, code)
		}
		if !tc.ok && (code != kerr.InvalidTransactionTimeout.Code || pid != -1 || epoch != -1) {
			t.Errorf("timeout %d ms: pid %d epoch %d code %d, want -1, -1, INVALID_TRANSACTION_TIMEOUT", tc.ms, pid, epoch, code)
		}
	}
	// The answer to the handler carries the same -1s.
	if r := initTxn(t, b, "tx-bad", 0); r.ErrorCode != kerr.InvalidTransactionTimeout.Code || r.ProducerID != -1 || r.ProducerEpoch != -1 {
		t.Errorf("InitProducerId with timeout 0: %+v", r)
	}
}

// Each InitProducerId makes the caller the owner at the next epoch, and settles
// whatever the last owner left: an open or aborting transaction is aborted, a
// committing one is carried out.
func TestInitTxnProducerFencesAndSettlesWhatWasLeft(t *testing.T) {
	b, _, _, _ := newTxnRig(t)
	first := initTxn(t, b, "tx", 60000)
	if first.ErrorCode != 0 || first.ProducerID < 0 || first.ProducerEpoch != 0 {
		t.Fatalf("first init: %+v", first)
	}
	e := b.txns.get("tx")
	second := initTxn(t, b, "tx", 30000)
	if second.ProducerID != first.ProducerID || second.ProducerEpoch != 1 {
		t.Errorf("second init: pid %d epoch %d, want pid %d epoch 1", second.ProducerID, second.ProducerEpoch, first.ProducerID)
	}
	if e.rec.State != txnEmpty || e.rec.TimeoutMs != 30000 {
		t.Errorf("after the second init: state %q timeout %d, want Empty and 30000", e.rec.State, e.rec.TimeoutMs)
	}

	// An open transaction is aborted and its owner fenced: the epoch moves twice.
	if p := addPartitions(t, b, "tx", second.ProducerID, 1, "t", 0); p[0].ErrorCode != 0 {
		t.Fatalf("AddPartitionsToTxn: %d", p[0].ErrorCode)
	}
	third := initTxn(t, b, "tx", 30000)
	if third.ProducerID != first.ProducerID || third.ProducerEpoch != 3 {
		t.Errorf("init over an open transaction: pid %d epoch %d, want the same pid at epoch 3", third.ProducerID, third.ProducerEpoch)
	}
	if e.rec.State != txnEmpty || len(e.rec.Partitions) != 0 {
		t.Errorf("after it: state %q with %d partitions, want Empty and none", e.rec.State, len(e.rec.Partitions))
	}

	// A commit that was decided and not carried out is carried out, offsets and all.
	e.mu.Lock()
	e.rec.State = txnPrepareCommit
	e.rec.Offsets = []meta.TxnOffset{{Group: "g", Topic: "t", Partition: 0, Offset: 7}}
	e.mu.Unlock()
	fourth := initTxn(t, b, "tx", 30000)
	if fourth.ErrorCode != 0 || fourth.ProducerEpoch != 4 {
		t.Errorf("init over a decided commit: %+v, want epoch 4", fourth)
	}
	if off, ok, err := b.meta.FetchOffset("g", "t", 0); err != nil || !ok || off != 7 {
		t.Errorf("the committed offset: %d %v %v, want 7", off, ok, err)
	}
	// A decided abort is carried out and commits nothing.
	e.mu.Lock()
	e.rec.State = txnPrepareAbort
	e.rec.Offsets = []meta.TxnOffset{{Group: "g2", Topic: "t", Partition: 0, Offset: 9}}
	e.mu.Unlock()
	if r := initTxn(t, b, "tx", 30000); r.ErrorCode != 0 || r.ProducerEpoch != 6 {
		t.Errorf("init over a decided abort: %+v, want epoch 6 (the abort fences once, the init once)", r)
	}
	if _, ok, _ := b.meta.FetchOffset("g2", "t", 0); ok {
		t.Error("an aborted transaction's offsets were committed")
	}
	// Settled states are not aborted: the epoch moves once.
	for _, state := range []string{txnCompleteCommit, txnCompleteAbort, txnEmpty} {
		e.mu.Lock()
		e.rec.State = state
		before := e.rec.Epoch
		e.mu.Unlock()
		if r := initTxn(t, b, "tx", 30000); r.ProducerEpoch != before+1 {
			t.Errorf("init over %s: epoch %d, want %d", state, r.ProducerEpoch, before+1)
		}
	}
}

// Epochs run out at the top of an int16: the producer id changes and the epoch starts
// again, one epoch before the very last.
func TestInitTxnProducerStartsAgainWhenEpochsRunOut(t *testing.T) {
	b, rig, _, _ := newTxnRig(t)
	for _, tc := range []struct {
		epoch    int16
		samePID  bool
		wantNext int16
	}{
		{math.MaxInt16 - 2, true, math.MaxInt16 - 1},
		{math.MaxInt16 - 1, false, 0},
	} {
		e := b.txns.getOrCreate("tx-run-out")
		e.mu.Lock()
		e.rec = meta.TxnRecord{ID: "tx-run-out", ProducerID: 77, Epoch: tc.epoch, State: txnEmpty}
		e.mu.Unlock()
		r := initTxn(t, b, "tx-run-out", 1000)
		if r.ErrorCode != 0 || (r.ProducerID == 77) != tc.samePID || r.ProducerEpoch != tc.wantNext {
			t.Errorf("init at epoch %d: pid %d epoch %d code %d, want same pid %v and epoch %d", tc.epoch, r.ProducerID, r.ProducerEpoch, r.ErrorCode, tc.samePID, tc.wantNext)
		}
	}
	// A new id that cannot be reserved is the coordinator being unavailable.
	e := b.txns.getOrCreate("tx-run-out")
	e.mu.Lock()
	e.rec.Epoch = math.MaxInt16 - 1
	e.mu.Unlock()
	rig.reserveErr = errStore
	b.pidMu.Lock()
	b.pidNext, b.pidEnd = 0, 0
	b.pidMu.Unlock()
	if r := initTxn(t, b, "tx-run-out", 1000); r.ErrorCode != txnStoreUnavailable || r.ProducerID != -1 || r.ProducerEpoch != -1 {
		t.Errorf("no id to reserve at the last epoch: %+v, want NOT_COORDINATOR with -1s", r)
	}
	if r := initTxn(t, b, "tx-brand-new", 1000); r.ErrorCode != txnStoreUnavailable || r.ProducerID != -1 || r.ProducerEpoch != -1 {
		t.Errorf("no id to reserve for a new transactional id: %+v, want NOT_COORDINATOR with -1s", r)
	}
}

// When the state cannot be stored nothing changes and the answer is NOT_COORDINATOR;
// a decision that cannot be carried out is CONCURRENT_TRANSACTIONS and stands.
func TestInitTxnProducerWhenTheStateCannotBeStoredOrFinished(t *testing.T) {
	b, rig, failSync, _ := newTxnRig(t)
	rig.putErr = errStore
	if r := initTxn(t, b, "tx", 1000); r.ErrorCode != txnStoreUnavailable || r.ProducerID != -1 || r.ProducerEpoch != -1 {
		t.Errorf("a first init, store down: %+v, want NOT_COORDINATOR with -1s", r)
	}
	if st := b.txns.get("tx").rec.State; st != "" {
		t.Errorf("state %q after a write that failed, want none", st)
	}
	rig.putErr = nil
	owner := initTxn(t, b, "tx", 1000)
	if owner.ErrorCode != 0 {
		t.Fatalf("init: %+v", owner)
	}

	// Open, and the abort cannot be stored.
	if p := addPartitions(t, b, "tx", owner.ProducerID, owner.ProducerEpoch, "t", 0); p[0].ErrorCode != 0 {
		t.Fatalf("AddPartitionsToTxn: %d", p[0].ErrorCode)
	}
	rig.putErr = errStore
	if r := initTxn(t, b, "tx", 1000); r.ErrorCode != txnStoreUnavailable || r.ProducerID != -1 || r.ProducerEpoch != -1 {
		t.Errorf("init over an open transaction, store down: %+v, want NOT_COORDINATOR with -1s", r)
	}
	rig.putErr = nil
	if st := b.txns.get("tx").rec; st.State != txnOngoing || st.Epoch != owner.ProducerEpoch {
		t.Errorf("after it: state %q epoch %d, want Ongoing at epoch %d", st.State, st.Epoch, owner.ProducerEpoch)
	}

	// Open, and the markers cannot be flushed: the abort is decided and stands.
	dirty(t, b)
	*failSync = true
	r := initTxn(t, b, "tx", 1000)
	if r.ErrorCode != kerr.ConcurrentTransactions.Code || r.ProducerID != -1 || r.ProducerEpoch != -1 {
		t.Errorf("init over an open transaction, flush failing: %+v, want CONCURRENT_TRANSACTIONS with -1s", r)
	}
	if st := b.txns.get("tx").rec.State; st != txnPrepareAbort {
		t.Errorf("state %q, want the decision PrepareAbort to stand", st)
	}
	// A commit that cannot be finished is the same.
	e := b.txns.get("tx")
	e.mu.Lock()
	e.rec.State = txnPrepareCommit
	e.mu.Unlock()
	if r := initTxn(t, b, "tx", 1000); r.ErrorCode != kerr.ConcurrentTransactions.Code || r.ProducerID != -1 || r.ProducerEpoch != -1 {
		t.Errorf("init over a decided commit, flush failing: %+v, want CONCURRENT_TRANSACTIONS with -1s", r)
	}
	// And once the log can flush, init settles it.
	*failSync = false
	if r := initTxn(t, b, "tx", 1000); r.ErrorCode != 0 || r.ProducerEpoch <= owner.ProducerEpoch {
		t.Errorf("init once the flush works: %+v", r)
	}
	// The last write of an init is the one that can fail after everything else did.
	rig.putErr = errStore
	if r := initTxn(t, b, "tx", 1000); r.ErrorCode != txnStoreUnavailable || r.ProducerID != -1 || r.ProducerEpoch != -1 {
		t.Errorf("init over a settled transaction, store down: %+v, want NOT_COORDINATOR with -1s", r)
	}
}

// EndTxn by state and decision, as the comment on endTxn lays them out, and who may
// end a transaction at all.
func TestEndTxnAnswersByStateAndDecision(t *testing.T) {
	b, rig, failSync, _ := newTxnRig(t)
	owner := initTxn(t, b, "tx", 60000)
	pid, epoch := owner.ProducerID, owner.ProducerEpoch
	e := b.txns.get("tx")
	set := func(state string) {
		e.mu.Lock()
		e.rec.State = state
		e.rec.Partitions = []meta.TxnPartition{{Topic: "t", Partition: 0}}
		e.mu.Unlock()
	}
	state := func() string { e.mu.RLock(); defer e.mu.RUnlock(); return e.rec.State }

	for _, tc := range []struct {
		from       string
		commit     bool
		code       int16
		settledAs  string
		failedFlow bool // the flush fails: a decided transaction stays decided.
	}{
		{txnOngoing, true, 0, txnCompleteCommit, false},
		{txnOngoing, false, 0, txnCompleteAbort, false},
		{txnPrepareCommit, true, 0, txnCompleteCommit, false},
		{txnPrepareCommit, false, kerr.InvalidTxnState.Code, txnCompleteCommit, false},
		{txnPrepareAbort, false, 0, txnCompleteAbort, false},
		{txnPrepareAbort, true, kerr.InvalidTxnState.Code, txnCompleteAbort, false},
		{txnCompleteCommit, true, 0, txnCompleteCommit, false},
		{txnCompleteCommit, false, kerr.InvalidTxnState.Code, txnCompleteCommit, false},
		{txnCompleteAbort, false, 0, txnCompleteAbort, false},
		{txnCompleteAbort, true, kerr.InvalidTxnState.Code, txnCompleteAbort, false},
		{txnEmpty, true, kerr.InvalidTxnState.Code, txnEmpty, false},
		{txnEmpty, false, kerr.InvalidTxnState.Code, txnEmpty, false},
		// The markers cannot be flushed: the decision is stored and stands.
		{txnOngoing, true, kerr.ConcurrentTransactions.Code, txnPrepareCommit, true},
		{txnOngoing, false, kerr.ConcurrentTransactions.Code, txnPrepareAbort, true},
		{txnPrepareCommit, false, kerr.ConcurrentTransactions.Code, txnPrepareCommit, true},
		{txnPrepareAbort, true, kerr.ConcurrentTransactions.Code, txnPrepareAbort, true},
	} {
		set(tc.from)
		if tc.failedFlow {
			dirty(t, b)
		}
		*failSync = tc.failedFlow
		if got := endTxnCall(t, b, "tx", pid, epoch, tc.commit); got != tc.code {
			t.Errorf("%s, commit=%v, flush failing=%v: code %d, want %d", tc.from, tc.commit, tc.failedFlow, got, tc.code)
		}
		if got := state(); got != tc.settledAs {
			t.Errorf("%s, commit=%v, flush failing=%v: left in %s, want %s", tc.from, tc.commit, tc.failedFlow, got, tc.settledAs)
		}
	}
	*failSync = false

	// A decision that cannot be stored leaves the transaction open and answers NOT_COORDINATOR.
	set(txnOngoing)
	rig.putErr = errStore
	if got := endTxnCall(t, b, "tx", pid, epoch, true); got != txnStoreUnavailable {
		t.Errorf("EndTxn with the store down: code %d, want NOT_COORDINATOR", got)
	}
	rig.putErr = nil
	if got := state(); got != txnOngoing {
		t.Errorf("state %s after a decision that could not be stored, want Ongoing", got)
	}

	// Who may end it.
	for _, tc := range []struct {
		name  string
		id    string
		pid   int64
		epoch int16
		code  int16
	}{
		{"an id nobody owns", "nobody", pid, epoch, kerr.InvalidProducerIDMapping.Code},
		{"another producer id", "tx", pid + 1, epoch, kerr.InvalidProducerIDMapping.Code},
		{"an older epoch", "tx", pid, epoch - 1, kerr.ProducerFenced.Code},
		{"a newer epoch", "tx", pid, epoch + 1, kerr.InvalidProducerEpoch.Code},
		{"no id at all", "", pid, epoch, kerr.InvalidRequest.Code},
	} {
		if got := endTxnCall(t, b, tc.id, tc.pid, tc.epoch, true); got != tc.code {
			t.Errorf("%s: code %d, want %d", tc.name, got, tc.code)
		}
	}
	// An entry that exists but was never initialised is owned by nobody either.
	b.txns.getOrCreate("blank")
	if got := endTxnCall(t, b, "blank", 0, 0, true); got != kerr.InvalidProducerIDMapping.Code {
		t.Errorf("an entry with no state: code %d, want INVALID_PRODUCER_ID_MAPPING", got)
	}
}

// aborting to fence raises the epoch by one, and stops at the top of an int16.
func TestAbortFencingRaisesTheEpochToTheTop(t *testing.T) {
	b, _, _, _ := newTxnRig(t)
	e := b.txns.getOrCreate("tx-fence")
	for _, tc := range []struct{ from, want int16 }{{5, 6}, {math.MaxInt16 - 1, math.MaxInt16}, {math.MaxInt16, math.MaxInt16}} {
		e.mu.Lock()
		e.rec = meta.TxnRecord{ID: "tx-fence", ProducerID: 3, Epoch: tc.from, State: txnOngoing}
		if code := b.abortFencing(e); code != 0 {
			t.Fatalf("abortFencing: %d", code)
		}
		got, state := e.rec.Epoch, e.rec.State
		e.mu.Unlock()
		if got != tc.want || state != txnCompleteAbort {
			t.Errorf("from epoch %d: epoch %d state %s, want epoch %d and CompleteAbort", tc.from, got, state, tc.want)
		}
	}
}

// dirty leaves unsynced bytes in the log's only partition, so that a flush has
// something to fail on.
func dirty(t *testing.T, b *Broker) {
	t.Helper()
	if p := produceTo(t, b, "t", 0, 1, plainBatch()); p.ErrorCode != 0 {
		t.Fatalf("produce: %d", p.ErrorCode)
	}
}

// Producer ids are handed out one after another, from blocks reserved in the store.
func TestProducerIDsAreConsecutiveAcrossBlocks(t *testing.T) {
	b, rig, _, _ := newTxnRig(t)
	first, err := b.nextProducerID()
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= producerIDBlock+2; i++ {
		id, err := b.nextProducerID()
		if err != nil || id != first+i {
			t.Fatalf("id %d after %d: %d %v, want %d", i, first, id, err, first+i)
		}
	}
	// A block that cannot be reserved is an error, and nothing is handed out.
	b.pidMu.Lock()
	b.pidNext = b.pidEnd
	b.pidMu.Unlock()
	rig.reserveErr = errStore
	if id, err := b.nextProducerID(); err == nil {
		t.Errorf("an id %d with the store down", id)
	}
}

func TestInitProducerIDAnswersForTheIdempotentProducer(t *testing.T) {
	b, _, _, _ := newTxnRig(t)
	init := func() *kmsg.InitProducerIDResponse {
		req := kmsg.NewPtrInitProducerIDRequest()
		return callAPI(t, b.handleInitProducerID, req, 3)
	}
	a, c := init(), init()
	if a.ErrorCode != 0 || a.ProducerEpoch != 0 || c.ErrorCode != 0 || c.ProducerEpoch != 0 || c.ProducerID != a.ProducerID+1 {
		t.Errorf("two idempotent producers: %+v and %+v, want consecutive ids at epoch 0", a, c)
	}
	// Without the right to write idempotently the answer carries no id.
	denyUnlessAllowed(b)
	if r := init(); r.ErrorCode != kerr.ClusterAuthorizationFailed.Code || r.ProducerID != -1 || r.ProducerEpoch != -1 {
		t.Errorf("no right on the cluster: %+v, want CLUSTER_AUTHORIZATION_FAILED with -1s", r)
	}
	// Some right to write on a topic is enough, unless idempotent writes are denied.
	denyUnlessAllowed(b, allowAnon(resTopic, "t", opWrite))
	if r := init(); r.ErrorCode != 0 {
		t.Errorf("a right to write a topic: code %d", r.ErrorCode)
	}
	denyUnlessAllowed(b, allowAnon(resTopic, "t", opWrite), acl(resCluster, clusterResourceName, aclLiteral, "User:ANONYMOUS", "*", opIdempotent, permDeny))
	if r := init(); r.ErrorCode != kerr.ClusterAuthorizationFailed.Code {
		t.Errorf("idempotent writes denied: code %d", r.ErrorCode)
	}
}

func TestInitProducerIDRefusesATransactionalIDItCannotKeep(t *testing.T) {
	b, _, _, _ := newTxnRig(t)
	for _, tc := range []struct {
		name string
		id   string
		code int16
	}{
		{"an id longer than a name may be", strings.Repeat("x", maxNameBytes+1), kerr.InvalidRequest.Code},
		{"an empty id", "", kerr.InvalidRequest.Code},
		{"an id of the longest length", strings.Repeat("x", maxNameBytes), 0},
	} {
		r := initTxn(t, b, tc.id, 1000)
		if r.ErrorCode != tc.code || (tc.code != 0 && (r.ProducerID != -1 || r.ProducerEpoch != -1)) {
			t.Errorf("%s: %+v, want code %d (and -1s if refused)", tc.name, r, tc.code)
		}
	}
	denyUnlessAllowed(b)
	if r := initTxn(t, b, "tx", 1000); r.ErrorCode != kerr.TransactionalIDAuthorizationFailed.Code || r.ProducerID != -1 || r.ProducerEpoch != -1 {
		t.Errorf("no right on the id: %+v, want TRANSACTIONAL_ID_AUTHORIZATION_FAILED with -1s", r)
	}
}
