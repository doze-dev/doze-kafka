package conformance

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"net"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// More scenarios of the "Bad days" stage: what a real Apache Kafka answers
//
//   - to the end of a transaction that has already been decided, the same way or
//     the other (endtxn-decisions), including while the decision is still being
//     carried out (endtxn-while-preparing);
//   - on each API that writes, when the volume its log directory is on has no
//     room left (the full-disk-* scenarios, one broker each: a Kafka whose only
//     log directory fails shuts down, so each API has to be the first to find out).

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "endtxn-decisions", Run: endTxnDecisions, KafkaEnv: brokerReachesItself},
		Scenario{Name: "endtxn-while-preparing", Run: endTxnWhilePreparing, KafkaEnv: brokerReachesItself,
			Frozen:   "min.insync.replicas=2 on one broker does not keep the markers from being written (Apache Kafka 4.3.1 accepts acks=all there), so a transaction is in Prepare for the milliseconds the markers take and a recording catches that on some runs and not on others",
			NoReplay: "doze-kafka writes a transaction's markers before it answers EndTxn, so a client never finds it in PrepareCommit or PrepareAbort unless a write failed; what it answers then is held by TestEndTxnAnswersTheOppositeDecisionAsKafkaDoes"},
	)
	for _, s := range diskFullScenarios {
		env := append(append([]string(nil), brokerReachesItself...), metadataLogElsewhere)
		known := append([]Known{{Step: "broker-after", Limitation: staysUp}}, s.known...)
		Scenarios = append(Scenarios, Scenario{Name: "full-disk-" + s.name, Run: s.run, DiskMiB: 64, KafkaEnv: append(env, s.env...), Known: known})
	}
}

// The sentences of docs/limitations.md that explain how doze-kafka on a full
// volume differs from the recordings.
const (
	staysUp        = "this one stays up, which is kinder to a"
	registryOnDisk = "keeps its topic registry, and the log"
	groupsOnDisk   = "DeleteGroups and OffsetDelete answer `KAFKA_STORAGE_ERROR` where Kafka answers"
)

// brokerReachesItself gives the recorded broker a second listener that it
// advertises to itself. A transaction coordinator writes markers, and checks a
// transactional produce, by calling the partition's leader over the inter-broker
// listener, which here is the same broker. The recorder's one listener is
// advertised as 127.0.0.1 and a port of the host, which nothing inside the
// container listens on: with it every transaction stays in PrepareCommit for
// ever and every transactional produce is answered NOT_ENOUGH_REPLICAS (the
// first recording of endtxn-decisions showed exactly that).
var brokerReachesItself = []string{
	"KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,INTERNAL://:9094",
	"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT",
	"KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
}

// txnBatch is encodeBatch for a transactional producer: the same bytes with the
// producer's id, epoch and sequence and the transactional bit, and the CRC of that.
func txnBatch(pid int64, epoch int16, seq int32, recs ...ProduceRecord) []byte {
	b := encodeBatch(recs)
	binary.BigEndian.PutUint16(b[21:], binary.BigEndian.Uint16(b[21:])|0x10)
	binary.BigEndian.PutUint64(b[43:], uint64(pid))
	binary.BigEndian.PutUint16(b[51:], uint16(epoch))
	binary.BigEndian.PutUint32(b[53:], uint32(seq))
	binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], crc32.MakeTable(crc32.Castagnoli)))
	return b
}

// txn is one transactional producer in a script.
type txn struct {
	e     *Env
	id    string
	pid   int64
	epoch int16
	seq   int32
}

// beginTxn is InitProducerId for a transactional id (v3).
func (e *Env) beginTxn(label, id string) *txn {
	r := kmsg.NewPtrInitProducerIDRequest()
	r.TransactionalID = &id
	r.TransactionTimeoutMillis = 60000
	r.ProducerID, r.ProducerEpoch = -1, -1
	resp, _ := e.Do(label, r, 3).(*kmsg.InitProducerIDResponse)
	t := &txn{e: e, id: id}
	if resp != nil {
		t.pid, t.epoch = resp.ProducerID, resp.ProducerEpoch
	}
	return t
}

func (t *txn) addRequest(topic string) *kmsg.AddPartitionsToTxnRequest {
	r := kmsg.NewPtrAddPartitionsToTxnRequest()
	r.TransactionalID, r.ProducerID, r.ProducerEpoch = t.id, t.pid, t.epoch
	rt := kmsg.NewAddPartitionsToTxnRequestTopic()
	rt.Topic, rt.Partitions = topic, []int32{0}
	r.Topics = append(r.Topics, rt)
	return r
}

// add is AddPartitionsToTxn (v3) for partition 0 of topic.
func (t *txn) add(label, topic string) { t.e.Do(label, t.addRequest(topic), 3) }

func (t *txn) produceRequest(topic, value string) *kmsg.ProduceRequest {
	r := kmsg.NewPtrProduceRequest()
	r.Acks, r.TimeoutMillis, r.TransactionID = -1, 10000, &t.id
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	p := kmsg.NewProduceRequestTopicPartition()
	p.Records = txnBatch(t.pid, t.epoch, t.seq, ProduceRecord{TimestampMs: 1000, Key: "k", Value: value})
	rt.Partitions = append(rt.Partitions, p)
	r.Topics = append(r.Topics, rt)
	return r
}

// produce writes one transactional record to partition 0 of topic (Produce v9).
func (t *txn) produce(label, topic, value string) {
	req := t.produceRequest(topic, value)
	t.seq++
	t.e.DoUntil(label, req, 9, func(resp kmsg.Response) bool {
		// No leader yet (6), or the leader has not yet heard from the coordinator
		// that the partition is in the transaction, which a produce of this
		// version is told as NOT_ENOUGH_REPLICAS (19).
		code := resp.(*kmsg.ProduceResponse).Topics[0].Partitions[0].ErrorCode
		return code != 6 && code != 19
	})
}

func (t *txn) endRequest(commit bool) *kmsg.EndTxnRequest {
	r := kmsg.NewPtrEndTxnRequest()
	r.TransactionalID, r.ProducerID, r.ProducerEpoch, r.Commit = t.id, t.pid, t.epoch, commit
	return r
}

// end is EndTxn (v3), recorded as it was first answered: CONCURRENT_TRANSACTIONS
// is an answer here, not something to wait out.
func (t *txn) end(label string, commit bool) int16 {
	resp, _ := t.e.once(label, t.endRequest(commit), 3).(*kmsg.EndTxnResponse)
	if resp == nil {
		return -1
	}
	return resp.ErrorCode
}

// state waits (up to 20 s) for the coordinator to report the transaction in one
// of the states named, and records the state it was last in. Only the state is
// recorded: the rest of DescribeTransactions is not this script's business.
func (t *txn) state(label string, want ...string) string {
	t.e.fail2(label)
	r := kmsg.NewPtrDescribeTransactionsRequest()
	r.TransactionalIDs = []string{t.id}
	got := ""
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		resp, err := t.e.w.roundTrip(r, 0)
		if err != nil {
			t.e.fail("%s: %v", label, err)
			return ""
		}
		d := resp.(*kmsg.DescribeTransactionsResponse)
		if len(d.TransactionStates) == 1 {
			got = d.TransactionStates[0].State
		}
		done := false
		for _, w := range want {
			done = done || got == w
		}
		if done {
			break
		}
	}
	t.e.steps = append(t.e.steps, Step{Label: label, Conn: t.e.cur, API: "DescribeTransactions", Version: 0,
		Request:  map[string]any{"TransactionalIDs": []any{t.id}},
		Response: map[string]any{"State": got}})
	return got
}

// once sends req and records the first answer, whatever it is: no waiting out a
// coordinator, and a connection the broker closed or never answered on is the
// answer. It returns nil when there was none.
func (e *Env) once(label string, req kmsg.Request, version int16) kmsg.Response {
	e.fail2(label)
	nreq, err := e.norm.normalize(req)
	if err != nil {
		e.fail("%s: %v", label, err)
	}
	step := Step{Label: label, Conn: e.cur, API: kmsg.NameForKey(req.Key()), Version: version, Request: nreq}
	resp, err := e.w.roundTrip(req, version)
	if err != nil {
		what := "closed by the broker"
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			what = "no answer within 60 s"
		} else if !isClosed(err) {
			e.fail("%s: %v", label, err)
			return nil
		}
		step.Response = map[string]any{"(connection)": what}
		e.steps = append(e.steps, step)
		// A new connection for whatever follows, if the broker is still there.
		e.w.close()
		if w, derr := dialOnce(e.target.Addr()); derr == nil {
			e.w = w
			e.conns[e.cur].w = w
		}
		return nil
	}
	sortProduceResponse(resp)
	nresp, err := e.norm.normalize(resp)
	if err != nil {
		e.fail("%s: %v", label, err)
	}
	step.Response = nresp
	e.steps = append(e.steps, step)
	return resp
}

// endTxnDecisions: a transaction that has been committed (or aborted) is ended
// again, the same way and the other way; and a producer with no transaction open
// ends one.
func endTxnDecisions(e *Env) {
	e.Do("create", createTopics(false, newTopic("tx", 1, 1)), 7)
	fc := kmsg.NewPtrFindCoordinatorRequest()
	fc.CoordinatorType, fc.CoordinatorKeys = 1, []string{"tx-a"}
	e.Do("find-coordinator", fc, 4) // makes a real broker create its transaction log.

	a := e.beginTxn("a-init", "tx-a")
	a.add("a-add", "tx")
	a.produce("a-produce", "tx", "committed")
	a.end("a-commit", true)
	a.state("a-state", "CompleteCommit")
	a.end("a-commit-again", true)
	a.end("a-abort-after-commit", false)
	a.end("a-commit-after-the-refused-abort", true)
	a.state("a-state-after", "CompleteCommit")

	b := e.beginTxn("b-init", "tx-b")
	b.add("b-add", "tx")
	b.produce("b-produce", "tx", "aborted")
	b.end("b-abort", false)
	b.state("b-state", "CompleteAbort")
	b.end("b-abort-again", false)
	b.end("b-commit-after-abort", true)
	b.end("b-abort-after-the-refused-commit", false)
	b.state("b-state-after", "CompleteAbort")

	// No transaction open at all.
	c := e.beginTxn("c-init", "tx-c")
	c.end("c-commit-with-nothing-open", true)
	c.end("c-abort-with-nothing-open", false)
	c.state("c-state", "Empty")

	// Where a read-committed consumer may read to: past both transactions, the
	// committed one and the aborted one, and nothing left open.
	lo := kmsg.NewPtrListOffsetsRequest()
	lo.ReplicaID, lo.IsolationLevel = -1, 1
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = "tx"
	lp := kmsg.NewListOffsetsRequestTopicPartition()
	lp.Partition, lp.Timestamp, lp.CurrentLeaderEpoch = 0, -1, -1
	lt.Partitions = append(lt.Partitions, lp)
	lo.Topics = append(lo.Topics, lt)
	e.Do("last-stable-offset", lo, 7)
}

// endTxnWhilePreparing: a transaction whose markers cannot be written stays in
// PrepareCommit (or PrepareAbort), and is ended again there. The markers cannot
// be written because the partition wants two in-sync replicas and has one.
func endTxnWhilePreparing(e *Env) {
	e.Do("create", createTopics(false, newTopic("stuck", 1, 1, "min.insync.replicas", "2")), 7)
	fc := kmsg.NewPtrFindCoordinatorRequest()
	fc.CoordinatorType, fc.CoordinatorKeys = 1, []string{"tx-p"}
	e.Do("find-coordinator", fc, 4)

	p := e.beginTxn("p-init", "tx-p")
	p.add("p-add", "stuck")
	p.end("p-commit", true)
	p.state("p-state", "PrepareCommit", "CompleteCommit")
	p.end("p-commit-again", true)
	p.end("p-abort-while-committing", false)
	e.once("p-add-while-committing", p.addRequest("stuck"), 3)
	init := kmsg.NewPtrInitProducerIDRequest()
	init.TransactionalID, init.TransactionTimeoutMillis, init.ProducerID, init.ProducerEpoch = &p.id, 60000, -1, -1
	e.once("p-init-while-committing", init, 3)
	p.state("p-state-after", "PrepareCommit", "CompleteCommit")

	q := e.beginTxn("q-init", "tx-q")
	q.add("q-add", "stuck")
	q.end("q-abort", false)
	q.state("q-state", "PrepareAbort", "CompleteAbort")
	q.end("q-abort-again", false)
	q.end("q-commit-while-aborting", true)
	q.state("q-state-after", "PrepareAbort", "CompleteAbort")
}

// --- a full disk, API by API ---

// metadataLogElsewhere keeps the KRaft metadata log off the volume that is filled.
// A combined broker and controller writes to that log twice a second whether or
// not anyone asks it to, so with it on the full volume the process dies within a
// moment of the fill, whatever the script then sends: every scenario recorded
// "the broker is gone" and nothing about the API it was for.
const metadataLogElsewhere = "KAFKA_METADATA_LOG_DIR=/tmp/kraft-metadata"

// big is a string of n bytes. A full volume still has room in the last page of a
// file that is already there, so a write of a few bytes succeeds on it (the first
// recording of these scenarios answered almost everything with success). Every
// write these scripts make after the fill is of more than a page.
func big(prefix string, n int) string { return prefix + strings.Repeat("x", n) }

var (
	bigGroup = big("g-", 4090) // under the broker's 4,096-byte bound on a name, and a page long with the rest of the record.
	bigTxnID = big("tx-", 4090)
	bigValue = big("", 9000)
	bigMeta  = big("", 4000) // offset.metadata.max.bytes is 4096.
)

// Execer is a target that can run a command where the broker's files are.
type Execer interface {
	Exec(args ...string) (string, error)
}

// armDiskFull, when the fault seam is compiled in (fault_on.go), makes every
// write of the doze-kafka in this process fail with "no space left on device".
var armDiskFull func()

const kafkaLogDir = "/var/lib/kafka/data"

// fillDisk leaves the broker's data volume with no room at all. Against Apache
// Kafka it writes filler files beside the logs until the volume refuses; against
// doze-kafka, which shares this process, the fault seam stands in for the volume.
func (e *Env) fillDisk(label string) {
	e.fail2(label)
	switch t := e.target.(type) {
	case Execer:
		// Megabytes, then pages, then bytes, until nothing more goes in.
		script := "cd " + kafkaLogDir + " && " +
			"dd if=/dev/zero of=.filler-1 bs=1M 2>/dev/null; " +
			"dd if=/dev/zero of=.filler-2 bs=4k 2>/dev/null; " +
			"dd if=/dev/zero of=.filler-3 bs=1 count=8192 2>/dev/null; " +
			"df -k . | tail -1"
		out, err := t.Exec("sh", "-c", script)
		if err != nil {
			e.fail("%s: %v: %s", label, err, out)
			return
		}
		f := strings.Fields(out)
		if len(f) < 4 || f[3] != "0" {
			e.fail("%s: the volume is not full: %s", label, strings.TrimSpace(out))
			return
		}
	default:
		if armDiskFull == nil {
			e.fail("%s: filling doze-kafka's disk needs the fault seam (go test -tags faultinject)", label)
			return
		}
		armDiskFull()
	}
	e.steps = append(e.steps, Step{Label: label, API: "(the data volume is filled)"})
}

// brokerAfter records whether the broker still answers.
func (e *Env) brokerAfter(label string) {
	e.fail2(label)
	answer := "gone"
	// A broker that is shutting down may answer for a moment longer.
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		w, err := dialOnce(e.target.Addr())
		if err != nil {
			answer = "gone"
			break
		}
		ok := probe(w)
		w.close()
		if !ok {
			answer = "gone"
			break
		}
		answer = "still answering Metadata"
	}
	e.steps = append(e.steps, Step{Label: label, API: "(is the broker there?)", Response: map[string]any{"broker": answer}})
}

type diskFullScenario struct {
	name  string
	run   func(e *Env)
	env   []string
	known []Known
}

func commitRequestFor(group, topic string, offset int64) *kmsg.OffsetCommitRequest {
	r := kmsg.NewPtrOffsetCommitRequest()
	r.Group, r.Generation = group, -1
	t := kmsg.NewOffsetCommitRequestTopic()
	t.Topic = topic
	p := kmsg.NewOffsetCommitRequestTopicPartition()
	p.Partition, p.Offset, p.LeaderEpoch, p.Metadata = 0, offset, -1, &bigMeta
	t.Partitions = append(t.Partitions, p)
	r.Topics = append(r.Topics, t)
	return r
}

func findTxnCoordinator(e *Env, id string) {
	fc := kmsg.NewPtrFindCoordinatorRequest()
	fc.CoordinatorType, fc.CoordinatorKeys = 1, []string{id}
	e.Do("find-txn-coordinator", fc, 4)
}

// diskFullSetup is what every one of these starts with: a topic with a record in
// it, a group with a committed offset (so the offsets log exists), and a
// transactional producer (so the transaction log does).
func diskFullSetup(e *Env) *txn {
	e.Do("create", createTopics(false, newTopic("disk", 1, 1)), 7)
	e.DoUntil("small-ok", produceOne("disk", -1), 9, func(resp kmsg.Response) bool {
		return resp.(*kmsg.ProduceResponse).Topics[0].Partitions[0].ErrorCode != 6
	})
	e.findCoordinators(bigGroup)
	e.Do("commit-ok", commitRequestFor(bigGroup, "disk", 1), 8)
	findTxnCoordinator(e, bigTxnID)
	return e.beginTxn("init-ok", bigTxnID)
}

func produceOne(topic string, acks int16) *kmsg.ProduceRequest {
	r := kmsg.NewPtrProduceRequest()
	r.Acks, r.TimeoutMillis = acks, 10000
	t := kmsg.NewProduceRequestTopic()
	t.Topic = topic
	p := kmsg.NewProduceRequestTopicPartition()
	p.Records = encodeBatch([]ProduceRecord{{TimestampMs: 1000, Key: "k", Value: bigValue}})
	t.Partitions = append(t.Partitions, p)
	r.Topics = append(r.Topics, t)
	return r
}

// The request is sent twice: the second answer shows whether the first failure
// was the end of the broker.
var diskFullScenarios = []diskFullScenario{
	{name: "produce", run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		e.once("produce-acks-all", produceOne("disk", -1), 9)
		e.once("produce-acks-1", produceOne("disk", 1), 9)
		e.brokerAfter("broker-after")
	}},
	{name: "offset-commit", run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		e.once("offset-commit", commitRequestFor(bigGroup, "disk", 1), 8)
		e.brokerAfter("broker-after")
	}},
	{name: "offset-delete", known: []Known{{Step: "offset-delete", Limitation: groupsOnDisk}}, run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		r := kmsg.NewPtrOffsetDeleteRequest()
		r.Group = bigGroup
		t := kmsg.NewOffsetDeleteRequestTopic()
		t.Topic = "disk"
		p := kmsg.NewOffsetDeleteRequestTopicPartition()
		t.Partitions = append(t.Partitions, p)
		r.Topics = append(r.Topics, t)
		e.once("offset-delete", r, 0)
		e.brokerAfter("broker-after")
	}},
	{name: "delete-groups", known: []Known{{Step: "delete-groups", Limitation: groupsOnDisk}}, run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		r := kmsg.NewPtrDeleteGroupsRequest()
		r.Groups = []string{bigGroup}
		e.once("delete-groups", r, 2)
		e.brokerAfter("broker-after")
	}},
	{name: "init-producer-id", run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		r := kmsg.NewPtrInitProducerIDRequest()
		r.TransactionTimeoutMillis, r.ProducerID, r.ProducerEpoch = 60000, -1, -1
		e.once("init-idempotent", r, 3)
		other := big("tx-new-", 4086)
		r2 := kmsg.NewPtrInitProducerIDRequest()
		r2.TransactionalID, r2.TransactionTimeoutMillis, r2.ProducerID, r2.ProducerEpoch = &other, 60000, -1, -1
		e.once("init-transactional", r2, 3)
		e.brokerAfter("broker-after")
	}},
	{name: "add-partitions-to-txn", run: func(e *Env) {
		t := diskFullSetup(e)
		e.fillDisk("fill")
		e.once("add-partitions", t.addRequest("disk"), 3)
		e.brokerAfter("broker-after")
	}},
	{name: "add-offsets-to-txn", run: func(e *Env) {
		t := diskFullSetup(e)
		e.fillDisk("fill")
		r := kmsg.NewPtrAddOffsetsToTxnRequest()
		r.TransactionalID, r.ProducerID, r.ProducerEpoch, r.Group = t.id, t.pid, t.epoch, bigGroup
		e.once("add-offsets", r, 3)
		e.brokerAfter("broker-after")
	}},
	{name: "txn-offset-commit", run: func(e *Env) {
		t := diskFullSetup(e)
		ao := kmsg.NewPtrAddOffsetsToTxnRequest()
		ao.TransactionalID, ao.ProducerID, ao.ProducerEpoch, ao.Group = t.id, t.pid, t.epoch, bigGroup
		e.Do("add-offsets-ok", ao, 3)
		e.fillDisk("fill")
		r := kmsg.NewPtrTxnOffsetCommitRequest()
		r.TransactionalID, r.Group, r.ProducerID, r.ProducerEpoch, r.Generation = t.id, bigGroup, t.pid, t.epoch, -1
		rt := kmsg.NewTxnOffsetCommitRequestTopic()
		rt.Topic = "disk"
		rp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
		rp.Partition, rp.Offset, rp.LeaderEpoch, rp.Metadata = 0, 1, -1, &bigMeta
		rt.Partitions = append(rt.Partitions, rp)
		r.Topics = append(r.Topics, rt)
		e.once("txn-offset-commit", r, 3)
		e.brokerAfter("broker-after")
	}},
	{name: "end-txn", run: func(e *Env) {
		t := diskFullSetup(e)
		t.add("add-ok", "disk")
		t.produce("txn-produce-ok", "disk", "in a transaction")
		e.fillDisk("fill")
		t.end("end-commit", true)
		e.brokerAfter("broker-after")
	}},
	{name: "create-topics", known: []Known{{Step: "create-topics", Limitation: registryOnDisk}, {Step: "produce-to-the-new-topic", Limitation: registryOnDisk}}, run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		e.once("create-topics", createTopics(false, newTopic("disk-2", 1, 1)), 7)
		e.Wait(3 * time.Second) // the controller has said yes; the broker has yet to make the log.
		e.once("produce-to-the-new-topic", produceOne("disk-2", -1), 9)
		e.brokerAfter("broker-after")
	}},
	{name: "delete-topics", known: []Known{{Step: "delete-topics", Limitation: registryOnDisk}}, run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		r := kmsg.NewPtrDeleteTopicsRequest()
		r.TopicNames = []string{"disk"}
		r.TimeoutMillis = 15000
		e.once("delete-topics", r, 3)
		e.brokerAfter("broker-after")
	}},
	{name: "create-partitions", known: []Known{{Step: "create-partitions", Limitation: registryOnDisk}}, run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		r := kmsg.NewPtrCreatePartitionsRequest()
		t := kmsg.NewCreatePartitionsRequestTopic()
		t.Topic, t.Count = "disk", 3
		r.Topics = append(r.Topics, t)
		r.TimeoutMillis = 15000
		e.once("create-partitions", r, 3)
		e.brokerAfter("broker-after")
	}},
	{name: "alter-configs", known: []Known{{Step: "incremental-alter-configs", Limitation: registryOnDisk}}, run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		r := kmsg.NewPtrIncrementalAlterConfigsRequest()
		res := kmsg.NewIncrementalAlterConfigsRequestResource()
		res.ResourceType, res.ResourceName = kmsg.ConfigResourceTypeTopic, "disk"
		c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		c.Name, c.Op, c.Value = "retention.ms", kmsg.IncrementalAlterConfigOpSet, str("3600000")
		res.Configs = append(res.Configs, c)
		r.Resources = append(r.Resources, res)
		e.once("incremental-alter-configs", r, 1)
		e.brokerAfter("broker-after")
	}},
	{name: "delete-records", known: []Known{{Step: "delete-records", Limitation: registryOnDisk}}, run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		r := kmsg.NewPtrDeleteRecordsRequest()
		t := kmsg.NewDeleteRecordsRequestTopic()
		t.Topic = "disk"
		p := kmsg.NewDeleteRecordsRequestTopicPartition()
		p.Partition, p.Offset = 0, 1
		t.Partitions = append(t.Partitions, p)
		r.Topics = append(r.Topics, t)
		r.TimeoutMillis = 15000
		e.once("delete-records", r, 2)
		e.brokerAfter("broker-after")
	}},
	{name: "metadata-auto-create", run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		e.once("metadata-auto-create", metadata(true, "made-by-metadata"), 9)
		e.brokerAfter("broker-after")
	}},
	{name: "consumer-group-heartbeat", known: []Known{{Step: "consumer-group-join", Limitation: groupsOnDisk}}, run: func(e *Env) {
		diskFullSetup(e)
		e.fillDisk("fill")
		r := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		r.Group, r.MemberEpoch, r.RebalanceTimeoutMillis = big("cg-", 4090), 0, 30000
		r.MemberID = "AAAAAAAAAAAAAAAAAAAAAA"
		r.SubscribedTopicNames = []string{"disk"}
		r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
		e.once("consumer-group-join", r, 0)
		e.brokerAfter("broker-after")
	}},
}
