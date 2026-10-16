package conformance

import (
	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Scenarios of requests a broker refuses for their shape (conformance/claims/api.md,
// rows V4 and V5): a transactional API with an empty transactional id, and an ACL
// API with a code that names nothing.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "txn-empty-id", Run: txnEmptyID, Known: knownTxnEmptyID},
		Scenario{Name: "acl-unknown-codes", Run: aclUnknownCodes, Known: knownACLCodes, Users: saslUsers, Admin: "admin",
			KafkaEnv: []string{
				"KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
				"KAFKA_SUPER_USERS=User:admin;User:ANONYMOUS",
			},
			Doze: func(c *config.Config) { c.ACLDefault, c.ACLSuperUsers = "deny", []string{"User:admin"} }},
	)
}

var (
	// Apache Kafka 4.3.1 answers COORDINATOR_LOAD_IN_PROGRESS to a TxnOffsetCommit
	// sent from outside the group (generation -1, no member id), on every try for
	// thirty seconds and whatever the transaction's state; doze-kafka answers for
	// the transaction's state. Open: see docs/limitations.md.
	knownTxnEmptyID = []Known{{Step: "txn-offset-commit-*", Path: "Topics[0].Partitions[0].ErrorCode", Limitation: txnOffsetCommitLimitation}}
	knownACLCodes   []Known
)

const txnOffsetCommitLimitation = "TxnOffsetCommit sent from outside the consumer group"

// txnEmptyID sends each transactional API with an empty transactional id, and with
// the id of a transaction nobody began, for contrast: the first is a malformed
// request, the second a well-formed one about nothing.
func txnEmptyID(e *Env) {
	e.Do("create-topic", createTopics(false, newTopic("tx", 1, 1)), 7)
	// The transaction coordinator, and the group coordinator TxnOffsetCommit goes to.
	for _, c := range []struct {
		label string
		typ   int8
		key   string
	}{{"find-txn-coordinator", 1, "nobody"}, {"find-group-coordinator", 0, "g"}} {
		fc := kmsg.NewPtrFindCoordinatorRequest()
		fc.CoordinatorType, fc.CoordinatorKeys = c.typ, []string{c.key}
		e.Do(c.label, fc, 4)
	}
	// The group's coordinator has loaded: an OffsetFetch for it is answered.
	of := kmsg.NewPtrOffsetFetchRequest()
	og := kmsg.NewOffsetFetchRequestGroup()
	og.Group = "g"
	of.Groups = append(of.Groups, og)
	e.Await("the coordinator of group g answers", of, 8, func(r kmsg.Response) bool {
		gs := r.(*kmsg.OffsetFetchResponse).Groups
		return len(gs) == 1 && gs[0].ErrorCode == 0
	})
	// And the transaction coordinator: it gives out a producer id.
	warm := kmsg.NewPtrInitProducerIDRequest()
	warm.TransactionalID, warm.TransactionTimeoutMillis, warm.ProducerID, warm.ProducerEpoch = str("warm-up"), 60000, -1, -1
	e.Await("the transaction coordinator gives out a producer id", warm, 4, func(r kmsg.Response) bool {
		return r.(*kmsg.InitProducerIDResponse).ErrorCode == 0
	})
	// From here every step is recorded as first answered (DoFirst): some of these
	// answers are themselves COORDINATOR_LOAD_IN_PROGRESS, and asking again for
	// thirty seconds each would outlast the transaction's timeout.
	txnCommit := func(label, id string, pid int64, epoch int16) {
		commit := kmsg.NewPtrTxnOffsetCommitRequest()
		commit.TransactionalID, commit.Group, commit.ProducerID, commit.ProducerEpoch = id, "g", pid, epoch
		commit.Generation, commit.MemberID = -1, ""
		ct := kmsg.NewTxnOffsetCommitRequestTopic()
		ct.Topic = "tx"
		cp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
		cp.Partition, cp.Offset = 0, 1
		ct.Partitions = append(ct.Partitions, cp)
		commit.Topics = append(commit.Topics, ct)
		e.DoFirst(label, commit, 3)
	}
	for _, id := range []struct{ tag, id string }{{"empty-id", ""}, {"unknown-id", "nobody"}} {
		end := kmsg.NewPtrEndTxnRequest()
		end.TransactionalID, end.ProducerID, end.ProducerEpoch, end.Commit = id.id, 1, 0, true
		e.DoFirst("end-txn-commit-"+id.tag, end, 3)
		abort := kmsg.NewPtrEndTxnRequest()
		abort.TransactionalID, abort.ProducerID, abort.ProducerEpoch, abort.Commit = id.id, 1, 0, false
		e.DoFirst("end-txn-abort-"+id.tag, abort, 3)
		e.DoFirst("end-txn-v0-"+id.tag, end, 0)

		off := kmsg.NewPtrAddOffsetsToTxnRequest()
		off.TransactionalID, off.ProducerID, off.ProducerEpoch, off.Group = id.id, 1, 0, "g"
		e.DoFirst("add-offsets-"+id.tag, off, 3)
		e.DoFirst("add-offsets-v0-"+id.tag, off, 0)
		noGroup := kmsg.NewPtrAddOffsetsToTxnRequest()
		noGroup.TransactionalID, noGroup.ProducerID, noGroup.ProducerEpoch = id.id, 1, 0
		e.DoFirst("add-offsets-no-group-"+id.tag, noGroup, 3)

		parts := kmsg.NewPtrAddPartitionsToTxnRequest()
		parts.TransactionalID, parts.ProducerID, parts.ProducerEpoch = id.id, 1, 0
		t := kmsg.NewAddPartitionsToTxnRequestTopic()
		t.Topic, t.Partitions = "tx", []int32{0}
		parts.Topics = append(parts.Topics, t)
		e.DoFirst("add-partitions-"+id.tag, parts, 3)

		txnCommit("txn-offset-commit-"+id.tag, id.id, 1, 0)
	}
	// InitProducerID: an empty id is not "no id" (that is null).
	init := kmsg.NewPtrInitProducerIDRequest()
	init.TransactionalID, init.TransactionTimeoutMillis, init.ProducerID, init.ProducerEpoch = str(""), 60000, -1, -1
	e.DoFirst("init-producer-id-empty-id", init, 4)

	// The same calls for a transactional id that has a producer, so that what the
	// rows above answer is known to be about the id and not about the call.
	real := kmsg.NewPtrInitProducerIDRequest()
	real.TransactionalID, real.TransactionTimeoutMillis, real.ProducerID, real.ProducerEpoch = str("tx-real"), 600000, -1, -1
	got := e.DoFirst("init-producer-id-real", real, 4).(*kmsg.InitProducerIDResponse)
	pid, epoch := got.ProducerID, got.ProducerEpoch
	txnCommit("txn-offset-commit-real-before-add-offsets", "tx-real", pid, epoch)
	noGroup := kmsg.NewPtrAddOffsetsToTxnRequest()
	noGroup.TransactionalID, noGroup.ProducerID, noGroup.ProducerEpoch = "tx-real", pid, epoch
	e.DoFirst("add-offsets-real-no-group", noGroup, 3)
	off := kmsg.NewPtrAddOffsetsToTxnRequest()
	off.TransactionalID, off.ProducerID, off.ProducerEpoch, off.Group = "tx-real", pid, epoch, "g"
	e.DoFirst("add-offsets-real", off, 3)
	txnCommit("txn-offset-commit-real-wrong-epoch", "tx-real", pid, epoch+5)
	txnCommit("txn-offset-commit-real-wrong-producer", "tx-real", pid+1000, epoch)
	txnCommit("txn-offset-commit-real", "tx-real", pid, epoch)
	end := kmsg.NewPtrEndTxnRequest()
	end.TransactionalID, end.ProducerID, end.ProducerEpoch, end.Commit = "tx-real", pid, epoch, true
	// Not the same EndTxn again: whether Kafka has finished writing the markers
	// (0) or not (CONCURRENT_TRANSACTIONS) is a race inside Kafka.
	e.DoFirst("end-txn-real", end, 3)
}

// aclUnknownCodes sends the three ACL APIs with each coded field (resource type,
// pattern type, operation, permission) in turn set to 0, which is UNKNOWN in every
// one of them, and to a number that is no code at all, as the super user of a
// broker with an authorizer.
func aclUnknownCodes(e *Env) {
	e.Open("admin", "admin", saslUsers["admin"])
	e.Use("admin")
	e.Do("create-topic", createTopics(false, newTopic("acl", 1, 1)), 7)

	type codes struct {
		tag                                      string
		resource, pattern, operation, permission int8
	}
	// Describe and delete take a filter: ANY (1) is a code there.
	filter := func(c codes) codes { return c }
	describe := func(c codes) {
		d := kmsg.NewPtrDescribeACLsRequest()
		d.ResourceType, d.ResourcePatternType = kmsg.ACLResourceType(c.resource), kmsg.ACLResourcePatternType(c.pattern)
		d.Operation, d.PermissionType = kmsg.ACLOperation(c.operation), kmsg.ACLPermissionType(c.permission)
		e.Do("describe-"+c.tag, d, 3)
		e.Do("describe-"+c.tag+"-then", metadata(false), 9)
	}
	remove := func(c codes) {
		d := kmsg.NewPtrDeleteACLsRequest()
		f := kmsg.NewDeleteACLsRequestFilter()
		f.ResourceType, f.ResourcePatternType = kmsg.ACLResourceType(c.resource), kmsg.ACLResourcePatternType(c.pattern)
		f.Operation, f.PermissionType = kmsg.ACLOperation(c.operation), kmsg.ACLPermissionType(c.permission)
		d.Filters = append(d.Filters, f)
		e.Do("delete-"+c.tag, d, 3)
		e.Do("delete-"+c.tag+"-then", metadata(false), 9)
	}
	for _, c := range []codes{
		filter(codes{"any", 1, 1, 1, 1}),
		filter(codes{"topic-literal", 2, 3, 1, 1}),
		filter(codes{"resource-0", 0, 3, 1, 1}),
		filter(codes{"pattern-0", 2, 0, 1, 1}),
		filter(codes{"operation-0", 2, 3, 0, 1}),
		filter(codes{"permission-0", 2, 3, 1, 0}),
		filter(codes{"resource-99", 99, 3, 1, 1}),
		filter(codes{"pattern-99", 2, 99, 1, 1}),
		filter(codes{"operation-99", 2, 3, 99, 1}),
		filter(codes{"permission-99", 2, 3, 1, 99}),
		filter(codes{"resource-negative", -1, 3, 1, 1}),
		filter(codes{"all-0", 0, 0, 0, 0}),
	} {
		describe(c)
		remove(c)
	}
	// Create takes an ACL: ANY is not one, and neither is the MATCH pattern (2).
	for _, c := range []codes{
		{"good", 2, 3, 3, 3},
		{"resource-0", 0, 3, 3, 3},
		{"resource-any", 1, 3, 3, 3},
		{"pattern-0", 2, 0, 3, 3},
		{"pattern-any", 2, 1, 3, 3},
		{"pattern-match", 2, 2, 3, 3},
		{"operation-0", 2, 3, 0, 3},
		{"operation-any", 2, 3, 1, 3},
		{"permission-0", 2, 3, 3, 0},
		{"permission-any", 2, 3, 3, 1},
		{"resource-99", 99, 3, 3, 3},
		{"pattern-99", 2, 99, 3, 3},
		{"operation-99", 2, 3, 99, 3},
		{"permission-99", 2, 3, 3, 99},
	} {
		cr := kmsg.NewPtrCreateACLsRequest()
		a := kmsg.NewCreateACLsRequestCreation()
		a.ResourceType, a.ResourceName, a.ResourcePatternType = kmsg.ACLResourceType(c.resource), "acl", kmsg.ACLResourcePatternType(c.pattern)
		a.Principal, a.Host = "User:alice", "*"
		a.Operation, a.PermissionType = kmsg.ACLOperation(c.operation), kmsg.ACLPermissionType(c.permission)
		cr.Creations = append(cr.Creations, a)
		e.Do("create-"+c.tag, cr, 3)
		e.Do("create-"+c.tag+"-then", metadata(false), 9)
	}
}
