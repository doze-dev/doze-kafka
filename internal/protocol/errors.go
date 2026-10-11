package protocol

// What an error code means, and which of the four words it is.
//
// The wire answers with a number. A person looking at a request wants to know three
// things the number does not say: is this normal (a classic consumer's first join is
// answered MEMBER_ID_REQUIRED on purpose), is it the app's doing or the broker's, and
// what to do about it. This table is that, in one place beside ErrorName, so the
// Traffic page, its error counts, its explanations and the console's refusals read the
// same classification and cannot disagree.
//
// A row exists for every error the broker itself returns (TestEveryErrorTheBrokerReturnsHasARow
// walks internal/broker for them). A code with no row is still named (ErrorName) and
// still classed (by Kafka's own retriable flag), and is explained by Kafka's own
// description only.

import (
	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Class says what an error code means about the request that got it.
type Class string

const (
	// ClassOK is no error.
	ClassOK Class = ""
	// ClassExpected is a code that is part of a normal flow, not a problem: the first
	// JoinGroup of a classic consumer, a heartbeat that tells a member to rejoin. It is
	// shown, never counted as an error.
	ClassExpected Class = "expected"
	// ClassRefused is a no that real Kafka would also give: a topic that does not exist,
	// a fenced producer, an offset out of range.
	ClassRefused Class = "refused"
	// ClassDenied is a no an ACL or a login decided.
	ClassDenied Class = "denied"
	// ClassNotImplemented is a no for a feature or version this broker does not do.
	ClassNotImplemented Class = "not-implemented"
	// ClassFailed is the broker being unhappy: storage, a timeout, a bug.
	ClassFailed Class = "failed"
)

// IsError reports whether the class counts as an error: everything but ok and expected.
func (c Class) IsError() bool { return c != ClassOK && c != ClassExpected }

// ErrorInfo explains one error code.
type ErrorInfo struct {
	Code  int16
	Name  string
	Class Class
	// Meaning is Kafka's own description of the code.
	Meaning string
	// Cause is the usual reason an app gets it, and Fix what to do about it. Both are
	// empty for a code with no row here.
	Cause, Fix string
}

type errorRow struct {
	err        *kerr.Error
	class      Class
	cause, fix string
}

// errorTable is every error the broker returns, with its class, its usual cause and
// the fix. The class of an expected flow is by code alone here; Classify narrows it by
// the API where the same code elsewhere would be a problem.
var errorTable = []errorRow{
	// The request is not valid.
	{kerr.InvalidRequest, ClassRefused, "A field is missing or out of range for the version the client spoke.", "Compare the client library's version with the broker's, and the request it was building."},
	{kerr.UnknownTopicOrPartition, ClassRefused, "The app names a topic this broker does not have, or a partition past the topic's last.", "Check the name for a typo, create the topic, or let the producer's first Metadata create it."},
	{kerr.UnknownTopicID, ClassRefused, "A request named a topic by an id that no topic has now: it was deleted and made again.", "Nothing: the client refreshes its metadata and asks again."},
	{kerr.TopicAlreadyExists, ClassRefused, "CreateTopics for a name that is already a topic.", "Treat it as done, or choose another name."},
	{kerr.InvalidTopicException, ClassRefused, "The topic name is empty, is '.' or '..', is longer than 249 bytes, or has a character a name may not.", "Use letters, digits, '.', '_' and '-'."},
	{kerr.InvalidPartitions, ClassRefused, "A partition count below 1, or a CreatePartitions that would leave the topic with fewer than it has.", "Partitions can be added, never removed."},
	{kerr.InvalidReplicationFactor, ClassRefused, "A replication factor below 1.", "Use 1, or any higher number: this broker keeps one replica either way."},
	{kerr.InvalidReplicaAssignment, ClassRefused, "A manual replica assignment that names a broker this one is not.", "Leave the assignment out."},
	{kerr.InvalidConfig, ClassRefused, "A topic setting that is not one, or a value out of its range.", "See the topic's Config tab for the settings and their ranges."},
	{kerr.InvalidRequiredAcks, ClassRefused, "A producer's acks that is not -1, 0 or 1.", "Set acks to all, 1 or 0."},
	{kerr.MessageTooLarge, ClassRefused, "A batch larger than the topic's max.message.bytes.", "Send smaller batches, or raise max.message.bytes on the topic."},
	{kerr.InvalidRecord, ClassRefused, "A record the topic will not take: on a compacted topic, one without a key. For a telemetry push, a body that does not decompress or inflates past the size the broker said it would take.", "Give every record on a compacted topic a key. A client that pushes metrics must compress them as the codec it names."},
	{kerr.CorruptMessage, ClassRefused, "A record batch whose checksum or framing does not match.", "A client or proxy altered the bytes: look at what sits between the app and the broker."},
	{kerr.OffsetOutOfRange, ClassRefused, "A consumer asked for an offset the log no longer has, or has not reached.", "The consumer resets by its auto.offset.reset; check retention if it keeps happening."},
	{kerr.OffsetMetadataTooLarge, ClassRefused, "The metadata string on an offset commit is too long.", "Commit shorter metadata or none."},
	{kerr.UnknownLeaderEpoch, ClassRefused, "A fetch carried a leader epoch newer than the partition's.", "Nothing: the client refreshes its metadata."},
	{kerr.FetchSessionIDNotFound, ClassRefused, "A fetch used a session the broker no longer holds (it was evicted, or the broker restarted).", "Nothing: the client starts a new session."},

	// Groups.
	{kerr.GroupIDNotFound, ClassRefused, "The group has no state here: it never joined, or was deleted or expired.", "Start a consumer in the group, or check the group id."},
	{kerr.InvalidGroupID, ClassRefused, "A group id that is empty.", "Set group.id in the consumer."},
	{kerr.UnknownMemberID, ClassRefused, "The member is not in the group: its session expired, or the group was deleted.", "The consumer rejoins on its own; if it keeps happening, its poll loop is slower than max.poll.interval.ms."},
	{kerr.IllegalGeneration, ClassRefused, "A commit or heartbeat carried a generation the group has moved past.", "The member was in an earlier rebalance. It rejoins; commits made in the old generation are lost, so processing must be idempotent."},
	{kerr.RebalanceInProgress, ClassExpected, "The group is changing generation and the member must rejoin.", "Nothing: the client rejoins."},
	{kerr.MemberIDRequired, ClassExpected, "A classic consumer's first JoinGroup is answered with the id it must join with (KIP-394).", "Nothing: the client joins again with it."},
	{kerr.FencedInstanceID, ClassRefused, "Another consumer joined with the same group.instance.id.", "Give each static member its own group.instance.id."},
	{kerr.UnreleasedInstanceID, ClassRefused, "A static member's id is still held by its earlier session.", "Wait for the old session to expire, or stop the old process."},
	{kerr.InconsistentGroupProtocol, ClassRefused, "The member offers no assignment strategy the group's members all share.", "Use the same partition.assignment.strategy in every consumer of the group."},
	{kerr.InvalidSessionTimeout, ClassRefused, "A session.timeout.ms outside the broker's group.min and group.max session timeout.", "Use a value inside the range the Cluster page shows."},
	{kerr.NonEmptyGroup, ClassRefused, "A group with members cannot be deleted.", "Stop its consumers first."},
	{kerr.GroupSubscribedToTopic, ClassRefused, "The group is still subscribed to a topic whose offsets were to be deleted.", "Stop the consumers, or unsubscribe them."},
	{kerr.UnsupportedAssignor, ClassNotImplemented, "A server-side assignor this broker does not have.", "Use 'uniform' or 'range'."},
	{kerr.StaleMemberEpoch, ClassRefused, "A KIP-848 heartbeat carried an epoch older than the group's.", "Nothing: the client rejoins."},
	{kerr.FencedMemberEpoch, ClassRefused, "A KIP-848 member was replaced by one with a newer epoch.", "Nothing: the client rejoins."},
	{kerr.InvalidRegularExpression, ClassRefused, "A subscription pattern that is not a valid regular expression.", "Fix the pattern."},

	// Transactions and idempotence.
	{kerr.InvalidTxnState, ClassRefused, "The transaction is not in a state this request is allowed in.", "Begin a transaction before producing, and finish it before beginning another."},
	{kerr.InvalidProducerEpoch, ClassRefused, "A request from a producer epoch the transactional id has moved past.", "Another producer took the transactional id over. Only one instance may use it."},
	{kerr.ProducerFenced, ClassRefused, "A newer producer has this transactional id.", "Run one producer per transactional id; the fenced one must stop."},
	{kerr.InvalidProducerIDMapping, ClassRefused, "The producer id is not the one the transactional id maps to.", "The producer re-initialises on its own."},
	{kerr.OutOfOrderSequenceNumber, ClassRefused, "An idempotent producer skipped a sequence number.", "The producer's earlier batch was lost or refused; it resets its sequence."},
	{kerr.DuplicateSequenceNumber, ClassRefused, "An idempotent producer re-sent a batch the broker already has.", "Nothing: the batch is already written."},
	{kerr.InvalidTransactionTimeout, ClassRefused, "A transaction.timeout.ms above the broker's maximum.", "Use a value at or under transaction.max.timeout.ms."},
	{kerr.ConcurrentTransactions, ClassRefused, "The previous transaction of this id is still completing.", "Nothing: the producer retries."},
	{kerr.TransactionalIDNotFound, ClassRefused, "No transaction state for this id.", "Check the transactional id."},
	{kerr.OperationNotAttempted, ClassRefused, "The operation was not tried because an earlier one in the same request failed.", "Look at the first error in the request."},

	// Security.
	{kerr.TopicAuthorizationFailed, ClassDenied, "The principal has no ACL that allows this operation on the topic.", "Add an ACL for the principal, or use another user."},
	{kerr.GroupAuthorizationFailed, ClassDenied, "The principal has no ACL that allows this operation on the group.", "Add an ACL for the principal on the group, or use another user."},
	{kerr.ClusterAuthorizationFailed, ClassDenied, "The principal has no ACL that allows this cluster-level operation.", "Add a Cluster ACL for the principal."},
	{kerr.TransactionalIDAuthorizationFailed, ClassDenied, "The principal has no ACL on this transactional id.", "Add an ACL for the principal on the transactional id."},
	{kerr.SaslAuthenticationFailed, ClassDenied, "The username or password was refused.", "Check the credentials the client was given."},
	{kerr.UnsupportedSaslMechanism, ClassNotImplemented, "A SASL mechanism this broker does not offer.", "Use PLAIN or SCRAM-SHA-256 or -512, as the listener offers."},
	{kerr.IllegalSaslState, ClassRefused, "A SASL message that arrived in the wrong order.", "A client bug or a proxy in the way."},

	// The broker.
	{kerr.UnknownServerError, ClassFailed, "The broker failed in a way it has no name for.", "Look at the broker's log at this time."},
	{kerr.KafkaStorageError, ClassFailed, "The broker could not write or read its data directory.", "Check the disk: space, permissions, a lock. The Cluster page shows the data directory."},
	{kerr.CoordinatorNotAvailable, ClassFailed, "The group or transaction coordinator is not ready.", "Retry; if it persists, check the broker's log."},
	{kerr.UnsupportedVersion, ClassNotImplemented, "The client asked for a version of an API this broker does not offer.", "Use a client that negotiates versions with ApiVersions, or a newer broker profile."},
	{kerr.UnsupportedCompressionType, ClassNotImplemented, "A compression type this broker does not take for this purpose.", "Use gzip, snappy, lz4, zstd or none."},
	{kerr.SecurityDisabled, ClassNotImplemented, "A security feature (delegation tokens, an authorizer call) that this broker does not have turned on.", "Use the listener's SASL settings instead; see the Access page."},
	{kerr.UnsupportedForMessageFormat, ClassNotImplemented, "A request for something the record format in use cannot do.", "Use a newer message format or a different request."},
	{kerr.UnsupportedEndpointType, ClassNotImplemented, "A request for an endpoint type this broker does not offer.", "Use the broker listener."},
	{kerr.DelegationTokenAuthDisabled, ClassNotImplemented, "Delegation tokens are not a thing this broker issues.", "Use SASL/SCRAM credentials."},
	{kerr.DelegationTokenAuthorizationFailed, ClassDenied, "The principal may not use delegation tokens.", "Use SASL/SCRAM credentials."},
	{kerr.UnknownSubscriptionID, ClassRefused, "A telemetry push for a subscription the broker did not give this client instance (Apache Kafka answers so after a CLIENT_METRICS resource changed the subscription).", "Nothing: the client subscribes again."},
	{kerr.ThrottlingQuotaExceeded, ClassExpected, "A telemetry request that came before the push interval had passed: a subscription request or a push, one interval after the last one taken (KIP-714).", "Nothing: the client waits out its interval and pushes again."},
	{kerr.TelemetryTooLarge, ClassRefused, "A telemetry push larger than the broker said it would take.", "Nothing: the client sends less next time."},
}

var errorRows = func() map[int16]*errorRow {
	m := make(map[int16]*errorRow, len(errorTable))
	for i := range errorTable {
		m[errorTable[i].err.Code] = &errorTable[i]
	}
	return m
}()

// CodeClass is the class of a code with nothing else known: the table's, or for a code
// with no row Kafka's own retriable flag (a retriable error is the broker's trouble).
// 0 is ClassOK.
func CodeClass(code int16) Class {
	if code == 0 {
		return ClassOK
	}
	if r := errorRows[code]; r != nil {
		return r.class
	}
	if e := kerr.TypedErrorForCode(code); e != nil && e.Code == code && e.Retriable {
		return ClassFailed
	}
	return ClassRefused
}

// Classify is the class of an API's answer: CodeClass, except where the code is the
// normal reply to this API (ApiVersions answering a newer version with
// UNSUPPORTED_VERSION is how the client learns what to speak).
func Classify(api APIKey, code int16) Class {
	if api == kmsg.ApiVersions && code == kerr.UnsupportedVersion.Code {
		return ClassExpected
	}
	return CodeClass(code)
}

// Explain is everything the table knows about a code. The name and meaning are
// Kafka's own for a code with no row.
func Explain(code int16) ErrorInfo {
	info := ErrorInfo{Code: code, Name: ErrorName(code), Class: CodeClass(code)}
	if e := kerr.TypedErrorForCode(code); e != nil && e.Code == code {
		info.Meaning = e.Description
	}
	if r := errorRows[code]; r != nil {
		info.Cause, info.Fix = r.cause, r.fix
	}
	return info
}

// ErrorCodes lists the codes that have a row, in the table's order.
func ErrorCodes() []int16 {
	out := make([]int16, len(errorTable))
	for i, r := range errorTable {
		out[i] = r.err.Code
	}
	return out
}

// errorPhrases are the short words a row's Detail says for the errors people meet
// first: what the refusal is, in the console's table (Traf-03, Main). A code with no
// phrase has none: the Result column names it and the inspector explains it.
var errorPhrases = map[int16]string{
	kerr.UnknownTopicOrPartition.Code:            "no such topic",
	kerr.TopicAlreadyExists.Code:                 "the topic already exists",
	kerr.InvalidTopicException.Code:              "not a valid topic name",
	kerr.InvalidPartitions.Code:                  "not a valid partition count",
	kerr.InvalidReplicationFactor.Code:           "replication factor is more than the brokers",
	kerr.InvalidConfig.Code:                      "a config value is not valid",
	kerr.TopicAuthorizationFailed.Code:           "not authorised for the topic",
	kerr.GroupAuthorizationFailed.Code:           "not authorised to use the group",
	kerr.ClusterAuthorizationFailed.Code:         "not authorised for the cluster",
	kerr.TransactionalIDAuthorizationFailed.Code: "not authorised for the transactional id",
	kerr.ProducerFenced.Code:                     "a newer producer has this id",
	kerr.UnknownMemberID.Code:                    "the group does not know this member",
	kerr.IllegalGeneration.Code:                  "the member's generation is old",
	kerr.RebalanceInProgress.Code:                "the group is rebalancing",
	kerr.MemberIDRequired.Code:                   "a new member asks in; it is given an id to join with",
	kerr.OffsetOutOfRange.Code:                   "the offset is outside the log",
	kerr.NotLeaderForPartition.Code:              "this broker is not the leader",
	kerr.GroupIDNotFound.Code:                    "no such group",
	kerr.CoordinatorNotAvailable.Code:            "the coordinator is not ready",
}

// ErrorPhrase is the short phrase for a code, or "" when there is none.
func ErrorPhrase(code int16) string { return errorPhrases[code] }
