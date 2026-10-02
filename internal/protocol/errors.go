package protocol

// ErrorCode is a Kafka protocol error code, carried inside response bodies (not
// to be confused with Go errors). Clients drive their state machines off these,
// so returning the right one matters; see docs/api-support.md.
type ErrorCode int16

const (
	ErrNone                ErrorCode = 0
	ErrOffsetOutOfRange    ErrorCode = 1
	ErrUnknownTopicOrPart  ErrorCode = 3
	ErrRebalanceInProgress ErrorCode = 27
	ErrUnsupportedVersion  ErrorCode = 35
	ErrUnknownMemberID     ErrorCode = 25
	ErrIllegalGeneration   ErrorCode = 22
	ErrMemberIDRequired    ErrorCode = 79
	ErrInvalidTopic        ErrorCode = 17 // INVALID_TOPIC_EXCEPTION: the name breaks Kafka's rules.
	ErrInvalidPartitions   ErrorCode = 37
	ErrTopicAlreadyExists  ErrorCode = 36
	ErrUnknownServer       ErrorCode = -1
	ErrUnknownTopicID      ErrorCode = 100
	ErrInvalidRequest      ErrorCode = 42

	// Idempotent producer (sequence-number deduplication).
	ErrOutOfOrderSequence   ErrorCode = 45 // a gap in the producer's sequence numbers.
	ErrDuplicateSequence    ErrorCode = 46 // a duplicate the broker no longer has the offset for.
	ErrInvalidProducerEpoch ErrorCode = 47 // a batch from a fenced (older) producer epoch.

	// SASL / authentication.
	ErrUnsupportedSASLMechanism ErrorCode = 33 // handshake named a mechanism we don't offer.
	ErrIllegalSASLState         ErrorCode = 34 // a SASL request arrived out of sequence.
	ErrSASLAuthenticationFailed ErrorCode = 58 // credentials rejected.

	// KIP-848 consumer group protocol.
	ErrGroupIDNotFound     ErrorCode = 69  // ConsumerGroupDescribe of an unknown group.
	ErrFencedMemberEpoch   ErrorCode = 110 // member's epoch is fenced; it must abandon partitions and rejoin.
	ErrUnsupportedAssignor ErrorCode = 112 // the requested server assignor is not offered.
	ErrStaleMemberEpoch    ErrorCode = 113 // member's epoch is stale; retry after the coordinator updates it.
)
