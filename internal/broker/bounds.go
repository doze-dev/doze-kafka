package broker

import "github.com/doze-dev/doze-kafka/internal/storage/meta"

// What a client may make the broker keep.
//
// A request is bounded by its frame (1 MiB for everything but Produce), and that is
// the only bound Kafka's wire format puts on a string in a flexible request. A name is
// kept for as long as the thing it names: a group until it is deleted, a member until
// it leaves, a transactional id for ever, in memory and some of them on disk. So a
// megabyte in a name, thirty times, was thirty megabytes held, by anybody.
//
// These are the bounds, each enforced where the value would be stored, and each with
// a test that sends the most a frame allows (bounds_names_test.go):
//
//	a group id                                4,096 bytes   INVALID_GROUP_ID
//	a member id a client chose                4,096 + 64    INVALID_REQUEST
//	a group instance id, a rack id            4,096         INVALID_REQUEST
//	a protocol type, a protocol name          4,096         INVALID_REQUEST
//	a subscribed topic name or pattern        4,096         INVALID_REQUEST
//	a transactional id                        4,096         INVALID_REQUEST
//	the first message of a SCRAM login        4,096         SASL_AUTHENTICATION_FAILED
//	a server assignor                         one of two names, or UNSUPPORTED_ASSIGNOR
//	a SASL mechanism                          one that is enabled, or none is set
//	a principal                               a configured user's name
//	a client id (in the request header)       32,767: its length is an int16 on the wire
//	a client's software name and version      128, clipped (protocol.MaxClientSoftware)
//	a member's protocol metadata, assignment  the frame: Kafka's own bound, by its size
//
// Apache Kafka has none of the 4,096 bounds: it takes a name of any length its request
// size allows. They are this broker's, written down in docs/limitations.md, and far
// past anything a client sends (a group id is a few dozen bytes). What is shown to
// people (the Traffic page, the client metrics) is clipped separately, where it is
// stored (internal/traffic, internal/telemetry).
const (
	// maxNameBytes bounds a name a client gives for something the broker keeps.
	maxNameBytes = meta.MaxGroupIDBytes
	// maxMemberIDBytes is a little more: a static member's id is its instance id and a
	// suffix the broker adds, and it must be able to come back with the id it was given.
	maxMemberIDBytes = maxNameBytes + 64
	// maxSCRAMFirstBytes bounds the client-first message of a SCRAM exchange, which is
	// kept on the connection between the two steps: a user name and a nonce.
	maxSCRAMFirstBytes = 4096
)

// tooLong reports whether any of the names is past the bound.
func tooLong(names ...string) bool {
	for _, n := range names {
		if len(n) > maxNameBytes {
			return true
		}
	}
	return false
}

// deref is the string p points at, or "".
func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
