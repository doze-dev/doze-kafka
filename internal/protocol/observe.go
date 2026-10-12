package protocol

// Watching the wire.
//
// An Observer is told about every connection and every request that was answered:
// who sent it, what it was about, how it went and how long it took. It exists so the
// console can answer "what did my app just do?" without the console or the recorder
// reaching into the broker.
//
// What a request was ABOUT (its topics, its group) is not known by the connection
// loop, which sees bytes; it is known by whoever decoded the request. So handlers
// decode through Request.Decode and answer through Request.Reply, which keep the
// decoded pair, and the loop asks the API table (APIs, Summary) to describe it. A new
// API gets a description by being a row in that table; a test fails for a row with none.

import (
	"strconv"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Summary is what one request was about, in terms a person reads.
type Summary struct {
	// Topics are the topics named in the request, by name.
	Topics []string
	// TopicIDs are topics the request named only by id (Fetch v13, for one); the
	// observer resolves them, since only it knows the names.
	TopicIDs [][16]byte
	// Group is the consumer group the request was for.
	Group string
	// TxnID is the transactional id, for the requests that name one. It is not a
	// group: the recorder used to list it as one and link it to a page that does not
	// exist (TRF-10).
	TxnID string
	// Kind is the family the request belongs to, for the console's filter: "produce",
	// "fetch", "groups", "admin", or "" for the rest (Kinds).
	Kind string
	// Idle marks a request that moved nothing: a long-poll fetch that timed out
	// empty, a heartbeat. Repeats of these collapse into one row.
	Idle bool
	// Detail is a short phrase: "2 partitions, 14 records".
	Detail string
	// Software is "name version" from ApiVersions, when the client says.
	Software string
	// ErrorCode is the first non-zero error in the response, 0 if none.
	ErrorCode int16
	// Class says what the error means (Classify): normal flow, a refusal Kafka would
	// also make, a denial, something not implemented, or the broker failing.
	Class Class
}

// ErrorName is the Kafka name of an error code ("UNKNOWN_TOPIC_OR_PARTITION"), or
// "" for 0.
func ErrorName(code int16) string {
	if code == 0 {
		return ""
	}
	// TypedErrorForCode answers UNKNOWN_SERVER_ERROR for a number it has no name for;
	// that is not this code's name.
	if e := kerr.TypedErrorForCode(code); e != nil && e.Code == code {
		return e.Message
	}
	return "ERROR_" + strconv.Itoa(int(code))
}

// Observation is one answered request.
type Observation struct {
	Conn      uint64
	Remote    string
	Principal string
	ClientID  string
	API       APIKey
	Version   int16
	Start     time.Time
	Duration  time.Duration
	ReqBytes  int
	RespBytes int
	// Correlation is the request's correlation id: what the client's own log carries.
	Correlation int32
	// Closed, when set, says the broker closed the connection because of this request
	// and why ("API key 99 is not served"). The answer, if any, was never sent.
	Closed string
	// Req and Resp are what the handler decoded and answered with, for an observer that
	// wants to read more of them (Facts). They are read-only and must not be kept.
	Req  kmsg.Request
	Resp kmsg.Response
	Summary
}

// Observer receives what the wire does. Its methods are called from connection
// goroutines, so they must be quick and safe for concurrent use.
type Observer interface {
	ConnOpened(id uint64, remote string)
	ConnClosed(id uint64)
	Request(Observation)
}

// SetObserver makes o hear every connection and request. Call before Serve.
func (s *Server) SetObserver(o Observer) { s.observer = o }

// Decode decodes the request body, keeping the result so an observer can describe it.
func (r *Request) Decode() (kmsg.Request, error) {
	q, err := DecodeRequest(r.Header, r.Body)
	if err == nil {
		r.decoded = q
	}
	return q, err
}

// Reply frames resp for the wire and keeps it so an observer can read the outcome.
func (r *Request) Reply(resp kmsg.Response) []byte {
	r.answered = resp
	return EncodeResponse(r.Header, resp)
}

// ReplySized is Reply with a capacity hint for the encoded response (see
// EncodeResponseSized).
func (r *Request) ReplySized(resp kmsg.Response, sizeHint int) []byte {
	r.answered = resp
	return EncodeResponseSized(r.Header, resp, sizeHint)
}

// Noted lets a handler that answers without Reply (one that sends nothing: a Produce
// with acks=0) still say what it decoded.
func (r *Request) Noted(q kmsg.Request, resp kmsg.Response) { r.decoded, r.answered = q, resp }

// Describe is what the API table says this request was about. Safe with a nil
// request or response (a handler that failed, an acks=0 produce).
//
//fuzz:parser FuzzDescribe
func (r *Request) Describe() Summary {
	var s Summary
	for i := range APIs {
		if APIs[i].Key == r.Header.APIKey {
			if f := APIs[i].Summary; f != nil && r.decoded != nil {
				s = f(r.decoded, r.answered)
			}
			break
		}
	}
	s.ErrorCode = firstError(r.answered)
	// Answering a newer ApiVersions than we know with UNSUPPORTED_VERSION is how every
	// client learns what to speak, not a failure: listing it as an error put one on
	// every new connection of every client newer than the broker.
	if r.Header.APIKey == kmsg.ApiVersions && s.ErrorCode == kerr.UnsupportedVersion.Code {
		s.ErrorCode = 0
		s.Detail = "version negotiation: the client asks again at a version offered here"
	}
	s.Class = Classify(r.Header.APIKey, s.ErrorCode)
	// A refused request with nothing else to say says why, in a phrase.
	if s.Detail == "" && s.ErrorCode != 0 && s.Class != ClassExpected {
		s.Detail = ErrorPhrase(s.ErrorCode)
	}
	s.Kind = Kind(r.Header.APIKey)
	return s
}

// Kind is the family of an API, for the console's filter chips: "produce", "fetch",
// "groups" (the consumer group protocols, offsets and coordinator lookup), "admin"
// (topics, configs, ACLs, cluster questions), or "" for everything else (metadata,
// version negotiation, producer ids, transactions, telemetry), which is listed under
// "all" only.
func Kind(api APIKey) string {
	switch api {
	case kmsg.Produce:
		return "produce"
	case kmsg.Fetch, kmsg.ShareFetch:
		return "fetch"
	case kmsg.JoinGroup, kmsg.SyncGroup, kmsg.Heartbeat, kmsg.LeaveGroup, kmsg.OffsetCommit, kmsg.OffsetFetch,
		kmsg.OffsetDelete, kmsg.FindCoordinator, kmsg.ConsumerGroupHeartbeat, kmsg.ConsumerGroupDescribe,
		kmsg.DescribeGroups, kmsg.ListGroups, kmsg.DeleteGroups, kmsg.TxnOffsetCommit:
		return "groups"
	case kmsg.CreateTopics, kmsg.DeleteTopics, kmsg.CreatePartitions, kmsg.AlterConfigs, kmsg.IncrementalAlterConfigs,
		kmsg.DescribeConfigs, kmsg.CreateACLs, kmsg.DeleteACLs, kmsg.DescribeACLs, kmsg.DeleteRecords, kmsg.DescribeCluster,
		kmsg.DescribeLogDirs, kmsg.ListTransactions, kmsg.DescribeTransactions, kmsg.DescribeProducers:
		return "admin"
	}
	return ""
}
