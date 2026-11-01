package broker

import (
	"context"
	"encoding/binary"
	"hash/crc32"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/telemetry"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Client telemetry (KIP-714): a client asks what the broker wants to know
// (GetTelemetrySubscriptions), then pushes its own metrics on the interval it was given
// (PushTelemetry), as an OpenTelemetry metrics payload. Recent Java and librdkafka
// clients do so by default as soon as the broker advertises the two APIs.
//
// Real Kafka subscribes a client to nothing until an operator defines a CLIENT_METRICS
// resource. Here the answer is always "everything the standard set offers, every five
// seconds", because the one reader of the metrics is the console on a developer's
// machine: there is no one to configure it, and a subscription nobody wrote down gives
// no data. The payloads are kept in memory for a few minutes and shown by the console;
// they are not exported anywhere.

const (
	// telemetryPushMillis is how often a client is told to push. Short, for a tool that
	// looks at the last few minutes; Kafka's default is five minutes.
	telemetryPushMillis = 5000
	// telemetryMaxBytes is the largest payload accepted, and the most one may inflate to:
	// Apache Kafka's default (telemetry.max.bytes), which a recording shows it advertising
	// and accepting up to. The frame limit for PushTelemetry is this and a header, so a
	// payload at the limit arrives, and one past it is answered TELEMETRY_TOO_LARGE rather
	// than dropped.
	telemetryMaxBytes = 1 << 20
	// telemetryMetrics is what is asked for: every metric of the standard set, which is
	// what the prefix selects (a client matches a subscription by name prefix).
	telemetryMetrics = "org.apache.kafka."
)

// The compression types a push may use, in the broker's order of preference. These
// are record batch codec numbers: 1 gzip, 2 snappy, 3 lz4, 4 zstd, as Apache Kafka
// lists them; a push with none (0) is accepted all the same, as it accepts one.
var telemetryCompression = []int8{4, 3, 1, 2}

// Telemetry is what clients have reported, or nil if --client-telemetry is off.
func (b *Broker) Telemetry() *telemetry.Store { return b.telemetry }

// subscriptionID is the id of the subscription an instance is given: a function of the
// instance id and of what it is subscribed to, as Apache Kafka's is (a checksum over
// the instance id, the metrics and the interval). It is the same after a restart, so a
// push that holds the id a client was given before one is accepted, which is what the
// recording shows real Kafka doing (conformance/recordings/telemetry-broker-restart.json).
func subscriptionID(instance [16]byte) int32 {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	h.Write(instance[:])
	h.Write([]byte(telemetryMetrics))
	var iv [4]byte
	binary.BigEndian.PutUint32(iv[:], telemetryPushMillis)
	h.Write(iv[:])
	return int32(h.Sum32())
}

func telemetryWho(req *protocol.Request) telemetry.Who {
	return telemetry.Who{
		ClientID: req.Header.ClientID, Software: req.Conn.ClientSoftware(),
		Principal: req.Conn.Principal(), Host: req.Conn.RemoteHost(),
		// An instance belongs to a principal, not to a connection (TRF-27, narrowed): a
		// client subscribes on one connection and pushes on another, so the connection
		// is only where the instance was last heard from. It is how the console links a
		// client's requests to its metrics.
		Conn: req.Conn.ID(), Open: req.Conn.Open,
	}
}

func (b *Broker) handleGetTelemetrySubscriptions(ctx context.Context, req *protocol.Request, q *kmsg.GetTelemetrySubscriptionsRequest, resp *kmsg.GetTelemetrySubscriptionsResponse) error {
	id := q.ClientInstanceID
	if id == ([16]byte{}) {
		id = telemetry.NewID() // the first time a client asks, it has no id yet: the broker gives it one.
	}
	switch b.telemetry.Subscribe(id, telemetryWho(req)) {
	case telemetry.Early:
		// Asked again before the interval had passed: Apache Kafka throttles a
		// subscription request as it does a push.
		resp.ErrorCode = kerr.ThrottlingQuotaExceeded.Code
		return nil
	}
	resp.ClientInstanceID = id
	resp.SubscriptionID = subscriptionID(id)
	resp.AcceptedCompressionTypes = append([]int8(nil), telemetryCompression...)
	resp.PushIntervalMillis = telemetryPushMillis
	resp.TelemetryMaxBytes = telemetryMaxBytes
	resp.DeltaTemporality = false
	resp.RequestedMetrics = []string{telemetryMetrics}
	return nil
}

// handlePushTelemetry answers a push in the order Apache Kafka 4.3.1 does (recorded:
// conformance/recordings/telemetry-errors.json, telemetry-throttle.json and
// telemetry-instance-binding-spaced.json): no instance id, then the gate (an instance
// the broker has not seen is made, and the push after a terminating one is refused
// once, and an early one is throttled), the subscription id, the compression type, the
// size, the body inflating. A body that is not OpenTelemetry metrics is not refused: a
// broker with no plugin to take them does not read them.
func (b *Broker) handlePushTelemetry(ctx context.Context, req *protocol.Request, q *kmsg.PushTelemetryRequest, resp *kmsg.PushTelemetryResponse) error {
	if q.ClientInstanceID == ([16]byte{}) {
		resp.ErrorCode = kerr.InvalidRequest.Code
		return nil
	}
	who := telemetryWho(req)
	switch b.telemetry.Arrive(q.ClientInstanceID, who, q.Terminating) {
	case telemetry.Early:
		resp.ErrorCode = kerr.ThrottlingQuotaExceeded.Code
		return nil
	case telemetry.AfterTerminating:
		resp.ErrorCode = kerr.InvalidRequest.Code
		return nil
	}
	switch {
	case q.SubscriptionID != subscriptionID(q.ClientInstanceID):
		// A subscription this instance was not given: the client is meant to ask again,
		// and its next subscription request is answered though it comes at once.
		resp.ErrorCode = kerr.UnknownSubscriptionID.Code
		b.telemetry.AskAgain(q.ClientInstanceID)
	case q.CompressionType < 0 || q.CompressionType > 4:
		resp.ErrorCode = kerr.UnsupportedCompressionType.Code
		b.telemetry.AskAgain(q.ClientInstanceID)
	case len(q.Metrics) > telemetryMaxBytes:
		resp.ErrorCode = kerr.TelemetryTooLarge.Code
	default:
		raw, err := log.Decompress(int(q.CompressionType), q.Metrics, telemetryMaxBytes)
		if err != nil {
			b.log.Debug("client telemetry refused: the payload does not decompress", "client_id", req.Header.ClientID, "compression", q.CompressionType, "bytes", len(q.Metrics), "err", err)
			resp.ErrorCode = kerr.InvalidRecord.Code
			return nil
		}
		payload, err := telemetry.Decode(raw)
		if err != nil {
			// Answered as received and kept as a push with nothing in it.
			b.log.Debug("client telemetry not read: the payload is not OTLP metrics", "client_id", req.Header.ClientID, "compression", q.CompressionType, "bytes", len(raw), "err", err)
			payload = telemetry.Payload{}
		}
		b.telemetry.Push(q.ClientInstanceID, who, payload, q.Terminating)
	}
	return nil
}
