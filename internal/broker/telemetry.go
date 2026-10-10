package broker

import (
	"context"
	"crypto/rand"
	"encoding/binary"

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
	// telemetryMaxBytes is the largest payload accepted, and the most one may inflate to.
	// Half the largest request any API but Produce may be, so a payload at the limit still
	// arrives, and one past it is answered TELEMETRY_TOO_LARGE rather than dropped.
	telemetryMaxBytes = 512 << 10
	// telemetryMetrics is what is asked for: every metric of the standard set, which is
	// what the prefix selects (a client matches a subscription by name prefix).
	telemetryMetrics = "org.apache.kafka."
)

// The compression types a push may use, in the broker's order of preference. These
// are record batch codec numbers: 0 none, 1 gzip, 2 snappy, 3 lz4, 4 zstd.
var telemetryCompression = []int8{4, 1, 3, 2, 0}

// Telemetry is what clients have reported, or nil if --client-telemetry is off.
func (b *Broker) Telemetry() *telemetry.Store { return b.telemetry }

func newSubscriptionID() int32 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return int32(binary.BigEndian.Uint32(b[:]) & 0x7fffffff)
}

func telemetryWho(req *protocol.Request) telemetry.Who {
	return telemetry.Who{
		ClientID: req.Header.ClientID, Software: req.Conn.ClientSoftware(),
		Principal: req.Conn.Principal(), Host: req.Conn.RemoteHost(),
	}
}

func (b *Broker) handleGetTelemetrySubscriptions(ctx context.Context, req *protocol.Request, q *kmsg.GetTelemetrySubscriptionsRequest, resp *kmsg.GetTelemetrySubscriptionsResponse) error {
	id := q.ClientInstanceID
	if id == ([16]byte{}) {
		id = telemetry.NewID() // the first time a client asks, it has no id yet: the broker gives it one.
	}
	b.telemetry.Touch(id, telemetryWho(req))
	resp.ClientInstanceID = id
	resp.SubscriptionID = b.subscriptionID
	resp.AcceptedCompressionTypes = append([]int8(nil), telemetryCompression...)
	resp.PushIntervalMillis = telemetryPushMillis
	resp.TelemetryMaxBytes = telemetryMaxBytes
	resp.DeltaTemporality = false
	resp.RequestedMetrics = []string{telemetryMetrics}
	return nil
}

func (b *Broker) handlePushTelemetry(ctx context.Context, req *protocol.Request, q *kmsg.PushTelemetryRequest, resp *kmsg.PushTelemetryResponse) error {
	switch {
	case q.ClientInstanceID == ([16]byte{}):
		resp.ErrorCode = kerr.InvalidRequest.Code
	case q.SubscriptionID != b.subscriptionID || !b.telemetry.Known(q.ClientInstanceID):
		// A subscription from before a restart, or an id this broker never gave out: the
		// client is meant to ask again, and does.
		resp.ErrorCode = kerr.UnknownSubscriptionID.Code
	case q.CompressionType < 0 || q.CompressionType > 4:
		resp.ErrorCode = kerr.UnsupportedCompressionType.Code
	case len(q.Metrics) > telemetryMaxBytes:
		resp.ErrorCode = kerr.TelemetryTooLarge.Code
	default:
		raw, err := log.Decompress(int(q.CompressionType), q.Metrics, telemetryMaxBytes)
		if err != nil {
			b.log.Debug("client telemetry refused: the payload does not decompress", "client_id", req.Header.ClientID, "compression", q.CompressionType, "bytes", len(q.Metrics), "err", err)
			resp.ErrorCode = kerr.InvalidRequest.Code
			return nil
		}
		payload, err := telemetry.Decode(raw)
		if err != nil {
			b.log.Debug("client telemetry refused: the payload is not OTLP metrics", "client_id", req.Header.ClientID, "compression", q.CompressionType, "bytes", len(raw), "err", err)
			resp.ErrorCode = kerr.InvalidRequest.Code
			return nil
		}
		b.telemetry.Push(q.ClientInstanceID, telemetryWho(req), payload, q.Terminating)
	}
	return nil
}
