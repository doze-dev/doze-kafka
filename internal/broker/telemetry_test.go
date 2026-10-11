package broker_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
	"github.com/doze-dev/doze-kafka/internal/telemetry"
	"github.com/doze-dev/doze-kafka/server"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// What the broker tells a client to do (see telemetry.go).
const (
	pushMillis = 5000
	maxBytes   = 1 << 20
)

// bootServer is boot that also hands back the server, for a test that looks inside.
func bootServer(t *testing.T, cfg config.Config) (*server.Server, string) {
	t.Helper()
	skipShort(t)
	dir := t.TempDir()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.DataDir, cfg.MetaDB = filepath.Join(dir, "data"), filepath.Join(dir, "meta.db")
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	srv, err := server.NewFromConfig(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	bound, err := srv.Bind(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { srv.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return srv, bound.String()
}

func compressFor(t *testing.T, codec int8, data []byte) []byte {
	t.Helper()
	switch codec {
	case 0:
		return data
	case 1:
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		w.Write(data)
		w.Close()
		return b.Bytes()
	case 2:
		return snappy.Encode(nil, data)
	case 3:
		var b bytes.Buffer
		w := lz4.NewWriter(&b)
		w.Write(data)
		w.Close()
		return b.Bytes()
	case 4:
		e, err := zstd.NewWriter(nil)
		if err != nil {
			t.Fatal(err)
		}
		return e.EncodeAll(data, nil)
	}
	t.Fatalf("no codec %d", codec)
	return nil
}

func subscribe(t *testing.T, c *rawConn, id [16]byte) *kmsg.GetTelemetrySubscriptionsResponse {
	t.Helper()
	req := kmsg.NewPtrGetTelemetrySubscriptionsRequest()
	req.ClientInstanceID = id
	return ask(t, c, req, 0, kmsg.NewPtrGetTelemetrySubscriptionsResponse())
}

func push(t *testing.T, c *rawConn, id [16]byte, sub int32, codec int8, payload []byte, terminating bool) int16 {
	t.Helper()
	req := kmsg.NewPtrPushTelemetryRequest()
	req.ClientInstanceID, req.SubscriptionID, req.CompressionType, req.Metrics, req.Terminating = id, sub, codec, payload, terminating
	return ask(t, c, req, 0, kmsg.NewPtrPushTelemetryResponse()).ErrorCode
}

func metricsPayload(v float64) []byte {
	return telemetry.Encode(telemetry.Payload{
		Resource: map[string]string{"service.name": "billing"},
		Metrics: []telemetry.Metric{
			{Name: "org.apache.kafka.producer.node.request.latency.avg", Unit: "ms", Kind: telemetry.Gauge, Points: []telemetry.Point{{Attrs: "node_id=1", Time: time.Now(), Value: v}}},
		},
	})
}

func TestClientTelemetryIsAdvertisedOnlyWhereKafkaOffersItAndWhenOn(t *testing.T) {
	has := func(addr string) (get, push bool) {
		resp := ask(t, openRaw(t, addr), kmsg.NewPtrApiVersionsRequest(), 3, kmsg.NewPtrApiVersionsResponse())
		for _, k := range resp.ApiKeys {
			switch k.ApiKey {
			case int16(kmsg.GetTelemetrySubscriptions):
				get = true
			case int16(kmsg.PushTelemetry):
				push = true
			}
		}
		return
	}
	_, on := bootServer(t, config.Default())
	if g, p := has(on); !g || !p {
		t.Errorf("Kafka 4 with telemetry on advertises get=%v push=%v", g, p)
	}
	cfg := config.Default()
	cfg.ClientTelemetry = false
	_, off := bootServer(t, cfg)
	if g, p := has(off); g || p {
		t.Errorf("--client-telemetry=false still advertises get=%v push=%v", g, p)
	}
	cfg = config.Default()
	cfg.Version = 3
	_, three := bootServer(t, cfg)
	if g, p := has(three); g || p {
		t.Errorf("Kafka 3's profile advertises get=%v push=%v: the APIs are Kafka 3.7+, offered in the Kafka 4 column", g, p)
	}
	// A request for an API that is not offered closes the connection, as for any other.
	req := kmsg.NewPtrPushTelemetryRequest()
	req.SetVersion(0)
	if openRaw(t, off).do(req, kmsg.NewPtrPushTelemetryResponse()) {
		t.Error("a push to a broker with telemetry off was answered")
	}
}

func TestClientTelemetrySubscribeGivesAnIdAndWhatToReport(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)

	first := subscribe(t, c, [16]byte{})
	if first.ErrorCode != 0 || first.ClientInstanceID == ([16]byte{}) || first.SubscriptionID == 0 {
		t.Fatalf("first subscription = %+v", first)
	}
	if first.PushIntervalMillis != pushMillis || first.TelemetryMaxBytes != maxBytes ||
		len(first.RequestedMetrics) != 1 || first.RequestedMetrics[0] != "org.apache.kafka." {
		t.Errorf("what to report = %+v", first)
	}
	if got := first.AcceptedCompressionTypes; len(got) != 4 || got[0] != 4 || got[1] != 3 || got[2] != 1 || got[3] != 2 {
		t.Errorf("accepted compression = %v, want zstd, lz4, gzip, snappy as Apache Kafka lists them", got)
	}
	// Asking again at once is early, as it is on Apache Kafka (recorded:
	// telemetry-empty-subscription): THROTTLING_QUOTA_EXCEEDED and nothing else.
	if again := subscribe(t, c, first.ClientInstanceID); again.ErrorCode != kerr.ThrottlingQuotaExceeded.Code || again.ClientInstanceID != ([16]byte{}) {
		t.Errorf("a second subscription at once: %+v, want THROTTLING_QUOTA_EXCEEDED", again)
	}
	// A client with an id the broker has not heard of (it survived a restart) is adopted,
	// not refused.
	other := [16]byte{9, 9, 9}
	if got := subscribe(t, c, other); got.ClientInstanceID != other {
		t.Errorf("an id the client brought was replaced: %x", got.ClientInstanceID)
	}
	if n := len(serverhook.ClientMetrics(srv).Instances()); n != 2 {
		t.Errorf("%d instances known, want 2", n)
	}
}

func TestClientTelemetryPushIsDecodedInEveryCompression(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)

	// One instance for each codec: a second push from one is early, and refused.
	ids := map[int8][16]byte{}
	for codec := int8(0); codec <= 4; codec++ {
		sub := subscribe(t, c, [16]byte{})
		ids[codec] = sub.ClientInstanceID
		if code := push(t, c, sub.ClientInstanceID, sub.SubscriptionID, codec, compressFor(t, codec, metricsPayload(float64(10+codec))), false); code != 0 {
			t.Fatalf("compression %d: error %d (%s)", codec, code, kerr.ErrorForCode(code))
		}
	}
	for codec := int8(0); codec <= 4; codec++ {
		in, ok := serverhook.ClientMetrics(srv).Instance(ids[codec])
		if !ok || in.Pushes != 1 {
			t.Fatalf("compression %d: instance %+v, want one push", codec, in)
		}
		if in.Resource["service.name"] != "billing" || len(in.Series) != 1 {
			t.Fatalf("compression %d: resource %v, series %+v", codec, in.Resource, in.Series)
		}
		ser := in.Series[0]
		if ser.Name != "org.apache.kafka.producer.node.request.latency.avg" || ser.Attrs != "node_id=1" || ser.Unit != "ms" || len(ser.Samples) != 1 {
			t.Fatalf("compression %d: series = %+v", codec, ser)
		}
		if want := float64(10 + codec); ser.Samples[0].V != want {
			t.Errorf("compression %d: sample = %v, want %v", codec, ser.Samples[0].V, want)
		}
		if in.Principal != "User:ANONYMOUS" || in.Host == "" {
			t.Errorf("who it was: principal %q host %q", in.Principal, in.Host)
		}
	}
}

// Every refusal, with what Apache Kafka 4.3.1 answered the same push
// (conformance/recordings/telemetry-errors.json); each case is its own instance, because
// a second push from one is early.
func TestClientTelemetryPushErrors(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	good := metricsPayload(1)

	for _, tc := range []struct {
		name     string
		id       func(sub *kmsg.GetTelemetrySubscriptionsResponse) [16]byte
		sub      func(sub *kmsg.GetTelemetrySubscriptionsResponse) int32
		codec    int8
		payload  []byte
		wantCode int16
		stored   bool
	}{
		{"no client instance id", nil, nil, 0, good, kerr.InvalidRequest.Code, false},
		{"an instance the broker never saw, with another's subscription id", func(*kmsg.GetTelemetrySubscriptionsResponse) [16]byte { return [16]byte{5} }, nil, 0, good, kerr.UnknownSubscriptionID.Code, false},
		{"an out-of-date subscription id", nil, func(s *kmsg.GetTelemetrySubscriptionsResponse) int32 { return s.SubscriptionID + 1 }, 0, good, kerr.UnknownSubscriptionID.Code, false},
		{"a compression type nobody defined", nil, nil, 9, good, kerr.UnsupportedCompressionType.Code, false},
		{"a negative compression type", nil, nil, -1, good, kerr.UnsupportedCompressionType.Code, false},
		{"more than the broker will take", nil, nil, 0, bytes.Repeat([]byte{0}, maxBytes+1), kerr.TelemetryTooLarge.Code, false},
		{"exactly what the broker will take", nil, nil, 0, bytes.Repeat([]byte{0}, maxBytes), 0, false},
		{"gzip that is not gzip", nil, nil, 1, good, kerr.InvalidRecord.Code, false},
		{"snappy that is not snappy", nil, nil, 2, good, kerr.InvalidRecord.Code, false},
		{"lz4 that is not lz4", nil, nil, 3, good, kerr.InvalidRecord.Code, false},
		{"zstd that is not zstd", nil, nil, 4, good, kerr.InvalidRecord.Code, false},
		{"a payload that inflates past the cap", nil, nil, 4, compressFor(t, 4, bytes.Repeat(good, 30000)), kerr.InvalidRecord.Code, false},
		// A broker with no plugin to take the metrics does not read them.
		{"not OTLP", nil, nil, 0, []byte("this is not protobuf at all, just text"), 0, false},
		{"gzip of text", nil, nil, 1, compressFor(t, 1, []byte("this is not protobuf at all, just text")), 0, false},
		{"an empty push", nil, nil, 0, nil, 0, false},
		{"a good push", nil, nil, 1, compressFor(t, 1, good), 0, true},
	} {
		sub := subscribe(t, c, [16]byte{})
		id, subID := sub.ClientInstanceID, sub.SubscriptionID
		if tc.id != nil {
			id = tc.id(sub)
		} else if tc.name == "no client instance id" {
			id = [16]byte{}
		}
		if tc.sub != nil {
			subID = tc.sub(sub)
		}
		if got := push(t, c, id, subID, tc.codec, tc.payload, false); got != tc.wantCode {
			t.Errorf("%s: error %d (%s), want %d (%s)", tc.name, got, kerr.ErrorForCode(got), tc.wantCode, kerr.ErrorForCode(tc.wantCode))
		}
		in, _ := serverhook.ClientMetrics(srv).Instance(sub.ClientInstanceID)
		if has := len(in.Series) > 0; has != tc.stored {
			t.Errorf("%s: series stored = %v, want %v", tc.name, has, tc.stored)
		}
	}
}

// A client closing says so in its last push; it is shown as gone, and forgotten soon.
func TestClientTelemetryRemembersWhoItIsAndThatItIsClosing(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)

	hello := kmsg.NewPtrApiVersionsRequest()
	hello.ClientSoftwareName, hello.ClientSoftwareVersion = "apache-kafka-java", "3.7.0"
	ask(t, c, hello, 3, kmsg.NewPtrApiVersionsResponse())

	sub := subscribe(t, c, [16]byte{})
	if code := push(t, c, sub.ClientInstanceID, sub.SubscriptionID, 0, metricsPayload(1), true); code != 0 {
		t.Fatalf("terminating push: %d", code)
	}
	in := serverhook.ClientMetrics(srv).Instances()[0]
	if in.Software != "apache-kafka-java 3.7.0" {
		t.Errorf("software = %q, want what ApiVersions said", in.Software)
	}
	if !in.Terminated {
		t.Error("a terminating push was not recorded as the client closing")
	}
	if !strings.Contains(in.Host, "127.0.0.1") {
		t.Errorf("host = %q", in.Host)
	}
}

// A broker that restarted has forgotten every subscription and every instance, and a
// client's push that holds the subscription id it was given before is taken all the same:
// Apache Kafka's subscription id is a function of the instance id and what it subscribes
// to, not a number it keeps (recorded: telemetry-broker-restart). The instance is made by
// the push, and the client's next request for its subscription is early, as it is after
// any push taken.
func TestClientTelemetryPushHoldingAnIdFromBeforeARestartIsTaken(t *testing.T) {
	_, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	sub := subscribe(t, c, [16]byte{})

	srv2, addr2 := bootServer(t, config.Default()) // a different process's broker: it has never seen the instance.
	c2 := openRaw(t, addr2)
	if code := push(t, c2, sub.ClientInstanceID, sub.SubscriptionID, 0, metricsPayload(1), false); code != 0 {
		t.Errorf("a push to a broker that has not seen the instance: %d, want it taken", code)
	}
	if in, ok := serverhook.ClientMetrics(srv2).Instance(sub.ClientInstanceID); !ok || in.Pushes != 1 {
		t.Errorf("instance = %+v %v, want the push stored", in, ok)
	}
	again := subscribe(t, c2, sub.ClientInstanceID)
	if again.ErrorCode != kerr.ThrottlingQuotaExceeded.Code {
		t.Errorf("subscribing right after the push: code %d, want THROTTLING_QUOTA_EXCEEDED", again.ErrorCode)
	}
	// A client that asks first gets the id it had: the subscription is the same.
	_, addr3 := bootServer(t, config.Default())
	c3 := openRaw(t, addr3)
	first := subscribe(t, c3, sub.ClientInstanceID)
	if first.ErrorCode != 0 || first.ClientInstanceID != sub.ClientInstanceID || first.SubscriptionID != sub.SubscriptionID {
		t.Errorf("subscribing with the old instance id: %+v, want the same instance and subscription id", first)
	}
}

// A payload a real librdkafka 2.15.1 sent (zstd, 480 bytes, a 2 MiB window declared): the
// first version of this broker answered INVALID_REQUEST to it, and librdkafka then
// "stopped telemetry" for the life of the client.
func TestClientTelemetryAcceptsWhatRealLibrdkafkaSends(t *testing.T) {
	frame, err := os.ReadFile("../storage/log/testdata/librdkafka-telemetry.zst")
	if err != nil {
		t.Fatal(err)
	}
	srv, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	sub := subscribe(t, c, [16]byte{})
	if code := push(t, c, sub.ClientInstanceID, sub.SubscriptionID, 4, frame, false); code != 0 {
		t.Fatalf("the librdkafka payload was refused: %d (%s)", code, kerr.ErrorForCode(code))
	}
	in := serverhook.ClientMetrics(srv).Instances()[0]
	if len(in.Series) < 5 {
		t.Fatalf("%d series from a librdkafka report, want its producer metrics", len(in.Series))
	}
	for _, s := range in.Series {
		if !strings.HasPrefix(s.Name, "org.apache.kafka.") || len(s.Samples) != 1 {
			t.Errorf("series %q with %d samples", s.Name, len(s.Samples))
		}
	}
}
