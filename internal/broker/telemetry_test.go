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
	maxBytes   = 512 << 10
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
	if len(first.AcceptedCompressionTypes) != 5 {
		t.Errorf("accepted compression = %v, want all five", first.AcceptedCompressionTypes)
	}
	// Asking again with the id it was given keeps it; a client with an id the broker has
	// not heard of (it survived a restart) is adopted, not refused.
	again := subscribe(t, c, first.ClientInstanceID)
	if again.ClientInstanceID != first.ClientInstanceID || again.SubscriptionID != first.SubscriptionID {
		t.Errorf("a second subscription changed the id: %+v", again)
	}
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
	sub := subscribe(t, c, [16]byte{})

	for codec := int8(0); codec <= 4; codec++ {
		if code := push(t, c, sub.ClientInstanceID, sub.SubscriptionID, codec, compressFor(t, codec, metricsPayload(float64(10+codec))), false); code != 0 {
			t.Fatalf("compression %d: error %d (%s)", codec, code, kerr.ErrorForCode(code))
		}
	}
	got := serverhook.ClientMetrics(srv).Instances()
	if len(got) != 1 || got[0].Pushes != 5 {
		t.Fatalf("instances = %+v, want one with five pushes", got)
	}
	in := got[0]
	if in.Resource["service.name"] != "billing" || len(in.Series) != 1 {
		t.Fatalf("resource %v, series %+v", in.Resource, in.Series)
	}
	ser := in.Series[0]
	if ser.Name != "org.apache.kafka.producer.node.request.latency.avg" || ser.Attrs != "node_id=1" || ser.Unit != "ms" || len(ser.Samples) != 5 {
		t.Fatalf("series = %+v", ser)
	}
	for i, want := range []float64{10, 11, 12, 13, 14} {
		if ser.Samples[i].V != want {
			t.Errorf("sample %d (compression %d) = %v, want %v", i, i, ser.Samples[i].V, want)
		}
	}
	if in.Principal != "User:ANONYMOUS" || in.Host == "" {
		t.Errorf("who it was: principal %q host %q", in.Principal, in.Host)
	}
}

func TestClientTelemetryPushErrors(t *testing.T) {
	_, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	sub := subscribe(t, c, [16]byte{})
	good := metricsPayload(1) // valid OTLP, and valid however many times it is repeated: so only the cap refuses the repeat.

	for name, tc := range map[string]struct {
		id       [16]byte
		sub      int32
		codec    int8
		payload  []byte
		wantCode int16
	}{
		"a client that never subscribed":       {[16]byte{5}, sub.SubscriptionID, 0, good, kerr.UnknownSubscriptionID.Code},
		"an out-of-date subscription id":       {sub.ClientInstanceID, sub.SubscriptionID + 1, 0, good, kerr.UnknownSubscriptionID.Code},
		"no client instance id":                {[16]byte{}, sub.SubscriptionID, 0, good, kerr.InvalidRequest.Code},
		"a compression type nobody defined":    {sub.ClientInstanceID, sub.SubscriptionID, 9, good, kerr.UnsupportedCompressionType.Code},
		"more than the broker will take":       {sub.ClientInstanceID, sub.SubscriptionID, 0, bytes.Repeat([]byte{0}, maxBytes+1), kerr.TelemetryTooLarge.Code},
		"not OTLP":                             {sub.ClientInstanceID, sub.SubscriptionID, 0, []byte("this is not protobuf at all, just text"), kerr.InvalidRequest.Code},
		"gzip that is not gzip":                {sub.ClientInstanceID, sub.SubscriptionID, 1, good, kerr.InvalidRequest.Code},
		"a payload that inflates past the cap": {sub.ClientInstanceID, sub.SubscriptionID, 4, compressFor(t, 4, bytes.Repeat(good, 30000)), kerr.InvalidRequest.Code},
	} {
		if got := push(t, c, tc.id, tc.sub, tc.codec, tc.payload, false); got != tc.wantCode {
			t.Errorf("%s: error %d (%s), want %d (%s)", name, got, kerr.ErrorForCode(got), tc.wantCode, kerr.ErrorForCode(tc.wantCode))
		}
	}
	// An empty push is a valid, empty report: a client may have nothing to say.
	if got := push(t, c, sub.ClientInstanceID, sub.SubscriptionID, 0, nil, false); got != 0 {
		t.Errorf("an empty push: error %d", got)
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

// A broker that did not issue a subscription (a restarted one has forgotten them all)
// tells the client to subscribe again, which is what the Java client does on that error.
func TestClientTelemetryFromAnotherBrokersSubscriptionAsksTheClientToSubscribeAgain(t *testing.T) {
	_, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	sub := subscribe(t, c, [16]byte{})

	_, addr2 := bootServer(t, config.Default()) // a different process's broker: its own subscription ids.
	c2 := openRaw(t, addr2)
	if code := push(t, c2, sub.ClientInstanceID, sub.SubscriptionID, 0, metricsPayload(1), false); code != kerr.UnknownSubscriptionID.Code {
		t.Errorf("a push to a broker that never subscribed it: %d, want UNKNOWN_SUBSCRIPTION_ID", code)
	}
	// What the client does next: it asks again, with the instance id it had. The restarted
	// broker (an empty store) takes the id as its own and the client's pushes are stored
	// again, over a new connection and for the same principal: it resumes, and does not stop.
	again := subscribe(t, c2, sub.ClientInstanceID)
	if again.ErrorCode != 0 || again.ClientInstanceID != sub.ClientInstanceID {
		t.Fatalf("subscribing again with the old instance id: code %d, id %x (was %x)", again.ErrorCode, again.ClientInstanceID, sub.ClientInstanceID)
	}
	if code := push(t, c2, again.ClientInstanceID, again.SubscriptionID, 0, metricsPayload(1), false); code != 0 {
		t.Errorf("the push after subscribing again: %d, want none", code)
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
