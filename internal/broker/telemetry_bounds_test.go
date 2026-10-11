package broker_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
	"github.com/doze-dev/doze-kafka/internal/telemetry"
	"github.com/doze-dev/doze-kafka/internal/traffic"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TRF-1, on the wire: an anonymous client's 900 KiB join reason is what the recorder
// shows for the request, and the recorder holds 2,000 of them.
func TestARecordedJoinReasonIsClipped(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)

	req := kmsg.NewPtrJoinGroupRequest()
	req.Group, req.SessionTimeoutMillis, req.RebalanceTimeoutMillis, req.ProtocolType = "g", 10000, 10000, "consumer"
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name = "range"
	req.Protocols = append(req.Protocols, p)
	reason := strings.Repeat("r", 900<<10)
	req.Reason = &reason
	req.MemberID = "m-1"
	ask(t, c, req, 9, kmsg.NewPtrJoinGroupResponse())

	var join *traffic.Event
	for _, e := range serverhook.Traffic(srv).Events(traffic.Filter{}, 0) {
		if e.API == "JoinGroup" {
			e := e
			join = &e
		}
	}
	if join == nil {
		t.Fatal("the recorder has no JoinGroup row")
	}
	if len(join.Detail) > 256 {
		t.Errorf("the row keeps %d bytes of the reason, want at most 256", len(join.Detail))
	}
}

// TRF-3, on the wire: a payload that is a snappy stream cut short is a bad payload,
// answered as one, and the connection that sent it is still there for the next request.
func TestPushOfACutShortXerialStreamIsRefusedAndTheConnectionLives(t *testing.T) {
	_, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	// Each push is its own instance: a second from one is early.
	xerial := []byte{0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0}
	for n := 8; n < 16; n++ {
		sub := subscribe(t, c, [16]byte{})
		body := append(append([]byte{}, xerial...), make([]byte, n-8)...)
		req := kmsg.NewPtrPushTelemetryRequest()
		req.ClientInstanceID, req.SubscriptionID, req.CompressionType, req.Metrics = sub.ClientInstanceID, sub.SubscriptionID, 2, body
		req.SetVersion(0)
		resp := kmsg.NewPtrPushTelemetryResponse()
		if !c.do(req, resp) {
			t.Fatalf("a %d byte snappy stream closed the connection", n)
		}
		if resp.ErrorCode != kerr.InvalidRecord.Code {
			t.Errorf("a %d byte snappy stream: error %d (%s), want INVALID_RECORD", n, resp.ErrorCode, kerr.ErrorForCode(resp.ErrorCode))
		}
	}
	next := subscribe(t, c, [16]byte{})
	if code := push(t, c, next.ClientInstanceID, next.SubscriptionID, 0, metricsPayload(1), false); code != 0 {
		t.Errorf("the connection's next push: error %d", code)
	}
}

// TRF-2, on the wire: a push of a few KB that claims to inflate to 60 MiB is refused
// without the broker taking the 60 MiB first.
func TestPushOfAZstdBombIsRefusedWithoutInflatingIt(t *testing.T) {
	_, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	sub := subscribe(t, c, [16]byte{})

	enc, err := zstd.NewWriter(nil, zstd.WithWindowSize(2<<20), zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	bomb := enc.EncodeAll(make([]byte, 60<<20), nil)
	if len(bomb) > 64<<10 {
		t.Fatalf("the bomb is %d bytes", len(bomb))
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	code := push(t, c, sub.ClientInstanceID, sub.SubscriptionID, 4, bomb, false)
	runtime.ReadMemStats(&after)
	if code != kerr.InvalidRecord.Code {
		t.Errorf("error %d (%s), want INVALID_RECORD", code, kerr.ErrorForCode(code))
	}
	// This process also runs the client side and the test's own garbage; the bomb alone
	// was 60 MiB.
	if got := after.TotalAlloc - before.TotalAlloc; got > 16<<20 {
		t.Errorf("the push allocated %d bytes, want well under the 60 MiB it claimed", got)
	}
}

// TRF-4, on the wire: one push that adds 40 keys of 1 KiB and a value of 20 KiB each to the
// instance's resource.
func TestAResourceBurstDoesNotGrowTheStore(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	sub := subscribe(t, c, [16]byte{})
	resource := map[string]string{}
	for i := 0; i < 40; i++ {
		resource[fmt.Sprintf("k%03d-%s", i, strings.Repeat("k", 1024))] = strings.Repeat("v", 20<<10)
	}
	payload := telemetry.Encode(telemetry.Payload{
		Resource: resource,
		Metrics: []telemetry.Metric{{Name: "org.apache.kafka.producer.x", Kind: telemetry.Gauge,
			Points: []telemetry.Point{{Time: time.Now(), Value: 1}}}},
	})
	if code := push(t, c, sub.ClientInstanceID, sub.SubscriptionID, 0, payload, false); code != 0 {
		t.Fatalf("push: error %d", code)
	}
	in := serverhook.ClientMetrics(srv).Instances()[0]
	total := 0
	for k, v := range in.Resource {
		total += len(k) + len(v)
	}
	if total > 64<<10 {
		t.Errorf("the instance's resource holds %d bytes, want under 64 KiB", total)
	}
}
