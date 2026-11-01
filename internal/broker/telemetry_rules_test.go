package broker_test

import (
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/serverhook"

	"github.com/twmb/franz-go/pkg/kerr"
)

// A client may subscribe on one connection and push on another (the Java client's
// bootstrap connection and the one metadata gave it): that works, and the instance then
// belongs to the connection that last pushed, which is how the console finds a client's
// metrics. Apache Kafka 4.3.1 does the same, and ties an instance to no connection and no
// principal at all (recorded: telemetry-instance-binding-spaced).
func TestAnInstanceMayPushOverAnotherConnectionOfTheSameClient(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	a, b := openRaw(t, addr), openRaw(t, addr)
	sub := subscribe(t, a, [16]byte{})
	if code := push(t, b, sub.ClientInstanceID, sub.SubscriptionID, 0, metricsPayload(1), false); code != 0 {
		t.Errorf("another connection of the same client pushed: error %d, want 0", code)
	}
	in, ok := serverhook.ClientMetrics(srv).Instance(sub.ClientInstanceID)
	if !ok || in.Pushes != 1 || in.Conn == 0 {
		t.Errorf("instance = %+v %v, want one push and a connection number", in, ok)
	}
}

// A push before its interval has passed is THROTTLING_QUOTA_EXCEEDED, and counted; the
// first push after a subscription is taken whenever it comes; a terminating push is never
// early, and the push after it is INVALID_REQUEST once. Each answer is what Apache Kafka
// 4.3.1 gave (recorded: conformance/recordings/telemetry-throttle.json, which the
// conformance replay holds this broker to; this is the same without the replay).
func TestEarlyPushIsThrottledAsKafkaDoes(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	sub := subscribe(t, c, [16]byte{})
	id, sid := sub.ClientInstanceID, sub.SubscriptionID
	body := metricsPayload(1)

	for i, want := range []int16{0, kerr.ThrottlingQuotaExceeded.Code, kerr.ThrottlingQuotaExceeded.Code} {
		if code := push(t, c, id, sid, 0, body, false); code != want {
			t.Errorf("push %d: error %d (%s), want %d (%s)", i, code, kerr.ErrorForCode(code), want, kerr.ErrorForCode(want))
		}
	}
	// An early push that is also wrong is answered early: the interval is looked at first.
	if code := push(t, c, id, sid+1, 0, body, false); code != kerr.ThrottlingQuotaExceeded.Code {
		t.Errorf("an early push with an unknown subscription id: %d, want THROTTLING_QUOTA_EXCEEDED", code)
	}
	if code := push(t, c, id, sid, 0, body, true); code != 0 {
		t.Errorf("a terminating push, early: %d, want 0", code)
	}
	if code := push(t, c, id, sid, 0, body, false); code != kerr.InvalidRequest.Code {
		t.Errorf("the push after a terminating one: %d, want INVALID_REQUEST", code)
	}
	if code := push(t, c, id, sid, 0, body, false); code != kerr.ThrottlingQuotaExceeded.Code {
		t.Errorf("the push after that, still inside the interval: %d, want THROTTLING_QUOTA_EXCEEDED", code)
	}
	in, _ := serverhook.ClientMetrics(srv).Instance(id)
	if in.Pushes != 2 || in.Skipped != 4 || !in.Terminated {
		t.Errorf("instance = pushes %d, skipped %d, terminated %v; want 2, 4, true", in.Pushes, in.Skipped, in.Terminated)
	}
}
