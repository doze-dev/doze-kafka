package broker_test

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/serverhook"

	"github.com/twmb/franz-go/pkg/kerr"
)

// A client may subscribe on one connection and push on another (the Java client's
// bootstrap connection and the one metadata gave it): that works, and the instance then
// belongs to the connection that last pushed, which is how the console finds a client's
// metrics. What real Kafka does is unrecorded (queue R4, `memory`), so nothing here is
// refused on the evidence of the connection.
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
	_ = kerr.InvalidRequest
	_ = time.Second
}

// A client told to push every five seconds that pushes a hundred times in a hundred
// milliseconds still gets success, and the broker decodes the first of them only.
func TestPushesPastTheBurstAreCountedAndNotStored(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	sub := subscribe(t, c, [16]byte{})
	for i := 0; i < 100; i++ {
		if code := push(t, c, sub.ClientInstanceID, sub.SubscriptionID, 0, metricsPayload(float64(i)), false); code != 0 {
			t.Fatalf("push %d: error %d", i, code)
		}
	}
	in, _ := serverhook.ClientMetrics(srv).Instance(sub.ClientInstanceID)
	if in.Pushes >= 100 || in.Skipped == 0 || in.Pushes+in.Skipped != 100 {
		t.Errorf("stored %d pushes and skipped %d, want them to add up to 100 with some skipped", in.Pushes, in.Skipped)
	}
	// A terminating push is never skipped.
	if code := push(t, c, sub.ClientInstanceID, sub.SubscriptionID, 0, metricsPayload(1), true); code != 0 {
		t.Errorf("terminating push: %d", code)
	}
	in, _ = serverhook.ClientMetrics(srv).Instance(sub.ClientInstanceID)
	if !in.Terminated {
		t.Error("the terminating push was not recorded")
	}
}

// What real Kafka answers to a push that comes before its interval has passed is not
// recorded (queue R1, `memory`). The assertion waits for the recording; until then this
// broker answers success, and the test says what it is waiting for.
func TestEarlyPushIsAnsweredAsRecorded(t *testing.T) {
	t.Skip("recording queue R1 (telemetry-throttle): the code real Kafka answers a push made before the interval has passed is not recorded; nothing here is changed on memory")
}
