package broker_test

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/serverhook"

	"github.com/twmb/franz-go/pkg/kerr"
)

// An instance belongs to the connection that made it while that connection is open
// (TRF-27). What real Kafka answers a push for somebody else's instance id is not
// recorded (queue R4, `memory`); this is this broker's rule, written down so it is not
// an accident.
func TestInstanceIsBoundToItsConnectionOnTheWire(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	a, b := openRaw(t, addr), openRaw(t, addr)
	sub := subscribe(t, a, [16]byte{})

	if code := push(t, b, sub.ClientInstanceID, sub.SubscriptionID, 0, metricsPayload(1), false); code != kerr.InvalidRequest.Code {
		t.Errorf("another open connection pushed as the instance: error %d, want INVALID_REQUEST", code)
	}
	if got := subscribe(t, b, sub.ClientInstanceID); got.ClientInstanceID == sub.ClientInstanceID || got.ClientInstanceID == ([16]byte{}) {
		t.Errorf("another connection was given the instance id it quoted: %x", got.ClientInstanceID)
	}
	if code := push(t, a, sub.ClientInstanceID, sub.SubscriptionID, 0, metricsPayload(1), false); code != 0 {
		t.Errorf("the owner's push: error %d", code)
	}
	in, ok := serverhook.ClientMetrics(srv).Instance(sub.ClientInstanceID)
	if !ok || in.Pushes != 1 || in.Conn == 0 {
		t.Errorf("instance = %+v %v, want one push and the owning connection's number", in, ok)
	}

	// The owner's connection closes and the same client comes back on a new one.
	a.c.Close()
	c := openRaw(t, addr)
	deadline := time.Now().Add(3 * time.Second)
	for {
		code := push(t, c, sub.ClientInstanceID, sub.SubscriptionID, 0, metricsPayload(2), false)
		if code == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a reconnect was still refused after its old connection closed: error %d", code)
		}
		time.Sleep(20 * time.Millisecond)
	}
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
