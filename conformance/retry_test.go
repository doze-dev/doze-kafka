package conformance

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// A replay holds doze-kafka to its first answer: DoUntil and Do ask again only
// against the real broker, where it stands for "Kafka had not finished". Asked
// again against doze-kafka, an answer that came out right on the tenth try would
// compare equal to Kafka's first, and a broker answering NOT_COORDINATOR where
// Kafka answered 0 would pass.
func TestAReplayNeverAsksTwice(t *testing.T) {
	target, err := StartDoze(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	e, err := NewEnv(target, t.Fatalf)
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	e.DoUntil("never done", kmsg.NewPtrMetadataRequest(), 9, func(kmsg.Response) bool { return false })
	if e.asked != 0 || len(e.Steps()) != 1 {
		t.Fatalf("against doze-kafka a step was asked %d more times (%d steps recorded, %v)", e.asked, len(e.Steps()), time.Since(began))
	}
}
