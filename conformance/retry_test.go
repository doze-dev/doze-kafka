package conformance

import (
	"fmt"
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

// SubscriptionID 0 is "no subscription": the normaliser numbers the made-up ids
// by first appearance and keeps 0 as 0, so doze-kafka inventing an id where
// Kafka sent 0 is a difference.
func TestZeroSubscriptionIDIsKeptByTheNormalizer(t *testing.T) {
	n := newNormalizer()
	r := kmsg.NewPtrPushTelemetryRequest()
	got, err := n.normalize(r)
	if err != nil {
		t.Fatal(err)
	}
	if id := got.(map[string]any)["SubscriptionID"]; fmt.Sprint(id) != "0" {
		t.Fatalf("SubscriptionID 0 normalised to %v", id)
	}
	r.SubscriptionID = 7
	got, _ = n.normalize(r)
	if id := got.(map[string]any)["SubscriptionID"]; id != "<subscription:1>" {
		t.Fatalf("SubscriptionID 7 normalised to %v, want the first made-up id", id)
	}
}

// A tagged field one broker sends and the decoder does not know is counted, so
// it is a difference; a field named Version inside a message is data and kept
// (only the message's own version, which the step names, is dropped).
func TestUnknownTagsAndNestedVersionsReachTheRecording(t *testing.T) {
	n := newNormalizer()
	r := kmsg.NewPtrDescribeClusterResponse()
	r.Version = 1
	b := kmsg.NewDescribeClusterResponseBroker()
	b.UnknownTags.Set(99, []byte{1})
	r.Brokers = append(r.Brokers, b)
	got, err := n.normalize(r)
	if err != nil {
		t.Fatal(err)
	}
	m := got.(map[string]any)
	if fmt.Sprint(m[unknownTagsKey]) != "1" {
		t.Errorf("an unknown tagged field is not in the normalised message: %v", m)
	}
	if _, ok := m["Version"]; ok {
		t.Errorf("the message's own version was kept: %v", m)
	}
	nested := struct{ Inner struct{ Version int } }{}
	nested.Inner.Version = 3
	got, _ = n.normalize(nested)
	if fmt.Sprint(got.(map[string]any)["Inner"]) != "map[Version:3]" {
		t.Errorf("a nested Version was dropped: %v", got)
	}
}
