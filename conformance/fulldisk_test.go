package conformance

import (
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// The full-disk scenario (Q2) needs a volume that fills. Against Apache Kafka it is a
// tmpfs; against doze-kafka, which shares this process, the fault seam stands in for
// it (TestReplayFullDisk, go test -tags faultinject).
func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "full-disk", Run: fullDisk(-1), DiskMiB: 24, Known: fullDiskKnown},
		Scenario{Name: "full-disk-acks-1", Run: fullDisk(1), DiskMiB: 24, Known: fullDiskKnown})
}

// fullDisk is Q2: the data volume fills while a producer writes at acks. It
// records the code of the first refused Produce and then that the broker goes
// away. It does not record requests between the two: a single-directory Kafka
// whose log directory failed shuts itself down a moment later, and whether a
// request is answered first is a race inside Kafka (one recording answered
// Metadata, the next closed the connection). So the script waits, up to a
// minute, until the broker no longer answers, and records that; which is why
// acks=1 is a scenario of its own and not a request after the acks=all one.
func fullDisk(acks int16) func(e *Env) {
	return func(e *Env) { fullDiskAt(e, acks) }
}

// fullDiskKnown is what doze-kafka answers differently: Kafka's only log directory
// failed, so it shuts down; this broker stays up (docs/limitations.md).
var fullDiskKnown = []Known{{Step: "broker-after-the-refusal", Limitation: staysUp}}

func fullDiskAt(e *Env, acks int16) {
	e.Do("create", createTopics(false, newTopic("disk", 1, 1)), 7)
	e.Produce("small-ok", "disk", 0, ProduceRecord{TimestampMs: 1000, Key: "k", Value: "v"})

	big := make([]byte, 700<<10)
	for i := range big {
		big[i] = byte(i*131 + i>>8)
	}
	req := func(acks int16) *kmsg.ProduceRequest {
		r := kmsg.NewPtrProduceRequest()
		r.Acks, r.TimeoutMillis = acks, 10000
		t := kmsg.NewProduceRequestTopic()
		t.Topic = "disk"
		p := kmsg.NewProduceRequestTopicPartition()
		p.Records = encodeBatch([]ProduceRecord{{TimestampMs: 2000, Key: "big", Value: string(big)}})
		t.Partitions = append(t.Partitions, p)
		r.Topics = append(r.Topics, t)
		return r
	}
	// doze-kafka shares this process: the fault seam stands in for the volume.
	if _, real := e.target.(Execer); !real {
		if armDiskFull == nil {
			e.fail("filling doze-kafka's disk needs the fault seam (go test -tags faultinject)")
			return
		}
		armDiskFull()
	}
	// doze-kafka shares this process: the fault seam stands in for the volume.
	if _, real := e.target.(Execer); !real {
		if armDiskFull == nil {
			e.fail("filling doze-kafka's disk needs the fault seam (go test -tags faultinject)")
			return
		}
		armDiskFull()
	}
	// Fill until the first refusal. The count depends on the filesystem, not on
	// the broker, so only the refusal is recorded, with a stub request.
	var first *kmsg.ProduceResponse
	for i := 0; i < 200 && first == nil; i++ {
		resp, err := e.w.roundTrip(req(acks), 9)
		if err != nil {
			e.fail("fill %d: %v", i, err)
			return
		}
		if pr := resp.(*kmsg.ProduceResponse); pr.Topics[0].Partitions[0].ErrorCode != 0 {
			first = pr
		}
	}
	if first == nil {
		e.fail("the volume never filled")
		return
	}
	nresp, err := e.norm.normalize(first)
	if err != nil {
		e.fail("normalize: %v", err)
		return
	}
	e.steps = append(e.steps, Step{Label: "first-refused", API: "Produce", Version: 9,
		Request: map[string]any{"(a batch of 700 KiB, repeated until the volume was full)": acks}, Response: nresp})

	gone := "still answering a minute after the refusal"
	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		w, err := dialOnce(e.target.Addr())
		if err != nil {
			gone = "stopped"
			break
		}
		w.fast = true
		_, err = w.roundTrip(metadata(false, "disk"), 9)
		w.close()
		if err != nil {
			gone = "stopped"
			break
		}
	}
	e.steps = append(e.steps, Step{Label: "broker-after-the-refusal", API: "(broker)",
		Request: map[string]any{"(Metadata on a new connection, until it is not answered)": 0}, Response: map[string]any{"(broker)": gone}})
}
