package conformance

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// The full-disk scenario (Q2) needs a volume that fills. Against Apache Kafka it
// is a tmpfs; against doze-kafka, which shares this process, the fault seam stands
// in for it (TestReplayFullDisk, go test -tags faultinject).
func init() {
	Scenarios = append(Scenarios, Scenario{Name: "full-disk", Run: fullDisk, DiskMiB: 24, Known: []Known{
		{Step: "list-earliest-after", Limitation: staysUp}, {Step: "create-after", Limitation: staysUp}}})
}

// fullDisk is Q2: the data volume fills while a producer writes. It records
// the code of the first refused Produce at acks=all and at acks=1, what a Fetch
// and a Metadata request do afterwards, and whether the broker is still there.
func fullDisk(e *Env) {
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
	// Fill until the first refusal. The count depends on the filesystem, not on
	// the broker, so only the refusal is recorded, with a stub request.
	var first *kmsg.ProduceResponse
	for i := 0; i < 200 && first == nil; i++ {
		resp, err := e.w.roundTrip(req(-1), 9)
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
	e.steps = append(e.steps, Step{Label: "first-refused-acks-all", API: "Produce", Version: 9,
		Request: map[string]any{"(a batch of 700 KiB, repeated until the volume was full)": 0}, Response: nresp})

	stub := func(label string, acks int16) {
		resp, err := e.w.roundTrip(req(acks), 9)
		if err != nil {
			e.steps = append(e.steps, Step{Label: label, API: "Produce", Version: 9,
				Request:  map[string]any{"(a batch of 700 KiB)": acks},
				Response: map[string]any{"(connection)": fmt.Sprint("error: ", err)}})
			return
		}
		n, _ := e.norm.normalize(resp)
		e.steps = append(e.steps, Step{Label: label, API: "Produce", Version: 9,
			Request: map[string]any{"(a batch of 700 KiB)": acks}, Response: n})
	}
	stub("refused-again-acks-all", -1)
	stub("refused-acks-1", 1)

	e.Do("metadata-after", metadata(false, "disk"), 9)
	lo := kmsg.NewPtrListOffsetsRequest()
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = "disk"
	lp := kmsg.NewListOffsetsRequestTopicPartition()
	lp.Partition, lp.Timestamp = 0, -2
	lt.Partitions = append(lt.Partitions, lp)
	lo.Topics = append(lo.Topics, lt)
	e.Do("list-earliest-after", lo, 7)
	e.Do("create-after", createTopics(false, newTopic("disk-2", 1, 1)), 7)
}
