package broker_test

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TOP-2: a topic that sets nothing is still described with every key Kafka has,
// and each value is the one the recording of apache/kafka:4.3.1 holds
// (conformance/recordings/topic-config-keys.json), except the three this broker sets
// differently on purpose: retention.ms and segment.bytes (config.go) and
// min.cleanable.dirty.ratio (it cleans at 0).
func TestDescribeConfigsReportsEveryKeyKafkaDoes(t *testing.T) {
	raw, err := os.ReadFile("../../conformance/recordings/topic-config-keys.json")
	if err != nil {
		t.Fatal(err)
	}
	var rec struct {
		Steps []struct {
			Label    string
			Response struct {
				Resources []struct {
					Configs []struct {
						Name   string
						Value  *string
						Source string
					}
				}
			}
		}
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	var want map[string]string
	for _, s := range rec.Steps {
		if s.Label == "describe-all" {
			want = map[string]string{}
			for _, c := range s.Response.Resources[0].Configs {
				want[c.Name] = *c.Value
			}
		}
	}
	if len(want) < 33 {
		t.Fatalf("recording holds %d keys", len(want))
	}

	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	creq := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = "allkeys", 1, 1
	creq.Topics = append(creq.Topics, ct)
	if _, err := creq.RequestWith(ctx, cl); err != nil {
		t.Fatal(err)
	}
	req := kmsg.NewPtrDescribeConfigsRequest()
	rr := kmsg.NewDescribeConfigsRequestResource()
	rr.ResourceType, rr.ResourceName = kmsg.ConfigResourceTypeTopic, "allkeys"
	req.Resources = append(req.Resources, rr)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Resources) != 1 {
		t.Fatalf("describe: %v", err)
	}
	got := map[string]string{}
	for _, c := range resp.Resources[0].Configs {
		got[c.Name] = *c.Value
	}
	differs := []string{"retention.ms", "segment.bytes", "min.cleanable.dirty.ratio"}
	for k, v := range want {
		g, ok := got[k]
		switch {
		case !ok:
			t.Errorf("%s is missing; Kafka reports %q", k, v)
		case g != v && !slices.Contains(differs, k):
			t.Errorf("%s = %q, Kafka reports %q", k, g, v)
		}
	}
	if len(got) < len(want) {
		t.Errorf("%d keys, Kafka reports %d", len(got), len(want))
	}
}
