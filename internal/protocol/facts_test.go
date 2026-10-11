package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

func TestFactsOfAProduceNameEachPartitionsResult(t *testing.T) {
	q := kmsg.NewPtrProduceRequest()
	q.Acks, q.TransactionID = -1, str("checkout-1")
	tp := kmsg.NewProduceRequestTopic()
	tp.Topic = "orders"
	pp := kmsg.NewProduceRequestTopicPartition()
	pp.Partition, pp.Records = 1, batchOf(1)
	tp.Partitions = append(tp.Partitions, pp)
	q.Topics = append(q.Topics, tp)

	resp := kmsg.NewPtrProduceResponse()
	rt := kmsg.NewProduceResponseTopic()
	rt.Topic = "orders"
	rp := kmsg.NewProduceResponseTopicPartition()
	rp.Partition, rp.BaseOffset = 1, 4812
	rt.Partitions = append(rt.Partitions, rp)
	resp.Topics = append(resp.Topics, rt)

	f := DescribeFacts(q, resp)
	if !f.HasAcks || f.Acks != -1 {
		t.Errorf("acks = %d (%v)", f.Acks, f.HasAcks)
	}
	if len(f.Partitions) != 1 || f.Partitions[0] != (PartResult{Topic: "orders", Partition: 1, Offset: 4812, HasOffset: true, Records: 1, Bytes: 81}) {
		t.Errorf("partitions = %+v", f.Partitions)
	}
	if !json.Valid([]byte(f.Decoded)) || !strings.Contains(f.Decoded, `"transaction_id": "checkout-1"`) ||
		!strings.Contains(f.Decoded, `"records": "1 batch, 1 record, 81 B"`) || !strings.Contains(f.Decoded, `"acks": -1`) {
		t.Errorf("decoded = %s", f.Decoded)
	}
}

// What the inspector shows can never hold what a client sent as data: a record's key,
// value or header, or a secret in a configuration value (TRF-23).
func TestFactsNeverHoldRecordBytesOrSecrets(t *testing.T) {
	secret := []byte("a-distinctive-value-9f3b")
	batch := append(batchOf(1), secret...)
	q := kmsg.NewPtrProduceRequest()
	tp := kmsg.NewProduceRequestTopic()
	tp.Topic = "orders"
	pp := kmsg.NewProduceRequestTopicPartition()
	pp.Records = batch
	tp.Partitions = append(tp.Partitions, pp)
	q.Topics = append(q.Topics, tp)
	if f := DescribeFacts(q, nil); strings.Contains(f.Decoded, string(secret)) {
		t.Errorf("record bytes in the decoded fields: %s", f.Decoded)
	}

	cfg := kmsg.NewPtrIncrementalAlterConfigsRequest()
	res := kmsg.NewIncrementalAlterConfigsRequestResource()
	res.ResourceType, res.ResourceName = kmsg.ConfigResourceTypeBroker, "0"
	for _, n := range []string{"ssl.keystore.password", "retention.ms"} {
		c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		v := "hunter2-" + n
		c.Name, c.Value = n, &v
		res.Configs = append(res.Configs, c)
	}
	cfg.Resources = append(cfg.Resources, res)
	f := DescribeFacts(cfg, nil)
	if strings.Contains(f.Decoded, "hunter2-ssl.keystore.password") || !strings.Contains(f.Decoded, "(hidden)") {
		t.Errorf("a secret configuration value is shown: %s", f.Decoded)
	}
	if !strings.Contains(f.Decoded, "hunter2-retention.ms") {
		t.Errorf("an ordinary configuration value is not shown: %s", f.Decoded)
	}

	auth := kmsg.NewPtrSASLAuthenticateRequest()
	auth.SASLAuthBytes = []byte("\x00alice\x00her-password")
	if f := DescribeFacts(auth, nil); strings.Contains(f.Decoded, "her-password") {
		t.Errorf("a SASL password is shown: %s", f.Decoded)
	}
}

func TestFactsAreBounded(t *testing.T) {
	q := kmsg.NewPtrProduceRequest()
	for ti := 0; ti < 30; ti++ {
		tp := kmsg.NewProduceRequestTopic()
		tp.Topic = strings.Repeat("t", 100)
		for p := int32(0); p < 500; p++ {
			pp := kmsg.NewProduceRequestTopicPartition()
			pp.Partition, pp.Records = p, bytes.Repeat([]byte{1}, 10)
			tp.Partitions = append(tp.Partitions, pp)
		}
		q.Topics = append(q.Topics, tp)
	}
	f := DescribeFacts(q, nil)
	if len(f.Decoded) > maxDecodedBytes {
		t.Errorf("decoded is %d bytes, want at most %d", len(f.Decoded), maxDecodedBytes)
	}
	if !json.Valid([]byte(f.Decoded)) {
		t.Errorf("not JSON: %s", f.Decoded)
	}
	if len(f.Partitions) != MaxFactPartitions || f.PartitionsMore != 30*500-MaxFactPartitions {
		t.Errorf("%d partitions kept, %d more", len(f.Partitions), f.PartitionsMore)
	}
}

func TestFactsSnakeCase(t *testing.T) {
	for in, want := range map[string]string{
		"TransactionID": "transaction_id", "TimeoutMillis": "timeout_millis", "Topics": "topics",
		"ClientInstanceID": "client_instance_id", "SASLAuthBytes": "sasl_auth_bytes", "Acks": "acks",
	} {
		if got := snake(in); got != want {
			t.Errorf("snake(%q) = %q, want %q", in, got, want)
		}
	}
}
