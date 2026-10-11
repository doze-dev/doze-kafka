package main

// The harbour world: the fixture the design's boards are drawn from (foundation
// B13). Everything is named harbour-*, so a spec can count it without counting what
// the other specs create beside it.
//
//	topics    7   harbour-arrivals (3 partitions), -departures, -berths (compacted),
//	              -customs, -manifests (Avro through the registry), -ledger (a committed
//	              transaction, an aborted one and one left open), -alerts
//	groups    4   harbour-pilots (behind on arrivals), harbour-customs-desk (caught up),
//	              harbour-billing (behind on manifests), harbour-dashboard (alerts, caught up)
//	clients   5   harbour-gate, harbour-crane, harbour-pilot-boat, harbour-customs-scanner,
//	              harbour-billing-sync: each makes its own requests, so Traffic names them
//	subjects  5   harbour-manifests-value (two versions), harbour-vessels-value,
//	              harbour-voyage-value (references vessels), harbour-customs-value (a Protobuf
//	              registered as a descriptor), harbour-alerts-value (READONLY); and
//	              harbour-retired-value, soft-deleted, which leaves a gap in the ids
//	ACLs      6
//	trace         one traceparent on harbour-arrivals, then harbour-customs, then
//	              harbour-manifests
//	registry      one record on harbour-manifests carries a schema id nothing registered
//
// Deterministic: fixed keys, values and timestamps, one fixed order, so offsets and
// schema ids are the same on every fresh broker. Idempotent: each section is written
// only when its topic or subject is not already there.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// harbourTrace is the trace id the three topics share.
const harbourTrace = "5bd66ef5095369c7b0d1f8f4bd33716a"

// harbourEpoch is the time of the first record; the rest follow a minute apart.
var harbourEpoch = time.Date(2026, 3, 14, 6, 0, 0, 0, time.UTC)

// topicHasRecords reports whether the topic exists and holds a record (its end
// offsets are not all zero).
func topicHasRecords(ctx context.Context, cl *kgo.Client, topic string) bool {
	meta := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr(topic)
	meta.Topics = append(meta.Topics, mt)
	mr, err := meta.RequestWith(ctx, cl)
	if err != nil || len(mr.Topics) != 1 || mr.Topics[0].ErrorCode != 0 {
		return false
	}
	lo := kmsg.NewPtrListOffsetsRequest()
	lo.ReplicaID = -1
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = topic
	for _, p := range mr.Topics[0].Partitions {
		lp := kmsg.NewListOffsetsRequestTopicPartition()
		lp.Partition, lp.Timestamp = p.Partition, -1 // the end.
		lt.Partitions = append(lt.Partitions, lp)
	}
	lo.Topics = append(lo.Topics, lt)
	lr, err := lo.RequestWith(ctx, cl)
	if err != nil {
		return false
	}
	for _, t := range lr.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode == 0 && p.Offset > 0 {
				return true
			}
		}
	}
	return false
}

func seedHarbour(ctx context.Context, addr, registryURL string) error {
	clients := map[string]*kgo.Client{}
	as := func(id string) (*kgo.Client, error) {
		if c, ok := clients[id]; ok {
			return c, nil
		}
		c, err := newKgo(kgo.SeedBrokers(addr), kgo.ClientID(id), kgo.RecordPartitioner(kgo.ManualPartitioner()))
		if err != nil {
			return nil, err
		}
		clients[id] = c
		return c, nil
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	gate, err := as("harbour-gate")
	if err != nil {
		return err
	}
	// Topics.
	for _, t := range []struct {
		name  string
		parts int32
		cfg   map[string]string
	}{
		{"harbour-arrivals", 3, nil}, {"harbour-departures", 1, nil},
		{"harbour-berths", 1, map[string]string{"cleanup.policy": "compact"}},
		{"harbour-customs", 1, nil}, {"harbour-manifests", 1, nil}, {"harbour-ledger", 1, nil}, {"harbour-alerts", 1, nil},
	} {
		req := kmsg.NewPtrCreateTopicsRequest()
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = t.name, t.parts, 1
		for _, k := range sortedKeys(t.cfg) {
			c := kmsg.NewCreateTopicsRequestTopicConfig()
			c.Name, c.Value = k, kmsg.StringPtr(t.cfg[k])
			rt.Configs = append(rt.Configs, c)
		}
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, gate)
		if err != nil || len(resp.Topics) != 1 || (resp.Topics[0].ErrorCode != 0 && resp.Topics[0].ErrorCode != 36) { // 36: already there.
			return fmt.Errorf("create %s: %v %+v", t.name, err, resp)
		}
	}

	trace := func(span string) kgo.RecordHeader {
		return kgo.RecordHeader{Key: "traceparent", Value: []byte("00-" + harbourTrace + "-" + span + "-01")}
	}
	minute := 0
	at := func() time.Time { minute++; return harbourEpoch.Add(time.Duration(minute) * time.Minute) }
	send := func(cl *kgo.Client, topic string, part int32, key, value []byte, h ...kgo.RecordHeader) error {
		return cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: part, Key: key, Value: value, Headers: h, Timestamp: at()}).FirstErr()
	}
	vessels := []string{"MV Marguerite", "MV Tern", "SS Halcyon", "MV Orsay", "MV Corvid", "SS Lindis"}

	// harbour-arrivals: thirty records over three partitions; the first starts the trace.
	if !topicHasRecords(ctx, gate, "harbour-arrivals") {
		for i := 0; i < 30; i++ {
			v := fmt.Sprintf(`{"vessel":%q,"imo":"IMO-93214%02d","eta":"2026-03-%02dT%02d:00:00Z","berth":"B%d"}`, vessels[i%len(vessels)], i, 14+i/10, 6+i%12, 1+i%4)
			var h []kgo.RecordHeader
			if i == 0 {
				h = append(h, trace("00f067aa0ba902b7"))
			}
			if err := send(gate, "harbour-arrivals", int32(i%3), []byte(fmt.Sprintf("IMO-93214%02d", i)), []byte(v), h...); err != nil {
				return err
			}
		}
	}
	if !topicHasRecords(ctx, gate, "harbour-departures") {
		for i := 0; i < 12; i++ {
			v := fmt.Sprintf(`{"vessel":%q,"cleared":"2026-03-14T%02d:30:00Z","destination":"Rotterdam"}`, vessels[i%len(vessels)], 8+i)
			if err := send(gate, "harbour-departures", 0, []byte(fmt.Sprintf("dep-%02d", i)), []byte(v)); err != nil {
				return err
			}
		}
	}
	// harbour-berths: compacted. Each of four berths is written twice, and one is
	// then released with a tombstone, so the topic has something to compact.
	if crane, err := as("harbour-crane"); err != nil {
		return err
	} else if !topicHasRecords(ctx, crane, "harbour-berths") {
		for round := 0; round < 2; round++ {
			for b := 1; b <= 4; b++ {
				v := fmt.Sprintf(`{"berth":"B%d","vessel":%q,"round":%d}`, b, vessels[(b+round)%len(vessels)], round)
				if err := send(crane, "harbour-berths", 0, []byte(fmt.Sprintf("B%d", b)), []byte(v)); err != nil {
					return err
				}
			}
		}
		if err := send(crane, "harbour-berths", 0, []byte("B3"), nil); err != nil {
			return err
		}
	}
	if scanner, err := as("harbour-customs-scanner"); err != nil {
		return err
	} else if !topicHasRecords(ctx, scanner, "harbour-customs") {
		for i := 0; i < 10; i++ {
			var h []kgo.RecordHeader
			if i == 0 {
				h = append(h, trace("b7ad6b7169203331"))
			}
			v := fmt.Sprintf(`{"container":"HRB%07d","cleared":%t,"inspector":"insp-%d"}`, 4400100+i, i%4 != 3, 1+i%3)
			if err := send(scanner, "harbour-customs", 0, []byte(fmt.Sprintf("HRB%07d", 4400100+i)), []byte(v), h...); err != nil {
				return err
			}
		}
	}
	boat, err := as("harbour-pilot-boat")
	if err != nil {
		return err
	}
	if !topicHasRecords(ctx, boat, "harbour-alerts") {
		for i, level := range []string{"info", "info", "warn", "info", "critical"} {
			v := fmt.Sprintf(`{"level":%q,"text":"alert %d from the breakwater"}`, level, i)
			if err := send(boat, "harbour-alerts", 0, []byte(level), []byte(v)); err != nil {
				return err
			}
		}
	}

	billing, err := as("harbour-billing-sync")
	if err != nil {
		return err
	}
	if err := seedHarbourRegistry(ctx, billing, registryURL, trace, at); err != nil {
		return err
	}
	if !topicHasRecords(ctx, billing, "harbour-ledger") {
		if err := seedHarbourLedger(ctx, addr, at); err != nil {
			return err
		}
	}

	// Four groups, each at a fixed place in a topic.
	commits := []struct {
		client, group, topic string
		offsets              map[int32]int64
	}{
		{"harbour-pilot-boat", "harbour-pilots", "harbour-arrivals", map[int32]int64{0: 4, 1: 6, 2: 10}},
		{"harbour-customs-scanner", "harbour-customs-desk", "harbour-customs", map[int32]int64{0: 10}},
		{"harbour-billing-sync", "harbour-billing", "harbour-manifests", map[int32]int64{0: 3}},
		{"harbour-gate", "harbour-dashboard", "harbour-alerts", map[int32]int64{0: 5}},
	}
	for _, c := range commits {
		cl, err := as(c.client)
		if err != nil {
			return err
		}
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group, req.Generation = c.group, -1
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = c.topic
		for _, p := range []int32{0, 1, 2} {
			off, ok := c.offsets[p]
			if !ok {
				continue
			}
			rp := kmsg.NewOffsetCommitRequestTopicPartition()
			rp.Partition, rp.Offset = p, off
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil || len(resp.Topics) != 1 {
			return fmt.Errorf("commit for %s: %v", c.group, err)
		}
		for _, p := range resp.Topics[0].Partitions {
			if p.ErrorCode != 0 {
				return fmt.Errorf("commit for %s: error %d", c.group, p.ErrorCode)
			}
		}
	}

	// ACLs: everyone may use the harbour, and two people are told no. Creating the
	// same ACL twice is not an error, so this is safe to run again.
	acls := []struct {
		rt        kmsg.ACLResourceType
		name      string
		pattern   kmsg.ACLResourcePatternType
		principal string
		op        kmsg.ACLOperation
		perm      kmsg.ACLPermissionType
	}{
		{kmsg.ACLResourceTypeTopic, "harbour-", kmsg.ACLResourcePatternTypePrefixed, "User:*", kmsg.ACLOperationAll, kmsg.ACLPermissionTypeAllow},
		{kmsg.ACLResourceTypeGroup, "harbour-", kmsg.ACLResourcePatternTypePrefixed, "User:*", kmsg.ACLOperationAll, kmsg.ACLPermissionTypeAllow},
		{kmsg.ACLResourceTypeTopic, "harbour-arrivals", kmsg.ACLResourcePatternTypeLiteral, "User:pilot", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow},
		{kmsg.ACLResourceTypeTopic, "harbour-customs", kmsg.ACLResourcePatternTypeLiteral, "User:customs", kmsg.ACLOperationWrite, kmsg.ACLPermissionTypeAllow},
		{kmsg.ACLResourceTypeTopic, "harbour-ledger", kmsg.ACLResourcePatternTypeLiteral, "User:intern", kmsg.ACLOperationWrite, kmsg.ACLPermissionTypeDeny},
		{kmsg.ACLResourceTypeGroup, "harbour-billing", kmsg.ACLResourcePatternTypeLiteral, "User:billing", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow},
	}
	req := kmsg.NewPtrCreateACLsRequest()
	for _, a := range acls {
		c := kmsg.NewCreateACLsRequestCreation()
		c.ResourceType, c.ResourceName, c.ResourcePatternType = a.rt, a.name, a.pattern
		c.Principal, c.Host, c.Operation, c.PermissionType = a.principal, "*", a.op, a.perm
		req.Creations = append(req.Creations, c)
	}
	resp, err := req.RequestWith(ctx, gate)
	if err != nil {
		return fmt.Errorf("create ACLs: %w", err)
	}
	for _, r := range resp.Results {
		if r.ErrorCode != 0 {
			return fmt.Errorf("create ACLs: error %d %v", r.ErrorCode, r.ErrorMessage)
		}
	}
	return nil
}

// seedHarbourLedger writes harbour-ledger: a committed transaction, an aborted one
// and one left open (its own producer, with a timeout that outlasts a run).
func seedHarbourLedger(ctx context.Context, addr string, at func() time.Time) error {
	newTx := func(id string) (*kgo.Client, error) {
		return newKgo(kgo.SeedBrokers(addr), kgo.ClientID("harbour-billing-sync"), kgo.TransactionalID(id),
			kgo.TransactionTimeout(10*time.Minute), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	}
	write := func(cl *kgo.Client, v string) error {
		return cl.ProduceSync(ctx, &kgo.Record{Topic: "harbour-ledger", Partition: 0, Key: []byte("invoice"), Value: []byte(v), Timestamp: at()}).FirstErr()
	}
	p, err := newTx("harbour-ledger-writer")
	if err != nil {
		return err
	}
	defer p.Close()
	for _, step := range []struct {
		values []string
		commit bool
	}{
		{[]string{`{"invoice":"INV-1001","amount":1200}`, `{"invoice":"INV-1002","amount":340}`, `{"invoice":"INV-1003","amount":87}`}, true},
		{[]string{`{"invoice":"INV-1004","amount":9999,"note":"duplicate, rolled back"}`}, false},
	} {
		if err := p.BeginTransaction(); err != nil {
			return err
		}
		for _, v := range step.values {
			if err := write(p, v); err != nil {
				return err
			}
		}
		if err := p.EndTransaction(ctx, kgo.TransactionEndTry(step.commit)); err != nil {
			return err
		}
	}
	open, err := newTx("harbour-ledger-open")
	if err != nil {
		return err
	}
	defer open.Close()
	if err := open.BeginTransaction(); err != nil {
		return err
	}
	return write(open, `{"invoice":"INV-1005","amount":410,"note":"in flight"}`)
}

// seedHarbourRegistry registers the five subjects (and the soft-deleted sixth) and
// writes harbour-manifests with them.
func seedHarbourRegistry(ctx context.Context, cl *kgo.Client, base string, trace func(string) kgo.RecordHeader, at func() time.Time) error {
	call := func(method, path string, body any) (int, []byte, error) {
		var rd io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rd = bytes.NewReader(b)
		}
		req, err := http.NewRequestWithContext(ctx, method, base+path, rd)
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		resp, err := doRegistry(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw, nil
	}
	// register returns the id; a subject that is already there is not registered
	// again (it may be READONLY by now), and its latest id is read instead.
	register := func(subject, typ, schema string, refs []map[string]any) (int, error) {
		if code, raw, err := call("GET", "/subjects/"+subject+"/versions/latest", nil); err != nil {
			return 0, err
		} else if code == 200 {
			var out struct {
				ID int `json:"id"`
			}
			if json.Unmarshal(raw, &out) == nil && out.ID != 0 {
				return out.ID, nil
			}
		}
		// Soft-deleted, and so not registered again (that would bring it back).
		if code, _, err := call("GET", "/subjects/"+subject+"/versions?deleted=true", nil); err == nil && code == 200 {
			return 0, nil
		}
		body := map[string]any{"schema": schema, "schemaType": typ}
		if len(refs) > 0 {
			body["references"] = refs
		}
		code, raw, err := call("POST", "/subjects/"+subject+"/versions", body)
		if err != nil {
			return 0, err
		}
		var out struct {
			ID int `json:"id"`
		}
		if code != 200 || json.Unmarshal(raw, &out) != nil {
			return 0, fmt.Errorf("register %s: %d %s", subject, code, raw)
		}
		return out.ID, nil
	}
	const (
		manifestV1 = `{"type":"record","name":"Manifest","namespace":"harbour","fields":[{"name":"vessel","type":"string"},{"name":"containers","type":"int"}]}`
		manifestV2 = `{"type":"record","name":"Manifest","namespace":"harbour","fields":[{"name":"vessel","type":"string"},{"name":"containers","type":"int"},{"name":"hazmat","type":["null","string"],"default":null}]}`
		vessel     = `{"type":"record","name":"Vessel","namespace":"harbour","fields":[{"name":"imo","type":"string"},{"name":"name","type":"string"}]}`
		voyage     = `{"type":"record","name":"Voyage","namespace":"harbour","fields":[{"name":"vessel","type":"harbour.Vessel"},{"name":"legs","type":"int"}]}`
		retired    = `{"type":"record","name":"Pontoon","namespace":"harbour","fields":[{"name":"id","type":"string"}]}`
		alerts     = `{"type":"record","name":"Alert","namespace":"harbour","fields":[{"name":"level","type":"string"},{"name":"text","type":"string"}]}`
	)
	// A Protobuf as a client that registers descriptors sends it: the base64 of a
	// FileDescriptorProto, not .proto text.
	fd := &descriptorpb.FileDescriptorProto{
		Name: proto.String("customs.proto"), Package: proto.String("harbour"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Declaration"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("container"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), JsonName: proto.String("container")},
				{Name: proto.String("weight_kg"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), JsonName: proto.String("weightKg")},
			},
		}},
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(fd)
	if err != nil {
		return err
	}
	customsDesc := base64.StdEncoding.EncodeToString(raw)

	had := topicHasRecords(ctx, cl, "harbour-manifests")
	id1, err := register("harbour-manifests-value", "AVRO", manifestV1, nil)
	if err != nil {
		return err
	}
	id2, err := register2("harbour-manifests-value", manifestV2, call)
	if err != nil {
		return err
	}
	if _, err := register("harbour-retired-value", "AVRO", retired, nil); err != nil { // takes an id, then goes.
		return err
	}
	if _, err := register("harbour-vessels-value", "AVRO", vessel, nil); err != nil {
		return err
	}
	if _, err := register("harbour-voyage-value", "AVRO", voyage, []map[string]any{{"name": "harbour.Vessel", "subject": "harbour-vessels-value", "version": 1}}); err != nil {
		return err
	}
	if _, err := register("harbour-customs-value", "PROTOBUF", customsDesc, nil); err != nil {
		return err
	}
	if _, err := register("harbour-alerts-value", "AVRO", alerts, nil); err != nil {
		return err
	}
	// Soft-delete the retired subject (a second delete of it is a 404, which is fine)
	// and make the alerts subject read-only.
	if code, body, err := call("DELETE", "/subjects/harbour-retired-value", nil); err != nil {
		return err
	} else if code != 200 && code != 404 {
		return fmt.Errorf("soft-delete harbour-retired-value: %d %s", code, body)
	}
	if code, body, err := call("PUT", "/mode/harbour-alerts-value", map[string]string{"mode": "READONLY"}); err != nil || code != 200 {
		return fmt.Errorf("READONLY on harbour-alerts-value: %d %s %v", code, body, err)
	}

	if had {
		return nil
	}
	write := func(schema string, id int, doc string, h ...kgo.RecordHeader) error {
		codec, err := schemaregistry.NewCodec("AVRO", schema, nil)
		if err != nil {
			return err
		}
		value, err := codec.Encode(id, doc)
		if err != nil {
			return err
		}
		return cl.ProduceSync(ctx, &kgo.Record{Topic: "harbour-manifests", Partition: 0, Key: []byte("manifest"), Value: value, Headers: h, Timestamp: at()}).FirstErr()
	}
	for i := 0; i < 3; i++ {
		var h []kgo.RecordHeader
		if i == 0 {
			h = append(h, trace("a2fb4a1d1a96d312"))
		}
		if err := write(manifestV1, id1, fmt.Sprintf(`{"vessel":"MV Tern","containers":%d}`, 120+i), h...); err != nil {
			return err
		}
	}
	for i := 0; i < 2; i++ {
		if err := write(manifestV2, id2, fmt.Sprintf(`{"vessel":"SS Halcyon","containers":%d,"hazmat":"UN1203"}`, 80+i)); err != nil {
			return err
		}
	}
	// One record that names a schema id the registry has never held: magic byte,
	// big-endian id, then bytes that decode as nothing.
	orphan := append([]byte{0}, binary.BigEndian.AppendUint32(nil, 9999)...)
	orphan = append(orphan, []byte(strings.Repeat("\x02", 6))...)
	return cl.ProduceSync(ctx, &kgo.Record{Topic: "harbour-manifests", Partition: 0, Key: []byte("orphan"), Value: orphan, Timestamp: at()}).FirstErr()
}

// register2 registers a further version of an Avro subject and returns its id.
func register2(subject, schema string, call func(method, path string, body any) (int, []byte, error)) (int, error) {
	code, raw, err := call("POST", "/subjects/"+subject+"/versions", map[string]any{"schema": schema, "schemaType": "AVRO"})
	if err != nil {
		return 0, err
	}
	var out struct {
		ID int `json:"id"`
	}
	if code != 200 || json.Unmarshal(raw, &out) != nil {
		return 0, fmt.Errorf("register %s: %d %s", subject, code, raw)
	}
	return out.ID, nil
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
