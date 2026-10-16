package protocol

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// How each API describes a request, for the APIs table (API.Summary). A summariser is
// handed the decoded request and the response the handler filled in (nil when there
// was none: a Produce with acks=0, a handler that failed), and says what the request
// was about. It must not assume the response is there, or complete.

// Summariser describes one request.
type Summariser func(q kmsg.Request, r kmsg.Response) Summary

// topicNames collects the distinct non-empty names f yields, in first-seen order.
func topicNames[T any](xs []T, f func(T) string) []string {
	var out []string
	for _, x := range xs {
		if n := f(x); n != "" && !slices.Contains(out, n) {
			out = append(out, n)
		}
	}
	return out
}

func plural(n int, one string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + one + "s"
}

func bytesText(n int) string {
	switch {
	case n < 1024:
		return strconv.Itoa(n) + " B"
	case n < 1<<20:
		return fmt.Sprintf("%.1f KiB", float64(n)/1024)
	default:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	}
}

// partitionRanges writes partition numbers as "p1", "p0–p2", "p0–p2, p5", sorted and
// without repeats.
func partitionRanges(parts []int32) string {
	if len(parts) == 0 {
		return ""
	}
	ps := slices.Clone(parts)
	slices.Sort(ps)
	ps = slices.Compact(ps)
	var out []string
	for i := 0; i < len(ps); {
		j := i
		for j+1 < len(ps) && ps[j+1] == ps[j]+1 {
			j++
		}
		switch {
		case j == i:
			out = append(out, "p"+strconv.Itoa(int(ps[i])))
		case j == i+1:
			out = append(out, "p"+strconv.Itoa(int(ps[i])), "p"+strconv.Itoa(int(ps[j])))
		default:
			out = append(out, "p"+strconv.Itoa(int(ps[i]))+"–p"+strconv.Itoa(int(ps[j])))
		}
		i = j + 1
	}
	return strings.Join(out, ", ")
}

// batchCounts reads how many record batches and records a run of batches holds, from
// their headers alone: base offset (8), length (4), ... record count (4, at 57).
func batchCounts(b []byte) (batches, records int) {
	for len(b) >= 61 {
		n := int(int32(binary.BigEndian.Uint32(b[8:])))
		if n < 49 || 12+n > len(b) {
			break
		}
		batches++
		records += int(int32(binary.BigEndian.Uint32(b[57:])))
		b = b[12+n:]
	}
	return batches, records
}

// withDetails joins the non-empty parts with the board's separator.
func withDetails(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, " · ")
}

// sumNothing is for an API with nothing worth saying about its target.
func sumNothing(kmsg.Request, kmsg.Response) Summary { return Summary{} }

func sumProduce(q kmsg.Request, r kmsg.Response) Summary {
	req, ok := q.(*kmsg.ProduceRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{Topics: topicNames(req.Topics, func(t kmsg.ProduceRequestTopic) string { return t.Topic })}
	var parts []int32
	batches, size := 0, 0
	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			parts = append(parts, p.Partition)
			b, _ := batchCounts(p.Records)
			batches += b
			size += len(p.Records)
		}
	}
	where := partitionRanges(parts)
	if len(req.Topics) > 1 {
		where = plural(len(parts), "partition") // p0 of which topic? the numbers alone would mislead.
	}
	batchText := ""
	if batches > 0 {
		batchText = batchesText(batches)
	}
	s.Detail = withDetails(where, batchText, bytesText(size))
	if req.Acks == 0 {
		s.Detail += " · acks=0"
	}
	if req.TransactionID != nil {
		s.TxnID = *req.TransactionID
	}
	return s
}

func batchesText(n int) string {
	if n == 1 {
		return "1 batch"
	}
	return strconv.Itoa(n) + " batches"
}

func sumFetch(q kmsg.Request, r kmsg.Response) Summary {
	req, ok := q.(*kmsg.FetchRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{}
	var parts []int32
	var from int64 = -1
	for _, t := range req.Topics {
		if t.Topic != "" {
			if !slices.Contains(s.Topics, t.Topic) {
				s.Topics = append(s.Topics, t.Topic)
			}
		} else if t.TopicID != ([16]byte{}) {
			s.TopicIDs = append(s.TopicIDs, t.TopicID)
		}
		for _, p := range t.Partitions {
			parts = append(parts, p.Partition)
			if from < 0 {
				from = p.FetchOffset
			}
		}
	}
	size, records := 0, 0
	if resp, ok := r.(*kmsg.FetchResponse); ok {
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				size += len(p.RecordBatches)
				_, n := batchCounts(p.RecordBatches)
				records += n
			}
		}
		s.Idle = size == 0 && resp.ErrorCode == 0
	}
	where := partitionRanges(parts)
	if len(req.Topics) > 1 {
		where = plural(len(parts), "partition")
	}
	switch {
	case size == 0:
		wait := ""
		if req.MaxWaitMillis > 0 {
			wait = ", waiting up to " + shortMillis(req.MaxWaitMillis)
		}
		s.Detail = "empty" + wait
	case len(parts) == 1 && from >= 0:
		s.Detail = withDetails(where+" from "+groupInt(from), plural(records, "record"))
	default:
		s.Detail = withDetails(where, plural(records, "record"), bytesText(size))
	}
	return s
}

// shortMillis is "500 ms" or "30 s".
func shortMillis(ms int32) string {
	if ms >= 1000 && ms%1000 == 0 {
		return strconv.Itoa(int(ms/1000)) + " s"
	}
	return strconv.Itoa(int(ms)) + " ms"
}

// groupInt writes 4812 as "4,812".
func groupInt(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := ""
	if n < 0 {
		neg, s = "-", s[1:]
	}
	var out []byte
	for i := range len(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	return neg + string(out)
}

func sumListOffsets(q kmsg.Request, _ kmsg.Response) Summary {
	req, ok := q.(*kmsg.ListOffsetsRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{Topics: topicNames(req.Topics, func(t kmsg.ListOffsetsRequestTopic) string { return t.Topic })}
	// What it asks for: where each partition ends, begins or was at a time. It is a
	// consumer finding where to start, and the answer is the offset.
	var parts []int32
	var stamps []int64
	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			parts = append(parts, p.Partition)
			if !slices.Contains(stamps, p.Timestamp) {
				stamps = append(stamps, p.Timestamp)
			}
		}
	}
	where := partitionRanges(parts)
	if len(req.Topics) > 1 {
		where = plural(len(parts), "partition")
	}
	what := ""
	if len(stamps) == 1 {
		switch stamps[0] {
		case -1:
			what = "latest offset"
		case -2:
			what = "earliest offset"
		case -3:
			what = "max timestamp"
		default:
			what = "offset at a time"
		}
	} else if len(stamps) > 1 {
		what = "offsets"
	}
	s.Detail = withDetails(where, what)
	return s
}

func sumMetadata(q kmsg.Request, r kmsg.Response) Summary {
	req, ok := q.(*kmsg.MetadataRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{}
	for _, t := range req.Topics {
		if t.Topic != nil && *t.Topic != "" {
			s.Topics = append(s.Topics, *t.Topic)
		} else if t.TopicID != ([16]byte{}) {
			s.TopicIDs = append(s.TopicIDs, t.TopicID)
		}
	}
	// Whether the request allowed the broker to create what it named. Whether the broker
	// does is its own setting, which the page writes (A8).
	s.AutoCreate = req.AllowAutoTopicCreation && len(req.Topics) > 0
	if len(req.Topics) == 0 {
		s.Detail = "all topics"
	}
	// With the answer, say what came back: how many topics, or why a named one did not.
	if resp, ok := r.(*kmsg.MetadataResponse); ok {
		var bad int16
		for _, t := range resp.Topics {
			if t.ErrorCode != 0 && bad == 0 {
				bad = t.ErrorCode
			}
		}
		if bad != 0 && len(req.Topics) > 0 {
			s.Detail = ErrorPhrase(bad)
		} else {
			s.Detail = plural(len(resp.Topics), "topic")
		}
	}
	return s
}

func sumOffsetCommit(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.OffsetCommitRequest); ok {
		var parts []int32
		for _, t := range req.Topics {
			for _, p := range t.Partitions {
				parts = append(parts, p.Partition)
			}
		}
		detail := partitionRanges(parts)
		if len(req.Topics) > 1 {
			detail = plural(len(parts), "partition")
		}
		s := Summary{Group: req.Group, Parts: detail,
			Topics: topicNames(req.Topics, func(t kmsg.OffsetCommitRequestTopic) string { return t.Topic })}
		if req.Generation > 0 {
			s.Gen, s.HasGen = req.Generation, true
			detail = withDetails(detail, "generation "+strconv.Itoa(int(req.Generation)))
		}
		s.Detail = detail
		return s
	}
	return Summary{}
}

func sumOffsetFetch(q kmsg.Request, _ kmsg.Response) Summary {
	req, ok := q.(*kmsg.OffsetFetchRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{Group: req.Group, Topics: topicNames(req.Topics, func(t kmsg.OffsetFetchRequestTopic) string { return t.Topic })}
	var parts []int32
	for _, t := range req.Topics {
		parts = append(parts, t.Partitions...)
	}
	named := len(req.Topics) > 0
	if s.Group == "" && len(req.Groups) > 0 {
		s.Group = req.Groups[0].Group
		named = len(req.Groups[0].Topics) > 0
		for _, t := range req.Groups[0].Topics {
			parts = append(parts, t.Partitions...)
			if t.Topic != "" && !slices.Contains(s.Topics, t.Topic) {
				s.Topics = append(s.Topics, t.Topic)
			}
		}
	}
	// Where the group's consumers were: the partitions asked about, or every committed
	// offset of the group when none were named.
	switch {
	case !named:
		s.Detail = "all committed offsets"
	case len(s.Topics) > 1:
		s.Detail = plural(len(parts), "partition")
	default:
		s.Detail = partitionRanges(parts)
	}
	return s
}

func sumFindCoordinator(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.FindCoordinatorRequest); ok {
		key := req.CoordinatorKey
		if key == "" && len(req.CoordinatorKeys) > 0 {
			key = req.CoordinatorKeys[0]
		}
		if req.CoordinatorType == 1 {
			return Summary{TxnID: key, Detail: "transaction coordinator"}
		}
		return Summary{Group: key, Detail: "group coordinator"}
	}
	return Summary{}
}

func sumJoin(q kmsg.Request, r kmsg.Response) Summary {
	req, ok := q.(*kmsg.JoinGroupRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{Group: req.Group}
	var detail string
	if resp, ok := r.(*kmsg.JoinGroupResponse); ok {
		switch {
		case resp.ErrorCode == kerr.MemberIDRequired.Code:
			detail = "a new member asks in; it is given an id to join with"
		case resp.ErrorCode == 0 && resp.Generation >= 0:
			detail = "generation " + strconv.Itoa(int(resp.Generation))
			if len(resp.Members) > 0 {
				detail += " · " + plural(len(resp.Members), "member") + ", leader"
			}
		}
	}
	reason := ""
	if req.Reason != nil {
		reason = *req.Reason
	}
	s.Detail = withDetails(detail, reason)
	return s
}

func sumHeartbeat(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.HeartbeatRequest); ok {
		return Summary{Group: req.Group, Idle: true, Detail: "generation " + strconv.Itoa(int(req.Generation))}
	}
	return Summary{}
}

func sumLeave(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.LeaveGroupRequest); ok {
		return Summary{Group: req.Group}
	}
	return Summary{}
}

func sumSync(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.SyncGroupRequest); ok {
		detail := "generation " + strconv.Itoa(int(req.Generation))
		if len(req.GroupAssignment) > 0 {
			detail += " · hands out partitions to " + plural(len(req.GroupAssignment), "member")
		}
		return Summary{Group: req.Group, Detail: detail}
	}
	return Summary{}
}

func sumGroups(q kmsg.Request, _ kmsg.Response) Summary {
	var groups []string
	switch req := q.(type) {
	case *kmsg.DescribeGroupsRequest:
		groups = req.Groups
	case *kmsg.DeleteGroupsRequest:
		groups = req.Groups
	case *kmsg.ConsumerGroupDescribeRequest:
		groups = req.Groups
	}
	if len(groups) == 0 {
		return Summary{}
	}
	s := Summary{Group: groups[0]}
	if len(groups) > 1 {
		s.Detail = plural(len(groups), "group")
	}
	return s
}

func sumCreateTopics(q kmsg.Request, r kmsg.Response) Summary {
	if req, ok := q.(*kmsg.CreateTopicsRequest); ok {
		s := Summary{Topics: topicNames(req.Topics, func(t kmsg.CreateTopicsRequestTopic) string { return t.Topic })}
		// A refusal says why, in the words of the table; a creation says what it made.
		var bad int16
		if resp, ok := r.(*kmsg.CreateTopicsResponse); ok {
			for _, t := range resp.Topics {
				if t.ErrorCode != 0 && bad == 0 {
					bad = t.ErrorCode
				}
			}
		}
		switch {
		case bad != 0:
			s.Detail = ErrorPhrase(bad)
		case len(req.Topics) == 1:
			parts := int(req.Topics[0].NumPartitions)
			if parts > 0 {
				s.Detail = plural(parts, "partition")
			}
		case len(req.Topics) > 1:
			s.Detail = plural(len(req.Topics), "topic")
		}
		if req.ValidateOnly {
			s.Detail = withDetails(s.Detail, "validate only")
		}
		return s
	}
	return Summary{}
}

func sumDeleteTopics(q kmsg.Request, _ kmsg.Response) Summary {
	req, ok := q.(*kmsg.DeleteTopicsRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{Topics: slices.Clone(req.TopicNames)}
	for _, t := range req.Topics {
		if t.Topic != nil && *t.Topic != "" {
			s.Topics = append(s.Topics, *t.Topic)
		} else if t.TopicID != ([16]byte{}) {
			s.TopicIDs = append(s.TopicIDs, t.TopicID)
		}
	}
	return s
}

func sumDeleteRecords(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.DeleteRecordsRequest); ok {
		return Summary{Topics: topicNames(req.Topics, func(t kmsg.DeleteRecordsRequestTopic) string { return t.Topic })}
	}
	return Summary{}
}

func sumInitProducerID(q kmsg.Request, r kmsg.Response) Summary {
	req, ok := q.(*kmsg.InitProducerIDRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{Detail: "idempotent producer"}
	if req.TransactionalID != nil {
		s.TxnID = *req.TransactionalID
		s.Detail = "transactional producer"
	}
	if resp, ok := r.(*kmsg.InitProducerIDResponse); ok && resp.ErrorCode == 0 {
		s.Detail = "producer id " + groupInt(resp.ProducerID) + ", epoch " + strconv.Itoa(int(resp.ProducerEpoch))
		if req.ProducerEpoch >= 0 && req.ProducerID >= 0 {
			s.Detail = "epoch " + strconv.Itoa(int(req.ProducerEpoch)) + " → " + strconv.Itoa(int(resp.ProducerEpoch))
		}
		if s.TxnID != "" {
			s.Detail = withDetails("transactional", s.Detail)
		}
	}
	return s
}

func sumAddPartitionsToTxn(q kmsg.Request, _ kmsg.Response) Summary {
	req, ok := q.(*kmsg.AddPartitionsToTxnRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{TxnID: req.TransactionalID, Topics: topicNames(req.Topics, func(t kmsg.AddPartitionsToTxnRequestTopic) string { return t.Topic })}
	for _, tx := range req.Transactions {
		if s.TxnID == "" {
			s.TxnID = tx.TransactionalID
		}
		for _, t := range tx.Topics {
			if t.Topic != "" && !slices.Contains(s.Topics, t.Topic) {
				s.Topics = append(s.Topics, t.Topic)
			}
		}
	}
	return s
}

func sumAddOffsetsToTxn(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.AddOffsetsToTxnRequest); ok {
		return Summary{Group: req.Group, TxnID: req.TransactionalID, Detail: "adds the group's offsets to the transaction"}
	}
	return Summary{}
}

func sumEndTxn(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.EndTxnRequest); ok {
		verb := "abort"
		if req.Commit {
			verb = "commit"
		}
		return Summary{TxnID: req.TransactionalID, Detail: verb}
	}
	return Summary{}
}

func sumTxnOffsetCommit(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.TxnOffsetCommitRequest); ok {
		return Summary{Group: req.Group, TxnID: req.TransactionalID, Detail: "offsets inside the transaction",
			Topics: topicNames(req.Topics, func(t kmsg.TxnOffsetCommitRequestTopic) string { return t.Topic })}
	}
	return Summary{}
}

func sumConfigs(q kmsg.Request, _ kmsg.Response) Summary {
	switch req := q.(type) {
	case *kmsg.DescribeConfigsRequest:
		var topics []string
		for _, res := range req.Resources {
			if res.ResourceType == kmsg.ConfigResourceTypeTopic && !slices.Contains(topics, res.ResourceName) {
				topics = append(topics, res.ResourceName)
			}
		}
		return Summary{Topics: topics}
	case *kmsg.AlterConfigsRequest:
		var topics []string
		for _, res := range req.Resources {
			if res.ResourceType == kmsg.ConfigResourceTypeTopic && !slices.Contains(topics, res.ResourceName) {
				topics = append(topics, res.ResourceName)
			}
		}
		return Summary{Topics: topics}
	case *kmsg.IncrementalAlterConfigsRequest:
		var topics []string
		for _, res := range req.Resources {
			if res.ResourceType == kmsg.ConfigResourceTypeTopic && !slices.Contains(topics, res.ResourceName) {
				topics = append(topics, res.ResourceName)
			}
		}
		return Summary{Topics: topics}
	}
	return Summary{}
}

func sumCreatePartitions(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.CreatePartitionsRequest); ok {
		return Summary{Topics: topicNames(req.Topics, func(t kmsg.CreatePartitionsRequestTopic) string { return t.Topic })}
	}
	return Summary{}
}

func sumOffsetDelete(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.OffsetDeleteRequest); ok {
		return Summary{Group: req.Group, Topics: topicNames(req.Topics, func(t kmsg.OffsetDeleteRequestTopic) string { return t.Topic })}
	}
	return Summary{}
}

func sumDescribeProducers(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.DescribeProducersRequest); ok {
		return Summary{Topics: topicNames(req.Topics, func(t kmsg.DescribeProducersRequestTopic) string { return t.Topic })}
	}
	return Summary{}
}

func sumDescribeTransactions(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.DescribeTransactionsRequest); ok && len(req.TransactionalIDs) > 0 {
		return Summary{TxnID: req.TransactionalIDs[0]}
	}
	return Summary{}
}

func sumGroupHeartbeat(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.ConsumerGroupHeartbeatRequest); ok {
		s := Summary{Group: req.Group, Topics: slices.Clone(req.SubscribedTopicNames)}
		// Epoch 0 joins, -1 leaves; anything else is the steady beat.
		switch {
		case req.MemberEpoch == 0:
			s.Detail = "join"
		case req.MemberEpoch < 0:
			s.Detail = "leave"
		default:
			s.Idle = true
		}
		return s
	}
	return Summary{}
}

func sumTelemetry(q kmsg.Request, r kmsg.Response) Summary {
	switch req := q.(type) {
	case *kmsg.GetTelemetrySubscriptionsRequest:
		if req.ClientInstanceID == ([16]byte{}) {
			return Summary{Detail: "subscribes (a new client instance)"}
		}
		return Summary{Detail: "asks what to report"}
	case *kmsg.PushTelemetryRequest:
		s := Summary{Detail: bytesText(len(req.Metrics)) + " of metrics"}
		if req.Terminating {
			s.Detail += ", client is closing"
		} else {
			s.Idle = true // every few seconds for as long as the client lives: one row, counted.
		}
		return s
	}
	return Summary{}
}

func sumApiVersions(q kmsg.Request, r kmsg.Response) Summary {
	req, ok := q.(*kmsg.ApiVersionsRequest)
	if !ok {
		return Summary{}
	}
	// A name or version Kafka does not take is refused, and is not what the client is
	// then known as: the row says what was sent and why it was not taken.
	if resp, ok := r.(*kmsg.ApiVersionsResponse); ok && resp.ErrorCode == kerr.InvalidRequest.Code {
		short := func(s string) string {
			if len(s) > 48 {
				s = strings.ToValidUTF8(s[:48], "") + "…"
			}
			return strconv.Quote(s)
		}
		return Summary{Detail: "client software " + short(req.ClientSoftwareName) + " version " + short(req.ClientSoftwareVersion) +
			": each must be letters and digits, with dots and dashes only between them"}
	}
	if req.ClientSoftwareName == "" {
		return Summary{}
	}
	sw := ClientSoftware(req.ClientSoftwareName, req.ClientSoftwareVersion)
	return Summary{Software: sw, Detail: sw}
}

// firstError is the first non-zero error code in a response, found by walking its
// fields: every response puts its errors in a field called ErrorCode, at the top or
// inside the topic and partition lists. Walking them once is shorter and harder to
// get wrong than a case per response type, and a response that gains a nested level
// is covered without anyone remembering.
func firstError(resp kmsg.Response) int16 {
	if resp == nil {
		return 0
	}
	v := reflect.ValueOf(resp)
	if v.Kind() == reflect.Pointer && v.IsNil() {
		return 0
	}
	budget := maxErrorWalk
	return walkErrors(v, &budget)
}

// maxErrorWalk is how many values firstError looks at in one response. The types are
// finite trees, so there is no depth to cap (a cap of six hid the codes of OffsetFetch
// v8, AddPartitionsToTxn v4 and others: TRF-7); what grows is the response, and a
// response with more than this many values is not searched past it.
const maxErrorWalk = 1 << 18

func walkErrors(v reflect.Value, budget *int) int16 {
	if *budget--; *budget < 0 {
		return 0
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return 0
		}
		return walkErrors(v.Elem(), budget)
	case reflect.Struct:
		t := v.Type()
		// The top-level error comes before the nested ones: it is the one that
		// explains the others.
		if f := v.FieldByName("ErrorCode"); f.IsValid() && f.Kind() == reflect.Int16 && f.Int() != 0 {
			return int16(f.Int())
		}
		for i := 0; i < v.NumField(); i++ {
			if !t.Field(i).IsExported() {
				continue
			}
			f := v.Field(i)
			if f.Kind() == reflect.Slice || f.Kind() == reflect.Struct || f.Kind() == reflect.Pointer {
				if f.Kind() == reflect.Slice && f.Type().Elem().Kind() != reflect.Struct {
					continue // a []byte of records, a []int32: no errors in there.
				}
				if code := walkErrors(f, budget); code != 0 {
					return code
				}
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if code := walkErrors(v.Index(i), budget); code != 0 {
				return code
			}
		}
	}
	return 0
}
