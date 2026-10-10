package protocol

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
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

// sumNothing is for an API with nothing worth saying about its target.
func sumNothing(kmsg.Request, kmsg.Response) Summary { return Summary{} }

func sumProduce(q kmsg.Request, r kmsg.Response) Summary {
	req, ok := q.(*kmsg.ProduceRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{Topics: topicNames(req.Topics, func(t kmsg.ProduceRequestTopic) string { return t.Topic })}
	parts, size := 0, 0
	for _, t := range req.Topics {
		parts += len(t.Partitions)
		for _, p := range t.Partitions {
			size += len(p.Records)
		}
	}
	s.Detail = plural(parts, "partition") + ", " + bytesText(size)
	if req.Acks == 0 {
		s.Detail += ", acks=0"
	}
	if req.TransactionID != nil {
		s.Group = *req.TransactionID
	}
	return s
}

func sumFetch(q kmsg.Request, r kmsg.Response) Summary {
	req, ok := q.(*kmsg.FetchRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{}
	for _, t := range req.Topics {
		if t.Topic != "" {
			if !slices.Contains(s.Topics, t.Topic) {
				s.Topics = append(s.Topics, t.Topic)
			}
		} else if t.TopicID != ([16]byte{}) {
			s.TopicIDs = append(s.TopicIDs, t.TopicID)
		}
	}
	size, parts := 0, 0
	if resp, ok := r.(*kmsg.FetchResponse); ok {
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				parts++
				size += len(p.RecordBatches)
			}
		}
		s.Idle = size == 0 && resp.ErrorCode == 0
	}
	if size == 0 {
		s.Detail = "nothing new"
	} else {
		s.Detail = bytesText(size) + " from " + plural(parts, "partition")
	}
	return s
}

func sumListOffsets(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.ListOffsetsRequest); ok {
		return Summary{Topics: topicNames(req.Topics, func(t kmsg.ListOffsetsRequestTopic) string { return t.Topic })}
	}
	return Summary{}
}

func sumMetadata(q kmsg.Request, _ kmsg.Response) Summary {
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
	if len(req.Topics) == 0 {
		s.Detail = "all topics"
	}
	return s
}

func sumOffsetCommit(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.OffsetCommitRequest); ok {
		parts := 0
		for _, t := range req.Topics {
			parts += len(t.Partitions)
		}
		return Summary{Group: req.Group, Detail: plural(parts, "partition"),
			Topics: topicNames(req.Topics, func(t kmsg.OffsetCommitRequestTopic) string { return t.Topic })}
	}
	return Summary{}
}

func sumOffsetFetch(q kmsg.Request, _ kmsg.Response) Summary {
	req, ok := q.(*kmsg.OffsetFetchRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{Group: req.Group, Topics: topicNames(req.Topics, func(t kmsg.OffsetFetchRequestTopic) string { return t.Topic })}
	if s.Group == "" && len(req.Groups) > 0 {
		s.Group = req.Groups[0].Group
		for _, t := range req.Groups[0].Topics {
			if t.Topic != "" && !slices.Contains(s.Topics, t.Topic) {
				s.Topics = append(s.Topics, t.Topic)
			}
		}
	}
	return s
}

func sumFindCoordinator(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.FindCoordinatorRequest); ok {
		key := req.CoordinatorKey
		if key == "" && len(req.CoordinatorKeys) > 0 {
			key = req.CoordinatorKeys[0]
		}
		kind := "group"
		if req.CoordinatorType == 1 {
			kind = "transaction"
		}
		return Summary{Group: key, Detail: kind + " coordinator"}
	}
	return Summary{}
}

func sumJoin(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.JoinGroupRequest); ok {
		s := Summary{Group: req.Group}
		if req.Reason != nil {
			s.Detail = *req.Reason
		}
		return s
	}
	return Summary{}
}

func sumHeartbeat(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.HeartbeatRequest); ok {
		return Summary{Group: req.Group, Idle: true}
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
		return Summary{Group: req.Group}
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

func sumCreateTopics(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.CreateTopicsRequest); ok {
		s := Summary{Topics: topicNames(req.Topics, func(t kmsg.CreateTopicsRequestTopic) string { return t.Topic })}
		if req.ValidateOnly {
			s.Detail = "validate only"
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

func sumInitProducerID(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.InitProducerIDRequest); ok && req.TransactionalID != nil {
		return Summary{Group: *req.TransactionalID, Detail: "transactional id"}
	}
	return Summary{Detail: "idempotent producer"}
}

func sumAddPartitionsToTxn(q kmsg.Request, _ kmsg.Response) Summary {
	req, ok := q.(*kmsg.AddPartitionsToTxnRequest)
	if !ok {
		return Summary{}
	}
	s := Summary{Group: req.TransactionalID, Topics: topicNames(req.Topics, func(t kmsg.AddPartitionsToTxnRequestTopic) string { return t.Topic })}
	for _, tx := range req.Transactions {
		if s.Group == "" {
			s.Group = tx.TransactionalID
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
		return Summary{Group: req.Group, Detail: "transaction " + req.TransactionalID}
	}
	return Summary{}
}

func sumEndTxn(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.EndTxnRequest); ok {
		verb := "abort"
		if req.Commit {
			verb = "commit"
		}
		return Summary{Group: req.TransactionalID, Detail: verb}
	}
	return Summary{}
}

func sumTxnOffsetCommit(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.TxnOffsetCommitRequest); ok {
		return Summary{Group: req.Group, Detail: "transaction " + req.TransactionalID,
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
		return Summary{Group: req.TransactionalIDs[0]}
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

func sumApiVersions(q kmsg.Request, _ kmsg.Response) Summary {
	if req, ok := q.(*kmsg.ApiVersionsRequest); ok && req.ClientSoftwareName != "" {
		sw := req.ClientSoftwareName
		if req.ClientSoftwareVersion != "" {
			sw += " " + req.ClientSoftwareVersion
		}
		return Summary{Software: sw}
	}
	return Summary{}
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
	return walkErrors(v, 0)
}

func walkErrors(v reflect.Value, depth int) int16 {
	if depth > 6 {
		return 0
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return 0
		}
		return walkErrors(v.Elem(), depth+1)
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
				if code := walkErrors(f, depth+1); code != 0 {
					return code
				}
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if code := walkErrors(v.Index(i), depth+1); code != 0 {
				return code
			}
		}
	}
	return 0
}
