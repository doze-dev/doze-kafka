package protocol

// What a request said and how each partition of it went, for the Traffic inspector.
//
// The recorder keeps a row per request and nothing of the request itself. The
// inspector wants more than a row has: the partitions and their results, and the
// decoded request. They are built once, when the row is made, from the request the
// handler decoded and the answer it gave, and they are bounded: a request may name
// thousands of partitions, and the recorder holds two thousand rows.
//
// Never record bytes, never a header value, never a secret. Every byte slice is
// reported as its length, and a record batch as "1 batch, 2 records, 412 B"; a
// configuration value whose name says it is a secret is hidden.

import (
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// MaxFactPartitions is how many per-partition results are kept for one request.
const MaxFactPartitions = 16

// maxDecodedBytes is the size the decoded-fields text is held under.
const maxDecodedBytes = 1024

// PartResult is how one partition of a request went.
type PartResult struct {
	Topic     string
	Partition int32
	// Offset is the base offset a produce was given, the offset a fetch began at, or
	// the offset a commit stored; HasOffset says it is meaningful.
	Offset    int64
	HasOffset bool
	// Records and Bytes are what a produce carried or a fetch returned.
	Records, Bytes int
	ErrorCode      int16
}

// Facts is what the inspector shows beyond a row.
type Facts struct {
	// Acks is a produce's acks (-1, 0 or 1); HasAcks says the request had one.
	Acks    int16
	HasAcks bool
	// Partitions are the per-partition results, at most MaxFactPartitions.
	Partitions     []PartResult
	PartitionsMore int
	// Decoded is the request's fields as JSON, in the names of Apache Kafka's JSON
	// specification of the message ("TransactionalId", "TopicData"), with bytes reduced
	// to their size. At most about a kilobyte.
	Decoded string
}

// DescribeFacts reads what it can from a decoded request and the response the handler
// filled in (nil for a request that was not answered).
func DescribeFacts(q kmsg.Request, r kmsg.Response) Facts { return DescribeFactsNamed(q, r, nil) }

// DescribeFactsNamed is DescribeFacts for a caller that can name a topic from its id:
// a fetch from v13 names its topics by id alone, and a partition listed without its
// topic says nothing. The name the broker put in its answer is used first; resolve
// (which may be nil) is asked only when there is none.
//
// The cost is bounded by what is kept. Only the first MaxFactPartitions partitions are
// read and paired with their answers; the rest are counted from the lengths of the
// lists. It was not always so: every partition was looked up in every partition of the
// response, and a fetch of 4,000 partitions took 48 ms to describe.
func DescribeFactsNamed(q kmsg.Request, r kmsg.Response, resolve func([16]byte) string) Facts {
	var f Facts
	if q == nil {
		return f
	}
	switch req := q.(type) {
	case *kmsg.ProduceRequest:
		f.Acks, f.HasAcks = req.Acks, true
		resp, _ := r.(*kmsg.ProduceResponse)
		var answers []kmsg.ProduceResponseTopic
		if resp != nil {
			answers = resp.Topics
		}
		kept(&f, req.Topics, func(t *kmsg.ProduceRequestTopic) int { return len(t.Partitions) },
			func(ti, pi int, t *kmsg.ProduceRequestTopic) PartResult {
				p := &t.Partitions[pi]
				_, n := batchCounts(p.Records)
				pr := PartResult{Topic: topicName(t.Topic, t.TopicID, resolve), Partition: p.Partition, Records: n, Bytes: len(p.Records)}
				rt, rp := answerFor(answers, ti, pi, p.Partition,
					func(rt *kmsg.ProduceResponseTopic) bool { return sameTopic(t.Topic, t.TopicID, rt.Topic, rt.TopicID) },
					func(rt *kmsg.ProduceResponseTopic) []kmsg.ProduceResponseTopicPartition { return rt.Partitions },
					func(rp *kmsg.ProduceResponseTopicPartition) int32 { return rp.Partition })
				if rp != nil {
					pr.ErrorCode = rp.ErrorCode
					if rp.ErrorCode == 0 {
						pr.Offset, pr.HasOffset = rp.BaseOffset, true
					}
					if pr.Topic == "" {
						pr.Topic = rt.Topic
					}
				}
				return pr
			})
	case *kmsg.FetchRequest:
		resp, _ := r.(*kmsg.FetchResponse)
		var answers []kmsg.FetchResponseTopic
		if resp != nil {
			answers = resp.Topics
		}
		kept(&f, req.Topics, func(t *kmsg.FetchRequestTopic) int { return len(t.Partitions) },
			func(ti, pi int, t *kmsg.FetchRequestTopic) PartResult {
				p := &t.Partitions[pi]
				pr := PartResult{Topic: t.Topic, Partition: p.Partition, Offset: p.FetchOffset, HasOffset: true}
				rt, rp := answerFor(answers, ti, pi, p.Partition,
					func(rt *kmsg.FetchResponseTopic) bool { return sameTopic(t.Topic, t.TopicID, rt.Topic, rt.TopicID) },
					func(rt *kmsg.FetchResponseTopic) []kmsg.FetchResponseTopicPartition { return rt.Partitions },
					func(rp *kmsg.FetchResponseTopicPartition) int32 { return rp.Partition })
				if rp != nil {
					pr.ErrorCode = rp.ErrorCode
					_, pr.Records = batchCounts(rp.RecordBatches)
					pr.Bytes = len(rp.RecordBatches)
					if pr.Topic == "" {
						pr.Topic = rt.Topic // the broker resolved the id to answer it.
					}
				}
				if pr.Topic == "" {
					pr.Topic = topicName("", t.TopicID, resolve)
				}
				return pr
			})
	case *kmsg.OffsetCommitRequest:
		resp, _ := r.(*kmsg.OffsetCommitResponse)
		var answers []kmsg.OffsetCommitResponseTopic
		if resp != nil {
			answers = resp.Topics
		}
		kept(&f, req.Topics, func(t *kmsg.OffsetCommitRequestTopic) int { return len(t.Partitions) },
			func(ti, pi int, t *kmsg.OffsetCommitRequestTopic) PartResult {
				p := &t.Partitions[pi]
				pr := PartResult{Topic: topicName(t.Topic, t.TopicID, resolve), Partition: p.Partition, Offset: p.Offset, HasOffset: true}
				rt, rp := answerFor(answers, ti, pi, p.Partition,
					func(rt *kmsg.OffsetCommitResponseTopic) bool {
						return sameTopic(t.Topic, t.TopicID, rt.Topic, rt.TopicID)
					},
					func(rt *kmsg.OffsetCommitResponseTopic) []kmsg.OffsetCommitResponseTopicPartition {
						return rt.Partitions
					},
					func(rp *kmsg.OffsetCommitResponseTopicPartition) int32 { return rp.Partition })
				if rp != nil {
					pr.ErrorCode = rp.ErrorCode
					if pr.Topic == "" {
						pr.Topic = rt.Topic
					}
				}
				return pr
			})
	case *kmsg.TxnOffsetCommitRequest:
		resp, _ := r.(*kmsg.TxnOffsetCommitResponse)
		var answers []kmsg.TxnOffsetCommitResponseTopic
		if resp != nil {
			answers = resp.Topics
		}
		kept(&f, req.Topics, func(t *kmsg.TxnOffsetCommitRequestTopic) int { return len(t.Partitions) },
			func(ti, pi int, t *kmsg.TxnOffsetCommitRequestTopic) PartResult {
				p := &t.Partitions[pi]
				pr := PartResult{Topic: t.Topic, Partition: p.Partition, Offset: p.Offset, HasOffset: true}
				_, rp := answerFor(answers, ti, pi, p.Partition,
					func(rt *kmsg.TxnOffsetCommitResponseTopic) bool { return rt.Topic == t.Topic },
					func(rt *kmsg.TxnOffsetCommitResponseTopic) []kmsg.TxnOffsetCommitResponseTopicPartition {
						return rt.Partitions
					},
					func(rp *kmsg.TxnOffsetCommitResponseTopicPartition) int32 { return rp.Partition })
				if rp != nil {
					pr.ErrorCode = rp.ErrorCode
				}
				return pr
			})
	case *kmsg.ListOffsetsRequest:
		resp, _ := r.(*kmsg.ListOffsetsResponse)
		var answers []kmsg.ListOffsetsResponseTopic
		if resp != nil {
			answers = resp.Topics
		}
		kept(&f, req.Topics, func(t *kmsg.ListOffsetsRequestTopic) int { return len(t.Partitions) },
			func(ti, pi int, t *kmsg.ListOffsetsRequestTopic) PartResult {
				p := &t.Partitions[pi]
				pr := PartResult{Topic: t.Topic, Partition: p.Partition}
				_, rp := answerFor(answers, ti, pi, p.Partition,
					func(rt *kmsg.ListOffsetsResponseTopic) bool { return rt.Topic == t.Topic },
					func(rt *kmsg.ListOffsetsResponseTopic) []kmsg.ListOffsetsResponseTopicPartition { return rt.Partitions },
					func(rp *kmsg.ListOffsetsResponseTopicPartition) int32 { return rp.Partition })
				if rp != nil {
					pr.ErrorCode = rp.ErrorCode
					switch {
					case rp.ErrorCode != 0:
					case resp.Version == 0:
						// v0 answers a list of offsets, newest first; the first is the one asked for.
						if len(rp.OldStyleOffsets) > 0 {
							pr.Offset, pr.HasOffset = rp.OldStyleOffsets[0], true
						}
					case rp.Offset >= 0: // -1: no offset at that time.
						pr.Offset, pr.HasOffset = rp.Offset, true
					}
				}
				return pr
			})
	case *kmsg.DeleteRecordsRequest:
		resp, _ := r.(*kmsg.DeleteRecordsResponse)
		var answers []kmsg.DeleteRecordsResponseTopic
		if resp != nil {
			answers = resp.Topics
		}
		kept(&f, req.Topics, func(t *kmsg.DeleteRecordsRequestTopic) int { return len(t.Partitions) },
			func(ti, pi int, t *kmsg.DeleteRecordsRequestTopic) PartResult {
				p := &t.Partitions[pi]
				pr := PartResult{Topic: t.Topic, Partition: p.Partition}
				_, rp := answerFor(answers, ti, pi, p.Partition,
					func(rt *kmsg.DeleteRecordsResponseTopic) bool { return rt.Topic == t.Topic },
					func(rt *kmsg.DeleteRecordsResponseTopic) []kmsg.DeleteRecordsResponseTopicPartition {
						return rt.Partitions
					},
					func(rp *kmsg.DeleteRecordsResponseTopicPartition) int32 { return rp.Partition })
				if rp != nil {
					pr.ErrorCode = rp.ErrorCode
					if rp.ErrorCode == 0 {
						// Where the partition begins now: the request's own offset may be -1,
						// "up to the end", and this is what that came to.
						pr.Offset, pr.HasOffset = rp.LowWatermark, true
					}
				}
				return pr
			})
	case *kmsg.OffsetDeleteRequest:
		resp, _ := r.(*kmsg.OffsetDeleteResponse)
		var answers []kmsg.OffsetDeleteResponseTopic
		if resp != nil {
			answers = resp.Topics
		}
		kept(&f, req.Topics, func(t *kmsg.OffsetDeleteRequestTopic) int { return len(t.Partitions) },
			func(ti, pi int, t *kmsg.OffsetDeleteRequestTopic) PartResult {
				p := &t.Partitions[pi]
				pr := PartResult{Topic: t.Topic, Partition: p.Partition}
				_, rp := answerFor(answers, ti, pi, p.Partition,
					func(rt *kmsg.OffsetDeleteResponseTopic) bool { return rt.Topic == t.Topic },
					func(rt *kmsg.OffsetDeleteResponseTopic) []kmsg.OffsetDeleteResponseTopicPartition {
						return rt.Partitions
					},
					func(rp *kmsg.OffsetDeleteResponseTopicPartition) int32 { return rp.Partition })
				if rp != nil {
					pr.ErrorCode = rp.ErrorCode
				}
				return pr
			})
	case *kmsg.OffsetFetchRequest:
		offsetFetchFacts(&f, req, r, resolve)
	}
	f.Decoded = decodedFields(q)
	return f
}

// offsetFetchFacts lists the committed offsets a group was asked for. The request has
// two shapes (one group at the top to v7, a list of groups from v8) and may name no
// partition at all, which asks for every one the group has committed: then what came
// back is the list. A request for several groups lists nothing, since a partition's
// row has no place to say which group it is of.
func offsetFetchFacts(f *Facts, req *kmsg.OffsetFetchRequest, r kmsg.Response, resolve func([16]byte) string) {
	resp, _ := r.(*kmsg.OffsetFetchResponse)
	committed := func(pr *PartResult, code int16, offset int64) {
		pr.ErrorCode = code
		if code == 0 && offset >= 0 { // -1: the group has committed nothing there.
			pr.Offset, pr.HasOffset = offset, true
		}
	}
	if len(req.Groups) == 0 {
		var answers []kmsg.OffsetFetchResponseTopic
		if resp != nil {
			answers = resp.Topics
		}
		if len(req.Topics) == 0 {
			kept(f, answers, func(t *kmsg.OffsetFetchResponseTopic) int { return len(t.Partitions) },
				func(_, pi int, t *kmsg.OffsetFetchResponseTopic) PartResult {
					pr := PartResult{Topic: t.Topic, Partition: t.Partitions[pi].Partition}
					committed(&pr, t.Partitions[pi].ErrorCode, t.Partitions[pi].Offset)
					return pr
				})
			return
		}
		kept(f, req.Topics, func(t *kmsg.OffsetFetchRequestTopic) int { return len(t.Partitions) },
			func(ti, pi int, t *kmsg.OffsetFetchRequestTopic) PartResult {
				pr := PartResult{Topic: t.Topic, Partition: t.Partitions[pi]}
				_, rp := answerFor(answers, ti, pi, pr.Partition,
					func(rt *kmsg.OffsetFetchResponseTopic) bool { return rt.Topic == t.Topic },
					func(rt *kmsg.OffsetFetchResponseTopic) []kmsg.OffsetFetchResponseTopicPartition { return rt.Partitions },
					func(rp *kmsg.OffsetFetchResponseTopicPartition) int32 { return rp.Partition })
				if rp != nil {
					committed(&pr, rp.ErrorCode, rp.Offset)
				}
				return pr
			})
		return
	}
	if len(req.Groups) != 1 {
		return
	}
	g := &req.Groups[0]
	var answers []kmsg.OffsetFetchResponseGroupTopic
	if resp != nil {
		for i := range resp.Groups {
			if resp.Groups[i].Group == g.Group {
				answers = resp.Groups[i].Topics
				break
			}
		}
	}
	if len(g.Topics) == 0 {
		kept(f, answers, func(t *kmsg.OffsetFetchResponseGroupTopic) int { return len(t.Partitions) },
			func(_, pi int, t *kmsg.OffsetFetchResponseGroupTopic) PartResult {
				pr := PartResult{Topic: topicName(t.Topic, t.TopicID, resolve), Partition: t.Partitions[pi].Partition}
				committed(&pr, t.Partitions[pi].ErrorCode, t.Partitions[pi].Offset)
				return pr
			})
		return
	}
	kept(f, g.Topics, func(t *kmsg.OffsetFetchRequestGroupTopic) int { return len(t.Partitions) },
		func(ti, pi int, t *kmsg.OffsetFetchRequestGroupTopic) PartResult {
			pr := PartResult{Topic: topicName(t.Topic, t.TopicID, resolve), Partition: t.Partitions[pi]}
			rt, rp := answerFor(answers, ti, pi, pr.Partition,
				func(rt *kmsg.OffsetFetchResponseGroupTopic) bool {
					return sameTopic(t.Topic, t.TopicID, rt.Topic, rt.TopicID)
				},
				func(rt *kmsg.OffsetFetchResponseGroupTopic) []kmsg.OffsetFetchResponseGroupTopicPartition {
					return rt.Partitions
				},
				func(rp *kmsg.OffsetFetchResponseGroupTopicPartition) int32 { return rp.Partition })
			if rp != nil {
				committed(&pr, rp.ErrorCode, rp.Offset)
				if pr.Topic == "" {
					pr.Topic = rt.Topic
				}
			}
			return pr
		})
}

// kept walks a request's topics and their partitions, hands the first
// MaxFactPartitions of them to describe, and counts the rest from the lengths of the
// lists without reading them.
func kept[T any](f *Facts, topics []T, partitions func(*T) int, describe func(ti, pi int, t *T) PartResult) {
	total := 0
	for ti := range topics {
		t := &topics[ti]
		n := partitions(t)
		total += n
		for pi := 0; pi < n && len(f.Partitions) < MaxFactPartitions; pi++ {
			f.Partitions = append(f.Partitions, describe(ti, pi, t))
		}
	}
	f.PartitionsMore = total - len(f.Partitions)
}

// sameTopic says whether an answered topic is the asked one. A version that names
// topics by id (Fetch from v13) carries no name in the request, and one that names
// them by name carries no id, so the two must not be compared together: with both ids
// zero "the ids are equal" made every topic the same topic.
func sameTopic(askedName string, askedID [16]byte, name string, id [16]byte) bool {
	if askedID != ([16]byte{}) {
		return id == askedID
	}
	return name == askedName
}

// topicName is a topic's name, from the request or, for one named by id, from resolve.
func topicName(name string, id [16]byte, resolve func([16]byte) string) string {
	if name == "" && resolve != nil && id != ([16]byte{}) {
		return resolve(id)
	}
	return name
}

// answerFor finds the answer to the partition asked at position (ti, pi). The broker
// answers in the order it was asked, so it is almost always at the same position, and
// that is looked at first: one comparison, however many partitions there are. Only
// when it is not there (a response cut short, a handler that answers in another
// order) is the response searched, and the search ends at the first match.
func answerFor[RT, RP any](answers []RT, ti, pi int, partition int32,
	same func(*RT) bool, partitions func(*RT) []RP, number func(*RP) int32) (*RT, *RP) {
	if ti < len(answers) {
		if rt := &answers[ti]; same(rt) {
			if ps := partitions(rt); pi < len(ps) && number(&ps[pi]) == partition {
				return rt, &ps[pi]
			}
		}
	}
	for i := range answers {
		rt := &answers[i]
		if !same(rt) {
			continue
		}
		ps := partitions(rt)
		for j := range ps {
			if number(&ps[j]) == partition {
				return rt, &ps[j]
			}
		}
	}
	return nil, nil
}

// decodedFields is the request as JSON, small enough to hold. It tries a broad view
// first and narrows (fewer items of each list, fewer levels) until it fits.
func decodedFields(q kmsg.Request) string {
	v := reflect.ValueOf(q)
	for _, lim := range []struct{ items, depth int }{{8, 8}, {4, 6}, {2, 5}, {1, 4}, {1, 3}} {
		var b strings.Builder
		writeValue(&b, v, lim.items, lim.depth)
		if b.Len() <= maxDecodedBytes {
			return b.String()
		}
	}
	return `{"…": "the request is too large to show here"}`
}

// secretish says a configuration name names a secret.
func secretish(name string) bool {
	n := strings.ToLower(name)
	for _, w := range []string{"password", "secret", "token", "jaas", "credential", "private", "keystore", "key.pem"} {
		if strings.Contains(n, w) {
			return true
		}
	}
	return false
}

func jsonString(b *strings.Builder, s string) {
	if utf8.RuneCountInString(s) > 128 {
		r := []rune(s)
		s = string(r[:128]) + "…"
	}
	out, _ := json.Marshal(s)
	b.Write(out)
}

// specFieldName is the name a field has in Apache Kafka's JSON specification of the
// message (specnames.go): "TransactionalId" for the library's TransactionID, "TimeoutMs"
// for TimeoutMillis, "TopicData" for Topics. A field the two name alike is its own name;
// "" is a field the specification does not have (one for a version no longer offered).
func specFieldName(t reflect.Type, field string) string {
	if n, ok := specNames[t.Name()+"."+field]; ok {
		return n
	}
	return field
}

func writeValue(b *strings.Builder, v reflect.Value, items, depth int) {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			b.WriteString("null")
			return
		}
		writeValue(b, v.Elem(), items, depth)
	case reflect.Bool:
		b.WriteString(strconv.FormatBool(v.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.WriteString(strconv.FormatInt(v.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		b.WriteString(strconv.FormatUint(v.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		b.WriteString(strconv.FormatFloat(v.Float(), 'g', -1, 64))
	case reflect.String:
		jsonString(b, v.String())
	case reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			raw := make([]byte, v.Len())
			for i := range raw {
				raw[i] = byte(v.Index(i).Uint())
			}
			if len(raw) == 16 { // a topic id, a client instance id: a UUID, written as one.
				h := hex.EncodeToString(raw)
				b.WriteString(strconv.Quote(h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]))
				return
			}
			b.WriteString(strconv.Quote(hex.EncodeToString(raw)))
			return
		}
		fallthrough
	case reflect.Slice:
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			b.WriteString(strconv.Quote(strconv.Itoa(v.Len()) + " bytes"))
			return
		}
		if depth <= 0 {
			b.WriteString(strconv.Quote(strconv.Itoa(v.Len()) + " items"))
			return
		}
		b.WriteByte('[')
		n := min(v.Len(), items)
		for i := 0; i < n; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			writeValue(b, v.Index(i), items, depth-1)
		}
		if v.Len() > n {
			if n > 0 {
				b.WriteString(", ")
			}
			b.WriteString(strconv.Quote("… " + strconv.Itoa(v.Len()-n) + " more"))
		}
		b.WriteByte(']')
	case reflect.Struct:
		if depth <= 0 {
			b.WriteString(`"…"`)
			return
		}
		t := v.Type()
		hidden := false
		if nf, ok := t.FieldByName("Name"); ok && nf.Type.Kind() == reflect.String && secretish(v.FieldByName("Name").String()) {
			hidden = true
		}
		if nf, ok := t.FieldByName("Key"); ok && nf.Type.Kind() == reflect.String && secretish(v.FieldByName("Key").String()) {
			hidden = true
		}
		b.WriteByte('{')
		first := true
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() || f.Name == "Version" || f.Name == "UnknownTags" {
				continue
			}
			fv := v.Field(i)
			if (fv.Kind() == reflect.Pointer || fv.Kind() == reflect.Interface) && fv.IsNil() {
				continue
			}
			if fv.Kind() == reflect.Slice && fv.Len() == 0 {
				continue
			}
			// A zero topic id says "named by name": a row of zeros says nothing and looks
			// like an id that matched nothing.
			if fv.Kind() == reflect.Array && fv.Len() == 16 && fv.Type().Elem().Kind() == reflect.Uint8 && fv.IsZero() {
				continue
			}
			name := specFieldName(t, f.Name)
			if name == "" {
				continue
			}
			if !first {
				b.WriteString(", ")
			}
			first = false
			b.WriteString(strconv.Quote(name))
			b.WriteString(": ")
			switch {
			case hidden && f.Name == "Value":
				b.WriteString(`"(hidden)"`)
			case f.Name == "Records" && fv.Kind() == reflect.Slice && fv.Type().Elem().Kind() == reflect.Uint8:
				bs, rs := batchCounts(fv.Bytes())
				b.WriteString(strconv.Quote(batchesText(bs) + ", " + plural(rs, "record") + ", " + bytesText(fv.Len())))
			default:
				writeValue(b, fv, items, depth-1)
			}
		}
		b.WriteByte('}')
	default:
		b.WriteString("null")
	}
}
