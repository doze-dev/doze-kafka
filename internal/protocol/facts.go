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
//
//fuzz:parser FuzzDescribe
func DescribeFacts(q kmsg.Request, r kmsg.Response) Facts {
	var f Facts
	if q == nil {
		return f
	}
	add := func(p PartResult) {
		if len(f.Partitions) >= MaxFactPartitions {
			f.PartitionsMore++
			return
		}
		f.Partitions = append(f.Partitions, p)
	}
	switch req := q.(type) {
	case *kmsg.ProduceRequest:
		f.Acks, f.HasAcks = req.Acks, true
		resp, _ := r.(*kmsg.ProduceResponse)
		for _, t := range req.Topics {
			for _, p := range t.Partitions {
				b, n := batchCounts(p.Records)
				_ = b
				pr := PartResult{Topic: t.Topic, Partition: p.Partition, Records: n, Bytes: len(p.Records)}
				if resp != nil {
					for _, rt := range resp.Topics {
						if rt.Topic != t.Topic {
							continue
						}
						for _, rp := range rt.Partitions {
							if rp.Partition == p.Partition {
								pr.ErrorCode = rp.ErrorCode
								if rp.ErrorCode == 0 {
									pr.Offset, pr.HasOffset = rp.BaseOffset, true
								}
							}
						}
					}
				}
				add(pr)
			}
		}
	case *kmsg.FetchRequest:
		resp, _ := r.(*kmsg.FetchResponse)
		for _, t := range req.Topics {
			for _, p := range t.Partitions {
				pr := PartResult{Topic: t.Topic, Partition: p.Partition, Offset: p.FetchOffset, HasOffset: true}
				if resp != nil {
					for _, rt := range resp.Topics {
						if rt.Topic != t.Topic && rt.TopicID != t.TopicID {
							continue
						}
						for _, rp := range rt.Partitions {
							if rp.Partition == p.Partition {
								pr.ErrorCode = rp.ErrorCode
								_, pr.Records = batchCounts(rp.RecordBatches)
								pr.Bytes = len(rp.RecordBatches)
							}
						}
					}
				}
				add(pr)
			}
		}
	case *kmsg.OffsetCommitRequest:
		resp, _ := r.(*kmsg.OffsetCommitResponse)
		for _, t := range req.Topics {
			for _, p := range t.Partitions {
				pr := PartResult{Topic: t.Topic, Partition: p.Partition, Offset: p.Offset, HasOffset: true}
				if resp != nil {
					for _, rt := range resp.Topics {
						if rt.Topic != t.Topic {
							continue
						}
						for _, rp := range rt.Partitions {
							if rp.Partition == p.Partition {
								pr.ErrorCode = rp.ErrorCode
							}
						}
					}
				}
				add(pr)
			}
		}
	}
	f.Decoded = decodedFields(q)
	return f
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
