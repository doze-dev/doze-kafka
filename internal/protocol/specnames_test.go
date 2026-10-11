package protocol

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

var updateSpecNames = flag.Bool("update-specnames", false, "rewrite specnames.go from testdata/kafka-spec")

// The request specifications of Apache Kafka 4.3.1, as they are in the kafka-clients jar
// (common/message/<Name>Request.json), for every API this broker serves. They are the
// authority for the field names the Traffic inspector shows (specnames.go).
const specDir = "testdata/kafka-spec"

type specField struct {
	Name   string      `json:"name"`
	Type   string      `json:"type"`
	Fields []specField `json:"fields"`
}

type specMessage struct {
	Name   string      `json:"name"`
	Fields []specField `json:"fields"`
	// CommonStructs are structures a message defines once and names as the type of
	// more than one field.
	CommonStructs []specField `json:"commonStructs"`
}

// resolve gives a field its nested fields: its own, or those of the common structure its
// type names.
func (m specMessage) resolve(f specField) []specField {
	if len(f.Fields) > 0 {
		return f.Fields
	}
	typ := strings.TrimPrefix(f.Type, "[]")
	for _, c := range m.CommonStructs {
		if c.Name == typ {
			return c.Fields
		}
	}
	return nil
}

var lineComment = regexp.MustCompile(`(?m)^\s*//.*$`)

func readSpec(t *testing.T, name string) specMessage {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(specDir, name+".json"))
	if err != nil {
		t.Fatalf("the specification of %s: %v", name, err)
	}
	var m specMessage
	if err := json.Unmarshal(lineComment.ReplaceAll(raw, nil), &m); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return m
}

// dataFields are the fields of a library struct that carry the message: not its version
// and not its unknown tags.
func dataFields(t reflect.Type) []reflect.StructField {
	var out []reflect.StructField
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() || f.Name == "Version" || f.Name == "UnknownTags" {
			continue
		}
		out = append(out, f)
	}
	return out
}

func elemStruct(t reflect.Type) (reflect.Type, bool) {
	for t.Kind() == reflect.Slice || t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t, t.Kind() == reflect.Struct
}

// explicitSpecNames are the few messages whose library struct is not the specification's
// field for field in order: the library is older than 4.3 (a field the specification has
// since dropped, or added), or orders a message's two forms the other way round. A field
// listed as "" is not in the specification (it is for an old version of the API only) and
// is not shown.
var explicitSpecNames = map[string]map[string]string{
	"AddPartitionsToTxnRequest": {"TransactionalID": "V3AndBelowTransactionalId", "ProducerID": "V3AndBelowProducerId",
		"ProducerEpoch": "V3AndBelowProducerEpoch", "Topics": "V3AndBelowTopics", "Transactions": "Transactions"},
	"DeleteTopicsRequest":   {"TopicNames": "TopicNames", "Topics": "Topics", "TimeoutMillis": "TimeoutMs"},
	"InitProducerIDRequest": {"TransactionalID": "TransactionalId", "TransactionTimeoutMillis": "TransactionTimeoutMs", "ProducerID": "ProducerId", "ProducerEpoch": "ProducerEpoch"},
	"ListOffsetsRequestTopicPartition": {"Partition": "PartitionIndex", "CurrentLeaderEpoch": "CurrentLeaderEpoch", "Timestamp": "Timestamp",
		"MaxNumOffsets": ""},
	"OffsetCommitRequestTopicPartition": {"Partition": "PartitionIndex", "Offset": "CommittedOffset", "Timestamp": "",
		"LeaderEpoch": "CommittedLeaderEpoch", "Metadata": "CommittedMetadata"},
}

// pair walks a library struct against its specification, field by field in order (the
// library is generated from the same files, so the order is the same), and records where
// the names differ.
func pair(t *testing.T, spec specMessage, gt reflect.Type, fields []specField, out map[string]string) {
	t.Helper()
	gf := dataFields(gt)
	byName := map[string]specField{}
	for _, f := range fields {
		byName[f.Name] = f
	}
	explicit, isExplicit := explicitSpecNames[gt.Name()]
	if !isExplicit && len(gf) != len(fields) {
		t.Errorf("%s has %d fields, its specification %d", gt.Name(), len(gf), len(fields))
		return
	}
	for i, f := range gf {
		var sf specField
		if isExplicit {
			name, ok := explicit[f.Name]
			if !ok {
				t.Errorf("%s.%s is not in the explicit table", gt.Name(), f.Name)
				continue
			}
			if name == "" {
				out[gt.Name()+"."+f.Name] = "" // not in the specification: not shown.
				continue
			}
			if sf, ok = byName[name]; !ok {
				t.Errorf("%s.%s is %q, which the specification does not have", gt.Name(), f.Name, name)
				continue
			}
		} else {
			sf = fields[i]
		}
		if f.Name != sf.Name {
			key := gt.Name() + "." + f.Name
			if old, ok := out[key]; ok && old != sf.Name {
				t.Errorf("%s is %q in one place and %q in another", key, old, sf.Name)
			}
			out[key] = sf.Name
		}
		if nested := spec.resolve(sf); len(nested) > 0 {
			et, ok := elemStruct(f.Type)
			if !ok {
				t.Errorf("%s.%s has nested fields in the specification and is %s", gt.Name(), f.Name, f.Type)
				continue
			}
			pair(t, spec, et, nested, out)
		}
	}
}

func servedRequests(t *testing.T) map[string]reflect.Type {
	t.Helper()
	out := map[string]reflect.Type{}
	for _, a := range APIs {
		req := kmsg.RequestForKey(int16(a.Key))
		if req == nil {
			t.Fatalf("no request type for key %d", a.Key)
		}
		out[kmsg.NameForKey(int16(a.Key))+"Request"] = reflect.TypeOf(req).Elem()
	}
	return out
}

func generatedSpecNames(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	for name, gt := range servedRequests(t) {
		spec := readSpec(t, name)
		if !strings.EqualFold(spec.Name, name) {
			t.Errorf("%s names itself %q", name, spec.Name)
		}
		pair(t, spec, gt, spec.Fields, out)
	}
	return out
}

func renderSpecNames(names map[string]string) []byte {
	keys := make([]string, 0, len(names))
	for k := range names {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	b.WriteString(`// Code generated by go test ./internal/protocol -run TestSpecNamesMatchTheSpecification -update-specnames; DO NOT EDIT.

package protocol

// specNames maps "<library type>.<library field>" to the name the field has in Apache
// Kafka's JSON specification of the message (testdata/kafka-spec, from kafka-clients
// 4.3.1), where the two differ. A field not listed has the same name in both; one listed
// as "" is not in the specification and is not shown.
var specNames = map[string]string{
`)
	for _, k := range keys {
		fmt.Fprintf(&b, "\t%q: %q,\n", k, names[k])
	}
	b.WriteString("}\n")
	out, err := format.Source(b.Bytes())
	if err != nil {
		panic(err)
	}
	return out
}

// The table the inspector uses is what the specifications say, for every request this
// broker serves: no field is left under the library's name, and none is renamed twice.
func TestSpecNamesMatchTheSpecification(t *testing.T) {
	want := renderSpecNames(generatedSpecNames(t))
	if *updateSpecNames {
		if err := os.WriteFile("specnames.go", want, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	got, err := os.ReadFile("specnames.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Error("specnames.go is not what testdata/kafka-spec says: go test ./internal/protocol -run TestSpecNamesMatchTheSpecification -update-specnames")
	}
}

// Every field the inspector can write for a request, in the 22 requests the summarisers
// describe and every other served API, is a name the specification has, and the old
// snake_case spellings do not appear.
func TestDecodedFieldsUseTheSpecificationNames(t *testing.T) {
	for name, gt := range servedRequests(t) {
		valid := specKeys(t, name)
		var walk func(rt reflect.Type, depth int)
		walk = func(rt reflect.Type, depth int) {
			for _, f := range dataFields(rt) {
				n := specFieldName(rt, f.Name)
				if n == "" {
					continue // not in the specification: not shown.
				}
				if !valid[n] {
					t.Errorf("%s.%s is written as %q, which %s does not have", rt.Name(), f.Name, n, name)
				}
				if et, ok := elemStruct(f.Type); ok && depth < 6 {
					walk(et, depth+1)
				}
			}
		}
		walk(gt, 0)
	}
	if strings.Contains(string(renderSpecNames(map[string]string{})), "snake") {
		t.Error("unreachable")
	}
}

// specKeys are every field name the specification of a request has, at any depth.
func specKeys(t *testing.T, request string) map[string]bool {
	t.Helper()
	valid := map[string]bool{}
	var collect func(fs []specField)
	collect = func(fs []specField) {
		for _, f := range fs {
			valid[f.Name] = true
			collect(f.Fields)
		}
	}
	spec := readSpec(t, request)
	collect(spec.Fields)
	for _, c := range spec.CommonStructs {
		collect(c.Fields)
	}
	return valid
}

// checkSpecKeys fails when the decoded-fields JSON has a key the request's specification
// does not.
func checkSpecKeys(t *testing.T, what, request, decoded string) {
	t.Helper()
	var tree any
	if err := json.Unmarshal([]byte(decoded), &tree); err != nil {
		t.Errorf("%s: not JSON: %v", what, err)
		return
	}
	valid := specKeys(t, request)
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for k, e := range x {
				if !valid[k] && k != "…" {
					t.Errorf("%s: the decoded fields have %q, which %s does not", what, k, request)
				}
				walk(e)
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		}
	}
	walk(tree)
}
