package schemaregistry

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Foundation B12 (and the registry's findings SCH-28): the REST layer, descriptors
// and references are bytes from whoever can reach the port. Each target has a
// per-iteration deadline: a request that takes seconds is a finding even if it
// returns the right answer, because the registry answers under one lock.

const iterationDeadline = 5 * time.Second

func newFuzzAPI(t testing.TB) *API {
	r, err := Open(NewMemoryStorage())
	if err != nil {
		t.Fatal(err)
	}
	return NewAPI(r, Options{})
}

// serve runs one request and checks what must hold of every answer.
func serve(t *testing.T, a *API, method, path, ctype, body string) *httptest.ResponseRecorder {
	t.Helper()
	req, err := http.NewRequest(method, "http://registry"+path, strings.NewReader(body))
	if err != nil {
		return nil // the fuzzer invented a path net/http will not build; not a registry matter.
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	rec := httptest.NewRecorder()
	start := time.Now()
	a.ServeHTTP(rec, req)
	if took := time.Since(start); took > iterationDeadline {
		t.Fatalf("%s %s took %v (body %d bytes)", method, path, took, len(body))
	}
	if rec.Code >= 500 {
		t.Fatalf("%s %s answered %d: %s", method, path, rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Content-Type"); strings.Contains(got, "json") && rec.Body.Len() > 0 && !json.Valid(rec.Body.Bytes()) {
		t.Fatalf("%s %s: %q is not JSON: %s", method, path, got, rec.Body)
	}
	return rec
}

// Every route that reads a body, with an arbitrary body and content type, over a
// registry that holds a subject, so reads and compares have something to meet.
func FuzzAPIBody(f *testing.F) {
	routes := []struct{ method, path string }{
		{"POST", "/subjects/s/versions"},
		{"POST", "/subjects/s"},
		{"POST", "/subjects/new/versions?normalize=true"},
		{"POST", "/compatibility/subjects/s/versions/latest"},
		{"POST", "/compatibility/subjects/s/versions"},
		{"PUT", "/config"},
		{"PUT", "/config/s"},
		{"PUT", "/mode/s"},
		{"PUT", "/mode?force=true"},
		{"GET", "/subjects/s/versions/latest"},
		{"GET", "/schemas/ids/1"},
		{"DELETE", "/subjects/s?permanent=true"},
	}
	f.Add(uint8(0), `{"schema":"{\"type\":\"string\"}"}`, "application/vnd.schemaregistry.v1+json")
	f.Add(uint8(0), `{"schema":"syntax = \"proto3\"; message M { string a = 1; }","schemaType":"PROTOBUF"}`, "application/json")
	f.Add(uint8(0), `{"schema":"{}","schemaType":"JSON","references":[{"name":"x","subject":"s","version":1}]}`, "")
	f.Add(uint8(3), `{"schema":"\"int\""}`, "application/json")
	f.Add(uint8(5), `{"compatibility":"FULL"}`, "application/json")
	f.Add(uint8(7), `{"mode":"READONLY"}`, "application/json")
	f.Add(uint8(0), `[1,2`, "text/plain")
	f.Fuzz(func(t *testing.T, route uint8, body, ctype string) {
		if len(body) > 1<<14 {
			t.Skip()
		}
		a := newFuzzAPI(t)
		if _, e := a.reg.Register("s", SchemaInput{Schema: `{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}`}, false); e != nil {
			t.Fatalf("seed schema: %v", e)
		}
		rt := routes[int(route)%len(routes)]
		serve(t, a, rt.method, rt.path, ctype, body)
	})
}

// A Protobuf schema may be registered as a base64 FileDescriptorProto. Whatever
// the bytes, they are an error or a schema, never a panic or a hang, and a schema
// that is accepted can be read back and compared with itself.
func FuzzDescriptor(f *testing.F) {
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("m.proto"),
		Package: proto.String("p"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("M"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("a"), Number: proto.Int32(1),
				Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			}},
		}},
	}
	good, err := proto.Marshal(fd)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	f.Add(good[:len(good)/2])
	f.Add([]byte{})
	f.Add([]byte{0x0a, 0x00})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<14 {
			t.Skip()
		}
		text := base64.StdEncoding.EncodeToString(raw)
		start := time.Now()
		if p, ok := parseDescriptor(text); ok && p == nil {
			t.Fatal("parseDescriptor reported ok with no schema")
		}
		a := newFuzzAPI(t)
		body, _ := json.Marshal(SchemaInput{Schema: text, SchemaType: TypeProtobuf})
		rec := serve(t, a, "POST", "/subjects/d/versions", "application/json", string(body))
		if rec != nil && rec.Code == 200 {
			serve(t, a, "GET", "/subjects/d/versions/latest", "", "")
			serve(t, a, "POST", "/compatibility/subjects/d/versions/latest", "application/json", string(body))
		}
		if took := time.Since(start); took > iterationDeadline {
			t.Fatalf("a %d byte descriptor took %v", len(raw), took)
		}
	})
}

// References form a graph chosen by the client: a schema may name a subject that
// does not exist, one that is itself, one registered later, or a chain. Whatever
// shape, registering is an error or a schema, and it ends.
func FuzzRegisterReferences(f *testing.F) {
	f.Add([]byte{0, 1, 1, 0, 2, 1, 1, 1, 0})
	f.Add([]byte{1, 0, 0, 0, 0, 0})
	f.Add([]byte{0, 9, 9, 9, 9, 9, 9, 9})
	f.Fuzz(func(t *testing.T, graph []byte) {
		if len(graph) > 90 {
			graph = graph[:90]
		}
		a := newFuzzAPI(t)
		start := time.Now()
		// Each triple is one registration: which type, and up to two references
		// (subject index and version) to subjects "s0".."s3".
		for i := 0; i+2 < len(graph); i += 3 {
			typ := []string{TypeAvro, TypeProtobuf, TypeJSON}[int(graph[i])%3]
			in := SchemaInput{SchemaType: typ}
			switch typ {
			case TypeAvro:
				in.Schema = fmt.Sprintf(`{"type":"record","name":"R%d","fields":[{"name":"f","type":"string"}]}`, i/3)
			case TypeProtobuf:
				in.Schema = fmt.Sprintf(`syntax = "proto3"; message M%d { string a = 1; }`, i/3)
			default:
				in.Schema = fmt.Sprintf(`{"type":"object","title":"t%d"}`, i/3)
			}
			for k, b := range graph[i+1 : i+3] {
				in.References = append(in.References, Reference{
					Name:    fmt.Sprintf("ref%d", k),
					Subject: fmt.Sprintf("s%d", b%4),
					Version: int(b>>4) % 3,
				})
			}
			body, _ := json.Marshal(in)
			serve(t, a, "POST", fmt.Sprintf("/subjects/s%d/versions", (i/3)%4), "application/json", string(bytes.TrimSpace(body)))
		}
		if took := time.Since(start); took > iterationDeadline {
			t.Fatalf("%d registrations took %v", len(graph)/3, took)
		}
	})
}
