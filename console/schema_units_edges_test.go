package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// Who registered a version is the user it signed in as, else the software its
// User-Agent names (the first word), else this console, else where it came from.
func TestRegisteredByNamesTheUserThenTheSoftwareThenTheConsoleThenTheAddress(t *testing.T) {
	for _, tc := range []struct {
		name string
		info schemaregistry.VersionInfo
		want string
	}{
		{"a signed-in user wins over the agent and the address", schemaregistry.VersionInfo{RegisteredBy: "alice", Agent: "Java/21 (x)", From: "10.0.0.7"}, "alice"},
		{"a client's software is its first word", schemaregistry.VersionInfo{Agent: "confluent-kafka-python/2.6.0 librdkafka/2.6", From: "10.0.0.7"}, "confluent-kafka-python/2.6.0"},
		{"the console's own agent says so", schemaregistry.VersionInfo{Agent: consoleAgent + "/1", From: "10.0.0.7"}, "this console"},
		{"with nothing else, where it came from", schemaregistry.VersionInfo{From: "10.0.0.7"}, "10.0.0.7"},
	} {
		if got := registeredBy(tc.info); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Try it starts from a value with a field of each of the schema's names and a value of
// its type; Protobuf fields are named as their JSON names, Avro's as they are.
func TestTheStartingValueForTryItHasEachFieldInItsOwnType(t *testing.T) {
	avro := schemaVersion{Subject: "s", Version: 1, ID: 1, Schema: `{"type":"record","name":"R","fields":[{"name":"user_id","type":"long"},{"name":"note","type":"string"},{"name":"on","type":"boolean"}]}`}
	if got, want := sampleDoc(avro, shapeOf(avro)), `{"user_id":0,"note":"","on":false}`; got != want {
		t.Errorf("Avro: %s, want %s", got, want)
	}
	proto := schemaVersion{Subject: "s", Version: 1, ID: 1, SchemaType: schemaregistry.TypeProtobuf,
		Schema: "syntax = \"proto3\";\nmessage M {\n  int64 user_id = 1;\n  string full_name = 2;\n}\n"}
	if got, want := sampleDoc(proto, shapeOf(proto)), `{"userId":0,"fullName":""}`; got != want {
		t.Errorf("Protobuf: %s, want %s", got, want)
	}
	if got := sampleDoc(avro, schemaShape{}); got != "{}" {
		t.Errorf("a schema with no readable fields: %s, want {}", got)
	}
}

// A field's default is the value to start from, written as JSON: null and numbers and
// booleans as they are, text quoted.
func TestAFieldsDefaultIsWrittenAsJSONInTheStartingValue(t *testing.T) {
	for def, want := range map[string]string{
		"null":  "null",
		"5":     "5",
		"-2.5":  "-2.5",
		"true":  "true",
		"false": "false",
		"abc":   `"abc"`,
		"nul":   `"nul"`,
		"":      `""`,
	} {
		if got := sampleValue(schemaField{Name: "f", Type: "string", HasDefault: true, Default: def}); got != want {
			t.Errorf("default %q: %s, want %s", def, got, want)
		}
	}
}

// Protobuf names a field in JSON by dropping its underscores and capitalising the letter
// after each.
func TestAProtobufFieldsJSONNameIsLowerCamelCase(t *testing.T) {
	for in, want := range map[string]string{
		"id":           "id",
		"user_id":      "userId",
		"full_name_ok": "fullNameOk",
		"a__b":         "aB",
		"trailing_":    "trailing",
		"_lead":        "Lead",
		"":             "",
	} {
		if got := protoJSONName(in); got != want {
			t.Errorf("protoJSONName(%q) = %q, want %q", in, got, want)
		}
	}
}

// The bytes a serializer would send are shown as hex pairs, the magic byte and the id
// (the first five) set apart from the value.
func TestTheBytesOfTryItAreHexPairsWithTheHeaderSetApart(t *testing.T) {
	for _, tc := range []struct {
		in   []byte
		want string
	}{
		{nil, ""},
		{[]byte{0x00}, "00"},
		{[]byte{0, 0, 0, 0, 7}, "00 00 00 00 07"},
		{[]byte{0, 0, 0, 0, 7, 0x0a, 0xff}, "00 00 00 00 07 | 0a ff"},
	} {
		if got := hexBytes(tc.in); got != tc.want {
			t.Errorf("hexBytes(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// A version's change phrases are joined with semicolons, the field names set in code type
// and everything else escaped.
func TestAVersionsChangePhrasesAreJoinedAndEscaped(t *testing.T) {
	got := string(changedHTML([]changePart{
		{Before: "added optional ", Name: "email", After: " (null or string)"},
		{Before: "removed <b>", Name: "id"},
		{Before: "widened"},
	}))
	want := `added optional <span class="m">email</span> (null or string); removed &lt;b&gt;<span class="m">id</span>; widened`
	if got != want {
		t.Errorf("changedHTML:\n got %s\nwant %s", got, want)
	}
	if got := changedHTML(nil); got != "" {
		t.Errorf("no phrases: %q", got)
	}
}

// The subject list is in name order, whatever order the subjects were registered in.
func TestSubjectsComeInNameOrder(t *testing.T) {
	reg, err := schemaregistry.Open(schemaregistry.NewMemoryStorage())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(schemaregistry.NewAPI(reg, schemaregistry.Options{}))
	defer ts.Close()
	rc := newRegistryClient(ts.URL, "", "", nil)
	ctx := context.Background()
	for _, s := range []string{"mid-value", "zeta-value", "alpha-value"} {
		if _, err := rc.Register(ctx, s, schemaBody{Schema: userV1}); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := rc.Subjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, r := range rows {
		names = append(names, r.Name)
	}
	if got := strings.Join(names, ","); got != "alpha-value,mid-value,zeta-value" {
		t.Errorf("subjects: %s, want them in name order", got)
	}
}

// The registry's request log is read a page at a time: the cursor a page gives asks
// for the requests before it.
func TestTheRegistrysRequestLogPagesBackFromACursor(t *testing.T) {
	reg, err := schemaregistry.Open(schemaregistry.NewMemoryStorage())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(schemaregistry.NewAPI(reg, schemaregistry.Options{}))
	defer ts.Close()
	for i := 0; i < 6; i++ {
		if s := client(t, "GET", fmt.Sprintf("%s/subjects/none-%d/versions", ts.URL, i), ""); s != 404 {
			t.Fatalf("request %d: %d", i, s)
		}
	}
	rc := newRegistryClient(ts.URL, "", "", nil)
	ctx := context.Background()
	first, err := rc.Requests(ctx, "", 0, 2)
	if err != nil || len(first.Requests) != 2 || first.Next == 0 {
		t.Fatalf("the newest page: %+v %v", first, err)
	}
	older, err := rc.Requests(ctx, "", first.Next, 2)
	if err != nil || len(older.Requests) != 2 {
		t.Fatalf("the page before the cursor: %+v %v", older, err)
	}
	for _, r := range older.Requests {
		if r.Seq >= first.Next || r.Seq >= first.Requests[len(first.Requests)-1].Seq {
			t.Errorf("request %d is not older than the newest page (cursor %d, last of it %d)", r.Seq, first.Next, first.Requests[len(first.Requests)-1].Seq)
		}
	}
}

// A schema may name at most 1,000 other schemas through its references (the registry's
// own bound): 1,000 resolve, a 1,001st is refused.
func TestAGraphOfReferencesIsFollowedToAThousandSchemasAndNoFurther(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /subjects/{subject}/versions/{version}", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"subject": r.PathValue("subject"), "version": 1, "id": 1, "schema": `"string"`})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	rc := newRegistryClient(ts.URL, "", "", nil)
	root := func(n int) schemaVersion {
		var refs []string
		for i := 0; i < n; i++ {
			refs = append(refs, fmt.Sprintf(`{"name":"n%d","subject":"s%d","version":1}`, i, i))
		}
		var v schemaVersion
		if err := json.Unmarshal([]byte(`{"subject":"root","version":1,"id":2,"schema":"{}","references":[`+strings.Join(refs, ",")+`]}`), &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	ctx := context.Background()
	if out, err := rc.resolve(ctx, root(maxResolved)); err != nil || len(out) != maxResolved {
		t.Fatalf("%d schemas: %d resolved, %v", maxResolved, len(out), err)
	}
	if _, err := rc.resolve(ctx, root(maxResolved+1)); err == nil || !strings.Contains(err.Error(), "more than 1000 schemas") {
		t.Errorf("%d schemas: %v, want the refusal", maxResolved+1, err)
	}
}
