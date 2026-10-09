package console

import (
	"context"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/schemaregistry"
	"github.com/twmb/franz-go/pkg/kgo"
)

// newRegistryConsole is a console with a schema registry (the real one, in memory)
// beside its broker.
func newRegistryConsole(t testing.TB) (*Console, string, *httptest.Server) {
	t.Helper()
	reg, err := schemaregistry.Open(schemaregistry.NewMemoryStorage())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(schemaregistry.NewAPI(reg, schemaregistry.Options{}))
	t.Cleanup(ts.Close)
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.RegistryURL = ts.URL })
	return c, addr, ts
}

const (
	userV1 = `{"type":"record","name":"User","fields":[{"name":"id","type":"long"}]}`
	userV2 = `{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"email","type":["null","string"],"default":null}]}`
)

var tagRe = regexp.MustCompile(`<[^>]*>`)

// readable is a page as a person reads it: no markup, entities decoded.
func readable(body string) string { return html.UnescapeString(tagRe.ReplaceAllString(body, "")) }

func register(t *testing.T, c *Console, subject, schema string) {
	t.Helper()
	rec := req(t, c, "POST", "/_console/new/schema", url.Values{"subject": {subject}, "schema": {schema}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("register %s: %d %s", subject, rec.Code, rec.Body)
	}
}

func TestSchemasSectionIsOnlyThereWithARegistry(t *testing.T) {
	without := newTestConsole(t)
	rec := req(t, without, "GET", "/_console/topics", nil)
	if strings.Contains(rec.Body.String(), "/_console/schemas") {
		t.Error("the rail links Schemas although no registry is connected")
	}
	rec = req(t, without, "GET", "/_console/schemas", nil)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "No schema registry is connected") {
		t.Errorf("Schemas without a registry = %d, want a page that says so", rec.Code)
	}
	if rec = req(t, without, "POST", "/_console/new/schema", url.Values{"subject": {"s"}, "schema": {userV1}}); rec.Code != http.StatusNotFound {
		t.Errorf("registering without a registry = %d, want 404", rec.Code)
	}

	with, _, _ := newRegistryConsole(t)
	rec = req(t, with, "GET", "/_console/topics", nil)
	if !strings.Contains(rec.Body.String(), `href="/_console/schemas"`) {
		t.Error("the rail has no Schemas item although a registry is connected")
	}
}

// The whole life of a subject through the console: register, read it back, add a
// version and see what changed, change its compatibility, delete a version, delete
// the subject.
func TestSchemaSubjectLifecycle(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	if rec := req(t, c, "GET", "/_console/schemas", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), "No schemas yet") {
		t.Fatalf("empty Schemas page: %d", rec.Code)
	}
	register(t, c, "orders-value", userV1)

	rec := req(t, c, "GET", "/_console/schemas/orders-value", nil)
	body := rec.Body.String()
	if rec.Code != 200 || !strings.Contains(body, "orders-value") || !strings.Contains(body, "Avro") || !strings.Contains(readable(body), `"name": "id"`) {
		t.Fatalf("subject page: %d\n%s", rec.Code, readable(body))
	}
	if strings.Contains(body, "Changes from") {
		t.Error("version 1 offers a comparison with a version before it")
	}

	// A second version through the form; the page compares it with the first.
	rec = req(t, c, "POST", "/_console/schemas/orders-value/register", url.Values{"schema": {userV2}, "type": {"AVRO"}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("register v2: %d %s", rec.Code, rec.Body)
	}
	rec = req(t, c, "GET", "/_console/schemas/orders-value", nil)
	body = rec.Body.String()
	for _, want := range []string{"Changes from v1", `class="dl add"`, "email", "v2", "latest"} {
		if !strings.Contains(body, want) {
			t.Errorf("v2 page is missing %q", want)
		}
	}
	rec = req(t, c, "GET", "/_console/schemas/orders-value?version=1", nil)
	if strings.Contains(rec.Body.String(), "email") && !strings.Contains(rec.Body.String(), "New version") {
		t.Error("version 1 page shows version 2's field")
	}

	// Compatibility: the registry's default, then the subject's own, then back.
	if body := req(t, c, "GET", "/_console/schemas/orders-value", nil).Body.String(); !strings.Contains(body, "BACKWARD · default") {
		t.Error("the page does not say BACKWARD is the registry default")
	}
	if rec := req(t, c, "POST", "/_console/schemas/orders-value/config", url.Values{"level": {"FULL"}}); rec.Code != http.StatusSeeOther {
		t.Fatalf("set level: %d %s", rec.Code, rec.Body)
	}
	if body := req(t, c, "GET", "/_console/schemas/orders-value", nil).Body.String(); !strings.Contains(body, "FULL") || strings.Contains(body, "FULL · default") {
		t.Error("the subject's own level is not shown")
	}
	if rec := req(t, c, "POST", "/_console/schemas/orders-value/config", url.Values{"level": {"default"}}); rec.Code != http.StatusSeeOther {
		t.Fatalf("clear level: %d %s", rec.Code, rec.Body)
	}
	if rec := req(t, c, "POST", "/_console/schemas/orders-value/config", url.Values{"level": {"SIDEWAYS"}}); rec.Code != http.StatusBadRequest {
		t.Errorf("a made-up level = %d, want 400", rec.Code)
	}

	// Delete version 2: the subject goes on with version 1.
	rec = req(t, c, "POST", "/_console/schemas/orders-value/delete", url.Values{"version": {"2"}})
	if rec.Code != http.StatusSeeOther || !strings.Contains(rec.Header().Get("Location"), "/_console/schemas/orders-value") {
		t.Errorf("delete a version: %d → %s", rec.Code, rec.Header().Get("Location"))
	}
	// Delete the subject: it is gone, with a page that says so.
	req(t, c, "POST", "/_console/schemas/orders-value/delete", url.Values{})
	rec = req(t, c, "GET", "/_console/schemas/orders-value", nil)
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "no subject named") {
		t.Errorf("a deleted subject = %d, want a 404 page that names it", rec.Code)
	}
}

// Trying a schema before registering it: compatible, not compatible and why, and a
// schema that is not valid at all are three different answers.
func TestSchemaCheckSaysWhyNot(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "s", userV1)
	check := func(schema string) string {
		rec := req(t, c, "POST", "/_console/schemas/s/check", url.Values{"schema": {schema}, "type": {"AVRO"}})
		if rec.Code != 200 {
			t.Fatalf("check: %d %s", rec.Code, rec.Body)
		}
		return rec.Body.String()
	}
	if got := check(userV2); !strings.Contains(got, "Compatible.") {
		t.Errorf("a compatible schema: %s", got)
	}
	bad := check(`{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"email","type":"string"}]}`)
	if !strings.Contains(bad, "Not compatible") || !strings.Contains(bad, "email") {
		t.Errorf("an incompatible schema: %s", bad)
	}
	if got := check(`{"type":"record"`); !strings.Contains(got, "Not a valid schema") {
		t.Errorf("a broken schema: %s", got)
	}
	// Registering the incompatible one is refused with the registry's reason.
	rec := req(t, c, "POST", "/_console/schemas/s/register", url.Values{"schema": {`{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"email","type":"string"}]}`}})
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "incompatible") {
		t.Errorf("registering an incompatible schema = %d %s", rec.Code, rec.Body)
	}
}

// A subject is whatever a client chose, "/" and "%" included; its links must work.
func TestSubjectNamesRoundTripThroughLinks(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	for _, name := range []string{"a/b", "100%", "q?x", "with space"} {
		register(t, c, name, userV1)
		link := schemaPath("/_console", name)
		rec := req(t, c, "GET", link, nil)
		if rec.Code != 200 || !strings.Contains(rec.Body.String(), "version 1") && !strings.Contains(rec.Body.String(), "Version 1") {
			t.Errorf("%q: GET %s = %d", name, link, rec.Code)
		}
	}
	body := req(t, c, "GET", "/_console/schemas", nil).Body.String()
	if !strings.Contains(body, `href="/_console/schemas/a%2Fb"`) {
		t.Errorf("the list does not link a/b as a single path segment:\n%s", body)
	}
}

// Records a Confluent serializer wrote are shown as the JSON they mean, in the table
// and in the drawer, with the schema named; anything else is shown as it always was.
func TestRegistryEncodedRecordsAreDecoded(t *testing.T) {
	c, addr, ts := newRegistryConsole(t)
	register(t, c, "events-value", userV1)
	topicWith(t, c, "events", 1, 0)

	codec, err := schemaregistry.NewCodec("AVRO", userV1, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.Encode(1, `{"id":4242}`)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, v := range [][]byte{encoded, []byte("plain text"), {0, 0, 0, 0, 99, 1, 2}} { // decoded, plain, an id nobody registered.
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "events", Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	page := req(t, c, "GET", "/_console/topics/events", nil).Body.String()
	if !strings.Contains(page, "4242") {
		t.Errorf("the table does not show the decoded value:\n%s", page)
	}
	if !strings.Contains(page, "events-value") {
		t.Error("the topic page does not link its value schema")
	}

	drawer := req(t, c, "GET", "/_console/topics/events/message?partition=0&offset=0", nil).Body.String()
	drawer = readable(drawer) + drawer
	for _, want := range []string{`"id": 4242`, "Avro · schema 1", "events-value v1", "data-schema=\"events-value\"", `data-value-enc="text"`} {
		if !strings.Contains(drawer, want) {
			t.Errorf("drawer for the decoded record is missing %q:\n%s", want, drawer)
		}
	}
	// Send again still carries the stored bytes: the edit button's JSON is for editing only.
	if !strings.Contains(drawer, "Send again") {
		t.Error("no Send again for a decoded record")
	}

	plain := req(t, c, "GET", "/_console/topics/events/message?partition=0&offset=1", nil).Body.String()
	if strings.Contains(plain, "schema 1") || !strings.Contains(plain, "plain text") {
		t.Errorf("a plain record was shown as decoded or lost:\n%s", plain)
	}
	unknown := req(t, c, "GET", "/_console/topics/events/message?partition=0&offset=2", nil).Body.String()
	if strings.Contains(unknown, "Avro ·") || !strings.Contains(unknown, "binary") {
		t.Errorf("a record naming an unregistered id should be shown as binary:\n%s", unknown)
	}

	// With the registry gone the pages still work; the record is simply binary again.
	ts.Close()
	if rec := req(t, c, "GET", "/_console/topics/events", nil); rec.Code != 200 {
		t.Errorf("topic page with the registry down = %d", rec.Code)
	}
	if rec := req(t, c, "GET", "/_console/topics/events/message?partition=0&offset=0", nil); rec.Code != 200 {
		t.Errorf("drawer with the registry down = %d", rec.Code)
	}
}

// Producing from the composer with a subject writes the value in Confluent's wire
// format, and refuses a value the schema does not accept.
func TestComposerEncodesWithASchema(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	topicWith(t, c, "orders", 1, 0)

	if page := req(t, c, "GET", "/_console/topics/orders", nil).Body.String(); !strings.Contains(page, `value="orders-value" selected`) {
		t.Error("the composer does not start on the topic's own value subject")
	}
	rec := req(t, c, "POST", "/_console/topics/orders/produce", url.Values{"partition": {"0"}, "partition_ctx": {"0"}, "value": {`{"id": 7}`}, "schema_subject": {"orders-value"}})
	if rec.Code != 200 {
		t.Fatalf("produce with a schema: %d %s", rec.Code, rec.Body)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics("orders"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fetches := cl.PollRecords(ctx, 1)
	var got []byte
	fetches.EachRecord(func(r *kgo.Record) { got = r.Value })
	if len(got) < 6 || got[0] != 0 || got[4] != 1 || got[5] != 14 { // magic, id 1, zigzag(7)
		t.Fatalf("stored value = %x, want magic 0, id 1, the long 7", got)
	}

	for name, form := range map[string]url.Values{
		"a value the schema rejects":    {"partition": {"0"}, "value": {`{"id":"seven"}`}, "schema_subject": {"orders-value"}},
		"not JSON at all":               {"partition": {"0"}, "value": {`seven`}, "schema_subject": {"orders-value"}},
		"a subject that does not exist": {"partition": {"0"}, "value": {`{"id":1}`}, "schema_subject": {"nope"}},
		"base64 with a schema":          {"partition": {"0"}, "value": {"AQID"}, "value_enc": {"base64"}, "schema_subject": {"orders-value"}},
	} {
		rec := req(t, c, "POST", "/_console/topics/orders/produce", form)
		if rec.Code < 400 || rec.Code >= 500 {
			t.Errorf("%s: %d, want a 4xx the person can read", name, rec.Code)
		}
	}
	fetches = cl.PollRecords(ctx, 10)
	if n := fetches.NumRecords(); n != 0 {
		t.Errorf("%d records were written by the refused attempts", n)
	}
}

func TestPaletteListsSubjects(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	body := req(t, c, "GET", "/_console/api/palette", nil).Body.String()
	if !strings.Contains(body, "orders-value") || !strings.Contains(body, "/_console/schemas/orders-value") {
		t.Errorf("palette lacks the subject: %s", body)
	}
	if counts := req(t, c, "GET", "/_console/api/counts", nil).Body.String(); !strings.Contains(counts, `"schemas":1`) {
		t.Errorf("counts = %s, want schemas:1", counts)
	}
}

func TestDiffLines(t *testing.T) {
	got := diffLines("a\nb\nc", "a\nx\nc\nd")
	var ops []string
	for _, l := range got {
		ops = append(ops, l.Op+":"+l.Text)
	}
	want := "same:a del:b add:x same:c add:d"
	if strings.Join(ops, " ") != want {
		t.Errorf("diff = %v, want %s", ops, want)
	}
}
