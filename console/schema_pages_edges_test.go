package console

import (
	"context"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	userV3 = `{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"email","type":["null","string"],"default":null},{"name":"nick","type":["null","string"],"default":null}]}`
	jsonV1 = `{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"],"additionalProperties":false}`
	proto  = "syntax = \"proto3\";\nmessage Ping {\n  int64 user_id = 1;\n}\n"
)

// registerAs registers a schema of a language through the console's form.
func registerAs(t *testing.T, c *Console, subject, typ, schema string) {
	t.Helper()
	rec := req(t, c, "POST", "/_console/new/schema", url.Values{"subject": {subject}, "schema": {schema}, "type": {typ}})
	if rec.Code != 303 {
		t.Fatalf("register %s: %d %s", subject, rec.Code, rec.Body)
	}
}

// produceFramedAs writes registry-framed values (one per id) to partition 0 of a topic.
func produceFramedAs(t *testing.T, addr, topic string, ids ...int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, id := range ids {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: framedValue(id)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// keysAtTheFoot is what the keys strip at the foot of a page says.
func keysAtTheFoot(body string) string {
	return readable(between(body, `id="keys"`, "</footer>"))
}

// The inspector of a version links the newest message that carries its id, counts how
// many of the newest records do, and links the diff with the version before it.
func TestTheVersionInspectorLinksTheNewestMessageOnItsIdAndTheDiffWithTheVersionBefore(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	topicWith(t, c, "orders", 1, 0)
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-value", userV2)
	register(t, c, "orders-value", userV3)
	produceFramedAs(t, addr, "orders", 1, 2, 2, 2)

	open := func(v string) (raw, text string) {
		t.Helper()
		rec := req(t, c, "GET", "/_console/inspect/version?key="+url.QueryEscape(joinKey("orders-value", v)), nil)
		if rec.Code != 200 {
			t.Fatalf("v%s: %d %s", v, rec.Code, rec.Body)
		}
		return rec.Body.String(), readable(rec.Body.String())
	}
	raw, text := open("2")
	for _, want := range []string{
		`href="/_console/topics/orders/message?partition=0&amp;offset=3"`, // the newest message on id 2,
		`href="/_console/schemas/orders-value/diff?from=1&amp;to=2"`,      // and the diff with v1.
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("v2's inspector is missing %s:\n%s", want, text)
		}
	}
	for _, want := range []string{"3 of the newest 4 on orders carry id 2", "Diff with v1", "by this console"} {
		if !strings.Contains(text, want) {
			t.Errorf("v2's inspector is missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "not kept") {
		t.Errorf("v2 was registered a moment ago and its inspector says it was not kept:\n%s", text)
	}

	raw, text = open("1")
	if !strings.Contains(raw, `offset=0"`) || !strings.Contains(text, "1 of the newest 4 on orders carry id 1") {
		t.Errorf("v1's inspector:\n%s", text)
	}
	if strings.Contains(text, "Diff with") {
		t.Errorf("v1 has no version before it and offers a diff:\n%s", text)
	}

	raw, text = open("3")
	if !strings.Contains(raw, `href="/_console/schemas/orders-value/diff?from=2&amp;to=3"`) || !strings.Contains(text, "Diff with v2") {
		t.Errorf("v3's inspector offers the wrong diff:\n%s", text)
	}
	if !strings.Contains(text, "None of the newest 4 records on orders carry id 3") {
		t.Errorf("v3's id is on no message and the inspector does not say so:\n%s", text)
	}
}

// The inspector of a version names the versions that reference it and the other subjects
// that hold the same schema.
func TestTheVersionInspectorNamesWhatReferencesItAndWhereTheSameSchemaIsHeld(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	ctx := context.Background()
	if _, err := c.reg.Register(ctx, "addr", schemaBody{Schema: `{"type":"record","name":"Address","fields":[{"name":"street","type":"string"}]}`}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.reg.Register(ctx, "person-value", schemaBody{
		Schema:     `{"type":"record","name":"Person","fields":[{"name":"home","type":"Address"}]}`,
		References: []map[string]any{{"name": "Address", "subject": "addr", "version": 1}},
	}); err != nil {
		t.Fatal(err)
	}
	register(t, c, "dup-a-value", userV1)
	register(t, c, "dup-b-value", userV1)

	open := func(subject string) string {
		t.Helper()
		rec := req(t, c, "GET", "/_console/inspect/version?key="+url.QueryEscape(joinKey(subject, "1")), nil)
		if rec.Code != 200 {
			t.Fatalf("%s: %d %s", subject, rec.Code, rec.Body)
		}
		return readable(rec.Body.String())
	}
	if text := open("addr"); !strings.Contains(text, "Referenced byperson-value v1") {
		t.Errorf("the referenced schema does not name who references it:\n%s", text)
	}
	if text := open("person-value"); !strings.Contains(text, "Referenced bynothing") {
		t.Errorf("a schema nothing references says otherwise:\n%s", text)
	}
	text := open("dup-a-value")
	if !strings.Contains(text, "Same schema indup-b-value v1") || strings.Contains(text, "no other subject") {
		t.Errorf("the same schema under another subject is not named:\n%s", text)
	}
}

// The versions table says which version is on show, how each was checked against the one
// before at the level in force (including a level of NONE and versions that name other
// schemas), and when each was registered.
func TestTheVersionsTableMarksTheVersionOnShowAndSaysHowEachWasChecked(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-value", userV2)
	register(t, c, "orders-value", userV3)

	body := req(t, c, "GET", "/_console/schemas/orders-value?version=2", nil).Body.String()
	on := regexp.MustCompile(`<a class="r on" href="([^"]*)" data-row aria-current="true">`).FindAllStringSubmatch(body, -1)
	if len(on) != 1 || !strings.Contains(on[0][1], "?version=2&amp;") {
		t.Errorf("the row on show: %v, want only v2's", on)
	}
	text := readable(body)
	if strings.Contains(text, "not kept") {
		t.Errorf("versions registered a moment ago are marked as not kept:\n%s", between(text, "VersionIdWhat", "Schema v2"))
	}
	for _, want := range []string{"v1, BACKWARD ok", "v2, BACKWARD ok"} {
		if !strings.Contains(text, want) {
			t.Errorf("the versions table is missing %q", want)
		}
	}

	if rec := req(t, c, "POST", "/_console/schemas/orders-value/config", url.Values{"level": {"NONE"}}); rec.Code != 303 {
		t.Fatalf("set the level: %d %s", rec.Code, rec.Body)
	}
	text = readable(req(t, c, "GET", "/_console/schemas/orders-value", nil).Body.String())
	for _, want := range []string{"v1, NONE: not checked", "v2, NONE: not checked"} {
		if !strings.Contains(text, want) {
			t.Errorf("at NONE the table is missing %q:\n%s", want, between(text, "VersionIdWhat", "Schema v3"))
		}
	}
	if strings.Contains(text, "v3, NONE") {
		t.Errorf("at NONE the table names a version after the latest:\n%s", between(text, "VersionIdWhat", "Schema v3"))
	}

	// A version that names another schema is checked with the names it uses.
	ctx := context.Background()
	if _, err := c.reg.Register(ctx, "addr", schemaBody{Schema: `{"type":"record","name":"Address","fields":[{"name":"street","type":"string"}]}`}); err != nil {
		t.Fatal(err)
	}
	refs := []map[string]any{{"name": "Address", "subject": "addr", "version": 1}}
	for _, schema := range []string{
		`{"type":"record","name":"Person","fields":[{"name":"home","type":"Address"}]}`,
		`{"type":"record","name":"Person","fields":[{"name":"home","type":"Address"},{"name":"nick","type":["null","string"],"default":null}]}`,
	} {
		if _, err := c.reg.Register(ctx, "person-value", schemaBody{Schema: schema, References: refs}); err != nil {
			t.Fatal(err)
		}
	}
	text = readable(req(t, c, "GET", "/_console/schemas/person-value", nil).Body.String())
	if !strings.Contains(text, "v1, BACKWARD ok") {
		t.Errorf("a version that names another schema was not checked:\n%s", between(text, "VersionIdWhat", "Schema v2"))
	}
}

// A subject whose only version a client's serializer registered has its own page (Sc-18);
// a subject registered in the console, or with more than one version, does not.
func TestASubjectOnlyAClientRegisteredIsMarkedAsTheClients(t *testing.T) {
	c, _, ts := newRegistryConsole(t)
	if s := client(t, "POST", ts.URL+"/subjects/harbour-value/versions", `{"schema":"{\"type\":\"record\",\"name\":\"Ping\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}"}`); s != 200 {
		t.Fatalf("register as a client: %d", s)
	}
	client(t, "POST", ts.URL+"/subjects/pair-value/versions", `{"schema":"{\"type\":\"record\",\"name\":\"Ping\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}"}`)
	client(t, "POST", ts.URL+"/subjects/pair-value/versions", `{"schema":"{\"type\":\"record\",\"name\":\"Ping\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"n\",\"type\":[\"null\",\"long\"],\"default\":null}]}"}`)
	register(t, c, "console-value", userV1)

	raw := req(t, c, "GET", "/_console/schemas/harbour-value", nil).Body.String()
	text := readable(raw)
	for _, want := range []string{"registered by a client", "Nobody registered this by hand", "by Go-http-client"} {
		if !strings.Contains(text, want) {
			t.Errorf("the client's subject is missing %q:\n%s", want, text)
		}
	}
	if keys := keysAtTheFoot(raw); !strings.Contains(keys, "register new version") || strings.Contains(keys, "diff with previous") || strings.Contains(keys, "tabs") {
		t.Errorf("the client's subject has the keys of a page with versions to diff: %s", keys)
	}
	for _, other := range []string{"pair-value", "console-value"} {
		if text := readable(req(t, c, "GET", "/_console/schemas/"+other, nil).Body.String()); strings.Contains(text, "registered by a client") || strings.Contains(text, "Nobody registered this by hand") {
			t.Errorf("%s is marked as a client's:\n%s", other, text)
		}
	}
}

// Each language's subject page, and each tab of it, draws the keys its page really has.
func TestEachKindOfSubjectPageDrawsItsOwnKeys(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "av-value", userV1)
	registerAs(t, c, "js-value", "JSON", jsonV1)
	registerAs(t, c, "pb-value", "PROTOBUF", proto)

	keys := func(subject, tab string) string {
		t.Helper()
		path := "/_console/schemas/" + subject
		if tab != "" {
			path += "?tab=" + tab
		}
		return keysAtTheFoot(page(t, c, path))
	}
	for _, tc := range []struct {
		subject, tab string
		has, lacks   []string
	}{
		{"av-value", "", []string{"diff with previous", "tabs", "register new version"}, nil},
		{"js-value", "", []string{"register new version", "version"}, []string{"diff with previous", "tabs"}},
		{"pb-value", "", []string{"diff with previous", "register new version"}, []string{"tabs"}},
		{"av-value", "compat", []string{"choose level"}, []string{"diff with previous"}},
		{"js-value", "compat", []string{"choose level"}, []string{"diff with previous", "register new version"}},
		{"pb-value", "compat", []string{"choose level"}, []string{"diff with previous", "register new version"}},
		{"av-value", "used", []string{"open messages"}, []string{"diff with previous"}},
		{"js-value", "used", []string{"open messages"}, []string{"diff with previous", "register new version"}},
		{"pb-value", "used", []string{"open messages"}, []string{"diff with previous", "register new version"}},
	} {
		got := keys(tc.subject, tc.tab)
		for _, w := range tc.has {
			if !strings.Contains(got, w) {
				t.Errorf("%s %q: the keys are missing %q: %s", tc.subject, tc.tab, w, got)
			}
		}
		for _, w := range tc.lacks {
			if strings.Contains(got, w) {
				t.Errorf("%s %q: the keys have %q: %s", tc.subject, tc.tab, w, got)
			}
		}
	}
}

// A subject is next to a topic only when the broker has the topic its name says, however
// many other topics there are.
func TestASubjectIsNextToATopicOnlyWhenTheBrokerHasTheTopicItsNameSays(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	topicWith(t, c, "orders", 1, 0)
	topicWith(t, c, "other", 1, 0)
	register(t, c, "orders-value", userV1)
	register(t, c, "ghost-value", userV1)

	if body := page(t, c, "/_console/schemas/orders-value"); !strings.Contains(body, `id="neighbours"`) || !strings.Contains(readable(body), "value schema") {
		t.Errorf("orders-value has the topic orders beside it and the page does not say so")
	}
	if body := page(t, c, "/_console/schemas/ghost-value"); strings.Contains(body, `id="neighbours"`) {
		t.Errorf("ghost-value is named for a topic the broker does not have, and the page names a neighbour:\n%s", readable(between(body, `id="neighbours"`, "</div>")))
	}
}

// Try it answers with the bytes a serializer would send, or with why the value does not
// fit.
func TestTryItAnswersWithTheBytesOrWithWhyTheValueDoesNotFit(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)

	rec := req(t, c, "POST", "/_console/schemas/orders-value/try", url.Values{"version": {"1"}, "doc": {`{"id":5}`}, "do": {"encode"}})
	text := readable(rec.Body.String())
	if rec.Code != 200 || !strings.Contains(text, "6 bytes: magic byte 0, id 1") || !strings.Contains(text, "00 00 00 00 01 | 0a") || strings.Contains(text, "Not encoded") {
		t.Errorf("a value that fits: %d\n%s", rec.Code, text)
	}

	rec = req(t, c, "POST", "/_console/schemas/orders-value/try", url.Values{"version": {"1"}, "doc": {`{"id":"not a number"}`}, "do": {"encode"}})
	text = readable(rec.Body.String())
	if rec.Code != 200 || !strings.Contains(text, "Not encoded") || strings.Contains(text, "bytes: magic byte") {
		t.Errorf("a value that does not fit: %d\n%s", rec.Code, text)
	}
}

// A topic can be given the most partitions a request can name, so that it is the broker
// that refuses; one more than that is refused by the console.
func TestAPartitionTotalAtTheLargestInt32IsLeftToTheBrokerToRefuse(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "wide", 1, 0)
	rec := req(t, c, "POST", "/_console/topics/wide/partitions", url.Values{"total": {"2147483647"}})
	text := readable(rec.Body.String())
	if !strings.Contains(text, "INVALID_PARTITIONS") || strings.Contains(text, "enter the number of partitions") {
		t.Errorf("the largest total (%d): %s", rec.Code, text)
	}
	rec = req(t, c, "POST", "/_console/topics/wide/partitions", url.Values{"total": {"2147483648"}})
	if text := readable(rec.Body.String()); rec.Code != 400 || !strings.Contains(text, "enter the number of partitions") {
		t.Errorf("a total past int32 (%d): %s", rec.Code, text)
	}
}
