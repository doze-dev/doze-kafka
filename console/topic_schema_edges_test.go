package console

import (
	"context"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestTopicSchemasComposerAndTags(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	ctx := context.Background()
	topicWith(t, c, "orders", 1, 0)
	topicWith(t, c, "plain", 1, 0)
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-value", userV2)
	register(t, c, "orders-key", userV1)
	register(t, c, "elsewhere-value", userV1)
	r := httptest.NewRequest("GET", "/x", nil)

	got := c.topicSchemas(r, "orders")
	if len(got) != 2 || got[0].Label != "key" || got[0].Subject != "orders-key" || got[0].Version != 1 || got[1].Label != "value" || got[1].Version != 2 ||
		got[1].URL != "/_console/schemas/orders-value" || got[1].TypeLabel != "Avro" {
		t.Errorf("%+v", got)
	}
	if got = c.topicSchemas(r, "plain"); len(got) != 0 {
		t.Errorf("a topic with no subject: %+v", got)
	}
	if got = newTestConsole(t).topicSchemas(r, "orders"); got != nil {
		t.Errorf("no registry: %+v", got)
	}

	data := map[string]any{}
	c.composerSchemas(r, "orders", data)
	if strings.Join(data["SchemaSubjects"].([]string), ",") != "elsewhere-value,orders-key,orders-value" || data["DefaultSubject"] != "orders-value" {
		t.Errorf("%v", data)
	}
	data = map[string]any{}
	c.composerSchemas(r, "plain", data)
	if _, ok := data["DefaultSubject"]; ok || data["SchemaSubjects"] == nil {
		t.Errorf("a topic with no subject of its own: %v", data)
	}
	data = map[string]any{}
	newTestConsole(t).composerSchemas(r, "orders", data)
	if len(data) != 0 {
		t.Errorf("no registry: %v", data)
	}

	// A record written through the registry is read as the JSON it means, with its schema named.
	enc, err := c.reg.encode(ctx, "orders-value", `{"id": 5, "email": "a@b.c"}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.be.Produce(ctx, "orders", 0, []byte("k1"), enc, nil, "1"); err != nil {
		t.Fatal(err)
	}
	m := Message{Offset: 0, Value: enc, Key: []byte("k1")}
	v := c.msgView(ctx, "orders", m)
	if v.Value.Schema == nil || v.Value.Schema.Subject != "orders-value" || v.Value.Schema.Version != 2 || v.Value.Schema.URL != "/_console/schemas/orders-value?version=2" ||
		v.Value.Binary || !strings.Contains(v.Value.Preview, `"email"`) || v.Key.Schema != nil {
		t.Errorf("%+v", v.Value)
	}
	rv := c.readThroughRegistry(ctx, "orders", newBytesView(enc, false), enc)
	if rv.Schema == nil || !rv.Pretty || rv.Binary || rv.Truncated || rv.Raw != "" || !strings.Contains(rv.Text, "\n") || !strings.Contains(rv.Text, "a@b.c") {
		t.Errorf("%+v", rv)
	}
	null := newBytesView(nil, true)
	if got := c.readThroughRegistry(ctx, "orders", null, nil); got.Schema != nil || !got.Null {
		t.Errorf("a null: %+v", got)
	}
	plain := newBytesView([]byte("hello"), false)
	if got := c.readThroughRegistry(ctx, "orders", plain, []byte("hello")); got.Schema != nil || got.Raw != "hello" {
		t.Errorf("plain text: %+v", got)
	}
	// The topic page names the schema of its newest record, and its tab opens.
	page := readable(req(t, c, "GET", "/_console/topics/orders", nil).Body.String())
	if !strings.Contains(page, "orders-value") || !strings.Contains(page, "Avro") {
		t.Errorf("topic page:\n%s", page)
	}
	rawTabs := req(t, c, "GET", "/_console/topics/orders?tab=schema", nil).Body.String()
	if on := tabOnRe.FindAllStringSubmatch(between(rawTabs, `aria-label="Topic views"`, "</nav>"), -1); len(on) != 1 || on[0][1] != "Schema" {
		t.Errorf("the schema tab: %v", on)
	}
	if on := tabOnRe.FindAllStringSubmatch(between(req(t, c, "GET", "/_console/topics/plain?tab=schema", nil).Body.String(), `aria-label="Topic views"`, "</nav>"), -1); len(on) != 1 || on[0][1] != "Messages" {
		t.Errorf("a schema tab for a topic with no schema: %v", on)
	}
}

// The create form's command follows what is typed.
func TestTopicShellFollowsTheForm(t *testing.T) {
	c := newTestConsole(t)
	shell := func(q url.Values) string {
		rec := req(t, c, "GET", "/_console/new/topic/shell?"+q.Encode(), nil)
		if rec.Code != 200 {
			t.Fatalf("%d", rec.Code)
		}
		return readable(rec.Body.String())
	}
	empty := shell(url.Values{})
	if !strings.Contains(empty, "--topic orders.created") || !strings.Contains(empty, "--partitions 1 --replication-factor 1") || strings.Contains(empty, "--config") {
		t.Errorf("empty form:\n%s", empty)
	}
	full := shell(url.Values{"name": {" my topic "}, "partitions": {" 3 "}, "retention.ms": {"7d"}, "segment.bytes": {"64MB"}})
	for _, want := range []string{"--topic 'my topic'", "--partitions 3", "--config retention.ms=604800000", "--config segment.bytes=67108864"} {
		if !strings.Contains(full, want) {
			t.Errorf("full form lacks %q:\n%s", want, full)
		}
	}
	bad := shell(url.Values{"name": {"t"}, "retention.ms": {"soon"}, "retention.bytes": {"1GB"}})
	if strings.Contains(bad, "--config retention.ms") || !strings.Contains(bad, "--config retention.bytes=1073741824") || !strings.Contains(bad, "Keep records for") {
		t.Errorf("a value that does not parse is left out and named:\n%s", bad)
	}
	if strings.Contains(shell(url.Values{"retention.ms": {"7d"}}), "Keep records for:") {
		t.Error("a note for a value that parsed")
	}
}

func TestTopicCreateFormFacts(t *testing.T) {
	c, _, _ := newRegistryConsole(t, func(o *Options) { o.Facts.MaxPartitions = 77 })
	topicWith(t, c, "taken", 1, 0)
	body := req(t, c, "GET", "/_console/new/topic?name=%20prefill%20", nil).Body.String()
	for _, want := range []string{`"taken"`, `"prefill"`, "77"} {
		if !strings.Contains(body, want) {
			t.Errorf("create form lacks %s", want)
		}
	}
	plain := newTestConsole(t)
	if strings.Contains(req(t, plain, "GET", "/_console/new/topic", nil).Body.String(), "prefill") {
		t.Error("a prefill nobody asked for")
	}
	// Creating: what was typed is what is made, or an error.
	for _, bad := range []string{"abc", "0", "-5", "4294967299", "1.5"} {
		if rec := req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"x" + bad}, "partitions": {bad}}); rec.Code != 400 {
			t.Errorf("partitions %q: %d", bad, rec.Code)
		}
	}
	rec := req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"made"}, "partitions": {" 4 "}, "retention.ms": {"1h"}})
	if rec.Code != 303 || !strings.Contains(rec.Header().Get("Location"), "flash=Topic+created") {
		t.Fatalf("%d %s", rec.Code, rec.Header().Get("Location"))
	}
	if n, err := c.be.PartitionCount(context.Background(), "made"); err != nil || n != 4 {
		t.Errorf("%d %v", n, err)
	}
	if cfg, _ := c.be.topicOverrides(context.Background(), "made"); cfg["retention.ms"] != "3600000" {
		t.Errorf("config %v", cfg)
	}
	if rec := req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"made2"}}); rec.Code != 303 {
		t.Errorf("default partitions: %d", rec.Code)
	} else if n, _ := c.be.PartitionCount(context.Background(), "made2"); n != 1 {
		t.Errorf("default partitions: %d", n)
	}
}
