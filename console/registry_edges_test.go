package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

func TestRegistryErrorsAreSortedIntoTheConsolesKinds(t *testing.T) {
	for status, want := range map[int][2]bool{404: {true, false}, 409: {false, true}, 422: {false, true}, 400: {false, true}, 500: {false, false}, 401: {false, false}, 200: {false, false}} {
		e := &regError{Status: status, Message: "m"}
		if errors.Is(e, errNotFound) != want[0] || errors.Is(e, errBadInput) != want[1] {
			t.Errorf("status %d: notFound=%v badInput=%v, want %v", status, errors.Is(e, errNotFound), errors.Is(e, errBadInput), want)
		}
	}
}

func TestDiffLinesTable(t *testing.T) {
	render := func(a, b string) string {
		var out []string
		for _, l := range diffLines(a, b) {
			out = append(out, l.Op[:1]+":"+l.Text)
		}
		return strings.Join(out, "|")
	}
	for _, tc := range []struct{ a, b, want string }{
		{"a\nb\nc", "a\nb\nc", "s:a|s:b|s:c"},
		{"a\nb\nc", "a\nc\nd", "s:a|d:b|s:c|a:d"},
		{"x", "y", "d:x|a:y"},
		{"a\nb", "b", "d:a|s:b"},
		{"b", "a\nb", "a:a|s:b"},
		{"a\nb\nc\nd", "a\nd", "s:a|d:b|d:c|s:d"},
		{"a", "a\nb\nc", "s:a|a:b|a:c"},
		{"a\nb\nc", "a", "s:a|d:b|d:c"},
		{"p\nq", "q\np", "d:p|s:q|a:p"},
	} {
		if got := render(tc.a, tc.b); got != tc.want {
			t.Errorf("diff(%q, %q) = %s, want %s", tc.a, tc.b, got, tc.want)
		}
	}
}

// countingRegistry is a registry that counts the requests it is asked.
func countingRegistry(t *testing.T) (*Console, *atomic.Int32, *httptest.Server) {
	t.Helper()
	reg, err := schemaregistry.Open(schemaregistry.NewMemoryStorage())
	if err != nil {
		t.Fatal(err)
	}
	api := schemaregistry.NewAPI(reg, schemaregistry.Options{})
	var n atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		api.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.RegistryURL = ts.URL })
	return c, &n, ts
}

func TestCodecCacheKeepsGoodAnswersAndForgetsBadOnesAfterAWhile(t *testing.T) {
	c, n, _ := countingRegistry(t)
	ctx := context.Background()
	id, err := c.reg.Register(ctx, "orders-value", schemaBody{Schema: userV1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.reg.codecFor(ctx, id, "orders"); err != nil {
		t.Fatal(err)
	}
	before := n.Load()
	for i := 0; i < 3; i++ {
		if _, err := c.reg.codecFor(ctx, id, "orders"); err != nil {
			t.Fatal(err)
		}
	}
	if n.Load() != before {
		t.Errorf("a known schema id was asked of the registry again (%d requests)", n.Load()-before)
	}
	// An id the registry does not know is cached too, briefly.
	if _, err := c.reg.codecFor(ctx, 9999, ""); err == nil {
		t.Fatal("an unknown id has a codec")
	}
	before = n.Load()
	if _, err := c.reg.codecFor(ctx, 9999, ""); err == nil || n.Load() != before {
		t.Errorf("an unknown id was asked again within the 5 seconds (%d requests, %v)", n.Load()-before, err)
	}
	c.reg.mu.Lock()
	c.reg.codecs[9999].at = c.reg.codecs[9999].at.Add(-6e9)
	c.reg.mu.Unlock()
	if _, err := c.reg.codecFor(ctx, 9999, ""); err == nil || n.Load() == before {
		t.Errorf("a refusal older than 5 seconds was not asked again (%v)", err)
	}
}

func TestCodecCacheIsEmptiedPast512Entries(t *testing.T) {
	c, _, _ := countingRegistry(t)
	ctx := context.Background()
	id, err := c.reg.Register(ctx, "t-value", schemaBody{Schema: userV1})
	if err != nil {
		t.Fatal(err)
	}
	fill := func(n int) {
		c.reg.mu.Lock()
		c.reg.codecs = map[int]*codecEntry{}
		for i := 0; i < n; i++ {
			c.reg.codecs[100000+i] = &codecEntry{}
		}
		c.reg.mu.Unlock()
	}
	fill(512)
	if _, err := c.reg.codecFor(ctx, id, ""); err != nil {
		t.Fatal(err)
	}
	if n := len(c.reg.codecs); n != 513 {
		t.Errorf("512 entries and one more: %d kept", n)
	}
	fill(513)
	if _, err := c.reg.codecFor(ctx, id, ""); err != nil {
		t.Fatal(err)
	}
	if n := len(c.reg.codecs); n != 1 {
		t.Errorf("513 entries and one more: %d kept, want the cache started over", n)
	}
}

// A schema known under two subjects is read as the one named for the topic.
func TestDecodeNamesTheSubjectOfTheTopic(t *testing.T) {
	c, _, _ := countingRegistry(t)
	ctx := context.Background()
	for _, s := range []string{"a-subject", "orders-value", "z-orders-key"} {
		if _, err := c.reg.Register(ctx, s, schemaBody{Schema: userV1}); err != nil {
			t.Fatal(err)
		}
	}
	id, _ := c.reg.Register(ctx, "orders-value", schemaBody{Schema: userV1})
	enc, err := c.reg.encode(ctx, "orders-value", `{"id": 7}`)
	if err != nil {
		t.Fatal(err)
	}
	for topic, want := range map[string]string{"orders": "orders-value", "": "a-subject", "other": "a-subject"} {
		c.reg.mu.Lock()
		c.reg.codecs = map[int]*codecEntry{}
		c.reg.mu.Unlock()
		d, ok := c.reg.decode(ctx, topic, enc)
		if !ok || d.Subject != want || d.ID != id || d.Version != 1 || !strings.Contains(d.JSON, `"id":7`) && !strings.Contains(d.JSON, `"id": 7`) {
			t.Errorf("topic %q: %+v %v, want subject %s", topic, d, ok, want)
		}
	}
	if _, ok := c.reg.decode(ctx, "orders", []byte("plain text")); ok {
		t.Error("plain text decoded")
	}
	if _, err := c.reg.encode(ctx, "orders-value", `{"id": "not a long"}`); err == nil || !errors.Is(err, errBadInput) || !strings.Contains(err.Error(), "does not fit orders-value version 1") {
		t.Errorf("a value that does not fit: %v", err)
	}
	if _, err := c.reg.encode(ctx, "nobody-value", `{}`); err == nil {
		t.Error("encoding under a subject that does not exist")
	}
}

// References are followed to their text, but not without end.
func TestReferenceDepthIsBounded(t *testing.T) {
	c, _, _ := countingRegistry(t)
	ctx := context.Background()
	record := func(k int, ref bool) string {
		if !ref {
			return fmt.Sprintf(`{"type":"record","name":"t%d","fields":[{"name":"id","type":"long"}]}`, k)
		}
		return fmt.Sprintf(`{"type":"record","name":"t%d","fields":[{"name":"next","type":"t%d"}]}`, k, k+1)
	}
	const levels = 18
	for k := levels; k >= 0; k-- {
		b := schemaBody{Schema: record(k, k < levels)}
		if k < levels {
			b.References = []map[string]any{{"name": fmt.Sprintf("t%d", k+1), "subject": fmt.Sprintf("s%d", k+1), "version": 1}}
		}
		if _, err := c.reg.Register(ctx, fmt.Sprintf("s%d", k), b); err != nil {
			t.Fatalf("register s%d: %v", k, err)
		}
	}
	// s2 has 16 schemas beneath it; s1 has 17.
	for _, tc := range []struct {
		subject string
		ok      bool
	}{{"s3", true}, {"s2", true}, {"s1", false}, {"s0", false}} {
		v, err := c.reg.Version(ctx, tc.subject, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.reg.resolve(ctx, v, 0)
		if tc.ok && err != nil {
			t.Errorf("%s: %v", tc.subject, err)
		}
		if !tc.ok && (err == nil || !strings.Contains(err.Error(), "nested too deeply")) {
			t.Errorf("%s: %v, want the nesting refused", tc.subject, err)
		}
	}
	var dangling schemaVersion
	if err := json.Unmarshal([]byte(`{"references":[{"name":"x","subject":"missing","version":1}]}`), &dangling); err != nil {
		t.Fatal(err)
	}
	if _, err := c.reg.resolve(ctx, dangling, 0); err == nil {
		t.Error("a reference to a subject that is not there resolved")
	}
	if _, err := c.reg.Version(ctx, "s2", 0); err != nil {
		t.Fatal(err)
	}
}

// Deleting a version or a subject is permanent: it is gone, not set aside.
func TestDeletesAreAlsoPermanent(t *testing.T) {
	c, _, ts := countingRegistry(t)
	ctx := context.Background()
	for _, s := range []string{"b-value", "a-value", "c-value"} {
		if _, err := c.reg.Register(ctx, s, schemaBody{Schema: userV1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.reg.Register(ctx, "a-value", schemaBody{Schema: userV2}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.reg.SubjectNames(ctx), ","); got != "a-value,b-value,c-value" {
		t.Errorf("subjects %s", got)
	}
	soft := func(path string) string {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var b strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			b.Write(buf[:n])
			if err != nil {
				break
			}
		}
		return b.String()
	}
	if err := c.reg.DeleteVersion(ctx, "a-value", 1); err != nil {
		t.Fatal(err)
	}
	if got := soft("/subjects/a-value/versions?deleted=true"); strings.Contains(got, "1,") || got != "[2]\n" && got != "[2]" {
		t.Errorf("after deleting version 1 for good, versions incl. deleted: %s", got)
	}
	if err := c.reg.DeleteSubject(ctx, "b-value"); err != nil {
		t.Fatal(err)
	}
	if got := soft("/subjects?deleted=true"); strings.Contains(got, "b-value") {
		t.Errorf("a deleted subject is still held: %s", got)
	}
	if err := c.reg.DeleteVersion(ctx, "a-value", 7); err == nil {
		t.Error("deleting a version that does not exist")
	}
	if err := c.reg.DeleteSubject(ctx, "nobody"); err == nil {
		t.Error("deleting a subject that does not exist")
	}
}

// The subject page: the versions newest first with what each changed, and the diff
// from the version before the one on show.
func TestSchemaPageVersionsAndDiff(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-value", userV2)
	register(t, c, "orders-value", userV2+" ") // the same schema again is not a new version.
	body := req(t, c, "GET", "/_console/schemas/orders-value", nil).Body.String()
	text := readable(body)
	i2 := strings.Index(text, "v2")
	i1 := strings.Index(text, "v1")
	if i2 < 0 || i1 < 0 || i2 > i1 {
		t.Errorf("v2 should be listed before v1 (%d, %d)", i2, i1)
	}
	if !strings.Contains(text, "first version") || !strings.Contains(text, "line") || !strings.Contains(text, "changed from v1") {
		t.Errorf("what each version changed is not said:\n%s", text)
	}
	if strings.Count(text, "changed from v1") != 1 {
		t.Errorf("v2 only changes from v1: %q", text)
	}
	for q, wantOn := range map[string]string{"": "Versions", "?tab=compat": "Compatibility", "?tab=register": "Register", "?tab=bogus": "Versions"} {
		b := req(t, c, "GET", "/_console/schemas/orders-value"+q, nil).Body.String()
		on := tabOnRe.FindAllStringSubmatch(between(b, `aria-label="Subject tabs"`, "</nav>"), -1)
		if len(on) != 1 || on[0][1] != wantOn && !strings.HasPrefix(wantOn, on[0][1]) {
			t.Errorf("%q: on %v, want %s", q, on, wantOn)
		}
	}
	// A subject with one version has nothing to compare; an unknown one is not found, with neighbours offered.
	register(t, c, "payments-value", userV1)
	if b := readable(req(t, c, "GET", "/_console/schemas/payments-value", nil).Body.String()); !strings.Contains(b, "first version") || strings.Contains(b, "changed from") {
		t.Errorf("a single version: %s", b)
	}
	rec := req(t, c, "GET", "/_console/schemas/order-value", nil)
	if rec.Code != 404 || !strings.Contains(rec.Body.String(), `href="/_console/schemas/orders-value"`) || !strings.Contains(rec.Body.String(), "Subject not found") {
		t.Errorf("a near miss: %d", rec.Code)
	}
	// Old versions can be looked at, and are not the latest.
	v1 := req(t, c, "GET", "/_console/schemas/orders-value?version=1", nil).Body.String()
	if strings.Contains(v1, `"email"`) {
		t.Error("version 1 shows the fields version 2 added")
	}
}

func TestSchemaFromForm(t *testing.T) {
	post := func(v url.Values) (schemaBody, error) {
		r := httptest.NewRequest("POST", "/x", strings.NewReader(v.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return schemaFromForm(r)
	}
	if _, err := post(url.Values{"schema": {"  "}}); err == nil {
		t.Error("an empty schema")
	}
	if b, err := post(url.Values{"schema": {" {} "}}); err != nil || b.Schema != "{}" || b.SchemaType != "" || b.References != nil {
		t.Errorf("plain: %+v %v", b, err)
	}
	if b, err := post(url.Values{"schema": {"x"}, "type": {"AVRO"}}); err != nil || b.SchemaType != "" {
		t.Errorf("Avro is the default and is not sent: %+v", b)
	}
	if b, err := post(url.Values{"schema": {"x"}, "type": {"PROTOBUF"}}); err != nil || b.SchemaType != "PROTOBUF" {
		t.Errorf("protobuf: %+v", b)
	}
	if b, err := post(url.Values{"schema": {"x"}, "references": {"null"}}); err != nil || b.References != nil {
		t.Errorf("null references: %+v %v", b, err)
	}
	if b, err := post(url.Values{"schema": {"x"}, "references": {"[]"}}); err != nil || b.References != nil {
		t.Errorf("no references: %+v %v", b, err)
	}
	if b, err := post(url.Values{"schema": {"x"}, "references": {`[{"name":"a","subject":"s","version":1}]`}}); err != nil || b.References == nil {
		t.Errorf("one reference: %+v %v", b, err)
	}
	if _, err := post(url.Values{"schema": {"x"}, "references": {"{not json"}}); err == nil || !strings.Contains(err.Error(), "not a JSON list") {
		t.Errorf("bad references: %v", err)
	}
}
