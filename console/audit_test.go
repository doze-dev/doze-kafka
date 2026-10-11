package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Tests added by the October audit for the console's server side.

// ---- JSON display is bounded ----

func nested(depth int) []byte {
	return []byte(strings.Repeat("[", depth) + strings.Repeat("]", depth))
}

// The hand-written indenter must agree with encoding/json on ordinary values.
func TestIndentJSONMatchesTheStandardLibrary(t *testing.T) {
	for _, src := range []string{
		`{"a":1,"b":[1,2,{"c":null}],"d":"x"}`,
		`[]`, `{}`, `[[],{}]`, `{"a":{}}`,
		`{"s":"brackets ] } [ { , : in a string","e":"quote \" and \\ backslash \\\\"}`,
		`{"u":"é ∅ 日本","n":-1.5e10,"t":true,"f":false}`,
		" {\n\t\"spaced\" :  [ 1 ,\r\n 2 ]  } ",
		`[1,[2,[3,[4]]]]`,
	} {
		got, ok := indentJSON([]byte(strings.TrimSpace(src)))
		var want bytes.Buffer
		if err := json.Indent(&want, []byte(strings.TrimSpace(src)), "", "  "); err != nil {
			t.Fatalf("test input is not JSON: %s", src)
		}
		if !ok || got != want.String() {
			t.Errorf("indentJSON(%s):\n got %q\nwant %q", src, got, want.String())
		}
	}
	if _, ok := indentJSON([]byte(`"just a string"`)); ok {
		t.Error("a bare scalar is not re-indented")
	}
	if _, ok := indentJSON([]byte(`{not json`)); ok {
		t.Error("invalid JSON is not re-indented")
	}
}

// Indentation grows with the square of the nesting depth. A 6 KB value became
// 18 MB of text, and a page of twenty such rows allocated gigabytes.
func TestDeeplyNestedJSONIsNotExpanded(t *testing.T) {
	v := newBytesView(nested(3000), false)
	if v.Pretty || len(v.Text) != 6000 {
		t.Fatalf("a 6 KB deeply nested value: Pretty=%v, %d bytes of text; want it shown as sent", v.Pretty, len(v.Text))
	}
	if small := newBytesView([]byte(`{"a":[1,2]}`), false); !small.Pretty {
		t.Error("ordinary JSON should still be re-indented")
	}

	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"n"}, "partitions": {"1"}})
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for i := 0; i < 20; i++ {
		if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: "n", Key: nested(3000), Value: nested(3000)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	page := req(t, c, "GET", "/_console/topics/n", nil)
	inspector := req(t, c, "GET", "/_console/topics/n/message?partition=0&offset=0", nil)
	runtime.ReadMemStats(&after)
	if page.Code != 200 || inspector.Code != 200 {
		t.Fatalf("page %d, inspector %d", page.Code, inspector.Code)
	}
	if mb := (after.TotalAlloc - before.TotalAlloc) >> 20; mb > 64 {
		t.Errorf("rendering a page and a inspector of deeply nested records allocated %d MB", mb)
	}
	if kb := inspector.Body.Len() >> 10; kb > 512 {
		t.Errorf("the inspector for a 6 KB record is %d KB", kb)
	}
}

// ---- "Send again" is exact ----

func TestSendAgainProducesAnExactCopy(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"x"}, "partitions": {"1"}})
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	bigBinary := bytes.Repeat([]byte{0x00, 0xff, 0x10}, 30_000) // 90 KB: more than a page will carry.
	records := []*kgo.Record{
		{Key: []byte(`{"id":7}`), Value: []byte(`{"a":1,"b":[1,2]}`), Headers: []kgo.RecordHeader{{Key: "ctx", Value: []byte(`{"trace":"abc"}`)}}},
		{Key: []byte{}, Value: []byte("an empty, non-null key")},
		{Key: nil, Value: []byte("line one\r\nline two\r\n")},
		{Key: []byte("multi\nline\tkey"), Value: nil},
		{Key: bigBinary, Value: []byte{0xfe, 0xff}, Headers: []kgo.RecordHeader{{Key: "", Value: []byte("x")}, {Key: "k\xff", Value: bigBinary}, {Key: "empty", Value: []byte{}}}},
	}
	for _, r := range records {
		r.Topic = "x"
		if err := cl.ProduceSync(context.Background(), r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range records {
		rec := req(t, c, "POST", "/_console/topics/x/resend", url.Values{"partition": {"0"}, "offset": {itoa(i)}})
		if rec.Code != 200 {
			t.Fatalf("resend of record %d: %d %s", i, rec.Code, rec.Body)
		}
		got := fetchRaw(t, c, "x", int64(len(records)+i))
		if !sameBytes(got.Key, want.Key) || !sameBytes(got.Value, want.Value) || len(got.Headers) != len(want.Headers) {
			t.Errorf("record %d resent as key %q value %q (%d headers); want key %q value %q (%d headers)",
				i, got.Key, got.Value, len(got.Headers), want.Key, want.Value, len(want.Headers))
			continue
		}
		for j, h := range want.Headers {
			if got.Headers[j].Key != h.Key || !bytes.Equal(got.Headers[j].Value, h.Value) {
				t.Errorf("record %d header %d resent as %q=%q, want %q=%q", i, j, got.Headers[j].Key, got.Headers[j].Value, h.Key, h.Value)
			}
		}
	}
	if rec := req(t, c, "POST", "/_console/topics/x/resend", url.Values{"partition": {"0"}, "offset": {"999"}}); rec.Code != 404 {
		t.Errorf("resend of a record that is not there: %d, want 404", rec.Code)
	}

	// "Edit a copy" carries the text as stored, not as displayed.
	d := req(t, c, "GET", "/_console/topics/x/message?partition=0&offset=0", nil).Body.String()
	if !strings.Contains(d, `data-value="{&#34;a&#34;:1,&#34;b&#34;:[1,2]}"`) {
		t.Error("the composer would be prefilled with re-indented JSON, not the JSON that was stored")
	}
	if !strings.Contains(d, "Send again") || !strings.Contains(d, "Edit a copy") {
		t.Error("the inspector should offer both an exact resend and an editable copy")
	}
}

// sameBytes compares two byte slices, treating nil (a null key or value) as
// different from empty.
func sameBytes(a, b []byte) bool {
	return (a == nil) == (b == nil) && bytes.Equal(a, b)
}

func fetchRaw(t *testing.T, c *Console, topic string, offset int64) Message {
	t.Helper()
	msgs, err := c.be.FetchWindow(context.Background(), topic, 0, offset, 1)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("FetchWindow(%d) = %d msgs, %v", offset, len(msgs), err)
	}
	return msgs[0]
}

// ---- what is typed is what happens, or it is an error ----

func TestNumbersAreValidatedNotCoerced(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"3"}})
	produceTo(t, c, "t", 0, 2)

	for _, p := range []string{"abc", "0", "-5", "2.5", "4294967299"} {
		rec := req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"made-" + p}, "partitions": {p}})
		if rec.Code != 400 {
			t.Errorf("creating a topic with partitions=%q: %d, want 400", p, rec.Code)
		}
	}
	if list := req(t, c, "GET", "/_console/topics", nil).Body.String(); strings.Contains(list, "made-") {
		t.Error("a topic was created from an invalid partition count")
	}
	for _, p := range []string{"4294967296", "4294967297", "3", "-1"} {
		if rec := req(t, c, "POST", "/_console/topics/t/produce", url.Values{"partition": {p}, "value": {"x"}}); rec.Code != 400 {
			t.Errorf("producing to partition %q of a 3-partition topic: %d, want 400", p, rec.Code)
		}
	}
	for _, q := range []string{"partition=abc&offset=0", "partition=-3&offset=0", "partition=0&offset=x"} {
		if rec := req(t, c, "GET", "/_console/topics/t/message/raw?"+q, nil); rec.Code != 400 {
			t.Errorf("raw download with %s: %d, want 400", q, rec.Code)
		}
	}
	for _, j := range []string{"abc", "-4", "12abc", "99999999999999999999"} {
		page := req(t, c, "GET", "/_console/topics/t?jump="+url.QueryEscape(j), nil).Body.String()
		if !strings.Contains(page, "That is not an offset") {
			t.Errorf("jump=%q was ignored without a word", j)
		}
	}
	// The URL-safe base64 alphabet is accepted, as the comment has always said.
	if rec := req(t, c, "POST", "/_console/topics/t/produce", url.Values{"partition": {"0"}, "value": {"-_-_"}, "value_enc": {"base64"}}); rec.Code != 200 {
		t.Errorf("URL-safe base64: %d %s", rec.Code, rec.Body)
	}
}

// ---- text and binary ----

func TestPreviewAndTruncationAreAccurate(t *testing.T) {
	// A character straddling the 4096-byte sample cut is not evidence of binary.
	straddle := append(bytes.Repeat([]byte("a"), 4095), []byte("é and more text")...)
	if v := previewOnly(straddle); v.Binary {
		t.Error("text whose 4096th byte falls inside a character is previewed as binary")
	}
	for _, bin := range [][]byte{{0xff, 0xfe, 0x00}, {0xff}, {0xc3}, {0x00, 0x01}} {
		if v := previewOnly(bin); !v.Binary {
			t.Errorf("%x is previewed as text", bin)
		}
		if v := newBytesView(bin, false); !v.Binary {
			t.Errorf("%x is shown as text", bin)
		}
	}
	// A value shown whole does not claim to be cut short.
	mid := []byte(`{"pad":"` + strings.Repeat("x", 17<<10) + `"}`)
	if v := newBytesView(mid, false); v.Truncated || !v.Pretty || !v.HexCut {
		t.Errorf("a 17 KB JSON value: Truncated=%v Pretty=%v HexCut=%v; the text is whole (only the hex view is cut)", v.Truncated, v.Pretty, v.HexCut)
	}
	if v := newBytesView(bytes.Repeat([]byte("y"), displayLimit+10), false); !v.Truncated {
		t.Error("a value longer than the display limit should say it is cut")
	}
}

// ---- purge ----

func TestPurgeKeepsTheTopicsOwnConfig(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ct := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = "cfg", 2, 1
	for k, v := range map[string]string{"retention.ms": "60000", "retention.bytes": "1048576"} {
		rc := kmsg.NewCreateTopicsRequestTopicConfig()
		rc.Name, rc.Value = k, kmsg.StringPtr(v)
		rt.Configs = append(rt.Configs, rc)
	}
	ct.Topics = append(ct.Topics, rt)
	if resp, err := ct.RequestWith(context.Background(), cl); err != nil || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create: %v %+v", err, resp)
	}
	produceTo(t, c, "cfg", 0, 3)
	before, _ := c.be.topicOverrides(context.Background(), "cfg")

	if rec := req(t, c, "POST", "/_console/topics/cfg/purge", url.Values{}); rec.Code != http.StatusSeeOther {
		t.Fatalf("purge: %d %s", rec.Code, rec.Body)
	}
	after, err := c.be.topicOverrides(context.Background(), "cfg")
	if err != nil || len(before) != 2 || fmt.Sprint(after) != fmt.Sprint(before) {
		t.Errorf("config before purge %v, after %v (%v); the topic's own settings must survive", before, after, err)
	}
	if n, _ := c.be.PartitionCount(context.Background(), "cfg"); n != 2 {
		t.Errorf("partitions after purge: %d", n)
	}
}

// ---- live polling notices every change ----

func TestTapeHashChangesWhenATopicIsPurgedAndRefilled(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"p"}, "partitions": {"1"}})
	produceTo(t, c, "p", 0, 3)
	first := doReq(t, c, "GET", "/_console/topics/p/tape?partition=0", map[string]string{"HX-Request": "true"})
	hash := first.Header().Get("HX-Live-Hash")
	if first.Code != 200 || hash == "" {
		t.Fatalf("first poll: %d, hash %q", first.Code, hash)
	}
	if rec := doReq(t, c, "GET", "/_console/topics/p/tape?partition=0&h="+url.QueryEscape(hash), map[string]string{"HX-Request": "true"}); rec.Code != 204 {
		t.Fatalf("an unchanged topic should answer 204, got %d", rec.Code)
	}
	req(t, c, "POST", "/_console/topics/p/purge", url.Values{})
	produceTo(t, c, "p", 0, 3) // the same three offsets, different records.
	rec := doReq(t, c, "GET", "/_console/topics/p/tape?partition=0&h="+url.QueryEscape(hash), map[string]string{"HX-Request": "true"})
	if rec.Code != 200 {
		t.Errorf("after a purge and three new records at the same offsets the poll answered %d; another tab would keep showing the old records", rec.Code)
	}
}

// ---- group ids that are path navigation ----

func TestGroupsNamedDotAndDotDotAreReachable(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "t", 1, 3)
	for _, id := range []string{".", "..", "~tilde", "a/b", "plain"} {
		commitFor(t, addr, id, "t", map[int32]int64{0: 1})
		u := c.groupURL(id)
		if strings.HasSuffix(u, "/.") || strings.HasSuffix(u, "/..") {
			t.Fatalf("the link to group %q is %s, which a browser resolves away", id, u)
		}
		page := req(t, c, "GET", u, nil)
		if page.Code != 200 || !strings.Contains(page.Body.String(), "Offsets &amp; lag") {
			t.Errorf("group %q at %s: %d", id, u, page.Code)
			continue
		}
		if rec := req(t, c, "POST", u+"/reset", url.Values{"to": {"latest"}}); rec.Code != 200 {
			t.Errorf("reset of group %q: %d %s", id, rec.Code, rec.Body)
		}
		if rec := req(t, c, "POST", u+"/delete", url.Values{}); rec.Code != http.StatusSeeOther {
			t.Errorf("delete of group %q: %d", id, rec.Code)
		}
	}
}

// ---- errors ----

func TestErrorsAreClassifiedByWhatTheyAre(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
	}{
		{kerr.RequestTimedOut, 503},
		{kerr.KafkaStorageError, 503},
		{kerr.CoordinatorLoadInProgress, 503},
		{kerr.UnknownServerError, 503},
		{kerr.InvalidTopicException, 400},
		{kerr.TopicAlreadyExists, 400},
		{kerr.UnknownTopicOrPartition, 404},
		// The words in a name do not make a network error.
		{fmt.Errorf("metadata for EOF-events: %w", kerr.InvalidTopicException), 400},
		{errors.New("topic connection refused-events is misconfigured"), 500},
		{context.DeadlineExceeded, 502},
		{errors.New("unable to dial: dial tcp 127.0.0.1:1: connect: connection refused"), 502},
	} {
		if got := refusalFor(tc.err).Status; got != tc.status {
			t.Errorf("refusalFor(%v) = %d, want %d", tc.err, got, tc.status)
		}
	}
}

// A partial (htmx) request that fails is explained in the same plain words a page
// would use, not with the client library's raw error.
func TestPartialErrorsDoNotShowRawClientErrors(t *testing.T) {
	old := requestTimeout
	requestTimeout = 300 * time.Millisecond
	defer func() { requestTimeout = old }()
	c, err := New(Options{Addr: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	rec := doReq(t, c, "GET", "/_console/topics/t/tape", map[string]string{"HX-Request": "true"})
	body := rec.Body.String()
	if rec.Code != 502 || strings.Contains(body, "dial tcp") || !strings.Contains(body, "could not get an answer from the broker") {
		t.Errorf("a failed poll: %d %q", rec.Code, body)
	}
}

// A record that is not there is found out at once, and a topic that does not exist
// is reported as that, not as a record lost to retention.
func TestAbsentRecordsAreAnsweredQuicklyAndCorrectly(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"1"}})
	produceTo(t, c, "t", 0, 2)

	start := time.Now()
	rec := req(t, c, "GET", "/_console/topics/t/message?partition=0&offset=50", nil)
	if took := time.Since(start); rec.Code != 404 || took > 400*time.Millisecond {
		t.Errorf("an offset past the end: %d after %v; want 404 without waiting out a fetch", rec.Code, took.Round(time.Millisecond))
	}
	if _, err := c.be.FetchWindow(context.Background(), "no-such-topic", 0, 0, 1); err == nil {
		t.Error("FetchWindow for a topic that does not exist returned no error")
	}
	if rec := req(t, c, "GET", "/_console/topics/t/message?partition=9&offset=0", nil); rec.Code != 404 || strings.Contains(rec.Body.String(), "retention") {
		t.Errorf("a partition that does not exist: %d %q", rec.Code, rec.Body.String())
	}
}

func TestSuggestionsAreOnlyForNearMisses(t *testing.T) {
	c := newTestConsole(t)
	for _, n := range []string{"zz", "orders", "cd"} {
		req(t, c, "POST", "/_console/new/topic", url.Values{"name": {n}, "partitions": {"1"}})
	}
	names := func(missing string) []string {
		var out []string
		for _, s := range c.suggest(context.Background(), "topic", missing) {
			out = append(out, s.Name)
		}
		return out
	}
	if got := names("ordres"); len(got) != 1 || got[0] != "orders" {
		t.Errorf("suggestions for ordres: %v, want [orders]", got)
	}
	if got := names("a"); len(got) != 0 {
		t.Errorf("suggestions for a: %v, want none (zz is not a near miss of a)", got)
	}
	if got := names("ab"); len(got) != 0 {
		t.Errorf("suggestions for ab: %v, want none", got)
	}
}

// ---- hardening ----

func TestPagesCannotBeFramedAndCrossSiteWritesAreRefused(t *testing.T) {
	c := newTestConsole(t)
	page := req(t, c, "GET", "/_console/topics", nil)
	if page.Header().Get("X-Frame-Options") != "DENY" || !strings.Contains(page.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") {
		t.Errorf("framing headers: X-Frame-Options=%q CSP=%q", page.Header().Get("X-Frame-Options"), page.Header().Get("Content-Security-Policy"))
	}
	r, _ := http.NewRequest("POST", "/_console/new/topic", strings.NewReader("name=x&partitions=1"))
	r.Host = "127.0.0.1:9093"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Sec-Fetch-Site", "cross-site") // and no Origin.
	rec := httptest.NewRecorder()
	serveInto(t, c, rec, r)
	if rec.Code != http.StatusForbidden {
		t.Errorf("a cross-site POST with no Origin header: %d, want 403", rec.Code)
	}
}

func TestUnknownPathsAre404AndRootMountWorks(t *testing.T) {
	c := newTestConsole(t)
	for _, p := range []string{"/_console/no/such/page", "/_console/groups/", "/_console/topicz"} {
		if rec := req(t, c, "GET", p, nil); rec.Code != 404 {
			t.Errorf("GET %s = %d, want 404", p, rec.Code)
		}
	}
	for _, p := range []string{"/_console", "/_console/", "/_console/topics"} {
		if rec := req(t, c, "GET", p, nil); rec.Code != 200 {
			t.Errorf("GET %s = %d, want 200", p, rec.Code)
		}
	}
	for _, p := range []string{"/_console/static/", "/_console/static/fonts/"} {
		if rec := req(t, c, "GET", p, nil); rec.Code != 404 {
			t.Errorf("GET %s = %d, want 404 (no directory listings)", p, rec.Code)
		}
	}
	// Mounted at the root. It used to panic building its routes.
	root, err := New(Options{Addr: "127.0.0.1:1", Prefix: "/"})
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if rec := req(t, root, "GET", "/static/app.css", nil); rec.Code != 200 {
		t.Errorf("root-mounted console: GET /static/app.css = %d", rec.Code)
	}
}

// Dev mode serves static/ from disk, and only static/.
func TestDevModeServesOnlyStaticFiles(t *testing.T) {
	dir := t.TempDir()
	for _, sub := range []string{"templates", "static"} {
		os.MkdirAll(filepath.Join(dir, sub), 0o755)
	}
	entries, _ := templateFS.ReadDir("templates")
	for _, e := range entries {
		b, _ := templateFS.ReadFile("templates/" + e.Name())
		os.WriteFile(filepath.Join(dir, "templates", e.Name()), b, 0o644)
	}
	os.WriteFile(filepath.Join(dir, "static", "app.css"), []byte("/* dev */"), 0o644)
	os.WriteFile(filepath.Join(dir, "secret.go"), []byte("package secret"), 0o644)
	t.Setenv("DOZE_KAFKA_CONSOLE_DEV", dir)
	c := newTestConsole(t)

	if rec := req(t, c, "GET", "/_console/static/app.css", nil); rec.Code != 200 || rec.Body.String() != "/* dev */" {
		t.Fatalf("static file: %d %q", rec.Code, rec.Body)
	}
	for _, p := range []string{
		"/_console/static/%2e%2e/secret.go", "/_console/static/%2e%2e/templates/layout.html",
		"/_console/static/%2e%2e/", "/_console/static/..%2fsecret.go", "/_console/static/",
	} {
		if rec := req(t, c, "GET", p, nil); rec.Code == 200 {
			t.Errorf("GET %s = 200: %.60q", p, rec.Body.String())
		}
	}
}

// ---- the browser side ----

// Producing to a partition other than the one on screen leaves the view where it
// was and says where the record went. It used to switch the tape to the other
// partition while the address bar and the composer still named the first.
func TestProduceToAnotherPartitionKeepsTheView(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"3"}})
	rec := req(t, c, "POST", "/_console/topics/t/produce", url.Values{"partition": {"2"}, "partition_ctx": {"0"}, "value": {"x"}})
	body := rec.Body.String()
	if rec.Code != 200 || !strings.Contains(body, "Messages · partition 0") || !strings.Contains(body, `partition=0"`) {
		t.Errorf("after producing to partition 2 while viewing 0, the view is not partition 0: %d", rec.Code)
	}
	if tr := rec.Header().Get("HX-Trigger"); !strings.Contains(tr, "partition 2") || !strings.Contains(tr, "viewing partition 0") {
		t.Errorf("the toast should say where the record went and what is on screen: %s", tr)
	}
	// Viewing the partition produced to, there is nothing extra to say.
	same := req(t, c, "POST", "/_console/topics/t/produce", url.Values{"partition": {"1"}, "partition_ctx": {"1"}, "value": {"x"}})
	if tr := same.Header().Get("HX-Trigger"); strings.Contains(tr, "viewing") {
		t.Errorf("toast for the partition on screen: %s", tr)
	}
}

// The fonts are part of the binary and are served as fonts: the pages carry
// "nosniff", so a browser will not use one whose type it has to guess.
func TestFontsAreVendoredAndServedAsFonts(t *testing.T) {
	c := newTestConsole(t)
	css := readAll(t, "static/app.css")
	urls := regexp.MustCompile(`url\(([^)]+\.woff2)\)`).FindAllStringSubmatch(css, -1)
	if len(urls) != 4 {
		t.Fatalf("app.css names %d font files, want 4 (Plex Sans variable; Plex Mono 400, 500, 600)", len(urls))
	}
	versions := readAll(t, "static/VERSIONS")
	for _, m := range urls {
		rec := req(t, c, "GET", "/_console/static/"+m[1], nil)
		if rec.Code != 200 || rec.Header().Get("Content-Type") != "font/woff2" || rec.Body.Len() < 10_000 {
			t.Errorf("%s: %d %q, %d bytes", m[1], rec.Code, rec.Header().Get("Content-Type"), rec.Body.Len())
		}
		if !strings.Contains(versions, filepath.Base(m[1])) {
			t.Errorf("%s is not recorded in static/VERSIONS", m[1])
		}
	}
	if strings.Contains(css, "fonts.googleapis") || strings.Contains(css, "@import") {
		t.Error("the stylesheet must not fetch anything from another origin")
	}
	if _, err := os.Stat("static/fonts/OFL.txt"); err != nil {
		t.Error("the fonts' licence (SIL OFL) must ship beside them")
	}
}

// Each theme is written once. "System" is resolved to light or dark before first
// paint by the script in <head>; a second copy of a theme under a media query is
// how the two light themes drifted apart before.
func TestEachThemeIsDefinedOnce(t *testing.T) {
	css := readAll(t, "static/app.css")
	if strings.Contains(css, "prefers-color-scheme") {
		t.Error("app.css has a prefers-color-scheme block: a theme is being defined twice")
	}
	if n := strings.Count(css, `:root[data-theme="dark"] {`); n != 1 {
		t.Errorf("the dark theme is defined %d times, want once", n)
	}
	c := newTestConsole(t)
	page := req(t, c, "GET", "/_console/topics", nil).Body.String()
	head := page[:strings.Index(page, "</head>")]
	if !strings.Contains(head, `prefers-color-scheme: dark`) || !strings.Contains(head, `setAttribute("data-theme"`) {
		t.Error("the page must choose its theme in <head>, before first paint")
	}
}

// themeTokens reads the custom properties of one rule in app.css.
func themeTokens(t *testing.T, css, selector string) map[string]string {
	t.Helper()
	i := strings.Index(css, selector+" {")
	if i < 0 {
		t.Fatalf("app.css has no %q rule", selector)
	}
	block := css[i : i+strings.Index(css[i:], "\n}")]
	out := map[string]string{}
	for _, m := range regexp.MustCompile(`(--[a-z-]+):\s*(#[0-9a-fA-F]{6})\b`).FindAllStringSubmatch(block, -1) {
		out[m[1]] = m[2]
	}
	return out
}

func luminance(hex string) float64 {
	var r, g, b int
	fmt.Sscanf(hex, "#%02x%02x%02x", &r, &g, &b)
	f := func(v int) float64 {
		c := float64(v) / 255
		if c <= 0.03928 {
			return c / 12.92
		}
		return math.Pow((c+0.055)/1.055, 2.4)
	}
	return 0.2126*f(r) + 0.7152*f(g) + 0.0722*f(b)
}

func contrast(a, b string) float64 {
	la, lb := luminance(a), luminance(b)
	if la < lb {
		la, lb = lb, la
	}
	return (la + 0.05) / (lb + 0.05)
}

// Text holds 4.5:1 on every surface it is set on, and the edge of a control holds
// 3:1, in both themes. A palette is easy to make pretty and unreadable; the last
// one had status colours at 3.6:1.
func TestThemeContrast(t *testing.T) {
	css := readAll(t, "static/app.css")
	light := themeTokens(t, css, ":root")
	dark := themeTokens(t, css, `:root[data-theme="dark"]`)
	for k, v := range light { // dark overrides; what it does not name is shared.
		if _, ok := dark[k]; !ok {
			dark[k] = v
		}
	}
	surfaces := []string{"--background", "--card", "--popover", "--muted", "--sidebar"}
	text := []string{"--foreground", "--muted-foreground", "--ok-foreground", "--bad-foreground", "--warn-foreground", "--info-foreground", "--violet-foreground"}
	for name, th := range map[string]map[string]string{"light": light, "dark": dark} {
		check := func(fg, bg string, min float64) {
			if th[fg] == "" || th[bg] == "" {
				t.Errorf("%s: %s or %s is not defined", name, fg, bg)
				return
			}
			if r := contrast(th[fg], th[bg]); r < min {
				t.Errorf("%s: %s (%s) on %s (%s) is %.2f:1, want %.1f:1", name, fg, th[fg], bg, th[bg], r, min)
			}
		}
		for _, bg := range surfaces {
			for _, fg := range text {
				check(fg, bg, 4.5)
			}
			check("--input", bg, 3)
		}
		check("--foreground", "--accent", 4.5)
		check("--muted-foreground", "--accent", 4.5)
		check("--brand-foreground", "--brand", 4.5)
		check("--destructive-foreground", "--destructive", 4.5)
		check("--ring", "--background", 3)
	}
}

// ---- changing a group's offsets, one partition at a time ----

func committed(t *testing.T, c *Console, group string) map[string]int64 {
	t.Helper()
	offs, err := c.be.GroupOffsets(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, o := range offs {
		out[fmt.Sprintf("%s/%d", o.Topic, o.Partition)] = o.Committed
	}
	return out
}

func TestOneOffsetCanBeSetOrRemoved(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "a", 2, 10) // partition 0 holds offsets 0–9.
	topicWith(t, c, "b", 1, 5)
	commitFor(t, addr, "g", "a", map[int32]int64{0: 4, 1: 0})
	commitFor(t, addr, "g", "b", map[int32]int64{0: 2})
	post := func(path string, form url.Values) *httptest.ResponseRecorder {
		return req(t, c, "POST", "/_console/groups/g"+path, form)
	}

	// The page offers it, per row, and names the range.
	page := req(t, c, "GET", "/_console/groups/g", nil).Body.String()
	for _, want := range []string{`hx-post="/_console/groups/g/offset"`, `hx-post="/_console/groups/g/offset/delete"`, `aria-label="New offset, from 0 to 10"`, `<option value="b">b</option>`} {
		if !strings.Contains(page, want) {
			t.Errorf("the group page is missing %q", want)
		}
	}

	// A specific offset.
	if rec := post("/offset", url.Values{"topic": {"a"}, "partition": {"0"}, "offset": {"7"}}); rec.Code != 200 {
		t.Fatalf("set: %d %s", rec.Code, rec.Body)
	}
	if got := committed(t, c, "g"); got["a/0"] != 7 || got["a/1"] != 0 || got["b/0"] != 2 {
		t.Errorf("after setting a/0 to 7: %v", got)
	}
	// The end of the log is a position (nothing left to read); past it is not.
	if rec := post("/offset", url.Values{"topic": {"a"}, "partition": {"0"}, "offset": {"10"}}); rec.Code != 200 {
		t.Errorf("setting the offset to the log end: %d %s", rec.Code, rec.Body)
	}
	for _, bad := range []string{"11", "-1", "abc", "", "7.5"} {
		rec := post("/offset", url.Values{"topic": {"a"}, "partition": {"0"}, "offset": {bad}})
		if rec.Code != 400 {
			t.Errorf("offset %q: %d, want 400", bad, rec.Code)
		}
	}
	if rec := post("/offset", url.Values{"topic": {"a"}, "partition": {"9"}, "offset": {"1"}}); rec.Code != 400 {
		t.Errorf("a partition the topic does not have: %d", rec.Code)
	}
	if got := committed(t, c, "g"); got["a/0"] != 10 {
		t.Errorf("a refused change moved the offset: %v", got)
	}
	// By name.
	post("/offset", url.Values{"topic": {"a"}, "partition": {"0"}, "to": {"earliest"}})
	if got := committed(t, c, "g"); got["a/0"] != 0 {
		t.Errorf("to earliest: %v", got)
	}

	// Reset only one topic.
	if rec := post("/reset", url.Values{"to": {"latest"}, "topic": {"b"}}); rec.Code != 200 || !strings.Contains(rec.Header().Get("HX-Trigger"), "on b") {
		t.Fatalf("reset of one topic: %d %s", rec.Code, rec.Header().Get("HX-Trigger"))
	}
	if got := committed(t, c, "g"); got["b/0"] != 5 || got["a/0"] != 0 || got["a/1"] != 0 {
		t.Errorf("resetting topic b to latest touched other topics: %v", got)
	}
	if rec := post("/reset", url.Values{"to": {"latest"}, "topic": {"nope"}}); rec.Code != 400 {
		t.Errorf("reset of a topic the group has no offsets on: %d", rec.Code)
	}

	// Remove one; the others stay.
	if rec := post("/offset/delete", url.Values{"topic": {"a"}, "partition": {"1"}}); rec.Code != 200 {
		t.Fatalf("remove: %d %s", rec.Code, rec.Body)
	}
	if got := committed(t, c, "g"); len(got) != 2 || got["b/0"] != 5 {
		t.Errorf("after removing a/1: %v", got)
	}
	// Removing the last one leaves no group to show: back to the list.
	post("/offset/delete", url.Values{"topic": {"a"}, "partition": {"0"}})
	if rec := post("/offset/delete", url.Values{"topic": {"b"}, "partition": {"0"}}); rec.Code != http.StatusSeeOther {
		t.Errorf("removing a group's last offset: %d, want a redirect to the list", rec.Code)
	}
}

// A group with members commits its own offsets; changing them under it is refused.
func TestOffsetsOfARunningGroupAreNotChanged(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "t", 1, 5)
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup("live"), kgo.ConsumeTopics("t"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.AutoCommitInterval(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	cl.PollRecords(ctx, 5)
	cancel()
	cl.CommitUncommittedOffsets(context.Background())

	before := committed(t, c, "live")
	for path, form := range map[string]url.Values{
		"/offset":        {"topic": {"t"}, "partition": {"0"}, "offset": {"1"}},
		"/offset/delete": {"topic": {"t"}, "partition": {"0"}},
	} {
		rec := req(t, c, "POST", "/_console/groups/live"+path, form)
		if rec.Code != 400 || !strings.Contains(rec.Body.String(), "active member") {
			t.Errorf("%s on a running group: %d %s", path, rec.Code, rec.Body)
		}
	}
	if after := committed(t, c, "live"); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Errorf("a running group's offsets changed: %v -> %v", before, after)
	}
	if page := req(t, c, "GET", "/_console/groups/live", nil).Body.String(); strings.Contains(page, "/offset/delete") {
		t.Error("a running group's page should not offer to change its offsets")
	}
}

// ---- searching records ----

func TestSearchFindsRecordsAndSaysHowFarItLooked(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"s"}, "partitions": {"2"}})
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	p := func(part int32, key, value string, h ...kgo.RecordHeader) {
		if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: "s", Partition: part, Key: []byte(key), Value: []byte(value), Headers: h}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 30; i++ {
		p(0, fmt.Sprintf("k%d", i), fmt.Sprintf(`{"n":%d}`, i))
	}
	p(0, "needle-in-key", "x")                                                          // offset 30
	p(0, "k", `{"note":"a NEEDLE in a value"}`)                                         // 31
	p(0, "k", "x", kgo.RecordHeader{Key: "trace", Value: []byte("needle in a header")}) // 32
	p(0, "k", string([]byte{0xff, 0x00, 'n', 'e', 'e', 'd', 'l', 'e', 0xfe}))           // 33: binary.
	p(1, "other", "the needle on partition 1")                                          // p1 offset 0
	get := func(q string) string { return req(t, c, "GET", "/_console/topics/s?"+q, nil).Body.String() }

	one := get("partition=0&q=Needle")
	for _, id := range []string{"hit-0-30", "hit-0-31", "hit-0-32", "hit-0-33"} {
		if !strings.Contains(one, `id="`+id+`"`) {
			t.Errorf("searching partition 0 for Needle did not find %s", id)
		}
	}
	if strings.Contains(one, `id="hit-1-0"`) || strings.Contains(one, `id="hit-0-5"`) {
		t.Error("the search returned a record from another partition, or one that does not match")
	}
	if !strings.Contains(one, "Read offsets 0–33 of partition 0 (34 records): 4 matches.") {
		t.Error("the search should say exactly what it read and how many matched")
	}
	if strings.Contains(one, "data-live=") || strings.Contains(one, `class="tape-strip"`) {
		t.Error("search results are not the live tape")
	}
	// Rows open the record on the partition they are from.
	if !strings.Contains(one, `hx-get="/_console/topics/s/message?partition=0&offset=31"`) {
		t.Error("a result row should open its own record")
	}

	all := get("partition=0&q=needle&all=1")
	if !strings.Contains(all, `id="hit-1-0"`) || !strings.Contains(all, `id="hit-0-30"`) || !strings.Contains(all, "<th class=\"num\">Partition</th>") {
		t.Error("searching all partitions should find matches on each, and say which")
	}
	if !strings.Contains(all, "across 2 partitions: 5 matches") {
		t.Error("the all-partitions search should report what it read")
	}

	if none := get("partition=0&q=absent-word"); !strings.Contains(none, "0 matches") || !strings.Contains(none, "Nothing in the records that were read contains") {
		t.Error("a search with no matches should say so, and that it is about what was read")
	}
	// No text and "all partitions" is the latest records of every partition together.
	if latest := get("partition=0&all=1"); !strings.Contains(latest, `id="hit-1-0"`) || !strings.Contains(latest, "35 records, newest first") {
		t.Error("all partitions with no search text should show the latest records of each")
	}
	// The ordinary page still offers the search.
	if plain := get("partition=0"); !strings.Contains(plain, `id="find-q"`) || !strings.Contains(plain, "data-live=") {
		t.Error("the topic page should offer search and stay live")
	}
}

// A search reads a bounded number of records and offers to continue further back.
func TestSearchIsBoundedAndCanContinue(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"big"}, "partitions": {"1"}})
	if _, err := c.scanForTest(t, "big", 250); err != nil {
		t.Fatal(err)
	}
	hits, from, to, earliest, err := c.scanPartition(context.Background(), "big", 0, []byte("v24"), -1, 100)
	if err != nil {
		t.Fatal(err)
	}
	// The newest 100 records are offsets 150–249; "v24" matches v24 and v240–v249.
	if from != 150 || to != 250 || earliest != 0 || len(hits) != 10 || hits[0].Offset != 249 {
		t.Errorf("scan read [%d,%d) of a log starting at %d and found %d (newest %d); want [150,250), 10, 249", from, to, earliest, len(hits), hits[0].Offset)
	}
	// Continuing from where it stopped reaches the older match.
	hits, from, to, _, err = c.scanPartition(context.Background(), "big", 0, []byte("v24"), 150, 1000)
	if err != nil || from != 0 || to != 150 || len(hits) != 1 || hits[0].Offset != 24 {
		t.Errorf("continued scan read [%d,%d) and found %d: %v", from, to, len(hits), err)
	}
}

// scanForTest fills a topic with n records "v0".."v<n-1>" quickly.
func (c *Console) scanForTest(t *testing.T, topic string, n int) (int, error) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := c.be.Produce(context.Background(), topic, 0, nil, []byte("v"+itoa(i)), nil, "1"); err != nil {
			return i, err
		}
	}
	return n, nil
}

// ---- moving between topics ----

// A click in the list replaces the detail pane only, and nothing on the page
// arrives after it. Replacing the whole workspace redrew the list under the
// pointer and put its scroll position back to the top.
func TestListLinksReplaceOnlyTheDetailPane(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "t", 1, 1)
	commitFor(t, addr, "g", "t", map[int32]int64{0: 1})
	for _, path := range []string{"/_console/topics", "/_console/topics/t", "/_console/groups", "/_console/groups/g", "/_console/cluster", "/_console/access", "/_console/new/topic", "/_console/topics/missing"} {
		page := req(t, c, "GET", path, nil).Body.String()
		if n := strings.Count(page, `id="detail"`); n != 1 {
			t.Errorf("%s has %d elements with id=\"detail\", want 1", path, n)
		}
		for _, m := range regexp.MustCompile(`<a class="li[^>]*>`).FindAllString(page, -1) {
			if !strings.Contains(m, `hx-target="#detail"`) || !strings.Contains(m, `hx-select="#detail"`) {
				t.Errorf("%s: a list link replaces more than the detail pane: %s", path, m)
			}
		}
	}
	// The composer's state is not remembered across pages.
	if js := readAll(t, "static/shell.js"); strings.Contains(js, `store("dock"`) || !strings.Contains(js, "setDock(false);") {
		t.Error("the composer should close when you move to another page, and not be restored from storage")
	}
}

// ---- a topic's own retention and segment size ----

func TestTopicSettingsAreTypedAndShownAsPeopleSayThem(t *testing.T) {
	for _, tc := range []struct {
		parse    func(string) (string, error)
		in, want string
		show     func(string) string
		shown    string
	}{
		{parseDuration, "7d", "604800000", showDuration, "7d"},
		{parseDuration, " 12 H ", "43200000", showDuration, "12h"},
		{parseDuration, "90s", "90000", showDuration, "90s"},
		{parseDuration, "1500", "1500", showDuration, "1500ms"},
		{parseDuration, "forever", "-1", showDuration, "forever"},
		{parseDuration, "0", "0", showDuration, "0ms"},
		{parseSize, "1GB", "1073741824", showSize, "1 GB"},
		{parseSize, "500 mb", "524288000", showSize, "500 MB"},
		{parseSize, "4096", "4096", showSize, "4 KB"},
		{parseSize, "unlimited", "-1", showSize, "unlimited"},
		{parseSegment, "64MB", "67108864", showSize, "64 MB"},
	} {
		got, err := tc.parse(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parse(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
			continue
		}
		if s := tc.show(got); s != tc.shown {
			t.Errorf("show(%q) = %q, want %q", got, s, tc.shown)
		}
		// What is shown can be typed back in and means the same thing.
		if again, err := tc.parse(tc.shown); err != nil || again != tc.want {
			t.Errorf("parse(show) of %q = %q, %v; want %q", tc.in, again, err, tc.want)
		}
	}
	for _, bad := range []string{"soon", "7 weeks", "-5", "1.5h", "d", "99999999999999999999d", "12h30m"} {
		if got, err := parseDuration(bad); err == nil {
			t.Errorf("parseDuration(%q) = %q, want an error", bad, got)
		}
	}
	if got, err := parseSegment("unlimited"); err == nil {
		t.Errorf("a segment size of %q should not be accepted (got %q)", "unlimited", got)
	}
}

func TestATopicCanBeCreatedWithItsOwnRetentionAndChangedLater(t *testing.T) {
	c := newTestConsole(t)
	form := req(t, c, "GET", "/_console/new/topic", nil).Body.String()
	for _, name := range []string{`name="retention.ms"`, `name="retention.bytes"`, `name="segment.bytes"`} {
		if !strings.Contains(form, name) {
			t.Errorf("the create form has no field %s", name)
		}
	}

	rec := req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"t"}, "partitions": {"2"}, "retention.ms": {"7d"}, "segment.bytes": {"64MB"}, "retention.bytes": {""}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	own, err := c.be.topicOverrides(context.Background(), "t")
	if err != nil || len(own) != 2 || own["retention.ms"] != "604800000" || own["segment.bytes"] != "67108864" {
		t.Fatalf("the topic's own config after creating it: %v %v", own, err)
	}
	page := req(t, c, "GET", "/_console/topics/t", nil).Body.String()
	for _, want := range []string{`name="retention.ms" value="7d"`, `name="segment.bytes" value="64MB"`, `name="retention.bytes" value=""`, "unlimited (broker default)", "this topic"} {
		if !strings.Contains(page, want) {
			t.Errorf("the topic page is missing %q", want)
		}
	}

	// Change one, set one, clear one.
	rec = req(t, c, "POST", "/_console/topics/t/config", url.Values{"retention.ms": {"12h"}, "retention.bytes": {"1GB"}, "segment.bytes": {""}})
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `value="12h"`) || !strings.Contains(rec.Body.String(), "<details") {
		t.Fatalf("save: %d %.200s", rec.Code, rec.Body)
	}
	own, _ = c.be.topicOverrides(context.Background(), "t")
	if len(own) != 2 || own["retention.ms"] != "43200000" || own["retention.bytes"] != "1073741824" {
		t.Errorf("the topic's own config after the change: %v", own)
	}

	// The two that have no broker-wide counterpart: a size and a choice.
	rec = req(t, c, "POST", "/_console/topics/t/config", url.Values{"retention.ms": {"12h"}, "retention.bytes": {"1GB"}, "max.message.bytes": {"1MB"}, "message.timestamp.type": {"LogAppendTime"}})
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `<option value="LogAppendTime" selected>`) || !strings.Contains(rec.Body.String(), "largest batch") {
		t.Fatalf("save of the batch limit and timestamp type: %d %.300s", rec.Code, rec.Body)
	}
	if got, _ := c.be.topicOverrides(context.Background(), "t"); got["max.message.bytes"] != "1048576" || got["message.timestamp.type"] != "LogAppendTime" {
		t.Errorf("after setting them: %v", got)
	}
	if rec := req(t, c, "POST", "/_console/topics/t/config", url.Values{"message.timestamp.type": {"Sometime"}}); rec.Code != 400 {
		t.Errorf("a timestamp type that does not exist: %d", rec.Code)
	}
	req(t, c, "POST", "/_console/topics/t/config", url.Values{"retention.ms": {"12h"}, "retention.bytes": {"1GB"}})
	own, _ = c.be.topicOverrides(context.Background(), "t")
	if len(own) != 2 {
		t.Fatalf("clearing the two again left %v", own)
	}

	// Refusals: what was typed, and what the broker will not have.
	for _, bad := range []url.Values{
		{"retention.ms": {"soon"}},
		{"segment.bytes": {"10"}}, // below the broker's minimum: its own message is shown.
	} {
		rec := req(t, c, "POST", "/_console/topics/t/config", bad)
		if rec.Code != 400 || len(rec.Body.String()) < 20 {
			t.Errorf("%v: %d %q", bad, rec.Code, rec.Body)
		}
	}
	if rec := req(t, c, "POST", "/_console/topics/t/config", url.Values{"segment.bytes": {"10"}}); !strings.Contains(rec.Body.String(), "minimum of 1024") {
		t.Errorf("the broker's reason should be shown: %q", rec.Body)
	}
	if rec := req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"u"}, "partitions": {"1"}, "retention.bytes": {"lots"}}); rec.Code != 400 {
		t.Errorf("creating a topic with a setting that cannot be read: %d", rec.Code)
	}
	if after, _ := c.be.topicOverrides(context.Background(), "t"); fmt.Sprint(after) != fmt.Sprint(own) {
		t.Errorf("a refused change altered the topic: %v", after)
	}
	if rec := req(t, c, "POST", "/_console/topics/missing/config", url.Values{"retention.ms": {"1d"}}); rec.Code != 404 {
		t.Errorf("settings of a topic that does not exist: %d", rec.Code)
	}
}

// A compacted topic is said to be compacted, and to leave gaps in its offsets.
func TestACompactedTopicIsShownAsCompacted(t *testing.T) {
	c := newTestConsole(t)
	if err := c.be.CreateTopic(context.Background(), "c", 1, map[string]string{"cleanup.policy": "compact"}); err != nil {
		t.Fatal(err)
	}
	page := req(t, c, "GET", "/_console/topics/c", nil).Body.String()
	if !strings.Contains(page, `<span class="own">compacted</span>`) || !strings.Contains(page, "This topic is compacted") || strings.Contains(page, "not applied") || strings.Contains(page, "does not compact") {
		t.Error("a compacted topic's page should say it is compacted")
	}
	if plain := req(t, c, "GET", "/_console/new/topic", nil).Body.String(); strings.Contains(plain, "This topic is compacted") {
		t.Error("the note should appear only where compaction was asked for")
	}
}
