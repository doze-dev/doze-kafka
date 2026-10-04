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
	drawer := req(t, c, "GET", "/_console/topics/n/message?partition=0&offset=0", nil)
	runtime.ReadMemStats(&after)
	if page.Code != 200 || drawer.Code != 200 {
		t.Fatalf("page %d, drawer %d", page.Code, drawer.Code)
	}
	if mb := (after.TotalAlloc - before.TotalAlloc) >> 20; mb > 64 {
		t.Errorf("rendering a page and a drawer of deeply nested records allocated %d MB", mb)
	}
	if kb := drawer.Body.Len() >> 10; kb > 512 {
		t.Errorf("the drawer for a 6 KB record is %d KB", kb)
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
		t.Error("the drawer should offer both an exact resend and an editable copy")
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
		if got := classify(tc.err).status; got != tc.status {
			t.Errorf("classify(%v) = %d, want %d", tc.err, got, tc.status)
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
	c.ServeHTTP(rec, r)
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
	for _, p := range []string{"/_console/static/", "/_console/static/cm/"} {
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
