package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/server"
	"github.com/twmb/franz-go/pkg/kgo"
)

// newTestConsole boots a real broker (server.New, same as cmd/doze-kafka)
// and a console dialed at it — a genuine Kafka client talking real wire
// protocol, exactly like production.
func newTestConsole(t testing.TB) *Console {
	t.Helper()
	c, _ := newTestConsoleAddr(t)
	return c
}

// newTestConsoleAddr is newTestConsole that also returns the broker's address,
// for tests that need to act as another Kafka client (join a group, say).
func newTestConsoleAddr(t testing.TB) (*Console, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("boots a real broker; omit -short (or run `go tool task test:integration`)")
	}
	srv, err := server.New(server.Options{
		DataDir: t.TempDir(), ListenAddr: "127.0.0.1:0", AdvertisedHost: "127.0.0.1",
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Bind synchronously before returning: the console's client dials
	// immediately (see api tests below), and Serve's implicit bind-if-needed
	// would otherwise race the first request against actual listener setup.
	bound, err := srv.Bind(ctx)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	addr := bound.String()
	done := make(chan struct{})
	go func() { defer close(done); srv.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	con, err := New(Options{Addr: addr})
	if err != nil {
		t.Fatalf("console.New: %v", err)
	}
	t.Cleanup(con.Close)
	return con, addr
}

func req(t *testing.T, c *Console, method, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if form != nil {
		r = httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	r.Host = "127.0.0.1:9093" // httptest defaults to example.com, which the Host guard refuses.
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, r)
	return rec
}

func TestTopicLifecycle(t *testing.T) {
	c := newTestConsole(t)

	// Empty home renders.
	if rec := req(t, c, "GET", "/_console/", nil); rec.Code != 200 {
		t.Fatalf("home: status %d: %s", rec.Code, rec.Body)
	}

	// Create.
	rec := req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"orders.created"}, "partitions": {"3"}})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("create: status %d: %s", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); !strings.HasPrefix(loc, "/_console/topics/orders.created") {
		t.Errorf("create redirect = %q", loc)
	}

	// Detail shows identity + effective config, no messages yet.
	rec = req(t, c, "GET", "/_console/topics/orders.created", nil)
	if rec.Code != 200 {
		t.Fatalf("detail: status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{"orders.created", "3 partition", "Effective config", "No messages on this partition yet"} {
		if !strings.Contains(body, want) {
			t.Errorf("detail missing %q", want)
		}
	}

	// Delete.
	rec = req(t, c, "POST", "/_console/topics/orders.created/delete", url.Values{})
	loc := rec.Header().Get("Location")
	if rec.Code != http.StatusSeeOther || (loc != "/_console/topics" && !strings.HasPrefix(loc, "/_console/topics?")) {
		t.Fatalf("delete: status %d location %q", rec.Code, loc)
	}
	rec = req(t, c, "GET", "/_console/topics/orders.created", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("detail after delete: status %d, want 404 (topic not found)", rec.Code)
	}
}

func TestProduceAndTape(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"demo"}, "partitions": {"1"}})

	rec := req(t, c, "POST", "/_console/topics/demo/produce", url.Values{
		"partition": {"0"}, "key": {"k1"}, "value": {`{"a":1}`},
		"headers": {`[{"K":"trace","V":"tr-1"}]`}, "acks": {"1"},
	})
	if rec.Code != 200 {
		t.Fatalf("produce: status %d: %s", rec.Code, rec.Body)
	}
	if trig := rec.Header().Get("Hx-Trigger"); !strings.Contains(trig, "offset 0") {
		t.Errorf("produce toast = %q, want it to mention offset 0", trig)
	}
	body := rec.Body.String()
	// The value renders JSON-tinted: key and number tokens each wrapped in a
	// tint span, with the raw text HTML-escaped inside.
	for _, want := range []string{"msg-0", "k1", `<span class="jt-k">&#34;a&#34;</span>`, `<span class="jt-n">1</span>`, "hdrct"} {
		if !strings.Contains(body, want) {
			t.Errorf("tape/table response missing %q:\n%s", want, body)
		}
	}

	// acks=0 gets honest "no confirmation" messaging, not a fabricated offset.
	rec = req(t, c, "POST", "/_console/topics/demo/produce", url.Values{
		"partition": {"0"}, "value": {"fire and forget"}, "acks": {"0"},
	})
	if trig := rec.Header().Get("Hx-Trigger"); !strings.Contains(trig, "no confirmation requested") {
		t.Errorf("acks=0 toast = %q", trig)
	}

	// The drawer shows the full record, including headers.
	rec = req(t, c, "GET", "/_console/topics/demo/message?partition=0&offset=0", nil)
	if rec.Code != 200 {
		t.Fatalf("drawer: status %d: %s", rec.Code, rec.Body)
	}
	if b := rec.Body.String(); !strings.Contains(b, "trace") || !strings.Contains(b, "tr-1") {
		t.Errorf("drawer missing header content:\n%s", b)
	}

	// Live-poll: stale hash returns fresh content, current hash returns 204.
	rec = req(t, c, "GET", "/_console/topics/demo/tape?partition=0&h=bogus", nil)
	if rec.Code != 200 {
		t.Fatalf("tape stale-hash: status %d", rec.Code)
	}
	hash := rec.Header().Get("HX-Live-Hash")
	if hash == "" {
		t.Fatal("tape response missing HX-Live-Hash")
	}
	rec = req(t, c, "GET", "/_console/topics/demo/tape?partition=0&h="+hash, nil)
	if rec.Code != http.StatusNoContent {
		t.Errorf("tape current-hash: status %d, want 204", rec.Code)
	}
}

func TestPurgeTopic(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"junk"}, "partitions": {"2"}})
	req(t, c, "POST", "/_console/topics/junk/produce", url.Values{"partition": {"0"}, "value": {"m1"}, "acks": {"1"}})
	req(t, c, "POST", "/_console/topics/junk/produce", url.Values{"partition": {"1"}, "value": {"m2"}, "acks": {"1"}})

	rec := req(t, c, "POST", "/_console/topics/junk/purge", url.Values{})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("purge: status %d: %s", rec.Code, rec.Body)
	}

	// Same name, same partition count, zero messages.
	rec = req(t, c, "GET", "/_console/topics/junk", nil)
	if rec.Code != 200 {
		t.Fatalf("detail after purge: status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{"2 partitions", "No messages on this partition yet"} {
		if !strings.Contains(body, want) {
			t.Errorf("detail after purge missing %q", want)
		}
	}
}

func TestMessageWindowPaging(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"big"}, "partitions": {"1"}})
	for i := range 70 { // more than one tapeWindow (60)
		req(t, c, "POST", "/_console/topics/big/produce", url.Values{
			"partition": {"0"}, "value": {fmt.Sprintf("m%d", i)}, "acks": {"1"},
		})
	}

	// Tail window: offsets 10–69, live-polling, with an Older link.
	rec := req(t, c, "GET", "/_console/topics/big", nil)
	body := rec.Body.String()
	if !strings.Contains(body, "data-live=") {
		t.Error("tail window should live-poll")
	}
	if !strings.Contains(body, "from=0") || !strings.Contains(body, "Older") {
		t.Error("tail window of 70 messages should link to an older window")
	}

	// Paged-back window: pinned (no live poll), offset 0 visible, Newer link.
	rec = req(t, c, "GET", "/_console/topics/big?partition=0&from=0", nil)
	body = rec.Body.String()
	if strings.Contains(body, "data-live=") {
		t.Error("history window must not live-poll")
	}
	for _, want := range []string{"pinned to history", "Newer", "msg-0", "m0"} {
		if !strings.Contains(body, want) {
			t.Errorf("history window missing %q", want)
		}
	}
}

func TestConsumerGroup(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"events"}, "partitions": {"1"}})
	req(t, c, "POST", "/_console/topics/events/produce", url.Values{"partition": {"0"}, "value": {"hi"}, "acks": {"1"}})

	// A real classic consumer group joins, consumes, commits, leaves —
	// exactly like internal/broker's own sarama_test.go pattern, just with
	// franz-go's classic-protocol default.
	group, err := kgo.NewClient(
		kgo.SeedBrokers(c.be.addr),
		kgo.ConsumerGroup("workers"),
		kgo.ConsumeTopics("events"),
	)
	if err != nil {
		t.Fatalf("consumer group client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fs := group.PollFetches(ctx)
	if err := fs.Err0(); err != nil {
		t.Fatalf("poll fetches: %v", err)
	}
	if fs.NumRecords() != 1 {
		t.Fatalf("consumed %d records, want 1", fs.NumRecords())
	}
	if err := group.CommitUncommittedOffsets(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	group.Close()

	// The group is now Empty (member left) but its committed offset is durable.
	rec := req(t, c, "GET", "/_console/groups", nil)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "workers") {
		t.Fatalf("groups list: status %d body:\n%s", rec.Code, rec.Body)
	}

	rec = req(t, c, "GET", "/_console/groups/workers", nil)
	if rec.Code != 200 {
		t.Fatalf("group detail: status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{"workers", "classic", "No members connected", "events", "lagnum zero"} {
		if !strings.Contains(body, want) {
			t.Errorf("group detail missing %q:\n%s", want, body)
		}
	}
}

func TestClusterInfo(t *testing.T) {
	c := newTestConsole(t)
	rec := req(t, c, "GET", "/_console/cluster", nil)
	if rec.Code != 200 {
		t.Fatalf("cluster: status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{"doze-kafka", "node 1", "Produce", "ApiVersions"} {
		if !strings.Contains(body, want) {
			t.Errorf("cluster page missing %q:\n%s", want, body)
		}
	}
}

func TestAPIResourcesAndCounts(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"a"}, "partitions": {"1"}})

	rec := req(t, c, "GET", "/_console/api/resources", nil)
	var resources []struct {
		S, N, U string
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resources); err != nil {
		t.Fatalf("decode resources: %v", err)
	}
	found := false
	for _, r := range resources {
		if r.S == "topics" && r.N == "a" {
			found = true
		}
	}
	if !found {
		t.Errorf("resources missing topic 'a': %+v", resources)
	}

	rec = req(t, c, "GET", "/_console/api/counts", nil)
	var counts map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &counts); err != nil {
		t.Fatalf("decode counts: %v", err)
	}
	if counts["topics"] != 1 {
		t.Errorf("counts[topics] = %d, want 1", counts["topics"])
	}
}

// TestUnixSocketConsole runs the console over the broker's own unix socket —
// the embedded (doze module) topology. Every path the console reaches the
// broker through must survive a unix Addr: the admin client, produce, and
// crucially the tape's short-lived FetchWindow consumer (a raw unix path once
// leaked into its SeedBrokers and failed host:port parsing).
func TestUnixSocketConsole(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "kcon") // short path: unix sockets cap ~104 bytes
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := dir + "/kafka.sock"

	srv, err := server.New(server.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); srv.ServeListener(ctx, ln) }()
	t.Cleanup(func() { cancel(); <-done })

	c, err := New(Options{Addr: "unix://" + sock, DisplayAddr: "events.demo.doze:9092"})
	if err != nil {
		t.Fatalf("console.New: %v", err)
	}
	t.Cleanup(c.Close)

	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"demo"}, "partitions": {"1"}})
	if rec := req(t, c, "POST", "/_console/topics/demo/produce", url.Values{
		"partition": {"0"}, "key": {"k1"}, "value": {"over-unix"}, "acks": {"1"},
	}); rec.Code != 200 {
		t.Fatalf("produce: status %d: %s", rec.Code, rec.Body)
	}

	// The tape (FetchWindow) must read the record back, not error on the addr.
	rec := req(t, c, "GET", "/_console/topics/demo/tape?partition=0&h=bogus", nil)
	if rec.Code != 200 {
		t.Fatalf("tape: status %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	if strings.Contains(body, "unable to parse port") {
		t.Fatalf("tape leaked the unix addr into a client:\n%s", body)
	}
	if !strings.Contains(body, "over-unix") {
		t.Errorf("tape missing the produced record:\n%s", body)
	}

	// Pages show the client-facing bootstrap, never the socket path.
	rec = req(t, c, "GET", "/_console/", nil)
	if b := rec.Body.String(); !strings.Contains(b, "events.demo.doze:9092") || strings.Contains(b, "unix://") {
		t.Errorf("endpoint chip should show DisplayAddr, got page containing unix:// or missing it")
	}
}
