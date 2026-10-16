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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/server"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
func newTestConsoleAddr(t testing.TB, mod ...func(*Options)) (*Console, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("boots a real broker; omit -short (or run `go tool task test:integration`)")
	}
	// A data directory whose path has a space in it, and a listener that picks its
	// own port: nothing here may depend on either being tidy (plan A8).
	dataDir := filepath.Join(t.TempDir(), "data dir")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Options{
		DataDir: dataDir, ListenAddr: "127.0.0.1:0", AdvertisedHost: "127.0.0.1",
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

	opts := Options{Addr: addr, Facts: BrokerFacts{DataDir: dataDir}}
	for _, m := range mod {
		m(&opts)
	}
	con, err := New(opts)
	if err != nil {
		t.Fatalf("console.New: %v", err)
	}
	t.Cleanup(con.Close)
	return con, addr
}

// testPrefix is where the suite mounts the console when DOZE_KAFKA_CONSOLE_PREFIX
// is set (task test:identity): every console.New reads it. The tests are written against "/_console"; serveInto
// rewrites each request into the prefix and each answer back out of it, and fails
// for an answer that still says "/_console", which is a prefix hard-coded in the
// console.
var testPrefix = os.Getenv("DOZE_KAFKA_CONSOLE_PREFIX")

func serveInto(t testing.TB, c *Console, rec *httptest.ResponseRecorder, r *http.Request) {
	t.Helper()
	if testPrefix == "" {
		c.ServeHTTP(rec, r)
		return
	}
	r.URL.Path = strings.Replace(r.URL.Path, defaultPrefix, testPrefix, 1)
	r.URL.RawPath = strings.Replace(r.URL.RawPath, defaultPrefix, testPrefix, 1)
	r.RequestURI = ""
	if o := r.Header.Get("Referer"); o != "" {
		r.Header.Set("Referer", strings.Replace(o, defaultPrefix, testPrefix, 1))
	}
	tmp := httptest.NewRecorder()
	c.ServeHTTP(tmp, r)
	body := tmp.Body.String()
	if strings.Contains(body, defaultPrefix) {
		t.Errorf("%s answered with %q in it, with the console mounted at %q: the prefix is hard-coded", r.URL.Path, defaultPrefix, testPrefix)
	}
	rec.Code = tmp.Code
	for k, vs := range tmp.Header() {
		for _, v := range vs {
			rec.Header().Add(k, strings.ReplaceAll(v, testPrefix, defaultPrefix))
		}
	}
	rec.Body.WriteString(strings.ReplaceAll(body, testPrefix, defaultPrefix))
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
	serveInto(t, c, rec, r)
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
	for _, want := range []string{"orders.created", "3 partition", "No messages on this partition yet", "Produce a message"} {
		if !strings.Contains(body, want) {
			t.Errorf("detail missing %q", want)
		}
	}
	if conf := req(t, c, "GET", "/_console/topics/orders.created?tab=config", nil).Body.String(); !strings.Contains(conf, "Save settings") {
		t.Error("the Config tab does not offer to save settings")
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
	for _, want := range []string{"msg-0", "k1", `<span class="jt-k">&#34;a&#34;</span>`, `<span class="jt-n">1</span>`, `title="7 B · 1 header"`} {
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

	// The inspector shows the full record, including headers.
	rec = req(t, c, "GET", "/_console/topics/demo/message?partition=0&offset=0", nil)
	if rec.Code != 200 {
		t.Fatalf("inspector: status %d: %s", rec.Code, rec.Body)
	}
	if b := rec.Body.String(); !strings.Contains(b, "trace") || !strings.Contains(b, "tr-1") {
		t.Errorf("inspector missing header content:\n%s", b)
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

	idBefore, _, _ := c.be.TopicShape(context.Background(), "junk")
	rec := req(t, c, "POST", "/_console/topics/junk/purge", url.Values{})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("purge: status %d: %s", rec.Code, rec.Body)
	}

	if idAfter, _, _ := c.be.TopicShape(context.Background(), "junk"); idAfter != idBefore || idBefore == "" {
		t.Errorf("a purge changed the topic's id from %q to %q: it is DeleteRecords now, not a delete and recreate", idBefore, idAfter)
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
	for _, want := range []string{"workers", "classic", "No members", "caught up"} {
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
	body := rec.Body.String() + req(t, c, "GET", "/_console/cluster?view=apis", nil).Body.String()
	for _, want := range []string{"doze-kafka", "node 1", "Produce", "ApiVersions"} {
		if !strings.Contains(body, want) {
			t.Errorf("cluster page missing %q:\n%s", want, body)
		}
	}
}

func TestAPIPaletteAndCounts(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"a"}, "partitions": {"1"}})

	rec := req(t, c, "GET", "/_console/api/palette", nil)
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

// Growing a topic from its page: partitions are added, never removed, and the page
// says what that costs before it is done.
func TestAddPartitions(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"grow"}, "partitions": {"2"}})

	page := req(t, c, "GET", "/_console/topics/grow?tab=partitions", nil).Body.String()
	for _, want := range []string{`id="topic-addparts"`, "/topics/grow/partitions", "cannot be removed", "stop being ordered"} {
		if !strings.Contains(page, want) {
			t.Errorf("the topic page is missing %q", want)
		}
	}

	if rec := req(t, c, "POST", "/_console/topics/grow/partitions", url.Values{"add": {"3"}}); rec.Code != http.StatusSeeOther {
		t.Fatalf("add 3: status %d: %s", rec.Code, rec.Body)
	}
	if body := req(t, c, "GET", "/_console/topics/grow", nil).Body.String(); !strings.Contains(body, "5 partitions") {
		t.Error("the topic does not show 5 partitions after adding 3 to 2")
	}

	for _, bad := range []string{"0", "-1", "", "x"} {
		rec := req(t, c, "POST", "/_console/topics/grow/partitions", url.Values{"add": {bad}})
		if rec.Code < 400 || !strings.Contains(rec.Body.String()+rec.Header().Get("HX-Doze-Error"), "how many partitions to add") {
			t.Errorf("add %q: status %d, want a refusal that says what to enter: %q", bad, rec.Code, rec.Body)
		}
	}
	if rec := req(t, c, "POST", "/_console/topics/nope/partitions", url.Values{"add": {"1"}}); rec.Code < 400 {
		t.Errorf("an unknown topic: status %d", rec.Code)
	}
	if body := req(t, c, "GET", "/_console/topics/grow", nil).Body.String(); !strings.Contains(body, "5 partitions") {
		t.Error("refused requests changed the topic")
	}
}

// The Access page says, before anything else, that what it lists is enforced and how a
// resource with no ACL is treated; it lists what is stored, and says plainly when
// nothing is.
func TestAccessPageListsACLsUnderTheEnforcedBanner(t *testing.T) {
	c := newTestConsole(t)
	empty := req(t, c, "GET", "/_console/access", nil).Body.String()
	for _, want := range []string{"These are enforced", "open to everyone", "User:ANONYMOUS", "None stored"} {
		if !strings.Contains(empty, want) {
			t.Errorf("the empty Access page is missing %q", want)
		}
	}

	creq := kmsg.NewPtrCreateACLsRequest()
	cr := kmsg.NewCreateACLsRequestCreation()
	cr.ResourceType, cr.ResourceName, cr.ResourcePatternType = kmsg.ACLResourceTypeTopic, "orders", kmsg.ACLResourcePatternTypeLiteral
	cr.Principal, cr.Host, cr.Operation, cr.PermissionType = "User:alice", "*", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow
	creq.Creations = append(creq.Creations, cr)
	if resp, err := creq.RequestWith(context.Background(), c.be.cl); err != nil || resp.Results[0].ErrorCode != 0 {
		t.Fatalf("create an ACL: %v %+v", err, resp)
	}
	page := req(t, c, "GET", "/_console/access", nil).Body.String()
	for _, want := range []string{"These are enforced", ">ALLOW<", ">Read<", ">Topic<", "User:alice", "orders", "literal"} {
		if !strings.Contains(page, want) {
			t.Errorf("the Access page is missing %q after one ACL was stored", want)
		}
	}
	if strings.Contains(page, "None stored") {
		t.Error("the page says none are stored while one is listed")
	}
	// The section is reached by name from the rail and by g a; the palette does not list it.
	if pal := req(t, c, "GET", "/_console/api/palette", nil).Body.String(); strings.Contains(pal, `"u":"/_console/access"`) {
		t.Error("the palette lists the Access page as a destination (Sy-05 has no Go to group)")
	}
}

// Cl-Home counts the partitions of the topics whose cleanup.policy compacts them (from
// the topics' own configuration), and says what this broker is not.
func TestClusterOverviewCountsCompactedPartitionsAndSaysWhatTheBrokerIsNot(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "plain", 2, 1)
	if err := c.be.CreateTopic(context.Background(), "latest", 3, map[string]string{"cleanup.policy": "compact"}); err != nil {
		t.Fatal(err)
	}
	body := req(t, c, "GET", "/_console/cluster", nil).Body.String()
	for _, want := range []string{
		`<span>Compacted partitions</span><span class="m ra b">3</span>`,
		`What this broker is not`, `single node`, `replication factor reported as`, `no leader election, no ISR`,
		`no KRaft, no ZooKeeper`, `not durable against disk loss`, `All limits`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("cluster overview missing %q", want)
		}
	}
	// Another view does not read every topic's configuration for a count it does not draw.
	if strings.Contains(req(t, c, "GET", "/_console/cluster?view=apis", nil).Body.String(), "Compacted partitions") {
		t.Error("the APIs view draws the overview's counts")
	}
}
