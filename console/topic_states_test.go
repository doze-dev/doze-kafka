package console

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type fakeLogs map[string]CleanerState

func (f fakeLogs) Cleaner(topic string, p int32) (CleanerState, bool) {
	s, ok := f[topic+"/"+itoa(int(p))]
	return s, ok
}

func page(t *testing.T, c *Console, path string) string {
	t.Helper()
	rec := req(t, c, "GET", path, nil)
	if rec.Code != 200 {
		t.Fatalf("GET %s: %d %.200s", path, rec.Code, rec.Body)
	}
	return rec.Body.String()
}

func needs(t *testing.T, body string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(body, w) {
			t.Errorf("the page is missing %q", w)
		}
	}
}

// Tp-21: a compacted topic's page carries the cleaner card, made of what the broker
// holds, and says which things it cannot know. A topic that is not compacted has none.
func TestCompactedTopicShowsTheCleaner(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"cmp"}, "partitions": {"2"}, "cleanup.policy": {"compact"}})
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"plain"}, "partitions": {"1"}})

	needs(t, page(t, c, "/_console/topics/cmp"), `id="topic-cleaner"`, "The console cannot see the cleaner here", "gaps are normal here")
	if strings.Contains(page(t, c, "/_console/topics/plain"), `id="topic-cleaner"`) {
		t.Error("a topic that is not compacted has a cleaner card")
	}

	at := time.Now().Add(-38 * time.Second)
	c.logs = fakeLogs{"cmp/0": {Ran: true, LastAt: at, LastRemoved: 1204, TotalRemoved: 9877, Passes: 3, CleanedTo: 4102}, "cmp/1": {Running: true, CleanedTo: 12, TotalRemoved: 3}}
	body := page(t, c, "/_console/topics/cmp")
	needs(t, body, "running", "1,204 in the last pass, 9,880 since start", "p0 4,102 · p1 12", "Counts begin again when the broker restarts", "are not something the broker reports")
	one := page(t, c, "/_console/topics/cmp?partition=0")
	needs(t, one, "partition 0", "idle", "1,204 in the last pass, 9,877 since start")
}

// Tp-22: an open transaction is named where it holds the last stable offset back, and
// the page links to the list of open transactions, saying there is no page for one.
func TestOpenTransactionIsNamedOnTheTape(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "tx", 1, nil); err != nil {
		t.Fatal(err)
	}
	p := txnProducer(t, addr, "holder-1")
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := p.ProduceSync(ctx, kgoRecord("tx", 0, "in flight")).FirstErr(); err != nil {
		t.Fatal(err)
	}
	body := page(t, c, "/_console/topics/tx?partition=0")
	needs(t, body, `id="topic-txnhold"`, "Last stable offset is 0 on p0", "holder-1", "from 0", "read_committed reader of p0 waits there",
		"/cluster?view=transactions", "there is no page for a single transaction", "committed=1")
	needs(t, page(t, c, "/_console/topics/tx?tab=partitions"), `id="held-p0"`, "holder-1", "has one open on p0 from")
}

// Tp-25: a client the broker refused on this topic is named, with the request, and the
// card says what it can: nothing was written, which rule decided or that the ACLs could
// not be read, and a link to the check in Access.
func TestRefusedClientIsNamedOnTheTopic(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"den"}, "partitions": {"1"}})
	c.traffic = &fakeTraffic{events: []TrafficEvent{{ID: "r1", Time: time.Now(), Remote: "127.0.0.1:55127", ClientID: "loyalty-etl", Principal: "User:loyalty",
		API: "Produce", Version: 9, Topics: []string{"den"}, Parts: "p1", Error: "TOPIC_AUTHORIZATION_FAILED", Class: "denied", Count: 1}}}
	body := page(t, c, "/_console/topics/den")
	needs(t, body, `id="topic-denied"`, "TOPIC_AUTHORIZATION_FAILED", "loyalty-etl", "User:loyalty", "Produce v9", "and the broker said no. Nothing was written.",
		"/access/check?", "principal=User%3Aloyalty", "operation=write")
	if !strings.Contains(body, "Which rule decided") && !strings.Contains(body, "could not read the ACLs") {
		t.Error("the card says neither which rule decided nor that it could not tell")
	}
	c.traffic = &fakeTraffic{}
	if strings.Contains(page(t, c, "/_console/topics/den"), `id="topic-denied"`) {
		t.Error("a topic nobody was refused on has a denied card")
	}
}

// Tp-27: a topic with nothing in it says so, shows how to write the first record and how
// to from a client, and the form writes it.
func TestEmptyTopicOffersTheFirstRecord(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"nil1"}, "partitions": {"2"}})
	body := page(t, c, "/_console/topics/nil1")
	needs(t, body, `id="topic-empty"`, "Nothing has been written to this topic yet", "Start offset 0, end offset 0. This is not a filter and nothing was deleted",
		"The first record lands at offset 0", "kafka-console-producer.sh --bootstrap-server", "--topic nil1", `id="first-record"`)
	if rec := req(t, c, "POST", "/_console/topics/nil1/produce", url.Values{"partition": {"0"}, "partition_ctx": {"0"}, "acks": {"1"}, "value": {"first"}}); rec.Code != 200 {
		t.Fatalf("first record: %d %s", rec.Code, rec.Body)
	}
	if strings.Contains(page(t, c, "/_console/topics/nil1"), `id="topic-empty"`) {
		t.Error("a topic with a record still says it is empty")
	}
}

// Tp-28: a search that matches nothing says the topic is not empty, what was read, and
// offers to clear it.
func TestSearchThatMatchesNothingSaysOnlyTheSearchFoundNothing(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"sx"}, "partitions": {"1"}})
	produceTo(t, c, "sx", 0, 3)
	body := page(t, c, "/_console/topics/sx?partition=0&q=HRB-2099")
	needs(t, body, `id="search-empty"`, "Nothing in the records that were read contains", "HRB-2099", "The topic is not empty, and nothing was deleted: only this search matched nothing",
		"matched as text, ignoring case", "Show all records")
}

// Tp-29: a topic that is not there gets the nearest names with their sizes, the count of
// topics, a way to create it, and the clients the broker refused for asking for it.
func TestMissingTopicOffersWhatTheBrokerAndRecorderKnow(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"harbour.checkout.orders"}, "partitions": {"3"}})
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"harbour.checkout.orders.dlq"}, "partitions": {"1"}})
	c.traffic = &fakeTraffic{events: []TrafficEvent{{ID: "r1", Time: time.Now(), ClientID: "checkout-api", Principal: "User:checkout", API: "Produce", Version: 9,
		Topics: []string{"harbour.checkout.invoices"}, Error: "UNKNOWN_TOPIC_OR_PARTITION", Class: "refused", Count: 1}}}
	rec := req(t, c, "GET", "/_console/topics/harbour.checkout.invoices", nil)
	if rec.Code != 404 {
		t.Fatalf("status %d", rec.Code)
	}
	needs(t, rec.Body.String(), "Did you mean", "harbour.checkout.orders", "3 partitions · 0 records", "The broker has 2 topics and none has this name",
		"Create it instead", "/new/topic?name=harbour.checkout.invoices", "Why it happened", "checkout-api", "UNKNOWN_TOPIC_OR_PARTITION")
}

// Tp-30: a log whose start has moved says where, and names the groups whose commit is
// below it with how far behind, and what their next fetch does.
func TestLogStartMovedNamesTheGroupsBehindIt(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "mv", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "mv", 0, 10, vals("x"), time.Time{})
	commitAt(t, addr, "g-old", "mv", 2)
	commitAt(t, addr, "g-new", "mv", 9)
	deleteBefore(t, cl, "mv", 0, 5)
	body := page(t, c, "/_console/topics/mv?partition=0")
	needs(t, body, `id="log-moved"`, "The log on p0 now starts at offset 5", "Offsets 0 to 4 are gone", "g-old", "OFFSET_OUT_OF_RANGE", "Behind start by")
	if strings.Contains(body, `href="/_console/groups/g-new"`) && strings.Contains(body, "id=\"behind-start\"") {
		seg := body[strings.Index(body, `id="behind-start"`):]
		seg = seg[:strings.Index(seg, "</table>")]
		if strings.Contains(seg, "g-new") {
			t.Error("a group committed past the start is listed as behind it")
		}
	}
}

// Tp-26: the paused view asks how many records arrived since the log ends it was showing.
func TestNewRecordsSincePausedAreCounted(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"pz"}, "partitions": {"2"}})
	produceTo(t, c, "pz", 0, 2)
	body := page(t, c, "/_console/topics/pz?partition=0")
	needs(t, body, `id="topic-paused"`, `data-ends="0:2,1:0"`, "Resume live", "The view stays where it was")
	produceTo(t, c, "pz", 0, 3)
	produceTo(t, c, "pz", 1, 1)
	got := page(t, c, "/_console/topics/pz/new?ends=0:2,1:0")
	if !strings.Contains(got, `"n":4`) || !strings.Contains(got, `"0":3`) || !strings.Contains(got, `"1":1`) {
		t.Errorf("counted %s", got)
	}
}

// Tp-17: the form asks the new total; a total at or below the present one is sent as it
// is and the broker's own refusal is shown; a larger one grows the topic.
func TestAddPartitionsByTotal(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"gr"}, "partitions": {"3"}})
	body := page(t, c, "/_console/topics/gr?tab=partitions&add=1&total=6")
	needs(t, body, "Total partitions", `value="6"`, "now 3, so", "p3", "p4", "and", "p5", "are created empty", "Keyed ordering changes", "With <span class=\"m\">6</span>",
		"Partitions can only be added, never removed or merged", "Who picks them up", "Add 3 partitions")
	for _, tc := range []struct{ total, want string }{{"2", "fewer"}, {"3", "already has 3 partitions"}} {
		rec := req(t, c, "POST", "/_console/topics/gr/partitions", url.Values{"total": {tc.total}})
		if rec.Code != 400 && rec.Code != 422 && rec.Code != 409 {
			t.Errorf("total %s: %d", tc.total, rec.Code)
		}
		needs(t, rec.Body.String(), "INVALID_PARTITIONS", tc.want)
	}
	if rec := req(t, c, "POST", "/_console/topics/gr/partitions", url.Values{"total": {"x"}}); rec.Code < 400 {
		t.Errorf("a total that is not a number: %d", rec.Code)
	}
	if rec := req(t, c, "POST", "/_console/topics/gr/partitions", url.Values{"total": {"5"}}); rec.Code >= 400 {
		t.Fatalf("grow: %d %s", rec.Code, rec.Body)
	}
	n, _ := c.be.PartitionCount(context.Background(), "gr")
	if n != 5 {
		t.Errorf("the topic has %d partitions after asking for a total of 5", n)
	}
}

// Tp-18 to Tp-20: every key the broker reports, what acts and what is kept, an edit row
// with Before, After and what it affects, and refusals in the broker's words with what
// the console adds.
func TestConfigTabListsEveryKeyAndEditsOne(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"cf"}, "partitions": {"1"}})
	body := page(t, c, "/_console/topics/cf?tab=config")
	needs(t, body, "all 33", "overrides 0", "compaction 4", `id="cfg-row-retention.ms"`, `id="cfg-row-flush.messages"`, `id="cfg-row-min.insync.replicas"`,
		"stored and returned and change nothing", "it changes nothing here")
	if n := strings.Count(body, `id="cfg-row-`); n != 33 {
		t.Errorf("%d key rows, want 33", n)
	}
	row := body[strings.Index(body, `id="cfg-row-flush.messages"`):]
	row = row[:strings.Index(row, "</div>")]
	if !strings.Contains(row, ">kept<") {
		t.Errorf("a stored key is not marked kept: %.300s", row)
	}
	row = body[strings.Index(body, `id="cfg-row-retention.ms"`):]
	row = row[:strings.Index(row, "</div>")]
	if !strings.Contains(row, ">acts<") {
		t.Errorf("retention.ms is not marked as acting: %.300s", row)
	}

	edit := page(t, c, "/_console/topics/cf?tab=config&edit=retention.ms")
	needs(t, edit, `id="cfg-edit"`, "retention.ms =", "Before", "After", "sends IncrementalAlterConfigs SET", "Type milliseconds or use")
	prev := page(t, c, "/_console/topics/cf/config/retention.ms?value=1d")
	needs(t, prev, "Before", "After", "86400000", "(1d)", "Only closed segments whose newest record is older than 1d are deleted", "OFFSET_OUT_OF_RANGE")
	needs(t, page(t, c, "/_console/topics/cf/config/retention.ms?value=abc"), "not a length of time")

	if rec := req(t, c, "POST", "/_console/topics/cf/config/retention.ms", url.Values{"value": {"1d"}}); rec.Code >= 400 {
		t.Fatalf("save: %d %s", rec.Code, rec.Body)
	}
	after := page(t, c, "/_console/topics/cf?tab=config&show=overrides")
	needs(t, after, "overrides 1", `id="cfg-row-retention.ms"`, "86400000")
	if strings.Contains(after, `id="cfg-row-flush.messages"`) {
		t.Error("the overrides filter shows a key that is not an override")
	}
	if rec := req(t, c, "POST", "/_console/topics/cf/config/retention.ms", url.Values{"reset": {"1"}}); rec.Code >= 400 {
		t.Fatalf("reset: %d", rec.Code)
	}
	needs(t, page(t, c, "/_console/topics/cf?tab=config"), "overrides 0")

	for _, tc := range []struct {
		path, key, val string
		wants          []string
	}{
		{"/_console/topics/cf/config/-", "retention.mss", "1d", []string{"INVALID_CONFIG", "unknown topic config", "Did you mean retention.ms"}},
		{"/_console/topics/cf/config/segment.bytes", "", "512", []string{"INVALID_CONFIG", "Valid range: 1024 or more"}},
		{"/_console/topics/cf/config/min.cleanable.dirty.ratio", "", "2", []string{"INVALID_CONFIG", "Valid range: 0 to 1"}},
	} {
		rec := req(t, c, "POST", tc.path, url.Values{"key": {tc.key}, "value": {tc.val}})
		if rec.Code < 400 {
			t.Errorf("%s=%s: %d, want a refusal", tc.key, tc.val, rec.Code)
		}
		needs(t, rec.Body.String(), tc.wants...)
	}
}

// TOP-3 on the page: the retention sentence says how a segment's age is judged.
func TestRetentionSentenceSaysHowAgeIsJudged(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"rs"}, "partitions": {"1"}})
	needs(t, page(t, c, "/_console/topics/rs/config/retention.ms?value=1h"), "largest timestamp of its records", "judged by when it was written", "every second")
}

// Tp-23: a group with no members and an old commit is named as one that will not move.
func TestReadersNameAnIdleGroupWithLag(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(context.Background(), "rd", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "rd", 0, 8, vals("x"), time.Time{})
	commitAt(t, addr, "g-idle", "rd", 3)
	body := page(t, c, "/_console/topics/rd?tab=readers")
	needs(t, body, "Groups reading this topic", `href="/_console/groups/g-idle"`, "has no members and an old commit, so its lag will not move until someone starts it")
}

func kgoRecord(topic string, partition int32, value string) *kgo.Record {
	return &kgo.Record{Topic: topic, Partition: partition, Value: []byte(value)}
}

// Tp-25: with the ACLs readable, the card names the rule that decided, from the checker.
func TestRefusedClientNamesTheDenyThatDecided(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"harbour.den"}, "partitions": {"1"}})
	if err := c.be.CreateACL(context.Background(), ACLRow{Resource: "topic", Name: "*", Pattern: "literal", Principal: "User:*", Host: "*", Operation: "all", Permission: "allow"}); err != nil {
		t.Skipf("this broker takes no ACLs: %v", err)
	}
	if err := c.be.CreateACL(context.Background(), ACLRow{Resource: "topic", Name: "harbour.", Pattern: "prefixed", Principal: "User:loyalty", Host: "*", Operation: "write", Permission: "deny"}); err != nil {
		t.Skipf("this broker takes no ACLs: %v", err)
	}
	c.traffic = &fakeTraffic{events: []TrafficEvent{{ID: "r1", Time: time.Now(), Remote: "127.0.0.1:55127", ClientID: "loyalty-etl", Principal: "User:loyalty",
		API: "Produce", Version: 9, Topics: []string{"harbour.den"}, Error: "TOPIC_AUTHORIZATION_FAILED", Class: "denied", Count: 1}}}
	body := page(t, c, "/_console/topics/harbour.den")
	needs(t, body, `id="topic-denied-rule"`, "Which rule decided", "deny User:loyalty write topic harbour. (prefixed)", "decided this one")
}

// The empty topic's first-record form replaces the page's own tape-and-table and takes the
// empty-state card away with it. It used to swap a second #tape-and-table in beside the first
// (two regions with one id; the live poll filled only one): found by the e2e spec Tp-27 on
// a loaded runner, about one run in four.
func TestFirstRecordFromTheEmptyCardReplacesTheTapeAndDeletesTheCard(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"nil"}, "partitions": {"1"}})

	empty := page(t, c, "/_console/topics/nil")
	needs(t, empty, `id="topic-empty"`, `id="first-record"`, `hx-target="#tape-and-table"`)
	if strings.Contains(empty, `hx-target="#topic-empty"`) {
		t.Error("the first-record form still targets the card, so the response lands beside the page's own tape")
	}

	send := func(trigger string) string {
		form := url.Values{"partition": {"0"}, "partition_ctx": {"0"}, "acks": {"1"}, "key": {"k1"}, "value": {"hello"}}
		r := httptest.NewRequest("POST", "/_console/topics/nil/produce", strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("HX-Request", "true")
		if trigger != "" {
			r.Header.Set("HX-Trigger", trigger)
		}
		r.Host = "127.0.0.1:9093"
		rec := httptest.NewRecorder()
		serveInto(t, c, rec, r)
		if rec.Code != 200 {
			t.Fatalf("produce: %d %s", rec.Code, rec.Body)
		}
		return rec.Body.String()
	}
	first := send("first-record")
	needs(t, first, `id="tape-and-table"`, `id="topic-empty" hx-swap-oob="delete"`)
	if again := send(""); strings.Contains(again, `hx-swap-oob="delete"`) {
		t.Error("an ordinary produce deletes the empty card too")
	}
}
