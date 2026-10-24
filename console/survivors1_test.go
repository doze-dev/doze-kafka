package console

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestACLResourceWordsAreAllTheTypesTheWireHas(t *testing.T) {
	for _, w := range []string{"topic", "group", "cluster", "transactional id", "delegation token", "user"} {
		code, ok := aclResourceCode(w)
		if !ok || aclWord(code.String()) != w {
			t.Errorf("aclResourceCode(%q) = %v, %v", w, code, ok)
		}
	}
}

// The strip at the foot of the page is the page's own: by its view, else by its section.
func TestShellDataHintsFollowTheView(t *testing.T) {
	c := newTestConsole(t)
	r := httptest.NewRequest("GET", "/_console/topics", nil)
	for _, tc := range []struct {
		data map[string]any
		want string
	}{
		{map[string]any{"View": "topic", "Page": "topics"}, "topic"},
		{map[string]any{"View": "", "Page": "groups"}, "groups"},
		{map[string]any{"Page": "schemas"}, "schemas"},
		{map[string]any{"View": "group", "Page": "groups"}, "group"},
	} {
		c.shellData(r, tc.data)
		if got, want := fmt.Sprint(tc.data["Hints"]), fmt.Sprint(pageHints(tc.want)); got != want {
			t.Errorf("%v: hints of %q are not %q's", tc.data["View"], tc.data["Page"], tc.want)
		}
	}
	d := map[string]any{"View": "topic", "Hints": "mine"}
	c.shellData(r, d)
	if d["Hints"] != "mine" {
		t.Error("a page's own hints were replaced")
	}
	if fmt.Sprint(pageHints("topic")) == fmt.Sprint(pageHints("topics")) {
		t.Fatal("the test cannot tell a topic's hints from the list's")
	}
}

func TestDockCursorIsAtMost64Bytes(t *testing.T) {
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = &fakeTraffic{} })
	for n, want := range map[int]int{1: 200, 64: 200, 65: 400} {
		rec := req(t, c, "GET", "/_console/dock?topic=t&after="+strings.Repeat("a", n), nil)
		if got := rec.Code; got != want && !(want == 200 && got == 204) {
			t.Errorf("a cursor of %d bytes: %d", n, got)
		}
	}
}

// The region a page polls is drawn with the hash of its own content, so the first poll is
// answered "nothing changed".
func TestPolledRegionsCarryTheHashOfTheirContent(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "polled", 1, 2)
	commitFor(t, addr, "g", "polled", map[int32]int64{0: 1})
	hashRe := regexp.MustCompile(`data-hash="([0-9a-f]+)"`)
	for page, live := range map[string]string{"/_console/topics": "/_console/live/topics", "/_console/groups": "/_console/live/groups"} {
		body := req(t, c, "GET", page, nil).Body.String()
		m := hashRe.FindStringSubmatch(body)
		if m == nil {
			t.Fatalf("%s carries no data-hash", page)
		}
		if rec := req(t, c, "GET", live+"?h="+m[1], nil); rec.Code != 204 {
			t.Errorf("%s: the first poll is answered %d, not 204", live, rec.Code)
		}
		if rec := req(t, c, "GET", live+"?h=0000", nil); rec.Code != 200 || rec.Header().Get("HX-Live-Hash") != m[1] {
			t.Errorf("%s: a stale hash is answered %d (hash %q, want %q)", live, rec.Code, rec.Header().Get("HX-Live-Hash"), m[1])
		}
	}
}

func TestConnectFactsDeclareNothingWithoutAKafkaMajor(t *testing.T) {
	c := newTestConsole(t)
	if f := factsOf(t, c, false); f.Compat != "" {
		t.Errorf("compat %q with no Kafka major", f.Compat)
	}
	c.facts.KafkaMajor = 4
	if f := factsOf(t, c, false); f.Compat != "Kafka 4.0 compat" {
		t.Errorf("compat %q", f.Compat)
	}
}

func TestFirstRunPageLinksToTrafficOnlyWithARecorder(t *testing.T) {
	with, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = &fakeTraffic{} })
	without := newTestConsole(t)
	a := readable(req(t, with, "GET", "/_console/", nil).Body.String())
	b := readable(req(t, without, "GET", "/_console/", nil).Body.String())
	if !strings.Contains(a, "Your broker is up") || !strings.Contains(b, "Your broker is up") {
		t.Fatalf("not the first-run page:\n%s", b)
	}
	if !strings.Contains(a, "Open Traffic") || strings.Contains(a, "Open Topics") || !strings.Contains(a, "Every request shows up on Traffic") {
		t.Errorf("with a recorder:\n%s", a)
	}
	if !strings.Contains(b, "Open Topics") || strings.Contains(b, "Open Traffic") || strings.Contains(b, "Every request shows up on Traffic") {
		t.Errorf("without one:\n%s", b)
	}
}

// A subject that was deleted softly leaves its schema readable by id, under no subject.
func TestADecodedValueWhoseSubjectIsGone(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	ctx := context.Background()
	id, err := c.reg.Register(ctx, "orphan-value", schemaBody{Schema: userV1})
	if err != nil {
		t.Fatal(err)
	}
	enc, err := c.reg.encode(ctx, "orphan-value", `{"id": 3}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.reg.call(ctx, "DELETE", "/subjects/orphan-value", nil, nil); err != nil {
		t.Fatal(err)
	}
	d, ok := c.reg.decode(ctx, "orphan", enc)
	if !ok || d.ID != id || d.Subject != "" || d.Version != 0 || !strings.Contains(d.JSON, "3") {
		t.Errorf("%+v %v", d, ok)
	}
}

func TestCatalogueBindsToTheSubjectsTopicOnlyWhenThereIsOne(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	topicWith(t, c, "orders", 1, 0)
	register(t, c, "orders-value", userV1)
	register(t, c, "plain", userV1)
	register(t, c, "-value", userV1)
	register(t, c, "ghost-value", userV1)
	page := req(t, c, "GET", "/_console/schemas", nil).Body.String()
	for _, tc := range []struct {
		subject string
		topic   string
	}{{"orders-value", "orders"}, {"plain", ""}, {"ghost-value", ""}, {"-value", ""}} {
		cell := between(page, `href="/_console/schemas/`+tc.subject+`" data-row>`, `</a>`)
		got := strings.Contains(cell, `data-l="Topics using it"><span class="mut">—</span>`)
		if tc.topic == "" && !got || tc.topic != "" && (got || !strings.Contains(cell, `data-l="Topics using it">`+tc.topic+`<`)) {
			t.Errorf("%s: its topic should be %q:\n%s", tc.subject, tc.topic, cell)
		}
	}
	host := strings.TrimPrefix(c.reg.base, "http://")
	if !strings.Contains(page, "schema.registry.url="+c.reg.base) || !strings.Contains(readable(page), host) {
		t.Errorf("the registry's own address is not told")
	}
	c.facts.RegistryURL = "http://public.example:8081"
	if p := req(t, c, "GET", "/_console/schemas", nil).Body.String(); !strings.Contains(p, "schema.registry.url=http://public.example:8081") || strings.Contains(p, "schema.registry.url="+c.reg.base) {
		t.Error("the address clients are told is not the public one")
	}
	if strings.Contains(readable(page), "SASL is on") {
		t.Error("SASL is on, says a console without it")
	}
	c.facts.SASLMechanisms = []string{"PLAIN"}
	if p := readable(req(t, c, "GET", "/_console/schemas", nil).Body.String()); !strings.Contains(p, "SASL is on") {
		t.Errorf("SASL is on, and the page does not say so")
	}
	c.facts.SASLMechanisms = nil
	c.be.opts.SASLUser = "u"
	if p := readable(req(t, c, "GET", "/_console/schemas", nil).Body.String()); !strings.Contains(p, "SASL is on") {
		t.Errorf("the console signs in, and the page does not say SASL is on")
	}
	c.be.opts.SASLUser = ""
}

func TestGroupGenerationFromTheOnlyEvent(t *testing.T) {
	src := &fakeTraffic{groups: map[string][]TrafficGroupEvent{"solo": {{Seq: 1, Kind: "joined", Generation: 5}}}}
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "ge", 1, 1)
	commitFor(t, addr, "solo", "ge", map[int32]int64{0: 0})
	if g := groupPage(t, c, "solo", "")["Generation"]; g != int32(5) {
		t.Errorf("page: %v", g)
	}
	rows := c.groupRows(context.Background(), []GroupSummary{{ID: "solo"}})
	if rows[0].Generation != 5 {
		t.Errorf("row: %d", rows[0].Generation)
	}
}

func TestLastRecordsOfAPurgedTopicAreNotNewest(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "purged", 2, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "purged", 0, 3, vals("m"), time.Time{})
	if err := c.be.PurgeTopic(ctx, "purged"); err != nil {
		t.Fatal(err)
	}
	bounds, err := c.be.PartitionBounds(ctx, "purged", 2)
	if err != nil {
		t.Fatal(err)
	}
	if f := c.boundsFacts(ctx, "purged", bounds); f.HasNewest || f.Messages != 0 || !f.Newest.IsZero() {
		t.Errorf("a purged topic: %+v", f)
	}
}

func TestProduceResendAndTapeAnswersShowTheWholeSmallLog(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "small", 1, 3)
	rowIDs := func(body string) []string {
		var ids []string
		for _, m := range regexp.MustCompile(`id="row-0-(\d+)"|hit-0-(\d+)|offset=(\d+)"`).FindAllStringSubmatch(body, -1) {
			for _, g := range m[1:] {
				if g != "" {
					ids = append(ids, g)
				}
			}
		}
		return ids
	}
	has := func(body, off string) bool {
		for _, id := range rowIDs(body) {
			if id == off {
				return true
			}
		}
		return false
	}
	produce := req(t, c, "POST", "/_console/topics/small/produce", url.Values{"partition": {"0"}, "value": {"x"}, "acks": {"1"}})
	if produce.Code != 200 || !has(produce.Body.String(), "0") || !has(produce.Body.String(), "3") {
		t.Errorf("produce answers with offsets %v", rowIDs(produce.Body.String()))
	}
	resend := req(t, c, "POST", "/_console/topics/small/resend", url.Values{"partition": {"0"}, "offset": {"1"}})
	if resend.Code != 200 || !has(resend.Body.String(), "0") || !has(resend.Body.String(), "4") {
		t.Errorf("resend answers with offsets %v", rowIDs(resend.Body.String()))
	}
	tape := req(t, c, "GET", "/_console/topics/small/tape", nil)
	if tape.Code != 200 || !has(tape.Body.String(), "0") || !has(tape.Body.String(), "4") {
		t.Errorf("tape answers with offsets %v", rowIDs(tape.Body.String()))
	}
}

func TestJumpMalformedIsMinusTwo(t *testing.T) {
	if jumpMalformed != -2 {
		t.Errorf("jumpMalformed = %d", jumpMalformed)
	}
	for _, q := range []string{"jump=abc", "jump=-5", "jump=12abc"} {
		if got := jumpParam(httptest.NewRequest("GET", "/x?"+q, nil)); got != -2 {
			t.Errorf("%s: %d", q, got)
		}
	}
	c := newTestConsole(t)
	topicWith(t, c, "jmp", 1, 5)
	body := readable(req(t, c, "GET", "/_console/topics/jmp?jump=abc", nil).Body.String())
	if !strings.Contains(body, "That is not an offset.") {
		t.Errorf("a malformed jump:\n%s", body)
	}
}

// A page asked for more of a log than its start: the highlight is dropped, not moved.
func TestJumpBeforeTheLogStartHighlightsNothing(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "hl", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "hl", 0, 10, vals("m"), time.Time{})
	deleteBefore(t, cl, "hl", 0, 4)
	f := tapeFixture{c: c, addr: addr, id: "i"}
	if d := f.tape(t, "hl", 0, 1, -1, 7, 2, false); d["HL"] != int64(-1) || d["WindowFrom"] != int64(4) {
		t.Errorf("hl %v from %v", d["HL"], d["WindowFrom"])
	}
	if d := f.tape(t, "hl", 0, 1, -1, 7, 10, false); d["HL"] != int64(-1) || d["AtTail"] != true {
		t.Errorf("past the end: hl %v", d["HL"])
	}
}

func TestSearchPageKeepsTheFactsSentence(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "found", 2, 3)
	plain := readable(req(t, c, "GET", "/_console/topics/found", nil).Body.String())
	searched := readable(req(t, c, "GET", "/_console/topics/found?q=v1", nil).Body.String())
	for _, want := range []string{"2 partitions", "3 messages"} {
		if !strings.Contains(plain, want) || !strings.Contains(searched, want) {
			t.Errorf("%q: plain %v, searched %v", want, strings.Contains(plain, want), strings.Contains(searched, want))
		}
	}
}

func TestTopicTabsOfferASchemaTabOnlyForATopicWithASubject(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	topicWith(t, c, "with", 1, 0)
	topicWith(t, c, "without", 1, 0)
	register(t, c, "with-value", userV1)
	if body := req(t, c, "GET", "/_console/topics/with", nil).Body.String(); !strings.Contains(body, "?tab=schema") {
		t.Error("no schema tab for a topic with a subject")
	}
	if body := req(t, c, "GET", "/_console/topics/without", nil).Body.String(); strings.Contains(body, "?tab=schema") {
		t.Error("a schema tab for a topic with none")
	}
}

func TestReadersTabShowsWritersOnlyWithARecorder(t *testing.T) {
	src := &fakeTraffic{events: []TrafficEvent{{ID: "p", Kind: "produce", API: "Produce", Topics: []string{"rt"}, ClientID: "the-writer", Count: 2}}}
	with, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	topicWith(t, with, "rt", 1, 1)
	a := readable(req(t, with, "GET", "/_console/topics/rt?tab=readers", nil).Body.String())
	if !strings.Contains(a, "the-writer") {
		t.Errorf("the writer is not named:\n%s", a)
	}
	without := newTestConsole(t)
	topicWith(t, without, "rt", 1, 1)
	if b := readable(req(t, without, "GET", "/_console/topics/rt?tab=readers", nil).Body.String()); strings.Contains(b, "the-writer") {
		t.Errorf("a writer without a recorder:\n%s", b)
	}
}

func TestTopicsTotalsKnowWhenThereIsNoSize(t *testing.T) {
	c := newTestConsole(t)
	if d := topicsHome(t, c); d["HasSize"] != false {
		t.Errorf("no topics: HasSize %v", d["HasSize"])
	}
	topicWith(t, c, "sized", 1, 2)
	if d := topicsHome(t, c); d["HasSize"] != true {
		t.Errorf("a topic with files: HasSize %v", d["HasSize"])
	}
	plain := newTestConsole(t)
	topicWith(t, plain, "unsized", 1, 1)
	plain.facts.DataDir = ""
	if d := topicsHome(t, plain); d["HasSize"] != false {
		t.Errorf("no data directory: HasSize %v", d["HasSize"])
	}
}

func TestAbortedRecordsAreMarkedWhenNothingIsOpen(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "done", 1, nil); err != nil {
		t.Fatal(err)
	}
	p := txnProducer(t, addr, "decided")
	for i, end := range []kgo.TransactionEndTry{kgo.TryCommit, kgo.TryAbort} {
		if err := p.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		txnWrite(t, ctx, p, "done", fmt.Sprint("v", i))
		if err := p.EndTransaction(ctx, end); err != nil {
			t.Fatal(err)
		}
	}
	f := tapeFixture{c: c, addr: addr, id: "i"}
	d := f.tape(t, "done", 0, 1, -1, -1, -1, false)
	var got []string
	for _, m := range d["Messages"].([]msgView) {
		got = append(got, fmt.Sprintf("%d:%s", m.Offset, m.Txn))
	}
	if strings.Join(got, " ") != "0: 2:aborted" {
		t.Errorf("records %v (every transaction has ended)", got)
	}
}
