package console

import (
	"context"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestCreateCommandText(t *testing.T) {
	f := connectFacts{Bootstrap: "127.0.0.1:9092"}
	got := createCommand(f, "", "", nil)
	want := "kafka-topics.sh --bootstrap-server 127.0.0.1:9092 \\\n  --create --topic orders.created \\\n  --partitions 1 --replication-factor 1"
	if got != want {
		t.Errorf("defaults:\n%s\nwant\n%s", got, want)
	}
	f.CommandConfig = "  --command-config client.properties "
	got = createCommand(f, "it's", " 3 ", map[string]string{"retention.ms": "604800000", "message.timestamp.type": "LogAppendTime", "not.a.setting": "x"})
	want = "kafka-topics.sh --bootstrap-server 127.0.0.1:9092 \\\n  --command-config client.properties \\\n  --create --topic 'it'\\''s' \\\n  --partitions 3 --replication-factor 1 \\\n  --config retention.ms=604800000 \\\n  --config message.timestamp.type=LogAppendTime"
	if got != want {
		t.Errorf("full:\n%s\nwant\n%s", got, want)
	}
	for in, n := range map[string]string{"0": "1", "-2": "1", "x": "1", "": "1", "1": "1", "2": "2", "12": "12"} {
		if got := createCommand(f, "t", in, nil); !strings.Contains(got, "--partitions "+n+" ") {
			t.Errorf("partitions %q: %s", in, got)
		}
	}
}

func TestShellQuote(t *testing.T) {
	for _, safe := range []string{"a", "z", "A", "Z", "0", "9", "az09AZ", "a.b_c-d:e/f=g,h+i"} {
		if got := shellQuote(safe); got != safe {
			t.Errorf("shellQuote(%q) = %q, want it unquoted", safe, got)
		}
	}
	for _, unsafe := range []string{"", "`", "{", "@", "[", " ", "a b", "$x", "é", "it's"} {
		if got := shellQuote(unsafe); !strings.HasPrefix(got, "'") || !strings.HasSuffix(got, "'") {
			t.Errorf("shellQuote(%q) = %q, want it quoted", unsafe, got)
		}
	}
	if got := shellQuote("it's"); got != `'it'\''s'` {
		t.Errorf("%q", got)
	}
}

func TestUIText(t *testing.T) {
	if got := uiText("list.nomatch"); got != "No matches for “%s”" {
		t.Errorf("without arguments the text is as written: %q", got)
	}
	if got := uiText("list.nomatch", "abc"); got != "No matches for “abc”" {
		t.Errorf("%q", got)
	}
	if got := uiText("no.such.key"); got != "no.such.key" {
		t.Errorf("%q", got)
	}
}

func TestScaledLimitsAndShownUnits(t *testing.T) {
	if got, err := parseSize("4611686018427387904"); err != nil || got != "4611686018427387904" {
		t.Errorf("the largest size: %q %v", got, err)
	}
	if _, err := parseSize("4611686018427387905"); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Errorf("one past the largest size: %v", err)
	}
	if got, err := parseDuration("4611686018427387s"); err != nil || got != "4611686018427387000" {
		t.Errorf("the largest number of seconds: %q %v", got, err)
	}
	if _, err := parseDuration("4611686018427388s"); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Errorf("one more second: %v", err)
	}
	if got, err := parseDuration("0"); err != nil || got != "0" {
		t.Errorf("zero: %q %v", got, err)
	}
	for in, want := range map[string]string{"0": "0ms", "1": "1ms", "999": "999ms", "1000": "1s", "60000": "1m", "3600000": "1h", "86400000": "1d", "90000": "90s", "-1": "forever", "x": "x"} {
		if got := showDuration(in); got != want {
			t.Errorf("showDuration(%q) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{"1": "1 B", "1024": "1 KB", "1048576": "1 MB", "1073741824": "1 GB", "1536": "1536 B", "-1": "unlimited", "x": "x"} {
		if got := showSize(in); got != want {
			t.Errorf("showSize(%q) = %q, want %q", in, got, want)
		}
	}
	if got := showSize("0"); got != "0 B" {
		t.Errorf("showSize(0) = %q", got)
	}
}

func TestTrimPaddingAndHeaderRows(t *testing.T) {
	for in, want := range map[string]string{"": "", "=": "", "ab": "ab", "ab=": "ab", "ab==": "ab", "a=b=": "a=b"} {
		if got := trimPadding(in); got != want {
			t.Errorf("trimPadding(%q) = %q, want %q", in, got, want)
		}
	}
	form := func(kv ...string) *url.Values {
		v := url.Values{}
		for i := 0; i < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}
		return &v
	}
	post := func(v *url.Values, parts int32) (produceInput, error) {
		r := httptest.NewRequest("POST", "/x", strings.NewReader(v.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return parseProduceForm(r, parts)
	}
	_, err := post(form("headers", `[{"k":"a","v":"1"},{"k":"","v":"x"}]`), 3)
	if err == nil || !strings.Contains(err.Error(), "header 2 has a value but no key") {
		t.Errorf("a header with no key: %v", err)
	}
	_, err = post(form("partition", "3"), 3)
	if err == nil || !strings.Contains(err.Error(), "this topic has 3 (0–2)") {
		t.Errorf("a partition one past the last: %v", err)
	}
	in, err := post(form("partition", "2"), 3)
	if err != nil || in.Partition != 2 || in.Auto {
		t.Errorf("the last partition: %+v %v", in, err)
	}
}

// The neighbours sentence is joined "a, b and c".
func TestNeighbourClausesJoinNames(t *testing.T) {
	c := newTestConsole(t)
	for n, want := range map[int][]string{1: {""}, 2: {"", " and "}, 3: {"", ", ", " and "}, 4: {"", ", ", ", ", " and "}} {
		g := NewGraph()
		for i := 0; i < n; i++ {
			g.Add(Edge{From: Node{NodeClient, string(rune('a' + i))}, To: Node{NodeTopic, "t"}, Kind: EdgeWrites, Source: FromTraffic})
		}
		cls := c.neighbourClauses(g, NodeTopic, "t")
		if len(cls) == 0 {
			t.Fatalf("%d clients: no clause", n)
		}
		var seps []string
		for _, l := range cls[0].Links {
			seps = append(seps, l.Sep)
		}
		if strings.Join(seps, "|") != strings.Join(want, "|") {
			t.Errorf("%d clients: separators %q, want %q", n, seps, want)
		}
	}
}

// Edges come out in a stable order: by the node they leave, then by kind, then by the node they reach.
func TestGraphEdgesAreInAStableOrder(t *testing.T) {
	g := NewGraph()
	from := Node{NodeClient, "c"}
	g.Add(Edge{From: from, To: Node{NodeTopic, "z"}, Kind: EdgeWrites})
	g.Add(Edge{From: from, To: Node{NodeTopic, "a"}, Kind: EdgeWrites})
	g.Add(Edge{From: from, To: Node{NodeTopic, "m"}, Kind: EdgeReads})
	g.Add(Edge{From: Node{NodeClient, "b"}, To: Node{NodeTopic, "q"}, Kind: EdgeWrites})
	var got []string
	for _, e := range g.Edges() {
		got = append(got, e.From.Name+"-"+string(e.Kind)+"-"+e.To.Name)
	}
	if want := "b-writes-q c-reads-m c-writes-a c-writes-z"; strings.Join(got, " ") != want {
		t.Errorf("order %v, want %s", got, want)
	}
}

func TestCloseNamesSuggestions(t *testing.T) {
	c := newTestConsole(t)
	id := func(s string) string { return s }
	got := func(names []string, name string) string {
		var out []string
		for _, s := range c.closeNames(names, name, id) {
			out = append(out, s.Name)
		}
		return strings.Join(out, ",")
	}
	if g := got([]string{"orders-b", "orders-a", "orderz", "ordres", "unrelated", "orders"}, "orders"); g != "orders-a,orders-b,orderz" && g != "orders-a,orders-b,ordres" {
		t.Errorf("containing names come first and alphabetically, three at most: %s", g)
	}
	if g := got([]string{"ordres", "orderz", "zzz"}, "orders"); g != "orderz,ordres" {
		t.Errorf("near misses, closest then alphabetical: %s", g)
	}
	// A long name tolerates a couple of edits plus a sixth of its length, no more.
	long := strings.Repeat("a", 30)
	if g := got([]string{strings.Repeat("b", 9) + strings.Repeat("a", 21)}, long); g != "" {
		t.Errorf("nine edits of thirty letters suggested: %s", g)
	}
	if g := got([]string{strings.Repeat("b", 7) + strings.Repeat("a", 23)}, long); g == "" {
		t.Error("seven edits of thirty letters not suggested")
	}
}

// A missing topic or group is not found with its near namesakes beside it, and the page
// is the section it belongs to.
func TestNotFoundSuggestsNeighboursInTheRightSection(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "orders-events", 1, 1)
	commitFor(t, addr, "orders-workers", "orders-events", map[int32]int64{0: 1})
	for path, want := range map[string]string{
		"/_console/topics/orders-event":   `href="/_console/topics/orders-events"`,
		"/_console/groups/orders-worker":  `href="/_console/groups/orders-workers"`,
		"/_console/topics/orders-eventsz": `href="/_console/topics/orders-events"`,
	} {
		rec := req(t, c, "GET", path, nil)
		if rec.Code != 404 || !strings.Contains(rec.Body.String(), want) {
			t.Errorf("%s: %d, missing %s", path, rec.Code, want)
		}
	}
	nav := func(path string) string {
		body := req(t, c, "GET", path, nil).Body.String()
		var on []string
		for _, part := range strings.Split(body, `class="nav on"`)[1:] {
			if i := strings.Index(part, `title="`); i >= 0 {
				on = append(on, part[i+7:i+7+strings.Index(part[i+7:], `"`)])
			}
		}
		return strings.Join(on, ",")
	}
	if got := nav("/_console/groups/nope"); !strings.EqualFold(got, "groups") {
		t.Errorf("a missing group highlights %q in the rail", got)
	}
	if got := nav("/_console/topics/nope"); !strings.EqualFold(got, "topics") {
		t.Errorf("a missing topic highlights %q in the rail", got)
	}
}

func TestAddPartitionsRefusalsAndFlash(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "grow", 2, 0)
	ctx := context.Background()
	if err := c.be.AddPartitions(ctx, "grow", 1); err == nil {
		t.Error("shrinking a topic was accepted")
	}
	if err := c.be.AddPartitions(ctx, "grow", 3); err != nil {
		t.Errorf("growing: %v", err)
	}
	rec := req(t, c, "POST", "/_console/topics/grow/partitions", url.Values{"add": {"1"}})
	if rec.Code != 303 || !strings.Contains(rec.Header().Get("Location"), "flash=Topic+now+has+4+partitions") {
		t.Errorf("adding one: %d %s", rec.Code, rec.Header().Get("Location"))
	}
	rec = req(t, c, "POST", "/_console/topics/grow/partitions", url.Values{"add": {"3"}})
	if !strings.Contains(rec.Header().Get("Location"), "flash=Topic+now+has+7+partitions") {
		t.Errorf("adding three: %s", rec.Header().Get("Location"))
	}
	for _, bad := range []string{"0", "-1", "x", ""} {
		if rec := req(t, c, "POST", "/_console/topics/grow/partitions", url.Values{"add": {bad}}); rec.Code != 400 {
			t.Errorf("add=%q: %d", bad, rec.Code)
		}
	}
	if n, _ := c.be.PartitionCount(ctx, "grow"); n != 7 {
		t.Errorf("%d partitions", n)
	}
}

func TestSettingHint(t *testing.T) {
	say := settingHint("retention.ms", "")
	if say == "" || say == "retention.ms" {
		t.Fatalf("no teaching line for retention.ms: %q", say)
	}
	if got := settingHint("retention.ms", "7d or forever"); got != say+" Write it like 7d or forever." {
		t.Errorf("%q", got)
	}
	if got := settingHint("not.a.setting", "like this"); got != "like this" {
		t.Errorf("an unknown key: %q", got)
	}
}

func TestKeyGroupsHoldOnlyTheirOwnScope(t *testing.T) {
	groups := keyGroups()
	if len(groups) != 3 {
		t.Fatalf("%d groups", len(groups))
	}
	total := 0
	for _, g := range groups {
		if len(g.Keys) == 0 {
			t.Errorf("scope %q has no keys", g.Scope)
		}
		for _, k := range g.Keys {
			if k.Scope != g.Scope {
				t.Errorf("%q listed under %q", k.Scope, g.Scope)
			}
		}
		total += len(g.Keys)
	}
	if total == 0 {
		t.Error("no keys")
	}
}

func TestCustomTraceHeaderLengthLimit(t *testing.T) {
	at := func(n int) []traceRef {
		return traceRefs([]MessageHeader{hdr("x-my-trace", strings.Repeat("a", n))}, "x-my-trace")
	}
	if got := at(128); len(got) != 1 {
		t.Errorf("a 128-byte id: %v", got)
	}
	if got := at(129); len(got) != 0 {
		t.Errorf("a 129-byte id: %v", got)
	}
}
