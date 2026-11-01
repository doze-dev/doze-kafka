package console

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSchemaSubjectPageSaysWhatIsLatestAndWhatChanged(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "orders-value", userV1)
	register(t, c, "orders-value", userV2)
	raw := req(t, c, "GET", "/_console/schemas/orders-value", nil).Body.String()
	text := readable(raw)
	if !strings.Contains(text, "v2 latest") || strings.Contains(text, "v1 latest") || !strings.Contains(text, "8 lines changed from v1") || !strings.Contains(text, "first version") {
		t.Errorf("versions table:\n%s", between(text, "VersionIdWhat", "Schema v2"))
	}
	if !strings.Contains(text, "latest v2, id 2") {
		t.Error("the latest version and its id are not told")
	}
	// Tab addresses: Versions is the subject's own address; the others carry tab=.
	for _, want := range []string{`href="/_console/schemas/orders-value"`, `href="/_console/schemas/orders-value?tab=compat"`, `href="/_console/schemas/orders-value?tab=register"`} {
		if !strings.Contains(between(raw, `aria-label="Subject tabs"`, "</nav>"), want) {
			t.Errorf("no tab link %s", want)
		}
	}
	// The editor for a new version starts from the latest, whichever version is on show.
	reg := readable(req(t, c, "GET", "/_console/schemas/orders-value?tab=register&version=1", nil).Body.String())
	if !strings.Contains(reg, "Delete v1") || !strings.Contains(reg, `"email"`) || !strings.Contains(reg, "Schema starts from the latest version") {
		t.Errorf("register tab on version 1:\n%s", between(reg, "VersionsCompatibilityRegister", "Check compatibility"))
	}
}

func TestACLPrincipalsAndFilters(t *testing.T) {
	c, _ := newTestConsoleAddr(t)
	acl := func(p, res, name, op, perm string) {
		t.Helper()
		rec := req(t, c, "POST", "/_console/new/acl", url.Values{"principal": {p}, "host": {"*"}, "resource": {res}, "name": {name}, "pattern": {"literal"}, "operation": {op}, "permission": {perm}})
		if rec.Code != 303 {
			t.Fatalf("create %s: %d %s", name, rec.Code, rec.Body)
		}
	}
	acl("User:alice", "topic", "a1", "read", "allow")
	acl("User:alice", "topic", "a2", "write", "allow")
	acl("User:alice", "topic", "a3", "write", "deny")
	acl("User:bob", "group", "g1", "read", "allow")
	acl("User:*", "topic", "everyone", "describe", "allow")

	all := req(t, c, "GET", "/_console/access", nil).Body.String()
	list := between(all, `aria-label="Principals"`, "</nav>")
	for principal, want := range map[string]string{"User:alice": `<span class="m deny">1 deny</span>`, "User:bob": `<span class="m mut">1</span>`} {
		row := between(list, `data-name="`+principal+`"`, "</a>")
		if !strings.Contains(row, want) {
			t.Errorf("%s in the list:\n%s\nwant %s", principal, row, want)
		}
	}
	if row := between(list, `data-name="User:*"`, "</a>"); !strings.Contains(row, `<span class="m mut">1</span>`) {
		t.Errorf("User:*: %s", row)
	}
	if !strings.Contains(list, `>All ACLs</span><span class="m mut">5</span>`) {
		t.Errorf("the count of all ACLs:\n%s", list)
	}
	table := func(body string) string { return between(body, `id="acl-table"`, "</div>\n    <div class=\"note\">") }
	if got := strings.Count(table(all), "data-open-keys"); got != 5 {
		t.Errorf("all: %d rows", got)
	}
	// One principal's page lists its own ACLs and the ones for every user.
	alice := req(t, c, "GET", "/_console/access?principal=User:alice", nil).Body.String()
	rows := table(alice)
	if strings.Count(rows, "data-open-keys") != 4 || strings.Contains(rows, "g1") || !strings.Contains(rows, "everyone") || !strings.Contains(rows, "a3") {
		t.Errorf("alice's page:\n%s", rows)
	}
	bob := table(req(t, c, "GET", "/_console/access?principal=User:bob", nil).Body.String())
	if strings.Count(bob, "data-open-keys") != 2 || strings.Contains(bob, "a1") {
		t.Errorf("bob's page:\n%s", bob)
	}
	none := req(t, c, "GET", "/_console/access?principal=User:nobody", nil).Body.String()
	if strings.Count(table(none), "data-open-keys") != 1 {
		t.Errorf("a principal with no ACLs sees only the ones for every user:\n%s", table(none))
	}
	// The checker starts from the first DENY, else the first ACL.
	rowsList := []ACLRow{
		{Principal: "User:a", Operation: "read", Resource: "topic", Name: "first", Permission: "allow"},
		{Principal: "User:b", Operation: "write", Resource: "group", Name: "second", Permission: "deny"},
		{Principal: "User:c", Operation: "read", Resource: "topic", Name: "third", Permission: "deny"},
	}
	if q := c.checkPrefill(rowsList); q.Principal != "User:b" || q.Name != "second" || q.Operation != "write" || q.Resource != "group" {
		t.Errorf("%+v", q)
	}
	if q := c.checkPrefill(rowsList[:1]); q.Principal != "User:a" || q.Name != "first" {
		t.Errorf("%+v", q)
	}
	if q := c.checkPrefill(nil); q.Principal != "User:ANONYMOUS" || q.Operation != "read" || q.Resource != "topic" || q.Name != "" {
		t.Errorf("%+v", q)
	}
}

func TestACLCreateFormDefaultsAndClusterName(t *testing.T) {
	c := newTestConsole(t)
	page := req(t, c, "GET", "/_console/new/acl", nil).Body.String()
	// Nothing typed: a literal ALLOW of Read on a topic, from any host.
	if !strings.Contains(page, `name="host" value="*"`) || !strings.Contains(page, `name="permission" :value="perm" value="allow"`) {
		t.Errorf("the create form's own defaults")
	}
	if want := "--allow-principal &#39;&#39; --allow-host &#39;*&#39; \\\n  --operation Read --topic &#39;&#39;</pre>"; !strings.Contains(page, want) {
		t.Errorf("the default command:\n%s", between(page, `id="acl-command"`, "</pre>"))
	}
	if strings.Contains(between(page, `id="acl-command"`, "</pre>"), "resource-pattern-type") {
		t.Error("a pattern type on a literal ACL")
	}
	given := readable(req(t, c, "GET", "/_console/new/acl?host=10.1.1.1&resource=group&pattern=prefixed&operation=write&permission=deny&principal=User:z&name=g-", nil).Body.String())
	if !strings.Contains(given, "--deny-principal User:z --deny-host 10.1.1.1") || !strings.Contains(given, "--operation Write --group g-") || !strings.Contains(given, "--resource-pattern-type prefixed") {
		t.Errorf("what was asked for is not what the command says:\n%s", given)
	}
	// A topic ACL keeps the name it was given; with none it is refused.
	if rec := req(t, c, "POST", "/_console/new/acl", url.Values{"principal": {"User:y"}, "host": {"*"}, "resource": {"topic"}, "name": {"t"}, "pattern": {"literal"}, "operation": {"read"}, "permission": {"allow"}}); rec.Code != 303 {
		t.Fatalf("%d", rec.Code)
	}
	rows, err := c.be.ListACLs(context.Background())
	if err != nil || len(rows) != 1 || rows[0].Name != "t" {
		t.Errorf("%+v %v", rows, err)
	}
	if rec := req(t, c, "POST", "/_console/new/acl", url.Values{"principal": {"User:y"}, "host": {"*"}, "resource": {"topic"}, "pattern": {"literal"}, "operation": {"read"}, "permission": {"allow"}}); rec.Code == 303 {
		t.Error("a topic ACL with no name was created")
	}
	// The cluster has one name, so none need be given. (Last: once the cluster has an ACL
	// the console's own anonymous login may not describe ACLs any more.)
	rec := req(t, c, "POST", "/_console/new/acl", url.Values{"principal": {"User:x"}, "host": {"*"}, "resource": {"cluster"}, "pattern": {"literal"}, "operation": {"alter"}, "permission": {"allow"}})
	if rec.Code != 303 || !strings.Contains(rec.Header().Get("Location"), "on+cluster+kafka-cluster") {
		t.Errorf("%d %s", rec.Code, rec.Header().Get("Location"))
	}
}

func TestCheckerAnswersOnlyWhenAsked(t *testing.T) {
	c, _ := newTestConsoleAddr(t)
	check := func(q string) string {
		return readable(req(t, c, "GET", "/_console/access/check?"+q, nil).Body.String())
	}
	if body := check("principal=User:a&operation=read&resource=topic"); strings.Contains(body, "ALLOWED") || strings.Contains(body, "DENIED") {
		t.Errorf("a topic with no name was answered:\n%s", body)
	}
	if body := check("principal=User:a&operation=read&resource=cluster"); !strings.Contains(body, "ALLOWED") && !strings.Contains(body, "DENIED") {
		t.Errorf("the cluster needs no name:\n%s", body)
	}
	for _, q := range []string{"operation=read&resource=cluster", "principal=User:a&resource=cluster", "principal=User:a&operation=read"} {
		if body := check(q); strings.Contains(body, "ALLOWED") || strings.Contains(body, "DENIED") {
			t.Errorf("%s was answered", q)
		}
	}
	// The host defaults to where the broker is, unless one is given.
	if body := check("principal=User:a&operation=read&resource=topic&name=t"); !strings.Contains(body, "from 127.0.0.1") {
		t.Errorf("default host:\n%s", body)
	}
	if body := check("principal=User:a&operation=read&resource=topic&name=t&host=10.9.9.9"); !strings.Contains(body, "from 10.9.9.9") {
		t.Errorf("given host:\n%s", body)
	}
	if rec := req(t, c, "GET", "/_console/access/check?principal=User:a&operation=sideways&resource=topic&name=t", nil); rec.Code != 400 {
		t.Errorf("a made-up operation: %d", rec.Code)
	}
	if rec := req(t, c, "GET", "/_console/access/check?principal=User:a&operation=read&resource=nonsense&name=t", nil); rec.Code != 400 {
		t.Errorf("a made-up resource: %d", rec.Code)
	}
}

func TestCheckHostAndConsolePrincipal(t *testing.T) {
	src := &fakeTraffic{clients: []TrafficClient{{Conn: 1, Remote: "10.1.2.3:5555"}, {Conn: 2, Remote: "10.4.5.6:777"}}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	if h := c.checkHost(); h != "10.4.5.6" {
		t.Errorf("the newest client's host: %q", h)
	}
	src.clients = []TrafficClient{{Conn: 1, Remote: "no-port"}}
	if h := c.checkHost(); h != "127.0.0.1" {
		t.Errorf("a client with no usable address: %q", h)
	}
	src.clients = nil
	if h := c.checkHost(); h != "127.0.0.1" {
		t.Errorf("no clients: %q", h)
	}
	c.be.opts.DisplayAddr = ":9092"
	if h := c.checkHost(); h != "*" {
		t.Errorf("a display address with no host: %q", h)
	}
	c.be.opts.DisplayAddr = "nohostport"
	if h := c.checkHost(); h != "*" {
		t.Errorf("a display address that does not parse: %q", h)
	}
	c.be.opts.DisplayAddr = ""
	if c.consolePrincipal() != "User:ANONYMOUS" {
		t.Errorf("%q", c.consolePrincipal())
	}
	c.be.opts.SASLUser = "admin"
	if c.consolePrincipal() != "User:admin" {
		t.Errorf("%q", c.consolePrincipal())
	}
	c.be.opts.SASLUser = ""
	plain := newTestConsole(t)
	if h := plain.checkHost(); h != "127.0.0.1" {
		t.Errorf("without a recorder: %q", h)
	}
}

func TestACLInspectorCountsWhatItRefusedAndWhatElseIsAbout(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "d1", Seq: 1, First: now, Time: now, Count: 4, ClientID: "app", Principal: "User:alice", API: "Produce", Topics: []string{"secret"}, Error: "TOPIC_AUTHORIZATION_FAILED", Class: "denied"},
		{ID: "d2", Seq: 2, First: now, Time: now, Count: 1, ClientID: "app", Principal: "User:bob", API: "Produce", Topics: []string{"secret"}, Error: "TOPIC_AUTHORIZATION_FAILED", Class: "denied"},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	for _, f := range []url.Values{
		{"principal": {"User:alice"}, "host": {"*"}, "resource": {"topic"}, "name": {"secret"}, "pattern": {"literal"}, "operation": {"read"}, "permission": {"deny"}},
		{"principal": {"User:alice"}, "host": {"*"}, "resource": {"topic"}, "name": {"secret"}, "pattern": {"literal"}, "operation": {"write"}, "permission": {"allow"}},
		{"principal": {"User:carol"}, "host": {"*"}, "resource": {"topic"}, "name": {"secret"}, "pattern": {"literal"}, "operation": {"read"}, "permission": {"allow"}},
		{"principal": {"User:dan"}, "host": {"*"}, "resource": {"topic"}, "name": {"other"}, "pattern": {"literal"}, "operation": {"read"}, "permission": {"allow"}},
	} {
		if rec := req(t, c, "POST", "/_console/new/acl", f); rec.Code != 303 {
			t.Fatalf("%d", rec.Code)
		}
	}
	inspect := func(a ACLRow) string {
		rec := req(t, c, "GET", "/_console/inspect/acl?key="+url.QueryEscape(a.Key()), nil)
		if rec.Code != 200 {
			t.Fatalf("%d", rec.Code)
		}
		return readable(rec.Body.String())
	}
	deny := ACLRow{Principal: "User:alice", Host: "*", Resource: "topic", Name: "secret", Pattern: "literal", Operation: "read", Permission: "deny"}
	body := inspect(deny)
	for _, want := range []string{"DENY User:alice Read", "Topic secret", "Refused4 requests in the last hour", "2 other ACLs are about it too.", "Last", "TOPIC_AUTHORIZATION_FAILED"} {
		if !strings.Contains(body, want) {
			t.Errorf("deny lacks %q:\n%s", want, body)
		}
	}
	allow := deny
	allow.Principal, allow.Permission, allow.Operation = "User:carol", "allow", "read"
	body = inspect(allow)
	if strings.Contains(body, "Refused4") || strings.Contains(body, "right now") || !strings.Contains(body, "2 other ACLs are about it too.") {
		t.Errorf("an ALLOW is credited with refusals, or miscounts its neighbours:\n%s", body)
	}
	gone := deny
	gone.Name = "vanished"
	if body = inspect(gone); !strings.Contains(body, "no longer stored") {
		t.Errorf("an ACL that is not there:\n%s", body)
	}
	if rec := req(t, c, "GET", "/_console/inspect/acl?key=short", nil); rec.Code != 400 {
		t.Errorf("a malformed key: %d", rec.Code)
	}
}

func TestCheckAdviceForASuperUserWithOneStep(t *testing.T) {
	q := aclQuestion{Principal: "User:root", Operation: "read", Resource: "topic", Name: "t"}
	// A super user's answer: allowed, decided by the first step, nothing else read.
	got := segsText(checkAdvice(aclRules{}, q, aclAnswer{Allowed: true, Steps: []aclStep{{Decided: true}}}))
	if !strings.HasPrefix(got, "No ACL is about this topic") {
		t.Errorf("one step is not a super user's answer: %q", got)
	}
	got = segsText(checkAdvice(aclRules{}, q, aclAnswer{Allowed: true, Steps: []aclStep{{Decided: true}, {}}}))
	if !strings.HasPrefix(got, "A super user is never refused") {
		t.Errorf("%q", got)
	}
}

func TestConfirmFactsAndSegments(t *testing.T) {
	segs := []confirmSeg{{T: "<a>"}, {T: "m&m", M: true}, {T: `"b"`, B: true}}
	if got := string(segsHTML(segs)); got != `&lt;a&gt;<span class="m">m&amp;m</span><span class="b">&#34;b&#34;</span>` {
		t.Errorf("%q", got)
	}
	dir := t.TempDir()
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.Facts.KafkaMajor = 0 })
	c.facts.DataDir = dir
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "orders", 2, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "orders", 0, 5, vals("a"), time.Time{})
	bulkProduce(t, cl, "orders", 1, 2, vals("b"), time.Time{})
	deleteBefore(t, cl, "orders", 0, 2)
	if f := c.topicFacts(ctx, "orders", 2); f.AutoKnown || f.DiskOK || !f.DiskExact {
		t.Errorf("no files, no major: %+v", f)
	}
	logFile := func(part, base int) {
		d := filepath.Join(dir, "orders-"+string(rune('0'+part)))
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		name := leftPad(base) + ".log"
		if err := os.WriteFile(filepath.Join(d, name), make([]byte, 10), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	logFile(0, 0) // a segment that begins before the log start (2): its bytes include deleted records.
	logFile(1, 0)
	if f := c.topicFacts(ctx, "orders", 2); !f.DiskOK || f.DiskExact {
		t.Errorf("a segment before the log start: ok %v exact %v", f.DiskOK, f.DiskExact)
	}
	if err := os.Remove(filepath.Join(dir, "orders-0", leftPad(0)+".log")); err != nil {
		t.Fatal(err)
	}
	logFile(0, 2) // begins exactly at the log start.
	if f := c.topicFacts(ctx, "orders", 2); !f.DiskOK || !f.DiskExact {
		t.Errorf("a segment at the log start: ok %v exact %v", f.DiskOK, f.DiskExact)
	}
	if err := os.Remove(filepath.Join(dir, "orders-0", leftPad(2)+".log")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "orders-0", "other.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if f := c.topicFacts(ctx, "orders", 2); !f.DiskOK || !f.DiskExact {
		t.Errorf("no segment at all in a partition: ok %v exact %v", f.DiskOK, f.DiskExact)
	}
	c.facts.KafkaMajor = 4
	if f := c.topicFacts(ctx, "orders", 2); !f.AutoKnown {
		t.Error("a broker that declared its Kafka major does not know its auto-create")
	}
}

func leftPad(n int) string {
	s := "00000000000000000000"
	d := []byte(s)
	for i := len(d) - 1; n > 0 && i >= 0; i-- {
		d[i] = byte('0' + n%10)
		n /= 10
	}
	return string(d)
}

func TestPurgeSentenceListsEightLogStartsButNotNine(t *testing.T) {
	mk := func(n int) confirmFacts {
		f := confirmFacts{Name: "t", Partitions: n, Records: int64(n)}
		for i := 0; i < n; i++ {
			f.Counts, f.Ends = append(f.Counts, 1), append(f.Ends, int64(100+i))
		}
		return f
	}
	if got := rowText(topicPurgeSay(mk(8)), "New log start"); got != "100 / 101 / 102 / 103 / 104 / 105 / 106 / 107" {
		t.Errorf("eight: %q", got)
	}
	if got := rowText(topicPurgeSay(mk(9)), "New log start"); got != "each partition's log end" {
		t.Errorf("nine: %q", got)
	}
}
