package console

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// There is one inspector contract and every selection renders through it: a
// title, its kind, a close control, and the address (data-open) a page puts in
// its URL. A kind that skipped the contract would open without a way out or
// would not survive a refresh, so each of the kinds is fetched and held to it.
func TestEverySelectionRendersThroughTheInspectorContract(t *testing.T) {
	src := sampleTraffic()
	c, addr, _ := newRegistryConsole(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "orders", 2, 3)
	register(t, c, "orders-value", userV1)

	// A live group member.
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("billing-service"), kgo.ConsumerGroup("billing"), kgo.ConsumeTopics("orders"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	cl.PollRecords(ctx, 3)
	cancel()
	var memberID string
	for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline) && memberID == ""; time.Sleep(150 * time.Millisecond) {
		page := req(t, c, "GET", "/_console/groups/billing?tab=members", nil).Body.String()
		if m := regexp.MustCompile(`id="mb-([^"]+)"`).FindStringSubmatch(page); m != nil {
			memberID = m[1]
		}
	}
	if memberID == "" {
		t.Fatal("the group never showed a member")
	}

	// An open transaction.
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"tx"}, "partitions": {"1"}})
	p := txnProducer(t, addr, "inspect-txn")
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	txnWrite(t, context.Background(), p, "tx", "open")

	reqKey := src.events[1].ID
	// An ACL, made through the console's own form.
	if rec := req(t, c, "POST", "/_console/new/acl", url.Values{"principal": {"User:audit"}, "host": {"*"}, "resource": {"topic"}, "name": {"secrets"},
		"pattern": {"literal"}, "operation": {"read"}, "permission": {"deny"}}); rec.Code != 303 {
		t.Fatalf("create an ACL: %d %s", rec.Code, rec.Body)
	}
	aclKey := joinKey("User:audit", "topic", "secrets", "literal", "read", "deny", "*")
	cases := []struct {
		kind, path string
		title      string // the title the selection must have.
		open       string // the address it must name.
		want       []string
	}{
		{KindMessage, "/_console/topics/orders/message?partition=0&offset=1", "P0 · offset 1", "1", []string{"Timestamp", "Send again"}},
		{KindRequest, "/_console/inspect/request?key=" + reqKey, "Produce v9", "request:" + reqKey, []string{"billing", "1 partition, 2.0 KiB"}},
		{KindClient, "/_console/inspect/client?key=1", "billing", "client:1", []string{"apache-kafka-java 3.7.0", "billing-workers", "41"}},
		{KindMember, "/_console/inspect/member?key=" + url.QueryEscape(joinKey("billing", memberID)), "billing-service", "member:" + joinKey("billing", memberID), []string{"Assigned partitions", "orders", "0–1"}},
		{KindSubject, "/_console/inspect/subject?key=orders-value", "orders-value", "subject:orders-value", []string{"Compatibility", "Avro"}},
		{KindTransaction, "/_console/inspect/transaction?key=inspect-txn", "inspect-txn", "transaction:inspect-txn", []string{"tx-0", "Ongoing"}},
		{KindEvent, "/_console/inspect/event?key=" + url.QueryEscape(joinKey("billing-workers", "3")), "session expired", "event:" + joinKey("billing-workers", "3"), []string{"no heartbeat within its session timeout"}},
		{KindACL, "/_console/inspect/acl?key=" + url.QueryEscape(aclKey), "DENY User:audit Read", "acl:" + aclKey, []string{"Refuses every request that needs Read on this topic by User:audit", "It is the only ACL about it", "Check a request"}},
	}
	seen := map[string]bool{}
	for _, tc := range cases {
		seen[tc.kind] = true
		rec := req(t, c, "GET", tc.path, nil)
		if rec.Code != 200 {
			t.Errorf("%s: status %d: %s", tc.kind, rec.Code, rec.Body)
			continue
		}
		body := rec.Body.String()
		for _, want := range []string{
			`data-inspector-kind="` + tc.kind + `"`, // a kind,
			`id="inspector-title"`,                  // a title,
			`data-inspector-close`,                  // a close control,
			`data-open="` + tc.open + `"`,           // and an address.
		} {
			if !strings.Contains(body, want) {
				t.Errorf("%s: the inspector is missing %s:\n%s", tc.kind, want, body)
			}
		}
		if !strings.Contains(readable(body), tc.title) {
			t.Errorf("%s: the title should be %q:\n%s", tc.kind, tc.title, body)
		}
		for _, want := range tc.want {
			if !strings.Contains(readable(body), want) {
				t.Errorf("%s: the inspector is missing %q:\n%s", tc.kind, want, readable(body))
			}
		}
	}
	for _, k := range inspectorKinds {
		if !seen[k] {
			t.Errorf("kind %q is declared and has no case: the contract is untested for it", k)
		}
	}

	// A selection that is gone still opens, says so, and can be closed.
	for _, path := range []string{
		"/_console/inspect/request?key=nope", "/_console/inspect/client?key=99",
		"/_console/inspect/member?key=" + url.QueryEscape(joinKey("billing", "no-such-member")),
		"/_console/inspect/transaction?key=nobody", "/_console/inspect/event?key=" + url.QueryEscape(joinKey("billing-workers", "999")),
	} {
		rec := req(t, c, "GET", path, nil)
		body := rec.Body.String()
		if rec.Code != 200 || !strings.Contains(body, `data-inspector-close`) || !strings.Contains(body, "Close this and choose another") {
			t.Errorf("%s: a vanished selection answered %d without a way out:\n%s", path, rec.Code, body)
		}
	}
	// What cannot be a selection is refused, in the shell's own words.
	for path, code := range map[string]int{
		"/_console/inspect/nonsense?key=1":       404,
		"/_console/inspect/client?key=abc":       400,
		"/_console/inspect/member?key=one-part":  400,
		"/_console/inspect/event?key=g%3Aabc":    400,
		"/_console/inspect/subject?key=no-such":  404,
		"/_console/inspect/transaction":          400,
		"/_console/topics/orders/message?offset": 400,
	} {
		if rec := req(t, c, "GET", path, nil); rec.Code != code {
			t.Errorf("%s answered %d, want %d", path, rec.Code, code)
		}
	}
}

func TestKeysWithSeparatorsInTheirNamesRoundTrip(t *testing.T) {
	for _, parts := range [][]string{{"g", "m"}, {"a:b", "c/d"}, {"", "x"}, {"é ü", "100%"}, {"a?b=c&d", "#frag"}} {
		got := splitKey(joinKey(parts...), len(parts))
		if len(got) != len(parts) || got[0] != parts[0] || got[1] != parts[1] {
			t.Errorf("%q -> %q", parts, got)
		}
	}
	if splitKey("only-one", 2) != nil || splitKey("a:b:c", 2) != nil {
		t.Error("a key with the wrong number of parts was accepted")
	}
}

// The panel that used to be called the drawer is the inspector, and nothing in
// the console still names it (the brief: "#drawer is gone").
func TestThereIsNoDrawerLeft(t *testing.T) {
	for _, dir := range []string{"templates", "static"} {
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if e.IsDir() || strings.HasSuffix(e.Name(), ".min.js") {
				continue
			}
			b, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(strings.ToLower(string(b)), "drawer") {
				t.Errorf("%s/%s still mentions the drawer", dir, e.Name())
			}
		}
	}
	c := newTestConsole(t)
	body := req(t, c, "GET", "/_console/topics", nil).Body.String()
	if strings.Contains(body, `id="drawer`) || !strings.Contains(body, `id="inspector"`) {
		t.Error("the layout should have an #inspector and no #drawer")
	}
}
