package console

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

// Ac-Home, built from what the broker holds: Create ACL, the checker card, the
// principals (with User:ANONYMOUS and the super users), the requests an ACL refused
// in the last hour, a row that opens what it lets through or refuses, and the wire of
// ACL requests and refusals under the page.
func TestAccessHomeIsTheBoards(t *testing.T) {
	now := time.Now()
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "d2", Seq: 2, First: now.Add(-time.Minute), Time: now.Add(-time.Minute), Count: 3, ClientID: "loyalty-etl", Remote: "127.0.0.1:55127", Principal: "User:ANONYMOUS",
			API: "OffsetFetch", Version: 8, Group: "g-loyalty-etl", Error: "GROUP_AUTHORIZATION_FAILED", ErrorCode: 30, Class: "denied"},
		{ID: "d1", Seq: 1, First: now.Add(-2 * time.Minute), Time: now.Add(-2 * time.Minute), Count: 1, ClientID: "acl-admin", Remote: "127.0.0.1:55100", Principal: "User:ANONYMOUS",
			API: "DescribeAcls", Version: 3},
		{ID: "d0", Seq: 0, First: now.Add(-3 * time.Minute), Time: now.Add(-3 * time.Minute), Count: 1, ClientID: "app", Remote: "127.0.0.1:55101", Principal: "User:ANONYMOUS",
			API: "Produce", Version: 9, Topics: []string{"orders"}},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	for _, f := range []url.Values{
		{"principal": {"User:ANONYMOUS"}, "host": {"*"}, "resource": {"group"}, "name": {"g-loyalty-etl"}, "pattern": {"literal"}, "operation": {"read"}, "permission": {"deny"}},
		{"principal": {"User:validator"}, "host": {"*"}, "resource": {"group"}, "name": {"g-order-validator"}, "pattern": {"literal"}, "operation": {"read"}, "permission": {"allow"}},
	} {
		if rec := req(t, c, "POST", "/_console/new/acl", f); rec.Code != 303 {
			t.Fatalf("create %v: %d %s", f, rec.Code, rec.Body)
		}
	}
	page := readable(req(t, c, "GET", "/_console/access", nil).Body.String())
	for _, want := range []string{
		"Principals", "All ACLs", "User:ANONYMOUS", "User:validator", "1 deny",
		"Create ACL", "2 ACLs", "default allow (--acl-default)", "3 requests refused in the last hour",
		"Select a row to see what it lets through or refuses.",
		"Could a principal do this?", "Open the checker",
		"The wire, underneath", "ACL requests and refusals", "OffsetFetch v8 · g-loyalty-etl", "GROUP_AUTHORIZATION_FAILED", "DescribeAcls v3",
		"How the default works",
	} {
		if !strings.Contains(page, want) {
			t.Errorf("Ac-Home does not show %q", want)
		}
	}
	if strings.Contains(page, "Produce v9") {
		t.Error("the ACL wire shows a request that is neither about ACLs nor refused by one")
	}
	raw := req(t, c, "GET", "/_console/access", nil).Body.String()
	if !strings.Contains(raw, `href="/_console/new/acl" data-create`) || !strings.Contains(raw, `hx-get="/_console/inspect/acl?key=`) {
		t.Error("Create ACL is not the page's c, or a row does not open in the inspector")
	}

	// The checker page answers, and says the way a real request was refused.
	chk := readable(req(t, c, "GET", "/_console/access/check?principal=User:ANONYMOUS&operation=read&resource=group&name=g-loyalty-etl&host=127.0.0.1", nil).Body.String())
	for _, want := range []string{"DENIED", "User:ANONYMOUS cannot Read group g-loyalty-etl from 127.0.0.1", "GROUP_AUTHORIZATION_FAILED, the way loyalty-etl was at",
		"The ACL that decided", "How the rules were read", "A matching DENY? Yes: the ACL above.", "To let it through", "Delete the DENY, then add ALLOW User:ANONYMOUS Read on g-loyalty-etl"} {
		if !strings.Contains(chk, want) {
			t.Errorf("the checker does not say %q:\n%s", want, chk)
		}
	}
	ok := readable(req(t, c, "GET", "/_console/access/check?principal=User:validator&operation=describe&resource=group&name=g-order-validator&host=127.0.0.1", nil).Body.String())
	if !strings.Contains(ok, "ALLOWED") || !strings.Contains(ok, "A matching ALLOW? Yes") {
		t.Errorf("Read does not grant Describe in the checker:\n%s", ok)
	}

	// The create form: its command and what it will do, as typed; a refusal in place.
	prev := readable(req(t, c, "GET", "/_console/new/acl/preview?principal=User:billing&host=*&resource=topic&name=orders&pattern=prefixed&operation=describe%20configs&permission=allow", nil).Body.String())
	for _, want := range []string{"kafka-acls.sh --bootstrap-server", "--allow-principal User:billing", "--operation DescribeConfigs --topic orders --resource-pattern-type prefixed",
		"Every topic whose name starts with orders has no ACL yet and is open to everyone."} {
		if !strings.Contains(prev, want) {
			t.Errorf("the preview does not say %q:\n%s", want, prev)
		}
	}
	bad := req(t, c, "POST", "/_console/new/acl", url.Values{"principal": {"billing"}, "host": {"*"}, "resource": {"topic"}, "name": {"orders"}, "pattern": {"literal"}, "operation": {"read"}, "permission": {"allow"}})
	if bad.Code < 400 || !strings.Contains(readable(bad.Body.String()), "Invalid principal") {
		t.Errorf("a principal without its type: %d %s", bad.Code, bad.Body)
	}
}
