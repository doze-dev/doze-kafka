package console

import (
	"strings"
	"testing"
	"time"
)

func segTexts(segs []confirmSeg) string { return segsText(segs) }

func TestCheckAdviceSaysWhatWouldChangeTheAnswer(t *testing.T) {
	q := aclQuestion{Principal: "User:alice", Operation: "read", Resource: "topic", Name: "orders", Host: "h"}
	deny := aclRule{Principal: "User:alice", Host: "*", Resource: "topic", Name: "orders", Pattern: "literal", Operation: "read", Permission: "deny"}
	allow := deny
	allow.Permission = "allow"
	step := []aclStep{{Decided: true}, {}}
	for name, tc := range map[string]struct {
		rules aclRules
		ans   aclAnswer
		want  string
	}{
		"a super user":                   {aclRules{}, aclAnswer{Allowed: true, Steps: step}, "A super user is never refused; only taking it out of super.users (--acl-super-user) would change this."},
		"nothing is about it, open":      {aclRules{}, aclAnswer{Allowed: true, Steps: []aclStep{{}}}, "No ACL is about this topic, so everyone may do this. Any ACL on it closes it to everyone it does not allow."},
		"allowed by an ALLOW":            {aclRules{}, aclAnswer{Allowed: true, By: &allow, Applies: true, Steps: step}, "A DENY of it for User:alice on orders would refuse it: a DENY always wins."},
		"denied by a DENY, default open": {aclRules{OpenByDefault: true}, aclAnswer{By: &deny, Applies: true}, "Delete the DENY, then add ALLOW User:alice Read on orders. Deleting alone leaves the topic with no ACL, so it opens to everyone."},
		"denied by a DENY, default deny": {aclRules{}, aclAnswer{By: &deny, Applies: true}, "Delete the DENY, then add ALLOW User:alice Read on orders. Deleting alone leaves the topic with no ACL, so the default (deny) refuses it."},
		"denied for want of an ALLOW":    {aclRules{}, aclAnswer{Applies: true}, "Add ALLOW User:alice Read on orders."},
	} {
		if got := segTexts(checkAdvice(tc.rules, q, tc.ans)); got != tc.want {
			t.Errorf("%s:\n got %q\nwant %q", name, got, tc.want)
		}
	}
	// Segments: the principal's ALLOW and the name are in the mono face.
	segs := checkAdvice(aclRules{}, q, aclAnswer{Applies: true})
	if len(segs) != 5 || segs[0].M || !segs[1].M || segs[2].M || !segs[3].M || segs[3].T != "orders" {
		t.Errorf("%+v", segs)
	}
}

func TestCompareAllowedFindsAnotherRequestThatPasses(t *testing.T) {
	acl := func(p, h, res, name, pat, op, perm string) aclRule {
		return aclRule{Principal: p, Host: h, Resource: res, Name: name, Pattern: pat, Operation: op, Permission: perm}
	}
	q := aclQuestion{Principal: "User:bob", Operation: "read", Resource: "topic", Name: "orders", Host: "10.0.0.1"}
	rules := aclRules{ACLs: []aclRule{
		acl("User:alice", "*", "topic", "a-denied", "literal", "read", "deny"),       // not an allow.
		acl("User:alice", "*", "group", "g", "literal", "read", "allow"),             // another kind of resource.
		acl("User:alice", "*", "topic", "t", "literal", "write", "allow"),            // does not grant read.
		acl("User:*", "*", "topic", "any", "literal", "read", "allow"),               // every user: no one to name.
		acl("User:alice", "10.9.9.9", "topic", "hosted", "literal", "read", "allow"), // one host only.
		acl("User:alice", "*", "topic", "pre", "prefixed", "read", "allow"),          // a prefix: no one name.
		acl("User:carol", "*", "topic", "carol-topic", "literal", "all", "allow"),
	}}
	got, ok := compareAllowed(rules, q)
	if !ok {
		t.Fatal("no comparison")
	}
	other := got["Q"].(aclQuestion)
	if other.Principal != "User:carol" || other.Name != "carol-topic" || other.Operation != "read" || other.Host != "10.0.0.1" || got["By"].(aclRule).Name != "carol-topic" {
		t.Errorf("%+v", got)
	}
	// Describe is granted by a read; the question is kept as asked.
	q.Operation = "describe"
	rules.ACLs = []aclRule{acl("User:dan", "*", "topic", "dan", "literal", "read", "allow")}
	if got, ok = compareAllowed(rules, q); !ok || got["Q"].(aclQuestion).Operation != "describe" {
		t.Errorf("%+v %v", got, ok)
	}
	// The same question is not its own comparison.
	q = aclQuestion{Principal: "User:dan", Operation: "read", Resource: "topic", Name: "dan", Host: "h"}
	if _, ok = compareAllowed(rules, q); ok {
		t.Error("a question compared with itself")
	}
	// A deny of the other principal's request is no comparison.
	rules.ACLs = append(rules.ACLs, acl("User:dan", "*", "topic", "dan", "literal", "read", "deny"))
	q.Principal = "User:zed"
	if _, ok = compareAllowed(rules, q); ok {
		t.Error("a denied request compared as an allowed one")
	}
	if _, ok = compareAllowed(aclRules{}, q); ok {
		t.Error("no rules")
	}
}

func TestACLCommandAndEffect(t *testing.T) {
	f := connectFacts{Bootstrap: "127.0.0.1:9092"}
	a := ACLRow{Principal: "User:alice", Host: "*", Resource: "topic", Name: "orders", Pattern: "literal", Operation: "read", Permission: "allow"}
	want := "kafka-acls.sh --bootstrap-server 127.0.0.1:9092 --add \\\n  --allow-principal User:alice --allow-host '*' \\\n  --operation Read --topic orders"
	if got := aclCommand(f, a); got != want {
		t.Errorf("\n got %q\nwant %q", got, want)
	}
	f.CommandConfig = "--command-config client.properties "
	a.Permission, a.Resource, a.Pattern, a.Operation, a.Name = "deny", "group", "prefixed", "describe configs", "g-"
	want = "kafka-acls.sh --bootstrap-server 127.0.0.1:9092 --command-config client.properties --add \\\n  --deny-principal User:alice --deny-host '*' \\\n  --operation DescribeConfigs --group g- --resource-pattern-type prefixed"
	if got := aclCommand(f, a); got != want {
		t.Errorf("\n got %q\nwant %q", got, want)
	}
	if got := aclCommand(f, ACLRow{Principal: "User:a", Host: "h", Resource: "cluster", Operation: "all", Permission: "allow"}); !strings.HasSuffix(got, "--operation All --cluster") {
		t.Errorf("%q", got)
	}
	if got := aclCommand(f, ACLRow{Principal: "User:a", Host: "h", Resource: "transactional id", Name: "tx 1", Operation: "idempotent write", Permission: "allow"}); !strings.HasSuffix(got, "--operation IdempotentWrite --transactional-id 'tx 1'") {
		t.Errorf("%q", got)
	}
	for in, w := range map[string]string{"read": "Read", "alter configs": "AlterConfigs", "": "", "describe configs": "DescribeConfigs"} {
		if got := kafkaACLOp(in); got != w {
			t.Errorf("kafkaACLOp(%q) = %q", in, got)
		}
	}

	effect := func(rules aclRules, a ACLRow) string { return segsText(aclEffect(rules, a)) }
	row := ACLRow{Principal: "User:alice", Resource: "topic", Name: "orders", Pattern: "literal", Operation: "read", Permission: "allow"}
	if got := effect(aclRules{}, ACLRow{}); got != "Fill in the principal and the resource to see what this ACL will do." {
		t.Errorf("%q", got)
	}
	if got := effect(aclRules{}, ACLRow{Principal: "User:a", Resource: "topic"}); !strings.HasPrefix(got, "Fill in") {
		t.Errorf("no name: %q", got)
	}
	if got := effect(aclRules{}, ACLRow{Principal: "User:a", Resource: "cluster", Permission: "allow", Operation: "alter"}); strings.HasPrefix(got, "Fill in") {
		t.Errorf("the cluster needs no name: %q", got)
	}
	if got := effect(aclRules{OpenByDefault: true}, row); !strings.HasPrefix(got, "The topic orders has no ACL yet and is open to everyone.") || !strings.Contains(got, "User:alice may read it") || !strings.Contains(got, "TOPIC_AUTHORIZATION_FAILED") {
		t.Errorf("open: %q", got)
	}
	if got := effect(aclRules{}, row); !strings.Contains(got, "the default is deny, so nobody but a super user may use it. After this, User:alice may read it.") {
		t.Errorf("closed: %q", got)
	}
	existing := aclRules{ACLs: []aclRule{{Resource: "topic", Name: "orders", Pattern: "literal", Principal: "User:x", Permission: "allow", Operation: "read"}}}
	if got := effect(existing, row); !strings.Contains(got, "already has ACLs. After this, User:alice may also read it, unless a DENY that matches refuses it.") {
		t.Errorf("existing: %q", got)
	}
	prefixed := row
	prefixed.Pattern, prefixed.Name = "prefixed", "ord"
	if got := effect(existing, prefixed); !strings.HasPrefix(got, "Every topic whose name starts with ord already has ACLs.") {
		t.Errorf("a prefix covering an existing ACL: %q", got)
	}
	if got := effect(aclRules{}, prefixed); !strings.HasPrefix(got, "Every topic whose name starts with ord has no ACL yet") {
		t.Errorf("a prefix covering nothing: %q", got)
	}
	deny := row
	deny.Permission, deny.Operation = "deny", "write"
	if got := effect(existing, deny); got != "The topic orders: User:alice is refused Write, with TOPIC_AUTHORIZATION_FAILED, whatever an ALLOW says: a DENY always wins. A super user is never refused." {
		t.Errorf("deny: %q", got)
	}
}

func TestACLRowsKeysAndPrincipals(t *testing.T) {
	a := ACLRow{Principal: "User:a b", Resource: "topic", Name: "n:1", Pattern: "literal", Operation: "read", Permission: "allow", Host: "*"}
	if got := a.Key(); got != "User%3Aa+b:topic:n%3A1:literal:read:allow:%2A" {
		t.Errorf("%q", got)
	}
	if r := a.rule(); r.Principal != a.Principal || r.Host != "*" || r.Resource != "topic" || r.Name != "n:1" || r.Pattern != "literal" || r.Operation != "read" || r.Permission != "allow" {
		t.Errorf("%+v", r)
	}
	got := sortedPrincipals([]ACLRow{{Principal: "User:zed"}, {Principal: "User:amy"}, {Principal: "User:zed"}}, []string{"User:root", "User:amy"})
	if strings.Join(got, ",") != "User:ANONYMOUS,User:amy,User:root,User:zed" {
		t.Errorf("%v", got)
	}
	if got = sortedPrincipals(nil, nil); len(got) != 1 || got[0] != "User:ANONYMOUS" {
		t.Errorf("%v", got)
	}
	if contains([]string{"a", "b"}, "b") != true || contains([]string{"a"}, "c") || contains(nil, "a") {
		t.Error("contains")
	}
}

func TestDeniedWindowCountsOnlyTheLastHour(t *testing.T) {
	now := time.Now()
	denied := func(id string, ago time.Duration, count int) TrafficEvent {
		return TrafficEvent{ID: id, Seq: 1, First: now.Add(-ago), Time: now.Add(-ago), Count: count, Class: "denied", Error: "TOPIC_AUTHORIZATION_FAILED", Principal: "User:a", Topics: []string{"t"}, API: "Produce"}
	}
	src := &fakeTraffic{events: []TrafficEvent{
		denied("new", time.Minute, 3), denied("zero", 2*time.Minute, 0), denied("old", 61*time.Minute, 5),
		{ID: "refused", Time: now, First: now, Count: 9, Class: "refused", Error: "X"},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	n, over, last := c.deniedWindow(now, func(TrafficEvent) bool { return true })
	if n != 4 || over != "in the last hour" || last == nil || last.ID != "new" {
		t.Errorf("%d %q %+v", n, over, last)
	}
	if n, _, last = c.deniedWindow(now, func(e TrafficEvent) bool { return e.ID == "zero" }); n != 1 || last.ID != "zero" {
		t.Errorf("a match with count 0 counts once: %d", n)
	}
	if n, _, last = c.deniedWindow(now, func(TrafficEvent) bool { return false }); n != 0 || last != nil {
		t.Errorf("no match: %d %+v", n, last)
	}
	if n, over, last = newTestConsole(t).deniedWindow(now, func(TrafficEvent) bool { return true }); n != 0 || over != "" || last != nil {
		t.Errorf("no recorder: %d %q", n, over)
	}
	// The recorder is full and its oldest row is within the hour: it says since when.
	src.events = nil
	for i := 0; i < 2000; i++ {
		src.events = append(src.events, TrafficEvent{ID: "x", Seq: uint64(i), First: now.Add(-30 * time.Minute), Time: now, Count: 1, API: "Fetch"})
	}
	if _, over, _ = c.deniedWindow(now, func(TrafficEvent) bool { return true }); !strings.HasPrefix(over, "since ") || !strings.HasSuffix(over, "(the oldest request kept)") {
		t.Errorf("a full recorder: %q", over)
	}
	src.events = src.events[:1999]
	if _, over, _ = c.deniedWindow(now, func(TrafficEvent) bool { return true }); over != "in the last hour" {
		t.Errorf("a recorder with room: %q", over)
	}
}

func TestRefusedByNamesTheRequestsAnACLDecided(t *testing.T) {
	topicACL := aclRule{Principal: "User:a", Resource: "topic", Name: "ord", Pattern: "prefixed"}
	ev := func(p, err string, topics ...string) TrafficEvent {
		return TrafficEvent{Principal: p, Error: err, Topics: topics}
	}
	for name, tc := range map[string]struct {
		a    aclRule
		e    TrafficEvent
		want bool
	}{
		"a topic under the prefix":   {topicACL, ev("User:a", "TOPIC_AUTHORIZATION_FAILED", "orders"), true},
		"another topic":              {topicACL, ev("User:a", "TOPIC_AUTHORIZATION_FAILED", "payments"), false},
		"one of several topics":      {topicACL, ev("User:a", "TOPIC_AUTHORIZATION_FAILED", "payments", "orders"), true},
		"another principal":          {topicACL, ev("User:b", "TOPIC_AUTHORIZATION_FAILED", "orders"), false},
		"every user":                 {aclRule{Principal: "User:*", Resource: "topic", Name: "*", Pattern: "literal"}, ev("User:b", "TOPIC_AUTHORIZATION_FAILED", "x"), true},
		"another error":              {topicACL, ev("User:a", "GROUP_AUTHORIZATION_FAILED", "orders"), false},
		"no topics":                  {topicACL, ev("User:a", "TOPIC_AUTHORIZATION_FAILED"), false},
		"a group":                    {aclRule{Principal: "User:a", Resource: "group", Name: "g", Pattern: "literal"}, TrafficEvent{Principal: "User:a", Error: "GROUP_AUTHORIZATION_FAILED", Group: "g"}, true},
		"another group":              {aclRule{Principal: "User:a", Resource: "group", Name: "g", Pattern: "literal"}, TrafficEvent{Principal: "User:a", Error: "GROUP_AUTHORIZATION_FAILED", Group: "h"}, false},
		"a transactional id":         {aclRule{Principal: "User:a", Resource: "transactional id", Name: "tx", Pattern: "literal"}, TrafficEvent{Principal: "User:a", Error: "TRANSACTIONAL_ID_AUTHORIZATION_FAILED", TxnID: "tx"}, true},
		"another transactional id":   {aclRule{Principal: "User:a", Resource: "transactional id", Name: "tx", Pattern: "literal"}, TrafficEvent{Principal: "User:a", Error: "TRANSACTIONAL_ID_AUTHORIZATION_FAILED", TxnID: "ty"}, false},
		"the cluster":                {aclRule{Principal: "User:a", Resource: "cluster", Name: "kafka-cluster", Pattern: "literal"}, TrafficEvent{Principal: "User:a", Error: "CLUSTER_AUTHORIZATION_FAILED"}, true},
		"the cluster, another error": {aclRule{Principal: "User:a", Resource: "cluster", Name: "kafka-cluster", Pattern: "literal"}, TrafficEvent{Principal: "User:a", Error: "TOPIC_AUTHORIZATION_FAILED"}, false},
	} {
		if got := refusedBy(tc.a, tc.e); got != tc.want {
			t.Errorf("%s: %v", name, got)
		}
	}
}
