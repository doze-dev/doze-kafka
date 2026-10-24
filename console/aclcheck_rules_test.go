package console

import (
	"strings"
	"testing"
)

func TestACLGrants(t *testing.T) {
	for _, tc := range []struct {
		stored, wanted string
		want           bool
	}{
		{"read", "read", true}, {"all", "read", true}, {"all", "alter configs", true}, {"read", "write", false},
		{"read", "describe", true}, {"write", "describe", true}, {"delete", "describe", true}, {"alter", "describe", true},
		{"create", "describe", false}, {"describe", "describe", true}, {"describe configs", "describe", false},
		{"alter configs", "describe configs", true}, {"read", "describe configs", false}, {"describe configs", "describe configs", true},
		{"alter configs", "describe", false}, {"describe", "read", false},
	} {
		if got := grants(tc.stored, tc.wanted); got != tc.want {
			t.Errorf("grants(%q, %q) = %v", tc.stored, tc.wanted, got)
		}
	}
}

func TestACLAboutAndMatches(t *testing.T) {
	lit := aclRule{Resource: "topic", Name: "orders", Pattern: "literal"}
	pre := aclRule{Resource: "topic", Name: "ord", Pattern: "prefixed"}
	all := aclRule{Resource: "topic", Name: "*", Pattern: "literal"}
	for _, tc := range []struct {
		rule           aclRule
		resource, name string
		want           bool
	}{
		{lit, "topic", "orders", true}, {lit, "topic", "orders2", false}, {lit, "group", "orders", false}, {lit, "topic", "", false},
		{pre, "topic", "ord", true}, {pre, "topic", "orders", true}, {pre, "topic", "or", false}, {pre, "group", "orders", false},
		{all, "topic", "anything", true}, {all, "group", "anything", false}, {all, "topic", "", true},
		{aclRule{Resource: "topic", Name: "*", Pattern: "prefixed"}, "topic", "x", false},
	} {
		if got := tc.rule.about(tc.resource, tc.name); got != tc.want {
			t.Errorf("%+v about(%q, %q) = %v", tc.rule, tc.resource, tc.name, got)
		}
	}
	r := aclRule{Principal: "User:alice", Host: "10.0.0.1"}
	for _, tc := range []struct {
		rule            aclRule
		principal, host string
		want            bool
	}{
		{r, "User:alice", "10.0.0.1", true}, {r, "User:bob", "10.0.0.1", false}, {r, "User:alice", "10.0.0.2", false},
		{aclRule{Principal: "User:*", Host: "*"}, "User:anyone", "1.2.3.4", true}, {aclRule{Principal: "User:alice", Host: "*"}, "User:alice", "x", true},
		{aclRule{Principal: "User:alice", Host: "*"}, "User:bob", "x", false}, {aclRule{Principal: "User:*", Host: "10.0.0.1"}, "User:z", "10.0.0.2", false},
		{aclRule{Principal: "Group:*", Host: "*"}, "User:z", "x", false},
	} {
		if got := tc.rule.matches(tc.principal, tc.host); got != tc.want {
			t.Errorf("%+v matches(%q, %q) = %v", tc.rule, tc.principal, tc.host, got)
		}
	}
}

func TestACLCheckerFollowsTheBrokersOrder(t *testing.T) {
	acl := func(p, h, res, name, pat, op, perm string) aclRule {
		return aclRule{Principal: p, Host: h, Resource: res, Name: name, Pattern: pat, Operation: op, Permission: perm}
	}
	ask := func(p, op, res, name, host string) aclQuestion {
		return aclQuestion{Principal: p, Operation: op, Resource: res, Name: name, Host: host}
	}
	decided := func(a aclAnswer) string {
		for _, s := range a.Steps {
			if s.Decided {
				return s.Ask
			}
		}
		return ""
	}
	rules := aclRules{
		SuperUsers: []string{"User:root"},
		ACLs: []aclRule{
			acl("User:alice", "*", "topic", "orders", "literal", "read", "allow"),
			acl("User:alice", "*", "topic", "orders", "literal", "write", "deny"),
			acl("User:*", "*", "topic", "pub", "prefixed", "all", "allow"),
			acl("User:bob", "10.0.0.9", "topic", "pub-secret", "literal", "all", "deny"),
			acl("User:carol", "*", "cluster", "kafka-cluster", "literal", "alter configs", "allow"),
			acl("User:dave", "*", "topic", "orders", "literal", "all", "allow"),
			acl("User:dave", "*", "topic", "orders", "literal", "delete", "deny"),
		},
	}
	for name, tc := range map[string]struct {
		q       aclQuestion
		allowed bool
		by      int // index of the ACL that decided, or -1.
		applies bool
		step    string
		steps   int
	}{
		"a super user":          {ask("User:root", "write", "topic", "orders", "h"), true, -1, true, "Super user?", 4},
		"an allow":              {ask("User:alice", "read", "topic", "orders", "h"), true, 0, true, "A matching ALLOW?", 4},
		"describe through read": {ask("User:alice", "describe", "topic", "orders", "h"), true, 0, true, "A matching ALLOW?", 4},
		"a deny":                {ask("User:alice", "write", "topic", "orders", "h"), false, 1, true, "A matching DENY?", 4},
		"nothing allows it, but an ACL is about it": {ask("User:alice", "delete", "topic", "orders", "h"), false, -1, true, "The default?", 4},
		"a prefix for anyone":                       {ask("User:zed", "write", "topic", "pub-1", "h"), true, 2, true, "A matching ALLOW?", 4},
		"a deny beats an allow, on its host":        {ask("User:bob", "read", "topic", "pub-secret", "10.0.0.9"), false, 3, true, "A matching DENY?", 4},
		"the deny is for one host only":             {ask("User:bob", "read", "topic", "pub-secret", "10.0.0.8"), true, 2, true, "A matching ALLOW?", 4},
		"the cluster has the one name":              {ask("User:carol", "alter configs", "cluster", "ignored", "h"), true, 4, true, "A matching ALLOW?", 4},
		"alter configs allows describe configs":     {ask("User:carol", "describe configs", "cluster", "", "h"), true, 4, true, "A matching ALLOW?", 4},
		"all is denied by a deny of one operation":  {ask("User:dave", "delete", "topic", "orders", "h"), false, 6, true, "A matching DENY?", 4},
		"all allows the rest":                       {ask("User:dave", "write", "topic", "orders", "h"), true, 5, true, "A matching ALLOW?", 4},
		"no ACL about it: the default refuses":      {ask("User:alice", "read", "topic", "unlisted", "h"), false, -1, false, "The default?", 4},
		"a group with no ACL":                       {ask("User:alice", "read", "group", "orders", "h"), false, -1, false, "The default?", 4},
	} {
		ans := rules.check(tc.q)
		by := -1
		for i := range rules.ACLs {
			if ans.By == &rules.ACLs[i] {
				by = i
			}
		}
		if ans.Allowed != tc.allowed || by != tc.by || ans.Applies != tc.applies || decided(ans) != tc.step || len(ans.Steps) != tc.steps {
			t.Errorf("%s: allowed %v by %d applies %v decided %q in %d steps; want %v %d %v %q %d", name, ans.Allowed, by, ans.Applies, decided(ans), len(ans.Steps), tc.allowed, tc.by, tc.applies, tc.step, tc.steps)
		}
	}
	// The default decides when nothing is about the resource.
	rules.OpenByDefault = true
	ans := rules.check(ask("User:alice", "read", "topic", "unlisted", "h"))
	if !ans.Allowed || !strings.Contains(ans.Steps[3].Answer, "the default decides: allow") {
		t.Errorf("open by default: %+v", ans)
	}
	if ans = rules.check(ask("User:alice", "write", "topic", "orders", "h")); ans.Allowed {
		t.Error("the default overrides a deny")
	}
	rules.OpenByDefault = false
	if ans = rules.check(ask("User:alice", "read", "topic", "unlisted", "h")); !strings.Contains(ans.Steps[3].Answer, "the default decides: deny") {
		t.Errorf("%+v", ans.Steps)
	}
	// Super user wording.
	none := aclRules{}
	if a := none.check(ask("User:x", "read", "topic", "t", "h")); a.Steps[0].Answer != "No. There are none." || a.Steps[0].Decided {
		t.Errorf("no super users: %+v", a.Steps[0])
	}
	if a := rules.check(ask("User:nobody", "read", "topic", "t", "h")); a.Steps[0].Answer != "No." {
		t.Errorf("not a super user: %q", a.Steps[0].Answer)
	}
	if a := rules.check(ask("User:root", "read", "topic", "t", "h")); !strings.HasPrefix(a.Steps[0].Answer, "Yes.") || !a.Steps[0].Decided || a.By != nil || !a.Allowed {
		t.Errorf("a super user: %+v", a.Steps[0])
	}
}

func TestACLWordsAndCodes(t *testing.T) {
	ops := aclOperations()
	if len(ops) != 13 || ops[0] != "any" && ops[0] != "all" {
		t.Errorf("%d operations, first %q: %v", len(ops), ops[0], ops)
	}
	seen := map[string]bool{}
	for _, o := range ops {
		if seen[o] || strings.ContainsAny(o, "_ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
			t.Errorf("operation %q", o)
		}
		seen[o] = true
		if code, ok := aclOperationCode(o); !ok || aclWord(code.String()) != o {
			t.Errorf("code of %q: %v %v", o, code, ok)
		}
	}
	for _, want := range []string{"all", "read", "write", "alter configs", "describe configs", "cluster action", "idempotent write"} {
		if !seen[want] {
			t.Errorf("no operation %q in %v", want, ops)
		}
	}
	if _, ok := aclOperationCode("any"); ok {
		t.Error("the wildcard operation is a concrete one")
	}
	if _, ok := aclOperationCode("nonsense"); ok {
		t.Error("nonsense")
	}
	for _, r := range aclResources {
		if code, ok := aclResourceCode(r); !ok || aclWord(code.String()) != r {
			t.Errorf("resource %q: %v %v", r, code, ok)
		}
	}
	if _, ok := aclResourceCode("any"); ok {
		t.Error("the wildcard resource")
	}
	if aclWord("TRANSACTIONAL_ID") != "transactional id" || aclWord("ALL") != "all" {
		t.Error("aclWord")
	}
	for r, want := range map[string]string{"topic": "TOPIC_AUTHORIZATION_FAILED", "group": "GROUP_AUTHORIZATION_FAILED", "cluster": "CLUSTER_AUTHORIZATION_FAILED", "transactional id": "TRANSACTIONAL_ID_AUTHORIZATION_FAILED", "x": ""} {
		if authorizationError(r) != want {
			t.Errorf("authorizationError(%q)", r)
		}
	}
}
