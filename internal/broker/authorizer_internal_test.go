package broker

import (
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

func acl(rt int8, name string, pt int8, principal, host string, op, perm int8) meta.ACL {
	return meta.ACL{ResourceType: rt, Name: name, PatternType: pt, Principal: principal, Host: host, Operation: op, Permission: perm}
}

func authz(def string, supers []string, acls ...meta.ACL) *authorizer {
	cfg := config.Default()
	cfg.ACLDefault, cfg.ACLSuperUsers = def, supers
	return newAuthorizer(cfg, acls)
}

// Kafka's decision rules, one case each: deny first, then allow, then what a resource
// with no ACL means.
func TestAuthorizerFollowsKafkasRules(t *testing.T) {
	const alice, bob, host = "User:alice", "User:bob", "10.0.0.5"
	orders := func(p string, op, perm int8) meta.ACL { return acl(resTopic, "orders", aclLiteral, p, "*", op, perm) }

	for _, tc := range []struct {
		name  string
		a     *authorizer
		who   string
		host  string
		rt    int8
		res   string
		op    int8
		allow bool
	}{
		{"no ACLs, default allow", authz("allow", nil), alice, host, resTopic, "orders", opWrite, true},
		{"no ACLs, default deny", authz("deny", nil), alice, host, resTopic, "orders", opWrite, false},
		{"an allow", authz("deny", nil, orders(alice, opWrite, permAllow)), alice, host, resTopic, "orders", opWrite, true},
		{"an allow is for its principal", authz("deny", nil, orders(alice, opWrite, permAllow)), bob, host, resTopic, "orders", opWrite, false},
		{"an allow is for its operation", authz("deny", nil, orders(alice, opWrite, permAllow)), alice, host, resTopic, "orders", opRead, false},
		{"a resource with ACLs refuses others even by default allow", authz("allow", nil, orders(alice, opWrite, permAllow)), bob, host, resTopic, "orders", opWrite, false},
		{"…but a resource with none follows the default", authz("allow", nil, orders(alice, opWrite, permAllow)), bob, host, resTopic, "other", opWrite, true},
		{"a deny beats an allow", authz("allow", nil, orders(alice, opWrite, permAllow), orders(alice, opWrite, permDeny)), alice, host, resTopic, "orders", opWrite, false},
		{"a deny of ALL denies everything", authz("allow", nil, orders(alice, opAll, permDeny)), alice, host, resTopic, "orders", opRead, false},
		{"ALL allows everything", authz("deny", nil, orders(alice, opAll, permAllow)), alice, host, resTopic, "orders", opAlter, true},
		{"Read implies Describe", authz("deny", nil, orders(alice, opRead, permAllow)), alice, host, resTopic, "orders", opDescribe, true},
		{"Write implies Describe", authz("deny", nil, orders(alice, opWrite, permAllow)), alice, host, resTopic, "orders", opDescribe, true},
		{"Describe does not imply Read", authz("deny", nil, orders(alice, opDescribe, permAllow)), alice, host, resTopic, "orders", opRead, false},
		{"AlterConfigs implies DescribeConfigs", authz("deny", nil, orders(alice, opAlterConfigs, permAllow)), alice, host, resTopic, "orders", opDescribeConfig, true},
		{"a deny of Read does not by itself deny Describe", authz("allow", nil, orders(alice, opRead, permDeny), orders(alice, opDescribe, permAllow)), alice, host, resTopic, "orders", opDescribe, true},
		{"User:* is every user", authz("deny", nil, orders("User:*", opRead, permAllow)), bob, host, resTopic, "orders", opRead, true},
		{"a host", authz("deny", nil, acl(resTopic, "orders", aclLiteral, alice, host, opRead, permAllow)), alice, host, resTopic, "orders", opRead, true},
		{"another host", authz("deny", nil, acl(resTopic, "orders", aclLiteral, alice, host, opRead, permAllow)), alice, "10.9.9.9", resTopic, "orders", opRead, false},
		{"a prefix covers what starts with it", authz("deny", nil, acl(resTopic, "ord", aclPrefix, alice, "*", opRead, permAllow)), alice, host, resTopic, "orders", opRead, true},
		{"a prefix does not cover the rest", authz("deny", nil, acl(resTopic, "ord", aclPrefix, alice, "*", opRead, permAllow)), alice, host, resTopic, "invoices", opRead, false},
		{"a literal is not a prefix", authz("deny", nil, acl(resTopic, "ord", aclLiteral, alice, "*", opRead, permAllow)), alice, host, resTopic, "orders", opRead, false},
		{"* covers every resource of the type", authz("deny", nil, acl(resTopic, "*", aclLiteral, alice, "*", opRead, permAllow)), alice, host, resTopic, "anything", opRead, true},
		{"a topic ACL is not a group ACL", authz("deny", nil, orders(alice, opRead, permAllow)), alice, host, resGroup, "orders", opRead, false},
		{"a super user is never refused", authz("deny", []string{alice}, orders(alice, opAll, permDeny)), alice, host, resTopic, "orders", opWrite, true},
		{"the cluster", authz("deny", nil, acl(resCluster, clusterResourceName, aclLiteral, alice, "*", opCreate, permAllow)), alice, host, resCluster, clusterResourceName, opCreate, true},
	} {
		if got := tc.a.allowed(tc.who, tc.host, tc.rt, tc.res, tc.op); got != tc.allow {
			t.Errorf("%s: allowed = %v, want %v", tc.name, got, tc.allow)
		}
	}
}

// Producing idempotently is the right to write somewhere when the cluster has been
// restricted, and an explicit deny of it still stops it.
func TestIdempotentWriteIsImpliedByWritingSomewhere(t *testing.T) {
	const alice = "User:alice"
	restricted := acl(resCluster, clusterResourceName, aclLiteral, "User:ops", "*", opAlter, permAllow) // the cluster now has ACLs.
	a := authz("allow", nil, restricted, acl(resTopic, "orders", aclLiteral, alice, "*", opWrite, permAllow))
	b := &Broker{authz: a}
	if b.canWriteIdempotently(nil) {
		t.Error("an anonymous connection may produce idempotently on a restricted cluster")
	}
	if !a.writesSomewhere(alice, "h") || a.writesSomewhere("User:bob", "h") {
		t.Error("writesSomewhere does not tell a writer from a non-writer")
	}
	a2 := authz("allow", nil, restricted, acl(resTopic, "orders", aclLiteral, alice, "*", opWrite, permAllow),
		acl(resCluster, clusterResourceName, aclLiteral, alice, "*", opIdempotent, permDeny))
	if !a2.deniedOnCluster(alice, "h", opIdempotent) {
		t.Error("an explicit deny of IdempotentWrite is not seen")
	}
}
