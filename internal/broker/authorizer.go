package broker

import (
	"strings"
	"sync/atomic"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// The authorizer enforces the ACLs created through CreateAcls, with Kafka's rules.
//
// A request is checked as an operation on a resource by the connection's principal
// (the SASL user as "User:alice", or "User:ANONYMOUS" when nothing authenticated it)
// from the connection's address:
//
//   - a super user is never refused;
//   - a DENY that matches the principal, host and operation refuses it, before
//     anything else is considered;
//   - otherwise an ALLOW that matches permits it;
//   - otherwise, if no ACL at all applies to the resource, config.ACLDefault decides:
//     "allow" (the default: nothing changes until you create ACLs) or "deny" (Kafka's
//     own default);
//   - otherwise it is refused.
//
// ACLs apply to a resource by name (literal), by prefix, or to every resource of the
// type ("*"). The principal "User:*" is every user, the host "*" every address.
//
// With no ACLs and the default of allow, a check is one atomic load and a length test.

// Resource types and operations, as Kafka numbers them on the wire.
const (
	resTopic   int8 = 2
	resGroup   int8 = 3
	resCluster int8 = 4

	opAny            int8 = 1
	opAll            int8 = 2
	opRead           int8 = 3
	opWrite          int8 = 4
	opCreate         int8 = 5
	opDelete         int8 = 6
	opAlter          int8 = 7
	opDescribe       int8 = 8
	opDescribeConfig int8 = 10
	opAlterConfigs   int8 = 11
	opIdempotent     int8 = 12

	permDeny  int8 = 2
	permAllow int8 = 3
)

// authorizer holds the ACLs in memory. This process is the only writer of them (the
// store's file is locked to it), so the copy is coherent, and a request never reads
// the database.
type authorizer struct {
	acls       atomic.Pointer[[]meta.ACL]
	defaultAll bool
	supers     map[string]bool
}

func newAuthorizer(cfg config.Config, loaded []meta.ACL) *authorizer {
	a := &authorizer{defaultAll: cfg.ACLDefault != config.ACLDeny, supers: map[string]bool{}}
	for _, u := range cfg.ACLSuperUsers {
		a.supers[u] = true
	}
	a.set(loaded)
	return a
}

func (a *authorizer) set(acls []meta.ACL) { a.acls.Store(&acls) }

// implied reports whether a stored operation grants the requested one: ALL grants
// everything, and Describe (and DescribeConfigs) come with the operations that
// change or read what they describe.
func implied(stored, wanted int8) bool {
	if stored == wanted || stored == opAll {
		return true
	}
	switch wanted {
	case opDescribe:
		return stored == opRead || stored == opWrite || stored == opDelete || stored == opAlter
	case opDescribeConfig:
		return stored == opAlterConfigs
	}
	return false
}

// covers reports whether an ACL's resource pattern applies to the named resource.
func covers(a meta.ACL, rt int8, name string) bool {
	if a.ResourceType != rt {
		return false
	}
	switch a.PatternType {
	case aclPrefix:
		return strings.HasPrefix(name, a.Name)
	default:
		return a.Name == name || a.Name == "*"
	}
}

func principalMatches(stored, principal string) bool {
	return stored == principal || stored == "User:*"
}

func hostMatches(stored, host string) bool { return stored == "*" || stored == host }

// allowed is the decision for one operation on one resource.
func (a *authorizer) allowed(principal, host string, rt int8, name string, op int8) bool {
	if a.supers[principal] {
		return true
	}
	acls := *a.acls.Load()
	if len(acls) == 0 {
		return a.defaultAll
	}
	applies := false
	var allowed bool
	for _, acl := range acls {
		if !covers(acl, rt, name) {
			continue
		}
		applies = true
		if !principalMatches(acl.Principal, principal) || !hostMatches(acl.Host, host) {
			continue
		}
		// A deny is exact: DENY Read does not deny Describe on its own account, and
		// ALL denies everything.
		if acl.Permission == permDeny && (acl.Operation == op || acl.Operation == opAll) {
			return false
		}
		if acl.Permission == permAllow && implied(acl.Operation, op) {
			allowed = true
		}
	}
	if allowed {
		return true
	}
	if !applies {
		return a.defaultAll
	}
	return false
}

// writesSomewhere reports whether the principal has been allowed to write to any
// topic, which Kafka accepts as the right to produce idempotently when no one has
// granted IdempotentWrite on the cluster (claim: api V8).
func (a *authorizer) writesSomewhere(principal, host string) bool {
	for _, acl := range *a.acls.Load() {
		if acl.ResourceType == resTopic && acl.Permission == permAllow && implied(acl.Operation, opWrite) &&
			principalMatches(acl.Principal, principal) && hostMatches(acl.Host, host) {
			return true
		}
	}
	return false
}

// can is the check handlers call: may this connection do op to the named resource?
func (b *Broker) can(conn *protocol.Conn, rt int8, name string, op int8) bool {
	return b.authz.allowed(conn.Principal(), conn.RemoteIP(), rt, name, op)
}

func (b *Broker) canTopic(conn *protocol.Conn, topic string, op int8) bool {
	return b.can(conn, resTopic, topic, op)
}
func (b *Broker) canGroup(conn *protocol.Conn, group string, op int8) bool {
	return b.can(conn, resGroup, group, op)
}
func (b *Broker) canCluster(conn *protocol.Conn, op int8) bool {
	return b.can(conn, resCluster, clusterResourceName, op)
}

// canWriteIdempotently is the right to obtain a producer id.
func (b *Broker) canWriteIdempotently(conn *protocol.Conn) bool {
	if b.canCluster(conn, opIdempotent) {
		return true
	}
	// With the cluster restricted, a principal that may write to some topic may
	// still produce idempotently (Kafka 2.8 and later), as the clients enable it.
	return b.authz.writesSomewhere(conn.Principal(), conn.RemoteIP()) && !b.authz.deniedOnCluster(conn.Principal(), conn.RemoteIP(), opIdempotent)
}

// deniedOnCluster is an explicit DENY of op on the cluster for the principal.
func (a *authorizer) deniedOnCluster(principal, host string, op int8) bool {
	for _, acl := range *a.acls.Load() {
		if covers(acl, resCluster, clusterResourceName) && acl.Permission == permDeny &&
			(acl.Operation == op || acl.Operation == opAll) && principalMatches(acl.Principal, principal) && hostMatches(acl.Host, host) {
			return true
		}
	}
	return false
}

// canCreateTopic is the right to create the named topic: Create on the cluster, or on
// the topic itself.
func (b *Broker) canCreateTopic(conn *protocol.Conn, topic string) bool {
	return b.canCluster(conn, opCreate) || b.canTopic(conn, topic, opCreate)
}
