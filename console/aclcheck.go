package console

import (
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// The checker (Ac-Home's "Could a principal do this?", Ac-05). It answers whether a
// principal may do an operation to a resource from a host, without sending the
// request, by the rules the broker enforces (internal/broker/authorizer.go), read in
// the same order:
//
//  1. a super user is never refused;
//  2. a DENY that matches the principal, the host and the operation (or ALL) refuses;
//  3. otherwise an ALLOW that matches and whose operation grants this one permits
//     (ALL grants everything; Read, Write, Delete and Alter grant Describe;
//     AlterConfigs grants DescribeConfigs);
//  4. otherwise, if no ACL at all is about the resource, the broker's default decides
//     (allow.everyone.if.no.acl.found, set by --acl-default); else it is refused.
//
// An ACL is about a resource by its literal name, by a prefix of it, or by "*" (every
// resource of the type); the principal "User:*" is every user, the host "*" every
// address. The ACLs, the super users and the default are read from the broker over
// the wire (DescribeAcls, DescribeConfigs). TestTheCheckerAgreesWithTheBroker runs
// real requests as SASL users against a real broker and holds every answer of the
// checker to what the broker did.

// aclRule is one stored ACL, as DescribeAcls returns it, in the wire's words.
type aclRule struct {
	Principal, Host string
	// Resource is "topic", "group", "cluster" or "transactional id"; Pattern is
	// "literal" or "prefixed".
	Resource, Name, Pattern string
	// Operation is "read", "write", "all"…; Permission is "allow" or "deny".
	Operation, Permission string
}

// aclRules is what the broker enforces: its ACLs, its super users and its default.
type aclRules struct {
	ACLs          []aclRule
	SuperUsers    []string
	OpenByDefault bool
}

// aclQuestion is what the checker is asked.
type aclQuestion struct {
	Principal, Operation, Resource, Name, Host string
}

// aclStep is one rule read on the way to the answer: its question, its answer, and
// whether it was reached.
type aclStep struct {
	Ask, Answer string
	Reached     bool
	// Decided marks the step that gave the answer.
	Decided bool
}

// aclAnswer is the checker's answer.
type aclAnswer struct {
	Allowed bool
	// By is the ACL that decided, when one did (a DENY, or the ALLOW that permitted).
	By *aclRule
	// Applies says some ACL is about the resource (so the default does not decide).
	Applies bool
	Steps   []aclStep
}

// grants reports whether a stored operation grants the asked one.
func grants(stored, wanted string) bool {
	if stored == wanted || stored == "all" {
		return true
	}
	switch wanted {
	case "describe":
		return stored == "read" || stored == "write" || stored == "delete" || stored == "alter"
	case "describe configs":
		return stored == "alter configs"
	}
	return false
}

// about reports whether an ACL is about the named resource of the type.
func (a aclRule) about(resource, name string) bool {
	if a.Resource != resource {
		return false
	}
	if a.Pattern == "prefixed" {
		return strings.HasPrefix(name, a.Name)
	}
	return a.Name == name || a.Name == "*"
}

func (a aclRule) matches(principal, host string) bool {
	return (a.Principal == principal || a.Principal == "User:*") && (a.Host == "*" || a.Host == host)
}

// check answers q by r's rules.
func (r aclRules) check(q aclQuestion) aclAnswer {
	var ans aclAnswer
	name := q.Name
	if q.Resource == "cluster" {
		name = "kafka-cluster" // the one name the cluster resource has.
	}
	super := false
	for _, s := range r.SuperUsers {
		super = super || s == q.Principal
	}
	superAns := "No."
	if super {
		superAns = "Yes. A super user is never refused."
	} else if len(r.SuperUsers) == 0 {
		superAns = "No. There are none."
	}
	ans.Steps = append(ans.Steps, aclStep{Ask: "Super user?", Answer: superAns, Reached: true, Decided: super})
	for _, a := range r.ACLs {
		if a.about(q.Resource, name) {
			ans.Applies = true
		}
	}
	if super {
		ans.Allowed = true
		ans.Steps = append(ans.Steps,
			aclStep{Ask: "A matching DENY?", Answer: "Not reached."},
			aclStep{Ask: "A matching ALLOW?", Answer: "Not reached."},
			aclStep{Ask: "The default?", Answer: "Not reached."})
		return ans
	}
	for i, a := range r.ACLs {
		if a.Permission == "deny" && a.about(q.Resource, name) && a.matches(q.Principal, q.Host) && (a.Operation == q.Operation || a.Operation == "all") {
			ans.By = &r.ACLs[i]
			ans.Steps = append(ans.Steps,
				aclStep{Ask: "A matching DENY?", Answer: "Yes: the ACL above. It decides, and an ALLOW anywhere else could not change it.", Reached: true, Decided: true},
				aclStep{Ask: "A matching ALLOW?", Answer: "Not reached."},
				aclStep{Ask: "The default?", Answer: "Not reached."})
			return ans
		}
	}
	ans.Steps = append(ans.Steps, aclStep{Ask: "A matching DENY?", Answer: "No.", Reached: true})
	for i, a := range r.ACLs {
		if a.Permission == "allow" && a.about(q.Resource, name) && a.matches(q.Principal, q.Host) && grants(a.Operation, q.Operation) {
			ans.Allowed, ans.By = true, &r.ACLs[i]
			ans.Steps = append(ans.Steps,
				aclStep{Ask: "A matching ALLOW?", Answer: "Yes: the ACL above.", Reached: true, Decided: true},
				aclStep{Ask: "The default?", Answer: "Not reached."})
			return ans
		}
	}
	ans.Steps = append(ans.Steps, aclStep{Ask: "A matching ALLOW?", Answer: "No.", Reached: true})
	if ans.Applies {
		ans.Steps = append(ans.Steps, aclStep{Ask: "The default?", Answer: "Not reached; this " + q.Resource + " has an ACL, so the default does not apply to it. Nothing allows this, so it is refused.", Reached: true, Decided: true})
		return ans
	}
	ans.Allowed = r.OpenByDefault
	word := "deny"
	if r.OpenByDefault {
		word = "allow"
	}
	ans.Steps = append(ans.Steps, aclStep{Ask: "The default?", Answer: "No ACL is about this " + q.Resource + ", so the default decides: " + word + ".", Reached: true, Decided: true})
	return ans
}

// aclOperations are the operations a check or a new ACL may name: every concrete
// AclOperation of the wire protocol (kmsg.ACLOperation, 2 to 14), in its words. The
// broker stores any of them on any resource type, so the forms offer them all and
// the broker decides.
func aclOperations() []string {
	var out []string
	for op := kmsg.ACLOperation(2); op <= 14; op++ {
		out = append(out, aclWord(op.String()))
	}
	return out
}

// aclWord is a wire name as the pages write it: TRANSACTIONAL_ID, "transactional id".
func aclWord(s string) string { return strings.ToLower(strings.ReplaceAll(s, "_", " ")) }

// aclResources are the resource types a check or a new ACL may name: the ones the
// broker checks requests against (topic, group, cluster, transactional id), in that
// order.
var aclResources = []string{"topic", "group", "cluster", "transactional id"}

// aclOperationCode and aclResourceCode are a word's wire value, or false.
func aclOperationCode(word string) (kmsg.ACLOperation, bool) {
	for op := kmsg.ACLOperation(2); op <= 14; op++ {
		if aclWord(op.String()) == word {
			return op, true
		}
	}
	return 0, false
}

func aclResourceCode(word string) (kmsg.ACLResourceType, bool) {
	for rt := kmsg.ACLResourceType(2); rt <= 7; rt++ {
		if aclWord(rt.String()) == word {
			return rt, true
		}
	}
	return 0, false
}

// aclFieldNotes are the lines under the create form's fields (Ac-03). Each says how
// the broker reads that field, and each is held by TestTheCheckerAgreesWithTheBroker:
// a connection's principal is User:<its SASL user> (User:ANONYMOUS without a login)
// and User:* matches any; a host matches as written or as *; a prefixed name matches
// every name that starts with it; a DENY wins; ALL covers everything and the four
// operations that change or read a thing allow Describe.
var aclFieldNotes = map[string]string{
	"principal":  "Written Type:name. A client is User:<its SASL user>, or User:ANONYMOUS with no login. User:* means any.",
	"host":       "As written, or * for any. No ranges.",
	"resource":   "Topic, group, the cluster, or a transactional id.",
	"name":       "The resource's name, or * for every one of its type. The cluster's is kafka-cluster.",
	"pattern":    "Literal is the name as written; prefixed is every name that starts with it.",
	"operation":  "All covers every operation. Read, Write, Delete and Alter also allow Describe; AlterConfigs allows DescribeConfigs.",
	"permission": "A DENY that matches wins over any ALLOW.",
}

// authorizationError is the error a request refused on this resource type is
// answered with: the protocol has one per type (error codes 29, 30, 31 and 53).
func authorizationError(resource string) string {
	switch resource {
	case "topic":
		return "TOPIC_AUTHORIZATION_FAILED"
	case "group":
		return "GROUP_AUTHORIZATION_FAILED"
	case "cluster":
		return "CLUSTER_AUTHORIZATION_FAILED"
	case "transactional id":
		return "TRANSACTIONAL_ID_AUTHORIZATION_FAILED"
	}
	return ""
}
