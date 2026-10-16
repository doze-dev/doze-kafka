package broker

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The ACL APIs (DescribeAcls, CreateAcls, DeleteAcls) store entries durably, and the
// authorizer (authorizer.go) enforces them: an entry created here is what the next
// request is checked against. Creating and deleting them needs Alter on the cluster,
// and reading them Describe, as in Kafka.

// aclFilter is a describe or delete filter: nil strings and the "any" values match
// everything, as in Kafka's AclBindingFilter.
type aclFilter struct {
	resourceType int8
	name         *string
	patternType  int8
	principal    *string
	host         *string
	operation    int8
	permission   int8
}

const (
	aclAny     = 1 // ANY, for every one of the four enumerations.
	aclMatch   = 2 // pattern type MATCH.
	aclLiteral = 3
	aclPrefix  = 4

	clusterResourceName = "kafka-cluster" // the one name the CLUSTER resource has.
)

// concrete reports whether v is a real value of its enumeration (1 is "any", 0 is
// unknown), given how many values it has.
func known(v int8, max int8) bool { return v >= 1 && v <= max }

// unknownCode reports whether any of the codes is 0, UNKNOWN in each of the four
// enumerations. Kafka refuses such a request while reading it and closes the
// connection, for every ACL API (claim: api V5).
func unknownCode(codes ...int8) bool {
	for _, c := range codes {
		if c == 0 {
			return true
		}
	}
	return false
}

// errUnknownACLCode is what a handler returns for a request unknownCode refuses.
var errUnknownACLCode = &protocol.CloseError{Reason: "an ACL request names the UNKNOWN (0) resource type, pattern type, operation or permission"}

// validate says why a filter matches nothing Kafka can name, in Kafka's words, or
// "" if it can be used: a resource type or pattern type that is no code makes
// the pattern filter unknown, an operation or permission the entry filter
// (claim: api V5).
//
//fuzz:parser class=acl-filters FuzzACLFilterMatches
func (f aclFilter) validate() string {
	switch {
	case !known(f.resourceType, 7) || !known(f.patternType, 4):
		return "Unknown patternFilter."
	case !known(f.operation, 14) || !known(f.permission, 3):
		return "Unknown entryFilter."
	}
	return ""
}

// matches is Kafka's AclBindingFilter.matches.
//
//fuzz:parser class=acl-filters FuzzACLFilterMatches
func (f aclFilter) matches(a meta.ACL) bool {
	if f.resourceType != aclAny && f.resourceType != a.ResourceType {
		return false
	}
	if f.patternType != aclAny && f.patternType != aclMatch && f.patternType != a.PatternType {
		return false
	}
	if f.name != nil {
		name := *f.name
		switch {
		case f.patternType == aclMatch && a.PatternType == aclLiteral:
			if name != a.Name && a.Name != "*" {
				return false
			}
		case f.patternType == aclMatch && a.PatternType == aclPrefix:
			if !strings.HasPrefix(name, a.Name) {
				return false
			}
		default:
			if name != a.Name {
				return false
			}
		}
	}
	if f.principal != nil && *f.principal != a.Principal {
		return false
	}
	if f.host != nil && *f.host != a.Host {
		return false
	}
	return (f.operation == aclAny || f.operation == a.Operation) &&
		(f.permission == aclAny || f.permission == a.Permission)
}

// validateCreation says why an entry cannot be stored, or "" if it can, and the
// code to answer with. A code that is no value of its enumeration is
// INVALID_REQUEST in Kafka's words; ANY, or the MATCH pattern, is a value but
// not one an entry can have, and Kafka answers UNKNOWN_SERVER_ERROR with no
// message (claim: api V5). The rest are this broker's own checks.
//
//fuzz:parser FuzzACLFilterMatches
func validateCreation(a meta.ACL) (int16, string) {
	invalid := func(what string) (int16, string) {
		return kerr.InvalidRequest.Code, "Invalid " + what + " UNKNOWN"
	}
	switch {
	case !known(a.ResourceType, 7):
		return invalid("resourceType")
	case !known(a.PatternType, 4):
		return invalid("patternType")
	case !known(a.Operation, 14):
		return invalid("operation")
	case !known(a.Permission, 3):
		return invalid("permissionType")
	case a.ResourceType == aclAny || a.PatternType == aclAny || a.PatternType == aclMatch ||
		a.Operation == aclAny || a.Permission == aclAny:
		return kerr.UnknownServerError.Code, ""
	}
	msg := ""
	switch {
	case strings.ContainsRune(a.Name+a.Principal+a.Host, 0):
		// The store identifies an entry by its fields joined with NUL.
		msg = "Invalid entry: a NUL byte in the name, principal or host"
	case a.Name == "":
		msg = "Invalid resource name: it is empty"
	case !strings.Contains(a.Principal, ":") || strings.HasPrefix(a.Principal, ":"):
		msg = "Invalid principal: it is written Type:name, such as User:alice"
	case a.Host == "":
		msg = "Invalid host: it is empty (use * for any)"
	case a.ResourceType == 4 && a.Name != clusterResourceName:
		msg = "The only valid name for the CLUSTER resource is " + clusterResourceName
	}
	if msg == "" {
		return 0, ""
	}
	return kerr.InvalidRequest.Code, msg
}

// aclState serialises changes to the stored entries, so a delete's matches and its
// removal are one step.
type aclState struct {
	mu sync.Mutex
}

// refreshAuthorizer makes the authorizer see what is stored now. Callers hold acls.mu.
func (b *Broker) refreshAuthorizer() {
	if all, err := b.meta.ListACLs(); err == nil {
		b.authz.set(all)
	}
}

// AnnounceACLs says at start-up that ACLs are in force, when there are any or when
// the default is to refuse, so nobody wonders why a client is being turned away.
func (b *Broker) AnnounceACLs() {
	acls, err := b.meta.ListACLs()
	if err != nil {
		return
	}
	if len(acls) == 0 && b.authz.defaultAll {
		return
	}
	b.log.Info("ACLs are enforced", "acls", len(acls), "no_acl_means", b.cfg.ACLDefault, "super_users", len(b.authz.supers))
}

func (b *Broker) handleCreateACLs(ctx context.Context, req *protocol.Request, cr *kmsg.CreateACLsRequest, resp *kmsg.CreateACLsResponse) error {
	b.acls.mu.Lock()
	defer b.acls.mu.Unlock()

	for _, c := range cr.Creations {
		if unknownCode(int8(c.ResourceType), int8(c.ResourcePatternType), int8(c.Operation), int8(c.PermissionType)) {
			return errUnknownACLCode
		}
	}
	if !b.canCluster(req.Conn, opAlter) {
		for range cr.Creations {
			rr := kmsg.NewCreateACLsResponseResult()
			rr.ErrorCode = kerr.ClusterAuthorizationFailed.Code
			resp.Results = append(resp.Results, rr)
		}
		return nil
	}
	var accepted []meta.ACL
	valid := make([]bool, len(cr.Creations))
	for i, c := range cr.Creations {
		a := meta.ACL{
			ResourceType: int8(c.ResourceType), Name: c.ResourceName, PatternType: int8(c.ResourcePatternType),
			Principal: c.Principal, Host: c.Host, Operation: int8(c.Operation), Permission: int8(c.PermissionType),
		}
		rr := kmsg.NewCreateACLsResponseResult()
		if code, msg := validateCreation(a); code != 0 {
			rr.ErrorCode = code
			if msg != "" {
				rr.ErrorMessage = &msg
			}
		} else {
			accepted = append(accepted, a)
			valid[i] = true
			// An empty message, not a null one, as Kafka sends with an ACL it
			// created (claim: auth A1).
			rr.ErrorMessage = new(string)
		}
		resp.Results = append(resp.Results, rr)
	}
	if err := b.meta.AddACLs(accepted); err != nil {
		for i := range resp.Results {
			if valid[i] {
				resp.Results[i].ErrorCode = b.storageFault("create acls", err)
			}
		}
		return nil
	}
	b.refreshAuthorizer()
	return nil
}

func filterFrom(rt kmsg.ACLResourceType, name *string, pt kmsg.ACLResourcePatternType, principal, host *string, op kmsg.ACLOperation, perm kmsg.ACLPermissionType) aclFilter {
	return aclFilter{int8(rt), name, int8(pt), principal, host, int8(op), int8(perm)}
}

func (b *Broker) handleDescribeACLs(ctx context.Context, req *protocol.Request, dr *kmsg.DescribeACLsRequest, resp *kmsg.DescribeACLsResponse) error {
	if unknownCode(int8(dr.ResourceType), int8(dr.ResourcePatternType), int8(dr.Operation), int8(dr.PermissionType)) {
		return errUnknownACLCode
	}
	// An empty message, not a null one, as Kafka sends (claim: api V5).
	resp.ErrorMessage = new(string)
	if !b.canCluster(req.Conn, opDescribe) {
		resp.ErrorCode = kerr.ClusterAuthorizationFailed.Code
		return nil
	}
	f := filterFrom(dr.ResourceType, dr.ResourceName, dr.ResourcePatternType, dr.Principal, dr.Host, dr.Operation, dr.PermissionType)
	if f.validate() != "" {
		// A filter with a code Kafka cannot name matches nothing: Kafka answers
		// 0 with no entries (claim: api V5).
		return nil
	}
	all, err := b.meta.ListACLs()
	if err != nil {
		resp.ErrorCode = b.storageFault("describe acls", err)
		return nil
	}
	type resKey struct {
		rt   int8
		name string
		pt   int8
	}
	byRes := map[resKey][]meta.ACL{}
	var order []resKey
	for _, a := range all {
		if !f.matches(a) {
			continue
		}
		k := resKey{a.ResourceType, a.Name, a.PatternType}
		if _, ok := byRes[k]; !ok {
			order = append(order, k)
		}
		byRes[k] = append(byRes[k], a)
	}
	sort.Slice(order, func(i, j int) bool {
		a, b := order[i], order[j]
		if a.rt != b.rt {
			return a.rt < b.rt
		}
		if a.name != b.name {
			return a.name < b.name
		}
		return a.pt < b.pt
	})
	for _, k := range order {
		rr := kmsg.NewDescribeACLsResponseResource()
		rr.ResourceType, rr.ResourceName, rr.ResourcePatternType = kmsg.ACLResourceType(k.rt), k.name, kmsg.ACLResourcePatternType(k.pt)
		for _, a := range byRes[k] {
			ra := kmsg.NewDescribeACLsResponseResourceACL()
			ra.Principal, ra.Host, ra.Operation, ra.PermissionType = a.Principal, a.Host, kmsg.ACLOperation(a.Operation), kmsg.ACLPermissionType(a.Permission)
			rr.ACLs = append(rr.ACLs, ra)
		}
		resp.Resources = append(resp.Resources, rr)
	}
	return nil
}

func (b *Broker) handleDeleteACLs(ctx context.Context, req *protocol.Request, dr *kmsg.DeleteACLsRequest, resp *kmsg.DeleteACLsResponse) error {
	for _, f := range dr.Filters {
		if unknownCode(int8(f.ResourceType), int8(f.ResourcePatternType), int8(f.Operation), int8(f.PermissionType)) {
			return errUnknownACLCode
		}
	}
	b.acls.mu.Lock()
	defer b.acls.mu.Unlock()

	if !b.canCluster(req.Conn, opAlter) {
		for range dr.Filters {
			rr := kmsg.NewDeleteACLsResponseResult()
			rr.ErrorCode = kerr.ClusterAuthorizationFailed.Code
			resp.Results = append(resp.Results, rr)
		}
		return nil
	}
	all, err := b.meta.ListACLs()
	if err != nil {
		for range dr.Filters {
			rr := kmsg.NewDeleteACLsResponseResult()
			rr.ErrorCode = b.storageFault("delete acls", err)
			resp.Results = append(resp.Results, rr)
		}
		return nil
	}
	var doomed []meta.ACL
	for _, fr := range dr.Filters {
		rr := kmsg.NewDeleteACLsResponseResult()
		f := filterFrom(fr.ResourceType, fr.ResourceName, fr.ResourcePatternType, fr.Principal, fr.Host, fr.Operation, fr.PermissionType)
		if msg := f.validate(); msg != "" {
			rr.ErrorCode = kerr.InvalidRequest.Code
			rr.ErrorMessage = &msg
			resp.Results = append(resp.Results, rr)
			continue
		}
		for _, a := range all {
			if !f.matches(a) {
				continue
			}
			doomed = append(doomed, a)
			m := kmsg.NewDeleteACLsResponseResultMatchingACL()
			m.ResourceType, m.ResourceName, m.ResourcePatternType = kmsg.ACLResourceType(a.ResourceType), a.Name, kmsg.ACLResourcePatternType(a.PatternType)
			m.Principal, m.Host, m.Operation, m.PermissionType = a.Principal, a.Host, kmsg.ACLOperation(a.Operation), kmsg.ACLPermissionType(a.Permission)
			rr.MatchingACLs = append(rr.MatchingACLs, m)
		}
		resp.Results = append(resp.Results, rr)
	}
	if err := b.meta.RemoveACLs(doomed); err != nil {
		code := b.storageFault("delete acls", err)
		for i := range resp.Results {
			if resp.Results[i].ErrorCode == 0 {
				resp.Results[i].ErrorCode, resp.Results[i].MatchingACLs = code, nil
			}
		}
		return nil
	}
	b.refreshAuthorizer()
	return nil
}
