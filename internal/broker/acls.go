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

// The ACL APIs (DescribeAcls, CreateAcls, DeleteAcls) are served so that provisioning
// scripts and Terraform complete and can read back what they created. The entries are
// stored, durably, and NEVER ENFORCED: there is no authorizer, and every authenticated
// client may do everything whatever is stored here. That is said at start-up when any
// exist, in docs/limitations.md, and on the console's Access page, so a stored entry is
// not mistaken for security.

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

// validateFilter says why a filter cannot be used, or "" if it can.
func (f aclFilter) validate() string {
	switch {
	case !known(f.resourceType, 7):
		return "the resource type is not one this broker knows"
	case !known(f.patternType, 4):
		return "the resource pattern type is not one this broker knows"
	case !known(f.operation, 14):
		return "the operation is not one this broker knows"
	case !known(f.permission, 3):
		return "the permission type is not one this broker knows"
	}
	return ""
}

// matches is Kafka's AclBindingFilter.matches.
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

// validateCreation says why an entry cannot be stored, or "" if it can: a stored
// entry is concrete (no "any", no "match"), and its principal says what kind it is.
func validateCreation(a meta.ACL) string {
	switch {
	case !known(a.ResourceType, 7) || a.ResourceType == aclAny:
		return "Invalid resource type"
	case a.PatternType != aclLiteral && a.PatternType != aclPrefix:
		return "Invalid resource pattern type: an entry is literal or prefixed"
	case !known(a.Operation, 14) || a.Operation == aclAny:
		return "Invalid operation"
	case a.Permission != 2 && a.Permission != 3:
		return "Invalid permission type: an entry allows or denies"
	case a.Name == "":
		return "Invalid resource name: it is empty"
	case !strings.Contains(a.Principal, ":") || strings.HasPrefix(a.Principal, ":"):
		return "Invalid principal: it is written Type:name, such as User:alice"
	case a.Host == "":
		return "Invalid host: it is empty (use * for any)"
	case a.ResourceType == 4 && a.Name != clusterResourceName:
		return "The only valid name for the CLUSTER resource is " + clusterResourceName
	}
	return ""
}

// aclState serialises changes to the stored entries, so a delete's matches and its
// removal are one step, and says the not-enforced fact once per process.
type aclState struct {
	mu     sync.Mutex
	warned sync.Once
}

// WarnIfACLs says at start-up that stored ACLs are not enforced, when there are any.
func (b *Broker) WarnIfACLs() {
	if acls, err := b.meta.ListACLs(); err == nil && len(acls) > 0 {
		b.warnACLsNotEnforced(len(acls))
	}
}

func (b *Broker) warnACLsNotEnforced(n int) {
	b.acls.warned.Do(func() {
		b.log.Warn("ACLs are stored but NOT enforced: every authenticated client can still do everything", "acls", n)
	})
}

func (b *Broker) handleCreateACLs(ctx context.Context, req *protocol.Request, cr *kmsg.CreateACLsRequest, resp *kmsg.CreateACLsResponse) error {
	b.acls.mu.Lock()
	defer b.acls.mu.Unlock()

	var accepted []meta.ACL
	valid := make([]bool, len(cr.Creations))
	for i, c := range cr.Creations {
		a := meta.ACL{
			ResourceType: int8(c.ResourceType), Name: c.ResourceName, PatternType: int8(c.ResourcePatternType),
			Principal: c.Principal, Host: c.Host, Operation: int8(c.Operation), Permission: int8(c.PermissionType),
		}
		rr := kmsg.NewCreateACLsResponseResult()
		if msg := validateCreation(a); msg != "" {
			rr.ErrorCode = kerr.InvalidRequest.Code
			rr.ErrorMessage = &msg
		} else {
			accepted = append(accepted, a)
			valid[i] = true
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
	if len(accepted) > 0 {
		all, _ := b.meta.ListACLs()
		b.warnACLsNotEnforced(len(all))
	}
	return nil
}

func filterFrom(rt kmsg.ACLResourceType, name *string, pt kmsg.ACLResourcePatternType, principal, host *string, op kmsg.ACLOperation, perm kmsg.ACLPermissionType) aclFilter {
	return aclFilter{int8(rt), name, int8(pt), principal, host, int8(op), int8(perm)}
}

func (b *Broker) handleDescribeACLs(ctx context.Context, req *protocol.Request, dr *kmsg.DescribeACLsRequest, resp *kmsg.DescribeACLsResponse) error {
	f := filterFrom(dr.ResourceType, dr.ResourceName, dr.ResourcePatternType, dr.Principal, dr.Host, dr.Operation, dr.PermissionType)
	if msg := f.validate(); msg != "" {
		resp.ErrorCode = kerr.InvalidRequest.Code
		resp.ErrorMessage = &msg
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
	b.acls.mu.Lock()
	defer b.acls.mu.Unlock()

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
	}
	return nil
}
