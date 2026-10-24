package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// A filter that names a host matches the entries of that host, and only those.
func TestACLFiltersByHost(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const lit = kmsg.ACLResourcePatternTypeLiteral
	createACLs(t, ctx, cl,
		createACL(kmsg.ACLResourceTypeTopic, "t", lit, "User:a", "10.0.0.1", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow),
		createACL(kmsg.ACLResourceTypeTopic, "t", lit, "User:a", "10.0.0.2", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow),
		createACL(kmsg.ACLResourceTypeTopic, "t", lit, "User:a", "*", kmsg.ACLOperationWrite, kmsg.ACLPermissionTypeAllow),
	)
	hosts := func(host *string) []string {
		req := kmsg.NewPtrDescribeACLsRequest()
		req.ResourceType, req.ResourcePatternType = kmsg.ACLResourceTypeAny, kmsg.ACLResourcePatternTypeAny
		req.Operation, req.PermissionType = kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny
		req.Host = host
		resp, err := req.RequestWith(ctx, cl)
		if err != nil || resp.ErrorCode != 0 {
			t.Fatalf("DescribeAcls: %v %+v", err, resp)
		}
		var out []string
		for _, r := range resp.Resources {
			for _, a := range r.ACLs {
				out = append(out, a.Host)
			}
		}
		return out
	}
	if got := hosts(nil); len(got) != 3 {
		t.Errorf("no host filter: %v, want all three", got)
	}
	if got := hosts(sp("10.0.0.1")); len(got) != 1 || got[0] != "10.0.0.1" {
		t.Errorf("host 10.0.0.1: %v, want only that one", got)
	}
	if got := hosts(sp("10.0.0.9")); len(got) != 0 {
		t.Errorf("a host nothing is for: %v", got)
	}

	// Deleting by host removes only that host's entries.
	del := kmsg.NewPtrDeleteACLsRequest()
	f := kmsg.NewDeleteACLsRequestFilter()
	f.ResourceType, f.ResourcePatternType = kmsg.ACLResourceTypeAny, kmsg.ACLResourcePatternTypeAny
	f.Operation, f.PermissionType = kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny
	f.Host = sp("10.0.0.2")
	del.Filters = append(del.Filters, f)
	resp, err := del.RequestWith(ctx, cl)
	if err != nil || len(resp.Results) != 1 || resp.Results[0].ErrorCode != 0 || len(resp.Results[0].MatchingACLs) != 1 || resp.Results[0].MatchingACLs[0].Host != "10.0.0.2" {
		t.Fatalf("DeleteAcls by host: %v %+v", err, resp)
	}
	if got := hosts(nil); len(got) != 2 {
		t.Errorf("after deleting host 10.0.0.2's: %v, want the other two", got)
	}
}
