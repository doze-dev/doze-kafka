package console

import (
	"context"
	"net/http"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The Access page lists the ACLs stored on the broker, read-only, under a banner that
// says what they are not: doze-kafka stores them so that tooling which provisions
// ACLs completes and can read them back, and enforces none of them. Every
// authenticated client can do everything.

// ACLRow is one stored entry, in words.
type ACLRow struct {
	Resource, Name, Pattern, Principal, Host, Operation, Permission string
}

// ListACLs reads every stored ACL, one row per entry, in the broker's order.
func (b *backend) ListACLs(ctx context.Context) ([]ACLRow, error) {
	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType = kmsg.ACLResourceTypeAny
	req.ResourcePatternType = kmsg.ACLResourcePatternTypeAny
	req.Operation = kmsg.ACLOperationAny
	req.PermissionType = kmsg.ACLPermissionTypeAny
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if e := kerr.ErrorForCode(resp.ErrorCode); e != nil {
		return nil, e
	}
	word := func(s string) string { return strings.ToLower(strings.ReplaceAll(s, "_", " ")) }
	var rows []ACLRow
	for _, r := range resp.Resources {
		for _, a := range r.ACLs {
			rows = append(rows, ACLRow{
				Resource: word(r.ResourceType.String()), Name: r.ResourceName, Pattern: word(r.ResourcePatternType.String()),
				Principal: a.Principal, Host: a.Host, Operation: word(a.Operation.String()), Permission: word(a.PermissionType.String()),
			})
		}
	}
	return rows, nil
}

func (c *Console) access(w http.ResponseWriter, r *http.Request) {
	rows, err := c.be.ListACLs(r.Context())
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.render(w, r, "access_detail", map[string]any{"ACLs": rows, "Page": "access", "Title": "Access"})
}
