package console

import (
	"context"
	"net/http"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The Access page lists the ACLs stored on the broker, read-only, and says how they are
// applied: they are enforced, and a resource with no ACL is allowed or refused by the
// broker's default. They are managed through the Kafka API (kafka-acls, Terraform),
// not here.

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
	info, err := c.be.ClusterInfo(r.Context())
	if err != nil {
		c.fail(w, r, err)
		return
	}
	// What the broker reports about its authorizer, under Kafka's names.
	openByDefault, supers := true, ""
	for _, e := range info.BrokerConfig {
		switch e.Name {
		case "allow.everyone.if.no.acl.found":
			openByDefault = e.Value == "true"
		case "super.users":
			supers = e.Value
		}
	}
	// The principals the ACLs name, with how many each has and how many of them deny
	// (Ac-Home's list); the super users are listed beside them.
	type principalRow struct {
		Name          string
		Count, Denies int
	}
	var principals []principalRow
	at := map[string]int{}
	for _, a := range rows {
		i, ok := at[a.Principal]
		if !ok {
			at[a.Principal] = len(principals)
			principals = append(principals, principalRow{Name: a.Principal})
			i = len(principals) - 1
		}
		principals[i].Count++
		if a.Permission == "deny" {
			principals[i].Denies++
		}
	}
	var supersList []string
	for _, s := range strings.Split(supers, ";") {
		if s = strings.TrimSpace(s); s != "" {
			supersList = append(supersList, s)
		}
	}
	for _, s := range strings.Split(supers, ",") {
		if s = strings.TrimSpace(s); s != "" && len(supersList) == 0 {
			supersList = append(supersList, s)
		}
	}
	shown := rows
	selected := r.URL.Query().Get("principal")
	if selected != "" {
		shown = nil
		for _, a := range rows {
			if a.Principal == selected {
				shown = append(shown, a)
			}
		}
	}
	c.render(w, r, "access_detail", map[string]any{"ACLs": shown, "AllACLs": rows, "Principals": principals, "Principal": selected,
		"SuperUserList": supersList, "OpenByDefault": openByDefault, "SuperUsers": supers, "Page": "access", "Title": "Access"})
}
