package console

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Access (Ac-Home, Ac-03, Ac-05): the ACLs stored on the broker and the rule that
// applies where there is none; a form that creates one (CreateAcls, the call
// kafka-acls.sh --add makes); a checker that answers whether a principal could do
// something, by the broker's rules (aclcheck.go); what an ACL lets through or refuses
// (the inspector); and the wire of ACL requests and refusals under the page.

// ACLRow is one stored entry, in words.
type ACLRow struct {
	Resource, Name, Pattern, Principal, Host, Operation, Permission string
}

// Key is the row's inspector key: every field, for an ACL is all of them.
func (a ACLRow) Key() string {
	return joinKey(a.Principal, a.Resource, a.Name, a.Pattern, a.Operation, a.Permission, a.Host)
}

func (a ACLRow) rule() aclRule {
	return aclRule{Principal: a.Principal, Host: a.Host, Resource: a.Resource, Name: a.Name, Pattern: a.Pattern, Operation: a.Operation, Permission: a.Permission}
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
		return nil, &kafkaRefusal{Err: e.(*kerr.Error), Msg: derefString(resp.ErrorMessage)}
	}
	var rows []ACLRow
	for _, r := range resp.Resources {
		for _, a := range r.ACLs {
			rows = append(rows, ACLRow{
				Resource: aclWord(r.ResourceType.String()), Name: r.ResourceName, Pattern: aclWord(r.ResourcePatternType.String()),
				Principal: a.Principal, Host: a.Host, Operation: aclWord(a.Operation.String()), Permission: aclWord(a.PermissionType.String()),
			})
		}
	}
	return rows, nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// CreateACL stores one entry (CreateAcls).
func (b *backend) CreateACL(ctx context.Context, a ACLRow) error {
	rt, ok := aclResourceCode(a.Resource)
	if !ok {
		return badInput("the resource type is one of topic, group, cluster or transactional id")
	}
	op, ok := aclOperationCode(a.Operation)
	if !ok {
		return badInput("the operation is one of %s", strings.Join(aclOperations(), ", "))
	}
	c := kmsg.NewCreateACLsRequestCreation()
	c.ResourceType, c.ResourceName = rt, a.Name
	c.ResourcePatternType = kmsg.ACLResourcePatternTypeLiteral
	if a.Pattern == "prefixed" {
		c.ResourcePatternType = kmsg.ACLResourcePatternTypePrefixed
	}
	c.Principal, c.Host, c.Operation = a.Principal, a.Host, op
	c.PermissionType = kmsg.ACLPermissionTypeAllow
	if a.Permission == "deny" {
		c.PermissionType = kmsg.ACLPermissionTypeDeny
	}
	req := kmsg.NewPtrCreateACLsRequest()
	req.Creations = []kmsg.CreateACLsRequestCreation{c}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return err
	}
	if len(resp.Results) != 1 {
		return fmt.Errorf("the broker answered %d results for one ACL", len(resp.Results))
	}
	if e := kerr.ErrorForCode(resp.Results[0].ErrorCode); e != nil {
		return &kafkaRefusal{Err: e.(*kerr.Error), Msg: derefString(resp.Results[0].ErrorMessage)}
	}
	return nil
}

// aclRules reads what the broker enforces: its ACLs (DescribeAcls), its super users
// and its default (super.users and allow.everyone.if.no.acl.found, DescribeConfigs).
func (c *Console) aclRules(ctx context.Context) (aclRules, error) {
	rows, err := c.be.ListACLs(ctx)
	if err != nil {
		return aclRules{}, err
	}
	info, err := c.be.ClusterInfo(ctx)
	if err != nil {
		return aclRules{}, err
	}
	r := aclRules{OpenByDefault: true}
	for _, a := range rows {
		r.ACLs = append(r.ACLs, a.rule())
	}
	for _, e := range info.BrokerConfig {
		switch e.Name {
		case "allow.everyone.if.no.acl.found":
			r.OpenByDefault = e.Value == "true"
		case "super.users":
			for _, s := range strings.FieldsFunc(e.Value, func(r rune) bool { return r == ';' || r == ',' }) {
				if s = strings.TrimSpace(s); s != "" {
					r.SuperUsers = append(r.SuperUsers, s)
				}
			}
		}
	}
	return r, nil
}

// consolePrincipal is who the console's own requests come from.
func (c *Console) consolePrincipal() string {
	if u := c.be.opts.SASLUser; u != "" {
		return "User:" + u
	}
	return "User:ANONYMOUS"
}

// deniedWindow counts the requests an ACL refused (the denied class) that match,
// among those the recorder holds from the last hour, and says over what: "in the last
// hour" when the recorder holds all of that hour, else "since <the oldest it holds>".
func (c *Console) deniedWindow(now time.Time, match func(TrafficEvent) bool) (n int, over string, last *TrafficEvent) {
	if c.traffic == nil {
		return 0, "", nil
	}
	from := now.Add(-time.Hour)
	for _, e := range c.traffic.Events(TrafficFilter{ErrorsOnly: true}, 0) {
		if e.Class != "denied" || e.Time.Before(from) || !match(e) {
			continue
		}
		n += max(e.Count, 1)
		if last == nil {
			ev := e
			last = &ev
		}
	}
	held := c.traffic.Query(TrafficFilter{}, 1)
	switch {
	case held.Total == 0 || !held.Oldest.After(from) || held.Total < c.traffic.Capacity():
		over = "in the last hour"
	default:
		over = "since " + whenText(held.Oldest, StyleClock) + " (the oldest request kept)"
	}
	return n, over, last
}

// refusedBy says whether a refused request is one this ACL is about: the same
// principal (or every principal) on a resource the ACL names, refused with the error
// its type is answered with.
func refusedBy(a aclRule, e TrafficEvent) bool {
	if !(a.Principal == e.Principal || a.Principal == "User:*") || e.Error != authorizationError(a.Resource) {
		return false
	}
	switch a.Resource {
	case "topic":
		for _, t := range e.Topics {
			if a.about("topic", t) {
				return true
			}
		}
		return false
	case "group":
		return a.about("group", e.Group)
	case "transactional id":
		return a.about("transactional id", e.TxnID)
	}
	return true // the cluster: there is one.
}

// principalRow is one line of the Principals list.
type principalRow struct {
	Name          string
	Count, Denies int
	Super         bool
}

func (c *Console) access(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rows, err := c.be.ListACLs(ctx)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	rules, err := c.aclRules(ctx)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	// The principals the ACLs name, with how many each has and how many of them deny;
	// User:ANONYMOUS, which every client is without a login; and the super users.
	var principals []principalRow
	at := map[string]int{}
	add := func(name string) int {
		if i, ok := at[name]; ok {
			return i
		}
		at[name] = len(principals)
		principals = append(principals, principalRow{Name: name})
		return len(principals) - 1
	}
	for _, a := range rows {
		i := add(a.Principal)
		principals[i].Count++
		if a.Permission == "deny" {
			principals[i].Denies++
		}
	}
	add("User:ANONYMOUS")
	for _, s := range rules.SuperUsers {
		principals[add(s)].Super = true
	}
	shown := rows
	selected := r.URL.Query().Get("principal")
	if selected != "" {
		shown = nil
		for _, a := range rows {
			if a.Principal == selected || (a.Principal == "User:*" && selected != "") {
				shown = append(shown, a)
			}
		}
	}
	now := time.Now()
	refused, over, _ := c.deniedWindow(now, func(TrafficEvent) bool { return true })
	data := map[string]any{"ACLs": shown, "AllACLs": rows, "Principals": principals, "Principal": selected,
		"SuperUserList": rules.SuperUsers, "OpenByDefault": rules.OpenByDefault, "Page": "access", "Title": "Access",
		"HasTraffic": c.traffic != nil, "Refused": refused, "RefusedOver": over,
		"Check": c.checkPrefill(rows), "Operations": aclOperations(), "Resources": aclResources}
	for k, v := range c.dockData(DockFilter{ACL: true}) {
		data[k] = v
	}
	c.render(w, r, "access_detail", data)
}

// checkPrefill is what the checker card starts with: the first DENY, which is the
// question an ACL page is most often opened to answer, else the first ACL.
func (c *Console) checkPrefill(rows []ACLRow) aclQuestion {
	host := c.checkHost()
	for _, a := range rows {
		if a.Permission == "deny" {
			return aclQuestion{Principal: a.Principal, Operation: a.Operation, Resource: a.Resource, Name: a.Name, Host: host}
		}
	}
	if len(rows) > 0 {
		a := rows[0]
		return aclQuestion{Principal: a.Principal, Operation: a.Operation, Resource: a.Resource, Name: a.Name, Host: host}
	}
	return aclQuestion{Principal: "User:ANONYMOUS", Operation: "read", Resource: "topic", Host: host}
}

// checkHost is the address a check starts with: where the newest client connected
// from, or else the broker's own host (a client on this machine comes from there).
func (c *Console) checkHost() string {
	if c.traffic != nil {
		if cl := c.traffic.Clients(); len(cl) > 0 {
			if h, _, err := net.SplitHostPort(cl[len(cl)-1].Remote); err == nil {
				return h
			}
		}
	}
	if h, _, err := net.SplitHostPort(c.be.displayAddr()); err == nil && h != "" {
		return h
	}
	return "*"
}

// accessCheck is Ac-05: the question, the answer, the ACL that decided, how the rules
// were read, and what would change it.
func (c *Console) accessCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rows, err := c.be.ListACLs(ctx)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	rules, err := c.aclRules(ctx)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	q := r.URL.Query()
	ask := aclQuestion{Principal: strings.TrimSpace(q.Get("principal")), Operation: q.Get("operation"), Resource: q.Get("resource"),
		Name: strings.TrimSpace(q.Get("name")), Host: strings.TrimSpace(q.Get("host"))}
	if ask.Host == "" {
		ask.Host = c.checkHost()
	}
	data := map[string]any{"Page": "access", "View": "access_check", "Title": "Check a request", "Check": ask, "Operations": aclOperations(), "Resources": aclResources,
		"AllACLs": rows, "SuperUserList": rules.SuperUsers}
	asked := ask.Principal != "" && ask.Operation != "" && ask.Resource != "" && (ask.Name != "" || ask.Resource == "cluster")
	if asked {
		if _, ok := aclOperationCode(ask.Operation); !ok {
			c.fail(w, r, badInput("the operation is one of %s", strings.Join(aclOperations(), ", ")))
			return
		}
		if _, ok := aclResourceCode(ask.Resource); !ok {
			c.fail(w, r, badInput("the resource type is one of %s", strings.Join(aclResources, ", ")))
			return
		}
		ans := rules.check(ask)
		data["Answer"] = ans
		data["Error"] = authorizationError(ask.Resource)
		if !ans.Allowed {
			// The way a real request was refused like this, when the recorder holds one.
			_, _, last := c.deniedWindow(time.Now(), func(e TrafficEvent) bool {
				return refusedBy(aclRule{Principal: ask.Principal, Resource: ask.Resource, Name: ask.Name, Pattern: "literal"}, e)
			})
			data["LastRefused"] = last
		}
		data["Advice"] = checkAdvice(rules, ask, ans)
		if cmp, ok := compareAllowed(rules, ask); ok {
			data["Compare"] = cmp
		}
	}
	c.render(w, r, "access_check", data)
}

// checkAdvice is the "To let it through" (or "To refuse it") of Ac-05, from the rules.
func checkAdvice(rules aclRules, q aclQuestion, ans aclAnswer) []confirmSeg {
	what := mono("ALLOW " + q.Principal + " " + upperFirst(q.Operation))
	on := mono(q.Name)
	switch {
	case ans.Allowed && ans.By == nil && !ans.Applies && len(ans.Steps) > 1 && ans.Steps[0].Decided:
		return []confirmSeg{txt("A super user is never refused; only taking it out of super.users (--acl-super-user) would change this.")}
	case ans.Allowed && ans.By == nil:
		return []confirmSeg{txt("No ACL is about this " + q.Resource + ", so everyone may do this. Any ACL on it closes it to everyone it does not allow.")}
	case ans.Allowed:
		return []confirmSeg{txt("A DENY of it for " + q.Principal + " on "), on, txt(" would refuse it: a DENY always wins.")}
	case ans.By != nil:
		return []confirmSeg{txt("Delete the DENY, then add "), what, txt(" on "), on, txt(". Deleting alone leaves the " + q.Resource + " with no ACL"), txt(map[bool]string{true: ", so it opens to everyone.", false: ", so the default (deny) refuses it."}[rules.OpenByDefault])}
	}
	return []confirmSeg{txt("Add "), what, txt(" on "), on, txt(".")}
}

// compareAllowed finds a request of the same operation on the same kind of resource
// that the rules allow by an ACL, to set beside a refusal (Ac-05's "Compare").
func compareAllowed(rules aclRules, q aclQuestion) (map[string]any, bool) {
	for _, a := range rules.ACLs {
		if a.Permission != "allow" || a.Resource != q.Resource || !grants(a.Operation, q.Operation) || a.Principal == "User:*" || a.Host != "*" || a.Pattern != "literal" {
			continue
		}
		other := aclQuestion{Principal: a.Principal, Operation: q.Operation, Resource: q.Resource, Name: a.Name, Host: q.Host}
		if other == q {
			continue
		}
		if ans := rules.check(other); ans.Allowed && ans.By != nil {
			return map[string]any{"Q": other, "By": *ans.By}, true
		}
	}
	return nil, false
}

// aclCreatePage is Ac-03: the fields CreateAcls has, the same ACL as a kafka-acls.sh
// command, and what it will do.
func (c *Console) aclCreatePage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rules, err := c.aclRules(ctx)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	q := r.URL.Query()
	a := ACLRow{Principal: q.Get("principal"), Host: q.Get("host"), Resource: q.Get("resource"), Name: q.Get("name"), Pattern: q.Get("pattern"),
		Operation: q.Get("operation"), Permission: q.Get("permission")}
	if a.Host == "" {
		a.Host = "*"
	}
	if a.Resource == "" {
		a.Resource = "topic"
	}
	if a.Pattern == "" {
		a.Pattern = "literal"
	}
	if a.Operation == "" {
		a.Operation = "read"
	}
	if a.Permission == "" {
		a.Permission = "allow"
	}
	rows, _ := c.be.ListACLs(ctx)
	c.render(w, r, "access_create", map[string]any{"Page": "access", "View": "acl_create", "Title": "Create an ACL", "A": a, "Operations": aclOperations(), "Resources": aclResources,
		"AllACLs": rows, "SuperUserList": rules.SuperUsers, "Console": c.consolePrincipal(), "ConsoleSuper": contains(rules.SuperUsers, c.consolePrincipal()),
		"Command": aclCommand(c.connectFacts(r), a), "Effect": aclEffect(rules, a)})
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// aclCreatePreview answers the form as it is typed: the command and what it will do.
func (c *Console) aclCreatePreview(w http.ResponseWriter, r *http.Request) {
	rules, err := c.aclRules(r.Context())
	if err != nil {
		c.fail(w, r, err)
		return
	}
	a := aclFromForm(r)
	c.partial(w, "access_create_preview", map[string]any{"Command": aclCommand(c.connectFacts(r), a), "Effect": aclEffect(rules, a)})
}

func aclFromForm(r *http.Request) ACLRow {
	f := func(k string) string { return strings.TrimSpace(r.FormValue(k)) }
	return ACLRow{Principal: f("principal"), Host: f("host"), Resource: f("resource"), Name: f("name"), Pattern: f("pattern"), Operation: f("operation"), Permission: f("permission")}
}

// aclCreate sends the form's CreateAcls and goes back to the list.
func (c *Console) aclCreate(w http.ResponseWriter, r *http.Request) {
	a := aclFromForm(r)
	if a.Resource == "cluster" && a.Name == "" {
		a.Name = "kafka-cluster"
	}
	if err := c.be.CreateACL(r.Context(), a); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.prefix+"/access", "ACL created: "+strings.ToUpper(a.Permission)+" "+a.Principal+" "+upperFirst(a.Operation)+" on "+a.Resource+" "+a.Name)
}

// aclCommand is the same ACL from a terminal: kafka-acls.sh --add with the form's
// fields, in its own flag names.
func aclCommand(f connectFacts, a ACLRow) string {
	var b strings.Builder
	b.WriteString("kafka-acls.sh --bootstrap-server " + f.Bootstrap)
	if f.CommandConfig != "" {
		b.WriteString(" " + strings.TrimSpace(f.CommandConfig))
	}
	b.WriteString(" --add \\\n  --" + a.Permission + "-principal " + shellQuote(a.Principal) + " --" + a.Permission + "-host " + shellQuote(a.Host) + " \\\n  --operation " + kafkaACLOp(a.Operation))
	switch a.Resource {
	case "topic":
		b.WriteString(" --topic " + shellQuote(a.Name))
	case "group":
		b.WriteString(" --group " + shellQuote(a.Name))
	case "cluster":
		b.WriteString(" --cluster")
	case "transactional id":
		b.WriteString(" --transactional-id " + shellQuote(a.Name))
	}
	if a.Pattern == "prefixed" {
		b.WriteString(" --resource-pattern-type prefixed")
	}
	return b.String()
}

// kafkaACLOp is an operation as kafka-acls.sh's --operation takes it: Read,
// DescribeConfigs, IdempotentWrite.
func kafkaACLOp(word string) string {
	var b strings.Builder
	for _, w := range strings.Fields(word) {
		b.WriteString(upperFirst(w))
	}
	return b.String()
}

// aclEffect is "What this will do": the resource's state now and after, by the rules.
func aclEffect(rules aclRules, a ACLRow) []confirmSeg {
	if a.Principal == "" || (a.Name == "" && a.Resource != "cluster") {
		return []confirmSeg{txt("Fill in the principal and the resource to see what this ACL will do.")}
	}
	name := a.Name
	if a.Resource == "cluster" {
		name = "kafka-cluster"
	}
	applies := false
	for _, r := range rules.ACLs {
		if r.about(a.Resource, name) || (a.Pattern == "prefixed" && r.Resource == a.Resource && strings.HasPrefix(r.Name, a.Name)) {
			applies = true
		}
	}
	thing := []confirmSeg{txt("The " + a.Resource + " "), mono(a.Name)}
	if a.Pattern == "prefixed" {
		thing = []confirmSeg{txt("Every " + a.Resource + " whose name starts with "), mono(a.Name)}
	}
	out := append([]confirmSeg{}, thing...)
	if a.Permission == "deny" {
		return append(out, txt(": "), mono(a.Principal), txt(" is refused "+upperFirst(a.Operation)+", with "), mono(authorizationError(a.Resource)),
			txt(", whatever an ALLOW says: a DENY always wins. A super user is never refused."))
	}
	switch {
	case !applies && rules.OpenByDefault:
		out = append(out, txt(" has no ACL yet and is open to everyone. After this, only what an ACL allows passes: "), mono(a.Principal),
			txt(" may "+a.Operation+" it, and every other request on it, by any principal that is not a super user, is refused with "), mono(authorizationError(a.Resource)),
			txt(" until an ACL allows it."))
	case !applies:
		out = append(out, txt(" has no ACL yet and the default is deny, so nobody but a super user may use it. After this, "), mono(a.Principal), txt(" may "+a.Operation+" it."))
	default:
		out = append(out, txt(" already has ACLs. After this, "), mono(a.Principal), txt(" may also "+a.Operation+" it, unless a DENY that matches refuses it."))
	}
	return out
}

// inspectACL is what one ACL lets through or refuses (Ac-02's panel): the entry, what
// the rules make of it, and the requests the recorder holds that it decided.
func (c *Console) inspectACL(w http.ResponseWriter, r *http.Request, key string) {
	p := splitKey(key, 7)
	if p == nil {
		c.fail(w, r, badInput("an ACL is named by its principal, resource, name, pattern, operation, permission and host"))
		return
	}
	a := ACLRow{Principal: p[0], Resource: p[1], Name: p[2], Pattern: p[3], Operation: p[4], Permission: p[5], Host: p[6]}
	v := inspectorView{Kind: KindACL, Key: key, Title: strings.ToUpper(a.Permission) + " " + a.Principal + " " + upperFirst(a.Operation),
		Sub: upperFirst(a.Resource) + " " + a.Name + " · " + a.Pattern + " · host " + a.Host, Body: map[string]any{"A": a}}
	rows, err := c.be.ListACLs(r.Context())
	if err != nil {
		c.fail(w, r, err)
		return
	}
	found := false
	for _, x := range rows {
		found = found || x == a
	}
	if !found {
		v.Gone, v.GoneMsg = true, "This ACL is no longer stored."
		c.inspect(w, v)
		return
	}
	rule := a.rule()
	n, over, last := c.deniedWindow(time.Now(), func(e TrafficEvent) bool { return a.Permission == "deny" && refusedBy(rule, e) })
	v.Body["Refused"], v.Body["Over"], v.Body["Last"], v.Body["HasTraffic"] = n, over, last, c.traffic != nil
	others := 0
	for _, x := range rows {
		if x != a && x.rule().about(a.Resource, a.Name) {
			others++
		}
	}
	v.Body["Others"] = others
	v.Body["CheckURL"] = c.prefix + "/access/check?" + url.Values{"principal": {a.Principal}, "operation": {a.Operation}, "resource": {a.Resource}, "name": {a.Name}}.Encode()
	c.inspect(w, v)
}

// sortedPrincipals is for the checker's datalist: every principal an ACL names, and
// the super users.
func sortedPrincipals(rows []ACLRow, supers []string) []string {
	seen := map[string]bool{"User:ANONYMOUS": true}
	for _, a := range rows {
		seen[a.Principal] = true
	}
	for _, s := range supers {
		seen[s] = true
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
