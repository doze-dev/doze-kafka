package console

// The inspector: the right-hand panel a selection opens in. A message, a request,
// a client, a group member, a subject, a transaction or a group event is shown
// through one contract (title, kind, body, a close control and an address), so
// that opening, closing, focus and the browser's Back button work the same for
// all of them. There is no other drawer.
//
// A selection is named by an "open" value in the page's query string:
//
//	?open=5                  a message, by offset (the page names the partition)
//	?open=request:<row id>   every other kind, as kind:key
//
// A key may have several parts (a member needs its group); they are joined by ":"
// with each part escaped, so ":" in a name cannot be mistaken for the separator.

import (
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Inspector kinds.
const (
	KindMessage     = "message"
	KindRequest     = "request"
	KindClient      = "client"
	KindMember      = "member"
	KindSubject     = "subject"
	KindTransaction = "transaction"
	KindEvent       = "event"
)

// inspectorKinds is every kind the inspector can show, in the order the
// documentation lists them.
var inspectorKinds = []string{KindMessage, KindRequest, KindClient, KindMember, KindSubject, KindTransaction, KindEvent}

// inspectorKindLabels are the words above a selection's title (Sy-01: "Message",
// "Request", "Client").
var inspectorKindLabels = map[string]string{
	KindMessage: "Message", KindRequest: "Request", KindClient: "Client", KindMember: "Member",
	KindSubject: "Subject", KindTransaction: "Transaction", KindEvent: "Event",
}

// inspectorNav is one of the previous and next controls in the inspector's header.
type inspectorNav struct {
	Label    string // aria-label: "Older record".
	Title    string
	Glyph    string
	URL      string // where the control fetches; empty means disabled.
	Disabled bool
}

// inspectorView is the contract every kind renders through.
type inspectorView struct {
	Kind  string
	Key   string // the selection's key, as it is written after "open=".
	Title string
	Sub   string
	Nav   []inspectorNav
	// Gone says the selection no longer exists (it aged out, or was deleted): the
	// inspector still opens, with a title and a way out, and says so.
	Gone    bool
	GoneMsg string
	// NoURL keeps this selection out of the address (a message found by a search
	// across partitions has no single partition to name).
	NoURL bool
	// Body is what the kind's own template reads.
	Body map[string]any
}

// openParam is the value the page's "open" parameter takes for a selection.
func openParam(kind, key string) string {
	if kind == KindMessage {
		return key
	}
	return kind + ":" + key
}

// joinKey builds a multi-part key.
func joinKey(parts ...string) string {
	esc := make([]string, len(parts))
	for i, p := range parts {
		esc[i] = url.QueryEscape(p)
	}
	return strings.Join(esc, ":")
}

// splitKey is joinKey's inverse. It returns exactly n parts or nil.
func splitKey(key string, n int) []string {
	raw := strings.Split(key, ":")
	if len(raw) != n {
		return nil
	}
	out := make([]string, n)
	for i, p := range raw {
		s, err := url.QueryUnescape(p)
		if err != nil {
			return nil
		}
		out[i] = s
	}
	return out
}

// inspectURL is the address the inspector fetches a selection from.
func (c *Console) inspectURL(kind, key string) string {
	return c.prefix + "/inspect/" + kind + "?key=" + url.QueryEscape(key)
}

// requestKey is a request row's identity. It is made of what does not change when
// the row folds in another identical request (the first time, who, what, on what),
// not of its sequence number, which moves every time the row is repeated; a
// selection and the focus a poll puts back both depend on that.
func requestKey(e TrafficEvent) string {
	h := fnv.New64a()
	fmt.Fprint(h, e.First.UnixNano(), "|", e.Remote, "|", e.ClientID, "|", e.API, "|", e.Version, "|", strings.Join(e.Topics, ","), "|", e.Group)
	return strconv.FormatUint(h.Sum64(), 36)
}

// inspect renders one selection through the inspector contract.
func (c *Console) inspect(w http.ResponseWriter, v inspectorView) {
	data := map[string]any{}
	for k, val := range v.Body {
		data[k] = val
	}
	open := openParam(v.Kind, v.Key)
	if v.NoURL {
		open = ""
	}
	// Prefixed: a body's own data (a record has a Key and a Title of its own) must
	// not be overwritten by the contract's.
	data["InspKind"], data["InspKey"], data["InspTitle"], data["InspSub"] = v.Kind, v.Key, v.Title, v.Sub
	data["InspKindLabel"] = inspectorKindLabels[v.Kind]
	data["InspNav"], data["InspGone"], data["InspGoneMsg"], data["InspOpen"] = v.Nav, v.Gone, v.GoneMsg, open
	c.partial(w, "inspector", data)
}

// inspectRoute answers GET {prefix}/inspect/{kind}?key=…, the one address every
// kind other than a message is fetched from (a message keeps its topic's own
// address, which says the partition and the offset).
func (c *Console) inspectRoute(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	switch kind := r.PathValue("kind"); kind {
	case KindRequest:
		c.inspectRequest(w, r, key)
	case KindClient:
		c.inspectClient(w, r, key)
	case KindMember:
		c.inspectMember(w, r, key)
	case KindSubject:
		c.inspectSubject(w, r, key)
	case KindTransaction:
		c.inspectTransaction(w, r, key)
	case KindEvent:
		c.inspectEvent(w, r, key)
	default:
		c.fail(w, r, fmt.Errorf("%w: there is no %q to inspect", errNotFound, kind))
	}
}

func (c *Console) trafficOff(w http.ResponseWriter, r *http.Request) bool {
	if c.traffic != nil {
		return false
	}
	c.fail(w, r, fmt.Errorf("%w: there is no traffic view in this console", errNotFound))
	return true
}

func (c *Console) inspectRequest(w http.ResponseWriter, r *http.Request, key string) {
	if c.trafficOff(w, r) {
		return
	}
	if raw := r.URL.Query().Get("seq"); key == "" && raw != "" {
		// The earlier address named a sequence number.
		seq, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			c.fail(w, r, badInput("seq must be a number"))
			return
		}
		key = "seq-" + raw // no row: it has moved on or aged out.
		for _, e := range c.traffic.Events(TrafficFilter{}, 0) {
			if e.Seq == seq {
				key = requestKey(e)
			}
		}
	}
	if key == "" {
		c.fail(w, r, badInput("a request is named by its row id"))
		return
	}
	v := inspectorView{Kind: KindRequest, Key: key, Title: "Request", Body: map[string]any{}}
	for _, e := range c.traffic.Events(TrafficFilter{}, 0) {
		if requestKey(e) != key {
			continue
		}
		row := c.trafficRow(e)
		v.Title, v.Sub = fmt.Sprintf("%s v%d", e.API, e.Version), row.Who
		v.Body["R"] = row
		c.inspect(w, v)
		return
	}
	v.Gone = true
	v.GoneMsg = "This request has aged out of what the broker remembers. The last couple of thousand are kept, in memory only."
	c.inspect(w, v)
}

func (c *Console) inspectClient(w http.ResponseWriter, r *http.Request, key string) {
	if c.trafficOff(w, r) {
		return
	}
	conn, err := strconv.ParseUint(key, 10, 64)
	if err != nil {
		c.fail(w, r, badInput("a client is named by its connection number"))
		return
	}
	v := inspectorView{Kind: KindClient, Key: key, Title: "Client", Body: map[string]any{}}
	for _, cl := range c.traffic.Clients() {
		if cl.Conn != conn {
			continue
		}
		now := time.Now()
		who := cl.ClientID
		if who == "" {
			who = cl.Remote
		}
		v.Title, v.Sub = who, cl.Remote
		v.Body["C"] = clientRow{TrafficClient: cl, Who: who, Age: spanText(now.Sub(cl.Opened)), Idle: spanText(now.Sub(cl.Last)),
			Filter: TrafficFilter{Client: who}.query()}
		c.inspect(w, v)
		return
	}
	v.Gone = true
	v.GoneMsg = "This connection has closed."
	c.inspect(w, v)
}

func (c *Console) inspectMember(w http.ResponseWriter, r *http.Request, key string) {
	parts := splitKey(key, 2)
	if parts == nil {
		c.fail(w, r, badInput("a member is named by its group and its member id"))
		return
	}
	group, member := parts[0], parts[1]
	detail, err := c.be.DescribeGroup(r.Context(), group)
	if err != nil {
		c.failResource(w, r, err, "group", group)
		return
	}
	v := inspectorView{Kind: KindMember, Key: key, Title: shortID(member), Sub: group, Body: map[string]any{"Group": group, "GroupURL": c.groupURL(group)}}
	for _, m := range detail.Members {
		if m.ID != member {
			continue
		}
		mv := memberView{ID: m.ID, ClientID: m.ClientID, Host: m.Host, Label: m.ClientID}
		if mv.Label == "" {
			mv.Label = shortID(m.ID)
		}
		topics := make([]string, 0, len(m.Assigned))
		for t := range m.Assigned {
			topics = append(topics, t)
		}
		sort.Strings(topics)
		for _, t := range topics {
			mv.Assigned = append(mv.Assigned, assignView{Topic: t, TopicURL: c.topicURL(t), Parts: partRanges(m.Assigned[t])})
		}
		v.Title = mv.Label
		v.Body["M"] = mv
		v.Body["State"], v.Body["Protocol"] = detail.State, protoLabelOf(detail.Protocol)
		c.inspect(w, v)
		return
	}
	v.Gone = true
	v.GoneMsg = "This member is no longer in the group. Members leave when their consumer stops or misses its heartbeat."
	c.inspect(w, v)
}

func protoLabelOf(p string) string {
	if p == "848" {
		return "KIP-848"
	}
	return "classic"
}

func (c *Console) inspectSubject(w http.ResponseWriter, r *http.Request, key string) {
	if c.registryOff(w, r) {
		return
	}
	if key == "" {
		c.fail(w, r, badInput("a subject is named by its name"))
		return
	}
	ctx := r.Context()
	versions, err := c.reg.Versions(ctx, key)
	if err != nil {
		c.failSubject(w, r, err, key)
		return
	}
	cur, err := c.reg.Version(ctx, key, 0)
	if err != nil {
		c.failSubject(w, r, err, key)
		return
	}
	level, own, _ := c.reg.Compat(ctx, key)
	c.inspect(w, inspectorView{Kind: KindSubject, Key: key, Title: key, Sub: "subject", Body: map[string]any{
		"Subject": key, "SubjectURL": c.schemaURL(key), "Versions": versions, "Latest": cur,
		"Compat": level, "CompatOwn": own, "Type": cur.TypeLabel(),
	}})
}

func (c *Console) inspectTransaction(w http.ResponseWriter, r *http.Request, key string) {
	if key == "" {
		c.fail(w, r, badInput("a transaction is named by its transactional id"))
		return
	}
	v := inspectorView{Kind: KindTransaction, Key: key, Title: key, Sub: "transaction", Body: map[string]any{}}
	for _, t := range c.be.openTransactions(r.Context()) {
		if t.ID != key {
			continue
		}
		type part struct{ Name, URL string }
		var parts []part
		for _, tp := range t.Partitions {
			name := tp
			if i := strings.LastIndex(tp, "-"); i > 0 {
				name = tp[:i]
			}
			parts = append(parts, part{Name: tp, URL: c.topicURL(name)})
		}
		v.Body["T"], v.Body["Parts"] = t, parts
		c.inspect(w, v)
		return
	}
	v.Gone = true
	v.GoneMsg = "This transaction has ended: it committed or aborted since the page was drawn."
	c.inspect(w, v)
}

func (c *Console) inspectEvent(w http.ResponseWriter, r *http.Request, key string) {
	if c.trafficOff(w, r) {
		return
	}
	parts := splitKey(key, 2)
	if parts == nil {
		c.fail(w, r, badInput("an event is named by its group and its sequence number"))
		return
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		c.fail(w, r, badInput("an event is named by its group and its sequence number"))
		return
	}
	group := parts[0]
	v := inspectorView{Kind: KindEvent, Key: key, Title: "Event", Sub: group, Body: map[string]any{"Group": group, "GroupURL": c.groupURL(group)}}
	for _, e := range c.traffic.GroupEvents(group, 0) {
		if e.Seq != seq {
			continue
		}
		label, tone := timelineKind(e.Kind)
		v.Title = label
		v.Body["E"] = timelineRow{TrafficGroupEvent: e, Label: label, Tone: tone, Member: shortID(e.Member)}
		c.inspect(w, v)
		return
	}
	v.Gone = true
	v.GoneMsg = "This event has aged out of what the broker remembers about the group."
	c.inspect(w, v)
}
