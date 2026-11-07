package console

// The wire dock: every topic, group and subject page ends with the live requests
// that touched it. The component is built from the small interface below, so it is
// built and tested here with a fake, and the recorder's own filter (by topic or
// group, newest first, with a cursor) is the traffic stage's to provide.
//
// Until then the dock reads what the traffic source already offers (its topic and
// group filters), which is enough for a topic or a group. A subject's dock, and the
// Schemas home's, read the registry's own request log (registryDock), since the
// traffic recorder sees only Kafka requests.

import (
	"context"
	"fmt"
	"hash/fnv"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DockFilter names the one resource a dock is about. Exactly one is set.
type DockFilter struct {
	Topic, Group, Subject string
	// ACL is the Access page's dock: the requests about ACLs (DescribeAcls, CreateAcls,
	// DeleteAcls) and every request an ACL refused.
	ACL bool
	// Registry is the Schemas home's dock: every request the registry kept.
	Registry bool
}

// registry says whether the dock is about the schema registry's requests.
func (f DockFilter) registry() bool { return f.Subject != "" || f.Registry }

func (f DockFilter) empty() bool { return f == DockFilter{} }

// values is the filter as query parameters: the dock's own poll and its link to the
// Traffic page are both built from them, and each is written into the page as a
// whole address. A query string interpolated after a "?" in a template is escaped a
// second time ("topic%3dorders"), which the server reads as no filter at all.
func (f DockFilter) values() url.Values {
	v := url.Values{}
	switch {
	case f.Topic != "":
		v.Set("topic", f.Topic)
	case f.Group != "":
		v.Set("group", f.Group)
	case f.Subject != "":
		v.Set("subject", f.Subject)
	case f.ACL:
		v.Set("acl", "1")
	case f.Registry:
		v.Set("registry", "1")
	}
	return v
}

// DockEvent is one row of a dock. Source says whose request it was: the Kafka
// wire, or the schema registry's REST API (E4).
type DockEvent struct {
	ID      string // the row id: stable while the row folds in repeats; the cursor is one.
	Time    time.Time
	Source  string // "kafka" or "registry".
	Client  string
	Request string // "Produce v9", "GET /subjects".
	Target  string
	Detail  string
	Error   string // the error's name, empty for success.
	Took    string // how long the broker took, as the Traffic table writes it; empty when not known.
	Count   int
	// Kind is the inspector kind the row opens ("request"), or empty for a row that
	// has none.
	Kind string
	// A registry request's method, path (with its query) and HTTP status.
	Method, Path string
	Status       int
}

// StatusClass is how a registry row's status is coloured: a refusal is the board's
// deny, a failure of the registry's own is bad.
func (e DockEvent) StatusClass() string {
	switch {
	case e.Status < 300:
		return "ok"
	case e.Status < 500:
		return "deny"
	}
	return "bad"
}

// DockSource is what a dock needs: the events for one resource, newest first, at
// most limit, those after the cursor (the ID of the last row already shown; empty
// for the first page), and the cursor for the next page ("" when there is none).
// A cursor whose row the source no longer holds has nothing after it: what is kept
// is all newer.
type DockSource interface {
	DockEvents(f DockFilter, limit int, after string) (events []DockEvent, next string)
}

// dockRows is how many rows a dock shows.
const dockRows = 8

// trafficDock is a DockSource over the traffic source the console already has.
type trafficDock struct {
	src TrafficSource
}

func (d trafficDock) DockEvents(f DockFilter, limit int, after string) ([]DockEvent, string) {
	if f.Subject != "" {
		return nil, "" // the registry's requests are not recorded yet.
	}
	var all []TrafficEvent
	if f.ACL {
		for _, e := range d.src.Events(TrafficFilter{}, 0) {
			if e.API == "DescribeAcls" || e.API == "CreateAcls" || e.API == "DeleteAcls" || e.Class == "denied" {
				all = append(all, e)
			}
		}
	} else {
		all = d.src.Events(TrafficFilter{Topic: f.Topic, Group: f.Group}, 0)
	}
	start := 0
	if after != "" {
		start = len(all)
		for i, e := range all {
			if e.ID == after {
				start = i + 1
				break
			}
		}
	}
	var out []DockEvent
	next := ""
	for _, e := range all[min(start, len(all)):] {
		if len(out) == limit {
			next = out[len(out)-1].ID
			break
		}
		who := e.ClientID
		if who == "" {
			who = e.Remote
		}
		target := strings.Join(e.Topics, ", ")
		if target == "" {
			target = e.Group
		}
		out = append(out, DockEvent{
			ID: e.ID, Time: e.Time, Source: "kafka", Client: who, Request: fmt.Sprintf("%s v%d", e.API, e.Version),
			Target: target, Detail: e.Detail, Error: e.Error, Took: shortDuration(e.Duration), Count: e.Count, Kind: KindRequest,
		})
	}
	return out, next
}

// dockRow is a DockEvent ready to show.
type dockRow struct {
	DockEvent
}

// Name is the request without its version, as the board's wire prints it
// ("Produce"); the version stays on the hover.
func (r dockRow) Name() string {
	i := strings.LastIndex(r.Request, " v")
	if i < 0 || i+2 >= len(r.Request) {
		return r.Request
	}
	for _, c := range r.Request[i+2:] {
		if c < '0' || c > '9' {
			return r.Request
		}
	}
	return r.Request[:i]
}

// dockSource is where a dock's rows come from: the one given (a test's), the
// registry's log for a subject or the Schemas home, the traffic source otherwise.
func (c *Console) dockSource(f DockFilter) DockSource {
	switch {
	case c.dock != nil:
		return c.dock
	case f.registry():
		if c.reg != nil {
			return &registryDock{rc: c.reg}
		}
	case c.traffic != nil:
		return trafficDock{c.traffic}
	}
	return nil
}

// registryDock reads the registry's request log (GET /_doze/requests). A registry
// that keeps none (one that is not doze-kafka's) answers 404, and the dock says so.
type registryDock struct {
	rc  *registryClient
	err error
}

// registryDockWait bounds the read: the dock is a page's last part, and polled.
const registryDockWait = 2 * time.Second

func (d *registryDock) DockEvents(f DockFilter, limit int, after string) ([]DockEvent, string) {
	var before uint64
	if after != "" {
		n, err := strconv.ParseUint(after, 10, 64)
		if err != nil || n == 0 {
			return nil, "" // not a cursor of this log: nothing is after it.
		}
		before = n
	}
	ctx, cancel := context.WithTimeout(context.Background(), registryDockWait)
	defer cancel()
	page, err := d.rc.Requests(ctx, f.Subject, before, limit)
	if err != nil {
		d.err = err
		return nil, ""
	}
	out := make([]DockEvent, 0, len(page.Requests))
	for _, q := range page.Requests {
		who := q.User
		if who == "" {
			who = q.From
		}
		out = append(out, DockEvent{ID: strconv.FormatUint(q.Seq, 10), Time: q.At, Source: "registry", Client: who,
			Request: q.Method + " " + q.Path, Method: q.Method, Path: q.Path, Status: q.Status,
			Took: shortDuration(time.Duration(q.Took)), Count: 1})
	}
	next := ""
	if page.Next != 0 {
		next = strconv.FormatUint(page.Next, 10)
	}
	return out, next
}

// dockView is which dock is asked for: the resource, the cursor (the id of the last
// row of the page before; empty for the newest rows) and whether it carries the pause
// control (a page with none of its own).
type dockView struct {
	DockFilter
	After  string
	Toggle bool
}

// maxDockCursor bounds a cursor. It is a row id, which is a dozen bytes.
const maxDockCursor = 64

// url is the address of a dock: what it polls, and what Older and Newest ask for.
func (c *Console) dockURL(v dockView) template.URL {
	q := v.values()
	if v.After != "" {
		q.Set("after", v.After)
	}
	if v.Toggle {
		q.Set("t", "1")
	}
	return template.URL(c.prefix + "/dock?" + q.Encode())
}

// dockData is what a dock shows for one resource: its newest rows, as a page ends
// with them.
func (c *Console) dockData(f DockFilter) map[string]any { return c.dockPage(dockView{DockFilter: f}) }

// dockPage is what a dock shows for one resource, from its cursor on. It is the same
// for the page that ends with it and for its poll, so they cannot disagree; a dock
// that has been paged polls the page it shows, not the newest rows.
func (c *Console) dockPage(v dockView) map[string]any {
	f := v.DockFilter
	src := c.dockSource(f)
	d := map[string]any{"Prefix": c.prefix, "DockFilter": f, "HasDock": src != nil, "DockToggle": v.Toggle, "DockPaged": v.After != "",
		"DockRegistry": f.registry(), "DockKnown": true}
	d["DockLive"] = c.dockURL(v)
	// The Traffic page filters by topic and by group. It has no filter for a subject,
	// so a subject's dock does not link there as if it had.
	if !f.registry() && !f.ACL {
		d["DockTraffic"] = template.URL(c.prefix + "/traffic?" + f.values().Encode())
	}
	if src == nil {
		return d
	}
	events, next := src.DockEvents(f, dockRows, v.After)
	rows := make([]dockRow, 0, len(events))
	h := fnv.New64a()
	fmt.Fprint(h, v.After, "|")
	for _, e := range events {
		if e.Source == "registry" && e.Method == "" {
			// A source that gives the request whole ("GET /subjects"): its two parts.
			e.Method, e.Path, _ = strings.Cut(e.Request, " ")
		}
		rows = append(rows, dockRow{DockEvent: e})
		fmt.Fprint(h, e.ID, e.Count, e.Error, ";")
	}
	fmt.Fprint(h, "|", next)
	d["DockRows"], d["DockMore"], d["DockHash"] = rows, next != "", fmt.Sprintf("%x", h.Sum64())
	if next != "" {
		d["DockOlder"] = c.dockURL(dockView{DockFilter: f, After: next, Toggle: v.Toggle})
	}
	if v.After != "" {
		d["DockNewest"] = c.dockURL(dockView{DockFilter: f, Toggle: v.Toggle})
	}
	if rd, ok := src.(*registryDock); ok && rd.err != nil {
		d["DockKnown"] = false // the registry keeps no request log, or did not answer.
	}
	return d
}

// dockLive answers the dock's own poll, and its Older and Newest: the rows from the
// cursor on (after=, the contract's cursor: foundation A4), or 204 when nothing moved.
func (c *Console) dockLive(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := DockFilter{Topic: q.Get("topic"), Group: q.Get("group"), Subject: q.Get("subject"), ACL: q.Get("acl") == "1", Registry: q.Get("registry") == "1"}
	if f.empty() {
		c.fail(w, r, badInput("a dock is about one topic, group or subject, or the registry"))
		return
	}
	v := dockView{DockFilter: f, After: q.Get("after"), Toggle: q.Get("t") == "1"}
	if len(v.After) > maxDockCursor {
		c.fail(w, r, badInput("a dock's cursor is a row id"))
		return
	}
	d := c.dockPage(v)
	if !d["HasDock"].(bool) {
		c.fail(w, r, fmt.Errorf("%w: there is no wire dock in this console", errNotFound))
		return
	}
	if liveUnchanged(w, r, d["DockHash"].(string)) {
		return
	}
	c.partial(w, "wire_dock", d)
}
