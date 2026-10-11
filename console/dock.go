package console

// The wire dock: every topic, group and subject page ends with the live requests
// that touched it. The component is built from the small interface below, so it is
// built and tested here with a fake, and the recorder's own filter (by topic or
// group, newest first, with a cursor) is the traffic stage's to provide.
//
// Until then the dock reads what the traffic source already offers (its topic and
// group filters), which is enough for a topic or a group. A subject has no source
// yet: the registry's requests are not recorded, so its dock says so.

import (
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"
	"time"
)

// DockFilter names the one resource a dock is about. Exactly one is set.
type DockFilter struct {
	Topic, Group, Subject string
}

func (f DockFilter) empty() bool { return f == DockFilter{} }

// query is the filter as a query string, for the dock's own poll.
func (f DockFilter) query() string {
	switch {
	case f.Topic != "":
		return "topic=" + queryEscape(f.Topic)
	case f.Group != "":
		return "group=" + queryEscape(f.Group)
	case f.Subject != "":
		return "subject=" + queryEscape(f.Subject)
	}
	return ""
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
	Count   int
	// Kind is the inspector kind the row opens ("request"), or empty for a row that
	// has none.
	Kind string
}

// DockSource is what a dock needs: the events for one resource, newest first, at
// most limit, those after the cursor (the ID of the last row already shown; empty
// for the first page), and the cursor for the next page ("" when there is none).
type DockSource interface {
	DockEvents(f DockFilter, limit int, after string) (events []DockEvent, next string)
}

// dockRows is how many rows a dock shows.
const dockRows = 12

// trafficDock is a DockSource over the traffic source the console already has.
type trafficDock struct {
	src TrafficSource
}

func (d trafficDock) DockEvents(f DockFilter, limit int, after string) ([]DockEvent, string) {
	if f.Subject != "" {
		return nil, "" // the registry's requests are not recorded yet.
	}
	all := d.src.Events(TrafficFilter{Topic: f.Topic, Group: f.Group}, 0)
	start := 0
	if after != "" {
		start = len(all)
		for i, e := range all {
			if requestKey(e) == after {
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
			ID: requestKey(e), Time: e.Time, Source: "kafka", Client: who, Request: fmt.Sprintf("%s v%d", e.API, e.Version),
			Target: target, Detail: e.Detail, Error: e.Error, Count: e.Count, Kind: KindRequest,
		})
	}
	return out, next
}

// dockRow is a DockEvent ready to show.
type dockRow struct {
	DockEvent
}

func (c *Console) dockSource() DockSource {
	if c.dock != nil {
		return c.dock
	}
	if c.traffic != nil {
		return trafficDock{c.traffic}
	}
	return nil
}

// dockData is what a dock shows for one resource. It is the same for the page that
// ends with it and for its poll, so they cannot disagree.
func (c *Console) dockData(f DockFilter) map[string]any {
	src := c.dockSource()
	d := map[string]any{"Prefix": c.prefix, "DockFilter": f, "DockQuery": f.query(), "HasDock": src != nil}
	if src == nil {
		return d
	}
	events, next := src.DockEvents(f, dockRows, "")
	rows := make([]dockRow, 0, len(events))
	h := fnv.New64a()
	for _, e := range events {
		rows = append(rows, dockRow{DockEvent: e})
		fmt.Fprint(h, e.ID, e.Count, e.Error, ";")
	}
	d["DockRows"], d["DockMore"], d["DockHash"] = rows, next != "", fmt.Sprintf("%x", h.Sum64())
	d["DockKnown"] = f.Subject == "" || c.dock != nil // a subject has no source until the registry records its requests.
	return d
}

// dockLive answers the dock's own poll: the same rows, or 204 when nothing moved.
func (c *Console) dockLive(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := DockFilter{Topic: q.Get("topic"), Group: q.Get("group"), Subject: q.Get("subject")}
	if f.empty() {
		c.fail(w, r, badInput("a dock is about one topic, group or subject"))
		return
	}
	d := c.dockData(f)
	d["DockToggle"] = q.Get("t") == "1"
	if !d["HasDock"].(bool) {
		c.fail(w, r, fmt.Errorf("%w: there is no wire dock in this console", errNotFound))
		return
	}
	if liveUnchanged(w, r, d["DockHash"].(string)) {
		return
	}
	c.partial(w, "wire_dock", d)
}

// shellRoutes registers the shell's own addresses: the inspector and the dock.
func (c *Console) shellRoutes(m *http.ServeMux) {
	c.inspectorRoutes(m)
	m.HandleFunc("GET "+c.prefix+"/dock", c.dockLive)
}
