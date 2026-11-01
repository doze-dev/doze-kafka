package console

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Gr-06 (the reset form and its preview), Gr-07 (the refusal when the group has
// members) and the routes behind them. The preview and the reset read the same plan
// (backend.PlanReset), so what the preview says is what Apply writes.

// resetScope is one choice of the form's Topic field.
type resetScope struct {
	Topic      string // "" is every topic the group has offsets on.
	Label      string
	Partitions int
}

// resetPreviewRow is one line of the preview table.
type resetPreviewRow struct {
	Topic            string
	Partition        int32
	Committed, After int64
	Latest, LagAfter int64
	Changed          bool
	Note             string
	Field            string // the name of the offset input, in the "specific offsets" mode.
	Earliest         int64
	Ahead            bool
	LagNow           int64
}

// resetPreview is the table under "Preview" and what it adds up to.
type resetPreview struct {
	Rows             []resetPreviewRow
	Multi            bool // several topics: each row names its own.
	To               string
	Specific         bool
	LagNow, LagAfter int64
	Moves            int
	Count            string       // "6 partitions": the Apply button's number.
	Sentence         []confirmSeg // under the table.
}

// resetForm is the form, with what it starts on.
type resetForm struct {
	To, Topic, At string
	Scopes        []resetScope
	Preview       *resetPreview
	PreviewErr    string
}

// resetFieldPrefix names the per-partition offset inputs: "off:<topic>:<partition>".
const resetFieldPrefix = "off:"

func resetField(topic string, partition int32) string {
	return resetFieldPrefix + topic + ":" + strconv.Itoa(int(partition))
}

// resetSpecOf reads the form: what to move to, on which topic, and for the modes that
// need one the time or the offsets.
func resetSpecOf(r *http.Request) (ResetSpec, error) {
	spec := ResetSpec{To: r.FormValue("to"), Topic: r.FormValue("topic")}
	if spec.To == "" {
		spec.To = "earliest"
	}
	switch spec.To {
	case "timestamp":
		raw := strings.TrimSpace(r.FormValue("at"))
		t, err := parseAt(raw, time.Now())
		if err != nil {
			return spec, badInput("%q is not a time; write it like 2026-10-11T14:00:00Z (UTC unless it carries a zone), or 2h for two hours ago", raw)
		}
		spec.At = t
	case "offsets":
		spec.Offsets = map[TopicPartition]int64{}
		for k, vs := range r.Form {
			if !strings.HasPrefix(k, resetFieldPrefix) || len(vs) == 0 {
				continue
			}
			i := strings.LastIndex(k, ":")
			p, perr := strconv.ParseInt(k[i+1:], 10, 32)
			raw := strings.TrimSpace(vs[0])
			if i <= len(resetFieldPrefix)-1 || perr != nil || raw == "" {
				continue // an empty box is a partition left alone.
			}
			o, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || o < 0 {
				return spec, badInput("%q is not an offset; type a whole number, 0 or more", raw)
			}
			spec.Offsets[TopicPartition{k[len(resetFieldPrefix):i], int32(p)}] = o
		}
	}
	return spec, nil
}

// previewOf adds the plan up: the table's rows, the lag now and after, the Apply
// button's count and the sentence under the table.
func previewOf(rows []ResetRow, spec ResetSpec) *resetPreview {
	p := &resetPreview{To: spec.To, Specific: spec.To == "offsets"}
	topics := map[string]bool{}
	for _, r := range rows {
		topics[r.Topic] = true
	}
	p.Multi = len(topics) > 1
	for _, r := range rows {
		row := resetPreviewRow{Topic: r.Topic, Partition: r.Partition, Committed: r.Committed, After: r.After, Latest: r.Latest, Earliest: r.Earliest,
			Changed: r.Moves && r.After != r.Committed, Note: r.Note, Field: resetField(r.Topic, r.Partition)}
		row.Ahead = r.Committed > r.Latest
		if !row.Ahead {
			row.LagNow = r.Latest - r.Committed
		}
		row.LagAfter = r.Latest - r.After
		p.LagNow += row.LagNow
		p.LagAfter += row.LagAfter
		if r.Moves {
			p.Moves++
		}
		p.Rows = append(p.Rows, row)
	}
	p.Count = nounCount(p.Moves, "partition", "partitions")
	p.Sentence = previewSentence(p, spec)
	return p
}

func previewSentence(p *resetPreview, spec ResetSpec) []confirmSeg {
	var out []confirmSeg
	add := func(parts ...any) {
		for _, x := range parts {
			switch v := x.(type) {
			case string:
				out = append(out, txt(v))
			case confirmSeg:
				out = append(out, v)
			}
		}
	}
	switch spec.To {
	case "earliest":
		switch {
		case p.LagAfter > p.LagNow:
			add("Going to earliest makes the group re-read ", mono(num(p.LagAfter)), " records instead of ", mono(num(p.LagNow)), ". Pick latest to skip the backlog.")
		default:
			add("Going to earliest leaves ", mono(num(p.LagAfter)), " records to read, ", map[bool]string{true: "as now", false: "where now it is " + num(p.LagNow)}[p.LagAfter == p.LagNow], ".")
		}
	case "latest":
		if p.LagNow > 0 {
			add("Going to latest skips the ", mono(num(p.LagNow)), " records the group has not read: nothing is left to read. They stay in the topic.")
		} else {
			add("The group is already at the end of every partition it has an offset on.")
		}
	case "timestamp":
		add("Going to ", mono(utcStamp(spec.At)), " leaves ", mono(num(p.LagAfter)), " records to read instead of ", mono(num(p.LagNow)), ".")
	default:
		if p.Moves == 0 {
			add("Type an offset in a row to move it. A row left as it is stays where it is.")
		} else {
			add("With these offsets the group has ", mono(num(p.LagAfter)), " records to read instead of ", mono(num(p.LagNow)), ".")
		}
	}
	return out
}

// resetFormOf is the form as the Offsets tab first draws it: earliest, every topic
// (or the one), and its preview.
func (c *Console) resetFormOf(r *http.Request, id string, rows []groupLagRow) resetForm {
	f := resetForm{To: "earliest", At: time.Now().UTC().Truncate(time.Hour).Format(time.RFC3339)}
	count := map[string]int{}
	var order []string
	for _, row := range rows {
		if row.Deleted {
			continue
		}
		if count[row.Topic] == 0 {
			order = append(order, row.Topic)
		}
		count[row.Topic]++
	}
	sort.Strings(order)
	total := 0
	for _, t := range order {
		total += count[t]
	}
	if len(order) > 1 {
		f.Scopes = append(f.Scopes, resetScope{Label: fmt.Sprintf("all topics (%s)", nounCount(total, "partition", "partitions")), Partitions: total})
	}
	for _, t := range order {
		f.Scopes = append(f.Scopes, resetScope{Topic: t, Label: fmt.Sprintf("%s (%s)", t, nounCount(count[t], "partition", "partitions")), Partitions: count[t]})
	}
	if len(order) == 1 {
		f.Topic = order[0]
	}
	if len(order) > 0 {
		plan, err := c.be.PlanReset(r.Context(), id, ResetSpec{To: "earliest", Topic: f.Topic})
		if err != nil {
			f.PreviewErr = refusalFor(err).Message
		} else {
			f.Preview = previewOf(plan, ResetSpec{To: "earliest"})
		}
	}
	return f
}

// groupResetPreview answers the form as it is typed: committed now, after, and the
// lag after, from the plan. It sends nothing to the group.
func (c *Console) groupResetPreview(w http.ResponseWriter, r *http.Request) {
	id := groupFromSegment(r.PathValue("id"))
	spec, err := resetSpecOf(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if err := c.be.requireIdle(r.Context(), id); err != nil {
		c.groupRefused(w, r, id, err)
		return
	}
	rows, err := c.be.PlanReset(r.Context(), id, spec)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.partial(w, "group_reset_preview", map[string]any{"Preview": previewOf(rows, spec), "Group": GroupDetail{ID: id}, "Swap": true})
}

// groupRefused answers a change to a group that has members with the refusal and the
// way out (Gr-07), where the page keeps it: #reset-result. It is a 400 the page swaps
// in (X-Doze-Refusal says so), not a toast.
func (c *Console) groupRefused(w http.ResponseWriter, r *http.Request, id string, err error) {
	data, derr := c.groupData(r, id)
	if derr != nil {
		c.fail(w, r, err)
		return
	}
	f := refusalFor(err)
	data["Refusal"] = f
	w.Header().Set("X-Doze-Refusal", "reset")
	w.Header().Set("HX-Retarget", "#reset-result")
	w.Header().Set("HX-Reswap", "innerHTML")
	c.execute(w, f.Status, "group_refused", data, false)
}

// refusedSteps is what the way out names from the members (Gr-07 "What to do").
type refusedSteps struct {
	Count    int
	Clients  []string
	SameApp  bool
	Addrs    []string
	Session  string // the members' session timeout, when the coordinator holds one.
	Consumer bool   // a consumer-protocol group: the broker assigns, no session timeout of the member's.
}

func stepsOf(members []memberView, facts groupFactsView, protocol string) refusedSteps {
	s := refusedSteps{Count: len(members), Session: facts.Session, Consumer: protocol == "848"}
	seen := map[string]bool{}
	for _, m := range members {
		if m.ClientID != "" && !seen[m.ClientID] {
			seen[m.ClientID] = true
			s.Clients = append(s.Clients, m.ClientID)
		}
		switch {
		case m.Addr != "":
			s.Addrs = append(s.Addrs, m.Addr)
		case m.Host != "":
			s.Addrs = append(s.Addrs, m.Host)
		}
	}
	s.SameApp = len(s.Clients) == 1 && len(members) > 1
	return s
}

// resetConfirm is the confirm of a reset as the form stands: which sentence, and it.
type resetConfirm struct {
	id   string
	view confirmView
}

// groupResetConfirm measures the reset the form describes: the offsets it moves, from
// the same plan the preview shows.
func (c *Console) groupResetConfirm(r *http.Request, id string) (resetConfirm, error) {
	spec, err := resetSpecOf(r)
	if err != nil {
		return resetConfirm{}, err
	}
	rows, err := c.be.PlanReset(r.Context(), id, spec)
	if err != nil {
		return resetConfirm{}, err
	}
	f := confirmFacts{Name: id, Offsets: len(rows)}
	topics := map[string]bool{}
	for _, row := range rows {
		topics[row.Topic] = true
		if row.Moves && row.After != row.Committed {
			f.Moves++
		}
	}
	f.Topics = len(topics)
	if spec.Topic != "" {
		f.Topics = 1
	}
	cid := "group.reset." + spec.To
	if spec.To == "timestamp" {
		f.ResetAt = utcStamp(spec.At)
	}
	return resetConfirm{id: cid, view: viewOf(cid, f)}, nil
}
