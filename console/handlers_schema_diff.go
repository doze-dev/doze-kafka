package console

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Sc-06: two versions of a subject side by side (or unified), what the change means
// for readers, and whether it would pass at the other levels. Nothing is changed:
// the checks at other levels leave the subject's level as it is (/_doze/ check).

// diffRow is one row of the side-by-side table: a line of the older version, of the
// newer one, or both (the same, or a line changed in place).
type diffRow struct {
	OldN, NewN   int // line numbers; 0 when the side has no line here.
	Old, New     string
	OldOp, NewOp string // "same", "del", "add"; "" for an empty side.
	Changed      bool
	UnifiedOp    string
	UnifiedText  string
	UnifiedN     int
}

// pairDiff turns a line diff into side-by-side rows: runs of removed lines are put
// beside the added lines that follow them, line for line, as changed lines.
func pairDiff(lines []diffLine) (rows []diffRow, added, removed, changed int) {
	oldN, newN := 0, 0
	for i := 0; i < len(lines); {
		if lines[i].Op == "same" {
			oldN++
			newN++
			rows = append(rows, diffRow{OldN: oldN, NewN: newN, Old: lines[i].Text, New: lines[i].Text, OldOp: "same", NewOp: "same"})
			i++
			continue
		}
		var dels, adds []string
		for i < len(lines) && lines[i].Op == "del" {
			dels = append(dels, lines[i].Text)
			i++
		}
		for i < len(lines) && lines[i].Op == "add" {
			adds = append(adds, lines[i].Text)
			i++
		}
		for j := 0; j < max(len(dels), len(adds)); j++ {
			r := diffRow{}
			if j < len(dels) {
				oldN++
				r.OldN, r.Old, r.OldOp = oldN, dels[j], "del"
			}
			if j < len(adds) {
				newN++
				r.NewN, r.New, r.NewOp = newN, adds[j], "add"
			}
			switch {
			case r.OldOp != "" && r.NewOp != "":
				r.Changed = true
				changed++
			case r.OldOp != "":
				removed++
			default:
				added++
			}
			rows = append(rows, r)
		}
	}
	return rows, added, removed, changed
}

// otherLevels are the levels Sc-06 checks a change at besides the subject's own.
var otherLevels = []string{"BACKWARD", "FORWARD", "FULL"}

func (c *Console) schemaDiff(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	ctx := r.Context()
	subject := groupFromSegment(r.PathValue("subject"))
	versions, err := c.reg.Versions(ctx, subject)
	if err != nil {
		c.failSubject(w, r, err, subject)
		return
	}
	// The pair: ?from and ?to, else the latest and the one before it.
	q := r.URL.Query()
	to, _ := strconv.Atoi(q.Get("to"))
	from, _ := strconv.Atoi(q.Get("from"))
	idx := func(v int) int {
		for i, x := range versions {
			if x == v {
				return i
			}
		}
		return -1
	}
	if idx(to) < 0 {
		to = versions[len(versions)-1]
	}
	if idx(from) < 0 || from == to {
		if i := idx(to); i > 0 {
			from = versions[i-1]
		} else if len(versions) > 1 {
			from = versions[1]
		} else {
			from = to
		}
	}
	older, err := c.reg.Version(ctx, subject, from)
	if err != nil {
		c.failSubject(w, r, err, subject)
		return
	}
	newer, err := c.reg.Version(ctx, subject, to)
	if err != nil {
		c.failSubject(w, r, err, subject)
		return
	}
	unified := q.Get("layout") == "unified"
	lines := diffLines(older.Pretty(), newer.Pretty())
	rows, added, removed, changedN := pairDiff(lines)
	// Unified: one column, the old line numbers for removed, the new for the rest.
	var urows []diffRow
	oldN, newN := 0, 0
	for _, l := range lines {
		switch l.Op {
		case "del":
			oldN++
			urows = append(urows, diffRow{UnifiedOp: "del", UnifiedText: l.Text, UnifiedN: oldN})
		case "add":
			newN++
			urows = append(urows, diffRow{UnifiedOp: "add", UnifiedText: l.Text, UnifiedN: newN})
		default:
			oldN++
			newN++
			urows = append(urows, diffRow{UnifiedOp: "same", UnifiedText: l.Text, UnifiedN: newN})
		}
	}

	list, _ := c.reg.Subjects(ctx)
	level, own, _ := c.reg.Compat(ctx, subject)
	body := schemaBody{Schema: newer.Schema, SchemaType: newer.SchemaType, References: refsOf(newer)}
	verdict := map[string]checkAnswer{}
	for _, l := range append([]string{""}, otherLevels...) {
		if a, err := c.reg.CheckAt(ctx, subject, body, l, strconv.Itoa(from), false); err == nil {
			verdict[l] = a
		}
	}
	parts := changeSummary(newer.Type(), shapeOf(older), shapeOf(newer))
	pairURL := func(a, b int) string {
		u := fmt.Sprintf("%s/diff?from=%d&to=%d", c.schemaURL(subject), a, b)
		if unified {
			u += "&layout=unified"
		}
		return u
	}
	data := map[string]any{
		"List": list, "Sel": subject, "Subject": subject, "Page": "schemas", "View": "schema_diff", "Title": subject + " v" + strconv.Itoa(from) + " to v" + strconv.Itoa(to),
		"Versions": versions, "Cur": newer, "From": older, "To": newer, "Rows": rows, "URows": urows, "Unified": unified,
		"Added": added, "Removed": removed, "ChangedLines": changedN, "Compat": level, "CompatOwn": own,
		"Own": verdict[""], "Meaning": readersMeaning(newer.Type(), parts, from, to),
		"Others": otherLevelsSentence(verdict, level, to), "Allowed": allowedNote(verdict[""], parts),
		"NewVersionURL": newSchemaPath(c.prefix) + "?subject=" + url.QueryEscape(subject),
		"SideURL":       fmt.Sprintf("%s/diff?from=%d&to=%d", c.schemaURL(subject), from, to), "UnifiedURL": fmt.Sprintf("%s/diff?from=%d&to=%d&layout=unified", c.schemaURL(subject), from, to),
		"BackURL": c.schemaURL(subject),
	}
	if i := idx(from); i > 0 {
		data["OlderURL"] = pairURL(versions[i-1], versions[i])
	}
	if i := idx(to); i >= 0 && i+1 < len(versions) {
		data["NewerURL"] = pairURL(versions[i], versions[i+1])
	}
	data["FromAt"], data["ToAt"] = c.registeredAt(ctx, subject, from), c.registeredAt(ctx, subject, to)
	var tabs []tabLink
	for _, t := range subjectTabs {
		u := c.schemaURL(subject)
		if t.Key != "versions" {
			u += "?tab=" + t.Key
		}
		tabs = append(tabs, tabLink{Key: t.Key, Label: t.Label, URL: u, On: t.Key == "versions"})
	}
	data["Tabs"] = tabs
	if topics, err := c.be.ListTopics(ctx); err == nil {
		var names []string
		for _, t := range topics {
			names = append(names, t.Name)
		}
		g := NewGraph()
		g.AddSubjects([]string{subject}, names)
		data["Neighbours"] = c.neighbourClauses(g, NodeSubject, subject)
	}
	data["Confirm"] = subjectConfirms(subject, versions, to)
	maps.Copy(data, c.dockData(DockFilter{Subject: subject}))
	c.render(w, r, "schema_diff", data)
}

// registeredAt is when a version was registered, or zero.
func (c *Console) registeredAt(ctx context.Context, subject string, version int) time.Time {
	if info, ok := c.reg.VersionInfo(ctx, subject, version); ok && info.RegisteredAt != nil {
		return *info.RegisteredAt
	}
	return time.Time{}
}

// allowedNote is the facts line's last clause: "the change is allowed at BACKWARD:
// the new field has a default".
func allowedNote(a checkAnswer, parts []changePart) string {
	if a.Level == "" {
		return ""
	}
	if a.Level == "NONE" {
		return "the level is NONE: any change is allowed"
	}
	if !a.Compatible {
		why := ""
		if len(a.Findings) > 0 && len(a.Findings[0].Reasons) > 0 {
			why = ": " + a.Findings[0].Reasons[0].Type
		}
		return "the change would be refused at " + a.Level + why
	}
	note := "the change is allowed at " + a.Level
	defaults := 0
	for _, p := range parts {
		if strings.HasPrefix(p.Before, "added") && (strings.Contains(p.After, "default") || strings.HasPrefix(p.Before, "added optional")) {
			defaults++
		}
	}
	if defaults > 0 && defaults == len(parts) {
		if defaults == 1 {
			return note + ": the new field has a default"
		}
		return note + ": the new fields have defaults"
	}
	return note
}

// readersMeaning is "What this means for readers": for fields added with a
// default, what an old and a new reader each do.
func readersMeaning(typ string, parts []changePart, from, to int) string {
	var added, removed []string
	for _, p := range parts {
		switch {
		case strings.HasPrefix(p.Before, "added"):
			added = append(added, p.Name)
		case strings.HasPrefix(p.Before, "removed"):
			removed = append(removed, p.Name)
		}
	}
	switch {
	case len(parts) == 0:
		return fmt.Sprintf("v%d and v%d differ in their text only: readers on either read both alike.", from, to)
	case len(added) == len(parts) && typ == "PROTOBUF":
		return fmt.Sprintf("A reader on v%d reading a v%d message sees %s unset. A reader still on v%d skips the new field numbers.", to, from, strings.Join(added, ", "), from)
	case len(added) == len(parts):
		return fmt.Sprintf("A consumer on v%d reading a v%d message fills %s from the default. A consumer still on v%d reading a v%d message skips what it does not know.", to, from, strings.Join(added, ", "), from, to)
	case len(removed) == len(parts):
		return fmt.Sprintf("A consumer on v%d reading a v%d message skips %s. A consumer still on v%d reading a v%d message needs a default for it.", to, from, strings.Join(removed, ", "), from, to)
	}
	return fmt.Sprintf("Fields were added, removed or changed between v%d and v%d: whether each reader copes is what the level checks.", from, to)
}

// otherLevelsSentence is "Check this change at another level": the subject's own
// verdict, then the others.
func otherLevelsSentence(v map[string]checkAnswer, level string, to int) string {
	own, ok := v[""]
	if !ok {
		return ""
	}
	var b strings.Builder
	if own.Compatible {
		fmt.Fprintf(&b, "At %s, the subject's level, v%d passes.", level, to)
	} else {
		fmt.Fprintf(&b, "At %s, the subject's level, v%d is refused.", level, to)
	}
	for _, l := range otherLevels {
		if l == level {
			continue
		}
		a, ok := v[l]
		if !ok {
			continue
		}
		if a.Compatible {
			fmt.Fprintf(&b, " At %s it would pass.", l)
		} else if len(a.Findings) > 0 && len(a.Findings[0].Reasons) > 0 {
			fmt.Fprintf(&b, " At %s it would be refused (%s).", l, a.Findings[0].Reasons[0].Type)
		} else {
			fmt.Fprintf(&b, " At %s it would be refused.", l)
		}
	}
	return b.String()
}
