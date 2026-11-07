package console

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// One subject (Sc-05, and its Protobuf, JSON Schema and registered-by-a-client
// faces, Sc-07, Sc-08 and Sc-18): its versions, what each changed, when it was
// registered and whether it passes the level against the one before; the version on
// show with its text, a place to try a value against it, and the version open in
// the inspector (?open=version:<subject>:<n>). Tabs: Versions, Compatibility
// (Sc-09), Used by (Sc-11), Raw.

// consoleAgent is the User-Agent the console's registry calls carry, so a version it
// registered is told from one a client's serializer did (Sc-18).
const consoleAgent = "doze-kafka-console"

// versionRow is one row of the versions table.
type versionRow struct {
	Version, ID int
	Latest, On  bool
	Changed     []changePart // what changed, in phrases; Text when they cannot be named.
	Text        string
	At          time.Time // registered; zero when not kept.
	Checked     string    // "v2, BACKWARD ok"; "" for the first version.
	CheckedOK   bool
	ByClient    bool   // registered by a client's serializer, not here.
	By          string // who: the user signed in as, else the client's software.
	Shape       string // Sc-18's "AuditEvent: actor string, …".
	Open        string // the version's inspector key.
	URL         string
}

// subjectTabs are the subject's tabs (Sc-05).
var subjectTabs = []struct{ Key, Label string }{
	{"versions", "Versions"}, {"compat", "Compatibility"}, {"used", "Used by"}, {"raw", "Raw"},
}

func (c *Console) schemaDetail(w http.ResponseWriter, r *http.Request) {
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
	want := 0
	if n, err := strconv.Atoi(r.URL.Query().Get("version")); err == nil {
		want = n
	}
	cur, err := c.reg.Version(ctx, subject, want)
	if err != nil {
		c.failSubject(w, r, err, subject)
		return
	}
	tab := r.URL.Query().Get("tab")
	if tab == "register" {
		// The register form is its own page now (Sc-03).
		http.Redirect(w, r, newSchemaPath(c.prefix)+"?subject="+url.QueryEscape(subject), http.StatusSeeOther)
		return
	}
	known := false
	for _, t := range subjectTabs {
		known = known || t.Key == tab
	}
	if !known {
		tab = "versions"
	}
	list, _ := c.reg.Subjects(ctx)
	level, own, _ := c.reg.Compat(ctx, subject)
	mode, modeOwn, _ := c.reg.SubjectMode(ctx, subject)
	latest := versions[len(versions)-1]
	shape := shapeOf(cur)

	var tabs []tabLink
	for _, t := range subjectTabs {
		u := c.schemaURL(subject)
		if t.Key != "versions" {
			u += "?tab=" + t.Key
		}
		tabs = append(tabs, tabLink{Key: t.Key, Label: t.Label, URL: u, On: t.Key == tab})
	}
	data := map[string]any{
		"List": list, "Sel": subject, "Subject": subject, "Page": "schemas", "View": "schema", "Title": subject,
		"Versions": versions, "Cur": cur, "Text": cur.Pretty(), "Latest": latest, "IsLatest": cur.Version == latest,
		"Compat": level, "CompatOwn": own, "Levels": compatLevels(), "Mode": mode, "ModeOwn": modeOwn,
		"Tab": tab, "Tabs": tabs, "Shape": shape, "NewVersionURL": newSchemaPath(c.prefix) + "?subject=" + url.QueryEscape(subject),
	}
	if lv, err := c.reg.Version(ctx, subject, 0); err == nil {
		data["LatestID"] = lv.ID
	}
	switch tab {
	case "versions":
		rows := c.versionRows(ctx, subject, versions, level, cur.Version)
		data["VersionRows"] = rows
		// The latest version's row says when, and Sc-18's face is a subject whose one
		// version a client's serializer registered.
		for _, row := range rows {
			if row.Version == latest {
				data["LastAt"] = row.At
				if len(rows) == 1 && row.ByClient {
					data["ByClient"], data["By"] = true, row.By
				}
			}
		}
		for i, v := range versions {
			if v == cur.Version && i > 0 {
				data["Prev"] = versions[i-1]
			}
		}
		data["TryDoc"] = sampleDoc(cur, shape)
	case "raw":
		var raw bytes.Buffer
		b, _ := json.Marshal(cur)
		if json.Indent(&raw, b, "", "  ") == nil {
			data["Raw"] = raw.String()
		}
		data["RawPath"] = fmt.Sprintf("GET /subjects/%s/versions/%d", subject, cur.Version)
	}
	// The topic this subject is named for, when there is one.
	if topics, err := c.be.ListTopics(ctx); err == nil {
		var names []string
		for _, t := range topics {
			names = append(names, t.Name)
		}
		g := NewGraph()
		g.AddSubjects([]string{subject}, names)
		data["Neighbours"] = c.neighbourClauses(g, NodeSubject, subject)
	}
	data["Confirm"] = subjectConfirms(subject, versions, cur.Version)
	switch {
	case data["ByClient"] == true || cur.Type() == schemaregistry.TypeJSON:
		data["View"] = "schema_one"
	case cur.Type() == schemaregistry.TypeProtobuf:
		data["View"] = "schema_proto"
	}
	maps.Copy(data, c.dockData(DockFilter{Subject: subject}))
	c.render(w, r, "schema_detail", data)
}

// versionRows is the versions table, newest first: what each changed, when it was
// registered, and whether it passes the subject's level against the version before.
func (c *Console) versionRows(ctx context.Context, subject string, versions []int, level string, on int) []versionRow {
	latest := versions[len(versions)-1]
	rows := make([]versionRow, 0, len(versions))
	var prev schemaVersion
	for i, n := range versions {
		v, err := c.reg.Version(ctx, subject, n)
		if err != nil {
			continue
		}
		row := versionRow{Version: n, ID: v.ID, Latest: n == latest, On: n == on,
			Open: joinKey(subject, strconv.Itoa(n)), Shape: shapeLine(shapeOf(v))}
		row.URL = c.schemaURL(subject) + "?version=" + strconv.Itoa(n) + "&open=" + url.QueryEscape(openParam(KindVersion, row.Open))
		if info, ok := c.reg.VersionInfo(ctx, subject, n); ok {
			if info.RegisteredAt != nil {
				row.At = *info.RegisteredAt
				row.ByClient = info.Agent != "" && !strings.HasPrefix(info.Agent, consoleAgent)
			}
			row.By = registeredBy(info)
		}
		if i == 0 {
			row.Text = firstVersionSummary(shapeOf(v))
		} else {
			row.Changed = changeSummary(v.Type(), shapeOf(prev), shapeOf(v))
			if row.Changed == nil {
				changed := 0
				for _, l := range diffLines(prev.Pretty(), v.Pretty()) {
					if l.Op != "same" {
						changed++
					}
				}
				if changed == 0 {
					row.Text = "no change to the text"
				} else {
					row.Text = fmt.Sprintf("%d line%s changed", changed, plural(changed))
				}
			}
			// Checked against the version before, at the level in force now.
			if level == "NONE" {
				row.Checked = fmt.Sprintf("v%d, NONE: not checked", versions[i-1])
			} else if a, err := c.reg.CheckAt(ctx, subject, schemaBody{Schema: v.Schema, SchemaType: v.SchemaType, References: refsOf(v)}, "", strconv.Itoa(versions[i-1]), false); err == nil {
				row.CheckedOK = a.Compatible
				row.Checked = fmt.Sprintf("v%d, %s %s", versions[i-1], strings.TrimSuffix(a.Level, "_TRANSITIVE"), map[bool]string{true: "ok", false: "refused"}[a.Compatible])
			}
		}
		prev = v
		rows = append(rows, row)
	}
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows
}

// refsOf is a version's references as the registry takes them in a body.
func refsOf(v schemaVersion) any {
	if len(v.References) == 0 {
		return nil
	}
	return v.References
}

// registeredBy is who registered a version: the user it signed in as, else the
// software its User-Agent names (its first word, "Java/21" or
// "confluent-kafka-python/2.6.0"), else where it came from.
func registeredBy(info schemaregistry.VersionInfo) string {
	switch {
	case info.RegisteredBy != "":
		return info.RegisteredBy
	case info.Agent != "" && !strings.HasPrefix(info.Agent, consoleAgent):
		agent, _, _ := strings.Cut(info.Agent, " ")
		return agent
	case info.Agent != "":
		return "this console"
	}
	return info.From
}

// sampleDoc is a value to start Try it with: the schema's fields with values of
// their types.
func sampleDoc(v schemaVersion, s schemaShape) string {
	if !s.ok {
		return "{}"
	}
	var b strings.Builder
	b.WriteString("{")
	for i, f := range s.Fields {
		if i > 0 {
			b.WriteString(",")
		}
		key := f.Name
		if v.Type() == schemaregistry.TypeProtobuf {
			key = protoJSONName(f.Name)
		}
		fmt.Fprintf(&b, "%q:%s", key, sampleValue(f))
	}
	b.WriteString("}")
	return b.String()
}

func sampleValue(f schemaField) string {
	if f.HasDefault {
		if f.Default == "null" {
			return "null"
		}
		if _, err := strconv.ParseFloat(f.Default, 64); err == nil || f.Default == "true" || f.Default == "false" {
			return f.Default
		}
		return strconv.Quote(f.Default)
	}
	t, _, _ := strings.Cut(f.Type, " or ")
	t, _, _ = strings.Cut(t, ",")
	switch t {
	case "null":
		return "null"
	case "int", "long", "int32", "int64", "uint32", "uint64", "sint32", "sint64", "integer", "number", "float", "double":
		return "0"
	case "boolean", "bool":
		return "false"
	}
	return `""`
}

// protoJSONName is a Protobuf field's JSON name (lowerCamelCase).
func protoJSONName(s string) string {
	parts := strings.Split(s, "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

// schemaTry answers Try it (Sc-05, Sc-07, Sc-08): the bytes a serializer would send
// for a JSON value under a version, or whether the value fits it.
func (c *Console) schemaTry(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	ctx := r.Context()
	subject := groupFromSegment(r.PathValue("subject"))
	n, _ := strconv.Atoi(r.FormValue("version"))
	v, err := c.reg.Version(ctx, subject, n)
	if err != nil {
		c.failSubject(w, r, err, subject)
		return
	}
	doc := strings.TrimSpace(r.FormValue("doc"))
	out := map[string]any{"Version": v.Version, "ID": v.ID, "Do": r.FormValue("do")}
	refs, err := c.reg.resolve(ctx, v)
	if err == nil {
		var codec *schemaregistry.Codec
		if codec, err = schemaregistry.NewCodec(v.Type(), v.Schema, refs); err == nil {
			var b []byte
			if b, err = codec.Encode(v.ID, doc); err == nil {
				out["Bytes"], out["Len"] = hexBytes(b), len(b)
			}
		}
	}
	if err != nil {
		out["Err"] = err.Error()
	}
	c.partial(w, "schema_try", out)
}

// hexBytes is bytes as hex pairs, the first five (magic byte and id) apart.
func hexBytes(b []byte) string {
	var s strings.Builder
	for i, x := range b {
		if i > 0 {
			s.WriteByte(' ')
		}
		if i == 5 {
			s.WriteString("| ")
		}
		fmt.Fprintf(&s, "%02x", x)
	}
	return s.String()
}

// versionInsp is the version open in the inspector (Sc-05's right).
func (c *Console) inspectVersion(w http.ResponseWriter, r *http.Request, key string) {
	if c.registryOff(w, r) {
		return
	}
	parts := splitKey(key, 2)
	n, err := 0, error(nil)
	if parts != nil {
		n, err = strconv.Atoi(parts[1])
	}
	if parts == nil || err != nil || n <= 0 {
		c.fail(w, r, badInput("a version is named by its subject and its number"))
		return
	}
	ctx := r.Context()
	subject := parts[0]
	v, err := c.reg.Version(ctx, subject, n)
	if err != nil {
		c.failSubject(w, r, err, subject)
		return
	}
	body := map[string]any{"V": v, "Subject": subject, "SubjectURL": c.schemaURL(subject)}
	if info, ok := c.reg.VersionInfo(ctx, subject, n); ok && info.RegisteredAt != nil {
		body["At"], body["By"] = *info.RegisteredAt, registeredBy(info)
	}
	// What references it, and which other subjects hold the same schema.
	if ids, err := c.reg.ReferencedBy(ctx, subject, n); err == nil {
		var refs []subjectVersion
		for _, id := range ids {
			if at, err := c.reg.IDVersions(ctx, id); err == nil {
				refs = append(refs, at...)
			}
		}
		body["ReferencedBy"] = refs
	}
	if at, err := c.reg.IDVersions(ctx, v.ID); err == nil {
		var others []subjectVersion
		for _, sv := range at {
			if sv.Subject != subject {
				others = append(others, sv)
			}
		}
		body["SameIn"] = others
	}
	body["Wire"] = []string{fmt.Sprintf("GET /subjects/%s/versions/%d", subject, n), fmt.Sprintf("GET /schemas/ids/%d/subjects", v.ID)}
	// The messages on this id, in the topic the subject is named for.
	if topic := subjectTopic(subject); topic != "" {
		body["Topic"], body["TopicURL"] = topic, c.topicURL(topic)
		if scan, err := c.scanSchemaIDs(ctx, topic); err == nil {
			body["Scanned"] = scan.Scanned
			if u := scan.idUse(v.ID); u != nil {
				body["Use"] = u
				body["LastURL"] = c.topicURL(topic) + "/message?partition=" + strconv.Itoa(int(u.Last.Partition)) + "&offset=" + strconv.FormatInt(u.Last.Offset, 10)
			}
		}
	}
	versions, _ := c.reg.Versions(ctx, subject)
	for i, x := range versions {
		if x == n && i > 0 {
			body["Prev"] = versions[i-1]
			body["DiffURL"] = c.schemaURL(subject) + "/diff?from=" + strconv.Itoa(versions[i-1]) + "&to=" + strconv.Itoa(n)
		}
	}
	body["Confirm"] = subjectConfirms(subject, versions, n)
	body["Many"] = len(versions) > 1
	c.inspect(w, inspectorView{Kind: KindVersion, Key: key, Title: fmt.Sprintf("%s v%d", subject, n), Body: body})
}

// changedHTML is a version's change phrases with the field names in code type.
func changedHTML(parts []changePart) template.HTML {
	var b strings.Builder
	for i, p := range parts {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(template.HTMLEscapeString(p.Before))
		if p.Name != "" {
			b.WriteString(`<span class="m">` + template.HTMLEscapeString(p.Name) + `</span>`)
		}
		b.WriteString(template.HTMLEscapeString(p.After))
	}
	return template.HTML(b.String())
}
