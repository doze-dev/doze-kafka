package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// The Schemas section: the subjects in the built-in schema registry, their versions
// and how they differ, the compatibility level in force, and a way to try a new
// version against it before registering. A subject name is data like a group id, and
// travels in the path the same way (see groupSegment).

func schemasPath(prefix string) string             { return prefix + "/schemas" }
func schemaPath(prefix, subject string) string     { return prefix + "/schemas/" + groupSegment(subject) }
func newSchemaPath(prefix string) string           { return prefix + "/new/schema" }
func (c *Console) schemaURL(subject string) string { return schemaPath(c.prefix, subject) }

// registryOff is the page for a console that has no registry to talk to.
func (c *Console) registryOff(w http.ResponseWriter, r *http.Request) bool {
	if c.reg != nil {
		return false
	}
	if r.Method != http.MethodGet {
		c.fail(w, r, fmt.Errorf("%w: no schema registry is connected to this console", errNotFound))
		return true
	}
	c.render(w, r, "schemas_off", map[string]any{"Page": "schemas", "Title": "Schemas"})
	return true
}

func (c *Console) schemas(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	ctx := r.Context()
	list, err := c.reg.Subjects(ctx)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	// The home table (Sc-Home): each subject's versions, latest id, level and the topic
	// its name says it is for. One request or two per subject: it is a handful.
	topicSet := map[string]bool{}
	if ts, err := c.be.ListTopics(ctx); err == nil {
		for _, t := range ts {
			topicSet[t.Name] = true
		}
	}
	type row struct {
		SubjectRow
		Versions int
		Level    string
		OwnLevel bool
		Topic    string
	}
	rows := make([]row, 0, len(list))
	var versions int
	for _, s := range list {
		r := row{SubjectRow: s}
		if vs, err := c.reg.Versions(ctx, s.Name); err == nil {
			r.Versions = len(vs)
		}
		versions += r.Versions
		r.Level, r.OwnLevel, _ = c.reg.Compat(ctx, s.Name)
		if t := subjectTopic(s.Name); t != "" && topicSet[t] {
			r.Topic = t
		}
		rows = append(rows, r)
	}
	global, _ := c.reg.GlobalCompat(ctx)
	regURL := c.facts.RegistryURL
	if regURL == "" {
		regURL = c.reg.base
	}
	host := strings.TrimPrefix(strings.TrimPrefix(regURL, "http://"), "https://")
	c.render(w, r, "schemas_home", map[string]any{"List": list, "Rows": rows, "Page": "schemas", "Title": "Schemas",
		"VersionCount": versions, "GlobalLevel": global, "RegistryURL": regURL, "RegistryHost": host,
		"SASL": len(c.facts.SASLMechanisms) > 0 || c.be.opts.SASLUser != ""})
}

func (c *Console) schemaCreatePage(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	list, _ := c.reg.Subjects(r.Context())
	subject := r.URL.Query().Get("subject")
	c.render(w, r, "schema_create", map[string]any{"List": list, "Page": "schemas", "Title": "Register schema", "Subject": subject,
		"Types": []string{schemaregistry.TypeAvro, schemaregistry.TypeJSON, schemaregistry.TypeProtobuf}})
}

func (c *Console) schemaCreate(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	subject := strings.TrimSpace(r.FormValue("subject"))
	if subject == "" {
		c.fail(w, r, badInput("give the subject a name, such as orders-value"))
		return
	}
	body, err := schemaFromForm(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if _, err := c.reg.Register(r.Context(), subject, body); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.schemaURL(subject), "Schema registered")
}

// schemaFromForm reads a schema text, its type and any references from a form.
func schemaFromForm(r *http.Request) (schemaBody, error) {
	text := strings.TrimSpace(r.FormValue("schema"))
	if text == "" {
		return schemaBody{}, badInput("paste the schema")
	}
	b := schemaBody{Schema: text}
	if t := r.FormValue("type"); t != "" && t != schemaregistry.TypeAvro {
		b.SchemaType = t
	}
	if raw := strings.TrimSpace(r.FormValue("references")); raw != "" && raw != "null" {
		var refs []map[string]any
		if err := json.Unmarshal([]byte(raw), &refs); err != nil {
			return schemaBody{}, badInput("the references are not a JSON list: %v", err)
		}
		if len(refs) > 0 {
			b.References = refs
		}
	}
	return b, nil
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
	list, _ := c.reg.Subjects(ctx)
	level, own, _ := c.reg.Compat(ctx, subject)

	tab := r.URL.Query().Get("tab")
	if tab != "compat" && tab != "register" {
		tab = "versions"
	}
	tabURL := func(key string) string {
		if key == "versions" {
			return c.schemaURL(subject)
		}
		return c.schemaURL(subject) + "?tab=" + key
	}
	latestV := versions[len(versions)-1]
	data := map[string]any{
		"List": list, "Sel": subject, "Subject": subject, "Page": "schemas", "View": "schema", "Title": subject,
		"Versions": versions, "Cur": cur, "Text": cur.Pretty(), "Latest": latestV,
		"Compat": level, "CompatOwn": own, "Levels": compatLevels(),
		"IsLatest": cur.Version == latestV, "Tab": tab,
		"Tabs": []tabLink{
			{Key: "versions", Label: "Versions", URL: tabURL("versions"), On: tab == "versions"},
			{Key: "compat", Label: "Compatibility", URL: tabURL("compat"), On: tab == "compat"},
			{Key: "register", Label: "Register", URL: tabURL("register"), On: tab == "register"},
		},
	}
	// The versions table: each version, its id and what it changed from the one before.
	if tab == "versions" {
		type vrow struct {
			Version, ID int
			Latest      bool
			Changed     string
		}
		var vrows []vrow
		var prevText string
		for i, v := range versions {
			sv, err := c.reg.Version(ctx, subject, v)
			if err != nil {
				continue
			}
			vr := vrow{Version: v, ID: sv.ID, Latest: v == latestV}
			if i == 0 {
				vr.Changed = "first version"
			} else {
				changed := 0
				for _, l := range diffLines(prevText, sv.Pretty()) {
					if l.Op != "same" {
						changed++
					}
				}
				if changed == 0 {
					vr.Changed = "no change to the text"
				} else {
					vr.Changed = fmt.Sprintf("%d line%s changed from v%d", changed, plural(changed), versions[i-1])
				}
			}
			prevText = sv.Pretty()
			vrows = append(vrows, vr)
		}
		// Newest first, as the board lists them.
		for i, j := 0, len(vrows)-1; i < j; i, j = i+1, j-1 {
			vrows[i], vrows[j] = vrows[j], vrows[i]
		}
		data["VersionRows"] = vrows
	}
	if lv, err := c.reg.Version(ctx, subject, 0); err == nil {
		data["LatestID"] = lv.ID
	}
	// The editor for a new version starts from the latest, whichever is on show.
	if latest, err := c.reg.Version(ctx, subject, 0); err == nil {
		data["LatestText"] = latest.Pretty()
	} else {
		data["LatestText"] = cur.Pretty()
	}
	// What changed from the version before this one.
	for i, v := range versions {
		if v == cur.Version && i > 0 {
			if prev, err := c.reg.Version(ctx, subject, versions[i-1]); err == nil {
				data["Prev"] = prev
				data["Diff"] = diffLines(prev.Pretty(), cur.Pretty())
				changed := 0
				for _, l := range diffLines(prev.Pretty(), cur.Pretty()) {
					if l.Op != "same" {
						changed++
					}
				}
				data["Changed"] = changed
			}
		}
	}
	data["Confirm"] = subjectConfirms(subject, versions, cur.Version)
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
	c.render(w, r, "schema_detail", data)
}

// failSubject is failResource for a subject, which is in the Schemas section.
func (c *Console) failSubject(w http.ResponseWriter, r *http.Request, err error, subject string) {
	f := refusalFor(err)
	if !f.NotFound || !wantsPage(r) {
		c.fail(w, r, err)
		return
	}
	f.Heading = "Subject not found"
	f.Message = fmt.Sprintf("There is no subject named %q in the schema registry.", subject)
	page := errorPage{Section: "schemas", Back: schemasPath(c.prefix)}
	if rows, lerr := c.reg.Subjects(r.Context()); lerr == nil {
		var names []string
		for _, row := range rows {
			names = append(names, row.Name)
		}
		page.Suggestions = c.closeNames(names, subject, c.schemaURL)
	}
	c.renderError(w, r, f, page)
}

// schemaCheck answers, beside the form, whether the schema in it may be registered.
func (c *Console) schemaCheck(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	subject := groupFromSegment(r.PathValue("subject"))
	body, err := schemaFromForm(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	ok, reasons, err := c.reg.Check(r.Context(), subject, body)
	if err != nil {
		var re *regError
		if errors.As(err, &re) && re.Status == http.StatusUnprocessableEntity {
			// A schema that does not parse is an answer to this question, not a failure.
			c.partial(w, "schema_check", map[string]any{"Invalid": re.Message})
			return
		}
		c.fail(w, r, err)
		return
	}
	level, _, _ := c.reg.Compat(r.Context(), subject)
	c.partial(w, "schema_check", map[string]any{"OK": ok, "Reasons": reasons, "Level": level})
}

func (c *Console) schemaRegister(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	subject := groupFromSegment(r.PathValue("subject"))
	body, err := schemaFromForm(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if _, err := c.reg.Register(r.Context(), subject, body); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.schemaURL(subject), "Schema registered")
}

func (c *Console) schemaConfig(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	subject := groupFromSegment(r.PathValue("subject"))
	level := r.FormValue("level")
	var err error
	if level == "" || level == "default" {
		err = c.reg.ClearCompat(r.Context(), subject)
		var re *regError
		if errors.As(err, &re) && re.Status == http.StatusNotFound {
			err = nil // it was not set.
		}
	} else {
		err = c.reg.SetCompat(r.Context(), subject, level)
	}
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.schemaURL(subject), "Compatibility updated")
}

func (c *Console) schemaDelete(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	subject := groupFromSegment(r.PathValue("subject"))
	if v := r.FormValue("version"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			c.fail(w, r, badInput("%q is not a version", v))
			return
		}
		if err := c.reg.DeleteVersion(r.Context(), subject, n); err != nil {
			c.fail(w, r, err)
			return
		}
		if rest, err := c.reg.Versions(r.Context(), subject); err == nil && len(rest) > 0 {
			c.redirect(w, r, c.schemaURL(subject), "Version "+v+" deleted")
			return
		}
		c.redirect(w, r, schemasPath(c.prefix), "Version deleted — that was the last one, so the subject is gone")
		return
	}
	if err := c.reg.DeleteSubject(r.Context(), subject); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, schemasPath(c.prefix), "Subject deleted")
}

// topicSchemas lists the subjects that belong to a topic by the usual naming
// (<topic>-key and <topic>-value), for the topic page to link to.
type topicSchema struct {
	Label, Subject, URL string
	Version             int
	TypeLabel           string
}

func (c *Console) topicSchemas(r *http.Request, topic string) []topicSchema {
	if c.reg == nil {
		return nil
	}
	var out []topicSchema
	for _, part := range []string{"key", "value"} {
		subject := topic + "-" + part
		v, err := c.reg.Version(r.Context(), subject, 0)
		if err != nil {
			continue
		}
		out = append(out, topicSchema{Label: part, Subject: subject, URL: c.schemaURL(subject), Version: v.Version, TypeLabel: v.TypeLabel()})
	}
	return out
}

// msgView dresses a record for a table row, reading its key and value through the
// registry when they were written by a serializer that uses one.
func (c *Console) msgView(ctx context.Context, topic string, m Message) msgView {
	v := newMsgView(m)
	if c.reg == nil {
		return v
	}
	if d, ok := c.reg.decode(ctx, topic, m.Value); ok {
		v.Value.Binary = false
		v.Value.Preview = oneLine(d.JSON, previewRunes)
		v.Value.Schema = c.tag(d)
	}
	if d, ok := c.reg.decode(ctx, topic, m.Key); ok {
		v.Key.Binary = false
		v.Key.Preview = oneLine(d.JSON, previewRunes)
		v.Key.Schema = c.tag(d)
	}
	return v
}

// readThroughRegistry replaces the display of a value that a registry-aware serializer
// wrote with the JSON it decodes to.
//
//fuzz:not-a-parser hands the record value to registryClient.decode and lays out the JSON that comes back; it reads no bytes itself
func (c *Console) readThroughRegistry(ctx context.Context, topic string, v bytesView, raw []byte) bytesView {
	if c.reg == nil || v.Null || v.Empty {
		return v
	}
	d, ok := c.reg.decode(ctx, topic, raw)
	if !ok {
		return v
	}
	text := d.JSON
	if p, ok := indentJSON([]byte(d.JSON)); ok {
		text, v.Pretty = p, true
	} else {
		v.Pretty = false
	}
	v.Binary, v.Truncated = false, false
	v.Text, v.Raw, v.Preview = text, "", oneLine(d.JSON, previewRunes)
	v.Schema = c.tag(d)
	return v
}

func (c *Console) tag(d decoded) *schemaTag {
	t := &schemaTag{ID: d.ID, Type: d.Type, TypeLabel: d.TypeLbl, Subject: d.Subject, Version: d.Version}
	if d.Subject != "" {
		t.URL = c.schemaURL(d.Subject) + "?version=" + strconv.Itoa(d.Version)
	}
	return t
}

// composerSchemas adds the subjects the composer can encode a value with, and the one
// to start on: the topic's own value subject, when it has one.
func (c *Console) composerSchemas(r *http.Request, topic string, data map[string]any) {
	if c.reg == nil {
		return
	}
	names := c.reg.SubjectNames(r.Context())
	if len(names) == 0 {
		return
	}
	data["SchemaSubjects"] = names
	for _, n := range names {
		if n == topic+"-value" {
			data["DefaultSubject"] = n
		}
	}
}
