package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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
	c.render(w, r, "schemas_off", map[string]any{"Page": "schemas", "Title": "Schemas", "Reason": c.regOff})
	return true
}

func (c *Console) schemas(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	ctx := r.Context()
	list, ids, err := c.reg.Catalogue(ctx)
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
		r := row{SubjectRow: s, Versions: s.Versions}
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
	mode, _ := c.reg.Mode(ctx)
	data := map[string]any{"List": list, "Rows": rows, "Page": "schemas", "Title": "Schemas",
		"VersionCount": versions, "GlobalLevel": global, "RegistryURL": regURL, "RegistryHost": host, "Mode": mode,
		"BuiltIn": c.facts.RegistryBuiltIn, "IDs": idSummary(ids), "Bootstrap": c.be.displayAddr(),
		"SASL": len(c.facts.SASLMechanisms) > 0 || c.be.opts.SASLUser != ""}
	if len(rows) == 0 {
		data["View"] = "schemas_none"
		data["ProducerProps"], data["SampleTopic"] = c.producerProps(r), "<topic>"
		if tps, err := c.be.ListTopics(ctx); err == nil {
			for _, t := range tps {
				if !strings.HasPrefix(t.Name, "__") {
					data["SampleTopic"] = t.Name
					break
				}
			}
		}
	}
	// Subjects a client soft-deleted (SCH-17): no longer listed or looked up, their text
	// still held until they are deleted for good.
	if deleted, err := c.reg.DeletedSubjects(ctx); err == nil && len(deleted) > 0 {
		type gone struct {
			Name    string
			Confirm confirmView
		}
		var rows []gone
		for _, d := range deleted {
			f := confirmFacts{Name: d, SoftDeleted: true}
			if vs, err := c.reg.AllVersions(ctx, d); err == nil {
				f.Versions = len(vs)
			}
			rows = append(rows, gone{Name: d, Confirm: viewOf("schema.delete", f)})
		}
		data["Deleted"] = rows
	}
	maps.Copy(data, c.dockData(DockFilter{Registry: true}))
	c.render(w, r, "schemas_home", data)
}

// propLine is one line of a properties file: a key and its value, or a comment.
type propLine struct {
	Key, Value string
	Comment    bool
}

// producerProps is Sc-01's producer.properties: this broker's client settings (the
// Connect page's, so the login is the same), the registry's address, and the value
// serializer, with the default it relies on said.
func (c *Console) producerProps(r *http.Request) []propLine {
	f := c.connectFacts(r)
	out := []propLine{{Key: "# producer.properties", Comment: true}}
	for _, p := range f.Properties {
		if strings.HasPrefix(p, "#") {
			out = append(out, propLine{Key: p, Comment: true})
			continue
		}
		k, v, _ := strings.Cut(p, "=")
		out = append(out, propLine{Key: k, Value: v})
		if k == "bootstrap.servers" && f.Registry != "" {
			out = append(out, propLine{Key: "schema.registry.url", Value: f.Registry})
		}
	}
	return append(out, propLine{Key: "value.serializer", Value: "io.confluent.kafka.serializers.KafkaAvroSerializer"},
		propLine{Key: "# auto.register.schemas=true is the default", Comment: true})
}

// idSpan is Sc-Home's list foot: how many schemas, the ids they span, and the ids in
// that span no version uses ("ids 1 to 10. Id 9 is not in use here.").
type idSpan struct {
	Count, From, To int
	Unused          []int
	More            int // unused ids past the ones named.
}

func idSummary(ids []int) idSpan {
	s := idSpan{Count: len(ids)}
	if len(ids) == 0 {
		return s
	}
	s.From, s.To = ids[0], ids[len(ids)-1]
	used := map[int]bool{}
	for _, id := range ids {
		used[id] = true
	}
	for id := s.From; id <= s.To; id++ {
		if !used[id] {
			if len(s.Unused) < 3 {
				s.Unused = append(s.Unused, id)
			} else {
				s.More++
			}
		}
	}
	return s
}

// schemaFromForm reads a schema text, its type and any references from a form.
//
//fuzz:parser class=console-forms FuzzSchemaForms
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

// schemaDelete is Sc-12's two deletes. A version is soft-deleted: it leaves the
// list and lookups, and its id still resolves, so every message written with it
// still decodes. A subject is deleted for good, which is two calls in the order the
// registry asks for them (a soft delete, then the permanent one), and only when its
// name was typed to confirm it (confirm=<subject>). A subject a client already
// soft-deleted (SCH-17) takes the permanent call alone.
func (c *Console) schemaDelete(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	ctx := r.Context()
	subject := groupFromSegment(r.PathValue("subject"))
	if v := r.FormValue("version"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			c.fail(w, r, badInput("%q is not a version", v))
			return
		}
		if err := c.reg.SoftDeleteVersion(ctx, subject, n); err != nil {
			c.fail(w, r, err)
			return
		}
		if rest, err := c.reg.Versions(ctx, subject); err == nil && len(rest) > 0 {
			c.redirect(w, r, c.schemaURL(subject), "Version "+v+" soft-deleted; its id still resolves")
			return
		}
		c.redirect(w, r, schemasPath(c.prefix), "Version "+v+" soft-deleted: it was the last one, so the subject is listed under Soft-deleted")
		return
	}
	if r.FormValue("confirm") != subject {
		c.fail(w, r, badInput("type the subject's name to delete it for good"))
		return
	}
	if err := c.reg.SoftDeleteSubject(ctx, subject); err != nil {
		var re *regError
		// 40404: a client soft-deleted it already; the permanent call is what is left.
		if !errors.As(err, &re) || re.Code != 40404 {
			c.fail(w, r, err)
			return
		}
	}
	if err := c.reg.PermanentDeleteSubject(ctx, subject); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, schemasPath(c.prefix), "Subject deleted for good")
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
	if v.Null || v.Empty {
		return v
	}
	first := fmt.Sprintf("0x%02x", raw[0])
	if c.reg == nil {
		if v.Binary {
			v.Frame = &frameView{Kind: "noregistry", First: first}
		}
		return v
	}
	d, state, derr := c.reg.decodeState(ctx, topic, raw)
	switch state {
	case notFramed:
		if v.Binary {
			v.Frame = &frameView{Kind: "none", First: first}
		}
		return v
	case framedUnknown:
		v.Frame = &frameView{Kind: "unknown", First: first, ID: d.ID, Err: derr.Error()}
		return v
	case framedUnread:
		v.Frame = &frameView{Kind: "unread", First: first, ID: d.ID, Err: derr.Error()}
		return v
	case framedMismatch:
		v.Frame = &frameView{Kind: "mismatch", First: first, ID: d.ID, Err: derr.Error()}
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
	if pr, ok := data["PartitionRange"].([]int32); ok {
		data["PartNote"] = c.partitionNote(int32(len(pr)), nil)
	}
	if c.reg == nil {
		return
	}
	names := c.reg.SubjectNames(r.Context())
	if len(names) == 0 {
		return
	}
	data["SchemaSubjects"] = names
	for _, n := range names {
		if n == topic+valueSubjectSuffix {
			data["DefaultSubject"] = n
		}
	}
}
