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

// Registering (Sc-02 to Sc-04, Sc-10): one page for a new subject and for a new
// version of one, a form with the subject, the type, the schema, its references and
// normalize. Check asks the registry whether the schema may be registered and says
// why not, field by field, with a fix where there is one; Register sends it. A
// refusal is drawn where the check's answer is (Sc-04), with nothing stored. A draft
// with references shows them, and the schema as a client sees it with the
// referenced types written in (the resolved view).

// refRow is one reference of a draft.
type refRow struct {
	Name, Subject string
	Version       int
}

// regDraft is the register form as it was sent.
type regDraft struct {
	Subject, Type, Schema string
	Refs                  []refRow
	Normalize             bool
}

func (d regDraft) body() schemaBody {
	b := schemaBody{Schema: d.Schema}
	if d.Type != "" && d.Type != schemaregistry.TypeAvro {
		b.SchemaType = d.Type
	}
	if len(d.Refs) > 0 {
		var refs []map[string]any
		for _, r := range d.Refs {
			refs = append(refs, map[string]any{"name": r.Name, "subject": r.Subject, "version": r.Version})
		}
		b.References = refs
	}
	return b
}

// draftFromForm reads the register form: the fields schemaFromForm reads, and the
// references as rows (ref_name, ref_subject, ref_version, one of each per row).
func draftFromForm(r *http.Request) (regDraft, error) {
	b, err := schemaFromForm(r)
	if err != nil {
		return regDraft{}, err
	}
	d := regDraft{Subject: strings.TrimSpace(r.FormValue("subject")), Schema: b.Schema, Type: b.SchemaType, Normalize: r.FormValue("normalize") != ""}
	if d.Type == "" {
		d.Type = schemaregistry.TypeAvro
	}
	if refs, ok := b.References.([]map[string]any); ok {
		for _, m := range refs {
			row := refRow{}
			row.Name, _ = m["name"].(string)
			row.Subject, _ = m["subject"].(string)
			if v, ok := m["version"].(float64); ok {
				row.Version = int(v)
			}
			d.Refs = append(d.Refs, row)
		}
	}
	names, subjects, versions := r.Form["ref_name"], r.Form["ref_subject"], r.Form["ref_version"]
	for i := range names {
		row := refRow{Name: strings.TrimSpace(names[i])}
		if i < len(subjects) {
			row.Subject = strings.TrimSpace(subjects[i])
		}
		if i < len(versions) {
			row.Version, _ = strconv.Atoi(strings.TrimSpace(versions[i]))
		}
		if row.Name == "" && row.Subject == "" {
			continue // an empty row the person added and left
		}
		if row.Name == "" || row.Subject == "" || row.Version <= 0 {
			return regDraft{}, badInput("a reference needs the name the schema uses, a subject and a version")
		}
		d.Refs = append(d.Refs, row)
	}
	return d, nil
}

// regPage is what the register page and its answers read about the subject.
type regPage struct {
	Draft    regDraft
	Existing bool
	Latest   int // the latest version, and the one Register makes is Latest+1.
	LatestID int
	Level    string
	Own      bool
	NextID   int
	Topic    string // the topic the subject is named for, and whether the broker has it.
	TopicOK  bool
	Mode     string // the subject's mode, and whether it is its own (Sc-15).
	ModeOwn  bool
}

func (c *Console) regPage(ctx context.Context, d regDraft) regPage {
	p := regPage{Draft: d}
	if o, ok := c.reg.Overview(ctx); ok {
		p.NextID = o.NextID
	}
	if d.Subject != "" {
		if v, err := c.reg.Version(ctx, d.Subject, 0); err == nil {
			p.Existing, p.Latest, p.LatestID = true, v.Version, v.ID
		}
		p.Level, p.Own, _ = c.reg.Compat(ctx, d.Subject)
		p.Mode, p.ModeOwn, _ = c.reg.SubjectMode(ctx, d.Subject)
		if t := subjectTopic(d.Subject); t != "" {
			p.Topic = t
			if topics, err := c.be.ListTopics(ctx); err == nil {
				for _, x := range topics {
					p.TopicOK = p.TopicOK || x.Name == t
				}
			}
		}
	}
	return p
}

func (c *Console) schemaCreatePage(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	ctx := r.Context()
	list, _ := c.reg.Subjects(ctx)
	d := regDraft{Subject: strings.TrimSpace(r.URL.Query().Get("subject")), Type: schemaregistry.TypeAvro}
	// A new version starts from the latest, with its type and references.
	if d.Subject != "" {
		if v, err := c.reg.Version(ctx, d.Subject, 0); err == nil {
			d.Type, d.Schema = v.Type(), v.Pretty()
			for _, ref := range v.References {
				d.Refs = append(d.Refs, refRow{Name: ref.Name, Subject: ref.Subject, Version: ref.Version})
			}
		}
	}
	p := c.regPage(ctx, d)
	data := map[string]any{"List": list, "Page": "schemas", "View": "schema_register", "Title": "Register schema", "P": p,
		"Types": []string{schemaregistry.TypeAvro, schemaregistry.TypeProtobuf, schemaregistry.TypeJSON}, "SubjectURL": c.schemaURL(d.Subject)}
	if p.Existing {
		data["Title"] = "Register a new version of " + d.Subject
		data["Sel"] = d.Subject
		if len(d.Refs) > 0 {
			data["Resolved"] = c.resolvedDraft(ctx, d)
		}
	}
	maps.Copy(data, c.dockData(DockFilter{Registry: true}))
	c.render(w, r, "schema_register", data)
}

// resolvedDraft is a draft's resolved view: for Avro the schema with its references'
// types written in, else the references' texts listed. Empty when one cannot be read.
func (c *Console) resolvedDraft(ctx context.Context, d regDraft) string {
	if d.Type != schemaregistry.TypeAvro {
		return ""
	}
	v := schemaVersion{Schema: d.Schema, SchemaType: d.Type}
	for _, ref := range d.Refs {
		v.References = append(v.References, struct {
			Name    string `json:"name"`
			Subject string `json:"subject"`
			Version int    `json:"version"`
		}{ref.Name, ref.Subject, ref.Version})
	}
	refs, err := c.reg.resolve(ctx, v)
	if err != nil {
		return ""
	}
	text, err := schemaregistry.InlineAvro(d.Schema, refs)
	if err != nil {
		return ""
	}
	if p, ok := indentJSON([]byte(text)); ok {
		return p
	}
	return text
}

// regFinding is one row of Sc-04's table: what the registry found, where, and a fix
// when the console knows one.
type regFinding struct {
	N            int
	Type, Path   string
	Description  string
	Fix, FixWhat string // the fix's button and what it does; empty when there is none.
	FixLabel     string // the fix as the actions row says it: add "default": ""
}

// schemaRegisterCheck answers Check on the register page (Sc-03): whether the draft
// parses and may follow the subject's versions, with the reasons taken apart.
// Nothing is stored.
func (c *Console) schemaRegisterCheck(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	ctx := r.Context()
	d, err := draftFromForm(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if d.Subject == "" {
		c.fail(w, r, badInput("give the subject a name, such as orders-value"))
		return
	}
	p := c.regPage(ctx, d)
	out := map[string]any{"P": p, "Did": "check", "SubjectURL": c.schemaURL(d.Subject)}
	if !p.Existing {
		// A new subject has nothing to be compatible with: the check is that it parses
		// (the registry's own check at NONE, against nothing).
		if _, err := c.reg.CheckAt(ctx, d.Subject, d.body(), "NONE", "", d.Normalize); err != nil {
			if c.refusedHere(w, r, err, out) {
				return
			}
			c.fail(w, r, err)
			return
		}
		out["Parses"] = true
		c.partial(w, "schema_reg_answer", out)
		return
	}
	ok, _, err := c.reg.Check(ctx, d.Subject, d.body())
	if err != nil {
		if c.refusedHere(w, r, err, out) {
			return
		}
		c.fail(w, r, err)
		return
	}
	out["OK"] = ok
	if a, err := c.reg.CheckAt(ctx, d.Subject, d.body(), "", "", d.Normalize); err == nil {
		out["Compared"], out["Level"] = a.Compared, a.Level
		if !ok {
			out["Findings"] = c.regFindings(a, d)
		}
	}
	out["Why"] = compatibleWhy(d.Type)
	c.partial(w, "schema_reg_answer", out)
}

// refusedHere draws a registry refusal of the draft itself (a schema that does not
// parse, 42201; a reference that is not there, 42201 or 40402) where the answer
// goes, as the foundation's refusal.
func (c *Console) refusedHere(w http.ResponseWriter, r *http.Request, err error, out map[string]any) bool {
	var re *regError
	if !errors.As(err, &re) || (re.Status != http.StatusUnprocessableEntity && re.Status != http.StatusNotFound) {
		return false
	}
	out["Refusal"] = refusalFor(err).data(nil)
	c.partial(w, "schema_reg_answer", out)
	return true
}

// schemaCreate registers the draft: a first version or a new one. Stored, it opens
// the subject; refused as incompatible (409), the refusal is drawn where the check's
// answer is, with the registry's own message and its reasons taken apart (Sc-04).
func (c *Console) schemaCreate(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	ctx := r.Context()
	d, err := draftFromForm(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if d.Subject == "" {
		c.fail(w, r, badInput("give the subject a name, such as orders-value"))
		return
	}
	p := c.regPage(ctx, d)
	if _, err := c.reg.Register(ctx, d.Subject, d.body()); err != nil {
		var re *regError
		if errors.As(err, &re) && re.Code == 42205 && r.Header.Get("HX-Request") == "true" {
			// Sc-15: the subject (or the registry) is read-only.
			c.partial(w, "schema_reg_answer", map[string]any{"P": p, "Did": "register", "SubjectURL": c.schemaURL(d.Subject), "ReadOnly": re, "RawRefusal": refusalJSON(re)})
			return
		}
		if errors.As(err, &re) && re.Status == http.StatusConflict && r.Header.Get("HX-Request") == "true" {
			out := map[string]any{"P": p, "Did": "register", "SubjectURL": c.schemaURL(d.Subject), "Refused": re, "RawRefusal": refusalJSON(re)}
			if a, err := c.reg.CheckAt(ctx, d.Subject, d.body(), "", "", d.Normalize); err == nil {
				out["Findings"], out["Level"] = c.regFindings(a, d), a.Level
				if len(a.Findings) > 0 {
					out["Against"] = a.Findings[0].Version
				}
			}
			out["Rule"] = levelRule(p.Level)
			c.partial(w, "schema_reg_answer", out)
			return
		}
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.schemaURL(d.Subject), "Schema registered")
}

// refusalJSON is the registry's refusal as it was on the wire.
func refusalJSON(re *regError) string {
	b, _ := json.Marshal(map[string]any{"error_code": re.Code, "message": re.Message})
	return string(b)
}

// regFindings is Sc-04's table from a check's findings.
func (c *Console) regFindings(a checkAnswer, d regDraft) []regFinding {
	var out []regFinding
	for _, f := range a.Findings {
		for _, reason := range f.Reasons {
			rf := regFinding{N: len(out) + 1, Type: reason.Type, Path: reason.Path, Description: reason.Description}
			if reason.Type == "READER_FIELD_MISSING_DEFAULT_VALUE" && d.Type == schemaregistry.TypeAvro {
				if _, ok := addAvroDefault(d.Schema, reason.Path); ok {
					rf.Fix, rf.FixWhat = "Add default", "add a default to the field at "+reason.Path
					rf.FixLabel = `add "default"`
					if def, ok := avroFieldZero(d.Schema, reason.Path); ok {
						rf.FixLabel = `add "default": ` + def
					}
				}
			}
			out = append(out, rf)
		}
	}
	return out
}

// schemaRegisterFix applies a finding's fix to the draft and answers with the page's
// form as it now is (Sc-04's "Add default"). Nothing is sent to the registry.
func (c *Console) schemaRegisterFix(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	d, err := draftFromForm(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	text, ok := addAvroDefault(d.Schema, r.FormValue("path"))
	if !ok {
		c.fail(w, r, badInput("there is no field at %q to give a default", r.FormValue("path")))
		return
	}
	d.Schema = text
	// The text with the fix, and the actions as they are before a check: Check again.
	c.partial(w, "schema_reg_fixed", map[string]any{"P": c.regPage(r.Context(), d)})
}

// addAvroDefault gives the field at path (/fields/N) of an Avro record a default of
// its type's zero: null for a union whose first branch is null, "" for a string, 0
// for a number, false for a boolean, [] and {} for an array and a map. ok is false
// for a path or a type it does not know.
func addAvroDefault(text, path string) (string, bool) {
	n, err := strconv.Atoi(strings.TrimPrefix(path, "/fields/"))
	if err != nil || !strings.HasPrefix(path, "/fields/") {
		return "", false
	}
	var rec map[string]json.RawMessage
	if json.Unmarshal([]byte(text), &rec) != nil {
		return "", false
	}
	var fields []map[string]json.RawMessage
	if json.Unmarshal(rec["fields"], &fields) != nil || n < 0 || n >= len(fields) {
		return "", false
	}
	def, ok := avroZero(fields[n]["type"])
	if !ok {
		return "", false
	}
	// Written back in its original order, the default after the field's own keys.
	var raw []json.RawMessage
	_ = json.Unmarshal(rec["fields"], &raw)
	field := strings.TrimSpace(string(raw[n]))
	field = strings.TrimSuffix(field, "}") + `,"default":` + def + "}"
	raw[n] = json.RawMessage(field)
	fb, _ := json.Marshal(raw)
	var out strings.Builder
	out.WriteString("{")
	for i, k := range objectKeys(json.RawMessage(text)) {
		if i > 0 {
			out.WriteString(",")
		}
		v := rec[k]
		if k == "fields" {
			v = fb
		}
		out.WriteString(strconvQuote(k) + ":" + string(v))
	}
	out.WriteString("}")
	if p, ok := printSchema(schemaregistry.TypeAvro, out.String()); ok {
		return p, true
	}
	return out.String(), true
}

// avroFieldZero is the default addAvroDefault gives the field at path.
func avroFieldZero(text, path string) (string, bool) {
	n, err := strconv.Atoi(strings.TrimPrefix(path, "/fields/"))
	if err != nil {
		return "", false
	}
	var rec struct {
		Fields []map[string]json.RawMessage `json:"fields"`
	}
	if json.Unmarshal([]byte(text), &rec) != nil || n < 0 || n >= len(rec.Fields) {
		return "", false
	}
	return avroZero(rec.Fields[n]["type"])
}

func avroZero(typ json.RawMessage) (string, bool) {
	var name string
	if json.Unmarshal(typ, &name) == nil {
		switch name {
		case "null":
			return "null", true
		case "string", "bytes":
			return `""`, true
		case "int", "long", "float", "double":
			return "0", true
		case "boolean":
			return "false", true
		}
		return "", false
	}
	var union []json.RawMessage
	if json.Unmarshal(typ, &union) == nil && len(union) > 0 {
		return avroZero(union[0])
	}
	var obj struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(typ, &obj) == nil {
		switch obj.Type {
		case "array":
			return "[]", true
		case "map":
			return "{}", true
		}
	}
	return "", false
}

// compatibleWhy is Sc-03's sentence on why a change of this type may pass.
func compatibleWhy(typ string) string {
	switch typ {
	case schemaregistry.TypeProtobuf:
		return "In Protobuf, adding a field with a new number is wire-compatible."
	case schemaregistry.TypeJSON:
		return "In JSON Schema, a property added to a closed object (additionalProperties false) is allowed."
	}
	return "In Avro, a field added with a default can be read from older messages."
}

// levelRule is Sc-04's rule, said for the level.
func levelRule(level string) string {
	switch strings.TrimSuffix(level, "_TRANSITIVE") {
	case "BACKWARD":
		return fmt.Sprintf("at %s, a consumer on the new schema must still read every message written with the old one. A field with no default cannot be filled for old messages.", level)
	case "FORWARD":
		return fmt.Sprintf("at %s, a consumer still on the old schema must read every message written with the new one.", level)
	case "FULL":
		return fmt.Sprintf("at %s, both: readers on either schema read messages written with the other.", level)
	}
	return "the subject's level decides which earlier versions the new one is checked against."
}
