package console

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// What a version of a schema is, said in a line: its fields (Sc-05's "first
// version: orderId, lines, total"), and what changed from the version before
// ("added optional coupon (null or string, default null)"). Read from the schema
// text the registry returns; a schema this cannot read is described by the lines
// that changed instead.

// schemaField is one field of a schema's top-level record, message or object.
type schemaField struct {
	Name string
	Type string // as words: "string", "null or string", "int32"
	// Default is the default as written ("EUR", "null"); HasDefault says there is one.
	Default    string
	HasDefault bool
	Optional   bool // Avro: a union with null; JSON Schema: not required.
	Number     int  // Protobuf: the field number.
}

// schemaShape is what the pages say about a schema beyond its text.
type schemaShape struct {
	Fields []schemaField
	// Record is the Avro record's, or the first Protobuf message's, or the JSON
	// Schema's title.
	Record string
	// Messages is the Protobuf message types, in order.
	Messages []string
	// Required and Closed are JSON Schema's: how many properties are required, and
	// additionalProperties false. Draft is the $schema's draft ("draft-07").
	Required int
	Closed   bool
	Draft    string
	ok       bool
}

// shapeOf reads a version's fields. ok is false for a schema it cannot read (a
// top-level type that is not a record, message or object).
func shapeOf(v schemaVersion) schemaShape {
	switch v.Type() {
	case schemaregistry.TypeProtobuf:
		return protoShape(v.Schema)
	case schemaregistry.TypeJSON:
		return jsonSchemaShape(v.Schema)
	}
	return avroShape(v.Schema)
}

func avroShape(text string) schemaShape {
	var rec struct {
		Type   any `json:"type"`
		Name   string
		Fields []struct {
			Name    string          `json:"name"`
			Type    json.RawMessage `json:"type"`
			Default json.RawMessage `json:"default"`
		} `json:"fields"`
	}
	if json.Unmarshal([]byte(text), &rec) != nil || rec.Type != "record" {
		return schemaShape{}
	}
	s := schemaShape{Record: rec.Name, ok: true}
	for _, f := range rec.Fields {
		sf := schemaField{Name: f.Name, Type: avroTypeWords(f.Type)}
		var u []json.RawMessage
		if json.Unmarshal(f.Type, &u) == nil {
			for _, m := range u {
				if strings.TrimSpace(string(m)) == `"null"` {
					sf.Optional = true
				}
			}
		}
		if len(f.Default) > 0 {
			sf.HasDefault = true
			var str string
			if strings.TrimSpace(string(f.Default)) == "null" {
				sf.Default = "null"
			} else if json.Unmarshal(f.Default, &str) == nil {
				sf.Default = str
			} else {
				sf.Default = string(f.Default)
			}
		}
		s.Fields = append(s.Fields, sf)
	}
	return s
}

// avroTypeWords is an Avro type as words: "string", "null or string", "array of
// int", or a named type's name.
func avroTypeWords(raw json.RawMessage) string {
	var name string
	if json.Unmarshal(raw, &name) == nil {
		return name
	}
	var union []json.RawMessage
	if json.Unmarshal(raw, &union) == nil {
		parts := make([]string, len(union))
		for i, m := range union {
			parts[i] = avroTypeWords(m)
		}
		return strings.Join(parts, " or ")
	}
	var obj struct {
		Type    string          `json:"type"`
		Name    string          `json:"name"`
		Items   json.RawMessage `json:"items"`
		Values  json.RawMessage `json:"values"`
		Logical string          `json:"logicalType"`
	}
	if json.Unmarshal(raw, &obj) != nil {
		return "?"
	}
	switch {
	case obj.Logical != "":
		return obj.Logical
	case obj.Type == "array":
		return "array of " + avroTypeWords(obj.Items)
	case obj.Type == "map":
		return "map of " + avroTypeWords(obj.Values)
	case obj.Name != "":
		return obj.Name
	}
	return obj.Type
}

var (
	protoMessageRe = regexp.MustCompile(`^\s*message\s+(\w+)\s*\{`)
	protoFieldRe   = regexp.MustCompile(`^\s*(?:(repeated|optional)\s+)?([\w.]+(?:<[^>]*>)?)\s+(\w+)\s*=\s*(\d+)`)
)

// protoShape reads the canonical .proto text: the message types, and the fields of
// the first one (at its own nesting level).
func protoShape(text string) schemaShape {
	s := schemaShape{}
	depth := 0
	for _, line := range strings.Split(text, "\n") {
		if m := protoMessageRe.FindStringSubmatch(line); m != nil {
			if depth == 0 {
				s.Messages = append(s.Messages, m[1])
				if len(s.Messages) == 1 {
					s.Record = m[1]
				}
			}
			depth++
			continue
		}
		if depth == 1 && len(s.Messages) == 1 {
			if m := protoFieldRe.FindStringSubmatch(line); m != nil {
				t := m[2]
				if m[1] == "repeated" {
					t = "repeated " + t
				}
				var n int
				fmt.Sscan(m[4], &n)
				s.Fields = append(s.Fields, schemaField{Name: m[3], Type: strings.TrimPrefix(t, "."), Number: n, Optional: m[1] == "optional"})
			}
		}
		depth += strings.Count(line, "{") - strings.Count(line, "}")
		if depth < 0 {
			depth = 0
		}
	}
	s.ok = len(s.Messages) > 0
	return s
}

// jsonSchemaShape reads an object schema's properties in the order they are written.
func jsonSchemaShape(text string) schemaShape {
	var doc struct {
		Schema     string          `json:"$schema"`
		Title      string          `json:"title"`
		Type       any             `json:"type"`
		Properties json.RawMessage `json:"properties"`
		Required   []string        `json:"required"`
		Additional any             `json:"additionalProperties"`
	}
	if json.Unmarshal([]byte(text), &doc) != nil || len(doc.Properties) == 0 {
		return schemaShape{}
	}
	s := schemaShape{Record: doc.Title, Required: len(doc.Required), Closed: doc.Additional == false, ok: true}
	if m := regexp.MustCompile(`draft-\d+|\d{4}-\d{2}`).FindString(doc.Schema); m != "" {
		s.Draft = m
	}
	req := map[string]bool{}
	for _, r := range doc.Required {
		req[r] = true
	}
	var props map[string]struct {
		Type    any      `json:"type"`
		Ref     string   `json:"$ref"`
		Min     *float64 `json:"minimum"`
		Max     *float64 `json:"maximum"`
		Default any      `json:"default"`
	}
	_ = json.Unmarshal(doc.Properties, &props)
	for _, name := range objectKeys(doc.Properties) {
		p := props[name]
		t := ""
		switch x := p.Type.(type) {
		case string:
			t = x
		case []any:
			var parts []string
			for _, y := range x {
				parts = append(parts, fmt.Sprint(y))
			}
			t = strings.Join(parts, " or ")
		}
		if t == "" && p.Ref != "" {
			t = p.Ref
		}
		if p.Min != nil {
			t += fmt.Sprintf(", min %g", *p.Min)
		}
		if p.Max != nil {
			t += fmt.Sprintf(", max %g", *p.Max)
		}
		f := schemaField{Name: name, Type: t, Optional: !req[name]}
		if p.Default != nil {
			f.HasDefault, f.Default = true, fmt.Sprint(p.Default)
		}
		s.Fields = append(s.Fields, f)
	}
	return s
}

// objectKeys is a JSON object's keys in the order they are written.
func objectKeys(raw json.RawMessage) []string {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil
	}
	var keys []string
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return keys
		}
		k, _ := t.(string)
		keys = append(keys, k)
		var skip json.RawMessage
		if dec.Decode(&skip) != nil {
			return keys
		}
	}
	return keys
}

// firstVersionSummary is "first version: orderId, lines, total".
func firstVersionSummary(s schemaShape) string {
	if !s.ok || len(s.Fields) == 0 {
		return "first version"
	}
	names := make([]string, 0, 4)
	for i, f := range s.Fields {
		if i == 4 {
			return fmt.Sprintf("first version: %s and %d more", strings.Join(names, ", "), len(s.Fields)-4)
		}
		names = append(names, f.Name)
	}
	return "first version: " + strings.Join(names, ", ")
}

// changeParts are the changes from one version's fields to the next, each a phrase
// with the field names to be set in code type.
type changePart struct {
	Before, Name, After string
}

// changeSummary is what changed from prev to cur, as phrases; nil when the fields
// cannot be read or did not change (the caller then counts lines).
func changeSummary(typ string, prev, cur schemaShape) []changePart {
	if !prev.ok || !cur.ok {
		return nil
	}
	old := map[string]schemaField{}
	for _, f := range prev.Fields {
		old[f.Name] = f
	}
	now := map[string]bool{}
	var out []changePart
	for _, f := range cur.Fields {
		now[f.Name] = true
		o, had := old[f.Name]
		switch {
		case !had:
			out = append(out, addedPart(typ, f))
		case o.Type != f.Type:
			out = append(out, changePart{Before: "changed ", Name: f.Name, After: fmt.Sprintf(" from %s to %s", o.Type, f.Type)})
		case o.HasDefault != f.HasDefault || o.Default != f.Default:
			if f.HasDefault {
				out = append(out, changePart{Before: "set the default of ", Name: f.Name, After: " to " + f.Default})
			} else {
				out = append(out, changePart{Before: "dropped the default of ", Name: f.Name})
			}
		case o.Optional != f.Optional && typ == schemaregistry.TypeJSON:
			if f.Optional {
				out = append(out, changePart{Before: "made ", Name: f.Name, After: " optional"})
			} else {
				out = append(out, changePart{Before: "made ", Name: f.Name, After: " required"})
			}
		}
	}
	for _, f := range prev.Fields {
		if !now[f.Name] {
			p := changePart{Before: "removed ", Name: f.Name}
			if typ == schemaregistry.TypeProtobuf {
				p.Name = fmt.Sprintf("%s = %d", f.Name, f.Number)
			}
			out = append(out, p)
		}
	}
	return out
}

func addedPart(typ string, f schemaField) changePart {
	switch typ {
	case schemaregistry.TypeProtobuf:
		return changePart{Before: "added ", Name: fmt.Sprintf("%s = %d", f.Name, f.Number)}
	case schemaregistry.TypeJSON:
		if f.Optional {
			return changePart{Before: "added optional ", Name: f.Name, After: " (" + f.Type + ")"}
		}
		return changePart{Before: "added required ", Name: f.Name, After: " (" + f.Type + ")"}
	}
	switch {
	case f.Optional && f.HasDefault:
		return changePart{Before: "added optional ", Name: f.Name, After: fmt.Sprintf(" (%s, default %s)", f.Type, f.Default)}
	case f.HasDefault:
		return changePart{Before: "added ", Name: f.Name, After: " with default " + f.Default}
	}
	return changePart{Before: "added ", Name: f.Name, After: ", no default"}
}

// shapeLine is Sc-18's "AuditEvent: actor string, action string, at long".
func shapeLine(s schemaShape) string {
	if !s.ok {
		return ""
	}
	parts := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		parts[i] = f.Name + " " + f.Type
	}
	line := strings.Join(parts, ", ")
	if s.Record != "" {
		line = s.Record + ": " + line
	}
	return line
}
