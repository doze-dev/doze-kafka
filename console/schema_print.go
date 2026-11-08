package console

import (
	"bytes"
	"encoding/json"
	"strings"
)

// How the Schemas boards print a schema (Sc-05, Sc-06, Sc-08, Sc-18): an Avro record
// on a line, then one field a line with the field names lined up; a JSON Schema
// object a key a line, its properties one a line, lined up. One field a line is
// also what the diff (Sc-06) compares, so a change is the line of its field. A
// schema of another shape is indented as JSON.

// printSchema is a schema's text as the boards print it; ok is false when it has
// none of the shapes above.
func printSchema(typ, text string) (string, bool) {
	raw := json.RawMessage(text)
	if !json.Valid(raw) {
		return "", false
	}
	keys := objectKeys(raw)
	if len(keys) == 0 {
		return "", false
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil {
		return "", false
	}
	if typ == "JSON" {
		if _, ok := obj["properties"]; !ok {
			return "", false
		}
		return printJSONSchema(keys, obj), true
	}
	var t string
	if json.Unmarshal(obj["type"], &t) != nil || t != "record" {
		return "", false
	}
	var fields []json.RawMessage
	if json.Unmarshal(obj["fields"], &fields) != nil {
		return "", false
	}
	var head []string
	for _, k := range keys {
		if k != "fields" {
			head = append(head, spacedJSON(json.RawMessage(strconvQuote(k)))+": "+spacedJSON(obj[k]))
		}
	}
	var b strings.Builder
	b.WriteString("{" + strings.Join(append(head, `"fields": [`), ", "))
	// Each field: its name first, padded so what follows lines up.
	names := make([]string, len(fields))
	rests := make([]string, len(fields))
	width := 0
	for i, f := range fields {
		var fo map[string]json.RawMessage
		fkeys := objectKeys(f)
		if json.Unmarshal(f, &fo) != nil || fo["name"] == nil {
			names[i] = spacedJSON(f)
			continue
		}
		names[i] = `"name": ` + spacedJSON(fo["name"]) + ","
		var rest []string
		for _, k := range fkeys {
			if k != "name" {
				rest = append(rest, strconvQuote(k)+": "+spacedJSON(fo[k]))
			}
		}
		rests[i] = strings.Join(rest, ", ")
		width = max(width, len(names[i]))
	}
	for i := range fields {
		b.WriteString("\n  ")
		if rests[i] == "" && !strings.HasPrefix(names[i], `"name"`) {
			b.WriteString(names[i])
		} else {
			b.WriteString("{" + names[i] + strings.Repeat(" ", width-len(names[i])+1) + rests[i] + "}")
		}
		if i < len(fields)-1 {
			b.WriteString(",")
		}
	}
	b.WriteString("\n]}")
	return b.String(), true
}

func printJSONSchema(keys []string, obj map[string]json.RawMessage) string {
	var b strings.Builder
	b.WriteString("{")
	for i, k := range keys {
		b.WriteString("\n  " + strconvQuote(k) + ": ")
		if k == "properties" {
			props := objectKeys(obj[k])
			var po map[string]json.RawMessage
			_ = json.Unmarshal(obj[k], &po)
			width := 0
			for _, p := range props {
				width = max(width, len(strconvQuote(p))+1)
			}
			b.WriteString("{")
			for j, p := range props {
				label := strconvQuote(p) + ":"
				b.WriteString("\n    " + label + strings.Repeat(" ", width-len(label)+1) + spacedJSON(po[p]))
				if j < len(props)-1 {
					b.WriteString(",")
				}
			}
			b.WriteString("\n  }")
		} else {
			b.WriteString(spacedJSON(obj[k]))
		}
		if i < len(keys)-1 {
			b.WriteString(",")
		}
	}
	b.WriteString("\n}")
	return b.String()
}

func strconvQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// spacedJSON is JSON on one line with a space after each comma and colon, as the
// boards write it: {"type": "string", "default": "EUR"}.
func spacedJSON(raw json.RawMessage) string {
	var c bytes.Buffer
	if json.Compact(&c, raw) != nil {
		return string(raw)
	}
	var b strings.Builder
	in, esc := false, false
	for _, ch := range c.String() {
		b.WriteRune(ch)
		switch {
		case esc:
			esc = false
		case in && ch == '\\':
			esc = true
		case ch == '"':
			in = !in
		case !in && (ch == ',' || ch == ':'):
			b.WriteByte(' ')
		}
	}
	return b.String()
}
