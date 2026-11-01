package schemaregistry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Avro schemas are stored the way Avro's own library writes them, because that is
// what Confluent does: the same schema sent with its keys in another order, or with a
// redundant namespace, is one schema with one id. The order is fixed (type, name,
// namespace, doc, then the type's own attributes, then any custom ones in the order
// they came), a namespace is written only where it differs from the enclosing one,
// and a reference to a named type is written as short as the enclosing namespace
// allows.

// node is a JSON value that keeps the order of an object's keys.
type node struct {
	kind  byte // 'o'bject, 'a'rray, 's'tring, 'r'aw scalar (number, true, false, null).
	keys  []string
	vals  []*node
	str   string
	raw   string
	items []*node
}

func (n *node) get(key string) *node {
	for i, k := range n.keys {
		if k == key {
			return n.vals[i]
		}
	}
	return nil
}

//fuzz:parser class=schema-texts FuzzRegisterAndCompare
func parseNode(text string) (*node, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	n, err := readNode(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("unexpected content after the schema")
	}
	return n, nil
}

func readNode(dec *json.Decoder) (*node, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			n := &node{kind: 'o'}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				v, err := readNode(dec)
				if err != nil {
					return nil, err
				}
				n.keys = append(n.keys, kt.(string))
				n.vals = append(n.vals, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return n, nil
		case '[':
			n := &node{kind: 'a'}
			for dec.More() {
				v, err := readNode(dec)
				if err != nil {
					return nil, err
				}
				n.items = append(n.items, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return n, nil
		}
	case string:
		return &node{kind: 's', str: t}, nil
	case json.Number:
		return &node{kind: 'r', raw: t.String()}, nil
	case bool:
		return &node{kind: 'r', raw: fmt.Sprint(t)}, nil
	case nil:
		return &node{kind: 'r', raw: "null"}, nil
	}
	return nil, fmt.Errorf("unexpected JSON token")
}

func (n *node) write(b *bytes.Buffer) {
	switch n.kind {
	case 's':
		s, _ := json.Marshal(n.str)
		b.Write(s)
	case 'r':
		b.WriteString(n.raw)
	case 'a':
		b.WriteByte('[')
		for i, it := range n.items {
			if i > 0 {
				b.WriteByte(',')
			}
			it.write(b)
		}
		b.WriteByte(']')
	case 'o':
		b.WriteByte('{')
		for i, k := range n.keys {
			if i > 0 {
				b.WriteByte(',')
			}
			ks, _ := json.Marshal(k)
			b.Write(ks)
			b.WriteByte(':')
			n.vals[i].write(b)
		}
		b.WriteByte('}')
	}
}

var avroPrimitives = map[string]bool{
	"null": true, "boolean": true, "int": true, "long": true, "float": true, "double": true, "bytes": true, "string": true,
}

type avroWriter struct {
	b       bytes.Buffer
	defined map[string]bool // full names of the named types seen so far.
	sortAll bool            // normalize: custom attributes in alphabetical order.
}

// avroText renders an Avro schema in Avro's own order. known are the full names that
// referenced schemas define.
func avroText(text string, known []string, normalize bool) (string, error) {
	n, err := parseNode(text)
	if err != nil {
		return "", err
	}
	w := &avroWriter{defined: map[string]bool{}, sortAll: normalize}
	for _, k := range known {
		w.defined[k] = true
	}
	if err := w.schema(n, nil); err != nil {
		return "", err
	}
	return w.b.String(), nil
}

// avroDefinedNames lists the full names a schema text defines.
func avroDefinedNames(text string) []string {
	n, err := parseNode(text)
	if err != nil {
		return nil
	}
	var out []string
	var walk func(n *node, ns *string)
	walk = func(n *node, ns *string) {
		switch n.kind {
		case 'a':
			for _, it := range n.items {
				walk(it, ns)
			}
		case 'o':
			t := n.get("type")
			if t == nil || t.kind != 's' {
				if t != nil {
					walk(t, ns)
				}
				return
			}
			switch t.str {
			case "record", "error", "enum", "fixed":
				space, name := splitName(n, ns)
				full := name
				if space != nil && *space != "" {
					full = *space + "." + name
				}
				out = append(out, full)
				if f := n.get("fields"); f != nil {
					for _, fld := range f.items {
						if ft := fld.get("type"); ft != nil {
							walk(ft, space)
						}
					}
				}
			case "array":
				if it := n.get("items"); it != nil {
					walk(it, ns)
				}
			case "map":
				if v := n.get("values"); v != nil {
					walk(v, ns)
				}
			}
		}
	}
	walk(n, nil)
	return out
}

// splitName returns the namespace (nil = none) and simple name of a named type
// declaration, given the enclosing namespace.
func splitName(n *node, enclosing *string) (*string, string) {
	name := ""
	if v := n.get("name"); v != nil && v.kind == 's' {
		name = v.str
	}
	if i := strings.LastIndex(name, "."); i >= 0 {
		space := name[:i]
		return &space, name[i+1:]
	}
	if v := n.get("namespace"); v != nil && v.kind == 's' {
		space := v.str
		return &space, name
	}
	return enclosing, name
}

func nsEqual(a, b *string) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	}
	return *a == *b
}

func (w *avroWriter) str(s string) {
	j, _ := json.Marshal(s)
	w.b.Write(j)
}

func (w *avroWriter) field(first *bool, key string) {
	if !*first {
		w.b.WriteByte(',')
	}
	*first = false
	w.str(key)
	w.b.WriteByte(':')
}

// props writes the attributes a type does not itself define, in the order they came.
func (w *avroWriter) props(first *bool, n *node, reserved ...string) {
	skip := map[string]bool{}
	for _, r := range reserved {
		skip[r] = true
	}
	idx := make([]int, 0, len(n.keys))
	for i, k := range n.keys {
		if !skip[k] {
			idx = append(idx, i)
		}
	}
	if w.sortAll {
		sort.SliceStable(idx, func(a, b int) bool { return n.keys[idx[a]] < n.keys[idx[b]] })
	}
	for _, i := range idx {
		w.field(first, n.keys[i])
		n.vals[i].write(&w.b)
	}
}

func (w *avroWriter) schema(n *node, enclosing *string) error {
	switch n.kind {
	case 's':
		return w.reference(n.str, enclosing)
	case 'a':
		w.b.WriteByte('[')
		for i, it := range n.items {
			if i > 0 {
				w.b.WriteByte(',')
			}
			if err := w.schema(it, enclosing); err != nil {
				return err
			}
		}
		w.b.WriteByte(']')
		return nil
	case 'o':
		return w.object(n, enclosing)
	}
	return fmt.Errorf("not an Avro schema")
}

func (w *avroWriter) reference(name string, enclosing *string) error {
	if avroPrimitives[name] {
		w.str(name)
		return nil
	}
	var space *string
	simple := name
	if i := strings.LastIndex(name, "."); i >= 0 {
		s := name[:i]
		space, simple = &s, name[i+1:]
	} else if enclosing != nil && w.defined[*enclosing+"."+name] {
		space = enclosing
	} else if enclosing != nil && *enclosing != "" && !w.defined[name] {
		space = enclosing
	}
	if space == nil || nsEqual(space, enclosing) || *space == "" {
		w.str(simple)
		return nil
	}
	w.str(*space + "." + simple)
	return nil
}

func (w *avroWriter) object(n *node, enclosing *string) error {
	t := n.get("type")
	if t == nil {
		return fmt.Errorf("schema has no type")
	}
	if t.kind != 's' {
		// {"type": {...}} or {"type": [...]}: the inner schema is the schema.
		return w.schema(t, enclosing)
	}
	switch t.str {
	case "record", "error", "enum", "fixed", "array", "map":
	default:
		if len(n.keys) == 1 {
			// {"type":"string"} is just "string".
			return w.reference(t.str, enclosing)
		}
	}
	first := true
	w.b.WriteByte('{')
	switch t.str {
	case "record", "error", "enum", "fixed":
		space, name := splitName(n, enclosing)
		full := name
		if space != nil && *space != "" {
			full = *space + "." + name
		}
		w.defined[full] = true
		w.field(&first, "type")
		w.str(t.str)
		w.field(&first, "name")
		w.str(name)
		if space != nil && !nsEqual(space, enclosing) {
			w.field(&first, "namespace")
			w.str(*space)
		} else if space == nil && enclosing != nil {
			w.field(&first, "namespace")
			w.str("")
		}
		if d := n.get("doc"); d != nil {
			w.field(&first, "doc")
			d.write(&w.b)
		}
		switch t.str {
		case "enum":
			w.field(&first, "symbols")
			if s := n.get("symbols"); s != nil {
				s.write(&w.b)
			}
			if d := n.get("default"); d != nil {
				w.field(&first, "default")
				d.write(&w.b)
			}
			w.props(&first, n, "type", "name", "namespace", "doc", "symbols", "default", "aliases")
		case "fixed":
			w.field(&first, "size")
			if s := n.get("size"); s != nil {
				s.write(&w.b)
			}
			w.props(&first, n, "type", "name", "namespace", "doc", "size", "aliases")
		default:
			w.field(&first, "fields")
			w.b.WriteByte('[')
			if f := n.get("fields"); f != nil {
				for i, fld := range f.items {
					if i > 0 {
						w.b.WriteByte(',')
					}
					if err := w.recordField(fld, space); err != nil {
						return err
					}
				}
			}
			w.b.WriteByte(']')
			w.props(&first, n, "type", "name", "namespace", "doc", "fields", "aliases")
		}
		if a := n.get("aliases"); a != nil {
			w.field(&first, "aliases")
			a.write(&w.b)
		}
	case "array":
		w.field(&first, "type")
		w.str("array")
		w.field(&first, "items")
		if it := n.get("items"); it != nil {
			if err := w.schema(it, enclosing); err != nil {
				return err
			}
		}
		w.props(&first, n, "type", "items")
	case "map":
		w.field(&first, "type")
		w.str("map")
		w.field(&first, "values")
		if v := n.get("values"); v != nil {
			if err := w.schema(v, enclosing); err != nil {
				return err
			}
		}
		w.props(&first, n, "type", "values")
	default:
		// A primitive with attributes ({"type":"long","logicalType":"timestamp-millis"}).
		w.field(&first, "type")
		w.str(t.str)
		w.props(&first, n, "type")
	}
	w.b.WriteByte('}')
	return nil
}

// recordField writes one field of a record: name, type, doc, default, order, aliases,
// then any custom attributes.
func (w *avroWriter) recordField(f *node, enclosing *string) error {
	if f.kind != 'o' {
		return fmt.Errorf("a field must be an object")
	}
	first := true
	w.b.WriteByte('{')
	if nm := f.get("name"); nm != nil {
		w.field(&first, "name")
		nm.write(&w.b)
	}
	if ty := f.get("type"); ty != nil {
		w.field(&first, "type")
		if err := w.schema(ty, enclosing); err != nil {
			return err
		}
	}
	for _, key := range []string{"doc", "default", "order", "aliases"} {
		if v := f.get(key); v != nil {
			w.field(&first, key)
			v.write(&w.b)
		}
	}
	w.props(&first, f, "name", "type", "doc", "default", "order", "aliases")
	w.b.WriteByte('}')
	return nil
}
