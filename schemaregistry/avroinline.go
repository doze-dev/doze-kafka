package schemaregistry

import (
	"bytes"
	"fmt"
	"strings"
)

// The resolved view of an Avro schema (board Sc-10, SCH-22): the schema with each type
// it takes from a reference written out where it is first used, as Avro itself reads it
// (a named type is defined once and named after that). Everything else is as written:
// docs, defaults and properties are kept, which Avro's canonical form would drop.

// avroInline writes text with the named types its references define inlined.
func avroInline(text string, refs []resolvedRef) (string, error) {
	root, err := parseNode(text)
	if err != nil {
		return "", err
	}
	defs := map[string]*node{} // full name -> its definition, from the references
	for _, r := range refs {
		n, err := parseNode(r.Text)
		if err != nil {
			return "", fmt.Errorf("reference %q: %w", r.Name, err)
		}
		collectAvroDefs(n, nil, defs)
	}
	in := &avroInliner{defs: defs, done: map[string]bool{}}
	root = in.walk(root, nil)
	var b bytes.Buffer
	root.write(&b)
	return b.String(), nil
}

// collectAvroDefs records every named type n defines, nested ones included.
func collectAvroDefs(n *node, enclosing *string, defs map[string]*node) {
	switch n.kind {
	case 'a':
		for _, it := range n.items {
			collectAvroDefs(it, enclosing, defs)
		}
	case 'o':
		t := n.get("type")
		if t == nil {
			return
		}
		if t.kind != 's' {
			collectAvroDefs(t, enclosing, defs)
			return
		}
		switch t.str {
		case "record", "error", "enum", "fixed":
			space, name := splitName(n, enclosing)
			defs[fullAvroName(space, name)] = n
			if f := n.get("fields"); f != nil {
				for _, fld := range f.items {
					if ft := fld.get("type"); ft != nil {
						collectAvroDefs(ft, space, defs)
					}
				}
			}
		case "array":
			if it := n.get("items"); it != nil {
				collectAvroDefs(it, enclosing, defs)
			}
		case "map":
			if v := n.get("values"); v != nil {
				collectAvroDefs(v, enclosing, defs)
			}
		}
	}
}

func fullAvroName(space *string, name string) string {
	if space == nil || *space == "" || strings.Contains(name, ".") {
		return name
	}
	return *space + "." + name
}

type avroInliner struct {
	defs map[string]*node
	done map[string]bool // named types already defined in the output
}

// walk returns n with the first use of each referenced type replaced by its definition.
func (in *avroInliner) walk(n *node, enclosing *string) *node {
	switch n.kind {
	case 's':
		if avroPrimitives[n.str] {
			return n
		}
		full := n.str
		if !strings.Contains(full, ".") {
			full = fullAvroName(enclosing, n.str)
			if _, ok := in.defs[full]; !ok {
				full = n.str // a name in the null namespace
			}
		}
		def, ok := in.defs[full]
		if !ok || in.done[full] {
			return n
		}
		in.done[full] = true
		cp := cloneNode(def)
		// Written where it is used, the definition carries its full name, so it means
		// the same whatever namespace encloses it.
		space := full[:max(strings.LastIndex(full, "."), 0)]
		var sp *string
		if space != "" {
			sp = &space
		}
		in.walkNamed(cp, sp)
		setNode(cp, "name", &node{kind: 's', str: full})
		dropNode(cp, "namespace")
		return cp
	case 'a':
		for i, it := range n.items {
			n.items[i] = in.walk(it, enclosing)
		}
	case 'o':
		t := n.get("type")
		if t == nil {
			return n
		}
		if t.kind != 's' {
			setNode(n, "type", in.walk(t, enclosing))
			return n
		}
		switch t.str {
		case "record", "error", "enum", "fixed":
			space, name := splitName(n, enclosing)
			in.done[fullAvroName(space, name)] = true
			in.walkNamed(n, space)
		case "array":
			if it := n.get("items"); it != nil {
				setNode(n, "items", in.walk(it, enclosing))
			}
		case "map":
			if v := n.get("values"); v != nil {
				setNode(n, "values", in.walk(v, enclosing))
			}
		}
	}
	return n
}

// walkNamed walks the fields of a record whose namespace is space.
func (in *avroInliner) walkNamed(n *node, space *string) {
	if f := n.get("fields"); f != nil {
		for _, fld := range f.items {
			if ft := fld.get("type"); ft != nil {
				setNode(fld, "type", in.walk(ft, space))
			}
		}
	}
}

func setNode(n *node, key string, v *node) {
	for i, k := range n.keys {
		if k == key {
			n.vals[i] = v
			return
		}
	}
	n.keys = append(n.keys, key)
	n.vals = append(n.vals, v)
}

func dropNode(n *node, key string) {
	for i, k := range n.keys {
		if k == key {
			n.keys = append(n.keys[:i], n.keys[i+1:]...)
			n.vals = append(n.vals[:i], n.vals[i+1:]...)
			return
		}
	}
}

func cloneNode(n *node) *node {
	cp := *n
	cp.keys = append([]string(nil), n.keys...)
	cp.vals = make([]*node, len(n.vals))
	for i, v := range n.vals {
		cp.vals[i] = cloneNode(v)
	}
	cp.items = make([]*node, len(n.items))
	for i, it := range n.items {
		cp.items[i] = cloneNode(it)
	}
	return &cp
}
