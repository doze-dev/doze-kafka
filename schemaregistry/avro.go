package schemaregistry

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/hamba/avro/v2"
)

type avroSchema struct {
	text, norm string
	schema     avro.Schema
}

func (a *avroSchema) Type() string { return TypeAvro }

func (a *avroSchema) Canonical(normalize bool) string {
	if normalize {
		return a.norm
	}
	return a.text
}

func parseAvro(text string, refs []resolvedRef) (parsedSchema, error) {
	cache := &avro.SchemaCache{}
	for _, r := range refs {
		if _, err := avro.ParseWithCache(r.Text, "", cache); err != nil {
			return nil, fmt.Errorf("reference %q: %w", r.Name, err)
		}
	}
	s, err := avro.ParseWithCache(text, "", cache)
	if err != nil && strings.Contains(err.Error(), "invalid default") {
		// Confluent does not check that a default fits its field (a string default on
		// ["null","string"], null on a string); hamba/avro does. Compatibility only asks
		// whether a default exists, so retry with each default replaced by one that fits.
		relaxed, rerr := relaxDefaults(text)
		if rerr != nil {
			return nil, rerr
		}
		{
			cache = &avro.SchemaCache{}
			for _, r := range refs {
				if _, perr := avro.ParseWithCache(r.Text, "", cache); perr != nil {
					return nil, fmt.Errorf("reference %q: %w", r.Name, perr)
				}
			}
			s, err = avro.ParseWithCache(relaxed, "", cache)
		}
	}
	if err != nil {
		return nil, err
	}
	var known []string
	for _, r := range refs {
		known = append(known, avroDefinedNames(r.Text)...)
	}
	canon, err := avroText(text, known, false)
	if err != nil {
		return nil, err
	}
	norm, err := avroText(text, known, true)
	if err != nil {
		return nil, err
	}
	return &avroSchema{text: canon, norm: norm, schema: s}, nil
}

// avroCanRead reports why a reader using `reader` cannot read data written with
// `writer`, or nil when it can.
func avroCanRead(reader, writer *avroSchema) []string {
	if err := avro.NewSchemaCompatibility().Compatible(reader.schema, writer.schema); err != nil {
		return []string{err.Error()}
	}
	return nil
}

// relaxDefaults rewrites a schema so every field default is one its type accepts,
// keeping which fields have a default.
func relaxDefaults(text string) (string, error) {
	n, err := parseNode(text)
	if err != nil {
		return "", err
	}
	badDefault := false
	var walk func(n *node)
	walk = func(n *node) {
		switch n.kind {
		case 'a':
			for _, it := range n.items {
				walk(it)
			}
		case 'o':
			if f := n.get("fields"); f != nil {
				for _, fld := range f.items {
					if fld.kind != 'o' {
						continue
					}
					ty := fld.get("type")
					if ty != nil {
						walk(ty)
					}
					for i, k := range fld.keys {
						if k != "default" || ty == nil {
							continue
						}
						if emptyNumericDefault(ty, fld.vals[i]) {
							badDefault = true
						}
						fld.vals[i] = dummyDefault(ty)
					}
				}
			}
			for _, key := range []string{"items", "values", "type"} {
				if v := n.get(key); v != nil && v.kind != 's' {
					walk(v)
				}
			}
		}
	}
	walk(n)
	if badDefault {
		return "", fmt.Errorf("empty String")
	}
	var b bytes.Buffer
	n.write(&b)
	return b.String(), nil
}

// emptyNumericDefault: Confluent does not check that a default fits its type, with one
// exception it cannot help: an empty string as the default of a number fails to read.
func emptyNumericDefault(ty, def *node) bool {
	if def.kind != 's' || def.str != "" || ty.kind != 's' {
		return false
	}
	switch ty.str {
	case "int", "long", "float", "double":
		return true
	}
	return false
}

// dummyDefault is a default value that fits the type.
func dummyDefault(ty *node) *node {
	raw := func(s string) *node { return &node{kind: 'r', raw: s} }
	switch ty.kind {
	case 's':
		switch ty.str {
		case "null":
			return raw("null")
		case "boolean":
			return raw("false")
		case "int", "long", "float", "double":
			return raw("0")
		case "string", "bytes":
			return &node{kind: 's', str: ""}
		}
		return raw("null") // a named type: unknown here.
	case 'a':
		if len(ty.items) > 0 {
			return dummyDefault(ty.items[0])
		}
	case 'o':
		if t := ty.get("type"); t != nil && t.kind == 's' {
			switch t.str {
			case "array":
				return &node{kind: 'a'}
			case "map":
				return &node{kind: 'o'}
			case "enum":
				if sy := ty.get("symbols"); sy != nil && len(sy.items) > 0 {
					return sy.items[0]
				}
			case "fixed":
				size := 0
				if sz := ty.get("size"); sz != nil {
					fmt.Sscan(sz.raw, &size)
				}
				return &node{kind: 's', str: strings.Repeat("\u0000", size)}
			case "record":
				return &node{kind: 'o'}
			default:
				return dummyDefault(t)
			}
		}
	}
	return raw("null")
}
