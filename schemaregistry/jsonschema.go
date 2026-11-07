package schemaregistry

import (
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type jsonSchema struct {
	text     string
	node     *jnode             // the schema in the form compatibility compares.
	compiled *jsonschema.Schema // for validating documents.
}

func (j *jsonSchema) Type() string { return TypeJSON }

func (j *jsonSchema) Canonical(bool) string { return j.text }

//fuzz:parser class=schema-texts FuzzRegisterAndCompare
func parseJSONSchema(text string, refs []resolvedRef) (parsedSchema, error) {
	sc := scopeJSONRefs(refs)
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft7) // Confluent's default when a schema names no draft.
	for _, r := range sc.docs {
		doc, err := jsonschema.UnmarshalJSON(strings.NewReader(r.Text))
		if err != nil {
			return nil, fmt.Errorf("reference %q: %w", r.Name, err)
		}
		if err := compiler.AddResource(sc.url[r.Node], rewriteJSONRefs(doc, sc.of(r.Refs))); err != nil {
			return nil, fmt.Errorf("reference %q: %w", r.Name, err)
		}
	}
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(text))
	if err != nil {
		return nil, err
	}
	const main = "mem:///schema.json"
	if err := compiler.AddResource(main, rewriteJSONRefs(doc, sc.of(sc.main))); err != nil {
		return nil, err
	}
	compiled, err := compiler.Compile(main)
	if err != nil {
		return nil, err
	}
	compact, err := compactJSON(text)
	if err != nil {
		return nil, err
	}
	// The same documents, decoded for comparison.
	root, err := parseJSONDoc(text)
	if err != nil {
		return nil, err
	}
	root = rewriteJSONRefs(root, sc.of(sc.main))
	refDocs := map[string]any{}
	for _, r := range sc.docs {
		if d, err := parseJSONDoc(r.Text); err == nil {
			refDocs[sc.url[r.Node]] = rewriteJSONRefs(d, sc.of(r.Refs))
		}
	}
	node, err := loadJSON(root, refDocs)
	if err != nil {
		return nil, err
	}
	return &jsonSchema{text: compact, node: node, compiled: compiled}, nil
}

// jsonScope places each referenced schema at its own address, so a reference name
// means what the schema that uses it says it means: "x.json" in one schema and in a
// schema it references can be two different schemas (recorded: nested-references).
type jsonScope struct {
	docs []resolvedRef     // one per referenced schema
	url  map[string]string // node -> its address
	main map[string]string // the parsed schema's own names -> node
}

func scopeJSONRefs(refs []resolvedRef) jsonScope {
	sc := jsonScope{url: map[string]string{}, main: map[string]string{}}
	byName := map[string]string{}
	for _, r := range refs {
		if r.Node == "" {
			// Names only: every name is one schema, as a caller without the graph means.
			r.Node = "name\x00" + r.Name
			r.Direct = true
		}
		byName[r.Name] = r.Node
		if r.Direct {
			sc.main[r.Name] = r.Node
		}
		if _, ok := sc.url[r.Node]; !ok {
			sc.url[r.Node] = fmt.Sprintf("mem:///refs/%d/%s", len(sc.docs), strings.TrimPrefix(r.Name, "/"))
			sc.docs = append(sc.docs, r)
		}
	}
	for i := range sc.docs {
		if sc.docs[i].Refs == nil {
			sc.docs[i].Refs = byName
		}
	}
	return sc
}

// of turns a schema's names -> nodes into names -> addresses.
func (sc jsonScope) of(names map[string]string) map[string]string {
	out := make(map[string]string, len(names))
	for n, node := range names {
		if u, ok := sc.url[node]; ok {
			out[n] = u
		}
	}
	return out
}

// rewriteJSONRefs replaces, in a decoded document, each "$ref" that names one of the
// document's references with that reference's address, keeping any "#fragment".
// Other references (within the document, or to names it does not declare) are left.
func rewriteJSONRefs(v any, to map[string]string) any {
	if len(to) == 0 {
		return v
	}
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			if s, ok := x.(string); ok && k == "$ref" {
				name, frag, hasFrag := strings.Cut(s, "#")
				if u, ok := to[name]; ok {
					if hasFrag {
						u += "#" + frag
					}
					t[k] = u
				}
				continue
			}
			t[k] = rewriteJSONRefs(x, to)
		}
	case []any:
		for i := range t {
			t[i] = rewriteJSONRefs(t[i], to)
		}
	}
	return v
}
