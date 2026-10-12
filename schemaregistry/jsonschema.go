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

func jsonRefURL(name string) string {
	if strings.Contains(name, "://") {
		return name
	}
	return "mem:///" + strings.TrimPrefix(name, "/")
}

//fuzz:parser class=schema-texts FuzzRegisterAndCompare
func parseJSONSchema(text string, refs []resolvedRef) (parsedSchema, error) {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft7) // Confluent's default when a schema names no draft.
	for _, r := range refs {
		doc, err := jsonschema.UnmarshalJSON(strings.NewReader(r.Text))
		if err != nil {
			return nil, fmt.Errorf("reference %q: %w", r.Name, err)
		}
		if err := compiler.AddResource(jsonRefURL(r.Name), doc); err != nil {
			return nil, fmt.Errorf("reference %q: %w", r.Name, err)
		}
	}
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(text))
	if err != nil {
		return nil, err
	}
	const main = "mem:///schema.json"
	if err := compiler.AddResource(main, doc); err != nil {
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
	refDocs := map[string]any{}
	for _, r := range refs {
		if d, err := parseJSONDoc(r.Text); err == nil {
			refDocs[r.Name] = d
		}
	}
	node, err := loadJSON(root, refDocs)
	if err != nil {
		return nil, err
	}
	return &jsonSchema{text: compact, node: node, compiled: compiled}, nil
}
