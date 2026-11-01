package schemaregistry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// The three schema languages the registry understands. AVRO is what an absent
// schemaType means, as in Confluent.
const (
	TypeAvro     = "AVRO"
	TypeJSON     = "JSON"
	TypeProtobuf = "PROTOBUF"
)

// Reference names another registered schema that this one uses: an Avro named type,
// a JSON Schema $ref target, a Protobuf import.
type Reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// resolvedRef is a reference followed to the schema text it names.
type resolvedRef struct {
	Reference
	Type string
	Text string
}

// parsedSchema is a schema the registry has read and can compare. Its Go type is
// private to the language that produced it.
type parsedSchema interface {
	// Type is AVRO, JSON or PROTOBUF.
	Type() string
	// Canonical is the text the registry stores and compares: the same schema written
	// two ways (whitespace, say) is one schema, one id.
	Canonical(normalize bool) string
}

// parseSchema reads schema text of the given language. refs are every schema it
// depends on, dependencies before dependents.
//
//fuzz:parser class=schema-texts FuzzRegisterAndCompare
func parseSchema(typ, text string, refs []resolvedRef) (parsedSchema, error) {
	switch typ {
	case TypeAvro:
		return parseAvro(text, refs)
	case TypeJSON:
		return parseJSONSchema(text, refs)
	case TypeProtobuf:
		return parseProtobuf(text, refs)
	}
	return nil, fmt.Errorf("unknown schema type %q", typ)
}

func normalizeType(t string) (string, bool) {
	switch strings.ToUpper(t) {
	case "", TypeAvro:
		return TypeAvro, true
	case TypeJSON:
		return TypeJSON, true
	case TypeProtobuf:
		return TypeProtobuf, true
	}
	return "", false
}

// compactJSON removes insignificant whitespace and keeps key order, so the stored
// form of a JSON-based schema is stable without being reordered.
func compactJSON(text string) (string, error) {
	var b bytes.Buffer
	if err := json.Compact(&b, []byte(text)); err != nil {
		return "", err
	}
	return b.String(), nil
}
