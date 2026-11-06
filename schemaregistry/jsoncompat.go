package schemaregistry

// JSON Schema compatibility, after Confluent's SchemaDiff.
//
// Confluent does not ask whether one schema's documents are a subset of the other's
// (that is undecidable in general); it compares the two schemas structurally, keyword
// by keyword, and names each difference: "maxLength decreased", "property added to an
// open content model". A fixed set of those names is compatible. The rules are
// therefore the set, and where they look odd (a property added to an open model is a
// breaking change; "format" is ignored) they are Confluent's. The recorded corpus in
// testdata/compat is what this is held to.
//
// jsonCanRead(reader, writer) asks whether a reader built for `reader` can read what a
// writer of `writer` wrote: every difference going from writer to reader must be a
// compatible one.

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"unicode/utf16"
)

// jkind is the kind of schema a node is, as the validator library classes them. A
// change of kind is a change of type.
type jkind int

const (
	jEmpty jkind = iota // accepts anything.
	jFalse              // accepts nothing.
	jString
	jNumber
	jBoolean
	jNull
	jArray
	jObject
	jEnum
	jConst
	jCombined
	jNot
	jCond // if / then / else.
)

type jnode struct {
	kind       jkind
	hasDefault bool

	// string
	minLen, maxLen *int64
	pattern        *string

	// number
	integer                bool
	min, max, exMin, exMax *big.Float
	multipleOf             *big.Float

	// array
	items          *jnode   // every item, when items is a single schema.
	tuple          []*jnode // positional items, when items is an array.
	additionalItem *jnode   // nil = anything allowed; jFalse node = none.
	minItems       *int64
	maxItems       *int64
	unique         bool

	// object
	props        map[string]*jnode
	required     map[string]bool
	additional   *jnode // nil = open (anything); a jFalse node = closed; else a schema.
	minProps     *int64
	maxProps     *int64
	dependencies map[string][]string

	// enum and const: canonical JSON of each value.
	values []string

	// combined: allOf, anyOf or oneOf.
	criterion string
	subs      []*jnode

	// not
	not *jnode

	// if / then / else (any may be nil)
	cond [3]*jnode
}

type jsonLoader struct {
	root  any
	refs  map[string]any // referenced documents, by reference name.
	memo  map[string]*jnode
	depth int
}

//fuzz:parser class=schema-texts FuzzRegisterAndCompare
func parseJSONDoc(text string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// loadJSON turns a schema document into the comparison form. refs are the documents
// named by schema references.
func loadJSON(doc any, refs map[string]any) (*jnode, error) {
	l := &jsonLoader{root: doc, refs: refs, memo: map[string]*jnode{}}
	return l.node(doc, doc)
}

func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }

func (l *jsonLoader) node(v any, scope any) (*jnode, error) {
	l.depth++
	defer func() { l.depth-- }()
	if l.depth > 64 {
		return nil, fmt.Errorf("schema is nested or recursive too deeply to compare")
	}
	switch t := v.(type) {
	case bool:
		if t {
			return &jnode{kind: jEmpty}, nil
		}
		return &jnode{kind: jFalse}, nil
	case map[string]any:
		return l.object(t, scope)
	}
	return nil, fmt.Errorf("a schema must be an object or a boolean")
}

func (l *jsonLoader) resolve(ref string, scope any) (any, any, error) {
	doc := scope
	frag := ref
	if i := strings.Index(ref, "#"); i >= 0 {
		name := ref[:i]
		frag = ref[i+1:]
		if name != "" {
			d, ok := l.refs[name]
			if !ok {
				// A reference may be written as a path; match on the last element.
				for k, d2 := range l.refs {
					if strings.HasSuffix(name, k) || strings.HasSuffix(k, name) {
						d, ok = d2, true
					}
				}
			}
			if !ok {
				return nil, nil, fmt.Errorf("unresolved reference %q", ref)
			}
			doc = d
		} else {
			doc = l.root
		}
	} else {
		d, ok := l.refs[ref]
		if !ok {
			return nil, nil, fmt.Errorf("unresolved reference %q", ref)
		}
		return d, d, nil
	}
	cur := doc
	if frag != "" && frag != "/" {
		for _, part := range strings.Split(strings.TrimPrefix(frag, "/"), "/") {
			part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, nil, fmt.Errorf("unresolved reference %q", ref)
			}
			cur, ok = m[part]
			if !ok {
				return nil, nil, fmt.Errorf("unresolved reference %q", ref)
			}
		}
	}
	return cur, doc, nil
}

func num(v any) (*big.Float, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return nil, false
	}
	f, _, err := big.ParseFloat(n.String(), 10, 200, big.ToNearestEven)
	return f, err == nil
}

func intPtr(v any) *int64 {
	n, ok := v.(json.Number)
	if !ok {
		return nil
	}
	i, err := n.Int64()
	if err != nil {
		f, err := n.Float64()
		if err != nil {
			return nil
		}
		i = int64(f)
	}
	return &i
}

func canonJSON(v any) string {
	b, _ := json.Marshal(v)
	// Re-marshalling sorts map keys, so equal values print equally.
	return string(b)
}

func (l *jsonLoader) object(m map[string]any, scope any) (*jnode, error) {
	if ref, ok := m["$ref"].(string); ok {
		if n, ok := l.memo[ref]; ok && n != nil {
			return n, nil
		}
		target, doc, err := l.resolve(ref, scope)
		if err != nil {
			return nil, err
		}
		l.memo[ref] = &jnode{kind: jEmpty} // a recursive reference compares as "anything" at the point of recursion.
		n, err := l.node(target, doc)
		if err != nil {
			return nil, err
		}
		l.memo[ref] = n
		return n, nil
	}

	// A schema with a combinator next to its own keywords is the product of all of them.
	var parts []*jnode
	own, err := l.typed(m, scope)
	if err != nil {
		return nil, err
	}
	if own != nil {
		parts = append(parts, own)
	}
	for _, crit := range []string{"allOf", "anyOf", "oneOf"} {
		arr, ok := m[crit].([]any)
		if !ok {
			continue
		}
		c := &jnode{kind: jCombined, criterion: crit}
		for _, s := range arr {
			sub, err := l.node(s, scope)
			if err != nil {
				return nil, err
			}
			c.subs = append(c.subs, sub)
		}
		parts = append(parts, c)
	}
	if nv, ok := m["not"]; ok {
		sub, err := l.node(nv, scope)
		if err != nil {
			return nil, err
		}
		parts = append(parts, &jnode{kind: jNot, not: sub})
	}
	if _, ok := m["if"]; ok {
		c := &jnode{kind: jCond}
		for i, k := range []string{"if", "then", "else"} {
			if sv, ok := m[k]; ok {
				sub, err := l.node(sv, scope)
				if err != nil {
					return nil, err
				}
				c.cond[i] = sub
			}
		}
		parts = append(parts, c)
	}
	_, hasDefault := m["default"]
	var out *jnode
	switch len(parts) {
	case 0:
		out = &jnode{kind: jEmpty}
	case 1:
		out = parts[0]
	default:
		out = &jnode{kind: jCombined, criterion: "allOf", subs: parts}
	}
	if hasDefault {
		cp := *out
		cp.hasDefault = true
		out = &cp
	}
	return out, nil
}

// typed reads the keywords that belong to a type, or nil when there are none.
func (l *jsonLoader) typed(m map[string]any, scope any) (*jnode, error) {
	if e, ok := m["enum"].([]any); ok {
		n := &jnode{kind: jEnum}
		for _, v := range e {
			n.values = append(n.values, canonJSON(v))
		}
		sort.Strings(n.values)
		return n, nil
	}
	if c, ok := m["const"]; ok {
		return &jnode{kind: jConst, values: []string{canonJSON(c)}}, nil
	}
	var types []string
	switch t := m["type"].(type) {
	case string:
		types = []string{t}
	case []any:
		for _, v := range t {
			if s, ok := v.(string); ok {
				types = append(types, s)
			}
		}
	}
	if len(types) == 0 {
		// No "type": the keywords present say which kind it is.
		switch {
		case hasAny(m, "properties", "required", "additionalProperties", "patternProperties", "minProperties", "maxProperties", "dependencies", "propertyNames"):
			types = []string{"object"}
		case hasAny(m, "items", "additionalItems", "minItems", "maxItems", "uniqueItems", "contains"):
			types = []string{"array"}
		case hasAny(m, "minLength", "maxLength", "pattern"):
			types = []string{"string"}
		case hasAny(m, "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"):
			types = []string{"number"}
		default:
			return nil, nil
		}
	}
	var nodes []*jnode
	for _, t := range types {
		n, err := l.ofType(t, m, scope)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	if len(nodes) == 1 {
		return nodes[0], nil
	}
	return &jnode{kind: jCombined, criterion: "anyOf", subs: nodes}, nil
}

func hasAny(m map[string]any, keys ...string) bool {
	for _, k := range keys {
		if _, ok := m[k]; ok {
			return true
		}
	}
	return false
}

func (l *jsonLoader) ofType(t string, m map[string]any, scope any) (*jnode, error) {
	switch t {
	case "string":
		n := &jnode{kind: jString, minLen: intPtr(m["minLength"]), maxLen: intPtr(m["maxLength"])}
		if p, ok := m["pattern"].(string); ok {
			n.pattern = &p
		}
		return n, nil
	case "integer", "number":
		n := &jnode{kind: jNumber, integer: t == "integer"}
		n.min, _ = num(m["minimum"])
		n.max, _ = num(m["maximum"])
		switch ex := m["exclusiveMinimum"].(type) {
		case json.Number:
			n.exMin, _ = num(ex)
		case bool: // draft 4: the boolean makes "minimum" exclusive.
			if ex && n.min != nil {
				n.exMin, n.min = n.min, nil
			}
		}
		switch ex := m["exclusiveMaximum"].(type) {
		case json.Number:
			n.exMax, _ = num(ex)
		case bool:
			if ex && n.max != nil {
				n.exMax, n.max = n.max, nil
			}
		}
		n.multipleOf, _ = num(m["multipleOf"])
		return n, nil
	case "boolean":
		return &jnode{kind: jBoolean}, nil
	case "null":
		return &jnode{kind: jNull}, nil
	case "array":
		n := &jnode{kind: jArray, minItems: intPtr(m["minItems"]), maxItems: intPtr(m["maxItems"])}
		n.unique, _ = m["uniqueItems"].(bool)
		switch it := m["items"].(type) {
		case []any:
			for _, s := range it {
				sub, err := l.node(s, scope)
				if err != nil {
					return nil, err
				}
				n.tuple = append(n.tuple, sub)
			}
		case nil:
		default:
			sub, err := l.node(it, scope)
			if err != nil {
				return nil, err
			}
			n.items = sub
		}
		if ai, ok := m["additionalItems"]; ok {
			sub, err := l.node(ai, scope)
			if err != nil {
				return nil, err
			}
			if sub.kind != jEmpty {
				n.additionalItem = sub
			}
		}
		return n, nil
	case "object":
		n := &jnode{kind: jObject, props: map[string]*jnode{}, required: map[string]bool{}, minProps: intPtr(m["minProperties"]), maxProps: intPtr(m["maxProperties"])}
		if ps := asMap(m["properties"]); ps != nil {
			for name, s := range ps {
				sub, err := l.node(s, scope)
				if err != nil {
					return nil, err
				}
				n.props[name] = sub
			}
		}
		if rq, ok := m["required"].([]any); ok {
			for _, r := range rq {
				if s, ok := r.(string); ok {
					n.required[s] = true
				}
			}
		}
		if ap, ok := m["additionalProperties"]; ok {
			sub, err := l.node(ap, scope)
			if err != nil {
				return nil, err
			}
			if sub.kind != jEmpty {
				n.additional = sub
			}
		}
		if deps := asMap(m["dependencies"]); deps != nil {
			n.dependencies = map[string][]string{}
			for k, v := range deps {
				if arr, ok := v.([]any); ok {
					for _, x := range arr {
						if s, ok := x.(string); ok {
							n.dependencies[k] = append(n.dependencies[k], s)
						}
					}
					sort.Strings(n.dependencies[k])
				}
			}
		}
		return n, nil
	}
	return nil, fmt.Errorf("unknown type %q", t)
}

// The differences Confluent names, and which of them a reader can live with.
var jsonCompatible = map[string]bool{
	"TYPE_EXTENDED": true, "SCHEMA_REMOVED": true,
	"MAX_LENGTH_REMOVED": true, "MAX_LENGTH_INCREASED": true, "MIN_LENGTH_REMOVED": true, "MIN_LENGTH_DECREASED": true,
	"PATTERN_REMOVED": true,
	"MAXIMUM_REMOVED": true, "MAXIMUM_INCREASED": true, "MINIMUM_REMOVED": true, "MINIMUM_DECREASED": true,
	"EXCLUSIVE_MAXIMUM_REMOVED": true, "EXCLUSIVE_MAXIMUM_INCREASED": true,
	"EXCLUSIVE_MINIMUM_REMOVED": true, "EXCLUSIVE_MINIMUM_DECREASED": true,
	"MULTIPLE_OF_REMOVED": true, "MULTIPLE_OF_REDUCED": true,
	"REQUIRED_ATTRIBUTE_WITH_DEFAULT_ADDED": true, "REQUIRED_ATTRIBUTE_REMOVED": true,
	"DEPENDENCY_ARRAY_REMOVED": true, "DEPENDENCY_ARRAY_NARROWED": true,
	"PROPERTY_REMOVED_FROM_OPEN_CONTENT_MODEL": true, "PROPERTY_ADDED_TO_CLOSED_CONTENT_MODEL": true,
	"REQUIRED_PROPERTY_WITH_DEFAULT_ADDED_TO_UNOPEN_CONTENT_MODEL": true,
	"PROPERTY_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL":    true,
	"PROPERTY_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL":  true,
	"ADDITIONAL_PROPERTIES_ADDED":                                  true, "ADDITIONAL_PROPERTIES_EXTENDED": true,
	"MAX_PROPERTIES_REMOVED": true, "MAX_PROPERTIES_INCREASED": true, "MIN_PROPERTIES_REMOVED": true, "MIN_PROPERTIES_DECREASED": true,
	"MAX_ITEMS_REMOVED": true, "MAX_ITEMS_INCREASED": true, "MIN_ITEMS_REMOVED": true, "MIN_ITEMS_DECREASED": true,
	"UNIQUE_ITEMS_REMOVED": true, "ADDITIONAL_ITEMS_ADDED": true, "ADDITIONAL_ITEMS_EXTENDED": true,
	"ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL": true, "ITEM_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL": true,
	"ITEM_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL": true, "ITEM_REMOVED_FROM_OPEN_CONTENT_MODEL": true,
	"ITEM_ADDED_TO_CLOSED_CONTENT_MODEL": true,
	"ENUM_ARRAY_EXTENDED":                true, "COMBINED_TYPE_EXTENDED": true, "PRODUCT_TYPE_NARROWED": true, "SUM_TYPE_EXTENDED": true,
	"NOT_TYPE_NARROWED": true, "TYPE_NARROWED_NOT": false,
}

type jchange struct {
	typ, path string
}

type jdiff struct {
	changes []jchange
}

func (d *jdiff) add(typ, path string) { d.changes = append(d.changes, jchange{typ, path}) }

func (d *jdiff) bad() []jchange {
	var out []jchange
	for _, c := range d.changes {
		if !jsonCompatible[c.typ] {
			out = append(out, c)
		}
	}
	return out
}

func compatibleDiff(o, u *jnode) bool {
	var d jdiff
	d.compare(o, u, "#")
	return len(d.bad()) == 0
}

// jsonCanRead: a reader of `reader` reads what a writer of `writer` wrote when going
// from writer to reader changes nothing incompatibly.
func jsonCanRead(reader, writer *jsonSchema, readerIs, writerIs string) []CompatReason {
	o, n := writer.node, reader.node
	if o == nil || n == nil {
		return nil
	}
	var d jdiff
	d.compare(o, n, "#")
	var out []CompatReason
	for _, c := range d.bad() {
		desc := describeJSONChange(c, readerIs, writerIs)
		// The closing quote is Confluent's own: a double quote opens the description
		// and a single one closes it (recorded: testdata/compat/json-reasons.json).
		out = append(out, CompatReason{Type: c.typ, Path: c.path, Description: desc,
			Text: fmt.Sprintf("{errorType:\"%s\", description:\"%s'}", c.typ, desc)})
	}
	return out
}

// jsonSentences are Confluent's descriptions by change type, R the reader's side and W
// the writer's (each recorded for the corpus).
var jsonSentences = map[string]string{
	"TYPE_CHANGED":                         jsonTypeDiffers,
	"TYPE_NARROWED":                        jsonTypeDiffers,
	"COMBINED_TYPE_CHANGED":                jsonTypeDiffers,
	"COMBINED_TYPE_SUBSCHEMAS_CHANGED":     jsonTypeDiffers,
	"ENUM_ARRAY_CHANGED":                   jsonTypeDiffers,
	"MULTIPLE_OF_CHANGED":                  jsonValueDiffers,
	"PATTERN_CHANGED":                      jsonValueDiffers,
	"ITEM_ADDED_TO_OPEN_CONTENT_MODEL":     "The R schema has an open content model and has a property or item at path '%s' which is missing in the W schema",
	"PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL": "The R schema has an open content model and has a property or item at path '%s' which is missing in the W schema",
	"PROPERTY_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL":   "The R schema has a property or item at path '%s' which is missing in the W schema and is not covered by its partially open content model",
	"ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL":       "The R schema has a property or item at path '%s' which is missing in the W schema and is not covered by its partially open content model",
	"PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL":                   "The R has a closed content model and is missing a property or item present at path '%s' in the W schema",
	"ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL":                       "The R has a closed content model and is missing a property or item present at path '%s' in the W schema",
	"PROPERTY_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL": "A property or item is missing in the R schema but present at path '%s' in the W schema and is not covered by its partially open content model",
	"ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL":     "A property or item is missing in the R schema but present at path '%s' in the W schema and is not covered by its partially open content model",
	"REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL":              "The R schema has an unopen content model and has a required property at path '%s' which is missing in the W schema",
	"SCHEMA_ADDED": "",
}

const (
	jsonTypeDiffers  = "A type at path '%s' is different between the R schema and the W schema"
	jsonValueDiffers = "The value at path '%s' is different between the R and W schema"
	jsonNotPresent   = "The keyword at path '%s' in the R schema is not present in the W schema"
	jsonMoreThan     = "The value at path '%s' in the R schema is more than its value in the W schema"
	jsonLessThan     = "The value at path '%s' in the R schema is less than its value in the W schema"
	jsonFewer        = "An array or combined type at path '%s' has fewer elements in the R schema than the W schema"
	jsonMore         = "An array or combined type at path '%s' has more elements in the R schema than the W schema"
)

// describeJSONChange is Confluent's sentence for a change, the reader's side and the
// writer's named. A type the corpus has not shown takes its family's sentence (all
// the *_ADDED, *_INCREASED ... that were recorded share one each).
func describeJSONChange(c jchange, readerIs, writerIs string) string {
	t, ok := jsonSentences[c.typ]
	if !ok {
		switch {
		case strings.HasSuffix(c.typ, "_ADDED"), strings.HasSuffix(c.typ, "_REMOVED"):
			t = jsonNotPresent
		case strings.HasSuffix(c.typ, "_INCREASED"), strings.HasSuffix(c.typ, "_EXPANDED"):
			t = jsonMoreThan
		case strings.HasSuffix(c.typ, "_DECREASED"), strings.HasSuffix(c.typ, "_REDUCED"):
			t = jsonLessThan
		case strings.HasSuffix(c.typ, "_NARROWED"):
			t = jsonFewer
		case strings.HasSuffix(c.typ, "_EXTENDED"):
			t = jsonMore
		default:
			t = jsonTypeDiffers
		}
	}
	if !strings.Contains(t, "%s") {
		return t
	}
	path := c.path
	if path == "#" {
		path = "#/" // the root, as Confluent writes it
	}
	t = strings.NewReplacer("R schema", readerIs+" schema", "W schema", writerIs+" schema", "The R has", "The "+readerIs+" has", "R and W", readerIs+" and "+writerIs).Replace(t)
	return fmt.Sprintf(t, path)
}

func (d *jdiff) compare(o, u *jnode, path string) {
	oAny, uAny := o.kind == jEmpty, u.kind == jEmpty
	switch {
	case oAny && uAny:
		return
	case o.kind == jFalse && u.kind == jFalse:
		return
	case o.kind == jFalse, uAny:
		// Nothing was accepted before, or anything is now: compatible.
		d.add("TYPE_EXTENDED", path)
		return
	case u.kind == jFalse, oAny:
		// Two different kinds of schema, as Confluent's diff sees them.
		d.add("TYPE_CHANGED", path)
		return
	}
	// A combination of one schema is that schema.
	if o.kind == jCombined && len(o.subs) == 1 && u.kind != jCombined {
		d.compare(o.subs[0], u, path)
		return
	}
	if u.kind == jCombined && len(u.subs) == 1 && o.kind != jCombined {
		d.compare(o, u.subs[0], path)
		return
	}
	// A conjunction against a single schema (how a type with not, or with if/then/else,
	// is held here): dropping parts of it accepts more when what is left is one of
	// them; adding parts to a single schema is a change of kind (recorded: "not
	// added", "not removed", "if then else added").
	if o.kind == jCombined && o.criterion == "allOf" && u.kind != jCombined {
		for _, sub := range o.subs {
			var sd jdiff
			sd.compare(sub, u, path)
			if len(sd.bad()) == 0 {
				d.changes = append(d.changes, sd.changes...)
				d.add("PRODUCT_TYPE_NARROWED", path)
				return
			}
		}
		d.add("TYPE_CHANGED", path)
		return
	}
	if u.kind == jCombined && u.criterion == "allOf" && o.kind != jCombined {
		d.add("TYPE_CHANGED", path)
		return
	}
	// One schema becoming a choice that includes it accepts more; any other change
	// between a choice and a single schema is a change of kind (recorded:
	// "type to type array superset", "type array to type subset").
	if o.kind != jCombined && u.kind == jCombined {
		if u.criterion == "anyOf" || u.criterion == "oneOf" {
			for _, sub := range u.subs {
				var sd jdiff
				sd.compare(o, sub, path)
				if len(sd.bad()) == 0 {
					d.changes = append(d.changes, sd.changes...)
					d.add("SUM_TYPE_EXTENDED", path)
					return
				}
			}
		}
		d.add("TYPE_CHANGED", path)
		return
	}
	if o.kind == jCombined && u.kind != jCombined {
		d.add("TYPE_CHANGED", path)
		return
	}
	if o.kind == jCombined {
		d.combined(o, u, path)
		return
	}
	if o.kind != u.kind {
		d.add("TYPE_CHANGED", path)
		return
	}
	switch o.kind {
	case jString:
		d.str(o, u, path)
	case jNumber:
		d.number(o, u, path)
	case jArray:
		d.array(o, u, path)
	case jObject:
		d.object(o, u, path)
	case jEnum:
		d.enum(o, u, path)
	case jConst:
		if o.values[0] != u.values[0] {
			d.add("ENUM_ARRAY_CHANGED", path+"/const") // recorded: "const changed"
		}
	case jCond:
		for i := range o.cond {
			a, b := o.cond[i], u.cond[i]
			if (a == nil) != (b == nil) || (a != nil && !(compatibleDiff(a, b) && compatibleDiff(b, a))) {
				d.add("CONDITIONAL_CHANGED", path)
				return
			}
		}
	case jNot:
		if compatibleDiff(u.not, o.not) {
			d.add("NOT_TYPE_NARROWED", path)
			// not(A) -> not(B) accepts more when B is narrower than A.
		} else {
			d.add("NOT_TYPE_EXTENDED", path)
		}
	}
}

func intChange(d *jdiff, path, what string, o, u *int64) {
	switch {
	case o == nil && u == nil:
	case o == nil:
		d.add(what+"_ADDED", path)
	case u == nil:
		d.add(what+"_REMOVED", path)
	case *u > *o:
		d.add(what+"_INCREASED", path)
	case *u < *o:
		d.add(what+"_DECREASED", path)
	}
}

func (d *jdiff) str(o, u *jnode, path string) {
	intChange(d, path+"/maxLength", "MAX_LENGTH", o.maxLen, u.maxLen)
	intChange(d, path+"/minLength", "MIN_LENGTH", o.minLen, u.minLen)
	switch {
	case o.pattern == nil && u.pattern != nil:
		d.add("PATTERN_ADDED", path+"/pattern")
	case o.pattern != nil && u.pattern == nil:
		d.add("PATTERN_REMOVED", path+"/pattern")
	case o.pattern != nil && *o.pattern != *u.pattern:
		d.add("PATTERN_CHANGED", path+"/pattern")
	}
}

func floatChange(d *jdiff, path, what string, o, u *big.Float) {
	switch {
	case o == nil && u == nil:
	case o == nil:
		d.add(what+"_ADDED", path)
	case u == nil:
		d.add(what+"_REMOVED", path)
	default:
		switch o.Cmp(u) {
		case -1:
			d.add(what+"_INCREASED", path)
		case 1:
			d.add(what+"_DECREASED", path)
		}
	}
}

func (d *jdiff) number(o, u *jnode, path string) {
	// The limits first, then whether it became integer only (Confluent's order,
	// recorded: "random 114").
	defer func() {
		switch {
		case o.integer && !u.integer:
			d.add("TYPE_EXTENDED", path)
		case !o.integer && u.integer:
			d.add("TYPE_NARROWED", path)
		}
	}()
	floatChange(d, path+"/maximum", "MAXIMUM", o.max, u.max)
	floatChange(d, path+"/minimum", "MINIMUM", o.min, u.min)
	floatChange(d, path+"/exclusiveMaximum", "EXCLUSIVE_MAXIMUM", o.exMax, u.exMax)
	floatChange(d, path+"/exclusiveMinimum", "EXCLUSIVE_MINIMUM", o.exMin, u.exMin)
	switch {
	case o.multipleOf == nil && u.multipleOf != nil:
		d.add("MULTIPLE_OF_ADDED", path+"/multipleOf")
	case o.multipleOf != nil && u.multipleOf == nil:
		d.add("MULTIPLE_OF_REMOVED", path+"/multipleOf")
	case o.multipleOf != nil && o.multipleOf.Cmp(u.multipleOf) != 0:
		switch {
		case divides(o.multipleOf, u.multipleOf):
			d.add("MULTIPLE_OF_EXPANDED", path+"/multipleOf")
		case divides(u.multipleOf, o.multipleOf):
			d.add("MULTIPLE_OF_REDUCED", path+"/multipleOf")
		default:
			d.add("MULTIPLE_OF_CHANGED", path+"/multipleOf")
		}
	}
}

// divides reports whether b is a whole multiple of a.
func divides(a, b *big.Float) bool {
	if a.Sign() == 0 {
		return false
	}
	q := new(big.Float).Quo(b, a)
	i, _ := q.Int(nil)
	return q.Cmp(new(big.Float).SetInt(i)) == 0
}

func (d *jdiff) enum(o, u *jnode, path string) {
	os, us := stringSet(o.values), stringSet(u.values)
	oInU, uInO := subset(os, us), subset(us, os)
	switch {
	case oInU && uInO:
	case oInU:
		d.add("ENUM_ARRAY_EXTENDED", path+"/enum")
	case uInO:
		d.add("ENUM_ARRAY_NARROWED", path+"/enum")
	default:
		d.add("ENUM_ARRAY_CHANGED", path+"/enum")
	}
}

func stringSet(v []string) map[string]bool {
	m := map[string]bool{}
	for _, s := range v {
		m[s] = true
	}
	return m
}

func subset(a, b map[string]bool) bool {
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func (d *jdiff) array(o, u *jnode, path string) {
	intChange(d, path+"/maxItems", "MAX_ITEMS", o.maxItems, u.maxItems)
	intChange(d, path+"/minItems", "MIN_ITEMS", o.minItems, u.minItems)
	switch {
	case !o.unique && u.unique:
		d.add("UNIQUE_ITEMS_ADDED", path+"/uniqueItems")
	case o.unique && !u.unique:
		d.add("UNIQUE_ITEMS_REMOVED", path+"/uniqueItems")
	}
	any := &jnode{kind: jEmpty}
	switch {
	case o.tuple == nil && u.tuple == nil:
		// An items schema on one side only is a schema added or removed, as Confluent
		// names it (recorded: "array items added", "array items removed").
		switch oi, ui := o.items, u.items; {
		case oi == nil && ui == nil:
		case oi == nil:
			d.add("SCHEMA_ADDED", path+"/items")
		case ui == nil:
			d.add("SCHEMA_REMOVED", path+"/items")
		default:
			d.compare(oi, ui, path+"/items")
		}
	default:
		// Positional items. A position beyond the tuple is governed by additionalItems.
		rest := func(n *jnode) *jnode {
			if n.additionalItem != nil {
				return n.additionalItem
			}
			if n.tuple == nil && n.items != nil {
				return n.items
			}
			return any
		}
		count := len(o.tuple)
		if len(u.tuple) > count {
			count = len(u.tuple)
		}
		at := func(n *jnode, i int) (*jnode, bool) {
			if n.tuple == nil {
				if n.items != nil {
					return n.items, true
				}
				return any, false
			}
			if i < len(n.tuple) {
				return n.tuple[i], true
			}
			return rest(n), false
		}
		for i := 0; i < count; i++ {
			os, oIn := at(o, i)
			us, uIn := at(u, i)
			switch {
			case oIn && uIn:
				d.compare(os, us, fmt.Sprintf("%s/items/%d", path, i))
			case uIn && !oIn:
				// The new schema describes a position the old one left to additionalItems.
				if o.additionalItem == nil {
					d.add("ITEM_ADDED_TO_OPEN_CONTENT_MODEL", fmt.Sprintf("%s/items/%d", path, i))
				} else if o.additionalItem.kind == jFalse {
					d.add("ITEM_ADDED_TO_CLOSED_CONTENT_MODEL", fmt.Sprintf("%s/items/%d", path, i))
				} else if compatibleDiff(o.additionalItem, us) && compatibleDiff(us, o.additionalItem) {
					d.add("ITEM_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL", fmt.Sprintf("%s/items/%d", path, i))
				} else {
					d.add("ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL", fmt.Sprintf("%s/items/%d", path, i))
				}
			case oIn && !uIn:
				if u.additionalItem == nil {
					d.add("ITEM_REMOVED_FROM_OPEN_CONTENT_MODEL", fmt.Sprintf("%s/items/%d", path, i))
				} else if u.additionalItem.kind == jFalse {
					d.add("ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL", fmt.Sprintf("%s/items/%d", path, i))
				} else if compatibleDiff(os, u.additionalItem) && compatibleDiff(u.additionalItem, os) {
					d.add("ITEM_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL", fmt.Sprintf("%s/items/%d", path, i))
				} else {
					d.add("ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL", fmt.Sprintf("%s/items/%d", path, i))
				}
			}
		}
		switch {
		case o.additionalItem == nil && u.additionalItem != nil:
			d.add("ADDITIONAL_ITEMS_REMOVED", path+"/additionalItems")
		case o.additionalItem != nil && u.additionalItem == nil:
			d.add("ADDITIONAL_ITEMS_ADDED", path+"/additionalItems")
		case o.additionalItem != nil && u.additionalItem != nil:
			d.additional(o.additionalItem, u.additionalItem, path+"/additionalItems", "ADDITIONAL_ITEMS")
		}
	}
}

// additional compares two additionalProperties/additionalItems values where each is
// false or a schema.
func (d *jdiff) additional(o, u *jnode, path, prefix string) {
	switch {
	case o.kind == jFalse && u.kind == jFalse:
	case o.kind == jFalse:
		d.add(prefix+"_ADDED", path)
	case u.kind == jFalse:
		d.add(prefix+"_REMOVED", path)
	default:
		var sub jdiff
		sub.compare(o, u, path)
		if len(sub.bad()) == 0 {
			if len(sub.changes) > 0 {
				d.add(prefix+"_EXTENDED", path)
			}
		} else {
			d.add(prefix+"_NARROWED", path)
		}
	}
}

func (d *jdiff) object(o, u *jnode, path string) {
	// In Confluent's order (recorded: testdata/compat/json-reasons.json): required,
	// then each property, then additionalProperties, then the counts, then
	// dependencies. Properties come in the order a Java HashMap gives their names, the
	// old schema's first, as Confluent's diff walks them.

	// A property both schemas declare that is now required narrows what is accepted,
	// unless it has a default. One the new schema adds is judged with the property.
	var added, removed []string
	for r := range u.required {
		if !o.required[r] {
			added = append(added, r)
		}
	}
	for r := range o.required {
		if !u.required[r] {
			removed = append(removed, r)
		}
	}
	for _, r := range javaHashOrder(added) {
		ps, declared := u.props[r]
		_, before := o.props[r]
		switch {
		case !declared || !before:
		case ps.hasDefault:
			d.add("REQUIRED_ATTRIBUTE_WITH_DEFAULT_ADDED", path+"/required/"+r)
		default:
			d.add("REQUIRED_ATTRIBUTE_ADDED", path+"/required/"+r)
		}
	}
	for _, r := range javaHashOrder(removed) {
		d.add("REQUIRED_ATTRIBUTE_REMOVED", path+"/required/"+r)
	}

	// Inserted the compared-from schema's names first: two names in one bucket keep
	// that order (recorded: "random 003", "a2" before "a").
	var oNames, uOnly []string
	for n := range o.props {
		oNames = append(oNames, n)
	}
	for n := range u.props {
		if _, ok := o.props[n]; !ok {
			uOnly = append(uOnly, n)
		}
	}
	for _, name := range javaHashInserted(append(javaHashOrder(oNames), javaHashOrder(uOnly)...)) {
		op, inO := o.props[name]
		up, inU := u.props[name]
		p := path + "/properties/" + name
		switch {
		case inO && inU:
			d.compare(op, up, p)
		case inU:
			// A property the old schema did not name.
			if o.additional == nil {
				d.add("PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL", p)
				continue
			}
			switch {
			case o.additional.kind == jFalse:
				d.add("PROPERTY_ADDED_TO_CLOSED_CONTENT_MODEL", p)
			case compatibleDiff(o.additional, up):
				d.add("PROPERTY_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL", p)
			default:
				// Why it is not covered first, as Confluent lists it.
				var why jdiff
				why.compare(o.additional, up, p)
				d.changes = append(d.changes, why.bad()...)
				d.add("PROPERTY_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL", p)
			}
			// Required in a model that was not open: data written before it has none.
			if u.required[name] {
				if up.hasDefault {
					d.add("REQUIRED_PROPERTY_WITH_DEFAULT_ADDED_TO_UNOPEN_CONTENT_MODEL", p)
				} else {
					d.add("REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL", p)
				}
			}
		default:
			switch {
			case u.additional == nil:
				d.add("PROPERTY_REMOVED_FROM_OPEN_CONTENT_MODEL", p)
			case u.additional.kind == jFalse:
				d.add("PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL", p)
			case compatibleDiff(op, u.additional):
				d.add("PROPERTY_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL", p)
			default:
				var why jdiff
				why.compare(op, u.additional, p)
				d.changes = append(d.changes, why.bad()...)
				d.add("PROPERTY_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL", p)
			}
		}
	}

	switch {
	case o.additional == nil && u.additional == nil:
	case o.additional == nil:
		if u.additional.kind == jFalse {
			d.add("ADDITIONAL_PROPERTIES_REMOVED", path+"/additionalProperties")
		} else {
			d.add("ADDITIONAL_PROPERTIES_NARROWED", path+"/additionalProperties")
		}
	case u.additional == nil:
		if o.additional.kind == jFalse {
			d.add("ADDITIONAL_PROPERTIES_ADDED", path+"/additionalProperties")
		} else {
			d.add("ADDITIONAL_PROPERTIES_EXTENDED", path+"/additionalProperties")
		}
	case o.additional.kind == jFalse || u.additional.kind == jFalse:
		d.additional(o.additional, u.additional, path+"/additionalProperties", "ADDITIONAL_PROPERTIES")
	default:
		// Both allow more properties of a schema: what changed in that schema is the
		// change (recorded: "partial: widen additional type").
		d.compare(o.additional, u.additional, path+"/additionalProperties")
	}

	intChange(d, path+"/maxProperties", "MAX_PROPERTIES", o.maxProps, u.maxProps)
	intChange(d, path+"/minProperties", "MIN_PROPERTIES", o.minProps, u.minProps)

	// Dependencies: a property that now depends on more is a narrowing.
	var deps []string
	for k := range u.dependencies {
		deps = append(deps, k)
	}
	for _, k := range javaHashOrder(deps) {
		uv := u.dependencies[k]
		ov, had := o.dependencies[k]
		switch {
		case !had:
			d.add("DEPENDENCY_ARRAY_ADDED", path+"/dependencies/"+k)
		case !equalStrings(ov, uv):
			if subset(stringSet(uv), stringSet(ov)) {
				d.add("DEPENDENCY_ARRAY_NARROWED", path+"/dependencies/"+k)
			} else {
				d.add("DEPENDENCY_ARRAY_EXTENDED", path+"/dependencies/"+k)
			}
		}
	}
	var gone []string
	for k := range o.dependencies {
		if _, ok := u.dependencies[k]; !ok {
			gone = append(gone, k)
		}
	}
	for _, k := range javaHashOrder(gone) {
		d.add("DEPENDENCY_ARRAY_REMOVED", path+"/dependencies/"+k)
	}
}

// javaHashOrder is the order a java.util.HashMap built from these names iterates
// them in: by bucket (String.hashCode, spread, masked by a capacity that grows from
// 16 at three quarters full), and within a bucket in the order they were put in,
// taken here to be by name.
func javaHashOrder(names []string) []string {
	out := append([]string(nil), names...)
	sort.Strings(out)
	return javaHashInserted(out)
}

// javaHashInserted is javaHashOrder for names inserted in the order given.
func javaHashInserted(names []string) []string {
	out := append([]string(nil), names...)
	capacity := 16
	for len(out) > capacity*3/4 {
		capacity *= 2
	}
	bucket := func(s string) uint32 {
		var h int32
		for _, c := range utf16.Encode([]rune(s)) {
			h = 31*h + int32(c)
		}
		u := uint32(h)
		return (u ^ u>>16) & uint32(capacity-1)
	}
	sort.SliceStable(out, func(i, j int) bool { return bucket(out[i]) < bucket(out[j]) })
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// combined compares where either side is allOf, anyOf or oneOf. A schema that is not
// combined is treated as a combination of itself alone.
func (d *jdiff) combined(o, u *jnode, path string) {
	oc, uc := o, u
	if o.kind != jCombined {
		oc = &jnode{kind: jCombined, criterion: u.criterion, subs: []*jnode{o}}
	}
	if u.kind != jCombined {
		uc = &jnode{kind: jCombined, criterion: o.criterion, subs: []*jnode{u}}
	}
	crit := oc.criterion
	if oc.criterion != uc.criterion {
		switch {
		case oc.criterion == "oneOf" && uc.criterion == "anyOf":
			crit = "anyOf"
		case oc.criterion == "anyOf" && uc.criterion == "oneOf":
			d.add("COMBINED_TYPE_CHANGED", path)
			return
		case len(oc.subs) == 1:
			crit = uc.criterion
		case len(uc.subs) == 1:
			crit = oc.criterion
		default:
			d.add("COMBINED_TYPE_CHANGED", path)
			return
		}
	}
	// As Confluent's diff: a change in the number of branches is named by which side
	// has more (an allOf with more is narrower, an anyOf or oneOf with more is wider);
	// then each old branch is paired with a new one it is compatible with, identical
	// ones first, and if fewer pair up than the smaller side has, the branches changed;
	// otherwise each pair is compared.
	switch {
	case len(oc.subs) < len(uc.subs) && crit == "allOf":
		d.add("PRODUCT_TYPE_EXTENDED", path)
	case len(oc.subs) < len(uc.subs):
		d.add("SUM_TYPE_EXTENDED", path)
	case len(oc.subs) > len(uc.subs) && crit == "allOf":
		d.add("PRODUCT_TYPE_NARROWED", path)
	case len(oc.subs) > len(uc.subs):
		d.add("SUM_TYPE_NARROWED", path)
	}
	usedOld := make([]bool, len(oc.subs))
	usedNew := make([]bool, len(uc.subs))
	type pairing struct{ i, j int }
	var pairs []pairing
	pair := func(ok func(a, b *jnode) bool) {
		for i, ob := range oc.subs {
			if usedOld[i] {
				continue
			}
			for j, ub := range uc.subs {
				if usedNew[j] || !ok(ob, ub) {
					continue
				}
				usedOld[i], usedNew[j] = true, true
				pairs = append(pairs, pairing{i, j})
				break
			}
		}
	}
	pair(func(a, b *jnode) bool {
		var sub jdiff
		sub.compare(a, b, "#")
		return len(sub.changes) == 0
	})
	pair(compatibleDiff)
	if len(pairs) < min(len(oc.subs), len(uc.subs)) {
		d.add("COMBINED_TYPE_SUBSCHEMAS_CHANGED", path)
		return
	}
	sort.Slice(pairs, func(a, b int) bool { return pairs[a].i < pairs[b].i })
	for _, pr := range pairs {
		var sub jdiff
		sub.compare(oc.subs[pr.i], uc.subs[pr.j], fmt.Sprintf("%s/%s/%d", path, crit, pr.i))
		d.changes = append(d.changes, sub.changes...)
	}
}
