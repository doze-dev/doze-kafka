package schemaregistry

import "strings"

// jobj builds an object schema from property schemas ("name": schema JSON).
func jobj(extra string, props ...[2]string) string {
	var ps []string
	for _, p := range props {
		ps = append(ps, `"`+p[0]+`":`+p[1])
	}
	s := `{"type":"object","properties":{` + strings.Join(ps, ",") + `}`
	if extra != "" {
		s += "," + extra
	}
	return s + `}`
}

func jp(name, schema string) [2]string { return [2]string{name, schema} }

func jsonCorpus() []corpusCase {
	var cs []corpusCase
	add := func(name, old, new string) { cs = append(cs, corpusCase{Name: name, Old: old, New: new}) }
	str, num, integer := `{"type":"string"}`, `{"type":"number"}`, `{"type":"integer"}`

	base := jobj("", jp("a", str))
	add("identical", base, base)

	// Types.
	for _, p := range [][2]string{
		{"string", "integer"}, {"integer", "number"}, {"number", "integer"}, {"string", "boolean"},
		{"object", "array"}, {"string", "null"}, {"integer", "string"},
	} {
		add("type "+p[0]+" to "+p[1], jobj("", jp("a", `{"type":"`+p[0]+`"}`)), jobj("", jp("a", `{"type":"`+p[1]+`"}`)))
	}
	add("type to type array superset", jobj("", jp("a", str)), jobj("", jp("a", `{"type":["string","null"]}`)))
	add("type array to type subset", jobj("", jp("a", `{"type":["string","null"]}`)), jobj("", jp("a", str)))
	add("type array reorder", jobj("", jp("a", `{"type":["string","null"]}`)), jobj("", jp("a", `{"type":["null","string"]}`)))
	add("type array swap member", jobj("", jp("a", `{"type":["string","null"]}`)), jobj("", jp("a", `{"type":["integer","null"]}`)))
	add("top-level type change", `{"type":"string"}`, `{"type":"integer"}`)
	add("top-level same", `{"type":"string"}`, `{"type":"string"}`)
	add("type removed", jobj("", jp("a", str)), jobj("", jp("a", `{}`)))
	add("type added", jobj("", jp("a", `{}`)), jobj("", jp("a", str)))

	// Properties in an open content model (additionalProperties not set).
	add("open: add property", base, jobj("", jp("a", str), jp("b", integer)))
	add("open: remove property", jobj("", jp("a", str), jp("b", integer)), base)
	add("open: add required property", base, jobj(`"required":["b"]`, jp("a", str), jp("b", integer)))
	add("open: remove required property", jobj(`"required":["b"]`, jp("a", str), jp("b", integer)), base)
	add("open: make property required", base, jobj(`"required":["a"]`, jp("a", str)))
	add("open: make property optional", jobj(`"required":["a"]`, jp("a", str)), base)
	add("open: require undeclared property", base, jobj(`"required":["zzz"]`, jp("a", str)))
	add("open: change property type", base, jobj("", jp("a", integer)))
	add("open: rename property", base, jobj("", jp("b", str)))

	// Closed content model.
	closed := func(props ...[2]string) string { return jobj(`"additionalProperties":false`, props...) }
	add("closed: add property", closed(jp("a", str)), closed(jp("a", str), jp("b", integer)))
	add("closed: remove property", closed(jp("a", str), jp("b", integer)), closed(jp("a", str)))
	add("closed: add required property", closed(jp("a", str)), jobj(`"additionalProperties":false,"required":["b"]`, jp("a", str), jp("b", integer)))
	add("closed: change property type", closed(jp("a", str)), closed(jp("a", integer)))
	add("open to closed", base, closed(jp("a", str)))
	add("closed to open", closed(jp("a", str)), base)
	add("open to closed with new property", base, closed(jp("a", str), jp("b", str)))
	add("additionalProperties true to omitted", jobj(`"additionalProperties":true`, jp("a", str)), base)
	add("additionalProperties omitted to true", base, jobj(`"additionalProperties":true`, jp("a", str)))

	// Partially open: additionalProperties is a schema.
	part := func(ap string, props ...[2]string) string { return jobj(`"additionalProperties":`+ap, props...) }
	add("partial: widen additional type", part(str, jp("a", str)), part(`{"type":["string","integer"]}`, jp("a", str)))
	add("partial: narrow additional type", part(`{"type":["string","integer"]}`, jp("a", str)), part(str, jp("a", str)))
	add("partial: add property covered", part(str, jp("a", str)), part(str, jp("a", str), jp("b", str)))
	add("partial: add property not covered", part(str, jp("a", str)), part(str, jp("a", str), jp("b", integer)))
	add("partial: remove property covered", part(str, jp("a", str), jp("b", str)), part(str, jp("a", str)))
	add("partial: remove property not covered", part(str, jp("a", str), jp("b", integer)), part(str, jp("a", str)))
	add("partial to closed", part(str, jp("a", str)), closed(jp("a", str)))
	add("closed to partial", closed(jp("a", str)), part(str, jp("a", str)))
	add("partial to open", part(str, jp("a", str)), base)
	add("open to partial", base, part(str, jp("a", str)))

	// Scalars.
	for _, kw := range []struct{ name, kw, small, big string }{
		{"minLength", "minLength", "1", "5"}, {"maxLength", "maxLength", "5", "10"},
		{"minimum", "minimum", "1", "5"}, {"maximum", "maximum", "5", "10"},
		{"exclusiveMinimum", "exclusiveMinimum", "1", "5"}, {"exclusiveMaximum", "exclusiveMaximum", "5", "10"},
		{"multipleOf", "multipleOf", "2", "4"},
	} {
		typ := `"string"`
		if !strings.Contains(kw.kw, "Length") {
			typ = `"number"`
		}
		with := func(v string) string { return jobj("", jp("a", `{"type":`+typ+`,"`+kw.kw+`":`+v+`}`)) }
		without := jobj("", jp("a", `{"type":`+typ+`}`))
		add(kw.name+" added", without, with(kw.small))
		add(kw.name+" removed", with(kw.small), without)
		add(kw.name+" increased", with(kw.small), with(kw.big))
		add(kw.name+" decreased", with(kw.big), with(kw.small))
	}
	add("multipleOf non-multiple", jobj("", jp("a", `{"type":"number","multipleOf":4}`)), jobj("", jp("a", `{"type":"number","multipleOf":3}`)))
	add("multipleOf divisor", jobj("", jp("a", `{"type":"number","multipleOf":4}`)), jobj("", jp("a", `{"type":"number","multipleOf":2}`)))
	add("pattern added", jobj("", jp("a", str)), jobj("", jp("a", `{"type":"string","pattern":"^a"}`)))
	add("pattern removed", jobj("", jp("a", `{"type":"string","pattern":"^a"}`)), jobj("", jp("a", str)))
	add("pattern changed", jobj("", jp("a", `{"type":"string","pattern":"^a"}`)), jobj("", jp("a", `{"type":"string","pattern":"^b"}`)))
	add("format added", jobj("", jp("a", str)), jobj("", jp("a", `{"type":"string","format":"date-time"}`)))
	add("format changed", jobj("", jp("a", `{"type":"string","format":"date"}`)), jobj("", jp("a", `{"type":"string","format":"date-time"}`)))

	// Enum and const.
	en := func(vals string) string { return jobj("", jp("a", `{"enum":[`+vals+`]}`)) }
	add("enum add value", en(`"A","B"`), en(`"A","B","C"`))
	add("enum remove value", en(`"A","B","C"`), en(`"A","B"`))
	add("enum reorder", en(`"A","B"`), en(`"B","A"`))
	add("enum swap value", en(`"A","B"`), en(`"A","C"`))
	add("enum added", jobj("", jp("a", str)), en(`"A","B"`))
	add("enum removed", en(`"A","B"`), jobj("", jp("a", str)))
	add("enum with type", jobj("", jp("a", `{"type":"string","enum":["A","B"]}`)), jobj("", jp("a", `{"type":"string","enum":["A","B","C"]}`)))
	add("const added", jobj("", jp("a", str)), jobj("", jp("a", `{"const":"x"}`)))
	add("const changed", jobj("", jp("a", `{"const":"x"}`)), jobj("", jp("a", `{"const":"y"}`)))

	// Metadata.
	add("title changed", jobj(`"title":"A"`, jp("a", str)), jobj(`"title":"B"`, jp("a", str)))
	add("description changed", jobj(`"description":"A"`, jp("a", str)), jobj(`"description":"B"`, jp("a", str)))
	add("default changed", jobj("", jp("a", `{"type":"string","default":"x"}`)), jobj("", jp("a", `{"type":"string","default":"y"}`)))
	add("default added", jobj("", jp("a", str)), jobj("", jp("a", `{"type":"string","default":"x"}`)))
	add("id changed", `{"$id":"http://x/a","type":"object"}`, `{"$id":"http://x/b","type":"object"}`)
	add("schema keyword added", `{"type":"object"}`, `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object"}`)

	// Arrays.
	arr := func(extra string, items string) string {
		s := `{"type":"array"`
		if items != "" {
			s += `,"items":` + items
		}
		if extra != "" {
			s += "," + extra
		}
		return jobj("", jp("a", s+`}`))
	}
	add("array items type change", arr("", str), arr("", integer))
	add("array items widen", arr("", str), arr("", `{"type":["string","null"]}`))
	add("array items narrow", arr("", `{"type":["string","null"]}`), arr("", str))
	add("array items removed", arr("", str), arr("", ""))
	add("array items added", arr("", ""), arr("", str))
	add("array minItems added", arr("", str), arr(`"minItems":1`, str))
	add("array minItems removed", arr(`"minItems":1`, str), arr("", str))
	add("array minItems increased", arr(`"minItems":1`, str), arr(`"minItems":3`, str))
	add("array minItems decreased", arr(`"minItems":3`, str), arr(`"minItems":1`, str))
	add("array maxItems added", arr("", str), arr(`"maxItems":5`, str))
	add("array maxItems removed", arr(`"maxItems":5`, str), arr("", str))
	add("array maxItems increased", arr(`"maxItems":5`, str), arr(`"maxItems":9`, str))
	add("array maxItems decreased", arr(`"maxItems":9`, str), arr(`"maxItems":5`, str))
	add("array uniqueItems added", arr("", str), arr(`"uniqueItems":true`, str))
	add("array uniqueItems removed", arr(`"uniqueItems":true`, str), arr("", str))
	add("tuple add item", arr("", ""), arr("", ""))
	add("tuple items array", jobj("", jp("a", `{"type":"array","items":[{"type":"string"},{"type":"integer"}]}`)), jobj("", jp("a", `{"type":"array","items":[{"type":"string"},{"type":"integer"},{"type":"boolean"}]}`)))
	add("tuple items array remove", jobj("", jp("a", `{"type":"array","items":[{"type":"string"},{"type":"integer"},{"type":"boolean"}]}`)), jobj("", jp("a", `{"type":"array","items":[{"type":"string"},{"type":"integer"}]}`)))
	add("tuple additionalItems false", jobj("", jp("a", `{"type":"array","items":[{"type":"string"}]}`)), jobj("", jp("a", `{"type":"array","items":[{"type":"string"}],"additionalItems":false}`)))
	add("tuple additionalItems removed", jobj("", jp("a", `{"type":"array","items":[{"type":"string"}],"additionalItems":false}`)), jobj("", jp("a", `{"type":"array","items":[{"type":"string"}]}`)))

	// Object keywords.
	add("minProperties added", base, jobj(`"minProperties":1`, jp("a", str)))
	add("minProperties removed", jobj(`"minProperties":1`, jp("a", str)), base)
	add("minProperties increased", jobj(`"minProperties":1`, jp("a", str)), jobj(`"minProperties":3`, jp("a", str)))
	add("minProperties decreased", jobj(`"minProperties":3`, jp("a", str)), jobj(`"minProperties":1`, jp("a", str)))
	add("maxProperties added", base, jobj(`"maxProperties":5`, jp("a", str)))
	add("maxProperties removed", jobj(`"maxProperties":5`, jp("a", str)), base)
	add("maxProperties increased", jobj(`"maxProperties":5`, jp("a", str)), jobj(`"maxProperties":9`, jp("a", str)))
	add("maxProperties decreased", jobj(`"maxProperties":9`, jp("a", str)), jobj(`"maxProperties":5`, jp("a", str)))
	add("patternProperties added", base, jobj(`"patternProperties":{"^x":{"type":"string"}}`, jp("a", str)))
	add("patternProperties removed", jobj(`"patternProperties":{"^x":{"type":"string"}}`, jp("a", str)), base)
	add("patternProperties changed type", jobj(`"patternProperties":{"^x":{"type":"string"}}`, jp("a", str)), jobj(`"patternProperties":{"^x":{"type":"integer"}}`, jp("a", str)))
	add("propertyNames added", base, jobj(`"propertyNames":{"pattern":"^[a-z]+$"}`, jp("a", str)))
	add("dependencies added", base, jobj(`"dependencies":{"a":["b"]}`, jp("a", str), jp("b", str)))
	add("dependencies removed", jobj(`"dependencies":{"a":["b"]}`, jp("a", str), jp("b", str)), jobj("", jp("a", str), jp("b", str)))
	add("dependencies extended", jobj(`"dependencies":{"a":["b"]}`, jp("a", str), jp("b", str), jp("c", str)), jobj(`"dependencies":{"a":["b","c"]}`, jp("a", str), jp("b", str), jp("c", str)))

	// Nesting.
	nest := func(inner string) string { return jobj("", jp("o", inner)) }
	add("nested add property", nest(jobj("", jp("x", str))), nest(jobj("", jp("x", str), jp("y", str))))
	add("nested remove property", nest(jobj("", jp("x", str), jp("y", str))), nest(jobj("", jp("x", str))))
	add("nested add required", nest(jobj("", jp("x", str))), nest(jobj(`"required":["x"]`, jp("x", str))))
	add("nested closed add property", nest(jobj(`"additionalProperties":false`, jp("x", str))), nest(jobj(`"additionalProperties":false`, jp("x", str), jp("y", str))))
	add("nested closed remove property", nest(jobj(`"additionalProperties":false`, jp("x", str), jp("y", str))), nest(jobj(`"additionalProperties":false`, jp("x", str))))
	add("nested type change", nest(jobj("", jp("x", str))), nest(jobj("", jp("x", integer))))
	add("deeply nested change", nest(nest(jobj("", jp("x", str)))), nest(nest(jobj("", jp("x", integer)))))

	// Composition.
	add("oneOf add branch", jobj("", jp("a", `{"oneOf":[{"type":"string"},{"type":"integer"}]}`)), jobj("", jp("a", `{"oneOf":[{"type":"string"},{"type":"integer"},{"type":"boolean"}]}`)))
	add("oneOf remove branch", jobj("", jp("a", `{"oneOf":[{"type":"string"},{"type":"integer"},{"type":"boolean"}]}`)), jobj("", jp("a", `{"oneOf":[{"type":"string"},{"type":"integer"}]}`)))
	add("oneOf reorder", jobj("", jp("a", `{"oneOf":[{"type":"string"},{"type":"integer"}]}`)), jobj("", jp("a", `{"oneOf":[{"type":"integer"},{"type":"string"}]}`)))
	add("anyOf add branch", jobj("", jp("a", `{"anyOf":[{"type":"string"},{"type":"integer"}]}`)), jobj("", jp("a", `{"anyOf":[{"type":"string"},{"type":"integer"},{"type":"boolean"}]}`)))
	add("anyOf remove branch", jobj("", jp("a", `{"anyOf":[{"type":"string"},{"type":"integer"},{"type":"boolean"}]}`)), jobj("", jp("a", `{"anyOf":[{"type":"string"},{"type":"integer"}]}`)))
	add("allOf add branch", jobj("", jp("a", `{"allOf":[{"type":"string"}]}`)), jobj("", jp("a", `{"allOf":[{"type":"string"},{"minLength":1}]}`)))
	add("allOf remove branch", jobj("", jp("a", `{"allOf":[{"type":"string"},{"minLength":1}]}`)), jobj("", jp("a", `{"allOf":[{"type":"string"}]}`)))
	add("oneOf to anyOf", jobj("", jp("a", `{"oneOf":[{"type":"string"},{"type":"integer"}]}`)), jobj("", jp("a", `{"anyOf":[{"type":"string"},{"type":"integer"}]}`)))
	add("anyOf to oneOf", jobj("", jp("a", `{"anyOf":[{"type":"string"},{"type":"integer"}]}`)), jobj("", jp("a", `{"oneOf":[{"type":"string"},{"type":"integer"}]}`)))
	add("type to oneOf", jobj("", jp("a", str)), jobj("", jp("a", `{"oneOf":[{"type":"string"}]}`)))
	add("type to oneOf wider", jobj("", jp("a", str)), jobj("", jp("a", `{"oneOf":[{"type":"string"},{"type":"integer"}]}`)))
	add("oneOf to type", jobj("", jp("a", `{"oneOf":[{"type":"string"},{"type":"integer"}]}`)), jobj("", jp("a", str)))
	add("not added", jobj("", jp("a", str)), jobj("", jp("a", `{"type":"string","not":{"enum":["x"]}}`)))
	add("not removed", jobj("", jp("a", `{"type":"string","not":{"enum":["x"]}}`)), jobj("", jp("a", str)))
	add("if then else added", base, jobj(`"if":{"properties":{"a":{"const":"x"}}},"then":{"required":["b"]}`, jp("a", str), jp("b", str)))

	// References.
	defs := func(inner string) string {
		return `{"type":"object","properties":{"a":{"$ref":"#/definitions/d"}},"definitions":{"d":` + inner + `}}`
	}
	add("ref same", defs(str), defs(str))
	add("ref target type change", defs(str), defs(integer))
	add("ref target widened", defs(str), defs(`{"type":["string","null"]}`))
	add("ref inlined", defs(str), jobj("", jp("a", str)))
	add("ref introduced", jobj("", jp("a", str)), defs(str))
	add("ref unresolved", `{"type":"object"}`, `{"type":"object","properties":{"a":{"$ref":"#/definitions/missing"}}}`)

	// Boolean schemas, top-level shapes.
	add("true schema", `true`, `true`)
	add("true to false schema", `true`, `false`)
	add("false to true schema", `false`, `true`)
	add("empty to type", `{}`, `{"type":"string"}`)
	add("type to empty", `{"type":"string"}`, `{}`)
	add("array top-level items change", `{"type":"array","items":{"type":"string"}}`, `{"type":"array","items":{"type":"integer"}}`)
	add("integer top-level to number", `{"type":"integer"}`, `{"type":"number"}`)
	add("number top-level to integer", `{"type":"number"}`, `{"type":"integer"}`)
	_ = num

	// Not JSON Schema.
	add("invalid new", base, `{"type":"nonsense"}`)
	add("invalid old", `{"type":"nonsense"}`, base)
	return cs
}
