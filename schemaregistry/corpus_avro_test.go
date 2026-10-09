package schemaregistry

import "strings"

func arec(name string, fields ...string) string {
	return `{"type":"record","name":"` + name + `","fields":[` + strings.Join(fields, ",") + `]}`
}

// afld is a field; typ is JSON ("long", or {"type":"array",...}); dflt, if given, is JSON.
func afld(name, typ string, dflt ...string) string {
	s := `{"name":"` + name + `","type":` + typ
	if len(dflt) > 0 {
		s += `,"default":` + dflt[0]
	}
	return s + `}`
}

func avroCorpus() []corpusCase {
	q := func(s string) string { return `"` + s + `"` }
	var cs []corpusCase
	add := func(name, old, new string) { cs = append(cs, corpusCase{Name: name, Old: old, New: new}) }

	base := arec("R", afld("a", q("string")))
	add("identical", base, base)
	add("add field with default", base, arec("R", afld("a", q("string")), afld("b", q("int"), "0")))
	add("add field without default", base, arec("R", afld("a", q("string")), afld("b", q("int"))))
	add("add nullable field with null default", base, arec("R", afld("a", q("string")), afld("b", `["null","string"]`, "null")))
	add("add union field without default", base, arec("R", afld("a", q("string")), afld("b", `["null","string"]`)))
	add("remove field", arec("R", afld("a", q("string")), afld("b", q("int"))), base)
	add("remove field that had a default", arec("R", afld("a", q("string")), afld("b", q("int"), "0")), base)
	add("rename field", base, arec("R", afld("z", q("string"))))
	add("rename field with alias", base, arec("R", `{"name":"z","type":"string","aliases":["a"]}`))
	add("rename field with alias and default", base, arec("R", `{"name":"z","type":"string","aliases":["a"],"default":"x"}`))
	add("reorder fields", arec("R", afld("a", q("string")), afld("b", q("int"))), arec("R", afld("b", q("int")), afld("a", q("string"))))
	add("change field default", arec("R", afld("a", q("string"), `"x"`)), arec("R", afld("a", q("string"), `"y"`)))
	add("add default to field", base, arec("R", afld("a", q("string"), `"x"`)))
	add("remove default from field", arec("R", afld("a", q("string"), `"x"`)), base)

	// Promotions.
	for _, p := range [][2]string{
		{"int", "long"}, {"long", "int"}, {"int", "float"}, {"int", "double"}, {"long", "float"}, {"long", "double"},
		{"float", "double"}, {"double", "float"}, {"string", "bytes"}, {"bytes", "string"}, {"int", "string"},
		{"string", "int"}, {"boolean", "int"}, {"int", "boolean"}, {"float", "long"},
	} {
		add("promote "+p[0]+" to "+p[1], arec("R", afld("a", q(p[0]))), arec("R", afld("a", q(p[1]))))
	}
	add("primitive int to long", q("int"), q("long"))
	add("primitive long to int", q("long"), q("int"))
	add("primitive string to int", q("string"), q("int"))
	add("primitive string same", q("string"), q("string"))
	add("record to primitive", base, q("string"))
	add("primitive to record", q("string"), base)

	// Unions.
	add("widen to union", arec("R", afld("a", q("string"))), arec("R", afld("a", `["null","string"]`)))
	add("narrow from union", arec("R", afld("a", `["null","string"]`)), arec("R", afld("a", q("string"))))
	add("add union branch", arec("R", afld("a", `["null","string"]`)), arec("R", afld("a", `["null","string","int"]`)))
	add("remove union branch", arec("R", afld("a", `["null","string","int"]`)), arec("R", afld("a", `["null","string"]`)))
	add("reorder union", arec("R", afld("a", `["null","string"]`)), arec("R", afld("a", `["string","null"]`)))
	add("union promote branch", arec("R", afld("a", `["null","int"]`)), arec("R", afld("a", `["null","long"]`)))
	add("union demote branch", arec("R", afld("a", `["null","long"]`)), arec("R", afld("a", `["null","int"]`)))
	add("union to union disjoint", arec("R", afld("a", `["int","string"]`)), arec("R", afld("a", `["boolean","string"]`)))
	add("top-level union add", `["null","string"]`, `["null","string","int"]`)
	add("top-level union remove", `["null","string","int"]`, `["null","string"]`)

	// Enums.
	enum := func(syms string, dflt ...string) string {
		s := `{"type":"enum","name":"E","symbols":[` + syms + `]`
		if len(dflt) > 0 {
			s += `,"default":"` + dflt[0] + `"`
		}
		return s + `}`
	}
	add("enum add symbol", arec("R", afld("e", enum(`"A","B"`))), arec("R", afld("e", enum(`"A","B","C"`))))
	add("enum remove symbol", arec("R", afld("e", enum(`"A","B","C"`))), arec("R", afld("e", enum(`"A","B"`))))
	add("enum add symbol with default", arec("R", afld("e", enum(`"A","B"`, "A"))), arec("R", afld("e", enum(`"A","B","C"`, "A"))))
	add("enum remove symbol with default", arec("R", afld("e", enum(`"A","B","C"`, "A"))), arec("R", afld("e", enum(`"A","B"`, "A"))))
	add("enum reorder symbols", arec("R", afld("e", enum(`"A","B"`))), arec("R", afld("e", enum(`"B","A"`))))
	add("enum add default", arec("R", afld("e", enum(`"A","B"`))), arec("R", afld("e", enum(`"A","B"`, "A"))))
	add("enum rename", arec("R", afld("e", enum(`"A","B"`))), arec("R", afld("e", `{"type":"enum","name":"F","symbols":["A","B"]}`)))
	add("enum to string", arec("R", afld("e", enum(`"A","B"`))), arec("R", afld("e", q("string"))))
	add("string to enum", arec("R", afld("e", q("string"))), arec("R", afld("e", enum(`"A","B"`))))
	add("top-level enum add", enum(`"A","B"`), enum(`"A","B","C"`))

	// Collections and fixed.
	add("array item promote", arec("R", afld("a", `{"type":"array","items":"int"}`)), arec("R", afld("a", `{"type":"array","items":"long"}`)))
	add("array item demote", arec("R", afld("a", `{"type":"array","items":"long"}`)), arec("R", afld("a", `{"type":"array","items":"int"}`)))
	add("array to map", arec("R", afld("a", `{"type":"array","items":"int"}`)), arec("R", afld("a", `{"type":"map","values":"int"}`)))
	add("map value promote", arec("R", afld("a", `{"type":"map","values":"int"}`)), arec("R", afld("a", `{"type":"map","values":"long"}`)))
	add("map value change", arec("R", afld("a", `{"type":"map","values":"int"}`)), arec("R", afld("a", `{"type":"map","values":"string"}`)))
	add("fixed same", arec("R", afld("f", `{"type":"fixed","name":"F","size":4}`)), arec("R", afld("f", `{"type":"fixed","name":"F","size":4}`)))
	add("fixed size change", arec("R", afld("f", `{"type":"fixed","name":"F","size":4}`)), arec("R", afld("f", `{"type":"fixed","name":"F","size":8}`)))
	add("fixed rename", arec("R", afld("f", `{"type":"fixed","name":"F","size":4}`)), arec("R", afld("f", `{"type":"fixed","name":"G","size":4}`)))

	// Names.
	add("record rename", arec("R", afld("a", q("string"))), arec("S", afld("a", q("string"))))
	add("record rename with alias", arec("R", afld("a", q("string"))), `{"type":"record","name":"S","aliases":["R"],"fields":[{"name":"a","type":"string"}]}`)
	add("namespace change", `{"type":"record","name":"R","namespace":"a.b","fields":[{"name":"x","type":"int"}]}`, `{"type":"record","name":"R","namespace":"c.d","fields":[{"name":"x","type":"int"}]}`)
	add("namespace added", arec("R", afld("x", q("int"))), `{"type":"record","name":"R","namespace":"a.b","fields":[{"name":"x","type":"int"}]}`)
	add("namespace in name", `{"type":"record","name":"a.b.R","fields":[{"name":"x","type":"int"}]}`, `{"type":"record","name":"R","namespace":"a.b","fields":[{"name":"x","type":"int"}]}`)

	// Nesting.
	inner := func(extra ...string) string {
		fs := []string{afld("x", q("int"))}
		fs = append(fs, extra...)
		return `{"type":"record","name":"Inner","fields":[` + strings.Join(fs, ",") + `]}`
	}
	add("nested add field with default", arec("R", afld("i", inner())), arec("R", afld("i", inner(afld("y", q("int"), "0")))))
	add("nested add field without default", arec("R", afld("i", inner())), arec("R", afld("i", inner(afld("y", q("int"))))))
	add("nested remove field", arec("R", afld("i", inner(afld("y", q("int"))))), arec("R", afld("i", inner())))
	add("nested in array add field", arec("R", afld("i", `{"type":"array","items":`+inner()+`}`)), arec("R", afld("i", `{"type":"array","items":`+inner(afld("y", q("int")))+`}`)))
	add("nested record type change", arec("R", afld("i", inner())), arec("R", afld("i", q("string"))))

	// Recursion.
	tree := func(extra ...string) string {
		fs := []string{afld("v", q("int")), afld("kids", `{"type":"array","items":"Tree"}`)}
		fs = append(fs, extra...)
		return `{"type":"record","name":"Tree","fields":[` + strings.Join(fs, ",") + `]}`
	}
	add("recursive same", tree(), tree())
	add("recursive add field with default", tree(), tree(afld("w", q("int"), "0")))
	add("recursive add field without default", tree(), tree(afld("w", q("int"))))

	// Logical types and attributes.
	add("logical type added", arec("R", afld("t", q("long"))), arec("R", afld("t", `{"type":"long","logicalType":"timestamp-millis"}`)))
	add("logical type removed", arec("R", afld("t", `{"type":"long","logicalType":"timestamp-millis"}`)), arec("R", afld("t", q("long"))))
	add("logical type changed", arec("R", afld("t", `{"type":"long","logicalType":"timestamp-millis"}`)), arec("R", afld("t", `{"type":"long","logicalType":"timestamp-micros"}`)))
	add("decimal precision change", arec("R", afld("d", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)), arec("R", afld("d", `{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}`)))
	add("doc change", arec("R", afld("a", q("string"))), `{"type":"record","name":"R","doc":"d","fields":[{"name":"a","type":"string","doc":"x"}]}`)
	add("custom property change", `{"type":"record","name":"R","p":"1","fields":[{"name":"a","type":"string"}]}`, `{"type":"record","name":"R","p":"2","fields":[{"name":"a","type":"string"}]}`)
	add("field order attribute", arec("R", afld("a", q("string"))), `{"type":"record","name":"R","fields":[{"name":"a","type":"string","order":"descending"}]}`)

	// Several fields at once.
	add("add and remove field", arec("R", afld("a", q("string")), afld("b", q("int"))), arec("R", afld("a", q("string")), afld("c", q("int"), "0")))
	add("swap field types", arec("R", afld("a", q("string")), afld("b", q("int"))), arec("R", afld("a", q("int")), afld("b", q("string"))))
	add("null default for union first", arec("R", afld("a", `["null","string"]`, "null")), arec("R", afld("a", `["null","string"]`, "null"), afld("b", `["null","int"]`, "null")))
	add("string default for non-first union branch", base, arec("R", afld("a", q("string")), afld("b", `["null","string"]`, `"x"`)))
	add("bytes field add default", base, arec("R", afld("a", q("string")), afld("b", q("bytes"), `"\u0000"`)))

	// Not Avro, and broken Avro.
	add("invalid new", base, `{"type":"record"`)
	add("invalid old", `{"type":"record"`, base)
	add("empty record", arec("R"), arec("R"))
	add("empty record add field", arec("R"), arec("R", afld("a", q("int"))))
	return cs
}
