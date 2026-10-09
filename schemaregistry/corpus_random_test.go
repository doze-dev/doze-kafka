package schemaregistry

// Randomly generated schema pairs, to keep the compatibility rules honest beyond the
// cases someone thought to write down. The generator is deterministic (a fixed seed),
// so the pairs are the same on every run; their verdicts were recorded from Confluent
// (TestRecordCorpus) like the hand-written ones.

import (
	"fmt"
	"math/rand"
	"strings"
)

type gen struct{ r *rand.Rand }

func (g gen) pick(opts ...string) string { return opts[g.r.Intn(len(opts))] }
func (g gen) chance(n int) bool          { return g.r.Intn(n) == 0 }

// ---- Avro ----

type avroField struct {
	name, typ string
	dflt      string // JSON, or "" for none.
	alias     string
}

type avroRec struct {
	name   string
	fields []avroField
}

func (a avroRec) String() string {
	var fs []string
	for _, f := range a.fields {
		s := `{"name":"` + f.name + `","type":` + f.typ
		if f.dflt != "" {
			s += `,"default":` + f.dflt
		}
		if f.alias != "" {
			s += `,"aliases":["` + f.alias + `"]`
		}
		fs = append(fs, s+`}`)
	}
	return `{"type":"record","name":"` + a.name + `","fields":[` + strings.Join(fs, ",") + `]}`
}

func (g gen) avroType() (typ, dflt string) {
	switch g.r.Intn(12) {
	case 0:
		return `"int"`, g.pick("0", "1", "")
	case 1:
		return `"long"`, g.pick("0", "")
	case 2:
		return `"string"`, g.pick(`""`, `"x"`, "")
	case 3:
		return `"boolean"`, g.pick("false", "")
	case 4:
		return `"double"`, g.pick("0.0", "")
	case 5:
		return `"bytes"`, g.pick(`""`, "")
	case 6:
		return `["null","string"]`, g.pick("null", "")
	case 7:
		return `["null","long"]`, g.pick("null", "")
	case 8:
		return `{"type":"array","items":` + g.pick(`"string"`, `"int"`, `"long"`) + `}`, g.pick("[]", "")
	case 9:
		return `{"type":"map","values":` + g.pick(`"string"`, `"int"`, `"long"`) + `}`, g.pick("{}", "")
	case 10:
		syms := g.pick(`"A","B"`, `"A","B","C"`, `"X","Y"`)
		return `{"type":"enum","name":"E` + fmt.Sprint(g.r.Intn(3)) + `","symbols":[` + syms + `]}`, ""
	default:
		return `{"type":"fixed","name":"F` + fmt.Sprint(g.r.Intn(2)) + `","size":` + g.pick("4", "8") + `}`, ""
	}
}

func (g gen) avroRec() avroRec {
	a := avroRec{name: g.pick("R", "R", "S")}
	n := 1 + g.r.Intn(4)
	for i := 0; i < n; i++ {
		t, d := g.avroType()
		a.fields = append(a.fields, avroField{name: string(rune('a' + i)), typ: t, dflt: d})
	}
	return a
}

func (g gen) mutateAvro(a avroRec) avroRec {
	out := avroRec{name: a.name, fields: append([]avroField(nil), a.fields...)}
	for k := 0; k < 1+g.r.Intn(2); k++ {
		if len(out.fields) == 0 {
			t, d := g.avroType()
			out.fields = append(out.fields, avroField{name: "z", typ: t, dflt: d})
			continue
		}
		i := g.r.Intn(len(out.fields))
		switch g.r.Intn(8) {
		case 0:
			t, d := g.avroType()
			out.fields = append(out.fields, avroField{name: string(rune('m' + g.r.Intn(5))), typ: t, dflt: d})
		case 1:
			out.fields = append(out.fields[:i], out.fields[i+1:]...)
		case 2:
			t, d := g.avroType()
			out.fields[i].typ, out.fields[i].dflt = t, d
		case 3:
			if out.fields[i].dflt == "" {
				_, d := g.avroType()
				out.fields[i].dflt = d
			} else {
				out.fields[i].dflt = ""
			}
		case 4:
			out.fields[i].alias = out.fields[i].name
			out.fields[i].name += "2"
		case 5:
			j := g.r.Intn(len(out.fields))
			out.fields[i], out.fields[j] = out.fields[j], out.fields[i]
		case 6:
			out.name = g.pick("R", "S")
		default:
			// widen or narrow a primitive.
			switch out.fields[i].typ {
			case `"int"`:
				out.fields[i].typ = `"long"`
			case `"long"`:
				out.fields[i].typ = `"int"`
			case `"string"`:
				out.fields[i].typ = `["null","string"]`
			case `["null","string"]`:
				out.fields[i].typ = `"string"`
			}
		}
	}
	return out
}

// ---- JSON Schema ----

type jprop struct {
	name, schema string
	required     bool
}

type jobjSchema struct {
	props      []jprop
	additional string // "", "false", "true" or a schema.
	extra      []string
}

func (j jobjSchema) String() string {
	var ps, rq []string
	for _, p := range j.props {
		ps = append(ps, `"`+p.name+`":`+p.schema)
		if p.required {
			rq = append(rq, `"`+p.name+`"`)
		}
	}
	s := `{"type":"object","properties":{` + strings.Join(ps, ",") + `}`
	if len(rq) > 0 {
		s += `,"required":[` + strings.Join(rq, ",") + `]`
	}
	if j.additional != "" {
		s += `,"additionalProperties":` + j.additional
	}
	for _, e := range j.extra {
		s += "," + e
	}
	return s + `}`
}

func (g gen) jsonScalar() string {
	switch g.r.Intn(14) {
	case 0:
		return `{"type":"string"}`
	case 1:
		return `{"type":"string","maxLength":` + g.pick("5", "10", "20") + `}`
	case 2:
		return `{"type":"string","minLength":` + g.pick("1", "2", "5") + `}`
	case 3:
		return `{"type":"integer"}`
	case 4:
		return `{"type":"integer","minimum":` + g.pick("0", "1", "10") + `}`
	case 5:
		return `{"type":"number","maximum":` + g.pick("10", "100") + `}`
	case 6:
		return `{"type":"boolean"}`
	case 7:
		return `{"enum":[` + g.pick(`"A","B"`, `"A","B","C"`, `"X"`) + `]}`
	case 8:
		return `{"type":["string","null"]}`
	case 9:
		return `{"type":"array","items":` + g.pick(`{"type":"string"}`, `{"type":"integer"}`) + `}`
	case 10:
		return `{"type":"array","items":{"type":"string"},"minItems":` + g.pick("1", "2") + `}`
	case 11:
		return `{"type":"string","pattern":"` + g.pick("^a", "^b", "x$") + `"}`
	case 12:
		return `{"oneOf":[{"type":"string"},{"type":"integer"}]}`
	default:
		return `{"type":"number"}`
	}
}

func (g gen) jsonObj() jobjSchema {
	j := jobjSchema{additional: g.pick("", "", "false", "true", `{"type":"string"}`)}
	n := 1 + g.r.Intn(4)
	for i := 0; i < n; i++ {
		s := g.jsonScalar()
		if g.chance(6) {
			s = jobjSchema{props: []jprop{{name: "x", schema: g.jsonScalar(), required: g.chance(2)}}, additional: g.pick("", "false")}.String()
		}
		j.props = append(j.props, jprop{name: string(rune('a' + i)), schema: s, required: g.chance(3)})
	}
	if g.chance(5) {
		j.extra = append(j.extra, `"minProperties":1`)
	}
	return j
}

func (g gen) mutateJSON(j jobjSchema) jobjSchema {
	out := jobjSchema{additional: j.additional, extra: append([]string(nil), j.extra...), props: append([]jprop(nil), j.props...)}
	for k := 0; k < 1+g.r.Intn(2); k++ {
		if len(out.props) == 0 {
			out.props = append(out.props, jprop{name: "z", schema: g.jsonScalar()})
			continue
		}
		i := g.r.Intn(len(out.props))
		switch g.r.Intn(8) {
		case 0:
			out.props = append(out.props, jprop{name: string(rune('m' + g.r.Intn(5))), schema: g.jsonScalar(), required: g.chance(3)})
		case 1:
			out.props = append(out.props[:i], out.props[i+1:]...)
		case 2:
			out.props[i].schema = g.jsonScalar()
		case 3:
			out.props[i].required = !out.props[i].required
		case 4:
			out.additional = g.pick("", "false", "true", `{"type":"string"}`, `{"type":["string","integer"]}`)
		case 5:
			out.props[i].schema = g.jsonScalar()
			out.props[i].required = !out.props[i].required
		case 6:
			if len(out.extra) > 0 {
				out.extra = nil
			} else {
				out.extra = []string{`"maxProperties":` + g.pick("3", "5")}
			}
		default:
			out.props[i].name += "2"
		}
	}
	return out
}

// ---- Protobuf ----

type pfield struct {
	label, typ, name string
	num              int
	oneof            string
}

type pmessage struct {
	syntax string
	fields []pfield
}

func (m pmessage) String() string {
	var b strings.Builder
	b.WriteString(`syntax = "` + m.syntax + `";` + "\n")
	b.WriteString("enum E {\n  Z = 0;\n  A = 1;\n}\nmessage Sub {\n  int32 v = 1;\n}\nmessage Other {\n  string s = 1;\n}\n")
	b.WriteString("message M {\n")
	oneofs := map[string][]pfield{}
	var order []string
	for _, f := range m.fields {
		if f.oneof != "" {
			if _, ok := oneofs[f.oneof]; !ok {
				order = append(order, f.oneof)
			}
			oneofs[f.oneof] = append(oneofs[f.oneof], f)
			continue
		}
		l := f.label
		if l != "" {
			l += " "
		}
		fmt.Fprintf(&b, "  %s%s %s = %d;\n", l, f.typ, f.name, f.num)
	}
	for _, o := range order {
		fmt.Fprintf(&b, "  oneof %s {\n", o)
		for _, f := range oneofs[o] {
			fmt.Fprintf(&b, "    %s %s = %d;\n", f.typ, f.name, f.num)
		}
		b.WriteString("  }\n")
	}
	b.WriteString("}\n")
	return b.String()
}

func (g gen) protoType() string {
	return g.pick("int32", "int64", "uint32", "uint64", "sint32", "bool", "fixed32", "sfixed32", "fixed64", "float", "double", "string", "bytes", "E", "Sub", "Other", "map<string, int32>", "map<string, Sub>")
}

func (g gen) protoMessage() pmessage {
	m := pmessage{syntax: g.pick("proto3", "proto3", "proto2")}
	n := 1 + g.r.Intn(5)
	for i := 0; i < n; i++ {
		f := pfield{name: string(rune('a' + i)), num: i + 1, typ: g.protoType()}
		isMap := strings.HasPrefix(f.typ, "map<")
		switch {
		case m.syntax == "proto2":
			f.label = g.pick("optional", "optional", "required", "repeated")
			if isMap {
				f.label = ""
			}
		case !isMap && g.chance(3):
			f.label = g.pick("repeated", "optional")
		}
		if !isMap && f.label == "" && m.syntax == "proto3" && g.chance(8) {
			f.oneof = "o"
		}
		m.fields = append(m.fields, f)
	}
	// A oneof may not hold maps or repeated fields.
	for i, f := range m.fields {
		if f.oneof != "" && (f.label != "" || strings.HasPrefix(f.typ, "map<")) {
			m.fields[i].oneof = ""
		}
	}
	return m
}

func (g gen) mutateProto(m pmessage) pmessage {
	out := pmessage{syntax: m.syntax, fields: append([]pfield(nil), m.fields...)}
	for k := 0; k < 1+g.r.Intn(2); k++ {
		if len(out.fields) == 0 {
			out.fields = append(out.fields, pfield{name: "z", num: 9, typ: g.protoType()})
			continue
		}
		i := g.r.Intn(len(out.fields))
		switch g.r.Intn(7) {
		case 0:
			t := g.protoType()
			f := pfield{name: fmt.Sprintf("n%d", len(out.fields)), num: 100 + len(out.fields), typ: t}
			if out.syntax == "proto2" && !strings.HasPrefix(t, "map<") {
				f.label = g.pick("optional", "required")
			}
			out.fields = append(out.fields, f)
		case 1:
			out.fields = append(out.fields[:i], out.fields[i+1:]...)
		case 2:
			t := g.protoType()
			if out.fields[i].oneof != "" && strings.HasPrefix(t, "map<") {
				break
			}
			if strings.HasPrefix(t, "map<") != strings.HasPrefix(out.fields[i].typ, "map<") {
				out.fields[i].label = ""
				if out.syntax == "proto2" && !strings.HasPrefix(t, "map<") {
					out.fields[i].label = "optional"
				}
			}
			out.fields[i].typ = t
		case 3:
			if !strings.HasPrefix(out.fields[i].typ, "map<") && out.fields[i].oneof == "" {
				out.fields[i].label = g.pick("", "repeated", "optional")
				if out.syntax == "proto2" && out.fields[i].label == "" {
					out.fields[i].label = "optional"
				}
			}
		case 4:
			out.fields[i].name += "x"
		case 5:
			out.fields[i].num += 20 + i
		default:
			if out.fields[i].oneof == "" && out.fields[i].label == "" && !strings.HasPrefix(out.fields[i].typ, "map<") {
				out.fields[i].oneof = "o"
			} else {
				out.fields[i].oneof = ""
			}
		}
	}
	return out
}

func randomCorpus(typ string, n int) []corpusCase {
	g := gen{rand.New(rand.NewSource(int64(len(typ))*7919 + 17))}
	var cs []corpusCase
	for i := 0; i < n; i++ {
		var old, new string
		switch typ {
		case TypeAvro:
			a := g.avroRec()
			old, new = a.String(), g.mutateAvro(a).String()
		case TypeJSON:
			j := g.jsonObj()
			old, new = j.String(), g.mutateJSON(j).String()
		default:
			m := g.protoMessage()
			old, new = m.String(), g.mutateProto(m).String()
		}
		cs = append(cs, corpusCase{Name: fmt.Sprintf("random %03d", i), Old: old, New: new})
	}
	return cs
}
