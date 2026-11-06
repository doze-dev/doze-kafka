package schemaregistry

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/hamba/avro/v2"
)

// Avro compatibility, as Confluent's registry decides it: Avro's own reader/writer
// rules (org.apache.avro.SchemaCompatibility), each refusal a typed finding with the
// path in the reader schema where it was found, and Confluent's sentence for each type.
// Recorded for every incompatible pair of the corpus (testdata/compat/avro-reasons.json,
// TestCorpusReasons), so the verdicts and the reasons are held to Confluent's both.

// avroIncompatibility is one reason a reader schema cannot read what a writer wrote.
type avroIncompatibility struct {
	Type string // Avro's name for it: TYPE_MISMATCH, NAME_MISMATCH, ...
	Path string // a JSON pointer into the reader schema
	Info string // Avro's message: the names, sizes, symbols or types involved
}

// avroChecker walks a reader and a writer schema together. Named types are compared
// once per pair (a recursive type stops there), as Avro memoizes them.
type avroChecker struct {
	memo map[[2]avro.Schema][]avroIncompatibility
	busy map[[2]avro.Schema]bool
	path []string
}

// avroIncompatibilities lists why reader cannot read data written with writer.
func avroIncompatibilities(reader, writer avro.Schema) []avroIncompatibility {
	c := &avroChecker{memo: map[[2]avro.Schema][]avroIncompatibility{}, busy: map[[2]avro.Schema]bool{}}
	return c.check(reader, writer)
}

// deref follows a reference to the named schema it names.
func deref(s avro.Schema) avro.Schema {
	if r, ok := s.(*avro.RefSchema); ok {
		return r.Schema()
	}
	return s
}

// avroTypeName is Avro's Java name for a schema's type.
func avroTypeName(s avro.Schema) string {
	switch t := deref(s).Type(); t {
	case avro.Error:
		return "RECORD"
	default:
		return strings.ToUpper(string(t))
	}
}

func avroKind(s avro.Schema) avro.Type {
	t := deref(s).Type()
	if t == avro.Error {
		return avro.Record
	}
	return t
}

func (c *avroChecker) at(token string, f func() []avroIncompatibility) []avroIncompatibility {
	c.path = append(c.path, token)
	defer func() { c.path = c.path[:len(c.path)-1] }()
	return f()
}

func (c *avroChecker) here(typ, info string) []avroIncompatibility {
	var esc []string
	for _, p := range c.path {
		esc = append(esc, strings.ReplaceAll(strings.ReplaceAll(p, "~", "~0"), "/", "~1"))
	}
	return []avroIncompatibility{{Type: typ, Path: "/" + strings.Join(esc, "/"), Info: info}}
}

// check compares a pair. A pair of named types is remembered with its answer, which
// is given again (with the path it was first found at) wherever the pair comes back;
// a pair met again while it is still being compared counts as compatible.
func (c *avroChecker) check(reader, writer avro.Schema) []avroIncompatibility {
	reader, writer = deref(reader), deref(writer)
	_, rNamed := reader.(avro.NamedSchema)
	_, wNamed := writer.(avro.NamedSchema)
	if !rNamed || !wNamed {
		return c.calculate(reader, writer)
	}
	key := [2]avro.Schema{reader, writer}
	if c.busy[key] {
		return nil
	}
	if r, ok := c.memo[key]; ok {
		return r
	}
	c.busy[key] = true
	r := c.calculate(reader, writer)
	delete(c.busy, key)
	c.memo[key] = r
	return r
}

func (c *avroChecker) calculate(reader, writer avro.Schema) []avroIncompatibility {
	rk, wk := avroKind(reader), avroKind(writer)
	if rk == wk {
		switch rk {
		case avro.Array:
			return c.at("items", func() []avroIncompatibility {
				return c.check(reader.(*avro.ArraySchema).Items(), writer.(*avro.ArraySchema).Items())
			})
		case avro.Map:
			return c.at("values", func() []avroIncompatibility {
				return c.check(reader.(*avro.MapSchema).Values(), writer.(*avro.MapSchema).Values())
			})
		case avro.Fixed:
			out := c.names(reader, writer)
			rs, ws := reader.(*avro.FixedSchema).Size(), writer.(*avro.FixedSchema).Size()
			if rs != ws {
				out = append(out, c.at("size", func() []avroIncompatibility {
					return c.here("FIXED_SIZE_MISMATCH", fmt.Sprintf("expected: %d, found: %d", ws, rs))
				})...)
			}
			return out
		case avro.Enum:
			out := c.names(reader, writer)
			return append(out, c.symbols(reader.(*avro.EnumSchema), writer.(*avro.EnumSchema))...)
		case avro.Record:
			out := c.names(reader, writer)
			return append(out, c.fields(reader.(*avro.RecordSchema), writer.(*avro.RecordSchema))...)
		case avro.Union:
			// Every branch the writer may have written must be readable.
			var out []avroIncompatibility
			for i, wb := range writer.(*avro.UnionSchema).Types() {
				out = append(out, c.at(strconv.Itoa(i), func() []avroIncompatibility {
					if len(c.check(reader, wb)) == 0 {
						return nil
					}
					return c.here("MISSING_UNION_BRANCH", "reader union lacking writer type: "+avroTypeName(wb))
				})...)
			}
			return out
		}
		return nil // the same primitive
	}
	if wk == avro.Union {
		// Each branch the writer may have written is checked as a question of its own:
		// the Avro Confluent runs starts a new path for it, so what it finds there is
		// reported at "/" (recorded: "widen to union").
		var out []avroIncompatibility
		for _, wb := range writer.(*avro.UnionSchema).Types() {
			saved := c.path
			c.path = nil
			out = append(out, c.check(reader, wb)...)
			c.path = saved
		}
		return out
	}
	switch rk {
	case avro.Long:
		if wk == avro.Int {
			return nil
		}
	case avro.Float:
		if wk == avro.Int || wk == avro.Long {
			return nil
		}
	case avro.Double:
		if wk == avro.Int || wk == avro.Long || wk == avro.Float {
			return nil
		}
	case avro.Bytes:
		if wk == avro.String {
			return nil
		}
	case avro.String:
		if wk == avro.Bytes {
			return nil
		}
	case avro.Union:
		for _, rb := range reader.(*avro.UnionSchema).Types() {
			if len(c.check(rb, writer)) == 0 {
				return nil
			}
		}
		return c.here("MISSING_UNION_BRANCH", "reader union lacking writer type: "+avroTypeName(writer))
	}
	return c.here("TYPE_MISMATCH", fmt.Sprintf("reader type: %s not compatible with writer type: %s", avroTypeName(reader), avroTypeName(writer)))
}

// names compares the names of two named types: the same unqualified name, or a reader
// alias for the writer's full name.
func (c *avroChecker) names(reader, writer avro.Schema) []avroIncompatibility {
	r, w := reader.(avro.NamedSchema), writer.(avro.NamedSchema)
	if r.Name() == w.Name() || slices.Contains(r.Aliases(), w.FullName()) {
		return nil
	}
	return c.at("name", func() []avroIncompatibility {
		return c.here("NAME_MISMATCH", "expected: "+w.FullName())
	})
}

// symbols: every symbol the writer may write must be the reader's, unless the reader
// has a default symbol to read the others as.
func (c *avroChecker) symbols(reader, writer *avro.EnumSchema) []avroIncompatibility {
	var missing []string
	for _, s := range writer.Symbols() {
		if !slices.Contains(reader.Symbols(), s) {
			missing = append(missing, s)
		}
	}
	if len(missing) == 0 || (reader.HasDefault() && slices.Contains(reader.Symbols(), reader.Default())) {
		return nil
	}
	sort.Strings(missing)
	return c.at("symbols", func() []avroIncompatibility {
		return c.here("MISSING_ENUM_SYMBOLS", "["+strings.Join(missing, ", ")+"]")
	})
}

// fields: each reader field reads the writer field of its name (or of one of its
// aliases), or has a default to read when there is none.
func (c *avroChecker) fields(reader, writer *avro.RecordSchema) []avroIncompatibility {
	return c.at("fields", func() []avroIncompatibility {
		var out []avroIncompatibility
		for i, rf := range reader.Fields() {
			out = append(out, c.at(strconv.Itoa(i), func() []avroIncompatibility {
				wf := writerField(writer, rf)
				if wf != nil {
					return c.at("type", func() []avroIncompatibility { return c.check(rf.Type(), wf.Type()) })
				}
				if rf.HasDefault() {
					return nil
				}
				if e, ok := deref(rf.Type()).(*avro.EnumSchema); ok && e.HasDefault() {
					// Avro compares such a field's enum with the whole writer record.
					return c.at("type", func() []avroIncompatibility { return c.check(rf.Type(), writer) })
				}
				return c.here("READER_FIELD_MISSING_DEFAULT_VALUE", rf.Name())
			})...)
		}
		return out
	})
}

func writerField(writer *avro.RecordSchema, rf *avro.Field) *avro.Field {
	for _, wf := range writer.Fields() {
		if wf.Name() == rf.Name() {
			return wf
		}
	}
	for _, alias := range rf.Aliases() {
		for _, wf := range writer.Fields() {
			if wf.Name() == alias {
				return wf
			}
		}
	}
	return nil
}

// describe is Confluent's sentence for a finding, reader and writer named as the new
// or the old schema.
func (in avroIncompatibility) describe(readerIs, writerIs string) string {
	var d string
	switch in.Type {
	case "READER_FIELD_MISSING_DEFAULT_VALUE":
		d = fmt.Sprintf("The field '%s' at path '%s' in the %s schema has no default value and is missing in the %s schema", in.Info, in.Path, readerIs, writerIs)
	case "TYPE_MISMATCH":
		d = fmt.Sprintf("The type (path '%s') of a field in the %s schema does not match with the %s schema", in.Path, readerIs, writerIs)
	case "NAME_MISMATCH":
		d = fmt.Sprintf("The name of the schema has changed (path '%s')", in.Path)
	case "FIXED_SIZE_MISMATCH":
		d = fmt.Sprintf("The size of FIXED type field at path '%s' in the %s schema does not match with the %s schema", in.Path, readerIs, writerIs)
	case "MISSING_ENUM_SYMBOLS":
		d = fmt.Sprintf("The %s schema is missing enum symbols '%s' at path '%s' in the %s schema", readerIs, in.Info, in.Path, writerIs)
	case "MISSING_UNION_BRANCH":
		d = fmt.Sprintf("The %s schema is missing a type inside a union field at path '%s' in the %s schema", readerIs, in.Path, writerIs)
	}
	return fmt.Sprintf("{errorType:'%s', description:'%s', additionalInfo:'%s'}", in.Type, d, in.Info)
}
