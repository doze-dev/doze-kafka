package schemaregistry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hamba/avro/v2"
)

// A verdict means what it says: for every Avro pair Confluent's registry calls
// BACKWARD compatible, a value written with the old schema is read with the new one
// (Avro's schema resolution, through hamba); for every pair it calls FORWARD
// compatible, a value written with the new is read with the old. The verdicts are
// the recorded ones (testdata/compat/avro.json), so this checks the recording and the
// decoder together, and that this registry's "yes" is a promise a reader can keep.
func TestCompatibleMeansReadable(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(corpusDir, "avro.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f corpusFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	checked, skipped := 0, 0
	for _, c := range f.Cases {
		for _, dir := range []struct {
			verdict, writer, reader, name string
		}{{c.Backward, c.Old, c.New, "backward"}, {c.Forward, c.New, c.Old, "forward"}} {
			if dir.verdict != "yes" {
				continue
			}
			w, err := avroForDecode(dir.writer)
			if err != nil {
				t.Errorf("%s (%s): the writer schema does not parse: %v", c.Name, dir.name, err)
				continue
			}
			r, err := avroForDecode(dir.reader)
			if err != nil {
				t.Errorf("%s (%s): the reader schema does not parse: %v", c.Name, dir.name, err)
				continue
			}
			if refersBack(w, 0) || refersBack(r, 0) {
				// hamba's Resolve recurses without end on a recursive schema; a pair
				// with a named type used again is left out (counted below).
				skipped++
				continue
			}
			data, err := avro.Marshal(w, sampleAvro(w, 0))
			if err != nil {
				t.Errorf("%s (%s): no sample value could be written: %v", c.Name, dir.name, err)
				continue
			}
			resolved, err := avro.NewSchemaCompatibility().Resolve(r, w)
			if err != nil {
				t.Errorf("%s (%s): recorded compatible, but the reader cannot resolve the writer: %v", c.Name, dir.name, err)
				continue
			}
			var out any
			if err := avro.Unmarshal(resolved, data, &out); err != nil {
				t.Errorf("%s (%s): recorded compatible, but a value written with %s does not read with %s: %v", c.Name, dir.name, dir.writer, dir.reader, err)
				continue
			}
			checked++
		}
	}
	if checked < 300 {
		t.Errorf("only %d directions were checked (%d left out for a named type used again)", checked, skipped)
	}
	t.Logf("%d compatible directions read back; %d left out for a named type used again", checked, skipped)
}

// sampleAvro is a value of the schema, as hamba's generic encoder takes it. Past a
// few levels arrays and maps are empty, so a recursive schema ends.
func sampleAvro(s avro.Schema, depth int) any {
	depth++
	switch s := s.(type) {
	case *avro.RecordSchema:
		m := map[string]any{}
		for _, f := range s.Fields() {
			m[f.Name()] = sampleAvro(f.Type(), depth)
		}
		return m
	case *avro.RefSchema:
		return sampleAvro(s.Schema(), depth)
	case *avro.EnumSchema:
		return s.Symbols()[0]
	case *avro.FixedSchema:
		return reflect.New(reflect.ArrayOf(s.Size(), reflect.TypeFor[byte]())).Elem().Interface()
	case *avro.ArraySchema:
		if depth > 4 {
			return []any{}
		}
		return []any{sampleAvro(s.Items(), depth)}
	case *avro.MapSchema:
		if depth > 4 {
			return map[string]any{}
		}
		return map[string]any{"k": sampleAvro(s.Values(), depth)}
	case *avro.UnionSchema:
		if s.Nullable() {
			return nil
		}
		branch := s.Types()[0]
		name := string(branch.Type())
		if n, ok := branch.(avro.NamedSchema); ok {
			name = n.FullName()
		}
		return map[string]any{name: sampleAvro(branch, depth)}
	case *avro.PrimitiveSchema:
		switch s.Type() {
		case avro.String:
			return "s"
		case avro.Bytes:
			return []byte("b")
		case avro.Int:
			return int32(1)
		case avro.Long:
			return int64(1)
		case avro.Float:
			return float32(1)
		case avro.Double:
			return float64(1)
		case avro.Boolean:
			return true
		}
		return nil
	case *avro.NullSchema:
		return nil
	}
	return nil
}

// refersBack is whether a schema uses a named type again (hamba's RefSchema), which
// is what a recursive schema does.
func refersBack(s avro.Schema, depth int) bool {
	if depth > 64 {
		return true
	}
	depth++
	switch s := s.(type) {
	case *avro.RefSchema:
		return true
	case *avro.RecordSchema:
		for _, f := range s.Fields() {
			if refersBack(f.Type(), depth) {
				return true
			}
		}
	case *avro.ArraySchema:
		return refersBack(s.Items(), depth)
	case *avro.MapSchema:
		return refersBack(s.Values(), depth)
	case *avro.UnionSchema:
		for _, b := range s.Types() {
			if refersBack(b, depth) {
				return true
			}
		}
	}
	return false
}

// avroForDecode is the schema as the registry's codec holds it: parsed by parseAvro,
// which, as Confluent does, takes a default that does not fit its field.
func avroForDecode(text string) (avro.Schema, error) {
	p, err := parseAvro(text, nil)
	if err != nil {
		return nil, err
	}
	return p.(*avroSchema).schema, nil
}
