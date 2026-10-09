package schemaregistry

import "testing"

// The registry reads schema text from whoever can reach its port: a hostile or merely
// broken schema must come back as an error, never a panic or a hang.
func FuzzRegisterAndCompare(f *testing.F) {
	for _, c := range append(append(avroCorpus(), jsonCorpus()...), protoCorpus()...) {
		typ := TypeAvro
		switch {
		case len(c.Old) > 6 && c.Old[:6] == "syntax":
			typ = TypeProtobuf
		case len(c.Old) > 0 && c.Old[0] == '{' && containsAny(c.Old, `"properties"`, `"oneOf"`, `"enum"`, `"$ref"`, `"items"`) && !containsAny(c.Old, `"fields"`):
			typ = TypeJSON
		}
		f.Add(typ, c.Old, c.New)
	}
	f.Fuzz(func(t *testing.T, typ, old, new string) {
		if len(old)+len(new) > 1<<14 {
			t.Skip()
		}
		switch typ {
		case "JSON", "PROTOBUF":
		default:
			typ = "AVRO"
		}
		r, err := Open(newMemStore())
		if err != nil {
			t.Fatal(err)
		}
		if _, e := r.Register("s", SchemaInput{Schema: old, SchemaType: typ}, false); e != nil {
			return
		}
		r.SetCompat("s", CompatFull)
		r.CheckCompatible("s", "latest", SchemaInput{Schema: new, SchemaType: typ}, true)
		r.Register("s", SchemaInput{Schema: new, SchemaType: typ}, true)
	})
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}
