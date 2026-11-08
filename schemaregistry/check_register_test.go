package schemaregistry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The console's check and a client's register are one engine: for every corpus pair
// and level, a check of the new schema against the subject says compatible exactly
// when registering it succeeds, and gives the same reasons as the 409. (SCH-14,
// SCH-34; the one place they differ on purpose, a schema the subject already holds,
// is not in the corpus: old and new always differ.)
func TestCheckEqualsRegister(t *testing.T) {
	for _, typ := range []string{TypeAvro, TypeJSON, TypeProtobuf} {
		raw, err := os.ReadFile(filepath.Join(corpusDir, strings.ToLower(typ)+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var f corpusFile
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatal(err)
		}
		for _, c := range f.Cases {
			for _, level := range []string{CompatBackward, CompatForward, CompatFull, CompatFull + "_TRANSITIVE"} {
				r, _ := Open(NewMemoryStorage())
				if _, e := r.Register("s", SchemaInput{SchemaType: typ, Schema: c.Old}, false); e != nil {
					continue // Confluent refused it too, or it is not this case's question
				}
				if e := r.SetCompat("s", level); e != nil {
					t.Fatal(e)
				}
				in := SchemaInput{SchemaType: typ, Schema: c.New}
				found, ce := r.CheckCompatible("s", "", in, false)
				_, re := r.Register("s", in, false)
				if ce != nil {
					if re == nil || re.Code != ce.Code {
						t.Errorf("%s/%s at %s: check refused %v, register %v", typ, c.Name, level, ce, re)
					}
					continue
				}
				var msgs []string
				for _, fd := range found {
					msgs = append(msgs, fd.Messages()...)
				}
				switch {
				case len(found) == 0 && re != nil:
					t.Errorf("%s/%s at %s: check says compatible, register refused: %v", typ, c.Name, level, re)
				case len(found) > 0 && re == nil:
					t.Errorf("%s/%s at %s: check says incompatible (%v), register accepted", typ, c.Name, level, msgs)
				case len(found) > 0 && !strings.Contains(re.Message, "details: ["+strings.Join(msgs, ", ")+"]"):
					t.Errorf("%s/%s at %s: register's reasons are not the check's:\n  check:    %v\n  register: %s", typ, c.Name, level, msgs, re.Message)
				}
			}
		}
	}
}
