package schemaregistry

// A corpus of schema pairs whose compatibility verdicts were recorded from Confluent's
// registry (TestRecordCorpus), and replayed against this one (TestCorpus). Each pair is
// (old, new): the question asked is whether `new` may follow `old` at BACKWARD,
// FORWARD and FULL. Where this registry knowingly differs the case is listed in
// corpusDifferences with the reason.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type corpusCase struct {
	Name string `json:"name"`
	Old  string `json:"old"`
	New  string `json:"new"`
	// Verdicts recorded from Confluent: "yes", "no", or "invalid" (it refused one of the schemas).
	Backward string `json:"backward,omitempty"`
	Forward  string `json:"forward,omitempty"`
	Full     string `json:"full,omitempty"`
}

type corpusFile struct {
	Type  string       `json:"type"`
	Cases []corpusCase `json:"cases"`
}

const corpusDir = "testdata/compat"

func corpusFor(typ string) []corpusCase {
	return append(handWritten(typ), randomCorpus(typ, 300)...)
}

func handWritten(typ string) []corpusCase {
	switch typ {
	case TypeAvro:
		return avroCorpus()
	case TypeJSON:
		return jsonCorpus()
	}
	return protoCorpus()
}

// verdictOf asks a registry, over HTTP, whether new may follow old at a level.
func verdictOf(do doer, typ, old, new, level string) string {
	const subject = "zz-corpus"
	do("DELETE", "/subjects/"+subject, "")
	do("DELETE", "/subjects/"+subject+"?permanent=true", "")
	do("PUT", "/config/"+subject, `{"compatibility":"NONE"}`)
	defer func() {
		do("DELETE", "/subjects/"+subject, "")
		do("DELETE", "/subjects/"+subject+"?permanent=true", "")
		do("DELETE", "/config/"+subject, "")
	}()
	if st, _ := do("POST", "/subjects/"+subject+"/versions", typedBody(typ, old)); st != 200 {
		return "invalid"
	}
	do("PUT", "/config/"+subject, fmt.Sprintf(`{"compatibility":%q}`, level))
	st, body := do("POST", "/compatibility/subjects/"+subject+"/versions/latest", typedBody(typ, new))
	if st != 200 {
		return "invalid"
	}
	var out struct {
		Compatible bool `json:"is_compatible"`
	}
	json.Unmarshal([]byte(body), &out)
	if out.Compatible {
		return "yes"
	}
	return "no"
}

func TestRecordCorpus(t *testing.T) {
	url := os.Getenv("SR_RECORD_URL")
	if url == "" {
		t.Skip("set SR_RECORD_URL to a Confluent Schema Registry to record the compatibility corpus")
	}
	do := httpDoer(url)
	if err := os.MkdirAll(corpusDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{TypeAvro, TypeJSON, TypeProtobuf} {
		cases := corpusFor(typ)
		for i := range cases {
			c := &cases[i]
			c.Backward = verdictOf(do, typ, c.Old, c.New, "BACKWARD")
			c.Forward = verdictOf(do, typ, c.Old, c.New, "FORWARD")
			c.Full = verdictOf(do, typ, c.Old, c.New, "FULL")
		}
		b, _ := json.MarshalIndent(corpusFile{Type: typ, Cases: cases}, "", "  ")
		if err := os.WriteFile(filepath.Join(corpusDir, strings.ToLower(typ)+".json"), append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// corpusDifferences are cases (type/name) where this registry answers differently from
// Confluent's on purpose, with the reason. docs/schema-registry.md repeats them.
var corpusDifferences = map[string]string{
	"JSON/exclusiveMaximum increased": "Confluent answers 500 when a schema's exclusiveMaximum is lowered (its checker has no name for that change); this registry answers the comparison.",
	"JSON/exclusiveMaximum decreased": "Confluent answers 500 when a schema's exclusiveMaximum is lowered (its checker has no name for that change); this registry answers the comparison.",
}

func TestCorpus(t *testing.T) {
	for _, typ := range []string{TypeAvro, TypeJSON, TypeProtobuf} {
		t.Run(typ, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(corpusDir, strings.ToLower(typ)+".json"))
			if err != nil {
				t.Fatalf("no recorded corpus for %s (run TestRecordCorpus against Confluent): %v", typ, err)
			}
			var f corpusFile
			if err := json.Unmarshal(raw, &f); err != nil {
				t.Fatal(err)
			}
			reg, err := Open(newMemStore())
			if err != nil {
				t.Fatal(err)
			}
			do := recorderDoer(NewAPI(reg, Options{}))
			var bad []string
			skipped := 0
			for _, c := range f.Cases {
				if reason, ok := corpusDifferences[typ+"/"+c.Name]; ok {
					skipped++
					t.Logf("%s differs on purpose: %s", c.Name, reason)
					continue
				}
				var got [3]string
				for i, level := range []string{"BACKWARD", "FORWARD", "FULL"} {
					got[i] = verdictOf(do, typ, c.Old, c.New, level)
				}
				want := [3]string{c.Backward, c.Forward, c.Full}
				if got != want {
					bad = append(bad, fmt.Sprintf("%-40s backward/forward/full: ours %v, Confluent %v", c.Name, got, want))
				}
			}
			sort.Strings(bad)
			if len(bad) > 0 {
				t.Errorf("%d of %d recorded verdicts differ:\n  %s", len(bad), len(f.Cases), strings.Join(bad, "\n  "))
			}
			t.Logf("%d cases, %d differ on purpose", len(f.Cases), skipped)
		})
	}
}
