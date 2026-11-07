package schemaregistry

// R9: Confluent's reasons. For every corpus pair it refuses at a level, the verbose
// compatibility answer's messages, recorded so the reasons given here can be held to
// its shape and wording (SCH-6), not only its verdict.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// corpusReasons are the reasons Confluent gave, by case name and level, without the
// three lines that close every answer (oldSchemaVersion, oldSchema, compatibility).
type corpusReasons struct {
	Type    string                         `json:"type"`
	Reasons map[string]map[string][]string `json:"reasons"` // case -> level -> reasons
}

func reasonsFile(typ string) string {
	return filepath.Join(corpusDir, strings.ToLower(typ)+"-reasons.json")
}

// reasonsOf asks a registry, over HTTP, why new may not follow old at a level: the
// verbose messages before the closing three.
func reasonsOf(do doer, typ, old, new, level string) ([]string, bool) {
	const subject = "zz-corpus-reasons"
	do("DELETE", "/subjects/"+subject, "", "")
	do("DELETE", "/subjects/"+subject+"?permanent=true", "", "")
	do("PUT", "/config/"+subject, `{"compatibility":"NONE"}`, "")
	defer func() {
		do("DELETE", "/subjects/"+subject, "", "")
		do("DELETE", "/subjects/"+subject+"?permanent=true", "", "")
		do("DELETE", "/config/"+subject, "", "")
	}()
	if st, _ := do("POST", "/subjects/"+subject+"/versions", typedBody(typ, old), ""); st != 200 {
		return nil, false
	}
	do("PUT", "/config/"+subject, fmt.Sprintf(`{"compatibility":%q}`, level), "")
	st, body := do("POST", "/compatibility/subjects/"+subject+"/versions/latest?verbose=true", typedBody(typ, new), "")
	if st != 200 {
		return nil, false
	}
	var out struct {
		Compatible bool     `json:"is_compatible"`
		Messages   []string `json:"messages"`
	}
	json.Unmarshal([]byte(body), &out)
	if out.Compatible {
		return nil, false
	}
	return reasonLines(out.Messages), true
}

// reasonLines drops the closing lines of a verbose answer.
func reasonLines(msgs []string) []string {
	var out []string
	for _, m := range msgs {
		if strings.HasPrefix(m, "{oldSchemaVersion:") || strings.HasPrefix(m, "{oldSchema:") ||
			strings.HasPrefix(m, "{compatibility:") || strings.HasPrefix(m, "{validateFields:") {
			continue
		}
		out = append(out, m)
	}
	return out
}

func TestRecordCorpusReasons(t *testing.T) {
	url := os.Getenv("SR_RECORD_URL")
	if url == "" || os.Getenv("SR_RECORD_REASONS") == "" {
		t.Skip("set SR_RECORD_URL and SR_RECORD_REASONS=1 to record Confluent's reasons for the corpus")
	}
	do := httpDoer(url)
	for _, typ := range []string{TypeAvro, TypeJSON, TypeProtobuf} {
		raw, err := os.ReadFile(filepath.Join(corpusDir, strings.ToLower(typ)+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var f corpusFile
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatal(err)
		}
		out := corpusReasons{Type: typ, Reasons: map[string]map[string][]string{}}
		for _, c := range f.Cases {
			for level, verdict := range map[string]string{"BACKWARD": c.Backward, "FORWARD": c.Forward, "FULL": c.Full} {
				if verdict != "no" {
					continue
				}
				if rs, ok := reasonsOf(do, typ, c.Old, c.New, level); ok {
					if out.Reasons[c.Name] == nil {
						out.Reasons[c.Name] = map[string][]string{}
					}
					out.Reasons[c.Name][level] = rs
				}
			}
		}
		b, _ := json.MarshalIndent(out, "", "  ")
		if err := os.WriteFile(reasonsFile(typ), append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// The reasons given here for each refused corpus pair are Confluent's, word for word
// (SCH-6). corpusReasonDifferences lists, by type, case and level, where they knowingly
// are not, with the reason.
func TestCorpusReasons(t *testing.T) {
	for _, typ := range []string{TypeAvro, TypeJSON, TypeProtobuf} {
		t.Run(typ, func(t *testing.T) {
			raw, err := os.ReadFile(reasonsFile(typ))
			if err != nil {
				t.Fatalf("no recorded reasons for %s (run TestRecordCorpusReasons): %v", typ, err)
			}
			var want corpusReasons
			if err := json.Unmarshal(raw, &want); err != nil {
				t.Fatal(err)
			}
			raw, _ = os.ReadFile(filepath.Join(corpusDir, strings.ToLower(typ)+".json"))
			var f corpusFile
			json.Unmarshal(raw, &f)
			reg, err := Open(NewMemoryStorage())
			if err != nil {
				t.Fatal(err)
			}
			do := recorderDoer(NewAPI(reg, Options{AllowedHosts: testHosts}))
			var bad []string
			total := 0
			for _, c := range f.Cases {
				for level, rs := range want.Reasons[c.Name] {
					total++
					key := typ + "/" + c.Name + "/" + level
					if _, ok := corpusDifferences[typ+"/"+c.Name]; ok {
						continue
					}
					if _, ok := corpusReasonDifferences[key]; ok {
						continue
					}
					got, _ := reasonsOf(do, typ, c.Old, c.New, level)
					if strings.Join(got, "\n") != strings.Join(rs, "\n") {
						bad = append(bad, fmt.Sprintf("%s\n    ours:      %s\n    Confluent: %s", key, strings.Join(got, "\n               "), strings.Join(rs, "\n               ")))
					}
				}
			}
			if len(bad) > 0 {
				t.Errorf("%d of %d refusals give other reasons than Confluent's:\n  %s", len(bad), total, strings.Join(bad, "\n  "))
			}
		})
	}
}

// corpusReasonDifferences are refusals whose reasons differ from Confluent's on
// purpose, keyed type/case/level, with why.
var corpusReasonDifferences = map[string]string{
	"JSON/enum with type/FORWARD":  "A schema with both a type and an enum is two schemas combined in Confluent's JSON Schema library, so a narrower enum is reported as COMBINED_TYPE_SUBSCHEMAS_CHANGED at the property; here it is one schema, and the same refusal is ENUM_ARRAY_NARROWED at its enum.",
	"JSON/enum with type/FULL":     "As enum with type/FORWARD.",
	"PROTOBUF/random 045/BACKWARD": "A field moved into a oneof that also lost a member: Confluent reports only the member lost, this registry also the move (FIELD_MOVED_TO_EXISTING_ONEOF). The verdict is the same; one case is not enough to know Confluent's rule.",
	"PROTOBUF/random 045/FULL":     "As random 045/BACKWARD.",
}
