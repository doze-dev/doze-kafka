package console

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/settings"
)

var fieldRe = regexp.MustCompile(`(?s)<(?:input|select|textarea)\b[^>]*?\sname="([^"]*)"`)

// templateFields returns the named form fields in a template's source, skipping
// those whose name is itself generated ({{.Key}}): the topic settings, which are
// built from the table (TestTopicFormFieldsAreInTheSettingsTable).
func templateFields(src string) []string {
	var out []string
	for _, m := range fieldRe.FindAllStringSubmatch(src, -1) {
		if strings.Contains(m[1], "{{") {
			continue
		}
		out = append(out, m[1])
	}
	return out
}

// unaccountedFields is the fields in src with no entry in formFields, or whose
// entry says nothing (neither a record nor a reason).
func unaccountedFields(src string, fields map[string]formField) []string {
	var bad []string
	for _, f := range templateFields(src) {
		e, ok := fields[f]
		if !ok || (e.Name == "" && !e.Family && e.Not == "") {
			bad = append(bad, f)
		}
	}
	return bad
}

// Every form field in the console is accounted for: it has a settings record (and
// the template shows its teaching line, generated), or it says why it is not a
// setting. A field added without either fails here.
func TestEveryFormFieldHasASettingsRecordOrSaysWhyNot(t *testing.T) {
	files, err := filepath.Glob("templates/*.html")
	if err != nil || len(files) == 0 {
		t.Fatalf("templates: %v %v", files, err)
	}
	seen := map[string]bool{}
	var all strings.Builder
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		all.WriteString(src)
		for _, bad := range unaccountedFields(src, formFields) {
			t.Errorf("%s: form field %q has no settings record (add it to the table, or to formFields with the reason it is not a setting)", f, bad)
		}
		for _, name := range templateFields(src) {
			seen[name] = true
		}
	}
	for name, e := range formFields {
		if !seen[name] {
			t.Errorf("formFields lists %q and no template has such a field", name)
		}
		switch {
		case e.Not != "":
			if e.Name != "" || e.Family {
				t.Errorf("%q is both a setting and not one", name)
			}
		case e.Family:
			if len(settingsTable.All(e.Kind)) == 0 {
				t.Errorf("%q takes one of the %s settings and there are none", name, e.Kind)
			}
			if !strings.Contains(all.String(), `teach "`+string(e.Kind)+`"`) {
				t.Errorf("%q has records but no template shows a teaching line for the %s family", name, e.Kind)
			}
		default:
			if _, ok := settingsTable.Lookup(e.Kind, e.Name); !ok {
				t.Errorf("%q names the record %s/%s and the table has none", name, e.Kind, e.Name)
			}
			if !strings.Contains(all.String(), `teach "`+string(e.Kind)+`" "`+e.Name+`"`) {
				t.Errorf("%q has the record %s/%s but no template shows its teaching line", name, e.Kind, e.Name)
			}
		}
	}
}

// The check itself discriminates: a field that is nowhere, or that says nothing,
// is reported.
func TestTheFormFieldCheckCatchesAFieldWithNoRecord(t *testing.T) {
	src := `<input name="partitions"><select class="x" name="surprise"></select><textarea name="value"></textarea><input name="{{.Key}}"><input name="mute">`
	table := map[string]formField{
		"partitions": {Kind: settings.Broker, Name: "max-partitions"},
		"value":      {Not: "a record's value"},
		"mute":       {},
	}
	got := unaccountedFields(src, table)
	sort.Strings(got)
	if strings.Join(got, ",") != "mute,surprise" {
		t.Errorf("unaccounted = %v, want [mute surprise]", got)
	}
}

// The line is generated from the record, not written: the words are the table's.
func TestTeachingLinesAreGeneratedFromTheTable(t *testing.T) {
	line := teachLine("broker", "max-partitions")
	s, _ := settings.Lookup(settings.Broker, "max-partitions")
	if line == "" || !strings.Contains(line, s.Rule()) {
		t.Errorf("max-partitions teaching line %q does not carry the record's rule %q", line, s.Rule())
	}
	if teachLine("broker", "no-such-setting") != "" {
		t.Error("a field with no record produced a line")
	}
	// A stand-in table is read, so the console holds no copy of its own.
	old := settingsTable
	defer func() { settingsTable = old }()
	settingsTable = fakeSettings{}
	if got := teachLine("group", "new-offset"); !strings.Contains(got, "from the fake") {
		t.Errorf("the console did not read through its source: %q", got)
	}
}

type fakeSettings struct{}

func (fakeSettings) Lookup(settings.Kind, string) (settings.Setting, bool) {
	return settings.Setting{Name: "x", Meaning: "from the fake", Effect: settings.Applied}, true
}
func (fakeSettings) All(settings.Kind) []settings.Setting { return nil }
