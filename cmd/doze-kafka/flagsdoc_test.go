package main

import (
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// Every setting is in three places that are written by hand: the flags (--help),
// doze-kafka.example.toml ("all keys below, shown with their defaults") and the
// tables in docs/configuration.md. Each must name exactly the settings the others
// do; offsets-retention was a flag and a documented row and missing from the
// example file, and nothing compared them.
func TestEveryFlagIsInTheExampleFileAndTheReferenceAndNothingElseIs(t *testing.T) {
	d := config.Default()
	fs, _ := newFlagSet(&d)
	flags := map[string]bool{}
	fs.VisitAll(func(f *flag.Flag) { flags[f.Name] = true })

	// The example file's key for a flag: the same name, but for the two tables.
	fileKey := func(flagName string) string {
		switch flagName {
		case "config":
			return "" // the file does not name itself.
		case "sasl-user":
			return "[credentials]"
		case "tls-cert":
			return "[tls] cert"
		case "tls-key":
			return "[tls] key"
		}
		return flagName
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "doze-kafka.example.toml"))
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	table := ""
	for _, m := range regexp.MustCompile(`(?m)^#? ?(?:\[([a-z]+)\]|([a-z][a-z0-9-]*) *=)`).FindAllStringSubmatch(string(raw), -1) {
		switch {
		case m[1] != "":
			table = m[1]
			keys["["+table+"]"] = true
		case table == "tls":
			keys["[tls] "+m[2]] = true
		case table == "credentials":
			// user names, not keys.
		default:
			keys[m[2]] = true
		}
	}

	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "configuration.md"))
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]bool{}
	for _, m := range regexp.MustCompile("(?m)^\\| `--([a-z][a-z0-9-]*)`").FindAllStringSubmatch(string(doc), -1) {
		rows[m[1]] = true
	}

	want := map[string]bool{}
	for f := range flags {
		if k := fileKey(f); k != "" {
			want[k] = true
			if !keys[k] {
				t.Errorf("--%s has no key in doze-kafka.example.toml (want %q)", f, k)
			}
		}
		if !rows[f] {
			t.Errorf("--%s has no row in docs/configuration.md", f)
		}
	}
	var extra []string
	for k := range keys {
		if !want[k] && k != "[tls]" {
			extra = append(extra, k)
		}
	}
	for r := range rows {
		if !flags[r] {
			extra = append(extra, "configuration.md --"+r)
		}
	}
	sort.Strings(extra)
	for _, e := range extra {
		t.Errorf("%s is not a flag", e)
	}
}
