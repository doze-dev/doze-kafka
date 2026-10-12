package docs

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// repoFuzzRules is what checkFuzzMarkers holds this repository to.
func repoFuzzRules() fuzzRules {
	return fuzzRules{
		List: "testdata/fuzz-targets.txt",
		// The packages that face the network: a function in one of these that is
		// named and shaped like a parser must say which fuzz target reaches it, or
		// why it is not a parser of untrusted bytes.
		Net: []string{
			"internal/protocol", "internal/storage/log", "internal/telemetry", "internal/broker",
			"internal/config", "schemaregistry", "console", "internal/storage/meta",
		},
		// B12's list of parsers. Each needs a function marked class=<name>.
		Classes: []string{
			"frame", "request-header", "request-body", "record-batch", "record-reader",
			"decompression", "otlp", "sasl", "acl-filters", "config-values",
			"registry-rest", "schema-texts", "trace-headers", "console-forms",
		},
		Owned: ownedElsewhere,
	}
}

// ownedElsewhere holds the markers of functions in files this work could not
// edit (internal/storage/log's non-test files, console/traffic*.go). Each entry
// is a //fuzz: marker in waiting: move it into the function's doc comment and
// delete it here (the test says so once the function has a marker).
var ownedElsewhere = []fuzzOwned{
	// internal/storage/log: the record batch, the record reader, decompression.
	{File: "internal/storage/log/batch.go", Func: "ValidateBatches", Targets: []string{"FuzzValidateBatches"}, Class: "record-batch",
		Reason: "the target calls it on the fuzzed payload"},
	{File: "internal/storage/log/batch.go", Func: "parseHeader", Targets: []string{"FuzzValidateBatches", "FuzzScanLog", "FuzzCheckRecords"}, Class: "record-batch",
		Reason: "ValidateBatches, scanLog and CheckRecords all walk a payload with it"},
	{File: "internal/storage/log/batch.go", Func: "ParseBatch", Targets: []string{"FuzzValidateBatches"}, Class: "record-batch",
		Reason: "the target calls it on every batch ValidateBatches accepted"},
	{File: "internal/storage/log/recovery.go", Func: "scanLog", Targets: []string{"FuzzScanLog"}, Class: "record-batch",
		Reason: "the target calls it over the fuzzed bytes as a segment file"},
	{File: "internal/storage/log/recordcodec.go", Func: "DecodeRecords", Targets: []string{"FuzzDecodeRecords"}, Class: "record-reader",
		Reason: "the target calls it on the fuzzed batch"},
	{File: "internal/storage/log/compact.go", Func: "CheckRecords", Targets: []string{"FuzzCheckRecords"}, Class: "record-reader",
		Reason: "the target calls it on the fuzzed payload"},
	{File: "internal/storage/log/records.go", Func: "Decompress", Targets: []string{"FuzzDecompress"}, Class: "decompression",
		Reason: "the target calls it with every codec"},
	{File: "internal/storage/log/records.go", Func: "xerialReader.Read", Targets: []string{"FuzzDecompress"}, Class: "decompression",
		Reason: "Decompress with codec 2 and a xerial header reads its blocks through it (the target's xerial seeds)"},
	{File: "internal/storage/log/records.go", Func: "readAllInflated", Targets: []string{"FuzzDecompress"}, Class: "decompression",
		Reason: "Decompress reads every codec's stream whole through it"},
	{File: "internal/storage/log/records.go", Func: "boundedReader.Read",
		NotParser: "an io.Reader that counts what a decompressor produced against a limit; its []byte is the buffer it fills, and it parses nothing"},
	{File: "internal/storage/log/logstart.go", Func: "readLogStart",
		NotParser: "reads the log-start file this broker wrote itself (writeLogStart) in its own data directory; its string is that directory's path"},
	// console/traffic*.go.
	{File: "console/traffic_inspect.go", Func: "decodedView",
		NotParser: "indents the JSON the broker's own recorder wrote about a request (protocol.DescribeFacts, at most 1 KiB) with encoding/json, and shows it as it stands when that fails"},
}

// Foundation B12: every parser of untrusted bytes has a fuzz target, and the three
// places that say so agree: the Fuzz functions in the source, testdata/fuzz-targets.txt
// (which `task fuzz` and the weekly workflow read), and the parser table in
// architecture.md. And in the direction that matters: every function that reads
// bytes from outside names a target that exists and is run, every target is named
// by such a function, every class of parser the plan lists has one, and nothing
// in a package that faces the network is shaped like a parser and unmarked
// (checkFuzzMarkers, which fuzzmarkers_selftest_test.go shows failing on each).
func TestEveryFuzzTargetIsRunAndDocumented(t *testing.T) {
	rep := checkFuzzMarkers(os.DirFS(".."), repoFuzzRules())
	inSource := rep.Targets
	if len(inSource) < 20 {
		t.Fatalf("found %d fuzz targets in the source; the walk is broken", len(inSource))
	}
	problems := rep.Problems
	arch, err := os.ReadFile("architecture.md")
	if err != nil {
		t.Fatal(err)
	}
	for k := range inSource {
		name := strings.Fields(k)[1]
		if !strings.Contains(string(arch), "`"+name+"`") {
			problems = append(problems, "is not in the parser table of docs/architecture.md: "+name)
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Error(p)
	}
}

// The weekly job reads the list; it must give each target at least a minute.
func TestWeeklyFuzzWorkflowRunsTheListForAtLeastAMinute(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", ".github", "workflows", "fuzz.yml"))
	if err != nil {
		t.Fatal(err)
	}
	w := string(raw)
	if !strings.Contains(w, "testdata/fuzz-targets.txt") {
		t.Error("fuzz.yml does not read testdata/fuzz-targets.txt")
	}
	m := regexp.MustCompile(`inputs\.fuzztime \|\| '(\d+)s'`).FindStringSubmatch(w)
	if m == nil {
		t.Fatal("fuzz.yml has no default fuzztime")
	}
	if secs := atoi(m[1]); secs < 60 {
		t.Errorf("the weekly run gives each target %ds, the plan says at least 60", secs)
	}
	if d := regexp.MustCompile(`default: "(\d+)s"`).FindStringSubmatch(w); d == nil || atoi(d[1]) < 60 {
		t.Error("the on-demand default is under 60s")
	}
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	return n
}
