package conformance

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var (
	record = flag.Bool("record", false, "run every scenario against "+KafkaImage+" in Docker and rewrite recordings/ (go tool task conformance:record)")
	only   = flag.String("only", "", "with -record or on its own: only the scenario with this name")
)

const recordingsDir = "recordings"

// TestRecord is the recorder. It does nothing unless -record is given, and
// without Docker it says so and stops: a recording is what Apache Kafka
// answered, never something written by hand or taken from doze-kafka.
func TestRecord(t *testing.T) {
	if !*record {
		t.Skip("recorder: run with -record (go tool task conformance:record); needs Docker")
	}
	if !DockerAvailable() {
		t.Skip("Docker is not available: nothing was recorded")
	}
	for _, s := range Scenarios {
		if *only != "" && s.Name != *only {
			continue
		}
		t.Run(s.Name, func(t *testing.T) {
			target, err := StartKafkaFor(s)
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			steps, err := Run(s, target)
			if err != nil {
				t.Fatal(err)
			}
			if err := Save(recordingsDir, Recording{Scenario: s.Name, Kafka: KafkaImage, Steps: steps}); err != nil {
				t.Fatal(err)
			}
			t.Logf("recorded %d steps", len(steps))
		})
	}
}

// TestReplay runs every scenario against doze-kafka in this process, with no
// Docker, and compares each step with what Apache Kafka answered. A difference
// fails unless the scenario lists it as Known; a Known entry that no longer
// differs fails too, so the list is exactly the differences there are.
func TestReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("boots a broker per scenario")
	}
	for _, s := range Scenarios {
		if *only != "" && s.Name != *only {
			continue
		}
		if s.DiskMiB > 0 || s.NoReplay != "" {
			continue // a volume of its own (Scenario.DiskMiB), or evidence only (Scenario.NoReplay).
		}
		t.Run(s.Name, func(t *testing.T) {
			rec, err := Load(recordingsDir, s.Name)
			if err != nil {
				t.Fatalf("no recording: %v (go tool task conformance:record)", err)
			}
			target, err := StartDozeFor(t.TempDir(), s)
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			steps, err := Run(s, target)
			if err != nil {
				t.Fatal(err)
			}
			diffs := Compare(rec, steps)
			unknown, stale := s.Unexplained(diffs)
			for _, u := range unknown {
				t.Errorf("differs from %s: %s", rec.Kafka, u)
			}
			for _, k := range stale {
				t.Errorf("listed as a known difference but no longer differs: step %q path %q", k.Step, k.Path)
			}
		})
	}
}

// Every written-down difference points at a sentence in docs/limitations.md,
// and every scenario has a recording that was made against the image the
// compat matrix uses.
func TestKnownDifferencesAreWrittenDownAndRecordingsAreCurrent(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "docs", "limitations.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range Scenarios {
		for _, k := range s.Known {
			if k.Limitation == "" || !strings.Contains(string(doc), k.Limitation) {
				t.Errorf("%s: step %q is a known difference with no matching sentence in docs/limitations.md (%q)", s.Name, k.Step, k.Limitation)
			}
		}
		rec, err := Load(recordingsDir, s.Name)
		if err != nil {
			t.Errorf("%s: no recording: %v", s.Name, err)
			continue
		}
		if rec.Kafka != KafkaImage {
			t.Errorf("%s: recorded against %s, the matrix uses %s: record it again", s.Name, rec.Kafka, KafkaImage)
		}
		if len(rec.Steps) == 0 {
			t.Errorf("%s: an empty recording", s.Name)
		}
	}
}
