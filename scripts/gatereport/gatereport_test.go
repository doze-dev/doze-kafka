// Package gatereport tests scripts/gate-report.sh, the tool that writes a stage's gate
// results into plan/evidence/ so the close commit carries them.
package gatereport

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// A planted failing gate: the report records it as FAIL with its exit status and its
// output, records the passing one as PASS, and the script exits non-zero.
func TestTheReportRecordsEachGateAndFailsWithAFailedOne(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("a bash script")
	}
	dir := t.TempDir()
	gates := filepath.Join(dir, "gates")
	list := "passes\techo all well\nfails\techo planted failure; exit 3\n"
	if err := os.WriteFile(gates, []byte(list), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "report.md")
	cmd := exec.Command("bash", "../gate-report.sh", "--out", out)
	cmd.Env = append(os.Environ(), "GATE_REPORT_GATES="+gates)
	log, err := cmd.CombinedOutput()
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != 1 {
		t.Fatalf("want exit 1 with a failed gate, got %v\n%s", err, log)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("no report written: %v\n%s", err, log)
	}
	report := string(raw)
	for _, want := range []string{"| passes | PASS | 0 |", "| fails | FAIL | 3 |", "planted failure", "all well", "- commit: `", "- platform: "} {
		if !strings.Contains(report, want) {
			t.Errorf("the report does not say %q:\n%s", want, report)
		}
	}

	// Every gate passing: exit 0.
	if err := os.WriteFile(gates, []byte("passes\ttrue\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd = exec.Command("bash", "../gate-report.sh", "--out", out)
	cmd.Env = append(os.Environ(), "GATE_REPORT_GATES="+gates)
	if log, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("all gates passed and the script failed: %v\n%s", err, log)
	}
}
