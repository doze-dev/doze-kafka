package faultinject

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The seam is for tests. A release build must not carry it: no package, no
// symbol, no environment variable to arm. The check reads the binary's symbol
// table, so it holds whatever the source looks like. The same check on a build
// with the tag proves the grep can see the seam when it is there.
func TestSeamIsAbsentFromANormalBuildAndPresentUnderTheTag(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary twice")
	}
	symbols := func(tags string) string {
		t.Helper()
		bin := filepath.Join(t.TempDir(), "doze-kafka")
		args := []string{"build", "-o", bin}
		if tags != "" {
			args = append(args, "-tags", tags)
		}
		build := exec.Command("go", append(args, "github.com/doze-dev/doze-kafka/cmd/doze-kafka")...)
		build.Env = append(os.Environ(), "GOWORK=off")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("go build %s: %v\n%s", tags, err, out)
		}
		out, err := exec.Command("go", "tool", "nm", bin).Output()
		if err != nil {
			t.Fatalf("go tool nm: %v", err)
		}
		return string(out)
	}

	count := func(table string) (n int, first string) {
		for _, line := range strings.Split(table, "\n") {
			if strings.Contains(line, "internal/faultinject") {
				if n == 0 {
					first = line
				}
				n++
			}
		}
		return
	}

	if n, first := count(symbols("")); n != 0 {
		t.Fatalf("a normal build has %d fault-seam symbols, e.g. %q", n, first)
	}
	if n, _ := count(symbols("faultinject")); n == 0 {
		t.Fatal("the build with -tags faultinject has no fault-seam symbols: this test cannot see the seam")
	}
}
