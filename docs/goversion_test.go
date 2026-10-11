package docs

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// The prerequisite the document gives is the Go version go.mod demands: a reader
// on the older toolchain it names would be refused by the build.
func TestGettingStartedNamesTheGoVersionGoModNeeds(t *testing.T) {
	mod, err := os.ReadFile(filepath.Join("..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	want := regexp.MustCompile(`(?m)^go (\d+\.\d+)`).FindSubmatch(mod)
	doc, err := os.ReadFile("getting-started.md")
	if err != nil {
		t.Fatal(err)
	}
	got := regexp.MustCompile(`\*\*Go (\d+\.\d+)\+\*\*`).FindSubmatch(doc)
	if want == nil || got == nil {
		t.Fatalf("could not find the Go version in the module file (%v) and getting-started.md (%v)", want != nil, got != nil)
	}
	if string(want[1]) != string(got[1]) {
		t.Errorf("getting-started.md says Go %s+, the module requires %s", got[1], want[1])
	}
}
