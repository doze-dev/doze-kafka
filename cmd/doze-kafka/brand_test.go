package main

import (
	"regexp"
	"strings"
	"testing"
)

// The top bar says which broker this is by the instance's name (the one it answers
// to as kafka.<name>.doze), not by whatever the data directory happens to be called.
func TestConsoleBrandIsTheInstanceName(t *testing.T) {
	out, _ := start(t, "--name", "harbour")
	waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "doze-kafka is up") })
	console := regexp.MustCompile(`console   (http://\S+)`).FindStringSubmatch(out.String())[1]
	page := consoleGet(t, console, "/_console/topics")
	m := regexp.MustCompile(`id="workspace-label">([^<]*)<`).FindStringSubmatch(page)
	if m == nil || m[1] != "harbour" {
		t.Errorf("the top bar names the broker %q, want the instance name %q", m, "harbour")
	}
}

// The top bar prints "doze-kafka <instance>" (Sy-01). An instance with no name of
// its own must not make it "doze-kafka doze-kafka" or "doze-kafka local".
func TestWorkspaceLabelOmitsTheDefaultInstance(t *testing.T) {
	for name, want := range map[string]string{"harbour": "harbour", "doze-kafka": "", "local": ""} {
		if got := workspaceLabel(name); got != want {
			t.Errorf("workspaceLabel(%q) = %q, want %q", name, got, want)
		}
	}
}
