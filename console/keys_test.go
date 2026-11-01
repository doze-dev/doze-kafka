package console

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

// A key shell.js handles that the declared map does not list is a key nobody can
// look up: the help overlay is generated from the map, so the map has to be the
// whole truth. Every literal key in shell.js is compared with it.
func TestEveryKeyShellJSHandlesIsInTheDeclaredMap(t *testing.T) {
	b, err := os.ReadFile("static/shell.js")
	if err != nil {
		t.Fatal(err)
	}
	// Comments name keys in prose; they are not bindings.
	src := regexp.MustCompile(`(?m)//.*$`).ReplaceAllString(string(b), "")
	declared := map[string]bool{}
	for _, k := range keyMap {
		declared[strings.TrimPrefix(k.Key, "Mod+")] = true
		if f, _, ok := strings.Cut(k.Key, " "); ok {
			declared[f] = true // a chord is armed by its first key.
		}
	}
	lit := regexp.MustCompile(`\.key\s*===?\s*(?:"([^"]+)"|'([^']+)')`)
	found := 0
	for _, m := range lit.FindAllStringSubmatch(src, -1) {
		key := m[1] + m[2]
		found++
		if !declared[key] {
			t.Errorf("shell.js handles the key %q, which keys.go does not list (so the help overlay would not say it)", key)
		}
	}
	if found < 6 {
		t.Fatalf("found only %d key tests in shell.js; the scan is not reading it", found)
	}
	// The single keys it dispatches by name are all in the map too: every action in
	// pageActions is bound, and every page-scope binding has an action.
	acts := regexp.MustCompile(`(?s)var pageActions = \{(.*?)\n  \};`).FindStringSubmatch(src)
	if acts == nil {
		t.Fatal("pageActions not found in shell.js")
	}
	have := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^    ("?[\w-]+"?):`).FindAllStringSubmatch(acts[1], -1) {
		have[strings.Trim(m[1], `"`)] = true
	}
	for _, k := range keyMap {
		if k.Scope == ScopeDialogs || strings.HasPrefix(k.Key, "Mod+") {
			continue
		}
		if !have[k.Action] {
			t.Errorf("%q is declared with the action %q and shell.js has no such action", k.Key, k.Action)
		}
	}
	for a := range have {
		bound := false
		for _, k := range keyMap {
			if k.Action == a {
				bound = true
			}
		}
		if !bound {
			t.Errorf("shell.js has the action %q and no key is declared for it", a)
		}
	}
}

func TestTheKeyMapHasNoDuplicatesAndTheHelpOverlayListsEveryKey(t *testing.T) {
	// A key may mean two things where the pages differ (p is pause on a page that tails
	// and produce on a topic), so a binding is its key and its words.
	seen := map[string]bool{}
	for _, k := range keyMap {
		if seen[k.Key+"|"+k.Label] {
			t.Errorf("key %q is declared twice", k.Key)
		}
		seen[k.Key+"|"+k.Label] = true
		if k.Show == "" || k.Label == "" || k.Action == "" {
			t.Errorf("%q is incomplete: %+v", k.Key, k)
		}
	}
	c := newTestConsole(t)
	page := req(t, c, "GET", "/_console/topics", nil).Body.String()
	help := between(page, `id="help"`, `id="keymap"`)
	// Sy-06 lists three columns: where to go, what a list does, what a page does. The
	// dialogs' own keys are bound by the dialogs and are not in it.
	for _, k := range keyMap {
		if k.Scope == ScopeDialogs {
			continue
		}
		if k.Target == "/traffic" {
			continue // only where there is traffic; the plain test console has none.
		}
		caps := ""
		for _, f := range strings.Fields(k.Show) {
			caps += `<span class="kbd">` + f + `</span>`
		}
		if !strings.Contains(help, caps) || !strings.Contains(help, k.Label) {
			t.Errorf("the help overlay does not list %q (%s)", k.Show, k.Label)
		}
	}
	// And the map the script reads is the same table.
	m := regexp.MustCompile(`(?s)<script type="application/json" id="keymap">(.*?)</script>`).FindStringSubmatch(page)
	if m == nil {
		t.Fatal("the page does not carry the key map")
	}
	var got []keyBinding
	if err := json.Unmarshal([]byte(m[1]), &got); err != nil || len(got) != len(keyMap) {
		t.Fatalf("keymap JSON: %v, %d entries (want %d)", err, len(got), len(keyMap))
	}
}

// The palette catalogue carries the console's actions, with the server's own, and
// there is a theme action for every theme the stylesheet has.
func TestThePaletteCatalogueHasActionsAndEveryTheme(t *testing.T) {
	c := newTestConsole(t)
	var all []paletteEntry
	if err := json.Unmarshal(req(t, c, "GET", "/_console/api/palette", nil).Body.Bytes(), &all); err != nil {
		t.Fatal(err)
	}
	acts := map[string]bool{}
	var navs int
	for _, e := range all {
		if e.A != "" {
			acts[e.A] = true
			if e.K != "action" || e.U != "" {
				t.Errorf("client action %+v should be an action with no address", e)
			}
		}
		if e.K == "action" && e.U != "" {
			navs++
		}
	}
	// Sy-05 lists no theme actions: the theme is the top bar's control, not a palette row.
	for id := range acts {
		if strings.HasPrefix(id, "theme:") {
			t.Errorf("the palette offers %q, which Sy-05 does not list", id)
		}
	}
	for _, want := range []string{"pause", "help"} {
		if !acts[want] {
			t.Errorf("the palette has no action %q", want)
		}
	}
	if navs == 0 {
		t.Error("the palette lost the actions that go somewhere (create topic)")
	}
	css := readAll(t, "static/app.css")
	for _, theme := range []string{"dark", "contrast"} {
		if !strings.Contains(css, `:root[data-theme="`+theme+`"]`) {
			t.Errorf("the top bar offers the %s theme and the stylesheet has no block for it", theme)
		}
	}
	// The three themes define the same tokens, so none falls back to another's.
	base := tokensIn(css, `:root {`)
	for _, theme := range []string{"dark", "contrast"} {
		got := tokensIn(css, `:root[data-theme="`+theme+`"] {`)
		for _, tok := range []string{"--bg", "--card", "--ink", "--mut", "--line", "--line2", "--sel", "--well",
			"--ok", "--warn", "--bad", "--deny", "--okbg", "--warnbg", "--badbg", "--denybg"} {
			if _, ok := got[tok]; !ok {
				t.Errorf("theme %s does not define %s", theme, tok)
			}
			if _, ok := base[tok]; !ok {
				t.Errorf("the light theme does not define %s", tok)
			}
		}
	}
}

// tokensIn reads the custom properties of the block that starts with head.
func tokensIn(css, head string) map[string]string {
	i := strings.Index(css, head)
	if i < 0 {
		return nil
	}
	rest := css[i+len(head):]
	j := strings.Index(rest, "\n}")
	if j < 0 {
		return nil
	}
	out := map[string]string{}
	for _, m := range regexp.MustCompile(`(--[a-z0-9-]+)\s*:\s*([^;]+);`).FindAllStringSubmatch(rest[:j], -1) {
		out[m[1]] = strings.TrimSpace(m[2])
	}
	return out
}
