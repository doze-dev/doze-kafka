package console

// The key map is declared once, here. shell.js dispatches single keys from it (the
// page embeds it as JSON), the help overlay on "?" is generated from it (Sy-06), the
// strip at the foot of every page draws the hints of its page from it, and the
// palette lists the same actions; keys_test.go fails for a key that shell.js handles
// and this table does not list. Nothing is bound in one place and documented in
// another.
//
// Single keys work when no text field has focus, and they never act on data without
// asking first: a key that would change something opens the form or the confirm, it
// does not do it.

import (
	"encoding/json"
	"html/template"
)

// Key scopes, in the order the help overlay shows them (Sy-06's three columns). The
// dialogs' own keys are bound by the dialogs; they are declared so that the map is
// the whole truth, and the help overlay does not list them.
const (
	ScopeGoTo    = "Go to"
	ScopeLists   = "In a list or table"
	ScopePage    = "On this page"
	ScopeDialogs = "In a dialog"
)

// keyBinding is one key.
type keyBinding struct {
	// Key is the event's key: "j", "/", "?", "Escape"; "g t" is a chord (g, then t);
	// a key that needs ⌘ or Ctrl is written "Mod+k".
	Key string `json:"key"`
	// Show is how it is drawn: "j", "⌘K", "g t", "Esc".
	Show string `json:"show"`
	// Action is what shell.js runs for it.
	Action string `json:"action"`
	// Label says what it does, in the help overlay.
	Label string `json:"label"`
	Scope string `json:"scope"`
	// Target is where a "goto" action goes, relative to the console's prefix.
	Target string `json:"target,omitempty"`
}

var keyMap = []keyBinding{
	{"g r", "g r", "goto", "Traffic", ScopeGoTo, "/traffic"},
	{"g t", "g t", "goto", "Topics", ScopeGoTo, "/topics"},
	{"g g", "g g", "goto", "Groups", ScopeGoTo, "/groups"},
	{"g s", "g s", "goto", "Schemas", ScopeGoTo, "/schemas"},
	{"g a", "g a", "goto", "Access", ScopeGoTo, "/access"},
	{"g c", "g c", "goto", "Cluster", ScopeGoTo, "/cluster"},
	{"Mod+k", "⌘K", "palette", "Jump to anything", ScopeGoTo, ""},

	{"j", "j", "next", "Next row", ScopeLists, ""},
	{"k", "k", "prev", "Previous row", ScopeLists, ""},
	{"Enter", "↵", "open", "Open the row on the right", ScopeLists, ""},
	{"Escape", "esc", "close", "Close the panel, clear the filter", ScopeLists, ""},
	{"/", "/", "filter", "Filter this table", ScopeLists, ""},
	{"]", "]", "tab-next", "Next tab", ScopeLists, ""},
	{"[", "[", "tab-prev", "Previous tab", ScopeLists, ""},

	{"p", "p", "pause", "Pause or resume the live view", ScopePage, ""},
	{"p", "p", "produce", "Produce (on a topic)", ScopePage, ""},
	{"c", "c", "create", "Create (on Topics, Subjects)", ScopePage, ""},
	{"?", "?", "help", "This help", ScopePage, ""},
	{"Mod+Enter", "⌘↵", "send", "Send from the composer", ScopePage, ""},

	{"ArrowDown", "↓", "palette-down", "Next result in the palette", ScopeDialogs, ""},
	{"ArrowUp", "↑", "palette-up", "Previous result in the palette", ScopeDialogs, ""},
	{"Tab", "Tab", "trap", "Move between the controls of a dialog", ScopeDialogs, ""},
	{" ", "Space", "open-row", "Open a focused row", ScopeDialogs, ""},
}

// keymapJSON is the table for shell.js.
func keymapJSON() template.JS {
	b, _ := json.Marshal(keyMap)
	return template.JS(b)
}

// keyGroup is the table grouped for the help overlay.
type keyGroup struct {
	Scope string
	Keys  []keyBinding
}

func keyGroups() []keyGroup {
	var out []keyGroup
	for _, scope := range []string{ScopeGoTo, ScopeLists, ScopePage} {
		g := keyGroup{Scope: scope}
		for _, k := range keyMap {
			if k.Scope == scope {
				g.Keys = append(g.Keys, k)
			}
		}
		out = append(out, g)
	}
	return out
}

// keyHint is one entry of the strip at the foot of a page: the keys, and what they do.
type keyHint struct {
	Keys  []string
	Label string
}

var (
	hintMove   = keyHint{[]string{"j", "k"}, "move"}
	hintOpen   = keyHint{[]string{"↵"}, "open"}
	hintFilter = keyHint{[]string{"/"}, "filter"}
	hintJump   = keyHint{[]string{"⌘K"}, "jump or act"}
	hintClose  = keyHint{[]string{"esc"}, "close panel"}
	hintHelp   = keyHint{[]string{"?"}, "all keys"}
)

// pageHints are the hints each kind of page draws at its foot. Every key named here is
// one the page really has (design_test.go in the browser suite presses them).
func pageHints(view string) []keyHint {
	switch view {
	case "traffic":
		return []keyHint{hintMove, hintOpen, hintFilter, {[]string{"p"}, "pause"}, hintJump}
	case "topics":
		return []keyHint{hintMove, hintOpen, hintFilter, {[]string{"c"}, "create topic"}, hintJump}
	case "topic":
		return []keyHint{hintMove, hintOpen, hintClose, {[]string{"g", "t"}, "topics"}, {[]string{"p"}, "produce"}, hintHelp}
	case "groups":
		return []keyHint{hintMove, hintOpen, hintFilter, hintJump}
	case "group":
		return []keyHint{hintMove, hintOpen, hintClose, {[]string{"g", "g"}, "groups"}, hintHelp}
	case "schemas":
		return []keyHint{hintMove, {[]string{"↵"}, "open subject"}, hintFilter, {[]string{"c"}, "register schema"}, hintJump}
	case "schema":
		return []keyHint{hintMove, hintClose, {[]string{"g", "s"}, "schemas"}, hintHelp}
	case "cluster":
		return []keyHint{{[]string{"g", "c"}, "cluster"}, hintOpen, hintJump, hintHelp}
	case "access":
		return []keyHint{hintMove, hintOpen, hintFilter, hintJump}
	case "trace":
		return []keyHint{{[]string{"/"}, "trace id"}, hintMove, {[]string{"↵"}, "follow"}, hintJump}
	case "first_run":
		return []keyHint{{[]string{"c"}, "create a topic"}, hintJump, hintHelp}
	}
	return []keyHint{hintJump, hintHelp}
}

// clientAction is something the palette can do in the browser, not by going to an
// address: it is declared here so that the palette's catalogue (/api/palette) has
// the console's actions in one list with the server's own.
type clientAction struct {
	ID, Label string
}

var clientActions = []clientAction{
	{"theme:light", "Use the light theme"},
	{"theme:dark", "Use the dark theme"},
	{"theme:contrast", "Use the high-contrast theme"},
	{"theme:system", "Match the system theme"},
	{"pause", "Pause or resume live updates"},
	{"help", "Show the keyboard shortcuts"},
}
