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
	{"c", "c", "create", "Create (on Topics, Subjects, Access); connect an app (on an empty Traffic)", ScopePage, ""},
	{"r", "r", "retry", "Retry now, when the broker is lost (on Traffic)", ScopePage, ""},
	{"e", "e", "errors", "Errors only, and back (on Traffic)", ScopePage, ""},
	{"u", "u", "unfold", "Unfold the open folded row (on Traffic)", ScopePage, ""},
	{"m", "m", "metrics", "All metrics of the open client (on Traffic)", ScopePage, ""},
	{"c", "c", "copy", "Copy the decoded fields of the open request (on Traffic)", ScopePage, ""},
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
	// Only says when the hint is drawn: "" always, "live" while the broker answers,
	// "lost" while it does not (the page's own state, an attribute on the document).
	Only string
}

var (
	hintMove   = keyHint{Keys: []string{"j", "k"}, Label: "move"}
	hintOpen   = keyHint{Keys: []string{"↵"}, Label: "open"}
	hintFilter = keyHint{Keys: []string{"/"}, Label: "filter"}
	hintJump   = keyHint{Keys: []string{"⌘K"}, Label: "jump or act"}
	hintClose  = keyHint{Keys: []string{"esc"}, Label: "close panel"}
	hintHelp   = keyHint{Keys: []string{"?"}, Label: "all keys"}
)

// pageHints are the hints each kind of page draws at its foot. Every key named here is
// one the page really has (design_test.go in the browser suite presses them).
func pageHints(view string) []keyHint {
	switch view {
	case "traffic":
		// While the broker is lost (Traf-10) the strip is r, j k, jump: the live hints are
		// drawn only while it answers, and the lost ones only while it does not.
		return []keyHint{live(hintMove), live(hintOpen), live(hintFilter), live(keyHint{Keys: []string{"e"}, Label: "errors only"}), live(keyHint{Keys: []string{"p"}, Label: "pause"}), live(hintJump),
			lost(keyHint{Keys: []string{"r"}, Label: "retry now"}), lost(hintMove), lost(hintJump)}
	case "traffic_empty":
		// Nothing has reached the broker (Traf-09): connect an app, pause, jump.
		return []keyHint{live(keyHint{Keys: []string{"c"}, Label: "connect an app"}), live(keyHint{Keys: []string{"p"}, Label: "pause"}), live(hintJump),
			lost(keyHint{Keys: []string{"r"}, Label: "retry now"}), lost(hintJump)}
	case "topics":
		return []keyHint{hintMove, hintOpen, hintFilter, {Keys: []string{"c"}, Label: "create topic"}, hintJump}
	case "topic":
		return []keyHint{hintMove, hintOpen, hintClose, {Keys: []string{"g", "t"}, Label: "topics"}, {Keys: []string{"p"}, Label: "produce"}, hintHelp}
	case "groups":
		return []keyHint{hintMove, hintOpen, hintFilter, hintJump}
	case "group":
		return []keyHint{hintMove, hintOpen, hintClose, {Keys: []string{"g", "g"}, Label: "groups"}, hintHelp}
	case "schemas":
		return []keyHint{hintMove, {Keys: []string{"↵"}, Label: "open subject"}, hintFilter, {Keys: []string{"c"}, Label: "register schema"}, hintJump}
	case "schema":
		return []keyHint{hintMove, hintClose, {Keys: []string{"g", "s"}, Label: "schemas"}, hintHelp}
	case "cluster":
		return []keyHint{{Keys: []string{"g", "c"}, Label: "cluster"}, hintOpen, hintJump, hintHelp}
	case "access":
		return []keyHint{hintMove, hintOpen, {Keys: []string{"c"}, Label: "create ACL"}, hintFilter, hintJump}
	case "trace":
		return []keyHint{{Keys: []string{"/"}, Label: "trace id"}, hintMove, {Keys: []string{"↵"}, Label: "follow"}, hintJump}
	case "first_run":
		return []keyHint{{Keys: []string{"c"}, Label: "create a topic"}, hintJump, hintHelp}
	}
	return []keyHint{hintJump, hintHelp}
}

// dialogHints are the strip while a confirm is open (Tp-24): the dialog's own keys,
// which are all the keys there are then (shell.js turns the page's off).
func dialogHints() []keyHint {
	return []keyHint{
		{Keys: []string{"tab"}, Label: "move between buttons"},
		{Keys: []string{"esc"}, Label: "cancel"},
		{Keys: []string{"↵"}, Label: "press the focused button"},
	}
}

func live(h keyHint) keyHint { h.Only = "live"; return h }
func lost(h keyHint) keyHint { h.Only = "lost"; return h }

// clientAction is something the palette can do in the browser, not by going to an
// address: it is declared here so that the palette's catalogue (/api/palette) has
// the console's actions in one list with the server's own.
type clientAction struct {
	ID, Label string
}

var clientActions = []clientAction{
	{"pause", "Pause or resume live updates"},
	{"help", "Show the keyboard shortcuts"},
}
