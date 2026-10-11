package console

// The key map is declared once, here. shell.js dispatches single keys from it (the
// page embeds it as JSON), the help overlay on "?" is generated from it, and the
// palette lists the same actions; keys_test.go fails for a key that shell.js handles
// and this table does not list. Nothing is bound in one place and documented in
// another.

import (
	"encoding/json"
	"html/template"
)

// Key scopes, in the order the help overlay shows them.
const (
	ScopeAnywhere = "Anywhere"
	ScopeLists    = "In a list"
	ScopePage     = "On a page"
	ScopeDialogs  = "In a dialog"
)

// keyBinding is one key.
type keyBinding struct {
	// Key is the event's key: "j", "/", "?", "Escape". A key that needs ⌘ or Ctrl is
	// written "Mod+k".
	Key string `json:"key"`
	// Show is how it is drawn: "j", "⌘K", "Esc".
	Show string `json:"show"`
	// Action is what shell.js runs for it.
	Action string `json:"action"`
	// Label says what it does, in the help overlay.
	Label string `json:"label"`
	Scope string `json:"scope"`
}

var keyMap = []keyBinding{
	{"Mod+k", "⌘K", "palette", "Search or jump to a topic, group, subject or action", ScopeAnywhere},
	{"?", "?", "help", "Show this list of keys", ScopeAnywhere},
	{"/", "/", "filter", "Filter the list (or open the palette when there is none)", ScopeAnywhere},
	{"[", "[", "rail", "Collapse or expand the rail", ScopeAnywhere},
	{"j", "j", "next", "Move down the list", ScopeLists},
	{"k", "k", "prev", "Move up the list", ScopeLists},
	{"Enter", "Enter", "open", "Open the row under the cursor", ScopeLists},
	{"c", "c", "create", "New topic, or new schema", ScopeLists},
	{"m", "m", "compose", "Open the composer on a topic", ScopePage},
	{"p", "p", "pause", "Pause or resume live updates", ScopePage},
	{"Mod+Enter", "⌘↵", "send", "Send from the composer", ScopePage},
	{"Escape", "Esc", "close", "Close the dialog, palette or inspector", ScopeDialogs},
	{"ArrowDown", "↓", "palette-down", "Next result in the palette", ScopeDialogs},
	{"ArrowUp", "↑", "palette-up", "Previous result in the palette", ScopeDialogs},
	{"Tab", "Tab", "trap", "Move between the controls of a dialog", ScopeDialogs},
	{" ", "Space", "open-row", "Open a focused row", ScopeDialogs},
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
	for _, scope := range []string{ScopeAnywhere, ScopeLists, ScopePage, ScopeDialogs} {
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
	{"rail", "Collapse or expand the rail"},
	{"pause", "Pause or resume live updates"},
	{"help", "Show the keyboard shortcuts"},
}
