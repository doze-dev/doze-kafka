package console

import "fmt"

// The words the console says when there is nothing to show. They live in one
// place so the same situation is described the same way everywhere, and so each
// distinct situation has its own sentence: "nothing exists", "nothing is selected"
// and "your filter matched nothing" call for different next steps and used to
// share one line.
var uiCopy = map[string]string{
	"topics.none.title": "No topics",
	"topics.none.hint":  "Nothing exists on this broker yet.",
	"topics.pick":       "Select a topic to browse its messages, or create a new one.",

	"groups.none.title": "No consumer groups yet",
	"groups.none.hint":  "A group appears when a consumer with a group.id connects, and stays listed while it has committed offsets.",
	"groups.pick":       "Select a consumer group to see its members and lag.",

	"schemas.none.title": "No schemas yet",
	"schemas.none.hint":  "A subject appears when a serializer registers a schema, or when you register one here.",
	"schemas.pick":       "Select a subject to see its versions and how they differ, or register a schema.",

	"list.nomatch": "No matches for “%s”",

	"messages.none":      "No messages on this partition yet. Send one below, or write to the topic from your app and it will appear here.",
	"messages.retention": "Every record in this partition was deleted by retention; the log now starts at offset %d.",
}

// uiText returns a piece of console copy, formatted with any arguments.
func uiText(key string, args ...any) string {
	s, ok := uiCopy[key]
	if !ok {
		return key // a missing key shows itself, which is easy to spot and cannot blank the page.
	}
	if len(args) > 0 {
		return fmt.Sprintf(s, args...)
	}
	return s
}
