package console

import (
	"encoding/hex"
	"net/url"
	"strings"
)

// Every link to a topic or a group is built here, because a resource name is
// data, not a path. A group id is whatever a client chose — Kafka allows any
// non-empty string, "/", "?", "#" and "%" included — and gluing it into a URL
// with plain concatenation produced dead links (a "/" starts a new path
// segment) and mis-routed mutations (a "?" starts a query). Topic names are
// now validated, but topics created before that, and everything in templates,
// still go through the same builder so there is one place that gets it right.
//
// net/http's mux matches on the escaped path and unescapes each segment, so
// r.PathValue returns the original name and a %2F stays inside its segment.

// topicPath is the page for one topic.
func topicPath(prefix, name string) string { return prefix + "/topics/" + url.PathEscape(name) }

// groupPath is the page for one consumer group.
func groupPath(prefix, id string) string { return prefix + "/groups/" + groupSegment(id) }

// groupSegment is a group id as one path segment. PathEscape leaves dots alone,
// and a segment that is "." or ".." is navigation, not a name: the browser and the
// mux both resolve it away, so a group with such an id could not be opened, reset
// or deleted. Those (and anything starting with "~", to keep the scheme
// unambiguous) are written as "~" followed by the id in hex.
func groupSegment(id string) string {
	if id == "." || id == ".." || strings.HasPrefix(id, "~") {
		return "~" + hex.EncodeToString([]byte(id))
	}
	return url.PathEscape(id)
}

// groupFromSegment undoes groupSegment for a value from r.PathValue.
func groupFromSegment(seg string) string {
	if rest, ok := strings.CutPrefix(seg, "~"); ok {
		if b, err := hex.DecodeString(rest); err == nil {
			return string(b)
		}
	}
	return seg
}

// newTopicPath is the create-topic form. It is deliberately not under /topics/
// where a topic literally named "create" would have collided with it.
func newTopicPath(prefix string) string { return prefix + "/new/topic" }

func (c *Console) topicURL(name string) string { return topicPath(c.prefix, name) }
func (c *Console) newTopicURL() string         { return newTopicPath(c.prefix) }
func (c *Console) groupURL(id string) string   { return groupPath(c.prefix, id) }
