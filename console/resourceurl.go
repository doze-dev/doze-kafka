package console

import "net/url"

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
func groupPath(prefix, id string) string { return prefix + "/groups/" + url.PathEscape(id) }

// newTopicPath is the create-topic form. It is deliberately not under /topics/
// where a topic literally named "create" would have collided with it.
func newTopicPath(prefix string) string { return prefix + "/new/topic" }

func (c *Console) topicURL(name string) string { return topicPath(c.prefix, name) }
func (c *Console) newTopicURL() string         { return newTopicPath(c.prefix) }
func (c *Console) groupURL(id string) string   { return groupPath(c.prefix, id) }
